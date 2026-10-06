//! Production receive-pack is a resident-owned workflow. Request observers
//! never own native work or acknowledge disposable cache refs.
use super::*;
use crate::packs::{
    catalog::{CatalogFileLimits, CatalogFiles, CatalogIndexes},
    metadata::MetadataLimits,
    publication::{
        CatalogPreparation, NativeInputCertificate, PublicationCoordinator, PublicationOutcome,
        PublicationState, PushCompletionRequest, RegisteredRootRecovery, StagedPublicationTicket,
        StagingCoordinator, StagingError, StagingState, StagingTicket,
    },
    verification::{NativeMetadataLimits, PhysicalLimits, PhysicalVerifier},
};
use canopy_object_storage::artifact::ArtifactRead;

fn input(error: impl StdError + Send + Sync + 'static) -> StagingError {
    StagingError::Input(Box::new(error))
}
fn observed(error: Arc<StagingError>) -> StagingError {
    input(error)
}
fn mutation() -> Result<MutationIdentity, StagingError> {
    new_identity().map_err(input)
}

impl GitGateway {
    pub(in crate::git_gateway) async fn handle_native_push(
        &self,
        encoded: preflight::EncodedPush,
    ) -> Result<GitHttpResponse<Body>, GatewayError> {
        let staging = self
            .repository
            .staging_coordinator()
            .map_err(|e| GatewayError::Cell(Box::new(e)))?;
        let identity = encoded.identity().clone();
        let id = identity.operation;
        if let Some(response) = staging
            .replay_request(identity.clone(), &self.artifacts)
            .await
            .map_err(|error| match error {
                crate::packs::publication::RootPushReplayError::Denied(
                    crate::packs::publication::PreparationDenial::Conflict,
                ) => GatewayError::Push(crate::PushError::Conflict),
                crate::packs::publication::RootPushReplayError::Denied(
                    crate::packs::publication::PreparationDenial::Unauthorized,
                ) => GatewayError::Unauthorized,
                error => GatewayError::Cell(Box::new(error)),
            })?
        {
            return Ok(with_push_id(artifact_body(response), id));
        }
        // Serialize admission only. Independent pushes execute under bounded
        // worker admission and publish against the authoritative bound floor.
        let admission = self.push.lock().await;
        let ticket = if let Some(ticket) = staging
            .join_request(&identity)
            .map_err(|e| GatewayError::Cell(Box::new(e)))?
        {
            ticket
        } else {
            let ready = staging
                .ready_request(identity.clone(), new_identity()?)
                .await
                .map_err(|e| GatewayError::Cell(Box::new(e)))?;
            let ticket = staging
                .submit(ready)
                .map_err(|(error, _)| GatewayError::Cell(Box::new(error)))?;
            let gateway = self.clone();
            let owner = staging.clone();
            // Transfer encoded bytes synchronously before observing any state.
            ticket
                .drive_receive(move |ticket, publication| async move {
                    Box::pin(gateway.drive_push(owner, ticket, publication, encoded)).await
                })
                .map_err(|e| GatewayError::Cell(Box::new(e)))?;
            ticket
        };
        drop(admission);
        match ticket.wait_completion().await {
            StagingState::Published(Ok(PublicationOutcome::RootPush(_))) => {
                // Use the known completion's original read capability and
                // receipt. Recheck current authorization before streaming.
                let publication = ticket
                    .pending_publication()
                    .ok_or_else(|| GatewayError::Cell(Box::new(StagingError::Context)))?;
                let response = publication
                    .root_response(&self.artifacts)
                    .await
                    .map_err(|e| GatewayError::Cell(Box::new(e)))?;
                Ok(with_push_id(artifact_body(response), id))
            }
            StagingState::Published(Err(error)) => Err(GatewayError::Cell(Box::new(error))),
            StagingState::Uncertain(error) | StagingState::Fenced(error) => {
                Err(GatewayError::Cell(Box::new(error)))
            }
            _ => Err(GatewayError::Cell(Box::new(StagingError::NotReady))),
        }
    }

    async fn drive_push(
        &self,
        staging: Arc<StagingCoordinator>,
        ticket: StagingTicket,
        publication: PublicationCoordinator,
        encoded: preflight::EncodedPush,
    ) -> Result<(), StagingError> {
        active(&staging, &ticket).await?;
        let store = self.artifacts.clone();
        let (encoded, request) = ticket
            .spawn(move |context| async move {
                let (encoded, saved) = encoded.retain(&context, &store).await.map_err(input)?;
                let checkpoint = context
                    .seal_push_inputs(store, std::iter::empty(), saved)
                    .await
                    .map_err(input)?;
                Ok((encoded, checkpoint))
            })?
            .wait()
            .await
            .map_err(observed)?;
        checkpoint(&staging, &ticket, request.clone()).await?;
        let gateway = self.clone();
        let (packs, certificate, plan) = ticket
            .spawn(move |context| async move {
                let preflight = encoded
                    .decode(&gateway.scratch_root, &gateway.disk_budget, None)
                    .await
                    .map_err(input)?;
                let (completion, packs) = gateway
                    .native_result(&context, preflight)
                    .await
                    .map_err(input)?;
                let plan = completion.plan.clone();
                let result = context
                    .retain_native_result(
                        &gateway.artifacts,
                        &request,
                        completion,
                        &gateway.scratch_root,
                        &gateway.disk_budget,
                    )
                    .await
                    .map_err(input)?;
                let certificate = context
                    .append_native_result(
                        gateway.artifacts.clone(),
                        &request,
                        packs.iter().copied(),
                        result,
                    )
                    .await
                    .map_err(input)?;
                Ok((packs, certificate, plan))
            })?
            .wait()
            .await
            .map_err(observed)?;
        checkpoint(&staging, &ticket, certificate).await?;
        // Only bounded descriptor spools survive physical verification. Native
        // databases and worker activities drain before Bind can start.
        let mut metadata = Vec::with_capacity(packs.len());
        if plan.is_some() {
            for pack in packs {
                let gateway = self.clone();
                let staged = ticket
                    .spawn(move |context| async move {
                        PhysicalVerifier::download_staged(
                            &context,
                            &gateway.scratch_root,
                            gateway.disk_budget.clone(),
                            &gateway.artifacts,
                            pack,
                            PhysicalLimits::default(),
                            gateway.native.clone(),
                        )
                        .await
                        .map_err(input)?
                        .stage_metadata(NativeMetadataLimits::default())
                        .await
                        .map_err(input)
                    })?
                    .wait()
                    .await
                    .map_err(observed)?;
                metadata.push(staged);
            }
        }
        ticket.seal()?;
        bound(&staging, &ticket).await?;
        let session = ticket.bound_session()?;
        let Some(plan) = plan else {
            let gateway = self.clone();
            let ready = ticket
                .spawn_bound(move |_, _context| async move {
                    session
                        .ready_root_outcome(
                            mutation()?,
                            &gateway.artifacts,
                            &gateway.scratch_root,
                            gateway.disk_budget.clone(),
                            gateway.signer_directory.as_deref(),
                        )
                        .await
                        .map_err(input)
                })?
                .wait()
                .await
                .map_err(observed)?;
            let registered = ready
                .persist_recovery(&self.artifacts, mutation()?)
                .await
                .map_err(input)?;
            let ready = ready
                .bind_recovery(registered, &self.artifacts)
                .map_err(input)?;
            let observer = ticket
                .publish_wait(&publication, ready)
                .await
                .map_err(input)?;
            final_publication(&staging, &ticket, &observer).await?;
            return Ok(());
        };
        let format = self.repository.object_format();
        let indexes = Arc::new(CatalogIndexes::new(self.artifacts.clone(), format));
        let files = Arc::new(
            CatalogFiles::new(
                &self.scratch_root,
                self.disk_budget.clone(),
                self.artifacts.clone(),
                format,
                CatalogFileLimits::default(),
            )
            .map_err(input)?
            .with_native(self.native.clone()),
        );
        let base = Arc::new(ticket.open_base(indexes, files).await?);
        let gateway = self.clone();
        let prepared = Arc::new(
            ticket
                .spawn_bound(move |_, context| async move {
                    let mut builder = CatalogPreparation::new_staged(
                        &context,
                        &gateway.scratch_root,
                        gateway.disk_budget.clone(),
                        base,
                        MetadataLimits::default(),
                    )
                    .await
                    .map_err(input)?;
                    for staged in metadata {
                        builder.add_staged_pack(staged).await.map_err(input)?;
                    }
                    builder.finish().await.map_err(input)
                })?
                .wait()
                .await
                .map_err(observed)?,
        );
        let gateway = self.clone();
        let owner = prepared.clone();
        let (policy, refusal) = ticket
            .spawn_bound(move |_, _context| async move {
                let policy = Arc::new(
                    owner
                        .ref_policy_preparation(
                            plan,
                            &gateway.scratch_root,
                            gateway.disk_budget.clone(),
                            MetadataLimits::default(),
                        )
                        .await
                        .map_err(input)?,
                );
                let refusal = Arc::new(
                    session
                        .ready_root_refusal(
                            mutation()?,
                            &gateway.artifacts,
                            &gateway.scratch_root,
                            gateway.disk_budget.clone(),
                            gateway.signer_directory.as_deref(),
                        )
                        .await
                        .map_err(input)?,
                );
                Ok((policy, refusal))
            })?
            .wait()
            .await
            .map_err(observed)?;
        let mut offset = 0;
        let mut previous: Option<RegisteredRootRecovery> = None;
        while offset < policy.plan().updates.len() {
            let intent = policy.clone();
            let owner = prepared.clone();
            let refusal = refusal.clone();
            let store = self.artifacts.clone();
            let head = previous.clone();
            let (ready, registered, end) = ticket
                .spawn_bound(move |_, _context| async move {
                    let ready = intent
                        .ready_page(&owner, mutation()?, offset)
                        .await
                        .map_err(input)?
                        .with_refusal(refusal)
                        .map_err(input)?;
                    let end = ready.end_offset();
                    let registered = ready
                        .persist_recovery(&store, mutation()?, head.as_ref())
                        .await
                        .map_err(input)?;
                    let ready = ready
                        .bind_recovery(registered.clone(), &store)
                        .map_err(input)?;
                    Ok((ready, registered, end))
                })?
                .wait()
                .await
                .map_err(observed)?;
            let observer = ticket
                .register_policy_page_wait(&publication, ready)
                .await
                .map_err(input)?;
            match final_publication(&staging, &ticket, &observer).await? {
                PublicationOutcome::PolicyPage(_) => bound(&staging, &ticket).await?,
                PublicationOutcome::RootPush(_) => return Ok(()),
                _ => return Err(StagingError::Context),
            }
            previous = Some(registered);
            offset = end;
        }
        let gateway = self.clone();
        let frozen_refusal = refusal.clone();
        let ready = ticket
            .spawn_bound(move |_, _context| async move {
                let guard = policy.ready(&prepared).await.map_err(input)?;
                prepared
                    .ready_root_push(
                        mutation()?,
                        &guard,
                        &gateway.scratch_root,
                        gateway.disk_budget.clone(),
                        MetadataLimits::default(),
                        gateway.signer_directory.as_deref(),
                    )
                    .await
                    .map_err(input)?
                    .with_refusal(frozen_refusal)
                    .map_err(input)
            })?
            .wait()
            .await
            .map_err(observed)?;
        let registered = ready
            .persist_recovery_after(
                &self.artifacts,
                mutation()?,
                previous.as_ref().ok_or(StagingError::Context)?,
            )
            .await
            .map_err(input)?;
        let ready = ready
            .bind_recovery(registered, &self.artifacts)
            .map_err(input)?;
        let observer = ticket
            .publish_wait(&publication, ready)
            .await
            .map_err(input)?;
        final_publication(&staging, &ticket, &observer).await?;
        Ok(())
    }

    async fn native_result(
        &self,
        context: &crate::packs::publication::StagingContext,
        preflight: preflight::PushPreflight,
    ) -> Result<
        (
            PushCompletionRequest,
            Vec<crate::packs::sources::NativePackDescriptor>,
        ),
        GatewayError,
    > {
        let preflight::PushParts {
            request,
            commands,
            identity,
        } = preflight.into_parts();
        let actor = identity.actor.as_str();
        let option_error = commands.option_error().or_else(|| {
            (commands.certificate().is_some() && self.signer_directory.is_none())
                .then_some("Canopy signed pushes are unavailable on this gateway")
        });
        let result = async {
            if let Some(reason) = option_error {
                return Ok((
                    commands
                        .rejection(reason)?
                        .ok_or(GatewayError::MalformedCache)?,
                    None,
                    None,
                    Vec::new(),
                ));
            }
            let names = commands.names();
            let cached = self.build_cache(actor, &names).await?;
            self.install_branch_policy(&cached, &commands).await?;
            let signers = self
                .install_certificate_policy(&cached, &commands, actor)
                .await?;
            let backend = signers.map_or_else(
                || cached.backend.clone(),
                |path| cached.backend.with_signers(path),
            );
            let response = backend.run_native_receive(context, request).await?;
            let certificate = self
                .verified_certificate(&cached, &commands, actor, identity.request_digest)
                .await?;
            if let Some(verified) = &certificate {
                backend
                    .remove_disposable_certificate(
                        context,
                        crate::object_id(
                            self.repository.object_format(),
                            ObjectKind::Blob,
                            &verified.body,
                        ),
                    )
                    .await
                    .map_err(|e| GatewayError::Cell(Box::new(e)))?;
            }
            let plan = if response.status == 200 {
                let after = git_refs(&cached.backend, &names).await?;
                let plan = diff_refs(&cached.refs, &after, actor);
                (!plan.updates.is_empty()).then_some(plan)
            } else {
                None
            };
            let packs = if plan.is_some() {
                backend
                    .stage_native_packs(context, &self.artifacts, PhysicalLimits::default())
                    .await
                    .map_err(|e| GatewayError::Cell(Box::new(e)))?
            } else {
                Vec::new()
            };
            Ok::<_, GatewayError>((response, plan, certificate, packs))
        }
        .await;
        let (response, plan, certificate, packs) = match result {
            Ok(result) => result,
            Err(error) => {
                let reason = match &error {
                    GatewayError::Cache(error) | GatewayError::Http(GitHttpError::Cache(error))
                        if error.is_admission() =>
                    {
                        "Canopy push failed before publication: cache disk budget exhausted"
                    }
                    GatewayError::Certificate(reason) => reason,
                    _ => "Canopy push failed before publication; retry after server recovery",
                };
                let Some(response) = commands.rejection(reason)? else {
                    return Err(error);
                };
                tracing::warn!(push_id = %hex::encode(identity.operation), ?error, "native push failed before publication");
                (response, None, None, Vec::new())
            }
        };
        Ok((
            PushCompletionRequest {
                response,
                plan,
                certificate,
                options: if option_error.is_some() {
                    Vec::new()
                } else {
                    commands.options().to_vec()
                },
            },
            packs,
        ))
    }
}

pub(in crate::git_gateway) async fn active(
    staging: &StagingCoordinator,
    ticket: &StagingTicket,
) -> Result<(), StagingError> {
    loop {
        match ticket.wait().await {
            StagingState::Active(_) => return Ok(()),
            StagingState::Uncertain(_) => {
                staging.recover(ticket)?;
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
            StagingState::Fenced(error) => return Err(observed(error)),
            _ => return Err(StagingError::Inactive),
        }
    }
}
pub(in crate::git_gateway) async fn bound(
    staging: &StagingCoordinator,
    ticket: &StagingTicket,
) -> Result<(), StagingError> {
    loop {
        match ticket.wait_terminal().await {
            StagingState::Bound(_) => return Ok(()),
            StagingState::Uncertain(_) => {
                staging.recover(ticket)?;
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
            StagingState::Fenced(error) => return Err(observed(error)),
            _ => return Err(StagingError::Inactive),
        }
    }
}
pub(in crate::git_gateway) async fn checkpoint(
    staging: &StagingCoordinator,
    ticket: &StagingTicket,
    proof: NativeInputCertificate,
) -> Result<(), StagingError> {
    let registration = ticket
        .register_inputs(proof, mutation()?)
        .map_err(|(error, _)| error)?;
    loop {
        match registration.wait().await {
            Ok(_) => return Ok(()),
            Err(_) if matches!(ticket.state(), StagingState::Uncertain(_)) => {
                staging.recover(ticket)?;
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
            Err(error) => return Err(observed(error)),
        }
    }
}
pub(in crate::git_gateway) async fn final_publication(
    staging: &StagingCoordinator,
    ticket: &StagingTicket,
    observer: &StagedPublicationTicket,
) -> Result<PublicationOutcome, StagingError> {
    loop {
        match observer.wait().await {
            PublicationState::Finished(Ok(value)) => return Ok(value),
            PublicationState::Finished(Err(error)) => return Err(StagingError::Publication(error)),
            PublicationState::Uncertain(_) => {
                staging.recover(ticket)?;
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
            _ => return Err(StagingError::Inactive),
        }
    }
}
fn artifact_body(response: GitHttpResponse<ArtifactRead>) -> GitHttpResponse<Body> {
    let stream = futures_util::stream::try_unfold(response.body, |mut body| async move {
        Ok::<_, canopy_object_storage::artifact::ArtifactError>(
            body.next().await?.map(|bytes| (bytes, body)),
        )
    });
    GitHttpResponse {
        status: response.status,
        headers: response.headers,
        body: Body::from_stream(stream),
    }
}
