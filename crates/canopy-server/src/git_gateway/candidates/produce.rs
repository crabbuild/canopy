//! A resident producer owns native Git, exact generated inputs and final dispatch.
use super::*;
use crate::git_gateway::push::native::{active, bound, checkpoint, final_publication};
use crate::packs::{
    catalog::{CatalogFileLimits, CatalogFiles, CatalogIndexes},
    metadata::MetadataLimits,
    publication::{
        BeginRequest, CandidatePublicationReply, CatalogPreparation, DEFAULT_LEASE_MS,
        PublicationCoordinator, PublicationError, PublicationOutcome, StagingCoordinator,
        StagingError, StagingState, StagingTicket,
    },
    sources::NativePackDescriptor,
    verification::{NativeMetadataLimits, PhysicalLimits, PhysicalVerifier},
};
use std::io::Write;

/// Only the native producer can construct this witness. A client Ready DTO
/// cannot obtain authority to install a server-owned candidate ref.
pub(crate) struct ProducedCandidate {
    candidate: MergeCandidate,
    operation: [u8; 16],
}
impl ProducedCandidate {
    #[cfg(test)]
    pub(crate) fn verified_fixture(candidate: MergeCandidate, operation: [u8; 16]) -> Self {
        Self {
            candidate,
            operation,
        }
    }
    pub(crate) fn candidate(&self) -> &MergeCandidate {
        &self.candidate
    }
    pub(crate) fn operation(&self) -> [u8; 16] {
        self.operation
    }
}
fn work(error: impl StdError + Send + Sync + 'static) -> StagingError {
    StagingError::Input(Box::new(error))
}
fn failed(error: impl StdError + Send + Sync + 'static) -> GatewayError {
    GatewayError::Cell(Box::new(error))
}

impl GitGateway {
    pub(super) async fn publish_candidate(
        &self,
        candidate: MergeCandidate,
    ) -> Result<CandidateOutcome, GatewayError> {
        let bytes = serde_json::to_vec(&candidate).map_err(failed)?;
        let mut digest = blake3::Hasher::new();
        digest.update(b"canopy.generated-candidate-workflow.v1\0");
        digest.update(&self.repository.repository_id());
        digest.update(&bytes);
        let staging = self.repository.staging_coordinator().map_err(failed)?;
        let request = BeginRequest {
            repository: self.repository.repository_id(),
            operation: uuid::Uuid::new_v4().into_bytes(),
            request_digest: *digest.finalize().as_bytes(),
            actor: candidate.actor.clone(),
            lease_ms: DEFAULT_LEASE_MS,
        };
        // Serialize admission only: observers of the same frozen candidate
        // join its original owner and never race independent generated work.
        let admission = self.push.lock().await;
        let ticket = if let Some(ticket) =
            staging.join_generated_candidate(&request).map_err(failed)?
        {
            ticket
        } else {
            let ready = staging
                .ready_request(request, new_identity()?)
                .await
                .map_err(failed)?;
            let ticket = staging.submit(ready).map_err(|(error, _)| failed(error))?;
            let gateway = self.clone();
            let owner = staging.clone();
            let produced = candidate.clone();
            ticket
                .drive(move |ticket, publication| async move {
                    Box::pin(gateway.drive_candidate(owner, ticket, publication, produced)).await
                })
                .map_err(failed)?;
            ticket
        };
        drop(admission);
        let number = candidate.number;
        let actor = candidate.actor.clone();
        let id = candidate.request.id.clone();
        let reply = match ticket.wait_completion().await {
            StagingState::Published(Ok(PublicationOutcome::Candidate(value))) => value.output,
            StagingState::Published(Err(error)) => match error.as_ref() {
                PublicationError::Candidate(InvocationError::Rejected(value)) => {
                    value.output.clone()
                }
                _ => return Err(failed(error)),
            },
            StagingState::Uncertain(error) | StagingState::Fenced(error) => {
                return Err(failed(error));
            }
            _ => return Err(failed(StagingError::NotReady)),
        };
        match reply {
            CandidatePublicationReply::Applied {
                id: selected,
                digest,
                publication,
            } => {
                if uuid::Uuid::from_bytes(selected).to_string() != id {
                    return Err(failed(StagingError::Context));
                }
                let Some(candidate) = self
                    .repository
                    .merge_candidate(actor.as_str(), number, &id)
                    .await
                    .map_err(failed)?
                    .output
                else {
                    return Ok(CandidateOutcome::NotFound);
                };
                let bytes = serde_json::to_vec(&candidate).map_err(failed)?;
                if *blake3::hash(&bytes).as_bytes() != digest
                    || matches!(candidate.result, CandidateResult::Ready { .. })
                        != publication.is_some()
                {
                    return Err(failed(StagingError::Context));
                }
                Ok(CandidateOutcome::Applied(Box::new(candidate)))
            }
            CandidatePublicationReply::NotFound => Ok(CandidateOutcome::NotFound),
            CandidatePublicationReply::Forbidden => Ok(CandidateOutcome::Forbidden),
            CandidatePublicationReply::Conflict => Ok(CandidateOutcome::Conflict),
            CandidatePublicationReply::Denied(_) => Ok(CandidateOutcome::Conflict),
        }
    }

    async fn drive_candidate(
        &self,
        staging: Arc<StagingCoordinator>,
        ticket: StagingTicket,
        publication: PublicationCoordinator,
        mut candidate: MergeCandidate,
    ) -> Result<(), StagingError> {
        active(&staging, &ticket).await?;
        let gateway = self.clone();
        let (produced, packs, certificate) = ticket
            .spawn(move |context| async move {
                let cached = gateway
                    .build_cache(&candidate.actor, &[])
                    .await
                    .map_err(work)?;
                candidate.result = prepare_native(&context, &cached.backend, &candidate)
                    .await
                    .map_err(work)?;
                if !valid_result(&candidate.result) {
                    return Err(work(GitHttpError::TooLarge));
                }
                let packs = if matches!(candidate.result, CandidateResult::Ready { .. }) {
                    generated_pack(&context, &cached.backend, &gateway.artifacts)
                        .await
                        .map_err(work)?
                } else {
                    vec![]
                };
                let certificate = context
                    .seal_native_inputs(gateway.artifacts.clone(), packs.iter().copied())
                    .await
                    .map_err(work)?;
                let operation = context.token()?.operation;
                Ok((
                    ProducedCandidate {
                        candidate,
                        operation,
                    },
                    packs,
                    certificate,
                ))
            })?
            .wait()
            .await
            .map_err(work)?;
        checkpoint(&staging, &ticket, certificate).await?;
        let mut metadata = Vec::with_capacity(packs.len());
        for pack in packs {
            let gateway = self.clone();
            metadata.push(
                ticket
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
                        .map_err(work)?
                        .stage_metadata(NativeMetadataLimits::default())
                        .await
                        .map_err(work)
                    })?
                    .wait()
                    .await
                    .map_err(work)?,
            );
        }
        ticket.seal()?;
        bound(&staging, &ticket).await?;
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
            .map_err(work)?
            .with_native(self.native.clone()),
        );
        let base = Arc::new(ticket.open_base(indexes, files).await?);
        let gateway = self.clone();
        let ready = ticket
            .spawn_bound(move |_, context| async move {
                let mut builder = CatalogPreparation::new_staged(
                    &context,
                    &gateway.scratch_root,
                    gateway.disk_budget.clone(),
                    base,
                    MetadataLimits::default(),
                )
                .await
                .map_err(work)?;
                for staged in metadata {
                    builder.add_staged_pack(staged).await.map_err(work)?;
                }
                let prepared = Arc::new(builder.finish().await.map_err(work)?);
                prepared
                    .ready_native_candidate(
                        new_identity().map_err(work)?,
                        &produced,
                        &gateway.scratch_root,
                        gateway.disk_budget.clone(),
                        MetadataLimits::default(),
                    )
                    .await
                    .map_err(work)
            })?
            .wait()
            .await
            .map_err(work)?;
        let registered = ready
            .persist_recovery(&self.artifacts, new_identity().map_err(work)?)
            .await
            .map_err(work)?;
        let ready = ready
            .bind_recovery(registered, &self.artifacts)
            .map_err(work)?;
        let observer = ticket
            .publish_wait(&publication, ready)
            .await
            .map_err(work)?;
        match final_publication(&staging, &ticket, &observer).await {
            Ok(PublicationOutcome::Candidate(_)) => Ok(()),
            Err(StagingError::Publication(error))
                if matches!(
                    error.as_ref(),
                    PublicationError::Candidate(InvocationError::Rejected(_))
                ) =>
            {
                Ok(())
            }
            Err(error) => Err(error),
            _ => Err(StagingError::Context),
        }
    }
}

/// Pack only newly written loose objects. The base is composed of immutable
/// catalog packs, so this work and input size follow generated changes rather
/// than every historical object. Git resolves any deltas against these inputs
/// and emits a non-thin pack. The spool is disk-admitted and memory stays bounded.
async fn generated_pack(
    context: &crate::packs::publication::StagingContext,
    backend: &GitHttpBackend,
    store: &canopy_object_storage::artifact::ArtifactStore,
) -> Result<Vec<NativePackDescriptor>, GatewayError> {
    let cache = backend.cache.clone();
    let owner = context.physical_owner();
    let format = context.format();
    let operation = context.token().map_err(failed)?.artifact_operation;
    let relative = PathBuf::from(format!("canopy-generated-{}.oids", hex::encode(operation)));
    let spool = relative.clone();
    let claim = cache
        .native
        .try_admit(crate::native_resources::NativeWork::Read)?;
    let count = tokio::task::spawn_blocking(move || {
        let _owner = owner;
        let _claim = claim;
        let fence =
            crate::native_git::lock_file(&cache.git_dir().join(crate::native_git::WORKER_LOCK))?;
        fence.try_lock().map_err(std::io::Error::from)?;
        let mut writer = cache.writer(&spool)?;
        let mut count = 0u64;
        for directory in std::fs::read_dir(cache.git_dir().join("objects"))? {
            let directory = directory?;
            let prefix = directory.file_name();
            let prefix = prefix
                .to_str()
                .ok_or_else(|| std::io::Error::other("non-UTF8 loose object directory"))?;
            if matches!(prefix, "pack" | "info") {
                continue;
            }
            if prefix.len() != 2
                || !prefix
                    .bytes()
                    .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
                || !directory.file_type()?.is_dir()
            {
                return Err(std::io::Error::other(
                    "invalid generated loose object directory",
                ));
            }
            for entry in std::fs::read_dir(directory.path())? {
                let entry = entry?;
                let name = entry.file_name();
                let name = name
                    .to_str()
                    .ok_or_else(|| std::io::Error::other("invalid loose object name"))?;
                let oid = format!("{prefix}{name}");
                if !entry.file_type()?.is_file()
                    || oid.len() != format.bytes() * 2
                    || crate::ObjectId::from_hex(&oid).is_err()
                {
                    return Err(std::io::Error::other("invalid generated loose object"));
                }
                count += 1;
                // A fixed ceiling bounds request-private enumeration; disk
                // reservation independently enforces the service's byte limit.
                if count > 1_000_000 {
                    return Err(std::io::Error::other("generated object limit"));
                }
                writeln!(writer, "{oid}")?;
            }
        }
        writer.flush()?;
        drop(writer);
        fence.unlock()?;
        Ok::<_, std::io::Error>(count)
    })
    .await
    .map_err(failed)??;
    if count == 0 {
        return Ok(vec![]);
    }
    context.ensure_live().map_err(failed)?;
    let prefix = backend
        .git_dir()
        .join("objects/pack")
        .join(format!("canopy-generated-{}", hex::encode(operation)));
    let mut command = crate::native_git::command(&backend.git_dir())?;
    command
        .arg("--git-dir")
        .arg(backend.git_dir())
        .args(["pack-objects", "--no-reuse-object", "--no-reuse-delta"])
        .arg(prefix)
        .stdin(std::fs::File::open(backend.git_dir().join(relative))?)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut process = GitProcess::spawn(
        command,
        (backend.cache.clone(), context.physical_owner()),
        backend
            .cache
            .native
            .try_admit(crate::native_resources::NativeWork::Pack)?,
    )?;
    let stdout = process
        .child
        .stdout
        .take()
        .ok_or(GitHttpError::Interrupted)?;
    let stderr = process
        .child
        .stderr
        .take()
        .ok_or(GitHttpError::Interrupted)?;
    let (stdout, stderr) =
        tokio::try_join!(read_bounded(stdout, 128), read_bounded(stderr, 64 << 10))?;
    let status = process.wait().await?;
    if !status.success() {
        return Err(GitHttpError::GitExit {
            status,
            stderr: String::from_utf8_lossy(&stderr).into_owned(),
        }
        .into());
    }
    let checksum = output_oid(&stdout)?;
    backend
        .stage_generated_pack(context, store, PhysicalLimits::default(), &checksum)
        .await
        .map_err(failed)
}
