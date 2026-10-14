//! Symbolic HEAD changes use the same resident-owned staging and exact publication
//! lifecycle as pushes. HTTP observers own no producer or recovery command.
use super::push::native::{active, bound, final_publication};
use super::*;
use crate::{
    packs::publication::{HeadRequest, PublicationReply},
    packs::{
        catalog::{CatalogFileLimits, CatalogFiles, CatalogIndexes},
        metadata::MetadataLimits,
        publication::{
            BeginRequest, CatalogPreparation, DEFAULT_LEASE_MS, PublicationCoordinator,
            PublicationError, PublicationOutcome, StagingCoordinator, StagingError, StagingState,
            StagingTicket,
        },
    },
};
use cellule_runtime::{
    InvocationError,
    codec::{BoundedEncoder, WireValue},
};

fn work(error: impl StdError + Send + Sync + 'static) -> StagingError {
    StagingError::Input(Box::new(error))
}
fn failed(error: impl StdError + Send + Sync + 'static) -> GatewayError {
    GatewayError::Cell(Box::new(error))
}

impl GitGateway {
    /// Every request owns one exact attempt through resident staging.
    /// The final transaction rechecks ownership and the joint generation.
    pub async fn set_default_branch(
        &self,
        identity: MutationIdentity,
        actor: &str,
        request: HeadRequest,
    ) -> Result<PublicationReply, GatewayError> {
        if crate::directory::validate_component(actor).is_err() {
            return Err(failed(cellule_runtime::Error::Command(
                "invalid HEAD actor",
            )));
        }
        let mut encoded =
            BoundedEncoder::new(crate::packs::publication::NATIVE_HEAD_BYTES).map_err(failed)?;
        request.encode(&mut encoded).map_err(failed)?;
        let mut digest = blake3::Hasher::new();
        digest.update(b"canopy.symbolic-head-workflow.v1\0");
        digest.update(&self.repository.repository_id());
        digest.update(actor.as_bytes());
        digest.update(&[0]);
        digest.update(&encoded.finish());
        let staging = self.repository.staging_coordinator().map_err(failed)?;
        let ready = staging
            .ready_request(
                BeginRequest {
                    repository: self.repository.repository_id(),
                    operation: uuid::Uuid::new_v4().into_bytes(),
                    request_digest: *digest.finalize().as_bytes(),
                    actor: actor.into(),
                    lease_ms: DEFAULT_LEASE_MS,
                },
                new_identity()?,
            )
            .await
            .map_err(failed)?;
        let ticket = staging.submit(ready).map_err(|(error, _)| failed(error))?;
        let gateway = self.clone();
        let owner = staging.clone();
        ticket
            .drive(move |ticket, publication| async move {
                Box::pin(gateway.drive_head(owner, ticket, publication, identity, request)).await
            })
            .map_err(failed)?;
        match ticket.wait_completion().await {
            StagingState::Published(Ok(PublicationOutcome::Head(value))) => Ok(value.output),
            StagingState::Published(Err(error)) => match error.as_ref() {
                PublicationError::Head(InvocationError::Rejected(value)) => Ok(value.output),
                _ => Err(failed(error)),
            },
            StagingState::Uncertain(error) | StagingState::Fenced(error) => Err(failed(error)),
            _ => Err(failed(StagingError::NotReady)),
        }
    }

    async fn drive_head(
        &self,
        staging: Arc<StagingCoordinator>,
        ticket: StagingTicket,
        publication: PublicationCoordinator,
        identity: MutationIdentity,
        request: HeadRequest,
    ) -> Result<(), StagingError> {
        active(&staging, &ticket).await?;
        // Ref-only work has no incoming physical inputs. Bind selects the
        // current certified joint generation after all staged work drains.
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
                let prepared = Arc::new(
                    CatalogPreparation::new_staged(
                        &context,
                        &gateway.scratch_root,
                        gateway.disk_budget.clone(),
                        base,
                        MetadataLimits::default(),
                    )
                    .await
                    .map_err(work)?
                    .finish()
                    .await
                    .map_err(work)?,
                );
                prepared
                    .ready_native_head(identity, request)
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
        // Retrieve the producer result before Finishing so it cannot wait for
        // its own worker drain. The lifecycle captures the exact ready owner.
        let observer = ticket
            .publish_wait(&publication, ready)
            .await
            .map_err(work)?;
        match final_publication(&staging, &ticket, &observer).await {
            Ok(PublicationOutcome::Head(_)) => Ok(()),
            Err(StagingError::Publication(error))
                if matches!(
                    error.as_ref(),
                    PublicationError::Head(InvocationError::Rejected(_))
                ) =>
            {
                Ok(())
            }
            Err(error) => Err(error),
            _ => Err(StagingError::Context),
        }
    }
}
