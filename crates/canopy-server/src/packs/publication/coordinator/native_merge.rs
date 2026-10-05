//! Native merges share the original private owner and exact registered dispatch.
use super::*;
use crate::pulls::merge::{MergeRequest, command::MergeInput};
use canopy_object_storage::artifact::ArtifactStore;

#[must_use]
pub struct ReadyNativeMerge {
    owner: Arc<PreparedCatalog>,
    command: PreparedCommand<PublishReviewedMerge>,
}
impl PreparedCatalog {
    pub async fn ready_native_merge(
        self: &Arc<Self>,
        identity: MutationIdentity,
        number: i64,
        request: MergeRequest,
        directory: &Path,
        budget: DiskBudget,
        limits: MetadataLimits,
    ) -> Result<ReadyNativeMerge, NativeMergePreparationError> {
        let input = MergeInput {
            actor: self.base.capability().2.actor.clone(),
            number,
            request,
            issued_at_ms: identity.issued_at_ms,
        };
        let proof = self
            .native_merge_proof(input, directory, budget, limits)
            .await?;
        self.ensure_live()?;
        let (client, target, _) = self.base.capability();
        let command = client
            .prepare_command::<PublishReviewedMerge>(target, identity, proof)
            .await
            .map_err(|e| NativeMergePreparationError::Command(Box::new(e)))?;
        self.ensure_live()?;
        Ok(ReadyNativeMerge {
            owner: self.clone(),
            command,
        })
    }
}
impl ReadyNativeMerge {
    pub async fn persist_recovery(
        &self,
        store: &ArtifactStore,
        identity: MutationIdentity,
    ) -> Result<RegisteredRootRecovery, RootRecoveryError> {
        super::super::recovery::persist(
            &self.owner.base.session,
            &self.command,
            super::super::recovery::Kind::Merge,
            store,
            identity,
            0,
        )
        .await
    }
    pub fn bind_recovery(
        self,
        registered: RegisteredRootRecovery,
        store: &ArtifactStore,
    ) -> Result<ReadyBoundRecovery, Box<RecoveryBindingFailure<Self>>> {
        if !registered.matches_original(
            super::super::recovery::Kind::Merge,
            self.command.evidence(),
            None,
            &self.owner.base.session,
            store,
        ) {
            return Err(Box::new(RecoveryBindingFailure {
                original: self,
                registered,
            }));
        }
        Ok(ReadyBoundRecovery::new(
            PushPreparation::Catalog(self.owner),
            None,
            false,
            registered,
            store,
        ))
    }
}
