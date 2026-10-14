//! Generated candidates share the original private owner and exact registered dispatch.
use super::*;
use canopy_object_storage::artifact::ArtifactStore;

#[must_use]
pub struct ReadyNativeCandidate {
    owner: Arc<PreparedCatalog>,
    command: PreparedCommand<PublishNativeCandidate>,
}
impl PreparedCatalog {
    pub(crate) async fn ready_native_candidate(
        self: &Arc<Self>,
        identity: MutationIdentity,
        produced: &crate::git_gateway::candidates::ProducedCandidate,
        directory: &std::path::Path,
        budget: cellule_ltx::DiskBudget,
        limits: crate::packs::metadata::MetadataLimits,
    ) -> Result<ReadyNativeCandidate, NativeCandidatePublicationError> {
        let proof = self
            .native_candidate_proof(produced, directory, budget, limits)
            .await?;
        self.ensure_live()?;
        let (client, target, _) = self.base.capability();
        let command = client
            .prepare_command::<PublishNativeCandidate>(target, identity, proof)
            .await
            .map_err(|e| NativeCandidatePublicationError::Command(Box::new(e)))?;
        self.ensure_live()?;
        Ok(ReadyNativeCandidate {
            owner: self.clone(),
            command,
        })
    }
}
impl ReadyNativeCandidate {
    pub async fn persist_recovery(
        &self,
        store: &ArtifactStore,
        identity: MutationIdentity,
    ) -> Result<RegisteredRootRecovery, RootRecoveryError> {
        super::super::recovery::persist(
            &self.owner.base.session,
            &self.command,
            super::super::recovery::Kind::Candidate,
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
            super::super::recovery::Kind::Candidate,
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
