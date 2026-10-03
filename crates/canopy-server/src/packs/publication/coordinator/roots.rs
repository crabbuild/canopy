//! Exact immutable root completion uses the shared fair dispatch and lifecycle.
use super::*;
use crate::directory::DirectoryCell;

pub(super) const ROOT_RESERVATION: u64 = 2 * ROOT_COMPLETION_BYTES as u64;

#[derive(Debug, thiserror::Error)]
pub enum RootPushReadyError {
    #[error("immutable root completion preparation failed")]
    Preparation(#[source] Box<RootCompletionPreparationError>),
    #[error("immutable root completion preparation inactive")]
    Base(#[from] PreparationBaseError),
    #[error("immutable root completion encoding failed")]
    Codec(#[from] CodecError),
    #[error("immutable root completion command preparation failed")]
    Command(#[source] Box<InvocationError<RootCompletionReply>>),
}

/// Retain this exact private factory output on admission failure or uncertainty.
/// Regenerating a completion allocates a different response ID and is not retry.
#[must_use]
pub struct ReadyRootPush {
    pub(super) owner: PushPreparation,
    command: RootCommand,
    pub(super) refusal: bool,
}
impl PreparedCatalog {
    pub async fn ready_root_push(
        self: &Arc<Self>,
        identity: MutationIdentity,
        guard: &PreparedRefPolicyGuard,
        directory: &Path,
        budget: DiskBudget,
        limits: MetadataLimits,
        signers: Option<&DirectoryCell>,
    ) -> Result<ReadyRootPush, RootPushReadyError> {
        let input = self
            .root_push_completion(guard, directory, budget, limits, signers)
            .await
            .map_err(|error| RootPushReadyError::Preparation(Box::new(error)))?;
        input.encode(&mut BoundedEncoder::new(ROOT_COMPLETION_BYTES)?)?;
        self.ensure_live()?;
        let (client, target, _) = self.base.capability();
        let command = client
            .prepare_command::<CompleteRootPush>(target, identity, input)
            .await
            .map_err(|error| RootPushReadyError::Command(Box::new(error)))?;
        self.ensure_live()?;
        Ok(ReadyRootPush {
            owner: PushPreparation::Catalog(self.clone()),
            command: RootCommand::Publish(command),
            refusal: false,
        })
    }
}
impl PreparationSession {
    pub async fn ready_root_outcome(
        self: &Arc<Self>,
        identity: MutationIdentity,
        store: &canopy_object_storage::artifact::ArtifactStore,
        directory: &Path,
        budget: DiskBudget,
        signers: Option<&DirectoryCell>,
    ) -> Result<ReadyRootPush, RootPushReadyError> {
        let input = self
            .root_outcome_completion(store, directory, budget, signers)
            .await
            .map_err(|error| RootPushReadyError::Preparation(Box::new(error)))?;
        self.ready_immutable_outcome(identity, input).await
    }
    /// Prepare before policy dispatch, while custody and signer authority are
    /// live. Retain this exact command through any subsequent refusal/recovery.
    pub async fn ready_root_refusal(
        self: &Arc<Self>,
        identity: MutationIdentity,
        store: &canopy_object_storage::artifact::ArtifactStore,
        directory: &Path,
        budget: DiskBudget,
        signers: Option<&DirectoryCell>,
    ) -> Result<ReadyRootPush, RootPushReadyError> {
        let input = self
            .root_refusal_completion(store, directory, budget, signers)
            .await
            .map_err(|error| RootPushReadyError::Preparation(Box::new(error)))?;
        self.ready_immutable_outcome(identity, input).await
    }
    async fn ready_immutable_outcome(
        self: &Arc<Self>,
        identity: MutationIdentity,
        input: RootOutcomeCompletion,
    ) -> Result<ReadyRootPush, RootPushReadyError> {
        let refusal = input.refusal;
        input.encode(&mut BoundedEncoder::new(ROOT_COMPLETION_BYTES)?)?;
        self.live_lease()?;
        let command = self
            .client
            .prepare_command::<CompleteRootOutcome>(&self.target, identity, input)
            .await
            .map_err(|error| RootPushReadyError::Command(Box::new(error)))?;
        self.live_lease()?;
        Ok(ReadyRootPush {
            owner: PushPreparation::Outcome(self.clone()),
            command: RootCommand::Outcome(command),
            refusal,
        })
    }
}
#[derive(Clone)]
enum RootCommand {
    Publish(PreparedCommand<CompleteRootPush>),
    Outcome(PreparedCommand<CompleteRootOutcome>),
}
impl RootCommand {
    fn evidence(&self) -> &cellule_runtime::PendingMutation {
        match self {
            Self::Publish(command) => command.evidence(),
            Self::Outcome(command) => command.evidence(),
        }
    }
}
impl ReadyRootPush {
    pub(super) fn dispatch_copy(&self) -> Self {
        Self {
            owner: self.owner.clone(),
            command: self.command.clone(),
            refusal: self.refusal,
        }
    }
    pub(super) fn pending(&self) -> PublicationError {
        PublicationError::RootPush(InvocationError::Pending(Box::new(
            self.command.evidence().clone(),
        )))
    }
    #[cfg(test)]
    pub(in crate::packs::publication) fn evidence_for_test(
        &self,
    ) -> cellule_runtime::PendingMutation {
        self.command.evidence().clone()
    }
    pub(super) async fn dispatch(self, recover: bool, fault: u8) -> DispatchResult {
        let client = self.owner.capability().0.clone();
        let guard = move || {
            self.owner
                .session()
                .live_lease()
                .map(|_| ())
                .map_err(|_| Error::Command("inactive immutable root preparation"))
        };
        let outcome = match self.command {
            RootCommand::Publish(command) => {
                super::super::exact::invoke_guarded(&client, command, recover, 512, fault, guard)
                    .await
            }
            RootCommand::Outcome(command) => {
                super::super::exact::invoke_guarded(&client, command, recover, 512, fault, guard)
                    .await
            }
        };
        outcome
            .map(PublicationOutcome::RootPush)
            .map_err(PublicationError::RootPush)
    }
}
