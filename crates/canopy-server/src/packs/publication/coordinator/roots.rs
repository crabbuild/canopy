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
    pub(super) prepared: Arc<PreparedCatalog>,
    pub(super) command: PreparedCommand<CompleteRootPush>,
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
            prepared: self.clone(),
            command,
        })
    }
}
impl ReadyRootPush {
    #[cfg(test)]
    pub(in crate::packs::publication) fn evidence_for_test(
        &self,
    ) -> cellule_runtime::PendingMutation {
        self.command.evidence().clone()
    }
    pub(super) async fn dispatch(self, recover: bool, fault: u8) -> DispatchResult {
        let client = self.prepared.base.capability().0.clone();
        super::super::exact::invoke_guarded(&client, self.command, recover, 512, fault, move || {
            self.prepared
                .ensure_live()
                .map_err(|_| Error::Command("inactive immutable root preparation"))
        })
        .await
        .map(PublicationOutcome::RootPush)
        .map_err(PublicationError::RootPush)
    }
}
