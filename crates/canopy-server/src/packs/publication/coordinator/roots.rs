//! Exact immutable root completion uses the shared fair dispatch and lifecycle.
use super::*;
use crate::directory::DirectoryCell;

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

/// Persist and bind this private factory output before publication admission.
/// Retain the exact command if registration fails; regenerating a completion
/// allocates a different response ID and SDK identity.
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
    /// Preserve the original factory's shared lifecycle authority while
    /// dispatching from its exact registered SDK snapshot and body.
    pub fn bind_recovery(
        self,
        registered: RegisteredRootRecovery,
        store: &canopy_object_storage::artifact::ArtifactStore,
    ) -> Result<ReadyBoundRecovery, Box<RecoveryBindingFailure<Self>>> {
        let kind = match &self.command {
            RootCommand::Publish(_) => super::super::recovery::Kind::Publish,
            RootCommand::Outcome(_) => super::super::recovery::Kind::Outcome,
        };
        if !registered.matches_original(
            kind,
            self.command.evidence(),
            None,
            self.owner.session(),
            store,
        ) {
            return Err(Box::new(RecoveryBindingFailure {
                original: self,
                registered,
            }));
        }
        Ok(ReadyBoundRecovery::new(
            self.owner,
            None,
            self.refusal,
            registered,
            store,
        ))
    }
    pub(in crate::packs::publication) fn refusal_command(
        &self,
    ) -> Option<&PreparedCommand<CompleteRootOutcome>> {
        if let RootCommand::Outcome(command) = &self.command
            && self.refusal
        {
            Some(command)
        } else {
            None
        }
    }
    pub async fn persist_recovery_after(
        &self,
        store: &canopy_object_storage::artifact::ArtifactStore,
        identity: MutationIdentity,
        previous: &RegisteredRootRecovery,
    ) -> Result<RegisteredRootRecovery, RootRecoveryError> {
        self.persist_recovery_after_inner(store, identity, previous, 0)
            .await
    }
    #[cfg(test)]
    pub(in crate::packs::publication) async fn persist_recovery_after_for_test(
        &self,
        store: &canopy_object_storage::artifact::ArtifactStore,
        identity: MutationIdentity,
        previous: &RegisteredRootRecovery,
        fault: u8,
    ) -> Result<RegisteredRootRecovery, RootRecoveryError> {
        self.persist_recovery_after_inner(store, identity, previous, fault)
            .await
    }
    async fn persist_recovery_after_inner(
        &self,
        store: &canopy_object_storage::artifact::ArtifactStore,
        identity: MutationIdentity,
        previous: &RegisteredRootRecovery,
        fault: u8,
    ) -> Result<RegisteredRootRecovery, RootRecoveryError> {
        let session = self.owner.session();
        match &self.command {
            RootCommand::Publish(command) => {
                Box::pin(super::super::recovery::persist_full(
                    session,
                    command,
                    super::super::recovery::Kind::Publish,
                    None,
                    Some(previous),
                    store,
                    identity,
                    fault,
                ))
                .await
            }
            RootCommand::Outcome(command) => {
                Box::pin(super::super::recovery::persist_full(
                    session,
                    command,
                    super::super::recovery::Kind::Outcome,
                    None,
                    Some(previous),
                    store,
                    identity,
                    fault,
                ))
                .await
            }
        }
    }
    /// Persist exact bytes and authenticate their first-writer attempt pin
    /// before final submission. On uncertain registration, recover the winning
    /// record with RegisteredRootRecovery::load; do not regenerate the final
    /// command. No catalog or response identity is rebuilt during recovery.
    pub async fn persist_recovery(
        &self,
        store: &canopy_object_storage::artifact::ArtifactStore,
        identity: MutationIdentity,
    ) -> Result<RegisteredRootRecovery, RootRecoveryError> {
        self.persist_recovery_inner(store, identity, 0).await
    }
    #[cfg(test)]
    pub(in crate::packs::publication) async fn persist_recovery_for_test(
        &self,
        store: &canopy_object_storage::artifact::ArtifactStore,
        identity: MutationIdentity,
        fault: u8,
    ) -> Result<RegisteredRootRecovery, RootRecoveryError> {
        self.persist_recovery_inner(store, identity, fault).await
    }
    async fn persist_recovery_inner(
        &self,
        store: &canopy_object_storage::artifact::ArtifactStore,
        identity: MutationIdentity,
        fault: u8,
    ) -> Result<RegisteredRootRecovery, RootRecoveryError> {
        let session = self.owner.session();
        match &self.command {
            RootCommand::Publish(command) => {
                super::super::recovery::persist(
                    session,
                    command,
                    super::super::recovery::Kind::Publish,
                    store,
                    identity,
                    fault,
                )
                .await
            }
            RootCommand::Outcome(command) => {
                super::super::recovery::persist(
                    session,
                    command,
                    super::super::recovery::Kind::Outcome,
                    store,
                    identity,
                    fault,
                )
                .await
            }
        }
    }

    #[cfg(test)]
    pub(in crate::packs::publication) fn evidence_for_test(
        &self,
    ) -> cellule_runtime::PendingMutation {
        self.command.evidence().clone()
    }
    #[cfg(test)]
    pub(in crate::packs::publication) fn outcome_command_for_test(
        &self,
    ) -> Option<&PreparedCommand<CompleteRootOutcome>> {
        match &self.command {
            RootCommand::Outcome(command) => Some(command),
            RootCommand::Publish(_) => None,
        }
    }
}
