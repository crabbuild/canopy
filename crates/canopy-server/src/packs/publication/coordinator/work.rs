use super::*;

const MAINTENANCE_INLINE_BYTES: u32 = 4096;
pub(super) const MAINTENANCE_RESERVATION: u64 = 2 * MAINTENANCE_INLINE_BYTES as u64;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PublicationClass {
    Foreground,
    Maintenance,
}
impl PublicationClass {
    pub(super) fn index(self) -> usize {
        match self {
            Self::Foreground => 0,
            Self::Maintenance => 1,
        }
    }
    pub(super) fn reservation(self) -> u64 {
        match self {
            Self::Foreground => COMMAND_RESERVATION,
            Self::Maintenance => MAINTENANCE_RESERVATION,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum CompactionReadyError {
    #[error("compaction attestation failed")]
    Attestation(#[from] CatalogAttestationError),
    #[error("compaction preparation inactive")]
    Base(#[from] PreparationBaseError),
    #[error("compaction command encoding failed")]
    Codec(#[from] CodecError),
    #[error("compaction command preparation failed")]
    Command(#[source] Box<InvocationError<CompactionReply>>),
}

#[must_use]
pub struct ReadyCatalogCompaction {
    prepared: Arc<PreparedCompaction>,
    command: PreparedCommand<PublishCatalogCompaction>,
}
impl PreparedCompaction {
    /// Retain the private verified input and exact SDK command through dispatch
    /// and uncertain-outcome recovery. No alternate proof/identity on retry.
    pub async fn ready_compaction(
        self: &Arc<Self>,
        identity: MutationIdentity,
    ) -> Result<ReadyCatalogCompaction, CompactionReadyError> {
        let input = self.certificate().await?;
        input.encode(&mut BoundedEncoder::new(MAINTENANCE_INLINE_BYTES)?)?;
        let base = self.preparation_base();
        base.live_lease()?;
        let (client, target, _) = base.capability();
        let command = client
            .prepare_command::<PublishCatalogCompaction>(target, identity, input)
            .await
            .map_err(|error| CompactionReadyError::Command(Box::new(error)))?;
        base.live_lease()?;
        Ok(ReadyCatalogCompaction {
            prepared: Arc::clone(self),
            command,
        })
    }
}

/// Every variant is privately prepared. Wrapping one never constructs a proof.
#[must_use]
pub enum ReadyPublication {
    Push(ReadyCatalogPush),
    RootPush(ReadyRootPush),
    Compaction(ReadyCatalogCompaction),
    Inputs(ReadyNativeInputs),
    Preparation(ReadyPreparation),
}
impl From<ReadyRootPush> for ReadyPublication {
    fn from(ready: ReadyRootPush) -> Self {
        Self::RootPush(ready)
    }
}
impl From<ReadyCatalogPush> for ReadyPublication {
    fn from(ready: ReadyCatalogPush) -> Self {
        Self::Push(ready)
    }
}
impl From<ReadyCatalogCompaction> for ReadyPublication {
    fn from(ready: ReadyCatalogCompaction) -> Self {
        Self::Compaction(ready)
    }
}
impl From<ReadyNativeInputs> for ReadyPublication {
    fn from(ready: ReadyNativeInputs) -> Self {
        Self::Inputs(ready)
    }
}
impl From<ReadyPreparation> for ReadyPublication {
    fn from(ready: ReadyPreparation) -> Self {
        Self::Preparation(ready)
    }
}
impl ReadyPublication {
    /// Final work must share the lifecycle's exact session fence and clock.
    /// Equal SQL tokens from an independently opened session are insufficient.
    pub(in crate::packs::publication) fn belongs_to(&self, session: &PreparationSession) -> bool {
        let source = match self {
            Self::Push(ready) => ready.owner.session(),
            Self::RootPush(ready) => &ready.prepared.base.session,
            Self::Compaction(ready) => &ready.prepared.preparation_base().session,
            Self::Inputs(_) | Self::Preparation(_) => return false,
        };
        source.target == session.target
            && source.check == session.check
            && source.ceiling == session.ceiling
            && Arc::ptr_eq(&source.deadline, &session.deadline)
            && Arc::ptr_eq(&source.fenced, &session.fenced)
    }
    pub(super) fn reservation(&self) -> u64 {
        match self {
            Self::Inputs(_) => inputs::INPUT_RESERVATION,
            Self::Preparation(_) => preparation::RESERVATION,
            Self::RootPush(_) => roots::ROOT_RESERVATION,
            _ => self.class().reservation(),
        }
    }
    pub(super) fn dispatch_copy(&self) -> Self {
        match self {
            Self::Preparation(ready) => Self::Preparation(ready.dispatch_copy()),
            Self::RootPush(ready) => Self::RootPush(ReadyRootPush {
                prepared: ready.prepared.clone(),
                command: ready.command.clone(),
            }),
            Self::Push(ready) => Self::Push(ReadyCatalogPush {
                owner: ready.owner.clone(),
                command: ready.command.clone(),
            }),
            Self::Compaction(ready) => Self::Compaction(ReadyCatalogCompaction {
                prepared: Arc::clone(&ready.prepared),
                command: ready.command.clone(),
            }),
            Self::Inputs(ready) => Self::Inputs(ReadyNativeInputs {
                session: ready.session.clone(),
                command: ready.command.clone(),
                digest: ready.digest,
            }),
        }
    }
    pub(super) fn class(&self) -> PublicationClass {
        match self {
            Self::Push(_) | Self::RootPush(_) | Self::Inputs(_) | Self::Preparation(_) => {
                PublicationClass::Foreground
            }
            Self::Compaction(_) => PublicationClass::Maintenance,
        }
    }
    pub(super) fn capability(&self) -> (&CellClient, &CellTarget, &LeaseCheck) {
        match self {
            Self::Push(ready) => ready.owner.capability(),
            Self::RootPush(ready) => ready.prepared.base.capability(),
            Self::Inputs(ready) => ready.session.capability(),
            Self::Preparation(ready) => ready.capability(),
            Self::Compaction(ready) => ready.prepared.preparation_base().capability(),
        }
    }
    pub(super) fn pending(&self) -> PublicationError {
        match self {
            Self::Preparation(ready) => ready.pending(),
            Self::RootPush(ready) => PublicationError::RootPush(InvocationError::Pending(
                Box::new(ready.command.evidence().clone()),
            )),
            Self::Push(ready) => PublicationError::Push(InvocationError::Pending(Box::new(
                ready.command.evidence().clone(),
            ))),
            Self::Compaction(ready) => PublicationError::Compaction(InvocationError::Pending(
                Box::new(ready.command.evidence().clone()),
            )),
            Self::Inputs(ready) => PublicationError::Inputs(InvocationError::Pending(Box::new(
                ready.command.evidence().clone(),
            ))),
        }
    }
    pub(super) async fn dispatch(self, recover: bool, fault: u8) -> DispatchResult {
        let client = self.capability().0.clone();
        match self {
            Self::Inputs(ready) => ready.dispatch(recover, fault).await,
            Self::Preparation(ready) => ready.dispatch(recover, fault).await,
            Self::RootPush(ready) => ready.dispatch(recover, fault).await,
            Self::Push(ready) => super::super::exact::invoke_guarded(
                &client,
                ready.command,
                recover,
                128,
                fault,
                move || {
                    ready
                        .owner
                        .session()
                        .live_lease()
                        .map(|_| ())
                        .map_err(|_| Error::Command("inactive final preparation"))
                },
            )
            .await
            .map(PublicationOutcome::Push)
            .map_err(PublicationError::Push),
            Self::Compaction(ready) => super::super::exact::invoke_guarded(
                &client,
                ready.command,
                recover,
                128,
                fault,
                move || {
                    ready
                        .prepared
                        .preparation_base()
                        .live_lease()
                        .map(|_| ())
                        .map_err(|_| Error::Command("inactive final preparation"))
                },
            )
            .await
            .map(PublicationOutcome::Compaction)
            .map_err(PublicationError::Compaction),
        }
    }
}

#[derive(Clone, Debug)]
pub enum PublicationOutcome {
    Push(Committed<CatalogCompletionReply>),
    RootPush(Committed<RootCompletionReply>),
    Compaction(Committed<CompactionReply>),
    Inputs(RegisteredNativeInputs),
    Preparation(PreparationCommandOutcome),
}
#[derive(Debug, thiserror::Error)]
pub enum PublicationError {
    #[error("immutable root push publication: {0}")]
    RootPush(#[source] InvocationError<RootCompletionReply>),
    #[error("bound preparation command: {0}")]
    Preparation(#[source] InvocationError<PreparationReply>),
    #[error("bound input checkpoint: {0}")]
    Inputs(#[source] InvocationError<StagingReply>),
    #[error("push publication: {0}")]
    Push(#[source] InvocationError<CatalogCompletionReply>),
    #[error("compaction publication: {0}")]
    Compaction(#[source] InvocationError<CompactionReply>),
}
impl PublicationError {
    pub(super) fn disposition(&self) -> &'static str {
        fn kind<T>(error: &InvocationError<T>) -> &'static str {
            match error {
                InvocationError::Rejected(_) => "rejected",
                InvocationError::NotStarted(_) => "not_started",
                InvocationError::Pending(_) => "pending",
                InvocationError::InvalidPublishedResult { .. } => "invalid_published_result",
            }
        }
        match self {
            Self::Push(error) => kind(error),
            Self::RootPush(error) => kind(error),
            Self::Preparation(error) => kind(error),
            Self::Inputs(error) => kind(error),
            Self::Compaction(error) => kind(error),
        }
    }
    pub(super) fn uncertain(&self) -> bool {
        fn unknown<T>(error: &InvocationError<T>) -> bool {
            matches!(
                error,
                InvocationError::Pending(_) | InvocationError::InvalidPublishedResult { .. }
            )
        }
        match self {
            Self::Push(error) => unknown(error),
            Self::RootPush(error) => unknown(error),
            Self::Preparation(error) => unknown(error),
            Self::Inputs(error) => unknown(error),
            Self::Compaction(error) => unknown(error),
        }
    }
}
