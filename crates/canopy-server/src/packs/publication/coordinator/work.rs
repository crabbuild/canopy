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
/// Immutable root completions and policy pages require registered recovery.
#[must_use]
pub enum ReadyPublication {
    ServingRelease(ReadyServingRelease),
    ServingCommand(Box<ReadyServingCommand>),
    Push(ReadyCatalogPush),
    RootRecovery(ReadyRootRecovery),
    TerminalRelease(Box<ReadyTerminalRelease>),
    CustodyStop(Box<ReadyCustodyStop>),
    BoundRecovery(ReadyBoundRecovery),
    Compaction(ReadyCatalogCompaction),
    Inputs(ReadyNativeInputs),
    Preparation(ReadyPreparation),
}
impl From<ReadyServingCommand> for ReadyPublication {
    fn from(ready: ReadyServingCommand) -> Self {
        Self::ServingCommand(Box::new(ready))
    }
}
impl From<ReadyServingRelease> for ReadyPublication {
    fn from(ready: ReadyServingRelease) -> Self {
        Self::ServingRelease(ready)
    }
}
impl From<ReadyCustodyStop> for ReadyPublication {
    fn from(ready: ReadyCustodyStop) -> Self {
        Self::CustodyStop(Box::new(ready))
    }
}
impl From<ReadyTerminalRelease> for ReadyPublication {
    fn from(ready: ReadyTerminalRelease) -> Self {
        Self::TerminalRelease(Box::new(ready))
    }
}
impl From<ReadyBoundRecovery> for ReadyPublication {
    fn from(ready: ReadyBoundRecovery) -> Self {
        Self::BoundRecovery(ready)
    }
}
impl From<ReadyRootRecovery> for ReadyPublication {
    fn from(ready: ReadyRootRecovery) -> Self {
        Self::RootRecovery(ready)
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
    pub(super) fn serving_release_token(&self) -> Option<ServingToken> {
        match self {
            Self::ServingRelease(ready) => Some(ready.token()),
            _ => None,
        }
    }

    pub(super) fn job_kind(&self) -> JobKind {
        match self {
            Self::CustodyStop(ready) if ready.purpose() == CustodyPurpose::Serving => {
                JobKind::ServingStop
            }
            Self::CustodyStop(_) => JobKind::CustodyStop,
            Self::ServingCommand(_) => JobKind::ServingCommand,
            Self::ServingRelease(_) => JobKind::ServingRelease,
            _ => JobKind::Publication,
        }
    }
    pub(super) fn custody_original(&self) -> Option<&cellule_runtime::PendingMutation> {
        match self {
            Self::Preparation(ready) => Some(ready.evidence()),
            Self::ServingCommand(ready) => Some(ready.evidence()),
            _ => None,
        }
    }
    pub(in crate::packs::publication) fn is_policy_page(&self) -> bool {
        matches!(self, Self::BoundRecovery(ready) if ready.ready.is_policy_page())
    }
    pub(in crate::packs::publication) fn is_root_refusal(&self) -> bool {
        matches!(self, Self::BoundRecovery(ready) if ready.refusal)
    }
    /// Final work must share the lifecycle's exact session fence and clock.
    /// Equal SQL tokens from an independently opened session are insufficient.
    pub(in crate::packs::publication) fn belongs_to(&self, session: &PreparationSession) -> bool {
        let source = match self {
            Self::Push(ready) => ready.owner.session(),
            Self::BoundRecovery(ready) => ready.owner.session(),
            Self::Compaction(ready) => &ready.prepared.preparation_base().session,
            Self::Inputs(_)
            | Self::Preparation(_)
            | Self::RootRecovery(_)
            | Self::TerminalRelease(_)
            | Self::CustodyStop(_)
            | Self::ServingRelease(_)
            | Self::ServingCommand(_) => return false,
        };
        source.target == session.target
            && source.check == session.check
            && source.ceiling == session.ceiling
            && Arc::ptr_eq(&source.deadline, &session.deadline)
            && Arc::ptr_eq(&source.fenced, &session.fenced)
    }
    pub(super) fn reservation(&self) -> u64 {
        match self {
            Self::ServingCommand(ready) => ready.reservation(),
            Self::Inputs(_) => inputs::INPUT_RESERVATION,
            Self::Preparation(_) => preparation::RESERVATION,
            Self::RootRecovery(ready) => ready.reservation(),
            Self::BoundRecovery(ready) => ready.ready.reservation(),
            _ => self.class().reservation(),
        }
    }
    pub(super) fn dispatch_copy(&self) -> Self {
        match self {
            Self::ServingRelease(ready) => Self::ServingRelease(ready.dispatch_copy()),
            Self::ServingCommand(ready) => Self::ServingCommand(Box::new(ready.dispatch_copy())),
            Self::Preparation(ready) => Self::Preparation(ready.dispatch_copy()),
            Self::RootRecovery(ready) => Self::RootRecovery(ready.clone()),
            Self::TerminalRelease(ready) => Self::TerminalRelease(ready.clone()),
            Self::CustodyStop(ready) => Self::CustodyStop(ready.clone()),
            Self::BoundRecovery(ready) => Self::BoundRecovery(ready.clone()),
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
            Self::Push(_)
            | Self::ServingCommand(_)
            | Self::RootRecovery(_)
            | Self::BoundRecovery(_)
            | Self::Inputs(_)
            | Self::Preparation(_) => PublicationClass::Foreground,
            Self::Compaction(_)
            | Self::TerminalRelease(_)
            | Self::CustodyStop(_)
            | Self::ServingRelease(_) => PublicationClass::Maintenance,
        }
    }
    pub(super) fn context(&self) -> (&CellClient, &CellTarget, BeginRequest) {
        let (client, target, check) = match self {
            Self::ServingRelease(ready) => return ready.context(),
            Self::ServingCommand(ready) => return ready.context(),
            Self::CustodyStop(ready) => return ready.context(),
            Self::Push(ready) => ready.owner.capability(),
            Self::RootRecovery(ready) => ready.capability(),
            Self::TerminalRelease(ready) => ready.capability(),
            Self::BoundRecovery(ready) => ready.owner.capability(),
            Self::Inputs(ready) => ready.session.capability(),
            Self::Preparation(ready) => ready.capability(),
            Self::Compaction(ready) => ready.prepared.preparation_base().capability(),
        };
        (
            client,
            target,
            BeginRequest {
                repository: check.token.repository,
                operation: check.token.operation,
                request_digest: check.token.request_digest,
                actor: check.actor.clone(),
                lease_ms: DEFAULT_LEASE_MS,
            },
        )
    }
    pub(super) fn pending(&self) -> PublicationError {
        match self {
            Self::ServingRelease(ready) => ready.pending(),
            Self::ServingCommand(ready) => ready.pending(),
            Self::Preparation(ready) => ready.pending(),
            Self::RootRecovery(ready) => ready.pending(),
            Self::TerminalRelease(ready) => ready.pending(),
            Self::CustodyStop(ready) => ready.pending(),
            Self::BoundRecovery(ready) => ready.ready.pending(),
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
        let client = self.context().0.clone();
        match self {
            Self::ServingCommand(ready) => ready
                .dispatch(recover, fault)
                .await
                .map(PublicationOutcome::ServingCommand),
            Self::ServingRelease(ready) => ready
                .dispatch(recover, fault)
                .await
                .map(PublicationOutcome::ServingRelease)
                .map_err(PublicationError::ServingRelease),
            Self::Inputs(ready) => ready.dispatch(recover, fault).await,
            Self::Preparation(ready) => ready.dispatch(recover, fault).await,
            Self::RootRecovery(ready) => ready.dispatch(fault).await,
            Self::TerminalRelease(ready) => ready.dispatch(recover, fault).await,
            Self::CustodyStop(ready) => ready.dispatch(recover, fault).await,
            Self::BoundRecovery(ready) => ready.dispatch(fault).await,
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
    Head(Committed<PublicationReply>),
    Merge(Committed<crate::pulls::merge::MergeOutcome>),
    ServingRelease(Committed<ServingReleaseReply>),
    ServingCommand(Committed<ServingReply>),
    Initialization(Committed<InitializationReply>),
    Push(Committed<CatalogCompletionReply>),
    RootPush(Committed<RootCompletionReply>),
    /// Original page result/receipt only; fresh guard checks remain mandatory.
    PolicyPage(Committed<RefPolicyReply>),
    Compaction(Committed<CompactionReply>),
    Inputs(RegisteredNativeInputs),
    TerminalRelease(Committed<TerminalReleaseReply>),
    CustodyStop(Box<CustodyStopOutcome>),
    Preparation(PreparationCommandOutcome),
}
#[derive(Debug, thiserror::Error)]
pub enum PublicationError {
    #[error("symbolic HEAD publication: {0}")]
    Head(#[source] InvocationError<PublicationReply>),
    #[error("native reviewed merge publication: {0}")]
    Merge(#[source] InvocationError<crate::pulls::merge::MergeOutcome>),
    #[error("serving pin release: {0}")]
    ServingRelease(#[source] InvocationError<ServingReleaseReply>),
    #[error("serving custody command: {0}")]
    ServingCommand(#[source] InvocationError<ServingReply>),
    #[error("publication custody intent failed")]
    Custody {
        evidence: Box<cellule_runtime::PendingMutation>,
        source: Box<CustodyError>,
    },
    #[error("repository initialization publication: {0}")]
    Initialization(#[source] InvocationError<InitializationReply>),
    #[error("custody retirement: {0}")]
    CustodyStop(#[source] InvocationError<CustodyStopReply>),
    #[error("terminal recovery release: {0}")]
    TerminalRelease(#[source] InvocationError<TerminalReleaseReply>),
    #[error("durable publication phase could not be observed: {source}")]
    Recovery {
        evidence: Box<cellule_runtime::PendingMutation>,
        #[source]
        source: Box<RootRecoveryError>,
    },
    #[error("ref policy page registration: {0}")]
    PolicyPage(#[source] InvocationError<RefPolicyReply>),
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
            Self::Custody { source, .. } if source.uncertain() => "pending",
            Self::Custody { .. } => "not_started",
            Self::Recovery { .. } => "pending",
            Self::ServingRelease(error) => kind(error),
            Self::ServingCommand(error) => kind(error),
            Self::Initialization(error) => kind(error),
            Self::Merge(error) => kind(error),
            Self::Head(error) => kind(error),
            Self::Push(error) => kind(error),
            Self::RootPush(error) => kind(error),
            Self::PolicyPage(error) => kind(error),
            Self::Preparation(error) => kind(error),
            Self::Inputs(error) => kind(error),
            Self::Compaction(error) => kind(error),
            Self::TerminalRelease(error) => kind(error),
            Self::CustodyStop(error) => kind(error),
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
            Self::Custody { source, .. } => source.uncertain(),
            Self::Recovery { .. } => true,
            Self::ServingRelease(error) => unknown(error),
            Self::ServingCommand(error) => unknown(error),
            Self::Initialization(error) => unknown(error),
            Self::Merge(error) => unknown(error),
            Self::Head(error) => unknown(error),
            Self::Push(error) => unknown(error),
            Self::RootPush(error) => unknown(error),
            Self::PolicyPage(error) => unknown(error),
            Self::Preparation(error) => unknown(error),
            Self::Inputs(error) => unknown(error),
            Self::Compaction(error) => unknown(error),
            Self::TerminalRelease(error) => unknown(error),
            Self::CustodyStop(error) => unknown(error),
        }
    }
}
