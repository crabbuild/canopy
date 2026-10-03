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
    RootRecovery(ReadyRootRecovery),
    BoundRecovery(ReadyBoundRecovery),
    PolicyPage(ReadyRefPolicyPage),
    Compaction(ReadyCatalogCompaction),
    Inputs(ReadyNativeInputs),
    Preparation(ReadyPreparation),
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
impl From<ReadyRefPolicyPage> for ReadyPublication {
    fn from(ready: ReadyRefPolicyPage) -> Self {
        Self::PolicyPage(ready)
    }
}
impl From<Arc<ReadyRootPush>> for ReadyPublication {
    fn from(ready: Arc<ReadyRootPush>) -> Self {
        Self::RootPush(ready.dispatch_copy())
    }
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
    pub(in crate::packs::publication) fn is_policy_page(&self) -> bool {
        matches!(self, Self::PolicyPage(_))
            || matches!(self, Self::BoundRecovery(ready) if ready.ready.is_policy_page())
    }
    pub(in crate::packs::publication) fn is_root_refusal(&self) -> bool {
        matches!(self, Self::RootPush(ready) if ready.refusal)
            || matches!(self, Self::BoundRecovery(ready) if ready.refusal)
    }
    /// Final work must share the lifecycle's exact session fence and clock.
    /// Equal SQL tokens from an independently opened session are insufficient.
    pub(in crate::packs::publication) fn belongs_to(&self, session: &PreparationSession) -> bool {
        let source = match self {
            Self::Push(ready) => ready.owner.session(),
            Self::RootPush(ready) => ready.owner.session(),
            Self::BoundRecovery(ready) => ready.owner.session(),
            Self::PolicyPage(ready) => &ready.prepared.base.session,
            Self::Compaction(ready) => &ready.prepared.preparation_base().session,
            Self::Inputs(_) | Self::Preparation(_) | Self::RootRecovery(_) => return false,
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
            Self::RootRecovery(ready) => ready.reservation(),
            Self::BoundRecovery(ready) => ready.ready.reservation(),
            Self::PolicyPage(ready) => ready.reservation(),
            _ => self.class().reservation(),
        }
    }
    pub(super) fn dispatch_copy(&self) -> Self {
        match self {
            Self::Preparation(ready) => Self::Preparation(ready.dispatch_copy()),
            Self::RootPush(ready) => Self::RootPush(ready.dispatch_copy()),
            Self::RootRecovery(ready) => Self::RootRecovery(ready.clone()),
            Self::BoundRecovery(ready) => Self::BoundRecovery(ready.clone()),
            Self::PolicyPage(ready) => Self::PolicyPage(ready.dispatch_copy()),
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
            | Self::RootPush(_)
            | Self::RootRecovery(_)
            | Self::BoundRecovery(_)
            | Self::PolicyPage(_)
            | Self::Inputs(_)
            | Self::Preparation(_) => PublicationClass::Foreground,
            Self::Compaction(_) => PublicationClass::Maintenance,
        }
    }
    pub(super) fn capability(&self) -> (&CellClient, &CellTarget, &LeaseCheck) {
        match self {
            Self::Push(ready) => ready.owner.capability(),
            Self::RootPush(ready) => ready.owner.capability(),
            Self::RootRecovery(ready) => ready.capability(),
            Self::BoundRecovery(ready) => ready.owner.capability(),
            Self::PolicyPage(ready) => ready.prepared.base.capability(),
            Self::Inputs(ready) => ready.session.capability(),
            Self::Preparation(ready) => ready.capability(),
            Self::Compaction(ready) => ready.prepared.preparation_base().capability(),
        }
    }
    pub(super) fn pending(&self) -> PublicationError {
        match self {
            Self::Preparation(ready) => ready.pending(),
            Self::RootPush(ready) => ready.pending(),
            Self::RootRecovery(ready) => ready.pending(),
            Self::BoundRecovery(ready) => ready.ready.pending(),
            Self::PolicyPage(ready) => ready.pending(),
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
            Self::RootRecovery(ready) => ready.dispatch(fault).await,
            Self::BoundRecovery(ready) => ready.dispatch(fault).await,
            Self::PolicyPage(ready) => ready.dispatch(recover, fault).await,
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
    /// Original page result/receipt only; fresh guard checks remain mandatory.
    PolicyPage(Committed<RefPolicyReply>),
    Compaction(Committed<CompactionReply>),
    Inputs(RegisteredNativeInputs),
    Preparation(PreparationCommandOutcome),
}
#[derive(Debug, thiserror::Error)]
pub enum PublicationError {
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
            Self::Recovery { .. } => "pending",
            Self::Push(error) => kind(error),
            Self::RootPush(error) => kind(error),
            Self::PolicyPage(error) => kind(error),
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
            Self::Recovery { .. } => true,
            Self::Push(error) => unknown(error),
            Self::RootPush(error) => unknown(error),
            Self::PolicyPage(error) => unknown(error),
            Self::Preparation(error) => unknown(error),
            Self::Inputs(error) => unknown(error),
            Self::Compaction(error) => unknown(error),
        }
    }
}
