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
    Compaction(ReadyCatalogCompaction),
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
impl ReadyPublication {
    pub(super) fn dispatch_copy(&self) -> Self {
        match self {
            Self::Push(ready) => Self::Push(ReadyCatalogPush {
                owner: ready.owner.clone(),
                command: ready.command.clone(),
            }),
            Self::Compaction(ready) => Self::Compaction(ReadyCatalogCompaction {
                prepared: Arc::clone(&ready.prepared),
                command: ready.command.clone(),
            }),
        }
    }
    pub(super) fn class(&self) -> PublicationClass {
        match self {
            Self::Push(_) => PublicationClass::Foreground,
            Self::Compaction(_) => PublicationClass::Maintenance,
        }
    }
    pub(super) fn capability(&self) -> (&CellClient, &CellTarget, &LeaseCheck) {
        match self {
            Self::Push(ready) => ready.owner.capability(),
            Self::Compaction(ready) => ready.prepared.preparation_base().capability(),
        }
    }
    pub(super) fn pending(&self) -> PublicationError {
        match self {
            Self::Push(ready) => PublicationError::Push(InvocationError::Pending(Box::new(
                ready.command.evidence().clone(),
            ))),
            Self::Compaction(ready) => PublicationError::Compaction(InvocationError::Pending(
                Box::new(ready.command.evidence().clone()),
            )),
        }
    }
    pub(super) async fn dispatch(self, recover: bool, fault: u8) -> DispatchResult {
        let client = self.capability().0.clone();
        match self {
            Self::Push(ready) => {
                super::super::exact::invoke(&client, ready.command, recover, 128, fault)
                    .await
                    .map(PublicationOutcome::Push)
                    .map_err(PublicationError::Push)
            }
            Self::Compaction(ready) => {
                super::super::exact::invoke(&client, ready.command, recover, 128, fault)
                    .await
                    .map(PublicationOutcome::Compaction)
                    .map_err(PublicationError::Compaction)
            }
        }
    }
}

#[derive(Clone, Debug)]
pub enum PublicationOutcome {
    Push(Committed<CatalogCompletionReply>),
    Compaction(Committed<CompactionReply>),
}
#[derive(Debug, thiserror::Error)]
pub enum PublicationError {
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
            Self::Compaction(error) => unknown(error),
        }
    }
}
