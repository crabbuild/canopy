//! Bound attempt commands share exact publication ownership and admission.
use super::*;

const INLINE_BYTES: u32 = 4096;
pub(super) const RESERVATION: u64 = 2 * INLINE_BYTES as u64;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PreparationCommandKind {
    Claim,
    Renew,
}
#[derive(Debug, thiserror::Error)]
pub enum PreparationReadyError {
    #[error("preparation inactive or context differs")]
    Base(#[from] PreparationBaseError),
    #[error("preparation command encoding failed")]
    Codec(#[from] CodecError),
    #[error("preparation command preparation failed")]
    Command(#[source] Box<InvocationError<PreparationReply>>),
}
#[derive(Clone)]
enum ExactPreparation {
    Claim(PreparedCommand<ClaimPreparation>),
    Renew {
        command: PreparedCommand<RenewPreparation>,
        session: Arc<PreparationSession>,
    },
}
#[must_use]
pub struct ReadyPreparation {
    inner: Box<PreparationRequest>,
}
#[derive(Clone)]
struct PreparationRequest {
    client: CellClient,
    target: CellTarget,
    check: LeaseCheck,
    exact: ExactPreparation,
}
/// Recorded outcomes remain recoverable even when fresh custody is unavailable.
/// Only a freshly queried session can authorize subsequent private factories.
#[derive(Clone)]
pub struct PreparationCommandOutcome {
    pub kind: PreparationCommandKind,
    pub committed: Committed<PreparationReply>,
    pub session: Result<Arc<PreparationSession>, Arc<PreparationBaseError>>,
}
impl std::fmt::Debug for PreparationCommandOutcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PreparationCommandOutcome")
            .field("kind", &self.kind)
            .field("committed", &self.committed)
            .field("session", &self.session.as_ref().map(|s| s.lease.token))
            .finish()
    }
}
fn validate(target: &CellTarget, request: &LeaseRequest) -> Result<(), PreparationReadyError> {
    if crate::repository_target(
        target.tenant(),
        target.application(),
        request.check.token.repository,
    )
    .map_err(|_| PreparationBaseError::Context)?
        != *target
        || request.lease_ms == 0
        || request.lease_ms > MAX_LEASE_MS
    {
        return Err(PreparationBaseError::Context.into());
    }
    request.encode(&mut BoundedEncoder::new(INLINE_BYTES)?)?;
    Ok(())
}
impl ReadyPreparation {
    /// Claim may recover an expired or previous-owner attempt. Do not require a
    /// local live session; authoritative execution checks the exact old token.
    pub async fn claim(
        client: CellClient,
        target: CellTarget,
        request: LeaseRequest,
        identity: MutationIdentity,
    ) -> Result<Self, PreparationReadyError> {
        validate(&target, &request)?;
        let check = request.check.clone();
        let command = client
            .prepare_command::<ClaimPreparation>(&target, identity, request)
            .await
            .map_err(|e| PreparationReadyError::Command(Box::new(e)))?;
        Ok(Self {
            inner: Box::new(PreparationRequest {
                client,
                target,
                check,
                exact: ExactPreparation::Claim(command),
            }),
        })
    }
    pub(super) fn dispatch_copy(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
    pub(super) fn capability(&self) -> (&CellClient, &CellTarget, &LeaseCheck) {
        (&self.inner.client, &self.inner.target, &self.inner.check)
    }
    pub(super) fn pending(&self) -> PublicationError {
        let evidence = match &self.inner.exact {
            ExactPreparation::Claim(command) => command.evidence(),
            ExactPreparation::Renew { command, .. } => command.evidence(),
        };
        PublicationError::Preparation(InvocationError::Pending(Box::new(evidence.clone())))
    }
    pub(super) async fn dispatch(self, recover: bool, fault: u8) -> DispatchResult {
        let inner = *self.inner;
        let (kind, result, existing) = match inner.exact {
            ExactPreparation::Claim(command) => (
                PreparationCommandKind::Claim,
                super::super::exact::invoke(&inner.client, command, recover, INLINE_BYTES, fault)
                    .await,
                None,
            ),
            ExactPreparation::Renew { command, session } => (
                PreparationCommandKind::Renew,
                super::super::exact::invoke(&inner.client, command, recover, INLINE_BYTES, fault)
                    .await,
                Some(session),
            ),
        };
        let committed = match result {
            Ok(committed) => committed,
            Err(error) => {
                if !matches!(
                    &error,
                    InvocationError::Pending(_) | InvocationError::InvalidPublishedResult { .. }
                ) && let Some(session) = existing
                {
                    session.fence();
                }
                return Err(PublicationError::Preparation(error));
            }
        };
        let custody = async {
            let PreparationReply::Granted(lease) = &committed.output else {
                return Err(PreparationBaseError::Context);
            };
            if let Some(session) = &existing {
                if lease.token != session.lease.token
                    || lease.base != session.lease.base
                    || lease.format != session.lease.format
                {
                    return Err(PreparationBaseError::Context);
                }
                session.refresh(committed.receipt).await?;
                Ok(session.clone())
            } else {
                if lease.token.repository != inner.check.token.repository
                    || lease.token.operation != inner.check.token.operation
                    || lease.token.request_digest != inner.check.token.request_digest
                    || lease.token == inner.check.token
                {
                    return Err(PreparationBaseError::Context);
                }
                let session = PreparationSession::open(
                    inner.client.clone(),
                    inner.target.clone(),
                    LeaseCheck {
                        token: lease.token,
                        actor: inner.check.actor.clone(),
                    },
                    Some(committed.receipt),
                )
                .await?;
                if session.lease.base != lease.base || session.lease.format != lease.format {
                    return Err(PreparationBaseError::Context);
                }
                Ok(Arc::new(session))
            }
        }
        .await;
        if custody.is_err()
            && let Some(session) = existing
        {
            session.fence();
        }
        Ok(PublicationOutcome::Preparation(PreparationCommandOutcome {
            kind,
            committed,
            session: custody.map_err(Arc::new),
        }))
    }
}
impl PreparationSession {
    /// Prepare an exact renewal for service dispatch; a refused admission keeps
    /// the same identity. An ambiguous renewal keeps the previously observed
    /// deadline until resolved, and never grants custody from a recorded clock.
    pub async fn ready_renew(
        self: &Arc<Self>,
        identity: MutationIdentity,
        lease_ms: u64,
    ) -> Result<ReadyPreparation, PreparationReadyError> {
        self.live_lease()?;
        let request = LeaseRequest {
            check: self.check.clone(),
            lease_ms,
        };
        validate(&self.target, &request)?;
        let command = self
            .client
            .prepare_command::<RenewPreparation>(&self.target, identity, request)
            .await
            .map_err(|e| PreparationReadyError::Command(Box::new(e)))?;
        self.live_lease()?;
        Ok(ReadyPreparation {
            inner: Box::new(PreparationRequest {
                client: self.client.clone(),
                target: self.target.clone(),
                check: self.check.clone(),
                exact: ExactPreparation::Renew {
                    command,
                    session: self.clone(),
                },
            }),
        })
    }
}
