//! Bound attempt commands share exact publication ownership and admission.
use super::super::custody::OwnedCustody;
use super::*;

const INLINE_BYTES: u32 = 4096;
pub(super) const RESERVATION: u64 = super::super::custody::RESERVATION;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PreparationCommandKind {
    Claim,
    Renew,
}
#[derive(Debug, thiserror::Error)]
pub enum PreparationReadyError {
    #[error("preparation custody intent failed")]
    Custody(#[from] CustodyError),
    #[error("preparation inactive or context differs")]
    Base(#[from] PreparationBaseError),
    #[error("preparation command encoding failed")]
    Codec(#[from] CodecError),
}
#[derive(Clone)]
enum ExactPreparation {
    Claim(OwnedCustody),
    Renew {
        command: OwnedCustody,
        session: Option<Arc<PreparationSession>>,
    },
}
#[must_use]
pub struct ReadyPreparation {
    inner: Box<PreparationRequest>,
}
#[derive(Clone)]
struct PreparationRequest {
    authority: PreparationAuthority,
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
    /// Reconstruct the registered original after process loss. Known results
    /// remain knowledge; dispatch separately queries current lease authority.
    pub async fn restore(
        client: CellClient,
        target: CellTarget,
        operation: [u8; 16],
        authority: PreparationAuthority,
    ) -> Result<Self, PreparationReadyError> {
        let command = OwnedCustody::restore(&client, &target, operation).await?;
        let (request, renew) = match command.action()? {
            CustodyAction::ClaimPreparation(request) => (request, false),
            CustodyAction::RenewPreparation(request) => (request, true),
            _ => return Err(CustodyError::Context.into()),
        };
        validate(&target, &request)?;
        if !authority.matches(&target) {
            return Err(PreparationBaseError::Context.into());
        }
        Ok(Self {
            inner: Box::new(PreparationRequest {
                authority,
                client,
                target,
                check: request.check,
                exact: if renew {
                    ExactPreparation::Renew {
                        command,
                        session: None,
                    }
                } else {
                    ExactPreparation::Claim(command)
                },
            }),
        })
    }
    /// Claim may recover an expired or previous-owner attempt. Do not require a
    /// local live session; authoritative execution checks the exact old token.
    pub async fn claim(
        client: CellClient,
        target: CellTarget,
        request: LeaseRequest,
        identity: MutationIdentity,
        authority: PreparationAuthority,
    ) -> Result<Self, PreparationReadyError> {
        validate(&target, &request)?;
        if !authority.matches(&target) {
            return Err(PreparationBaseError::Context.into());
        }
        let check = request.check.clone();
        let command = OwnedCustody::prepare(
            &client,
            &target,
            CustodyAction::ClaimPreparation(request),
            identity,
        )
        .await?;
        Ok(Self {
            inner: Box::new(PreparationRequest {
                authority,
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
    pub(super) fn evidence(&self) -> &cellule_runtime::PendingMutation {
        match &self.inner.exact {
            ExactPreparation::Claim(command) => command.evidence(),
            ExactPreparation::Renew { command, .. } => command.evidence(),
        }
    }
    pub(super) fn pending(&self) -> PublicationError {
        PublicationError::Preparation(InvocationError::Pending(Box::new(self.evidence().clone())))
    }
    pub(super) async fn dispatch(self, recover: bool, fault: u8) -> DispatchResult {
        let inner = *self.inner;
        let (kind, command, existing) = match inner.exact {
            ExactPreparation::Claim(command) => (PreparationCommandKind::Claim, command, None),
            ExactPreparation::Renew { command, session } => {
                (PreparationCommandKind::Renew, command, session)
            }
        };
        let guard = existing.clone();
        let result = command
            .invoke(&inner.client, recover, fault, move || {
                if let Some(session) = guard {
                    session
                        .live_lease()
                        .map_err(|_| Error::Command("preparation renewal custody inactive"))?;
                }
                Ok(())
            })
            .await
            .map_err(|source| {
                if matches!(&source, CustodyError::Stopped(_))
                    && let Some(session) = &existing
                {
                    session.fence();
                }
                PublicationError::Custody {
                    evidence: Box::new(command.evidence().clone()),
                    source: Box::new(source),
                }
            })?;
        let result = super::super::custody::project(result, |reply| match reply {
            CustodyReply::Preparation(reply) => Some(reply),
            _ => None,
        });
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
                    || match kind {
                        PreparationCommandKind::Claim => lease.token == inner.check.token,
                        PreparationCommandKind::Renew => lease.token != inner.check.token,
                    }
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
                    inner.authority.clone(),
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
    /// Restore the registered renewal while retaining this session's permanent
    /// fence and conservative clock. Restoration is also valid after fencing:
    /// known outcomes remain recoverable, but absence cannot restart custody.
    pub async fn restore_renewal(
        self: &Arc<Self>,
    ) -> Result<ReadyPreparation, PreparationReadyError> {
        let command =
            OwnedCustody::restore(&self.client, &self.target, self.check.token.operation).await?;
        let CustodyAction::RenewPreparation(request) = command.action()? else {
            return Err(CustodyError::Context.into());
        };
        validate(&self.target, &request)?;
        if request.check.token != self.check.token || request.check.actor != self.check.actor {
            return Err(PreparationBaseError::Context.into());
        }
        Ok(ReadyPreparation {
            inner: Box::new(PreparationRequest {
                authority: self.authority.clone(),
                client: self.client.clone(),
                target: self.target.clone(),
                check: self.check.clone(),
                exact: ExactPreparation::Renew {
                    command,
                    session: Some(self.clone()),
                },
            }),
        })
    }
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
        let command = OwnedCustody::prepare(
            &self.client,
            &self.target,
            CustodyAction::RenewPreparation(request),
            identity,
        )
        .await?;
        self.live_lease()?;
        Ok(ReadyPreparation {
            inner: Box::new(PreparationRequest {
                authority: self.authority.clone(),
                client: self.client.clone(),
                target: self.target.clone(),
                check: self.check.clone(),
                exact: ExactPreparation::Renew {
                    command,
                    session: Some(self.clone()),
                },
            }),
        })
    }
}
