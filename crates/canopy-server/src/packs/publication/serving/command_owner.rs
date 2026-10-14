//! Registered originals reuse custody history and the common publication queue.
use super::super::custody::{OwnedCustody, RESERVATION, project};
use super::*;
use cellule_runtime::{Committed, InvocationError, PendingMutation};

#[must_use]
pub struct ReadyServingCommand {
    client: CellClient,
    target: CellTarget,
    request: BeginRequest,
    original: Arc<OwnedCustody>,
    // Only the original local acquisition can hand off physical ownership.
    // Restored journal knowledge and renewals cannot recreate it.
    local_acquisition: bool,
    // Renewal is owned physical work from preparation until known disposition.
    _guard: Option<Arc<super::session::Active>>,
}
impl ReadyServingCommand {
    pub async fn acquire(
        client: CellClient,
        target: CellTarget,
        request: BeginRequest,
        identity: MutationIdentity,
        authority: PreparationAuthority,
    ) -> Result<Self, CustodyError> {
        request.encode(&mut BoundedEncoder::new(4096)?)?;
        if !authority.matches(&target)
            || crate::repository_target(target.tenant(), target.application(), request.repository)?
                != target
        {
            return Err(CustodyError::Context);
        }
        let original = OwnedCustody::prepare(
            &client,
            &target,
            CustodyAction::AcquireServing(request.clone()),
            identity,
        )
        .await?;
        Ok(Self {
            client,
            target,
            request,
            original: Arc::new(original),
            local_acquisition: true,
            _guard: None,
        })
    }
    pub(super) async fn renew(
        client: CellClient,
        target: CellTarget,
        request: RenewServingRequest,
        request_digest: [u8; 32],
        identity: MutationIdentity,
        guard: Arc<super::session::Active>,
    ) -> Result<Self, CustodyError> {
        let action = CustodyAction::RenewServing {
            request,
            request_digest,
        };
        let context = context(&action)?;
        let original = OwnedCustody::prepare(&client, &target, action, identity).await?;
        Ok(Self {
            client,
            target,
            request: context,
            original: Arc::new(original),
            local_acquisition: false,
            _guard: Some(guard),
        })
    }
    /// Reconstruct the recorded serving original. A historical grant remains
    /// knowledge; ServingPin construction separately rechecks physical authority.
    pub async fn restore(
        client: CellClient,
        target: CellTarget,
        reader: [u8; 16],
        authority: PreparationAuthority,
    ) -> Result<Self, CustodyError> {
        if !authority.matches(&target) {
            return Err(CustodyError::Context);
        }
        let original =
            OwnedCustody::restore_for(&client, &target, CustodyPurpose::Serving, reader).await?;
        let request = context(&original.action()?)?;
        Ok(Self {
            client,
            target,
            request,
            original: Arc::new(original),
            local_acquisition: false,
            _guard: None,
        })
    }
    pub fn evidence(&self) -> &PendingMutation {
        self.original.evidence()
    }
    /// Retain this accepted local acquisition even if its lease expired or the
    /// requesting account lost Read. Every I/O still checks fresh Read/expiry;
    /// this handoff permits safe physical ownership and authenticated cleanup.
    pub async fn retain_acquisition(
        &self,
        context: ServingContext,
    ) -> Result<ServingPin, ServingReadError> {
        if !self.local_acquisition || self.target != context.target_for_handoff() {
            return Err(ServingReadError::Context);
        }
        ServingPin::retain_original(context, self.original.clone()).await
    }
    pub(in crate::packs::publication) fn reservation(&self) -> u64 {
        RESERVATION
    }
    pub(in crate::packs::publication) fn dispatch_copy(&self) -> Self {
        Self {
            client: self.client.clone(),
            target: self.target.clone(),
            request: self.request.clone(),
            original: self.original.clone(),
            local_acquisition: self.local_acquisition,
            _guard: self._guard.clone(),
        }
    }
    pub(in crate::packs::publication) fn context(
        &self,
    ) -> (&CellClient, &CellTarget, BeginRequest) {
        (&self.client, &self.target, self.request.clone())
    }
    pub(in crate::packs::publication) fn pending(&self) -> PublicationError {
        PublicationError::ServingCommand(InvocationError::Pending(Box::new(
            self.evidence().clone(),
        )))
    }
    pub(in crate::packs::publication) async fn dispatch(
        self,
        recover: bool,
        fault: u8,
    ) -> Result<Committed<ServingReply>, PublicationError> {
        let result = self
            .original
            .invoke(&self.client, recover, fault, || Ok(()))
            .await
            .map_err(|source| PublicationError::Custody {
                evidence: Box::new(self.evidence().clone()),
                source: Box::new(source),
            })?;
        project(result, |reply| match reply {
            CustodyReply::Serving(reply) => Some(reply),
            _ => None,
        })
        .map_err(PublicationError::ServingCommand)
    }
}
fn context(action: &CustodyAction) -> Result<BeginRequest, CustodyError> {
    match action {
        CustodyAction::AcquireServing(request) => Ok(request.clone()),
        CustodyAction::RenewServing {
            request,
            request_digest,
        } => Ok(BeginRequest {
            repository: request.check.token.repository,
            operation: request.check.token.reader,
            request_digest: *request_digest,
            actor: request.check.actor.clone().ok_or(CustodyError::Context)?,
            lease_ms: request.lease_ms,
        }),
        _ => Err(CustodyError::Context),
    }
}
