//! First accepted catalog preparation, retained separately from current custody.
pub use super::admission_receipt::AdmissionReceiptError as PreparationReceiptError;
use super::{
    admission_receipt::{Admission, InitialAdmission},
    recovery::phase::Recorded,
    *,
};
use cellule_runtime::{CellClient, CellTarget, Committed, MutationIdentity, PendingMutation};

#[derive(Clone)]
pub(super) struct Preparation;
impl Admission for Preparation {
    const DOMAIN: &'static [u8] = b"canopy.initial-preparation-receipt.v1\0";
    const COLUMN: &'static str = "initial_preparation";
    type Lease = PreparationLease;
    type Reply = PreparationReply;
    fn grant(lease: PreparationLease) -> PreparationReply {
        PreparationReply::Granted(Box::new(lease))
    }
    fn token(lease: &PreparationLease) -> PreparationToken {
        lease.token
    }
    fn lease(result: &Recorded, request: &BeginRequest) -> Result<PreparationLease, CodecError> {
        let PreparationReply::Granted(lease) = result.decode_reply()? else {
            return Err(CodecError::Invalid(
                "initial preparation receipt is not a grant",
            ));
        };
        // Begin can observe a staging attempt that was already bound, or an
        // existing preparation. Its command sequence then follows admission.
        if result.rejected()
            || lease.token.repository != request.repository
            || lease.token.operation != request.operation
            || lease.token.request_digest != request.request_digest
            || lease.token.attempt > result.sequence()
        {
            return Err(CodecError::Invalid(
                "initial preparation receipt result differs",
            ));
        }
        Ok(*lease)
    }
}

/// Authenticated original Begin knowledge, including its actual SDK sequence.
/// The saved clock never extends a live lease. Claim or a fresh Check is needed
/// before any preparation factory can use this attempt.
#[derive(Clone)]
pub struct PreparationAdmission(InitialAdmission<Preparation>);
impl PreparationAdmission {
    pub async fn load(
        client: &CellClient,
        target: &CellTarget,
        operation: [u8; 16],
    ) -> Result<Option<Self>, PreparationReceiptError> {
        Ok(InitialAdmission::load(client, target, operation)
            .await?
            .map(Self))
    }
    pub fn request(&self) -> &BeginRequest {
        self.0.request()
    }
    pub fn lease(&self) -> PreparationLease {
        self.0.lease()
    }
    pub fn receipt(&self) -> cellule_runtime::Receipt {
        self.0.receipt()
    }
    pub async fn ready_claim(
        &self,
        client: CellClient,
        lease_ms: u64,
        identity: MutationIdentity,
        authority: PreparationAuthority,
    ) -> Result<ReadyPreparation, PreparationReadyError> {
        ReadyPreparation::claim(
            client,
            self.0.target().clone(),
            LeaseRequest {
                check: LeaseCheck {
                    token: self.lease().token,
                    actor: self.request().actor.clone(),
                },
                lease_ms,
            },
            identity,
            authority,
        )
        .await
    }
    pub fn original(
        &self,
        evidence: &PendingMutation,
    ) -> Result<Option<Committed<PreparationReply>>, PreparationReceiptError> {
        self.0.original(evidence)
    }
}
pub(super) fn save(
    context: &CommandContext<'_, '_>,
    request: &BeginRequest,
    lease: PreparationLease,
) -> cellule_runtime::Result<()> {
    super::admission_receipt::save::<Preparation>(context, request, lease)
}
pub(super) fn restart_matches(
    context: &CommandContext<'_, '_>,
    check: &LeaseCheck,
) -> cellule_runtime::Result<bool> {
    super::admission_receipt::restart_matches::<Preparation>(context, check)
}
