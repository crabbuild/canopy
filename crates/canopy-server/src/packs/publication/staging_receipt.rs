//! Original initial-admission knowledge is independent of live input custody.
pub use super::admission_receipt::AdmissionReceiptError as StagingReceiptError;
use super::{
    admission_receipt::{Admission, InitialAdmission},
    recovery::phase::Recorded,
    *,
};
use cellule_runtime::{CellClient, CellTarget, Committed, MutationIdentity, PendingMutation};

#[derive(Clone)]
pub(super) struct Staging;
impl Admission for Staging {
    const DOMAIN: &'static [u8] = b"canopy.initial-staging-receipt.v1\0";
    const COLUMN: &'static str = "initial_staging";
    type Lease = StagingLease;
    type Reply = StagingReply;
    fn grant(lease: StagingLease) -> StagingReply {
        StagingReply::Granted(Box::new(lease))
    }
    fn token(lease: &StagingLease) -> PreparationToken {
        lease.token
    }
    fn lease(result: &Recorded, request: &BeginRequest) -> Result<StagingLease, CodecError> {
        let StagingReply::Granted(lease) = result.decode_reply()? else {
            return Err(CodecError::Invalid(
                "initial staging receipt is not a grant",
            ));
        };
        if result.rejected()
            || lease.token.repository != request.repository
            || lease.token.operation != request.operation
            || lease.token.request_digest != request.request_digest
            || lease.token.attempt != result.sequence()
            || lease.observed_at_ms < 0
            || lease.expires_at_ms <= lease.observed_at_ms
            || lease.expires_at_ms - lease.observed_at_ms != request.lease_ms as i64
        {
            return Err(CodecError::Invalid(
                "initial staging receipt result differs",
            ));
        }
        Ok(*lease)
    }
}
/// Trusted service knowledge of the first accepted Begin. It grants no upload,
/// write or response permission. A restarted caller must explicitly Claim.
#[derive(Clone)]
pub struct StagingAdmission(InitialAdmission<Staging>);
impl StagingAdmission {
    pub async fn load(
        client: &CellClient,
        target: &CellTarget,
        operation: [u8; 16],
    ) -> Result<Option<Self>, StagingReceiptError> {
        Ok(InitialAdmission::load(client, target, operation)
            .await?
            .map(Self))
    }
    pub fn lease(&self) -> StagingLease {
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
    ) -> Result<ReadyStaging, StagingError> {
        ReadyStaging::claim(
            client,
            self.0.target().clone(),
            LeaseRequest {
                check: LeaseCheck {
                    token: self.lease().token,
                    actor: self.0.request().actor.clone(),
                },
                lease_ms,
            },
            identity,
        )
        .await
    }
    pub(super) fn original(
        &self,
        evidence: &PendingMutation,
    ) -> Result<Option<Committed<StagingReply>>, StagingReceiptError> {
        self.0.original(evidence)
    }
}
pub(super) fn save(
    context: &CommandContext<'_, '_>,
    request: &BeginRequest,
    lease: StagingLease,
) -> cellule_runtime::Result<()> {
    super::admission_receipt::save::<Staging>(context, request, lease)
}
pub(super) fn restart_matches(
    context: &CommandContext<'_, '_>,
    check: &LeaseCheck,
) -> cellule_runtime::Result<bool> {
    super::admission_receipt::restart_matches::<Staging>(context, check)
}
