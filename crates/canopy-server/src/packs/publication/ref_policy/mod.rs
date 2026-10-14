//! Paged direct-ref policy predicates with exact check dependency invalidation.
//! Raw transport DTOs grant no authority. Final root publication must check the
//! live guard and current epoch in its owner-fenced root/outcome transaction.
use super::*;
use crate::{PushPlan, packs::metadata::MetadataLimits};
use cellule_ltx::DiskBudget;
use cellule_runtime::{InvocationError, primitives::sql::SqlCell};
use std::path::Path;
use tokio::time::timeout_at;

mod codec;
mod commands;
mod prepare;
pub(super) use commands::current;
pub use commands::{CheckRefPolicyGuard, ReapRefPolicyGuard, RegisterRefPolicyPage};
pub(super) use prepare::ensure_ready;

pub const REF_POLICY_PAGE_BYTES: u32 = 256 << 10;
pub const REF_POLICY_PAGE_UPDATES: usize = 128;
pub const MAX_REF_POLICY_GUARDS: u64 = 4096;
pub const MAX_REF_POLICY_WATCHES: u64 = 2_097_152;
const WATCH_REAP_ROWS: u64 = 512;
const TOKEN_BYTES: u32 = 256;

/// Generation-independent intent. The existing catalog certificate binds its
/// exact proposal separately, while this guard can survive unrelated rebases.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RefPolicyIntent {
    pub id: [u8; 16],
    pub epoch: u64,
    pub updates: u64,
    pub plan_digest: [u8; 32],
    pub evidence_digest: [u8; 32],
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RefPolicyPage {
    pub intent: RefPolicyIntent,
    pub offset: u64,
    pub proof: RefPublicationProof,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RefPolicyProgress {
    pub next: u64,
    pub total: u64,
    pub valid: bool,
}
impl RefPolicyProgress {
    pub fn ready(self) -> bool {
        self.valid && self.next == self.total
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RefPolicyReply {
    Registered(RefPolicyProgress),
    Denied(PreparationDenial),
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RefPolicyLookup {
    pub check: LeaseCheck,
    pub intent: RefPolicyIntent,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RefPolicyReap {
    pub maintenance: MaintenanceRequest,
    pub id: [u8; 16],
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RefPolicyReapReply {
    Reaped { watches: u64, removed: bool },
    Denied(PreparationDenial),
}
/// Owned immutable original intent and evidence. Observers retain this value
/// and the exact issued page/MutationIdentity through uncertain page outcomes.
pub struct RefPolicyPreparation {
    intent: RefPolicyIntent,
    token: PreparationToken,
    format: ObjectFormat,
    plan: PushPlan,
    ancestry: Vec<u8>,
}
/// Private readiness result. Final signing rechecks catalog evidence and the
/// conditional ref transition; final execution must recheck guard freshness.
pub struct PreparedRefPolicyGuard {
    intent: RefPolicyIntent,
    token: PreparationToken,
    actor: String,
    format: ObjectFormat,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RefRootPublicationProof {
    pub certificate: CatalogCertificate,
    pub guard: RefPolicyIntent,
    pub snapshot: RefStateSnapshotRoot,
}
#[derive(Debug, thiserror::Error)]
pub enum RefPolicyPreparationError {
    #[error("ref policy preparation is inactive")]
    Base(#[from] PreparationBaseError),
    #[error("ref policy catalog evidence failed")]
    Evidence(#[from] RefProofError),
    #[error("ref policy conditional snapshot failed")]
    Snapshot(#[from] RefSnapshotPreparationError),
    #[error("ref policy certificate failed")]
    Certificate(#[from] CatalogAttestationError),
    #[error("ref policy encoding failed")]
    Codec(#[from] CodecError),
    #[error("ref policy SQL capability failed")]
    Capability(#[from] Error),
    #[error("ref policy epoch query failed")]
    Query(#[source] Box<InvocationError<Vec<SqlResultSet>>>),
    #[error("ref policy guard query failed")]
    Guard(#[source] Box<InvocationError<Option<RefPolicyProgress>>>),
    #[error("ref policy intent, readiness or custody differs")]
    Context,
}

fn scope(
    token: PreparationToken,
    actor: &str,
    format: ObjectFormat,
    intent: RefPolicyIntent,
) -> Result<[u8; 32], CodecError> {
    let mut e = BoundedEncoder::new(512)?;
    token.encode(&mut e)?;
    e.write_text(actor)?;
    e.write_u8(format.bytes() as u8)?;
    intent.encode(&mut e)?;
    let mut h = blake3::Hasher::new();
    h.update(b"canopy.ref-policy-scope.v1\0");
    h.update(&e.finish());
    Ok(*h.finalize().as_bytes())
}
fn page_binding(page: &RefPolicyPage) -> Result<[u8; 32], CodecError> {
    page.shape()?;
    page_payload_binding(
        page.intent,
        page.offset,
        &page.proof.plan,
        &page.proof.ancestry,
    )
}
fn page_payload_binding(
    intent: RefPolicyIntent,
    offset: u64,
    plan: &PushPlan,
    ancestry: &[u8],
) -> Result<[u8; 32], CodecError> {
    let mut e = BoundedEncoder::new(256)?;
    intent.encode(&mut e)?;
    e.write_u64(offset)?;
    e.write_bytes(&super::ref_proof::binding(plan, ancestry)?)?;
    let mut h = blake3::Hasher::new();
    h.update(b"canopy.ref-policy-page.v1\0");
    h.update(&e.finish());
    Ok(*h.finalize().as_bytes())
}
pub(super) fn root_binding(
    intent: RefPolicyIntent,
    snapshot: RefStateSnapshotRoot,
) -> Result<[u8; 32], CodecError> {
    let mut e = BoundedEncoder::new(512)?;
    intent.encode(&mut e)?;
    snapshot.encode(&mut e)?;
    let mut h = blake3::Hasher::new();
    h.update(b"canopy.ref-root-publication.v1\0");
    h.update(&e.finish());
    Ok(*h.finalize().as_bytes())
}
