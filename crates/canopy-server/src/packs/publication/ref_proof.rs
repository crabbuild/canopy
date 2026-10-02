//! Privately prepared catalog -> exact ref targets and conditional ancestry.
//! Decoded proof DTOs are untrusted. Only MAC verification in an admitted final
//! command can turn them into publication authority.
use super::*;
use crate::packs::{
    catalog::CatalogReader,
    directory::index::IndexError,
    metadata::{MetadataError, MetadataLimits, PAGE_OBJECTS},
};
use crate::{ObjectId, ObjectKind, PushPlan};
use cellule_ltx::DiskBudget;
use cellule_runtime::{InvocationError, primitives::sql::SqlCell};
use std::{collections::BTreeSet, path::Path};
use tokio::time::timeout_at;

pub(super) mod ancestry;

#[derive(Debug, thiserror::Error)]
pub enum RefProofError {
    #[error("ref proof preparation lease is inactive")]
    Base(#[from] PreparationBaseError),
    #[error("ref proof catalog lookup failed")]
    Catalog(#[from] IndexError),
    #[error("ref proof scratch failed")]
    Metadata(#[from] MetadataError),
    #[error("ref proof issuance failed")]
    Attestation(#[from] CatalogAttestationError),
    #[error("ref proof SQL capability failed")]
    Capability(#[from] Error),
    #[error("ref proof policy query failed")]
    Query(#[source] Box<InvocationError<Vec<SqlResultSet>>>),
    #[error("ref proof encoding failed")]
    Codec(#[from] CodecError),
    #[error("ref plan has invalid or inconsistent targets")]
    Invalid,
    #[error("ref proof worker failed")]
    Task(#[from] tokio::task::JoinError),
    #[error("ref proof preparation was canceled")]
    Canceled,
}

/// Bounded evidence accompanying the existing PushPlan. Public fields permit
/// transport/inspection only; edits invalidate the signed binding. The ancestry
/// bit proves the fast-forward predicate (including vacuous creation/deletion
/// and identical tips); deletion permissions remain a separate current policy.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RefPublicationProof {
    pub plan: PushPlan,
    pub certificate: CatalogCertificate,
    pub ancestry: Vec<u8>,
}

impl WireValue for RefPublicationProof {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        evidence_shape(&self.plan, &self.ancestry)?;
        self.plan.encode(e)?;
        self.certificate.encode(e)?;
        e.write_bytes(&self.ancestry)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        let plan = PushPlan::decode(d)?;
        let certificate = CatalogCertificate::decode(d)?;
        let ancestry = d.read_bytes()?;
        evidence_shape(&plan, ancestry)?;
        Ok(Self {
            plan,
            certificate,
            ancestry: ancestry.to_vec(),
        })
    }
}
pub(super) fn proven(bits: &[u8], index: usize) -> bool {
    bits.get(index / 8)
        .is_some_and(|byte| byte & (1 << (index % 8)) != 0)
}
fn mark(bits: &mut [u8], index: usize) {
    bits[index / 8] |= 1 << (index % 8);
}
fn evidence_shape(plan: &PushPlan, bits: &[u8]) -> Result<(), CodecError> {
    let n = plan.updates.len();
    if n == 0
        || n > crate::refs::MAX_UPDATES
        || bits.len() != n.div_ceil(8)
        || (!n.is_multiple_of(8) && bits.last().is_some_and(|byte| *byte >> (n % 8) != 0))
    {
        return Err(CodecError::Invalid("invalid ref ancestry evidence"));
    }
    Ok(())
}
/// Hash the existing wire bytes in bounded chunks. Even a long valid ref name
/// is fed directly, without another full plan/name allocation.
pub(super) fn binding(plan: &PushPlan, bits: &[u8]) -> Result<[u8; 32], CodecError> {
    evidence_shape(plan, bits)?;
    let mut hash = blake3::Hasher::new();
    hash.update(b"canopy.ref-publication.v1\0");
    hash.update(&plan_digest(plan)?);
    let mut count = BoundedEncoder::new(4)?;
    count.write_count(bits.len())?;
    hash.update(&count.finish());
    hash.update(bits);
    Ok(*hash.finalize().as_bytes())
}
pub(super) fn plan_digest(plan: &PushPlan) -> Result<[u8; 32], CodecError> {
    let mut hash = blake3::Hasher::new();
    hash.update(b"canopy.ref-plan.v1\0");
    let mut prefix = BoundedEncoder::new(128)?;
    plan.encode_prefix(&mut prefix)?;
    hash.update(&prefix.finish());
    for update in &plan.updates {
        let mut name = BoundedEncoder::new(4)?;
        name.write_count(update.name.len())?;
        hash.update(&name.finish());
        hash.update(update.name.as_bytes());
        let mut suffix = BoundedEncoder::new(128)?;
        crate::refs::encode_update_suffix(update, &mut suffix)?;
        hash.update(&suffix.finish());
    }
    Ok(*hash.finalize().as_bytes())
}
fn shape(plan: &PushPlan, format: ObjectFormat) -> Result<(), RefProofError> {
    let mut prefix = BoundedEncoder::new(128)?;
    plan.encode_prefix(&mut prefix)?;
    let mut names = BTreeSet::new();
    for update in &plan.updates {
        if !crate::refs::valid_ref_name(&update.name)
            || crate::refs::server_owned_ref(&update.name)
            || !names.insert(update.name.as_str())
            || update
                .expected
                .as_ref()
                .is_some_and(|old| old.version <= 0 || old.version == i64::MAX)
            || [
                update.new_oid,
                update.expected.as_ref().and_then(|old| old.oid),
            ]
            .into_iter()
            .flatten()
            .any(|oid| oid.format() != format || oid.is_zero())
            || (update.new_oid.is_none()
                && update.expected.as_ref().and_then(|old| old.oid).is_none())
        {
            return Err(RefProofError::Invalid);
        }
    }
    Ok(())
}
impl PreparedCatalog {
    /// Validate targets through this prepared catalog. Ancestry is computed
    /// only for currently enabled fast-forward rules; final publication checks
    /// the current rule again and denies a newly required missing proof.
    pub async fn ref_proof(
        &self,
        plan: PushPlan,
        root: &Path,
        budget: DiskBudget,
        limits: MetadataLimits,
    ) -> Result<RefPublicationProof, RefProofError> {
        let (_, deadline) = self.base.live_lease()?;
        timeout_at(deadline, self.ref_proof_inner(plan, root, budget, limits))
            .await
            .map_err(|_| PreparationBaseError::Inactive)?
    }
    async fn ref_proof_inner(
        &self,
        plan: PushPlan,
        root: &Path,
        budget: DiskBudget,
        limits: MetadataLimits,
    ) -> Result<RefPublicationProof, RefProofError> {
        let (plan, bits) = self.ref_evidence(plan, root, budget, limits).await?;
        let certificate = self
            .issue_certificate(Some(binding(&plan, &bits)?), None)
            .await?;
        self.ensure_live()?;
        Ok(RefPublicationProof {
            plan,
            certificate,
            ancestry: bits,
        })
    }
    /// Checks the private catalog once. Completion signs these facts together
    /// with the native outcome, avoiding an intermediate certificate/query.
    pub(super) async fn ref_evidence(
        &self,
        plan: PushPlan,
        root: &Path,
        budget: DiskBudget,
        limits: MetadataLimits,
    ) -> Result<(PushPlan, Vec<u8>), RefProofError> {
        shape(&plan, self.catalog().format)?;
        if plan.actor != self.base.capability().2.actor {
            return Err(RefProofError::Invalid);
        }
        let reader = CatalogReader::open(self.base.indexes(), self.catalog()).await?;
        let files = self.base.files();
        for updates in plan.updates.chunks(PAGE_OBJECTS) {
            self.ensure_live()?;
            let ids: Vec<_> = updates.iter().filter_map(|update| update.new_oid).collect();
            let headers = reader.headers(&ids, &*files, &*files).await?;
            for (update, header) in updates
                .iter()
                .filter(|update| update.new_oid.is_some())
                .zip(headers)
            {
                let header = header.ok_or(RefProofError::Invalid)?;
                if Some(header.object.oid) != update.new_oid
                    || (update.name.starts_with("refs/heads/")
                        && header.object.kind != ObjectKind::Commit)
                {
                    return Err(RefProofError::Invalid);
                }
            }
        }
        let (client, target, _) = self.base.capability();
        let sql = SqlCell::<RepositoryModule>::new(client.clone(), target.clone())?;
        let mut bits = vec![0; plan.updates.len().div_ceil(8)];
        let mut walk: Option<ancestry::Walker> = None;
        for (page, updates) in plan.updates.chunks(128).enumerate() {
            self.ensure_live()?;
            let policies = sql.query(None, SqlBatch { statements: updates.iter().map(|update| SqlStatement { sql: "SELECT EXISTS(SELECT 1 FROM branch_rules WHERE reference=?1 AND enabled=1 AND fast_forward=1)".into(), parameters: vec![SqlValue::Text(update.name.clone())] }).collect() }).await.map_err(|error| RefProofError::Query(Box::new(error)))?;
            if policies.output.len() != updates.len() {
                return Err(RefProofError::Invalid);
            }
            for (at, (update, policy)) in updates.iter().zip(policies.output).enumerate() {
                let index = page * 128 + at;
                let required = match policy.rows.first().map(Vec::as_slice) {
                    Some([SqlValue::Integer(0)]) => false,
                    Some([SqlValue::Integer(1)]) => true,
                    _ => return Err(RefProofError::Invalid),
                };
                let old = update.expected.as_ref().and_then(|old| old.oid);
                if old.is_none() || old == update.new_oid || update.new_oid.is_none() {
                    mark(&mut bits, index);
                    continue;
                }
                if required {
                    if walk.is_none() {
                        walk = Some(ancestry::Walker::new(root, budget.clone(), limits).await?);
                    }
                    if walk
                        .as_mut()
                        .ok_or(RefProofError::Invalid)?
                        .is_ancestor(
                            &reader,
                            &files,
                            old.ok_or(RefProofError::Invalid)?,
                            update.new_oid.ok_or(RefProofError::Invalid)?,
                            &self.base,
                        )
                        .await?
                    {
                        mark(&mut bits, index);
                    }
                }
            }
        }
        self.ensure_live()?;
        Ok((plan, bits))
    }
}

#[cfg(test)]
mod tests;
