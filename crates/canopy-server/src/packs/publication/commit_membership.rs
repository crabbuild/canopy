//! Bounded historical commit membership under an existing retained serving pin.
//! This proves kind/existence only; callers must recheck their own current policy.
use super::*;
use crate::packs::directory::index::codec::fixed;
use crate::{ObjectId, ReadIdentity};
use cellule_runtime::{ApplicationId, CellId, TenantId};
use certificate::CertificateEnvelope;

const DOMAIN: &[u8] = b"canopy.commit-membership.v1\0";
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CommitMembership(CertificateEnvelope);
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct MembershipData {
    pub(super) tenant: [u8; 16],
    pub(super) application: [u8; 16],
    pub(super) token: ServingToken,
    pub(super) fact: GenerationFact,
    pub(super) actor: Option<String>,
    pub(super) oid: ObjectId,
}
impl MembershipData {
    fn validate(&self) -> Result<(), CodecError> {
        self.token.validate()?;
        self.fact.validate()?;
        if self.token.generation != self.fact.generation
            || self.fact.catalog.is_none_or(|c| {
                c.repository != self.token.repository || c.format != self.oid.format()
            })
            || self.fact.refs.is_none()
            || self.oid.is_zero()
            || self
                .actor
                .as_deref()
                .is_some_and(|a| validate_component(a).is_err())
        {
            return Err(CodecError::Invalid("invalid commit membership"));
        }
        Ok(())
    }
}
impl WireValue for MembershipData {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        self.validate()?;
        e.write_bytes(DOMAIN)?;
        e.write_bytes(&self.tenant)?;
        e.write_bytes(&self.application)?;
        self.token.encode(e)?;
        self.fact.encode(e)?;
        self.actor.encode(e)?;
        e.write_bytes(self.oid.as_ref())
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        if d.read_bytes()? != DOMAIN {
            return Err(CodecError::Invalid("invalid membership purpose"));
        }
        let value = Self {
            tenant: fixed(d)?,
            application: fixed(d)?,
            token: ServingToken::decode(d)?,
            fact: GenerationFact::decode(d)?,
            actor: Option::<String>::decode(d)?,
            oid: ObjectId::try_from(d.read_bytes()?)
                .map_err(|_| CodecError::Invalid("invalid membership OID"))?,
        };
        value.validate()?;
        Ok(value)
    }
}
pub(crate) struct MembershipRequest<'a> {
    pub(crate) cell: CellId,
    pub(crate) owner: Option<OwnerFence>,
    pub(crate) repository: [u8; 16],
    pub(crate) actor: &'a Option<String>,
    pub(crate) oid: ObjectId,
    pub(crate) admitted_ms: i64,
}
impl CommitMembership {
    pub(super) fn seal(data: MembershipData, seed: &[u8; 32]) -> Result<Self, CodecError> {
        Ok(Self(CertificateEnvelope::seal(&data, seed)?))
    }
    /// The receiver checks the original pin and immutable joint fact rather than
    /// the moving current head: unrelated pushes cannot invalidate membership.
    pub(crate) fn authorize(
        &self,
        request: MembershipRequest<'_>,
        mut query: impl FnMut(&SqlBatch) -> cellule_runtime::Result<Vec<SqlResultSet>>,
    ) -> cellule_runtime::Result<bool> {
        use sql::*;
        let MembershipRequest {
            cell,
            owner,
            repository,
            actor,
            oid,
            admitted_ms,
        } = request;
        let data: MembershipData = self.0.data()?;
        let target = crate::repository_target(
            TenantId::from_bytes(data.tenant),
            ApplicationId::from_bytes(data.application),
            repository,
        )?;
        if target.cell_id() != cell
            || data.token.repository != repository
            || data.actor != *actor
            || data.oid != oid
            || owner.is_some_and(|f| data.token.owner != f)
        {
            return Ok(false);
        }
        let scope = actor
            .as_deref()
            .map_or(ReadIdentity::Anonymous, ReadIdentity::Account);
        scope.validate()?;
        let access = query(&statement(
            &format!("SELECT 1 WHERE {}", crate::access::READ_ACCESS),
            vec![scope.parameter()],
        ))?;
        if rows(&access)?.is_empty() {
            return Ok(false);
        }
        let secret = query(&statement(
            "SELECT push_cert_seed FROM repository_identity WHERE singleton=1 AND repository_id=?1 AND object_format=?2",
            vec![
                blob(repository),
                SqlValue::Text(oid.format().as_str().into()),
            ],
        ))?;
        if rows(&secret)?.is_empty() || !self.0.authenticated(&attestation::seed(&secret)?) {
            return Ok(false);
        }
        let token = data.token;
        let pin = query(&statement(
            "SELECT 1 FROM catalog_serving_pins WHERE reader=?1 AND incarnation=?2 AND admission_sequence=?3 AND owner_epoch=?4 AND generation=?5 AND expires_at_ms>?6",
            vec![
                blob(token.reader),
                blob(token.owner.incarnation.as_bytes()),
                number(token.admission_sequence)?,
                blob(token.owner.epoch.to_be_bytes()),
                number(token.generation)?,
                SqlValue::Integer(now(admitted_ms)?),
            ],
        ))?;
        if rows(&pin)?.is_empty() {
            return Ok(false);
        }
        Ok(generation(
            &query(&statement(GENERATION, vec![number(token.generation)?]))?,
            repository,
            oid.format(),
        )? == data.fact)
    }
}
impl WireValue for CommitMembership {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        self.0.data::<MembershipData>()?;
        self.0.encode(e)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        let value = Self(CertificateEnvelope::decode(d)?);
        value.0.data::<MembershipData>()?;
        Ok(value)
    }
}
