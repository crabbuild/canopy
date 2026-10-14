//! Private exact-ref observation under a live serving pin and CURRENT joint fact.
//! Editorial receivers independently recheck current policy in their transaction.
use super::*;
use crate::ReadIdentity;
use crate::packs::directory::index::codec::fixed;
use cellule_runtime::{ApplicationId, CellId, TenantId};
use certificate::CertificateEnvelope;

mod selection;
pub(crate) use selection::{REF_SELECTION_BYTES, RefFact, RefSelection};

const DOMAIN: &[u8] = b"canopy.ref-observation.v1\0";
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RefObservation(CertificateEnvelope);
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct ObservationData {
    pub(super) tenant: [u8; 16],
    pub(super) application: [u8; 16],
    pub(super) token: ServingToken,
    pub(super) fact: GenerationFact,
    pub(super) actor: Option<String>,
    pub(super) binding: [u8; 32],
}
impl ObservationData {
    fn validate(&self) -> Result<(), CodecError> {
        self.token.validate()?;
        self.fact.validate()?;
        if self.token.generation != self.fact.generation
            || self
                .fact
                .catalog
                .is_none_or(|c| c.repository != self.token.repository)
            || self.fact.refs.is_none()
            || self.binding == [0; 32]
            || self
                .actor
                .as_deref()
                .is_some_and(|a| validate_component(a).is_err())
        {
            return Err(CodecError::Invalid("invalid ref observation"));
        }
        Ok(())
    }
}
impl WireValue for ObservationData {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        self.validate()?;
        e.write_bytes(DOMAIN)?;
        e.write_bytes(&self.tenant)?;
        e.write_bytes(&self.application)?;
        self.token.encode(e)?;
        self.fact.encode(e)?;
        self.actor.encode(e)?;
        e.write_bytes(&self.binding)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        if d.read_bytes()? != DOMAIN {
            return Err(CodecError::Invalid("invalid ref observation purpose"));
        }
        let value = Self {
            tenant: fixed(d)?,
            application: fixed(d)?,
            token: ServingToken::decode(d)?,
            fact: GenerationFact::decode(d)?,
            actor: Option::<String>::decode(d)?,
            binding: fixed(d)?,
        };
        value.validate()?;
        Ok(value)
    }
}
pub(crate) struct ObservationRequest<'a> {
    pub(crate) cell: CellId,
    pub(crate) owner: Option<OwnerFence>,
    pub(crate) repository: [u8; 16],
    pub(crate) actor: &'a Option<String>,
    pub(crate) binding: [u8; 32],
    pub(crate) admitted_ms: i64,
}
impl RefObservation {
    pub(super) fn seal(data: ObservationData, seed: &[u8; 32]) -> Result<Self, CodecError> {
        Ok(Self(CertificateEnvelope::seal(&data, seed)?))
    }
    /// This proof requires the original pin and equality with the current joint fact.
    pub(crate) fn authorize(
        &self,
        request: ObservationRequest<'_>,
        mut query: impl FnMut(&SqlBatch) -> cellule_runtime::Result<Vec<SqlResultSet>>,
    ) -> cellule_runtime::Result<bool> {
        use sql::*;
        let ObservationRequest {
            cell,
            owner,
            repository,
            actor,
            binding,
            admitted_ms,
        } = request;
        let data: ObservationData = self.0.data()?;
        let target = crate::repository_target(
            TenantId::from_bytes(data.tenant),
            ApplicationId::from_bytes(data.application),
            repository,
        )?;
        if target.cell_id() != cell
            || data.token.repository != repository
            || data.actor != *actor
            || data.binding != binding
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
                SqlValue::Text(
                    data.fact
                        .catalog
                        .ok_or(Error::Command("ref observation catalog absent"))?
                        .format
                        .as_str()
                        .into(),
                ),
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
        let format = data
            .fact
            .catalog
            .ok_or(Error::Command("ref observation catalog absent"))?
            .format;
        // A retained historical commit proof permits old generations. Ref policy
        // must match the moving current joint fact, including HEAD-only changes.
        Ok(generation(&query(&statement(CURRENT, vec![]))?, repository, format)? == data.fact)
    }
}
impl WireValue for RefObservation {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        self.0.data::<ObservationData>()?;
        self.0.encode(e)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        let value = Self(CertificateEnvelope::decode(d)?);
        value.0.data::<ObservationData>()?;
        Ok(value)
    }
}
