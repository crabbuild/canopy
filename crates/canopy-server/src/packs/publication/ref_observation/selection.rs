//! Byte-bounded exact facts reuse the immutable ref state's OID/version shape.
use super::*;
use crate::refs::{RefExpectation, valid_ref_name};

pub(crate) const REF_SELECTION_BYTES: u32 = 560 << 10;
const NAME_BYTES: usize = 512 << 10;
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RefFact {
    pub(crate) name: String,
    pub(crate) state: Option<RefExpectation>,
}
#[derive(Clone, Debug)]
pub(crate) struct RefSelection {
    pub(crate) repository: [u8; 16],
    pub(crate) actor: Option<String>,
    pub(crate) facts: Vec<RefFact>,
    pub(crate) proof: Option<RefObservation>,
}
impl RefSelection {
    pub(crate) fn binding(&self, request: [u8; 32]) -> Result<[u8; 32], CodecError> {
        self.shape()?;
        let mut h = blake3::Hasher::new();
        h.update(b"canopy.ref-selection.v1\0");
        h.update(&request);
        h.update(&(self.facts.len() as u64).to_le_bytes());
        for fact in &self.facts {
            h.update(&(fact.name.len() as u64).to_le_bytes());
            h.update(fact.name.as_bytes());
            h.update(&[u8::from(fact.state.is_some())]);
            if let Some(state) = &fact.state {
                h.update(&state.version.to_le_bytes());
                h.update(&[u8::from(state.oid.is_some())]);
                if let Some(oid) = state.oid {
                    h.update(&[oid.format().bytes() as u8]);
                    h.update(oid.as_ref());
                }
            }
        }
        Ok(*h.finalize().as_bytes())
    }
    pub(crate) fn authorized(
        &self,
        cell: cellule_runtime::CellId,
        owner: Option<OwnerFence>,
        admitted_ms: i64,
        request: [u8; 32],
        query: impl FnMut(&SqlBatch) -> cellule_runtime::Result<Vec<SqlResultSet>>,
    ) -> cellule_runtime::Result<bool> {
        let Some(proof) = &self.proof else {
            return Ok(false);
        };
        proof.authorize(
            ObservationRequest {
                cell,
                owner,
                repository: self.repository,
                actor: &self.actor,
                binding: self.binding(request)?,
                admitted_ms,
            },
            query,
        )
    }
    fn shape(&self) -> Result<(), CodecError> {
        if crate::validate_repository_id(self.repository).is_err()
            || self
                .actor
                .as_deref()
                .is_some_and(|a| validate_component(a).is_err())
            || self.facts.len() > 128
            || self.facts.iter().map(|f| f.name.len()).sum::<usize>() > NAME_BYTES
            || self.facts.windows(2).any(|p| p[0].name >= p[1].name)
            || self.facts.iter().any(|f| {
                !valid_ref_name(&f.name)
                    || f.name.len() > crate::packs::ref_state::MAX_NAME_BYTES
                    || f.state
                        .as_ref()
                        .is_some_and(|s| s.version < 1 || s.oid.is_some_and(|o| o.is_zero()))
            })
        {
            return Err(CodecError::Invalid("invalid exact ref selection"));
        }
        Ok(())
    }
}
impl WireValue for RefSelection {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        self.shape()?;
        e.write_bytes(&self.repository)?;
        self.actor.encode(e)?;
        e.write_count(self.facts.len())?;
        for fact in &self.facts {
            e.write_text(&fact.name)?;
            e.write_bool(fact.state.is_some())?;
            if let Some(state) = &fact.state {
                e.write_i64(state.version)?;
                e.write_bool(state.oid.is_some())?;
                if let Some(oid) = state.oid {
                    e.write_bytes(oid.as_ref())?;
                }
            }
        }
        self.proof.encode(e)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        let repository = fixed(d)?;
        let actor = Option::<String>::decode(d)?;
        let count = d.read_count()?;
        if count > 128 {
            return Err(CodecError::Invalid("ref selection count"));
        }
        let mut facts = Vec::with_capacity(count);
        let mut bytes = 0;
        for _ in 0..count {
            let name = d.read_text()?;
            bytes += name.len();
            if bytes > NAME_BYTES {
                return Err(CodecError::Invalid("ref selection bytes"));
            }
            let state = if d.read_bool()? {
                Some(RefExpectation {
                    version: d.read_i64()?,
                    oid: if d.read_bool()? {
                        Some(
                            crate::ObjectId::try_from(d.read_bytes()?)
                                .map_err(|_| CodecError::Invalid("ref selection OID"))?,
                        )
                    } else {
                        None
                    },
                })
            } else {
                None
            };
            facts.push(RefFact {
                name: name.into(),
                state,
            });
        }
        let value = Self {
            repository,
            actor,
            facts,
            proof: Option::<RefObservation>::decode(d)?,
        };
        value.shape()?;
        Ok(value)
    }
}
