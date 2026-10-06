//! Bounded conditional catalog certificate. Only the prepared catalog factory
//! supplies signing facts; decoded bytes remain untrusted until MAC verification.
use super::*;
use crate::packs::directory::index::codec::{artifact, fixed, read_artifact};

pub const CERTIFICATE_BYTES: u32 = 1024;
const PAYLOAD_BYTES: u32 = 960;
const DOMAIN: &[u8] = b"canopy.catalog-attestation.v4\0";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CatalogCertificate(pub(super) CertificateEnvelope);
/// Shared bounded MAC carrier. Each typed proof validates its own domain/data.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct CertificateEnvelope {
    pub(super) body: Vec<u8>,
    tag: [u8; 32],
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct CertificateData {
    pub(super) compaction: bool,
    pub(super) tenant: [u8; 16],
    pub(super) application: [u8; 16],
    pub(super) token: PreparationToken,
    pub(super) actor: String,
    pub(super) retention_floor: u64,
    pub(super) retention_certificate: Option<[u8; 32]>,
    pub(super) base: GenerationFact,
    pub(super) catalog: StoredCatalog,
    pub(super) object_count: u64,
    pub(super) edge_count: u64,
    pub(super) input_count: u64,
    pub(super) input_checkpoint_digest: Option<[u8; 32]>,
    pub(super) inputs_digest: [u8; 32],
    pub(super) inventory_digest: [u8; 32],
    /// Exact ref-plan and ancestry evidence binding, minted only after checks
    /// against this privately constructed prepared catalog. None is catalog-only.
    pub(super) refs_digest: Option<[u8; 32]>,
    /// Exact network completion payload, distinct from a refs-only publication.
    pub(super) completion_digest: Option<[u8; 32]>,
}
impl CertificateData {
    pub(super) fn from_prepared(prepared: &PreparedCatalog) -> Self {
        let (_, target, check) = prepared.base.capability();
        Self {
            compaction: false,
            tenant: *target.tenant().as_bytes(),
            application: *target.application().as_bytes(),
            token: prepared.token(),
            actor: check.actor.clone(),
            retention_floor: prepared.base.retention_floor().generation,
            retention_certificate: prepared.base.retention_floor().certificate,
            base: prepared.base(),
            catalog: prepared.catalog(),
            object_count: prepared.object_count(),
            edge_count: prepared.edge_count(),
            input_count: prepared.input_count(),
            input_checkpoint_digest: prepared.input_checkpoint_digest,
            inputs_digest: prepared.inputs_digest(),
            inventory_digest: prepared.inventory_digest(),
            refs_digest: None,
            completion_digest: None,
        }
    }
    fn validate(&self) -> Result<(), CodecError> {
        self.base.validate()?;
        self.catalog
            .validate()
            .map_err(|_| CodecError::Invalid("invalid attested catalog"))?;
        if validate_component(&self.actor).is_err()
            || self.retention_floor > self.base.generation
            || (self.retention_floor == 0) != self.retention_certificate.is_none()
            || self.catalog.repository != self.token.repository
            || self.catalog.operation != self.token.artifact_operation
            || self.base.catalog.is_some_and(|base| {
                base.repository != self.token.repository || base.format != self.catalog.format
            })
            || [self.object_count, self.edge_count, self.input_count]
                .iter()
                .any(|value| *value > i64::MAX as u64)
            || (self.input_count == 0) != (self.object_count == 0)
            || (self.object_count == 0 && self.edge_count != 0)
            // A delete-only/ref-only push retains authenticated request and
            // native outcome custody without adding any physical pack. Its
            // checkpoint is still checked by final publication. Compaction
            // has no native push checkpoint.
            || (self.input_checkpoint_digest.is_some() && self.compaction)
            || (self.compaction && (self.refs_digest.is_some() || self.completion_digest.is_some()))
        {
            return Err(CodecError::Invalid("invalid catalog attestation facts"));
        }
        Ok(())
    }
}
impl WireValue for CertificateData {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        self.validate()?;
        e.write_bytes(DOMAIN)?;
        e.write_bool(self.compaction)?;
        e.write_bytes(&self.tenant)?;
        e.write_bytes(&self.application)?;
        self.token.encode(e)?;
        e.write_text(&self.actor)?;
        e.write_u64(self.retention_floor)?;
        e.write_bool(self.retention_certificate.is_some())?;
        if let Some(certificate) = self.retention_certificate {
            e.write_bytes(&certificate)?;
        }
        // The token already binds repository and proposed creating namespace.
        // Encode that context once; reconstruct the existing typed structures
        // on decode instead of repeating full catalog descriptor domains.
        e.write_u8(self.catalog.format.bytes() as u8)?;
        e.write_u64(self.base.generation)?;
        e.write_bool(self.base.catalog.is_some())?;
        if let Some(catalog) = self.base.catalog {
            e.write_bytes(&catalog.operation)?;
            artifact(e, catalog.artifact)?;
        }
        self.base.refs.encode(e)?;
        self.base
            .certificate
            .as_ref()
            .map(|v| v.to_vec())
            .encode(e)?;
        artifact(e, self.catalog.artifact)?;
        e.write_u64(self.object_count)?;
        e.write_u64(self.edge_count)?;
        e.write_u64(self.input_count)?;
        e.write_bool(self.input_checkpoint_digest.is_some())?;
        if let Some(digest) = self.input_checkpoint_digest {
            e.write_bytes(&digest)?;
        }
        e.write_bytes(&self.inputs_digest)?;
        e.write_bytes(&self.inventory_digest)?;
        e.write_bool(self.refs_digest.is_some())?;
        if let Some(digest) = self.refs_digest {
            e.write_bytes(&digest)?;
        }
        e.write_bool(self.completion_digest.is_some())?;
        if let Some(digest) = self.completion_digest {
            e.write_bytes(&digest)?;
        }
        Ok(())
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        if d.read_bytes()? != DOMAIN {
            return Err(CodecError::Invalid("invalid catalog attestation domain"));
        }
        let compaction = d.read_bool()?;
        let tenant = fixed(d)?;
        let application = fixed(d)?;
        let token = PreparationToken::decode(d)?;
        let actor = d.read_text()?.into();
        let retention_floor = d.read_u64()?;
        let retention_certificate = if d.read_bool()? {
            Some(fixed(d)?)
        } else {
            None
        };
        let format = match d.read_u8()? {
            20 => ObjectFormat::Sha1,
            32 => ObjectFormat::Sha256,
            _ => return Err(CodecError::Invalid("invalid attested catalog format")),
        };
        let generation = d.read_u64()?;
        let catalog = if d.read_bool()? {
            Some(StoredCatalog {
                repository: token.repository,
                operation: fixed(d)?,
                format,
                artifact: read_artifact(d)?,
            })
        } else {
            None
        };
        let refs = Option::<RefStateSnapshotRoot>::decode(d)?;
        let certificate = Option::<Vec<u8>>::decode(d)?
            .map(|v| {
                v.try_into()
                    .map_err(|_| CodecError::Invalid("invalid generation certificate"))
            })
            .transpose()?;
        let base = GenerationFact {
            generation,
            catalog,
            refs,
            certificate,
        };
        let catalog = StoredCatalog {
            repository: token.repository,
            operation: token.artifact_operation,
            format,
            artifact: read_artifact(d)?,
        };
        let value = Self {
            compaction,
            tenant,
            application,
            token,
            actor,
            retention_floor,
            retention_certificate,
            base,
            catalog,
            object_count: d.read_u64()?,
            edge_count: d.read_u64()?,
            input_count: d.read_u64()?,
            input_checkpoint_digest: if d.read_bool()? {
                Some(fixed(d)?)
            } else {
                None
            },
            inputs_digest: fixed(d)?,
            inventory_digest: fixed(d)?,
            refs_digest: if d.read_bool()? {
                Some(fixed(d)?)
            } else {
                None
            },
            completion_digest: if d.read_bool()? {
                Some(fixed(d)?)
            } else {
                None
            },
        };
        value.validate()?;
        Ok(value)
    }
}
impl CertificateEnvelope {
    pub(super) fn seal(data: &impl WireValue, seed: &[u8; 32]) -> Result<Self, CodecError> {
        let mut encoder = BoundedEncoder::new(PAYLOAD_BYTES)?;
        data.encode(&mut encoder)?;
        let body = encoder.finish();
        let tag = *mac(seed, &body).as_bytes();
        Ok(Self { body, tag })
    }
    pub(super) fn data<T: WireValue>(&self) -> Result<T, CodecError> {
        let mut decoder = BoundedDecoder::new(&self.body, PAYLOAD_BYTES)?;
        let value = T::decode(&mut decoder)?;
        decoder.finish()?;
        Ok(value)
    }
    pub(super) fn authenticated(&self, seed: &[u8; 32]) -> bool {
        mac(seed, &self.body) == blake3::Hash::from_bytes(self.tag)
    }
}
impl CatalogCertificate {
    pub(super) fn seal(data: &CertificateData, seed: &[u8; 32]) -> Result<Self, CodecError> {
        Ok(Self(CertificateEnvelope::seal(data, seed)?))
    }
    pub(super) fn data(&self) -> Result<CertificateData, CodecError> {
        self.0.data()
    }
    pub(super) fn authenticated(&self, seed: &[u8; 32]) -> bool {
        self.0.authenticated(seed)
    }
    pub(super) fn bytes(&self) -> Result<Vec<u8>, CodecError> {
        let mut encoder = BoundedEncoder::new(CERTIFICATE_BYTES)?;
        self.encode(&mut encoder)?;
        Ok(encoder.finish())
    }
}
fn mac(seed: &[u8; 32], bytes: &[u8]) -> blake3::Hash {
    // Reuse the repository secret with a separate key derivation domain;
    // public signed-push nonces and catalog certificates share no MAC key.
    let key = blake3::derive_key("canopy.catalog-attestation-key.v1", seed);
    blake3::keyed_hash(&key, bytes)
}
impl WireValue for CertificateEnvelope {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        if self.body.is_empty() || self.body.len() > PAYLOAD_BYTES as usize {
            return Err(CodecError::Invalid("certificate size"));
        }
        e.write_bytes(&self.body)?;
        e.write_bytes(&self.tag)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        let body = d.read_bytes()?;
        if body.is_empty() || body.len() > PAYLOAD_BYTES as usize {
            return Err(CodecError::Invalid("certificate size"));
        }
        Ok(Self {
            body: body.into(),
            tag: fixed(d)?,
        })
    }
}
impl WireValue for CatalogCertificate {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        self.data()?;
        self.0.encode(e)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        let value = Self(CertificateEnvelope::decode(d)?);
        value.data()?;
        Ok(value)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RegisteredCatalog {
    pub token: PreparationToken,
    pub certificate_digest: [u8; 32],
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AttestationOutcome {
    Registered(RegisteredCatalog),
    Denied(PreparationDenial),
}
impl WireValue for AttestationOutcome {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        match self {
            Self::Registered(value) => {
                e.write_u8(0)?;
                value.token.encode(e)?;
                e.write_bytes(&value.certificate_digest)
            }
            Self::Denied(reason) => PreparationReply::Denied(*reason).encode(e),
        }
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        match d.read_u8()? {
            0 => Ok(Self::Registered(RegisteredCatalog {
                token: PreparationToken::decode(d)?,
                certificate_digest: fixed(d)?,
            })),
            1 => Ok(Self::Denied(PreparationDenial::Unauthorized)),
            2 => Ok(Self::Denied(PreparationDenial::Conflict)),
            3 => Ok(Self::Denied(PreparationDenial::Stale)),
            4 => Ok(Self::Denied(PreparationDenial::Expired)),
            5 => Ok(Self::Denied(PreparationDenial::Capacity)),
            6 => Ok(Self::Denied(PreparationDenial::Missing)),
            _ => Err(CodecError::Invalid("invalid attestation outcome")),
        }
    }
}
