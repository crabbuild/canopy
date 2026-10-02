use super::*;
use crate::packs::directory::index::codec::fixed;

fn invalid() -> CodecError {
    CodecError::Invalid("invalid preparation context")
}
pub(in crate::packs) fn artifact_valid(operation: [u8; 16]) -> Result<(), CodecError> {
    let sequence = u64::from_be_bytes(operation[8..].try_into().map_err(|_| invalid())?);
    if &operation[..8] != b"CANOPY01" || sequence == 0 || sequence > i64::MAX as u64 {
        return Err(invalid());
    }
    Ok(())
}
fn owner_valid(owner: OwnerFence) -> Result<(), CodecError> {
    if owner.epoch == 0 {
        Err(invalid())
    } else {
        Ok(())
    }
}
fn owner_encode(owner: OwnerFence, e: &mut BoundedEncoder) -> Result<(), CodecError> {
    owner_valid(owner)?;
    e.write_bytes(owner.incarnation.as_bytes())?;
    e.write_u64(owner.epoch)
}
fn owner_decode(d: &mut BoundedDecoder<'_>) -> Result<OwnerFence, CodecError> {
    let owner = OwnerFence {
        incarnation: IncarnationId::from_bytes(fixed(d)?),
        epoch: d.read_u64()?,
    };
    owner_valid(owner)?;
    Ok(owner)
}
fn repo_valid(repo: [u8; 16]) -> Result<(), CodecError> {
    crate::validate_repository_id(repo).map_err(|_| invalid())
}
fn actor_valid(actor: &str) -> Result<(), CodecError> {
    validate_component(actor).map_err(|_| invalid())
}
fn lease_valid(ms: u64) -> Result<(), CodecError> {
    if ms == 0 || ms > MAX_LEASE_MS {
        Err(invalid())
    } else {
        Ok(())
    }
}
impl WireValue for PreparationToken {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        repo_valid(self.repository)?;
        artifact_valid(self.artifact_operation)?;
        if self.operation == [0; 16] || self.attempt == 0 || self.attempt > i64::MAX as u64 {
            return Err(invalid());
        }
        e.write_bytes(&self.repository)?;
        e.write_bytes(&self.operation)?;
        e.write_bytes(&self.artifact_operation)?;
        e.write_bytes(&self.request_digest)?;
        owner_encode(self.owner, e)?;
        e.write_u64(self.attempt)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        let value = Self {
            repository: fixed(d)?,
            operation: fixed(d)?,
            artifact_operation: fixed(d)?,
            request_digest: fixed(d)?,
            owner: owner_decode(d)?,
            attempt: d.read_u64()?,
        };
        repo_valid(value.repository)?;
        artifact_valid(value.artifact_operation)?;
        if value.operation == [0; 16] || value.attempt == 0 || value.attempt > i64::MAX as u64 {
            return Err(invalid());
        }
        Ok(value)
    }
}
impl WireValue for GenerationFact {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        self.validate()?;
        e.write_u64(self.generation)?;
        self.catalog.encode(e)?;
        self.certificate.as_ref().map(|v| v.to_vec()).encode(e)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        let generation = d.read_u64()?;
        let catalog = Option::<StoredCatalog>::decode(d)?;
        let certificate = Option::<Vec<u8>>::decode(d)?
            .map(|v| v.try_into().map_err(|_| invalid()))
            .transpose()?;
        let value = Self {
            generation,
            catalog,
            certificate,
        };
        value.validate()?;
        Ok(value)
    }
}
impl WireValue for PreparationLease {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        self.validate()?;
        self.token.encode(e)?;
        self.base.encode(e)?;
        e.write_u8(self.format.bytes() as u8)?;
        e.write_i64(self.observed_at_ms)?;
        e.write_i64(self.expires_at_ms)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        let token = PreparationToken::decode(d)?;
        let base = GenerationFact::decode(d)?;
        let format = match d.read_u8()? {
            20 => ObjectFormat::Sha1,
            32 => ObjectFormat::Sha256,
            _ => return Err(invalid()),
        };
        let value = Self {
            token,
            base,
            format,
            observed_at_ms: d.read_i64()?,
            expires_at_ms: d.read_i64()?,
        };
        value.validate()?;
        Ok(value)
    }
}
impl PreparationLease {
    fn validate(self) -> Result<(), CodecError> {
        self.base.validate()?;
        if self.observed_at_ms < 0
            || self.expires_at_ms <= self.observed_at_ms
            || self.base.catalog.is_some_and(|catalog| {
                catalog.repository != self.token.repository || catalog.format != self.format
            })
        {
            return Err(invalid());
        }
        Ok(())
    }
}
impl PreparationFrontier {
    pub(super) fn validate(self) -> Result<(), CodecError> {
        self.lease.validate()?;
        self.current.validate()?;
        if self.current.generation < self.lease.base.generation
            || (self.current.generation == self.lease.base.generation
                && self.current != self.lease.base)
            || self.current.catalog.is_some_and(|catalog| {
                catalog.repository != self.lease.token.repository
                    || catalog.format != self.lease.format
            })
        {
            return Err(invalid());
        }
        Ok(())
    }
}
impl WireValue for PreparationFrontier {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        self.validate()?;
        self.lease.encode(e)?;
        self.current.encode(e)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        let value = Self {
            lease: PreparationLease::decode(d)?,
            current: GenerationFact::decode(d)?,
        };
        value.validate()?;
        Ok(value)
    }
}
impl WireValue for PreparationReply {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        match self {
            Self::Granted(lease) => {
                e.write_u8(0)?;
                lease.encode(e)
            }
            Self::Denied(reason) => e.write_u8(match reason {
                PreparationDenial::Unauthorized => 1,
                PreparationDenial::Conflict => 2,
                PreparationDenial::Stale => 3,
                PreparationDenial::Expired => 4,
                PreparationDenial::Capacity => 5,
                PreparationDenial::Missing => 6,
            }),
        }
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        Ok(match d.read_u8()? {
            0 => Self::Granted(Box::new(PreparationLease::decode(d)?)),
            1 => Self::Denied(PreparationDenial::Unauthorized),
            2 => Self::Denied(PreparationDenial::Conflict),
            3 => Self::Denied(PreparationDenial::Stale),
            4 => Self::Denied(PreparationDenial::Expired),
            5 => Self::Denied(PreparationDenial::Capacity),
            6 => Self::Denied(PreparationDenial::Missing),
            _ => return Err(invalid()),
        })
    }
}
impl WireValue for StagingLease {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        if self.observed_at_ms < 0 || self.expires_at_ms <= self.observed_at_ms {
            return Err(invalid());
        }
        self.token.encode(e)?;
        e.write_u8(self.format.bytes() as u8)?;
        e.write_i64(self.observed_at_ms)?;
        e.write_i64(self.expires_at_ms)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        let token = PreparationToken::decode(d)?;
        let format = match d.read_u8()? {
            20 => ObjectFormat::Sha1,
            32 => ObjectFormat::Sha256,
            _ => return Err(invalid()),
        };
        let value = Self {
            token,
            format,
            observed_at_ms: d.read_i64()?,
            expires_at_ms: d.read_i64()?,
        };
        if value.observed_at_ms < 0 || value.expires_at_ms <= value.observed_at_ms {
            return Err(invalid());
        }
        Ok(value)
    }
}
impl WireValue for StagingReply {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        match self {
            Self::Granted(lease) => {
                e.write_u8(0)?;
                lease.encode(e)
            }
            Self::Denied(reason) => PreparationReply::Denied(*reason).encode(e),
        }
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        Ok(match d.read_u8()? {
            0 => Self::Granted(Box::new(StagingLease::decode(d)?)),
            1 => Self::Denied(PreparationDenial::Unauthorized),
            2 => Self::Denied(PreparationDenial::Conflict),
            3 => Self::Denied(PreparationDenial::Stale),
            4 => Self::Denied(PreparationDenial::Expired),
            5 => Self::Denied(PreparationDenial::Capacity),
            6 => Self::Denied(PreparationDenial::Missing),
            _ => return Err(invalid()),
        })
    }
}
impl WireValue for BeginRequest {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        repo_valid(self.repository)?;
        actor_valid(&self.actor)?;
        lease_valid(self.lease_ms)?;
        if self.operation == [0; 16] {
            return Err(invalid());
        }
        e.write_bytes(&self.repository)?;
        e.write_bytes(&self.operation)?;
        e.write_bytes(&self.request_digest)?;
        e.write_text(&self.actor)?;
        e.write_u64(self.lease_ms)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        let value = Self {
            repository: fixed(d)?,
            operation: fixed(d)?,
            request_digest: fixed(d)?,
            actor: d.read_text()?.into(),
            lease_ms: d.read_u64()?,
        };
        repo_valid(value.repository)?;
        actor_valid(&value.actor)?;
        lease_valid(value.lease_ms)?;
        if value.operation == [0; 16] {
            return Err(invalid());
        }
        Ok(value)
    }
}
impl WireValue for LeaseCheck {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        actor_valid(&self.actor)?;
        self.token.encode(e)?;
        e.write_text(&self.actor)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        let value = Self {
            token: PreparationToken::decode(d)?,
            actor: d.read_text()?.into(),
        };
        actor_valid(&value.actor)?;
        Ok(value)
    }
}
impl WireValue for LeaseRequest {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        lease_valid(self.lease_ms)?;
        self.check.encode(e)?;
        e.write_u64(self.lease_ms)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        let value = Self {
            check: LeaseCheck::decode(d)?,
            lease_ms: d.read_u64()?,
        };
        lease_valid(value.lease_ms)?;
        Ok(value)
    }
}
impl WireValue for MaintenanceRequest {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        repo_valid(self.repository)?;
        actor_valid(&self.actor)?;
        e.write_bytes(&self.repository)?;
        e.write_text(&self.actor)?;
        owner_encode(self.owner, e)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        let value = Self {
            repository: fixed(d)?,
            actor: d.read_text()?.into(),
            owner: owner_decode(d)?,
        };
        repo_valid(value.repository)?;
        actor_valid(&value.actor)?;
        Ok(value)
    }
}
