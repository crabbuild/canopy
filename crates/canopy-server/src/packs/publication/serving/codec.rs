use super::*;
use crate::packs::directory::index::codec::fixed;
const DOMAIN: &[u8] = b"canopy.serving-workers-drained.v1\0";
fn invalid() -> CodecError {
    CodecError::Invalid("invalid serving pin")
}
fn actor(value: &Option<String>) -> Result<(), CodecError> {
    if let Some(value) = value {
        validate_component(value).map_err(|_| invalid())?;
    }
    Ok(())
}
fn duration(value: u64) -> Result<(), CodecError> {
    if value == 0 || value > MAX_LEASE_MS {
        return Err(invalid());
    }
    Ok(())
}
impl ServingToken {
    pub(in crate::packs::publication) fn validate(&self) -> Result<(), CodecError> {
        crate::validate_repository_id(self.repository).map_err(|_| invalid())?;
        if self.reader == [0; 16]
            || self.owner.epoch == 0
            || self.admission_sequence == 0
            || self.admission_sequence > i64::MAX as u64
            || self.generation == 0
            || self.generation > i64::MAX as u64
        {
            return Err(invalid());
        }
        Ok(())
    }
}
impl WireValue for ServingToken {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        self.validate()?;
        e.write_bytes(&self.repository)?;
        e.write_bytes(&self.reader)?;
        e.write_bytes(self.owner.incarnation.as_bytes())?;
        e.write_u64(self.owner.epoch)?;
        e.write_u64(self.admission_sequence)?;
        e.write_u64(self.generation)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        let value = Self {
            repository: fixed(d)?,
            reader: fixed(d)?,
            owner: OwnerFence {
                incarnation: IncarnationId::from_bytes(fixed(d)?),
                epoch: d.read_u64()?,
            },
            admission_sequence: d.read_u64()?,
            generation: d.read_u64()?,
        };
        value.validate()?;
        Ok(value)
    }
}
impl WireValue for AcquireServingRequest {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        crate::validate_repository_id(self.repository).map_err(|_| invalid())?;
        if self.reader == [0; 16] {
            return Err(invalid());
        }
        actor(&self.actor)?;
        duration(self.lease_ms)?;
        e.write_bytes(&self.repository)?;
        e.write_bytes(&self.reader)?;
        self.actor.encode(e)?;
        e.write_u64(self.lease_ms)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        let value = Self {
            repository: fixed(d)?,
            reader: fixed(d)?,
            actor: Option::<String>::decode(d)?,
            lease_ms: d.read_u64()?,
        };
        value.encode(&mut BoundedEncoder::new(1024)?)?;
        Ok(value)
    }
}
impl WireValue for ServingSelection {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        crate::validate_repository_id(self.repository).map_err(|_| invalid())?;
        actor(&self.actor)?;
        e.write_bytes(&self.repository)?;
        self.actor.encode(e)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        let value = Self {
            repository: fixed(d)?,
            actor: Option::<String>::decode(d)?,
        };
        value.encode(&mut BoundedEncoder::new(1024)?)?;
        Ok(value)
    }
}
impl WireValue for ServingCheck {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        actor(&self.actor)?;
        self.token.encode(e)?;
        self.actor.encode(e)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        let value = Self {
            token: ServingToken::decode(d)?,
            actor: Option::<String>::decode(d)?,
        };
        actor(&value.actor)?;
        Ok(value)
    }
}
impl WireValue for RenewServingRequest {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        duration(self.lease_ms)?;
        self.check.encode(e)?;
        e.write_u64(self.lease_ms)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        let value = Self {
            check: ServingCheck::decode(d)?,
            lease_ms: d.read_u64()?,
        };
        duration(value.lease_ms)?;
        Ok(value)
    }
}
impl ServingLease {
    pub(in crate::packs::publication) fn validate(&self) -> Result<(), CodecError> {
        self.token.validate()?;
        self.fact.validate()?;
        if self.fact.generation != self.token.generation
            || self.fact.refs.is_none()
            || self.fact.catalog.is_none_or(|catalog| {
                catalog.repository != self.token.repository || catalog.format != self.format
            })
            || self.observed_at_ms < 0
            || self.expires_at_ms <= self.observed_at_ms
        {
            return Err(invalid());
        }
        Ok(())
    }
}
impl WireValue for ServingLease {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        self.validate()?;
        self.token.encode(e)?;
        self.fact.encode(e)?;
        e.write_u8(self.format.bytes() as u8)?;
        e.write_i64(self.observed_at_ms)?;
        e.write_i64(self.expires_at_ms)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        let value = Self {
            token: ServingToken::decode(d)?,
            fact: GenerationFact::decode(d)?,
            format: match d.read_u8()? {
                20 => ObjectFormat::Sha1,
                32 => ObjectFormat::Sha256,
                _ => return Err(invalid()),
            },
            observed_at_ms: d.read_i64()?,
            expires_at_ms: d.read_i64()?,
        };
        value.validate()?;
        Ok(value)
    }
}
impl WireValue for ServingDenial {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        e.write_u8(match self {
            Self::Unauthorized => 0,
            Self::Conflict => 1,
            Self::Uninitialized => 2,
            Self::Stale => 3,
            Self::Expired => 4,
            Self::Capacity => 5,
        })
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        Ok(match d.read_u8()? {
            0 => Self::Unauthorized,
            1 => Self::Conflict,
            2 => Self::Uninitialized,
            3 => Self::Stale,
            4 => Self::Expired,
            5 => Self::Capacity,
            _ => return Err(invalid()),
        })
    }
}
impl WireValue for ServingReply {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        match self {
            Self::Granted(value) => {
                e.write_u8(0)?;
                value.encode(e)
            }
            Self::Denied(value) => {
                e.write_u8(1)?;
                value.encode(e)
            }
        }
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        match d.read_u8()? {
            0 => Ok(Self::Granted(Box::new(ServingLease::decode(d)?))),
            1 => Ok(Self::Denied(ServingDenial::decode(d)?)),
            _ => Err(invalid()),
        }
    }
}
impl WireValue for ServingReleaseReply {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        match self {
            Self::Released => e.write_u8(0),
            Self::Denied(value) => {
                e.write_u8(1)?;
                value.encode(e)
            }
        }
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        match d.read_u8()? {
            0 => Ok(Self::Released),
            1 => Ok(Self::Denied(ServingDenial::decode(d)?)),
            _ => Err(invalid()),
        }
    }
}
impl WireValue for DrainData {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        validate_component(&self.administrator).map_err(|_| invalid())?;
        e.write_bytes(DOMAIN)?;
        e.write_bytes(&self.tenant)?;
        e.write_bytes(&self.application)?;
        self.token.encode(e)?;
        e.write_text(&self.administrator)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        if d.read_bytes()? != DOMAIN {
            return Err(invalid());
        }
        let value = Self {
            tenant: fixed(d)?,
            application: fixed(d)?,
            token: ServingToken::decode(d)?,
            administrator: d.read_text()?.into(),
        };
        validate_component(&value.administrator).map_err(|_| invalid())?;
        Ok(value)
    }
}
impl ServingDrainProof {
    pub(super) fn data(&self) -> Result<DrainData, CodecError> {
        let mut decoder = BoundedDecoder::new(&self.0.body, 960)?;
        let data = DrainData::decode(&mut decoder)?;
        decoder.finish()?;
        Ok(data)
    }
}
impl WireValue for ServingDrainProof {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        self.data()?;
        self.0.encode(e)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        let value = Self(super::super::certificate::CertificateEnvelope::decode(d)?);
        value.data()?;
        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    type TestResult = Result<(), Box<dyn std::error::Error>>;
    fn token() -> ServingToken {
        ServingToken {
            repository: *uuid::Uuid::new_v4().as_bytes(),
            reader: [1; 16],
            owner: OwnerFence {
                incarnation: IncarnationId::from_bytes([2; 16]),
                epoch: 1,
            },
            admission_sequence: 1,
            generation: 1,
        }
    }
    fn qualify<T: WireValue + PartialEq + std::fmt::Debug>(value: T) -> TestResult {
        let mut e = BoundedEncoder::new(1024)?;
        value.encode(&mut e)?;
        let bytes = e.finish();
        let mut d = BoundedDecoder::new(&bytes, 1024)?;
        assert_eq!(T::decode(&mut d)?, value);
        d.finish()?;
        for cut in 0..bytes.len() {
            let mut d = BoundedDecoder::new(&bytes[..cut], 1024)?;
            assert!(T::decode(&mut d).is_err());
        }
        let mut trailing = bytes;
        trailing.push(0);
        let mut d = BoundedDecoder::new(&trailing, 1024)?;
        T::decode(&mut d)?;
        assert!(d.finish().is_err());
        Ok(())
    }
    #[test]
    fn serving_codecs_reject_truncation_trailing_bytes_and_invalid_bounds() -> TestResult {
        let token = token();
        qualify(token)?;
        qualify(ServingCheck { token, actor: None })?;
        qualify(AcquireServingRequest {
            repository: token.repository,
            reader: token.reader,
            actor: Some("reader".into()),
            lease_ms: MAX_LEASE_MS,
        })?;
        qualify(RenewServingRequest {
            check: ServingCheck {
                token,
                actor: Some("reader".into()),
            },
            lease_ms: 1,
        })?;
        for denial in [
            ServingDenial::Unauthorized,
            ServingDenial::Conflict,
            ServingDenial::Uninitialized,
            ServingDenial::Stale,
            ServingDenial::Expired,
            ServingDenial::Capacity,
        ] {
            qualify(ServingReply::Denied(denial))?;
            qualify(ServingReleaseReply::Denied(denial))?;
        }
        qualify(ServingReleaseReply::Released)?;
        for lease_ms in [0, MAX_LEASE_MS + 1, u64::MAX] {
            assert!(
                AcquireServingRequest {
                    repository: token.repository,
                    reader: token.reader,
                    actor: None,
                    lease_ms
                }
                .encode(&mut BoundedEncoder::new(1024)?)
                .is_err()
            );
        }
        for field in 0..5 {
            let mut invalid = token;
            match field {
                0 => invalid.reader = [0; 16],
                1 => invalid.owner.epoch = 0,
                2 => invalid.admission_sequence = 0,
                3 => invalid.generation = 0,
                _ => invalid.generation = u64::MAX,
            }
            assert!(invalid.encode(&mut BoundedEncoder::new(1024)?).is_err());
        }
        Ok(())
    }
    #[test]
    fn drain_proof_mac_and_domain_bind_scope_and_exact_pin() -> TestResult {
        let data = DrainData {
            tenant: [3; 16],
            application: [4; 16],
            token: token(),
            administrator: "owner".into(),
        };
        let seed = [5; 32];
        let proof = ServingDrainProof(super::super::super::certificate::CertificateEnvelope::seal(
            &data, &seed,
        )?);
        qualify(proof.clone())?;
        assert_eq!(proof.data()?, data);
        assert!(proof.0.authenticated(&seed));
        assert!(!proof.0.authenticated(&[6; 32]));
        let mut tampered = proof.clone();
        let last = tampered.0.body.len() - 1;
        tampered.0.body[last] ^= 1;
        assert!(tampered.data().is_ok());
        assert!(!tampered.0.authenticated(&seed));
        let mut other_domain = proof;
        other_domain.0.body[4] ^= 1;
        assert!(other_domain.data().is_err());
        Ok(())
    }
}
