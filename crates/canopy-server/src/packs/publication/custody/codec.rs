use super::*;
use crate::packs::directory::index::codec::fixed as wire_fixed;

impl WireValue for CustodyAction {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        match self {
            Self::BeginPreparation(r) => {
                e.write_u8(0)?;
                r.encode(e)
            }
            Self::ClaimPreparation(r) => {
                e.write_u8(1)?;
                r.encode(e)
            }
            Self::RenewPreparation(r) => {
                e.write_u8(2)?;
                r.encode(e)
            }
            Self::BeginStaging(r) => {
                e.write_u8(3)?;
                r.encode(e)
            }
            Self::ClaimStaging(r) => {
                e.write_u8(4)?;
                r.encode(e)
            }
            Self::RenewStaging(r) => {
                e.write_u8(5)?;
                r.encode(e)
            }
            Self::BindStaging(r) => {
                e.write_u8(6)?;
                r.encode(e)
            }
        }
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        Ok(match d.read_u8()? {
            0 => Self::BeginPreparation(BeginRequest::decode(d)?),
            1 => Self::ClaimPreparation(LeaseRequest::decode(d)?),
            2 => Self::RenewPreparation(LeaseRequest::decode(d)?),
            3 => Self::BeginStaging(BeginRequest::decode(d)?),
            4 => Self::ClaimStaging(LeaseRequest::decode(d)?),
            5 => Self::RenewStaging(LeaseRequest::decode(d)?),
            6 => Self::BindStaging(LeaseCheck::decode(d)?),
            _ => return Err(CodecError::Invalid("custody action purpose")),
        })
    }
}
impl WireValue for CustodyRequest {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        if self.step > MAX_STEPS || (self.step == 0) != self.previous.is_none() {
            return Err(CodecError::Invalid("custody predecessor step"));
        }
        e.write_bytes(DOMAIN)?;
        e.write_u32(self.step)?;
        e.write_bool(self.previous.is_some())?;
        if let Some(previous) = self.previous {
            e.write_bytes(&previous)?;
        }
        self.action.encode(e)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        if d.read_bytes()? != DOMAIN {
            return Err(CodecError::Invalid("custody request purpose"));
        }
        let value = Self {
            step: d.read_u32()?,
            previous: if d.read_bool()? {
                Some(wire_fixed(d)?)
            } else {
                None
            },
            action: CustodyAction::decode(d)?,
        };
        value.encode(&mut BoundedEncoder::new(INPUT_BYTES)?)?;
        Ok(value)
    }
}
impl WireValue for CustodyReply {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        match self {
            Self::Preparation(reply) => {
                e.write_u8(0)?;
                reply.encode(e)
            }
            Self::Staging(reply) => {
                e.write_u8(1)?;
                reply.encode(e)
            }
        }
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        match d.read_u8()? {
            0 => Ok(Self::Preparation(PreparationReply::decode(d)?)),
            1 => Ok(Self::Staging(StagingReply::decode(d)?)),
            _ => Err(CodecError::Invalid("custody reply purpose")),
        }
    }
}
impl WireValue for Header {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        if self.operation == [0; 16]
            || self.step > MAX_STEPS
            || (self.step == 0) != self.previous.is_none()
            || validate_component(&self.actor).is_err()
        {
            return Err(CodecError::Invalid("custody header identity"));
        }
        e.write_bytes(DOMAIN)?;
        e.write_bytes(&self.tenant)?;
        e.write_bytes(&self.application)?;
        e.write_bytes(self.incarnation.as_bytes())?;
        self.stamp.encode(e)?;
        e.write_bytes(&self.repository)?;
        e.write_bytes(&self.operation)?;
        e.write_bytes(&self.request_digest)?;
        e.write_text(&self.actor)?;
        e.write_u32(self.step)?;
        e.write_bool(self.previous.is_some())?;
        if let Some(previous) = self.previous {
            e.write_bytes(&previous)?;
        }
        e.write_bytes(&self.bundle_digest)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        if d.read_bytes()? != DOMAIN {
            return Err(CodecError::Invalid("custody header purpose"));
        }
        let value = Self {
            tenant: wire_fixed(d)?,
            application: wire_fixed(d)?,
            incarnation: IncarnationId::from_bytes(wire_fixed(d)?),
            stamp: Stamp::decode(d)?,
            repository: wire_fixed(d)?,
            operation: wire_fixed(d)?,
            request_digest: wire_fixed(d)?,
            actor: d.read_text()?.into(),
            step: d.read_u32()?,
            previous: if d.read_bool()? {
                Some(wire_fixed(d)?)
            } else {
                None
            },
            bundle_digest: wire_fixed(d)?,
        };
        value.encode(&mut BoundedEncoder::new(CERTIFICATE_BYTES)?)?;
        Ok(value)
    }
}
impl WireValue for CustodyIntent {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        self.header()?;
        self.request()?;
        self.certificate.encode(e)?;
        e.write_bytes(&self.snapshot.to_bytes()?)?;
        e.write_bytes(&self.body)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        let value = Self {
            certificate: CertificateEnvelope::decode(d)?,
            snapshot: PreparedCommandSnapshot::from_bytes(d.read_bytes()?)?,
            body: d.read_bytes()?.to_vec(),
        };
        value.encode(&mut BoundedEncoder::new(INTENT_BYTES)?)?;
        Ok(value)
    }
}

pub(super) fn validate_phase(phase: &Recorded, request: &CustodyRequest) -> Result<(), CodecError> {
    let reply: CustodyReply = phase.decode_reply()?;
    if phase.rejected() != reply.rejected()
        || request.action.staging() != matches!(reply, CustodyReply::Staging(_))
    {
        return Err(CodecError::Invalid("custody result purpose differs"));
    }
    let token = match reply {
        CustodyReply::Preparation(PreparationReply::Granted(lease)) => {
            lease.base.validate()?;
            Some(lease.token)
        }
        CustodyReply::Staging(StagingReply::Granted(lease)) => Some(lease.token),
        _ => None,
    };
    if let Some(token) = token {
        let (repository, operation, digest, _) = request.action.identity();
        if token.repository != repository
            || token.operation != operation
            || token.request_digest != digest
            || token.attempt > phase.sequence()
        {
            return Err(CodecError::Invalid("custody grant binding differs"));
        }
    }
    Ok(())
}
