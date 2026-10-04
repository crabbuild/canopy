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
            Self::AcquireServing(r) => {
                e.write_u8(7)?;
                r.encode(e)
            }
            Self::RenewServing {
                request,
                request_digest,
            } => {
                if request.check.actor.is_none() {
                    return Err(CodecError::Invalid(
                        "serving custody needs an account owner",
                    ));
                }
                e.write_u8(8)?;
                request.encode(e)?;
                e.write_bytes(request_digest)
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
            7 => Self::AcquireServing(BeginRequest::decode(d)?),
            8 => {
                let request = RenewServingRequest::decode(d)?;
                if request.check.actor.is_none() {
                    return Err(CodecError::Invalid(
                        "serving custody needs an account owner",
                    ));
                }
                Self::RenewServing {
                    request,
                    request_digest: wire_fixed(d)?,
                }
            }
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
            Self::Serving(reply) => {
                e.write_u8(2)?;
                reply.encode(e)
            }
        }
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        match d.read_u8()? {
            0 => Ok(Self::Preparation(PreparationReply::decode(d)?)),
            1 => Ok(Self::Staging(StagingReply::decode(d)?)),
            2 => Ok(Self::Serving(ServingReply::decode(d)?)),
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
        e.write_u8(self.purpose.number())?;
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
            purpose: CustodyPurpose::parse(d.read_u8()?)?,
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
        || (request.action.purpose() == CustodyPurpose::Serving)
            != matches!(reply, CustodyReply::Serving(_))
    {
        return Err(CodecError::Invalid("custody result purpose differs"));
    }
    let token = match reply {
        CustodyReply::Preparation(PreparationReply::Granted(lease)) => {
            lease.base.validate()?;
            Some(lease.token)
        }
        CustodyReply::Staging(StagingReply::Granted(lease)) => Some(lease.token),
        CustodyReply::Serving(ServingReply::Granted(lease)) => {
            lease.validate()?;
            let (repository, reader, _, _) = request.action.identity();
            if lease.token.repository != repository
                || lease.token.reader != reader
                || lease.token.admission_sequence > phase.sequence()
            {
                return Err(CodecError::Invalid("serving custody grant binding differs"));
            }
            match &request.action {
                CustodyAction::AcquireServing(_)
                    if lease.token.admission_sequence == phase.sequence() => {}
                CustodyAction::RenewServing { request, .. }
                    if request.check.token == lease.token => {}
                _ => return Err(CodecError::Invalid("serving custody original differs")),
            }
            None
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serving_wire_requires_account_exact_framing_and_v2_purpose() -> Result<(), CodecError> {
        let begin = BeginRequest {
            repository: *uuid::Uuid::new_v4().as_bytes(),
            operation: [2; 16],
            request_digest: [3; 32],
            actor: "viewer".into(),
            lease_ms: DEFAULT_LEASE_MS,
        };
        let token = ServingToken {
            repository: begin.repository,
            reader: begin.operation,
            owner: OwnerFence {
                incarnation: IncarnationId::from_bytes([4; 16]),
                epoch: 1,
            },
            admission_sequence: 5,
            generation: 1,
        };
        let renew = RenewServingRequest {
            check: ServingCheck {
                token,
                actor: Some("viewer".into()),
            },
            lease_ms: DEFAULT_LEASE_MS,
        };
        for action in [
            CustodyAction::AcquireServing(begin.clone()),
            CustodyAction::RenewServing {
                request: renew.clone(),
                request_digest: begin.request_digest,
            },
        ] {
            let request = CustodyRequest {
                step: 0,
                previous: None,
                action,
            };
            let bytes = encode(&request, INPUT_BYTES)?;
            assert_eq!(decode::<CustodyRequest>(&bytes, INPUT_BYTES)?, request);
            for length in 0..bytes.len() {
                assert!(decode::<CustodyRequest>(&bytes[..length], INPUT_BYTES).is_err());
            }
            let mut trailing = bytes.clone();
            trailing.push(0);
            assert!(decode::<CustodyRequest>(&trailing, INPUT_BYTES).is_err());
            let mut old = bytes;
            let version = old
                .windows(3)
                .position(|part| part == b"v2\0")
                .expect("domain version");
            old[version + 1] = b'1';
            assert!(decode::<CustodyRequest>(&old, INPUT_BYTES).is_err());
            for reply in [
                CustodyReply::Preparation(PreparationReply::Denied(
                    PreparationDenial::Unauthorized,
                )),
                CustodyReply::Staging(StagingReply::Denied(PreparationDenial::Unauthorized)),
            ] {
                let phase = Recorded::new(6, true, encode(&reply, 512)?)?;
                assert!(validate_phase(&phase, &request).is_err());
            }
            let reply = CustodyReply::Serving(ServingReply::Denied(ServingDenial::Unauthorized));
            let bytes = encode(&reply, 512)?;
            assert!(validate_phase(&Recorded::new(6, false, bytes.clone())?, &request).is_err());
            validate_phase(&Recorded::new(6, true, bytes)?, &request)?;
        }
        let mut anonymous = renew;
        anonymous.check.actor = None;
        assert!(
            encode(
                &CustodyAction::RenewServing {
                    request: anonymous,
                    request_digest: [3; 32]
                },
                INPUT_BYTES
            )
            .is_err()
        );
        assert!(CustodyPurpose::parse(2).is_err());
        Ok(())
    }
}
