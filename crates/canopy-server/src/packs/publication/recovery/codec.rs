use super::*;
impl WireValue for Record {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        self.root.validate(ROOT_BYTES)?;
        if self.root.operation != self.check.token.artifact_operation {
            return Err(CodecError::Invalid("root recovery namespace"));
        }
        e.write_bytes(DOMAIN)?;
        self.check.encode(e)?;
        e.write_bytes(&self.tenant)?;
        e.write_bytes(&self.application)?;
        e.write_u8(match self.kind {
            Kind::Publish => 0,
            Kind::Outcome => 1,
        })?;
        self.root.encode(e)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        if d.read_bytes()? != DOMAIN {
            return Err(CodecError::Invalid("root recovery purpose"));
        }
        let value = Self {
            check: LeaseCheck::decode(d)?,
            tenant: wire_fixed(d)?,
            application: wire_fixed(d)?,
            kind: match d.read_u8()? {
                0 => Kind::Publish,
                1 => Kind::Outcome,
                _ => return Err(CodecError::Invalid("root recovery command")),
            },
            root: StoredInputRoot::decode(d)?,
        };
        value.encode(&mut BoundedEncoder::new(CERTIFICATE_BYTES)?)?;
        Ok(value)
    }
}
impl WireValue for Bundle {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        if self.body.size == 0
            || self.body.size > u64::from(ROOT_COMPLETION_BYTES)
            || self.body.manifest_digest == [0; 32]
        {
            return Err(CodecError::Invalid("root recovery body bounds"));
        }
        e.write_bytes(DOMAIN)?;
        e.write_bytes(&self.snapshot.to_bytes()?)?;
        artifact(e, self.body)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        if d.read_bytes()? != DOMAIN {
            return Err(CodecError::Invalid("root recovery bundle purpose"));
        }
        let value = Self {
            snapshot: PreparedCommandSnapshot::from_bytes(d.read_bytes()?)?,
            body: read_artifact(d)?,
        };
        value.encode(&mut BoundedEncoder::new(ROOT_BYTES)?)?;
        Ok(value)
    }
}
impl WireValue for RootRecoveryCertificate {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        self.0.data::<Record>()?;
        self.0.encode(e)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        let value = Self(CertificateEnvelope::decode(d)?);
        value.0.data::<Record>()?;
        Ok(value)
    }
}
impl WireValue for RootRecoveryReply {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        match self {
            Self::Registered => e.write_u8(0),
            Self::Denied(reason) => {
                e.write_u8(1)?;
                PreparationReply::Denied(*reason).encode(e)
            }
        }
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        match d.read_u8()? {
            0 => Ok(Self::Registered),
            1 => match PreparationReply::decode(d)? {
                PreparationReply::Denied(reason) => Ok(Self::Denied(reason)),
                _ => Err(CodecError::Invalid("root recovery denial")),
            },
            _ => Err(CodecError::Invalid("root recovery reply")),
        }
    }
}
