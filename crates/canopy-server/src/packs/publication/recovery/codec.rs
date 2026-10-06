use super::*;
impl WireValue for Record {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        self.root.validate(ROOT_BYTES)?;
        if self.root.operation != self.check.token.artifact_operation {
            return Err(CodecError::Invalid("root recovery namespace"));
        }
        if (self.kind == Kind::Policy && self.refusal.is_none())
            || (self.refusal.is_some() && !matches!(self.kind, Kind::Policy | Kind::Publish))
        {
            return Err(CodecError::Invalid(
                "policy recovery requires frozen refusal",
            ));
        }
        if self.step > 65_535 || (self.step == 0) != self.previous.is_none() {
            return Err(CodecError::Invalid("invalid recovery predecessor step"));
        }
        if let Some(previous) = self.previous {
            previous.validate(ROOT_BYTES)?;
            if previous.operation != self.root.operation {
                return Err(CodecError::Invalid("foreign predecessor frame"));
            }
        }
        e.write_bytes(DOMAIN)?;
        self.check.encode(e)?;
        e.write_bytes(&self.tenant)?;
        e.write_bytes(&self.application)?;
        e.write_u8(match self.kind {
            Kind::Publish => 0,
            Kind::Outcome => 1,
            Kind::Policy => 2,
            Kind::Initialization => 3,
            Kind::Merge => 4,
            Kind::Head => 5,
            Kind::Candidate => 6,
        })?;
        self.primary.encode(e)?;
        e.write_bool(self.refusal.is_some())?;
        if let Some(refusal) = self.refusal {
            refusal.encode(e)?;
        }
        e.write_bool(self.previous.is_some())?;
        if let Some(previous) = self.previous {
            previous.encode(e)?;
        }
        e.write_u64(self.step)?;
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
                2 => Kind::Policy,
                3 => Kind::Initialization,
                4 => Kind::Merge,
                5 => Kind::Head,
                6 => Kind::Candidate,
                _ => return Err(CodecError::Invalid("root recovery command")),
            },
            primary: Stamp::decode(d)?,
            refusal: if d.read_bool()? {
                Some(Stamp::decode(d)?)
            } else {
                None
            },
            previous: if d.read_bool()? {
                Some(StoredInputRoot::decode(d)?)
            } else {
                None
            },
            step: d.read_u64()?,
            root: StoredInputRoot::decode(d)?,
        };
        value.encode(&mut BoundedEncoder::new(CERTIFICATE_BYTES)?)?;
        Ok(value)
    }
}
impl WireValue for Bundle {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        self.primary.validate(self.kind.body_limit())?;
        if (self.kind == Kind::Policy && self.refusal.is_none())
            || (self.refusal.is_some() && !matches!(self.kind, Kind::Policy | Kind::Publish))
        {
            return Err(CodecError::Invalid("missing frozen refusal"));
        }
        if let Some(refusal) = &self.refusal {
            refusal.validate(ROOT_COMPLETION_BYTES)?;
        }
        e.write_bytes(DOMAIN)?;
        e.write_u8(match self.kind {
            Kind::Publish => 0,
            Kind::Outcome => 1,
            Kind::Policy => 2,
            Kind::Initialization => 3,
            Kind::Merge => 4,
            Kind::Head => 5,
            Kind::Candidate => 6,
        })?;
        self.primary.encode(e)?;
        e.write_bool(self.refusal.is_some())?;
        if let Some(refusal) = &self.refusal {
            refusal.encode(e)?;
        }
        Ok(())
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        if d.read_bytes()? != DOMAIN {
            return Err(CodecError::Invalid("root recovery bundle purpose"));
        }
        let value = Self {
            kind: match d.read_u8()? {
                0 => Kind::Publish,
                1 => Kind::Outcome,
                2 => Kind::Policy,
                3 => Kind::Initialization,
                4 => Kind::Merge,
                5 => Kind::Head,
                6 => Kind::Candidate,
                _ => return Err(CodecError::Invalid("unknown recovery kind")),
            },
            primary: SavedCommand::decode(d)?,
            refusal: if d.read_bool()? {
                Some(SavedCommand::decode(d)?)
            } else {
                None
            },
        };
        value.encode(&mut BoundedEncoder::new(ROOT_BYTES)?)?;
        Ok(value)
    }
}
impl SavedCommand {
    fn validate(&self, limit: u32) -> Result<(), CodecError> {
        if self.body.size == 0
            || self.body.size > u64::from(limit)
            || self.body.manifest_digest == [0; 32]
        {
            return Err(CodecError::Invalid("recovery body bounds"));
        }
        Ok(())
    }
}
impl WireValue for SavedCommand {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        self.validate(REF_POLICY_PAGE_BYTES)?;
        e.write_bytes(&self.snapshot.to_bytes()?)?;
        artifact(e, self.body)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        let value = Self {
            snapshot: PreparedCommandSnapshot::from_bytes(d.read_bytes()?)?,
            body: read_artifact(d)?,
        };
        value.validate(REF_POLICY_PAGE_BYTES)?;
        Ok(value)
    }
}
impl WireValue for Stamp {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        if self.identity.issued_at_ms < 0
            || self.identity.expires_at_ms <= self.identity.issued_at_ms
        {
            return Err(CodecError::Invalid("invalid admitted mutation stamp"));
        }
        e.write_bytes(self.identity.request_id.as_bytes())?;
        e.write_u64(self.identity.issued_at_ms as u64)?;
        e.write_u64(self.identity.expires_at_ms as u64)?;
        e.write_bytes(&self.digest)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        let value = Self {
            identity: MutationIdentity {
                request_id: cellule_runtime::identity::RequestId::from_bytes(wire_fixed(d)?),
                issued_at_ms: i64::try_from(d.read_u64()?)
                    .map_err(|_| CodecError::Invalid("mutation issue time"))?,
                expires_at_ms: i64::try_from(d.read_u64()?)
                    .map_err(|_| CodecError::Invalid("mutation expiry time"))?,
            },
            digest: wire_fixed(d)?,
        };
        value.encode(&mut BoundedEncoder::new(128)?)?;
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
