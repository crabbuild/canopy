use super::*;

impl RefPolicyIntent {
    pub(super) fn shape(self) -> Result<(), CodecError> {
        crate::validate_repository_id(self.id)
            .map_err(|_| CodecError::Invalid("invalid ref policy guard ID"))?;
        if self.epoch > i64::MAX as u64
            || !(1..=crate::refs::MAX_UPDATES as u64).contains(&self.updates)
        {
            return Err(CodecError::Invalid("invalid ref policy intent"));
        }
        Ok(())
    }
}
impl RefPolicyPage {
    pub(super) fn shape(&self) -> Result<(), CodecError> {
        self.intent.shape()?;
        let n = self.proof.plan.updates.len() as u64;
        if n == 0
            || n > REF_POLICY_PAGE_UPDATES as u64
            || self
                .offset
                .checked_add(n)
                .is_none_or(|end| end > self.intent.updates)
        {
            return Err(CodecError::Invalid("invalid ref policy page"));
        }
        super::super::ref_proof::binding(&self.proof.plan, &self.proof.ancestry)?;
        Ok(())
    }
}
impl WireValue for RefPolicyIntent {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        self.shape()?;
        e.write_bytes(&self.id)?;
        e.write_u64(self.epoch)?;
        e.write_u64(self.updates)?;
        e.write_bytes(&self.plan_digest)?;
        e.write_bytes(&self.evidence_digest)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        let value = Self {
            id: crate::packs::directory::index::codec::fixed(d)?,
            epoch: d.read_u64()?,
            updates: d.read_u64()?,
            plan_digest: crate::packs::directory::index::codec::fixed(d)?,
            evidence_digest: crate::packs::directory::index::codec::fixed(d)?,
        };
        value.shape()?;
        Ok(value)
    }
}
impl WireValue for RefPolicyPage {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        self.shape()?;
        self.intent.encode(e)?;
        e.write_u64(self.offset)?;
        self.proof.encode(e)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        let value = Self {
            intent: RefPolicyIntent::decode(d)?,
            offset: d.read_u64()?,
            proof: RefPublicationProof::decode(d)?,
        };
        value.shape()?;
        Ok(value)
    }
}
impl WireValue for RefPolicyProgress {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        if self.next > self.total || !(1..=crate::refs::MAX_UPDATES as u64).contains(&self.total) {
            return Err(CodecError::Invalid("invalid ref policy progress"));
        }
        e.write_u64(self.next)?;
        e.write_u64(self.total)?;
        e.write_bool(self.valid)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        let value = Self {
            next: d.read_u64()?,
            total: d.read_u64()?,
            valid: d.read_bool()?,
        };
        value.encode(&mut BoundedEncoder::new(32)?)?;
        Ok(value)
    }
}
fn denial(tag: u8) -> Result<PreparationDenial, CodecError> {
    match tag {
        1 => Ok(PreparationDenial::Unauthorized),
        2 => Ok(PreparationDenial::Conflict),
        3 => Ok(PreparationDenial::Stale),
        4 => Ok(PreparationDenial::Expired),
        5 => Ok(PreparationDenial::Capacity),
        6 => Ok(PreparationDenial::Missing),
        _ => Err(CodecError::Invalid("invalid ref policy denial")),
    }
}
impl WireValue for RefPolicyReply {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        match self {
            Self::Registered(value) => {
                e.write_u8(0)?;
                value.encode(e)
            }
            Self::Denied(reason) => PreparationReply::Denied(*reason).encode(e),
        }
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        let tag = d.read_u8()?;
        if tag == 0 {
            Ok(Self::Registered(RefPolicyProgress::decode(d)?))
        } else {
            Ok(Self::Denied(denial(tag)?))
        }
    }
}
impl WireValue for RefPolicyLookup {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        self.check.encode(e)?;
        self.intent.encode(e)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        Ok(Self {
            check: LeaseCheck::decode(d)?,
            intent: RefPolicyIntent::decode(d)?,
        })
    }
}
impl WireValue for RefPolicyReap {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        self.maintenance.encode(e)?;
        crate::validate_repository_id(self.id)
            .map_err(|_| CodecError::Invalid("invalid ref policy guard ID"))?;
        e.write_bytes(&self.id)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        let value = Self {
            maintenance: MaintenanceRequest::decode(d)?,
            id: crate::packs::directory::index::codec::fixed(d)?,
        };
        value.encode(&mut BoundedEncoder::new(512)?)?;
        Ok(value)
    }
}
impl WireValue for RefPolicyReapReply {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        match self {
            Self::Reaped { watches, removed } => {
                if *watches > WATCH_REAP_ROWS {
                    return Err(CodecError::Invalid("invalid watch reap count"));
                }
                e.write_u8(0)?;
                e.write_u64(*watches)?;
                e.write_bool(*removed)
            }
            Self::Denied(reason) => PreparationReply::Denied(*reason).encode(e),
        }
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        let tag = d.read_u8()?;
        if tag == 0 {
            let value = Self::Reaped {
                watches: d.read_u64()?,
                removed: d.read_bool()?,
            };
            value.encode(&mut BoundedEncoder::new(32)?)?;
            Ok(value)
        } else {
            Ok(Self::Denied(denial(tag)?))
        }
    }
}
impl WireValue for RefRootPublicationProof {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        self.shape()?;
        self.certificate.encode(e)?;
        self.guard.encode(e)?;
        self.snapshot.encode(e)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        let value = Self {
            certificate: CatalogCertificate::decode(d)?,
            guard: RefPolicyIntent::decode(d)?,
            snapshot: RefStateSnapshotRoot::decode(d)?,
        };
        value.shape()?;
        Ok(value)
    }
}
impl RefRootPublicationProof {
    pub(in crate::packs::publication) fn shape(&self) -> Result<(), CodecError> {
        let data = self.certificate.data()?;
        if data.compaction
            || data.base.refs.is_none()
            || data.token.artifact_operation != self.snapshot.operation()
            || data.refs_digest != Some(root_binding(self.guard, self.snapshot)?)
        {
            return Err(CodecError::Invalid("invalid guarded root proof"));
        }
        Ok(())
    }
}
