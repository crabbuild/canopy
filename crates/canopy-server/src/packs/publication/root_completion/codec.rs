use super::*;
use crate::packs::directory::index::codec::fixed;
const DOMAIN: &[u8] = b"canopy.root-push-completion.v1\0";

impl WireValue for NativeOutcomeRoot {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        self.0.validate(INPUT_ROOT_BYTES)?;
        self.0.encode(e)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        let root = StoredInputRoot::decode(d)?;
        root.validate(INPUT_ROOT_BYTES)?;
        Ok(Self(root))
    }
}
impl WireValue for RootPushOutcomes {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        crate::validate_repository_id(self.response_id)
            .map_err(|_| CodecError::Invalid("invalid root response ID"))?;
        if self.ref_generation > i64::MAX as u64 {
            return Err(CodecError::Invalid("invalid completion ref generation"));
        }
        e.write_bytes(&self.response_id)?;
        e.write_u64(self.ref_generation)?;
        self.native.encode(e)?;
        self.rejected.encode(e)?;
        self.replayed.encode(e)?;
        e.write_bool(self.signed.is_some())?;
        if let Some(signed) = &self.signed {
            if signed.size == 0
                || signed.size > crate::push::MAX_RESPONSE_BYTES as u64
                || signed.key.is_empty()
                || signed.key.len() > 4096
                || signed.key.chars().any(char::is_control)
            {
                return Err(CodecError::Invalid("invalid root signed facts"));
            }
            e.write_bytes(&signed.digest)?;
            e.write_text(&signed.key)?;
            e.write_u64(signed.size)?;
        }
        Ok(())
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        let response_id = fixed(d)?;
        let ref_generation = d.read_u64()?;
        let native = NativeOutcomeRoot::decode(d)?;
        let rejected = NativeOutcomeRoot::decode(d)?;
        let replayed = NativeOutcomeRoot::decode(d)?;
        let signed = if d.read_bool()? {
            Some(RootSignedPushFact {
                digest: fixed(d)?,
                key: d.read_text()?.into(),
                size: d.read_u64()?,
            })
        } else {
            None
        };
        let value = Self {
            response_id,
            ref_generation,
            native,
            rejected,
            replayed,
            signed,
        };
        value.encode(&mut BoundedEncoder::new(ROOT_COMPLETION_BYTES)?)?;
        Ok(value)
    }
}
impl WireValue for RootPushCompletion {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        self.shape()?;
        e.write_bytes(DOMAIN)?;
        self.proof.encode(e)?;
        self.outcomes.encode(e)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        if d.read_bytes()? != DOMAIN {
            return Err(CodecError::Invalid("root completion domain"));
        }
        let value = Self {
            proof: RefRootPublicationProof::decode(d)?,
            outcomes: RootPushOutcomes::decode(d)?,
        };
        value.shape()?;
        Ok(value)
    }
}
impl WireValue for RootCompletionReply {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        match self {
            Self::Completed(value) => {
                CatalogCompletionReply::Completed(value.completion).encode(e)?;
                value.root.encode(e)
            }
            Self::Denied(reason) => CatalogCompletionReply::Denied(*reason).encode(e),
        }
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        Ok(match CatalogCompletionReply::decode(d)? {
            CatalogCompletionReply::Completed(completion) => {
                Self::Completed(Box::new(CompletedRootPush {
                    completion,
                    root: NativeOutcomeRoot::decode(d)?,
                }))
            }
            CatalogCompletionReply::Denied(reason) => Self::Denied(reason),
        })
    }
}
