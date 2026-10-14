use super::*;
use crate::packs::directory::index::codec::{artifact, fixed, read_artifact};
const DOMAIN: &[u8] = b"canopy.native-result.v1\0";
impl WireValue for NativeResultRoot {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        self.validate()?;
        self.0.encode(e)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        let value = Self(StoredInputRoot::decode(d)?);
        value.validate()?;
        Ok(value)
    }
}
impl ResultRecord {
    fn validate(&self) -> Result<(), CodecError> {
        super::super::codec::artifact_valid(self.operation)?;
        self.request.validate()?;
        if !(100..=599).contains(&self.response.status)
            || self.response.body.size > crate::push::MAX_RESPONSE_BYTES as u64
            || self.response.body.manifest_digest == [0; 32]
            || !crate::push::valid_options(&self.options)
            || self.plan.is_some_and(|plan| {
                plan.size == 0
                    || plan.size > super::plan::MAX_PLAN_BYTES
                    || plan.manifest_digest == [0; 32]
            })
            || self.signed.as_ref().is_some_and(|signed| {
                signed.body.size == 0
                    || signed.body.size > crate::push::MAX_RESPONSE_BYTES as u64
                    || signed.body.manifest_digest == [0; 32]
                    || validate_component(&signed.signer).is_err()
                    || signed.key.is_empty()
                    || signed.key.len() > 4096
                    || signed.key.chars().any(char::is_control)
            })
        {
            return Err(CodecError::Invalid("native result metadata"));
        }
        Ok(())
    }
}
impl WireValue for ResultRecord {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        self.validate()?;
        e.write_bytes(DOMAIN)?;
        e.write_bytes(&self.operation)?;
        self.request.encode(e)?;
        e.write_u32(self.response.status.into())?;
        e.write_count(self.response.headers.len())?;
        for (name, value) in &self.response.headers {
            e.write_text(name)?;
            e.write_text(value)?;
        }
        artifact(e, self.response.body)?;
        e.write_bool(self.plan.is_some())?;
        if let Some(plan) = self.plan {
            artifact(e, plan)?;
        }
        e.write_count(self.options.len())?;
        for option in &self.options {
            e.write_text(option)?;
        }
        e.write_bool(self.signed.is_some())?;
        if let Some(signed) = &self.signed {
            artifact(e, signed.body)?;
            e.write_text(&signed.signer)?;
            e.write_text(&signed.key)?;
        }
        Ok(())
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        if d.read_bytes()? != DOMAIN {
            return Err(CodecError::Invalid("native result domain"));
        }
        let operation = fixed(d)?;
        let request = WireRequestRoot::decode(d)?;
        let status = u16::try_from(d.read_u32()?)
            .map_err(|_| CodecError::Invalid("native result status"))?;
        let count = d.read_count()?;
        if count > INPUT_ROOT_BYTES as usize / 8 {
            return Err(CodecError::Limit);
        }
        let mut headers = Vec::with_capacity(count);
        for _ in 0..count {
            headers.push((d.read_text()?.into(), d.read_text()?.into()));
        }
        let response = GitHttpResponse {
            status,
            headers,
            body: read_artifact(d)?,
        };
        let plan = if d.read_bool()? {
            Some(read_artifact(d)?)
        } else {
            None
        };
        let count = d.read_count()?;
        if count > 16 {
            return Err(CodecError::Limit);
        }
        let mut options = Vec::with_capacity(count);
        for _ in 0..count {
            options.push(d.read_text()?.into());
        }
        let signed = if d.read_bool()? {
            Some(SignedPushAnnotation {
                body: read_artifact(d)?,
                signer: d.read_text()?.into(),
                key: d.read_text()?.into(),
            })
        } else {
            None
        };
        let value = Self {
            operation,
            request,
            response,
            plan,
            options,
            signed,
        };
        value.validate()?;
        Ok(value)
    }
}
