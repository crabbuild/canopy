use super::*;
use crate::packs::directory::index::codec::{artifact, fixed, read_artifact};
const DOMAIN: &[u8] = b"canopy.native-outcome.v1\0";

impl NativeOutcomeRoot {
    pub(super) async fn upload(
        store: &ArtifactStore,
        operation: [u8; 16],
        record: OutcomeRecord,
    ) -> Result<Self, RootCompletionPreparationError> {
        Ok(Self(
            StoredInputRoot::upload(store, operation, &record, INPUT_ROOT_BYTES).await?,
        ))
    }
}
pub(super) async fn retain_rejection(
    store: &ArtifactStore,
    operation: [u8; 16],
    native: NativeResultRoot,
    response: &GitHttpResponse,
    reason: &str,
) -> Result<NativeOutcomeRoot, RootCompletionPreparationError> {
    let GitHttpResponse {
        status,
        headers,
        body,
    } = crate::push::report::rejected_report(response, reason)?;
    let body = native_result::retain_body(store, operation, body).await?;
    NativeOutcomeRoot::upload(
        store,
        operation,
        OutcomeRecord {
            native,
            body_operation: operation,
            response: GitHttpResponse {
                status,
                headers,
                body,
            },
        },
    )
    .await
}
impl OutcomeRecord {
    fn validate(&self) -> Result<(), CodecError> {
        self.native.validate()?;
        super::super::codec::artifact_valid(self.body_operation)?;
        if !(100..=599).contains(&self.response.status)
            || self.response.body.size > crate::push::MAX_RESPONSE_BYTES as u64
            || self.response.body.manifest_digest == [0; 32]
            || self.response.headers.iter().any(|(name, value)| {
                axum::http::HeaderName::from_bytes(name.as_bytes()).is_err()
                    || axum::http::HeaderValue::from_str(value).is_err()
                    || name.eq_ignore_ascii_case("Content-Length")
                        && value.parse::<u64>().ok() != Some(self.response.body.size)
            })
        {
            return Err(CodecError::Invalid("native outcome metadata"));
        }
        Ok(())
    }
}
impl WireValue for OutcomeRecord {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        self.validate()?;
        e.write_bytes(DOMAIN)?;
        self.native.encode(e)?;
        e.write_bytes(&self.body_operation)?;
        e.write_u32(self.response.status.into())?;
        e.write_count(self.response.headers.len())?;
        for (name, value) in &self.response.headers {
            e.write_text(name)?;
            e.write_text(value)?;
        }
        artifact(e, self.response.body)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        if d.read_bytes()? != DOMAIN {
            return Err(CodecError::Invalid("native outcome domain"));
        }
        let native = NativeResultRoot::decode(d)?;
        let body_operation = fixed(d)?;
        let status = u16::try_from(d.read_u32()?)
            .map_err(|_| CodecError::Invalid("native outcome status"))?;
        let count = d.read_count()?;
        if count > INPUT_ROOT_BYTES as usize / 8 {
            return Err(CodecError::Limit);
        }
        let mut headers = Vec::with_capacity(count);
        for _ in 0..count {
            headers.push((d.read_text()?.into(), d.read_text()?.into()));
        }
        let value = Self {
            native,
            body_operation,
            response: GitHttpResponse {
                status,
                headers,
                body: read_artifact(d)?,
            },
        };
        value.validate()?;
        Ok(value)
    }
}
