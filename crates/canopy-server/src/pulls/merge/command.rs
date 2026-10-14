use super::*;
use cellule_runtime::{
    codec::BoundedDecoder, codec::BoundedEncoder, codec::CodecError, codec::WireValue,
};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct MergeInput {
    pub actor: String,
    pub number: i64,
    pub request: MergeRequest,
    pub issued_at_ms: i64,
}
impl WireValue for MergeInput {
    fn encode(&self, out: &mut BoundedEncoder) -> Result<(), CodecError> {
        if validate_component(&self.actor).is_err()
            || self.number < 1
            || self.issued_at_ms < 0
            || !valid_request(&self.request)
        {
            return Err(CodecError::Invalid("invalid merge input"));
        }
        out.write_bytes(
            &serde_json::to_vec(self).map_err(|_| CodecError::Invalid("merge input JSON"))?,
        )
    }
    fn decode(input: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        let bytes = input.read_bytes()?;
        if bytes.len() > 4096 {
            return Err(CodecError::Invalid("merge input limit"));
        }
        let value: Self =
            serde_json::from_slice(bytes).map_err(|_| CodecError::Invalid("merge input JSON"))?;
        if validate_component(&value.actor).is_err()
            || value.number < 1
            || value.issued_at_ms < 0
            || !valid_request(&value.request)
        {
            return Err(CodecError::Invalid("invalid merge input"));
        }
        Ok(value)
    }
}
impl WireValue for MergeOutcome {
    fn encode(&self, out: &mut BoundedEncoder) -> Result<(), CodecError> {
        out.write_bytes(
            &serde_json::to_vec(self).map_err(|_| CodecError::Invalid("merge output JSON"))?,
        )
    }
    fn decode(input: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        serde_json::from_slice(input.read_bytes()?)
            .map_err(|_| CodecError::Invalid("merge output JSON"))
    }
}
pub(crate) fn request_binding(input: &MergeInput) -> Vec<u8> {
    super::super::mutations::binding(&[
        &input.actor,
        &input.number.to_string(),
        &input.request.revision.pull_version.to_string(),
        &input.request.revision.source_oid,
        &input.request.revision.source_version.to_string(),
        &input.request.revision.base_oid,
        &input.request.revision.base_version.to_string(),
        match input.request.strategy {
            MergeStrategy::FastForward => "fast_forward",
            MergeStrategy::MergeCommit => "merge_commit",
            MergeStrategy::Squash => "squash",
            MergeStrategy::Rebase => "rebase",
        },
        input.request.candidate_id.as_deref().unwrap_or(""),
    ])
}
