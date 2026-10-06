use super::*;
use crate::{
    RepositoryModule,
    access::{access_statement, decode_access},
    directory::TokenScope,
    packs::publication::{REF_SELECTION_BYTES, RefSelection},
};
use cellule_runtime::{CellModule, Command, registry::CommandResult};

pub(crate) const INPUT_BYTES: u32 = REF_SELECTION_BYTES + (256 << 10);
pub(crate) const OUTPUT_BYTES: u32 = 1 << 20;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "action", deny_unknown_fields)]
pub(crate) enum CandidateAction {
    Reserve {
        actor: String,
        number: i64,
        request: CandidateRequest,
        created_ms: i64,
    },
    Finish {
        actor: String,
        id: String,
        result: CandidateResult,
    },
}
impl CandidateAction {
    pub(super) fn valid(&self) -> bool {
        match self {
            Self::Reserve {
                actor,
                number,
                request,
                created_ms,
            } => {
                validate_component(actor).is_ok()
                    && *number > 0
                    && *created_ms >= 0
                    && valid_request(request)
            }
            Self::Finish { actor, id, result } => {
                validate_component(actor).is_ok()
                    && uuid::Uuid::parse_str(id)
                        .ok()
                        .is_some_and(|uuid| uuid.to_string() == *id)
                    && valid_result(result)
            }
        }
    }
    pub(crate) fn actor(&self) -> &str {
        match self {
            Self::Reserve { actor, .. } | Self::Finish { actor, .. } => actor,
        }
    }
    pub(crate) fn digest(&self) -> Result<[u8; 32], CodecError> {
        let mut e = BoundedEncoder::new(256 << 10)?;
        self.encode(&mut e)?;
        let mut h = blake3::Hasher::new();
        h.update(b"canopy.native-candidate-intent.v1\0");
        h.update(&e.finish());
        Ok(*h.finalize().as_bytes())
    }
}
impl WireValue for CandidateAction {
    fn encode(&self, out: &mut BoundedEncoder) -> Result<(), CodecError> {
        if !self.valid() {
            return Err(CodecError::Invalid("invalid candidate action"));
        }
        out.write_bytes(
            &serde_json::to_vec(self).map_err(|_| CodecError::Invalid("candidate action"))?,
        )
    }
    fn decode(input: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        let result: Self = serde_json::from_slice(input.read_bytes()?)
            .map_err(|_| CodecError::Invalid("candidate action"))?;
        if !result.valid() {
            return Err(CodecError::Invalid("invalid candidate action"));
        }
        Ok(result)
    }
}

/// Certified current refs authorize only editorial reservation or a negative
/// preparation result. A Ready result requires joint generated publication;
/// no request DTO or serving observation can grant that write authority.
#[derive(Clone, Debug)]
pub(crate) struct CandidateRefRequest {
    pub(crate) selection: RefSelection,
    pub(crate) action: CandidateAction,
}
impl WireValue for CandidateRefRequest {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        if self.selection.actor.as_deref() != Some(self.action.actor())
            || self.selection.facts.len() > 2
            || matches!(
                &self.action,
                CandidateAction::Finish {
                    result: CandidateResult::Ready { .. },
                    ..
                }
            )
        {
            return Err(CodecError::Invalid(
                "invalid native candidate editorial scope",
            ));
        }
        self.selection.encode(e)?;
        self.action.encode(e)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        let value = Self {
            selection: RefSelection::decode(d)?,
            action: CandidateAction::decode(d)?,
        };
        value.encode(&mut BoundedEncoder::new(INPUT_BYTES)?)?;
        Ok(value)
    }
}
pub(crate) struct PrepareCandidate;
impl Command for PrepareCandidate {
    const MODULE: &'static str = RepositoryModule::NAME;
    const ID: u32 = 10;
    const CODEC_VERSION: u32 = 3;
    type Input = CandidateRefRequest;
    type Output = CandidateOutcome;
    fn execute(
        context: &mut CommandContext<'_, '_>,
        input: CandidateRefRequest,
    ) -> cellule_runtime::Result<CommandResult<CandidateOutcome>> {
        input.encode(&mut BoundedEncoder::new(INPUT_BYTES)?)?;
        let actor = input.action.actor();
        let rejected = |value| Ok(CommandResult::Rejected(value));
        let role = decode_access(&context.sql(&SqlBatch {
            statements: vec![access_statement(actor)],
        })?)?;
        let Some(role) = role else {
            return rejected(CandidateOutcome::NotFound);
        };
        if role < TokenScope::Write {
            return rejected(CandidateOutcome::Forbidden);
        }
        if !input.selection.authorized(
            context.target().cell_id(),
            Some(context.owner_fence()),
            context.now_ms(),
            input.action.digest()?,
            |q| context.sql(q),
        )? {
            return rejected(CandidateOutcome::Conflict);
        }
        let action = input.action;
        let (candidate, binding) = match action {
            CandidateAction::Reserve {
                actor,
                number,
                request,
                created_ms,
            } => {
                let bytes = serde_json::to_string(&request).map_err(|source| Error::Facility {
                    name: "candidate intent",
                    source: Box::new(source),
                })?;
                let binding =
                    super::super::mutations::binding(&[&actor, &number.to_string(), &bytes]);
                if let Some((stored, candidate)) = decode(&context.sql(&query(&request.id)?)?)? {
                    return if stored == binding {
                        Ok(CommandResult::Success(CandidateOutcome::Applied(Box::new(
                            candidate,
                        ))))
                    } else {
                        rejected(CandidateOutcome::Conflict)
                    };
                }
                (
                    MergeCandidate {
                        request,
                        number,
                        actor,
                        created_at_ms: created_ms,
                        result: CandidateResult::Pending,
                    },
                    Some(binding),
                )
            }
            CandidateAction::Finish { actor, id, result } => {
                let Some((_, mut candidate)) = decode(&context.sql(&query(&id)?)?)? else {
                    return rejected(CandidateOutcome::NotFound);
                };
                if candidate.actor != actor {
                    return rejected(CandidateOutcome::Forbidden);
                }
                if candidate.result != CandidateResult::Pending {
                    return Ok(CommandResult::Success(CandidateOutcome::Applied(Box::new(
                        candidate,
                    ))));
                }
                candidate.result = result;
                (candidate, None)
            }
        };
        let Some(state) = policy_state(&context.sql(&SqlBatch {
            statements: vec![super::super::native::with_refs(
                policy_statement(&candidate.actor, candidate.number),
                &input.selection,
            )],
        })?)?
        else {
            return rejected(CandidateOutcome::NotFound);
        };
        // Preparation may precede approvals/checks, but its input trees must
        // still be the current ready proposal at both reservation and completion.
        if !state.policy.ready
            || state.policy.revision.as_ref() != Some(&candidate.request.revision)
        {
            return rejected(CandidateOutcome::Conflict);
        }
        let id = uuid::Uuid::parse_str(&candidate.request.id)
            .map_err(|_| Error::Command("invalid candidate ID"))?;
        let result =
            serde_json::to_string(&candidate.result).map_err(|source| Error::Facility {
                name: "candidate result",
                source: Box::new(source),
            })?;
        let statement = if let Some(binding) = binding {
            let request =
                serde_json::to_string(&candidate.request).map_err(|source| Error::Facility {
                    name: "candidate intent",
                    source: Box::new(source),
                })?;
            SqlStatement {sql:"INSERT INTO merge_candidates (id,binding,pull_number,actor,request,created_ms,result,source_oid,base_oid) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9)".into(),parameters:vec![SqlValue::Blob(id.as_bytes().to_vec()),SqlValue::Blob(binding),SqlValue::Integer(candidate.number),SqlValue::Text(candidate.actor.clone()),SqlValue::Text(request),SqlValue::Integer(candidate.created_at_ms),SqlValue::Text(result),SqlValue::Blob(oid(&candidate.request.revision.source_oid)?.to_vec()),SqlValue::Blob(oid(&candidate.request.revision.base_oid)?.to_vec())]}
        } else {
            SqlStatement {
                sql: "UPDATE merge_candidates SET result = ?2 WHERE id = ?1".into(),
                parameters: vec![
                    SqlValue::Blob(id.as_bytes().to_vec()),
                    SqlValue::Text(result),
                ],
            }
        };
        context.sql(&SqlBatch {
            statements: vec![statement],
        })?;
        Ok(CommandResult::Success(CandidateOutcome::Applied(Box::new(
            candidate,
        ))))
    }
}
