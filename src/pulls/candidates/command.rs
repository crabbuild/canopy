use super::*;
use crate::{
    RepositoryModule,
    access::{access_statement, decode_access},
    directory::TokenScope,
};
use cellule_runtime::{CellModule, Command, CommandResult};

#[derive(Deserialize, Serialize)]
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
    fn valid(&self) -> bool {
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
pub(crate) struct PrepareCandidate;
impl Command for PrepareCandidate {
    const MODULE: &'static str = RepositoryModule::NAME;
    const ID: u32 = 10;
    const CODEC_VERSION: u32 = 1;
    type Input = CandidateAction;
    type Output = CandidateOutcome;
    fn execute(
        context: &mut CommandContext<'_, '_>,
        action: CandidateAction,
    ) -> cellule_runtime::Result<CommandResult<CandidateOutcome>> {
        let actor = match &action {
            CandidateAction::Reserve { actor, .. } | CandidateAction::Finish { actor, .. } => actor,
        };
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
                if let CandidateResult::Ready { oid, tree_oid } = &result
                    && !certified(context, &candidate, oid, tree_oid)?
                {
                    return rejected(CandidateOutcome::Conflict);
                }
                candidate.result = result;
                (candidate, None)
            }
        };
        let Some(state) = policy_state(&context.sql(&SqlBatch {
            statements: vec![policy_statement(&candidate.actor, candidate.number)],
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
            let ready = match &candidate.result {
                CandidateResult::Ready { oid: commit, .. } => SqlValue::Blob(oid(commit)?.to_vec()),
                _ => SqlValue::Null,
            };
            SqlStatement {
                sql: "UPDATE merge_candidates SET result = ?2, oid = ?3 WHERE id = ?1".into(),
                parameters: vec![
                    SqlValue::Blob(id.as_bytes().to_vec()),
                    SqlValue::Text(result),
                    ready,
                ],
            }
        };
        context.sql(&SqlBatch {
            statements: vec![statement],
        })?;
        if let CandidateResult::Ready { oid: commit, .. } = &candidate.result {
            // A candidate becomes fetchable in the same transaction as its ready
            // result. The reserved namespace cannot be changed by ordinary pushes.
            context.sql(&SqlBatch {
                statements: vec![SqlStatement {
                    sql: "INSERT INTO refs (name, oid, version) VALUES (?1, ?2, 1)".into(),
                    parameters: vec![
                        SqlValue::Text(candidate.fetch_ref()),
                        SqlValue::Blob(oid(commit)?.to_vec()),
                    ],
                }],
            })?;
            crate::refs::advance_generation(context)?;
        }
        Ok(CommandResult::Success(CandidateOutcome::Applied(Box::new(
            candidate,
        ))))
    }
}
