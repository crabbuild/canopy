use super::*;
use crate::{
    PushPlan, RepositoryModule,
    access::{access_statement, decode_access},
};
use cellule_runtime::{
    BoundedDecoder, BoundedEncoder, CellModule, CodecError, Command, CommandContext, CommandResult,
    WireValue,
};

#[derive(Deserialize, Serialize)]
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
pub(crate) struct MergePull;
impl Command for MergePull {
    const MODULE: &'static str = RepositoryModule::NAME;
    const ID: u32 = 9;
    const CODEC_VERSION: u32 = 3;
    type Input = MergeInput;
    type Output = MergeOutcome;
    fn execute(
        context: &mut CommandContext<'_, '_>,
        input: MergeInput,
    ) -> cellule_runtime::Result<CommandResult<MergeOutcome>> {
        let role = decode_access(&context.sql(&SqlBatch {
            statements: vec![access_statement(&input.actor)],
        })?)?;
        let rejected = |value| Ok(CommandResult::Rejected(value));
        let Some(role) = role else {
            return rejected(MergeOutcome::NotFound);
        };
        if role < TokenScope::Write {
            return rejected(MergeOutcome::Forbidden);
        }
        let id = uuid::Uuid::parse_str(&input.request.id)
            .map_err(|_| Error::Command("invalid merge UUID"))?;
        // Bind actor, parent and full intent. Command timestamps are not part of
        // application retry identity, so a lost reply can use a fresh command.
        let binding = super::super::mutations::binding(&[
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
            },
            input.request.candidate_id.as_deref().unwrap_or(""),
        ]);
        let previous = context.sql(&SqlBatch {
            statements: vec![SqlStatement {
                sql: "SELECT binding, id, pull_number, oid, merged_ms, pull_version, source_oid, source_version, base_oid, base_version FROM pull_merges WHERE id = ?1"
                    .into(),
                parameters: vec![SqlValue::Blob(id.as_bytes().to_vec())],
            }],
        })?;
        if let Some(row) = previous.first().and_then(|set| set.rows.first()) {
            let Some(SqlValue::Blob(stored)) = row.first() else {
                return Err(Error::Command("invalid merge binding"));
            };
            if *stored != binding {
                return rejected(MergeOutcome::Conflict);
            }
            return Ok(CommandResult::Success(MergeOutcome::Applied {
                merge: record(&row[1..])?,
            }));
        }
        let Some(state) = policy_state(&context.sql(&SqlBatch {
            statements: vec![policy_statement(&input.actor, input.number)],
        })?)?
        else {
            return rejected(MergeOutcome::NotFound);
        };
        if !state.policy.ready || state.policy.revision.as_ref() != Some(&input.request.revision) {
            return rejected(MergeOutcome::Conflict);
        }
        if !state.policy.reviews_satisfied {
            return rejected(MergeOutcome::ReviewsRequired);
        }
        let base = oid(&input.request.revision.base_oid)?;
        let source = match input.request.strategy {
            MergeStrategy::FastForward => oid(&input.request.revision.source_oid)?,
            MergeStrategy::MergeCommit | MergeStrategy::Squash => {
                match super::super::candidates::publication_oid(
                    context,
                    input.number,
                    &input.request,
                )? {
                    Some(oid) => oid,
                    None => return rejected(MergeOutcome::Conflict),
                }
            }
        };
        let ancestry = context.sql(&SqlBatch {
            statements: vec![SqlStatement {
                sql: "SELECT 1 FROM commit_ancestry WHERE ancestor = ?1 AND descendant = ?2".into(),
                parameters: vec![
                    SqlValue::Blob(base.to_vec()),
                    SqlValue::Blob(source.to_vec()),
                ],
            }],
        })?;
        if ancestry.first().is_none_or(|set| set.rows.is_empty()) {
            return rejected(MergeOutcome::NotFastForward);
        }
        let update = RefUpdate {
            name: state.base,
            expected: Some(RefExpectation {
                oid: Some(base),
                version: input.request.revision.base_version,
            }),
            new_oid: Some(source),
        };
        let reviewed = ReviewedMerge {
            update: update.clone(),
        };
        let plan = PushPlan {
            actor: input.actor,
            updates: vec![update],
        };
        // All writes below share this command transaction. Current checks and
        // every normal ref invariant still pass through the canonical publisher.
        if !crate::refs::apply_refs(context, &plan, Some(&reviewed))? {
            return rejected(MergeOutcome::BranchPolicy);
        }
        let result=context.sql(&SqlBatch {statements:vec![SqlStatement {
            sql:"UPDATE pull_requests SET state = 'merged', version = version + 1, updated_ms = max(updated_ms, ?2) WHERE number = ?1 AND state = 'open'".into(),parameters:vec![SqlValue::Integer(input.number),SqlValue::Integer(input.issued_at_ms)],
        },SqlStatement {
            sql:"INSERT INTO pull_merges (id, binding, pull_number, oid, merged_ms, pull_version, source_oid, source_version, base_oid, base_version) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)".into(),parameters:vec![SqlValue::Blob(id.as_bytes().to_vec()),SqlValue::Blob(binding),SqlValue::Integer(input.number),SqlValue::Blob(source.to_vec()),SqlValue::Integer(input.issued_at_ms),SqlValue::Integer(input.request.revision.pull_version),SqlValue::Blob(oid(&input.request.revision.source_oid)?.to_vec()),SqlValue::Integer(input.request.revision.source_version),SqlValue::Blob(base.to_vec()),SqlValue::Integer(input.request.revision.base_version)],
        }]})?;
        if result.first().is_none_or(|set| set.rows_affected != 1) {
            return Err(Error::Command("merged pull was not updated"));
        }
        Ok(CommandResult::Success(MergeOutcome::Applied {
            merge: MergeRecord {
                id: input.request.id,
                number: input.number,
                oid: hex::encode(source),
                merged_at_ms: input.issued_at_ms,
                revision: input.request.revision,
            },
        }))
    }
}
