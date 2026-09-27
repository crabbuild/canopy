use super::*;
use crate::{
    RepositoryModule,
    access::{access_statement, decode_access},
};
use crab_cell_runtime::{
    CellModule, Command, codec::BoundedDecoder, codec::BoundedEncoder, codec::CodecError,
    codec::WireValue, registry::CommandContext, registry::CommandResult,
};

pub(crate) struct RuleChange {
    pub actor: String,
    pub edit: BranchRuleEdit,
}
impl WireValue for RuleChange {
    fn encode(&self, out: &mut BoundedEncoder) -> Result<(), CodecError> {
        if validate_component(&self.actor).is_err() || !valid_edit(&self.edit) {
            return Err(CodecError::Invalid("invalid branch rule change"));
        }
        out.write_text(&self.actor)?;
        out.write_text(&self.edit.reference)?;
        out.write_i64(self.edit.expected_version)?;
        out.write_bool(self.edit.enabled)?;
        out.write_bool(self.edit.deny_deletions)?;
        out.write_bool(self.edit.fast_forward_only)?;
        out.write_bool(self.edit.require_pull_request)?;
        out.write_i64(i64::from(self.edit.required_approvals))?;
        out.write_count(self.edit.required_checks.len())?;
        for check in &self.edit.required_checks {
            out.write_text(check)?;
        }
        Ok(())
    }
    fn decode(input: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        let actor = input.read_text()?.into();
        let reference = input.read_text()?.into();
        let expected_version = input.read_i64()?;
        let enabled = input.read_bool()?;
        let deny_deletions = input.read_bool()?;
        let fast_forward_only = input.read_bool()?;
        let require_pull_request = input.read_bool()?;
        let required_approvals = u8::try_from(input.read_i64()?)
            .map_err(|_| CodecError::Invalid("invalid approval count"))?;
        let count = input.read_count()?;
        if count > 16 {
            return Err(CodecError::Invalid("too many required checks"));
        }
        let mut required_checks = Vec::with_capacity(count);
        for _ in 0..count {
            required_checks.push(input.read_text()?.into());
        }
        let result = Self {
            actor,
            edit: BranchRuleEdit {
                reference,
                expected_version,
                enabled,
                deny_deletions,
                fast_forward_only,
                required_checks,
                require_pull_request,
                required_approvals,
            },
        };
        if validate_component(&result.actor).is_err() || !valid_edit(&result.edit) {
            return Err(CodecError::Invalid("invalid branch rule change"));
        }
        Ok(result)
    }
}

pub(crate) struct SetBranchRule;
impl Command for SetBranchRule {
    const MODULE: &'static str = RepositoryModule::NAME;
    const ID: u32 = 8;
    const CODEC_VERSION: u32 = 2;
    type Input = RuleChange;
    type Output = bool;
    fn execute(
        context: &mut CommandContext<'_, '_>,
        change: RuleChange,
    ) -> crab_cell_runtime::Result<CommandResult<bool>> {
        if decode_access(&context.sql(&SqlBatch {
            statements: vec![access_statement(&change.actor)],
        })?)?
            != Some(TokenScope::Admin)
        {
            return Ok(CommandResult::Rejected(false));
        }
        let edit = change.edit;
        let current = context.sql(&SqlBatch {
            statements: vec![SqlStatement {
                sql: "SELECT coalesce((SELECT version FROM branch_rules WHERE reference = ?1), 0)"
                    .into(),
                parameters: vec![SqlValue::Text(edit.reference.clone())],
            }],
        })?;
        if !matches!(current.first().and_then(|set| set.rows.first()).map(Vec::as_slice), Some([SqlValue::Integer(version)]) if *version == edit.expected_version)
        {
            return Ok(CommandResult::Rejected(false));
        }
        if edit.enabled {
            for check in &edit.required_checks {
                let result = context.sql(&SqlBatch {
                    statements: vec![SqlStatement {
                        sql: "SELECT 1 FROM check_contexts WHERE name = ?1 AND enabled = 1".into(),
                        parameters: vec![SqlValue::Text(check.clone())],
                    }],
                })?;
                if result.first().is_none_or(|set| set.rows.is_empty()) {
                    return Ok(CommandResult::Rejected(false));
                }
            }
        }
        context.sql(&SqlBatch { statements: vec![
            SqlStatement {
                sql: "INSERT INTO branch_rules (reference, version, enabled, deny_deletions, fast_forward, require_pull_request, required_approvals) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7) ON CONFLICT(reference) DO UPDATE SET version = excluded.version, enabled = excluded.enabled, deny_deletions = excluded.deny_deletions, fast_forward = excluded.fast_forward, require_pull_request = excluded.require_pull_request, required_approvals = excluded.required_approvals".into(),
                parameters: vec![SqlValue::Text(edit.reference.clone()), SqlValue::Integer(edit.expected_version + 1), SqlValue::Integer(i64::from(edit.enabled)), SqlValue::Integer(i64::from(edit.deny_deletions)), SqlValue::Integer(i64::from(edit.fast_forward_only)), SqlValue::Integer(i64::from(edit.require_pull_request)), SqlValue::Integer(i64::from(edit.required_approvals))],
            },
            SqlStatement { sql: "DELETE FROM branch_required_checks WHERE reference = ?1".into(), parameters: vec![SqlValue::Text(edit.reference.clone())] },
        ] })?;
        for check in edit.required_checks {
            context.sql(&SqlBatch {
                statements: vec![SqlStatement {
                    sql: "INSERT INTO branch_required_checks (reference, context) VALUES (?1, ?2)"
                        .into(),
                    parameters: vec![
                        SqlValue::Text(edit.reference.clone()),
                        SqlValue::Text(check),
                    ],
                }],
            })?;
        }
        Ok(CommandResult::Success(true))
    }
}
