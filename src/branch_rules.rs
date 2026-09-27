//! Exact-branch policy enforced in the authoritative ref transaction.

use crate::ReadIdentity;

pub(crate) mod command;

use crate::{
    PushPlan, RefUpdate, RepositoryCell,
    default_branch::valid_default_branch,
    directory::{TokenScope, validate_component},
};
use crab_cell_runtime::{
    Committed, Error, InvocationError, MutationIdentity, Observed, primitives::sql::SqlBatch,
    primitives::sql::SqlResultSet, primitives::sql::SqlStatement, primitives::sql::SqlValue,
};
use serde::{Deserialize, Serialize};

pub const RULE_PAGE_SIZE: usize = 32;
use crate::access::READ_ACCESS as ACCESS;

/// Versioned policy for one exact branch; disabled records retain their versions.
#[derive(Debug, PartialEq, Eq, Serialize)]
pub struct BranchRule {
    pub reference: String,
    pub version: i64,
    pub enabled: bool,
    pub deny_deletions: bool,
    pub fast_forward_only: bool,
    pub required_checks: Vec<String>,
    pub require_pull_request: bool,
    pub required_approvals: u8,
}

/// Complete policy replacement, with zero as the version of a never-used name.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BranchRuleEdit {
    pub reference: String,
    pub expected_version: i64,
    pub enabled: bool,
    pub deny_deletions: bool,
    pub fast_forward_only: bool,
    pub required_checks: Vec<String>,
    pub require_pull_request: bool,
    pub required_approvals: u8,
}

pub(crate) fn valid_edit(edit: &BranchRuleEdit) -> bool {
    valid_default_branch(&edit.reference)
        && (0..i64::MAX).contains(&edit.expected_version)
        && edit.required_approvals <= 16
        && (edit.require_pull_request || edit.required_approvals == 0)
        && edit.required_checks.len() <= 16
        && edit
            .required_checks
            .iter()
            .all(|name| validate_component(name).is_ok())
        && edit
            .required_checks
            .iter()
            .collect::<std::collections::BTreeSet<_>>()
            .len()
            == edit.required_checks.len()
}

impl RepositoryCell {
    pub(crate) async fn has_branch_rules(
        &self,
    ) -> Result<bool, InvocationError<Vec<SqlResultSet>>> {
        let result = self
            .sql
            .query(
                None,
                SqlBatch {
                    statements: vec![SqlStatement {
                        sql: "SELECT EXISTS (SELECT 1 FROM branch_rules WHERE enabled = 1)".into(),
                        parameters: vec![],
                    }],
                },
            )
            .await?;
        match result
            .output
            .first()
            .and_then(|set| set.rows.first())
            .map(Vec::as_slice)
        {
            Some([SqlValue::Integer(0)]) => Ok(false),
            Some([SqlValue::Integer(1)]) => Ok(true),
            _ => Err(InvocationError::NotStarted(Error::Command(
                "invalid branch rule presence",
            ))),
        }
    }

    /// Reads up to 32 exact-branch rules, including disabled records, for a member.
    pub async fn branch_rules<'a>(
        &self,
        actor: impl Into<ReadIdentity<'a>>,
        after: Option<&str>,
    ) -> Result<Observed<Option<Vec<BranchRule>>>, InvocationError<Vec<SqlResultSet>>> {
        let actor = actor.into();
        actor.validate().map_err(InvocationError::NotStarted)?;
        if after.is_some_and(|name| !valid_default_branch(name)) {
            return Err(InvocationError::NotStarted(Error::Command(
                "invalid branch rule cursor",
            )));
        }
        let result = self.sql.query(None, SqlBatch { statements: vec![
            SqlStatement { sql: format!("SELECT ({ACCESS})"), parameters: vec![actor.parameter()] },
            SqlStatement {
                sql: format!("SELECT b.reference, b.version, b.enabled, b.deny_deletions, b.fast_forward, coalesce((SELECT group_concat(context, ',') FROM (SELECT context FROM branch_required_checks WHERE reference = b.reference ORDER BY context)), ''), b.require_pull_request, b.required_approvals FROM branch_rules b WHERE b.reference > ?2 AND ({ACCESS}) ORDER BY b.reference LIMIT {RULE_PAGE_SIZE}"),
                parameters: vec![actor.parameter(), SqlValue::Text(after.unwrap_or("").into())],
            },
        ] }).await?;
        let allowed = result
            .output
            .first()
            .and_then(|set| set.rows.first())
            .map(Vec::as_slice);
        let output = match allowed {
            Some([SqlValue::Integer(0)]) => None,
            Some([SqlValue::Integer(1)]) => {
                let rows =
                    result
                        .output
                        .get(1)
                        .ok_or(InvocationError::NotStarted(Error::Command(
                            "missing branch rules",
                        )))?;
                Some(
                    rows.rows
                        .iter()
                        .map(|row| decode_rule(row))
                        .collect::<crab_cell_runtime::Result<Vec<_>>>()
                        .map_err(InvocationError::NotStarted)?,
                )
            }
            _ => {
                return Err(InvocationError::NotStarted(Error::Command(
                    "invalid branch rule authorization",
                )));
            }
        };
        Ok(Observed {
            output,
            receipt: result.receipt,
        })
    }

    /// Replaces a policy only for the owner at its current version.
    ///
    /// Enabled rules require enabled check contexts. A rejected result means
    /// authority, version or context validation failed; the rule was not changed.
    pub async fn set_branch_rule(
        &self,
        identity: MutationIdentity,
        actor: &str,
        edit: BranchRuleEdit,
    ) -> Result<Committed<bool>, InvocationError<bool>> {
        self.application
            .command::<command::SetBranchRule>(
                &self.target,
                identity,
                command::RuleChange {
                    actor: actor.into(),
                    edit,
                },
            )
            .await
    }

    pub(crate) async fn branch_policies(
        &self,
        updates: &[RefUpdate],
    ) -> Result<Vec<Option<Policy>>, InvocationError<Vec<SqlResultSet>>> {
        let result = self
            .sql
            .query(
                None,
                SqlBatch {
                    statements: updates.iter().map(policy_statement).collect(),
                },
            )
            .await?;
        result
            .output
            .iter()
            .map(|set| decode_policy(std::slice::from_ref(set)))
            .collect::<crab_cell_runtime::Result<Vec<_>>>()
            .map_err(InvocationError::NotStarted)
    }

    pub(crate) async fn prepare_branch_proofs(
        &self,
        plan: &PushPlan,
    ) -> Result<(), InvocationError<bool>> {
        if plan.updates.is_empty() || plan.updates.len() > crate::refs::MAX_UPDATES {
            return Ok(());
        }
        let role = self
            .access_level(&plan.actor, None)
            .await
            .map_err(|source| {
                InvocationError::NotStarted(Error::Facility {
                    name: "branch proof authorization",
                    source: Box::new(source),
                })
            })?;
        if !role.output.is_some_and(|role| role >= TokenScope::Write) {
            return Ok(());
        }
        for updates in plan.updates.chunks(128) {
            let policies = self.branch_policies(updates).await.map_err(|source| {
                InvocationError::NotStarted(Error::Facility {
                    name: "branch proof policy",
                    source: Box::new(source),
                })
            })?;
            for (update, policy) in updates.iter().zip(policies) {
                let (Some(old), Some(new)) = (
                    update.expected.as_ref().and_then(|old| old.oid),
                    update.new_oid,
                ) else {
                    continue;
                };
                if policy.is_some_and(|policy| {
                    !policy.require_pull_request
                        && policy.fast_forward_only
                        && policy.checks_pass
                        && !policy.ancestry
                }) {
                    self.prepare_ancestry(old, new).await?;
                }
            }
        }
        Ok(())
    }
}

pub(crate) struct Policy {
    pub deny_deletions: bool,
    pub fast_forward_only: bool,
    checks_pass: bool,
    ancestry: bool,
    require_pull_request: bool,
}
impl Policy {
    pub(crate) fn allows(&self, update: &RefUpdate, require_ancestry: bool) -> bool {
        !self.require_pull_request && self.allows_ref(update, require_ancestry)
    }
    fn allows_ref(&self, update: &RefUpdate, require_ancestry: bool) -> bool {
        if update.new_oid.is_none() {
            return !self.deny_deletions;
        }
        self.checks_pass && (!require_ancestry || !self.fast_forward_only || self.ancestry)
    }
}

pub(crate) fn policies_allow(
    context: &crab_cell_runtime::registry::CommandContext<'_, '_>,
    plan: &PushPlan,
    merge: Option<&crate::pulls::merge::ReviewedMerge>,
) -> crab_cell_runtime::Result<bool> {
    for updates in plan.updates.chunks(128) {
        let result = context.sql(&SqlBatch {
            statements: updates.iter().map(policy_statement).collect(),
        })?;
        for (update, set) in updates.iter().zip(&result) {
            if decode_policy(std::slice::from_ref(set))?.is_some_and(|policy| {
                !policy.allows_ref(update, true)
                    || (policy.require_pull_request
                        && !merge.is_some_and(|merge| merge.authorizes(update)))
            }) {
                return Ok(false);
            }
        }
    }
    Ok(true)
}

fn policy_statement(update: &RefUpdate) -> SqlStatement {
    let old = update.expected.as_ref().and_then(|old| old.oid);
    SqlStatement {
        sql: "SELECT b.deny_deletions, b.fast_forward, NOT EXISTS (SELECT 1 FROM branch_required_checks q LEFT JOIN check_contexts c ON c.name = q.context LEFT JOIN check_runs r ON r.number = (SELECT number FROM check_runs WHERE oid = ?3 AND context = q.context AND context_version = c.version ORDER BY number DESC LIMIT 1) WHERE q.reference = b.reference AND (c.enabled IS NOT 1 OR r.state IS NOT 'success' OR r.reporter IS NOT c.reporter)), (?2 IS NULL OR coalesce(?2 = ?3, 0) OR EXISTS (SELECT 1 FROM commit_ancestry WHERE ancestor = ?2 AND descendant = ?3)), b.require_pull_request FROM branch_rules b WHERE b.reference = ?1 AND b.enabled = 1".into(),
        parameters: vec![SqlValue::Text(update.name.clone()), old.map_or(SqlValue::Null, |oid| SqlValue::Blob(oid.to_vec())), update.new_oid.map_or(SqlValue::Null, |oid| SqlValue::Blob(oid.to_vec()))],
    }
}
fn decode_policy(sets: &[SqlResultSet]) -> crab_cell_runtime::Result<Option<Policy>> {
    let set = sets
        .first()
        .ok_or(Error::Command("missing branch policy result"))?;
    let Some(row) = set.rows.first() else {
        return Ok(None);
    };
    let [
        SqlValue::Integer(delete),
        SqlValue::Integer(ff),
        SqlValue::Integer(checks),
        SqlValue::Integer(ancestry),
        SqlValue::Integer(pull),
    ] = row.as_slice()
    else {
        return Err(Error::Command("invalid branch policy"));
    };
    Ok(Some(Policy {
        deny_deletions: *delete == 1,
        fast_forward_only: *ff == 1,
        checks_pass: *checks == 1,
        ancestry: *ancestry == 1,
        require_pull_request: *pull == 1,
    }))
}
fn decode_rule(row: &[SqlValue]) -> crab_cell_runtime::Result<BranchRule> {
    let [
        SqlValue::Text(reference),
        SqlValue::Integer(version),
        SqlValue::Integer(enabled),
        SqlValue::Integer(delete),
        SqlValue::Integer(ff),
        SqlValue::Text(checks),
        SqlValue::Integer(pull),
        SqlValue::Integer(approvals),
    ] = row
    else {
        return Err(Error::Command("invalid branch rule"));
    };
    Ok(BranchRule {
        reference: reference.clone(),
        version: *version,
        enabled: *enabled == 1,
        deny_deletions: *delete == 1,
        fast_forward_only: *ff == 1,
        require_pull_request: *pull == 1,
        required_approvals: u8::try_from(*approvals)
            .map_err(|_| Error::Command("invalid approval count"))?,
        required_checks: if checks.is_empty() {
            Vec::new()
        } else {
            checks.split(',').map(str::to_owned).collect()
        },
    })
}
