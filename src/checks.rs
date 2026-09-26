//! Owner-configured commit checks with reporter identity and ordered reruns.

use crate::ReadIdentity;

mod mutations;

use crate::{RepositoryCell, directory::validate_component, validate_repository_id};
use cellule_runtime::{
    Committed, Error, InvocationError, MutationIdentity, Observed, SqlBatch, SqlResultSet,
    SqlStatement, SqlValue,
};
use serde::{Deserialize, Serialize};

type Invocation = InvocationError<Vec<SqlResultSet>>;
pub const CHECK_PAGE_SIZE: usize = 32;
use crate::access::READ_ACCESS as ACCESS;
const OWNER: &str = "EXISTS (SELECT 1 FROM repository_identity WHERE owner = ?1)";
const RUN_COLUMNS: &str = "r.id, r.oid, r.context, r.context_version, r.reporter, r.state, r.version, r.summary, r.created_ms, r.updated_ms";

/// Owner-defined check identity and the only account allowed to report it.
#[derive(Debug, PartialEq, Eq, Serialize)]
pub struct CheckContext {
    pub name: String,
    pub reporter: String,
    pub enabled: bool,
    pub version: i64,
}

/// Replacement context policy; version zero creates a previously unused name.
pub struct CheckContextEdit<'a> {
    pub expected_version: i64,
    pub reporter: &'a str,
    pub enabled: bool,
}

/// Lifecycle of one immutable-identity check attempt.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CheckState {
    Queued,
    InProgress,
    Success,
    Failure,
    Cancelled,
}

impl CheckState {
    fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::InProgress => "in_progress",
            Self::Success => "success",
            Self::Failure => "failure",
            Self::Cancelled => "cancelled",
        }
    }
}

/// A check attempt for one commit and one context-policy version.
#[derive(Debug, PartialEq, Eq, Serialize)]
pub struct CheckRun {
    pub id: String,
    pub oid: String,
    pub context: String,
    pub context_version: i64,
    pub reporter: String,
    pub state: CheckState,
    pub version: i64,
    pub summary: String,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

/// Latest run for a currently enabled context; absent runs have no result.
#[derive(Debug, PartialEq, Eq, Serialize)]
pub struct CommitCheck {
    pub context: CheckContext,
    pub run: Option<CheckRun>,
}

/// Client-chosen attempt identity bound to a commit and context-policy version.
pub struct NewCheck<'a> {
    pub id: [u8; 16],
    pub oid: [u8; 20],
    pub context: &'a str,
    pub context_version: i64,
}

/// Compare-and-set update of an active check attempt.
pub struct CheckEdit<'a> {
    pub expected_version: i64,
    pub state: CheckState,
    pub summary: &'a str,
}

/// A published policy or run mutation outcome.
#[derive(Debug, PartialEq, Eq)]
pub enum CheckChange {
    Applied,
    NotFound,
    Forbidden,
    Conflict,
}

pub(crate) fn valid_summary(value: &str) -> bool {
    value.len() <= 4096 && !value.contains('\0')
}

impl RepositoryCell {
    /// Lists bounded context policy, including disabled names, for a repository reader.
    pub async fn check_contexts<'a>(
        &self,
        actor: impl Into<ReadIdentity<'a>>,
        after: Option<&str>,
    ) -> Result<Observed<Option<Vec<CheckContext>>>, Invocation> {
        let actor = actor.into();
        let parameters = cursor_parameters(actor, after)?;
        let result = self.check_rows(
            SqlStatement { sql: format!("SELECT ({ACCESS})"), parameters: vec![parameters[0].clone()] },
            SqlStatement { sql: format!("SELECT name, reporter, enabled, version FROM check_contexts WHERE name > ?2 AND ({ACCESS}) ORDER BY name LIMIT {CHECK_PAGE_SIZE}"), parameters },
        ).await?;
        let output = result
            .output
            .map(|rows| rows.iter().map(|row| context(row)).collect())
            .transpose()
            .map_err(Invocation::NotStarted)?;
        Ok(Observed {
            output,
            receipt: result.receipt,
        })
    }

    /// Reads a historical attempt while the actor has repository read access.
    pub async fn check_run<'a>(
        &self,
        actor: impl Into<ReadIdentity<'a>>,
        id: [u8; 16],
    ) -> Result<Observed<Option<CheckRun>>, Invocation> {
        let actor = actor.into();
        actor.validate().map_err(Invocation::NotStarted)?;
        let result = self
            .sql
            .query(
                None,
                SqlBatch {
                    statements: vec![SqlStatement {
                        sql: format!(
                            "SELECT {RUN_COLUMNS} FROM check_runs r WHERE r.id = ?2 AND ({ACCESS})"
                        ),
                        parameters: vec![actor.parameter(), SqlValue::Blob(id.to_vec())],
                    }],
                },
            )
            .await?;
        let rows = result
            .output
            .first()
            .ok_or(Invocation::NotStarted(Error::Command(
                "missing check result",
            )))?;
        let output = rows
            .rows
            .first()
            .map(|row| run(row))
            .transpose()
            .map_err(Invocation::NotStarted)?;
        Ok(Observed {
            output,
            receipt: result.receipt,
        })
    }

    /// Reads the newest attempt for each enabled context on one stored commit.
    ///
    /// A context version change invalidates older attempts; exact start retries
    /// never change the ordering. Missing access or a non-commit returns `None`.
    pub async fn commit_checks<'a>(
        &self,
        actor: impl Into<ReadIdentity<'a>>,
        oid: [u8; 20],
        after: Option<&str>,
    ) -> Result<Observed<Option<Vec<CommitCheck>>>, Invocation> {
        let actor = actor.into();
        let mut parameters = cursor_parameters(actor, after)?;
        parameters.push(SqlValue::Blob(oid.to_vec()));
        let result = self.check_rows(
            SqlStatement { sql: format!("SELECT ({ACCESS}) AND EXISTS (SELECT 1 FROM objects WHERE oid = ?2 AND kind = 'commit')"), parameters: vec![parameters[0].clone(), parameters[2].clone()] },
            SqlStatement {
                sql: format!("SELECT c.name, c.reporter, c.enabled, c.version, {RUN_COLUMNS} FROM check_contexts c LEFT JOIN check_runs r ON r.number = (SELECT number FROM check_runs WHERE oid = ?3 AND context = c.name AND context_version = c.version ORDER BY number DESC LIMIT 1) WHERE c.enabled = 1 AND c.name > ?2 AND ({ACCESS}) ORDER BY c.name LIMIT {CHECK_PAGE_SIZE}"), parameters,
            },
        ).await?;
        let output = result
            .output
            .map(|rows| {
                rows.iter()
                    .map(|row| {
                        if row.len() != 14 {
                            return Err(Error::Command("invalid commit checks row"));
                        }
                        Ok(CommitCheck {
                            context: context(&row[..4])?,
                            run: if row[4] == SqlValue::Null {
                                None
                            } else {
                                Some(run(&row[4..])?)
                            },
                        })
                    })
                    .collect()
            })
            .transpose()
            .map_err(Invocation::NotStarted)?;
        Ok(Observed {
            output,
            receipt: result.receipt,
        })
    }

    async fn check_rows(
        &self,
        check: SqlStatement,
        query: SqlStatement,
    ) -> Result<Observed<Option<Vec<Vec<SqlValue>>>>, Invocation> {
        let result = self
            .sql
            .query(
                None,
                SqlBatch {
                    statements: vec![check, query],
                },
            )
            .await?;
        let mut sets = result.output.into_iter();
        let allowed = sets.next().ok_or(Invocation::NotStarted(Error::Command(
            "missing check authorization",
        )))?;
        let output = match allowed.rows.first().map(Vec::as_slice) {
            Some([SqlValue::Integer(0)]) => None,
            Some([SqlValue::Integer(1)]) => Some(
                sets.next()
                    .ok_or(Invocation::NotStarted(Error::Command(
                        "missing checks page",
                    )))?
                    .rows,
            ),
            _ => {
                return Err(Invocation::NotStarted(Error::Command(
                    "invalid check authorization",
                )));
            }
        };
        Ok(Observed {
            output,
            receipt: result.receipt,
        })
    }
}

fn cursor_parameters<'a>(
    actor: impl Into<ReadIdentity<'a>>,
    after: Option<&str>,
) -> Result<Vec<SqlValue>, Invocation> {
    let actor = actor.into();
    actor.validate().map_err(Invocation::NotStarted)?;
    if let Some(after) = after {
        validate_component(after).map_err(Invocation::NotStarted)?;
    }
    Ok(vec![
        actor.parameter(),
        SqlValue::Text(after.unwrap_or("").into()),
    ])
}

fn context(row: &[SqlValue]) -> cellule_runtime::Result<CheckContext> {
    let [
        SqlValue::Text(name),
        SqlValue::Text(reporter),
        SqlValue::Integer(enabled),
        SqlValue::Integer(version),
    ] = row
    else {
        return Err(Error::Command("invalid check context"));
    };
    if ![0, 1].contains(enabled) || *version < 1 {
        return Err(Error::Command("invalid check context version"));
    }
    Ok(CheckContext {
        name: name.clone(),
        reporter: reporter.clone(),
        enabled: *enabled == 1,
        version: *version,
    })
}

fn run(row: &[SqlValue]) -> cellule_runtime::Result<CheckRun> {
    let [
        SqlValue::Blob(id),
        SqlValue::Blob(oid),
        SqlValue::Text(context),
        SqlValue::Integer(context_version),
        SqlValue::Text(reporter),
        SqlValue::Text(state),
        SqlValue::Integer(version),
        SqlValue::Text(summary),
        SqlValue::Integer(created),
        SqlValue::Integer(updated),
    ] = row
    else {
        return Err(Error::Command("invalid check run"));
    };
    let state = match state.as_str() {
        "queued" => CheckState::Queued,
        "in_progress" => CheckState::InProgress,
        "success" => CheckState::Success,
        "failure" => CheckState::Failure,
        "cancelled" => CheckState::Cancelled,
        _ => return Err(Error::Command("invalid check state")),
    };
    let id = id
        .as_slice()
        .try_into()
        .map_err(|_| Error::Command("invalid check UUID"))?;
    validate_repository_id(id)?;
    if oid.len() != 20 {
        return Err(Error::Command("invalid check commit OID"));
    }
    Ok(CheckRun {
        id: uuid::Uuid::from_bytes(id).to_string(),
        oid: hex::encode(oid),
        context: context.clone(),
        context_version: *context_version,
        reporter: reporter.clone(),
        state,
        version: *version,
        summary: summary.clone(),
        created_at_ms: *created,
        updated_at_ms: *updated,
    })
}
