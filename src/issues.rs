//! Repository-local issues and discussion, stored with the repository ACL.

use crate::ReadIdentity;

mod mutations;

use crab_cell_runtime::{
    Committed, Error, InvocationError, MutationIdentity, Observed, primitives::sql::SqlBatch,
    primitives::sql::SqlResultSet, primitives::sql::SqlStatement, primitives::sql::SqlValue,
};
use serde::{Deserialize, Serialize};

use crate::{RepositoryCell, validate_repository_id};

pub const ISSUE_PAGE_SIZE: usize = 32;
pub const COMMENT_PAGE_SIZE: usize = 16;
pub const ISSUE_BODY_LIMIT: usize = 16 * 1024;
const TITLE_LIMIT: usize = 256;
use crate::access::READ_ACCESS;
const WRITE_ACCESS: &str = "EXISTS (SELECT 1 FROM repository_identity WHERE owner = ?1) OR EXISTS (SELECT 1 FROM repository_members WHERE account = ?1 AND role = 'write')";
const SUMMARY_COLUMNS: &str = "number, id, author, title, state, version, created_ms, updated_ms";

type Invocation = InvocationError<Vec<SqlResultSet>>;

/// Current issue lifecycle state.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum IssueState {
    Open,
    Closed,
}

impl IssueState {
    fn as_str(self) -> &'static str {
        match self {
            Self::Open => "open",
            Self::Closed => "closed",
        }
    }
}

/// Bounded issue metadata returned in number-ordered lists.
#[derive(Debug, PartialEq, Eq, Serialize)]
pub struct IssueSummary {
    pub number: i64,
    pub id: String,
    pub author: String,
    pub title: String,
    pub state: IssueState,
    pub version: i64,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

/// One issue with its unrendered text body.
#[derive(Debug, PartialEq, Eq, Serialize)]
pub struct Issue {
    #[serde(flatten)]
    pub summary: IssueSummary,
    pub body: String,
}

/// One discussion comment; its number is unique within the repository.
#[derive(Debug, PartialEq, Eq, Serialize)]
pub struct IssueComment {
    pub number: i64,
    pub id: String,
    pub author: String,
    pub body: String,
    pub version: i64,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

/// Immutable creation identity and initial issue contents.
pub struct NewIssue<'a> {
    pub id: [u8; 16],
    pub title: &'a str,
    pub body: &'a str,
}

/// Complete replacement of editable issue fields at one expected version.
pub struct IssueEdit<'a> {
    pub expected_version: i64,
    pub title: &'a str,
    pub body: &'a str,
    pub state: IssueState,
}

/// Immutable creation identity and initial comment contents.
pub struct NewComment<'a> {
    pub id: [u8; 16],
    pub body: &'a str,
}

/// Replacement comment text at one expected version.
pub struct CommentEdit<'a> {
    pub expected_version: i64,
    pub body: &'a str,
}

/// Published mutation result, including the issue or comment number on success.
#[derive(Debug, PartialEq, Eq)]
pub enum IssueChange {
    Applied(i64),
    NotFound,
    Forbidden,
    Conflict,
}

pub(crate) fn valid_issue_text(title: &str, body: &str) -> bool {
    !title.trim().is_empty()
        && title.len() <= TITLE_LIMIT
        && !title.chars().any(char::is_control)
        && valid_body(body)
}

pub(crate) fn valid_body(body: &str) -> bool {
    body.len() <= ISSUE_BODY_LIMIT && !body.contains('\0')
}

fn actor_parameters<'a>(
    actor: impl Into<ReadIdentity<'a>>,
) -> crab_cell_runtime::Result<Vec<SqlValue>> {
    let actor = actor.into();
    actor.validate()?;
    Ok(vec![actor.parameter()])
}

impl RepositoryCell {
    /// Lists up to 32 issue summaries after a number, optionally filtered by state.
    ///
    /// Returns `None` without repository access. Pages are independent observations.
    pub async fn issues<'a>(
        &self,
        actor: impl Into<ReadIdentity<'a>>,
        after: i64,
        state: Option<IssueState>,
    ) -> Result<Observed<Option<Vec<IssueSummary>>>, Invocation> {
        let actor = actor.into();
        if after < 0 {
            return Err(Invocation::NotStarted(Error::Command(
                "invalid issue cursor",
            )));
        }
        let parameters = actor_parameters(actor).map_err(Invocation::NotStarted)?;
        let check = SqlStatement {
            sql: format!("SELECT ({READ_ACCESS})"),
            parameters: parameters.clone(),
        };
        let mut parameters = parameters;
        parameters.push(SqlValue::Integer(after));
        let filter = if let Some(state) = state {
            parameters.push(SqlValue::Text(state.as_str().into()));
            "AND state = ?3"
        } else {
            ""
        };
        let page = self.issue_rows(check, SqlStatement {
            sql: format!("SELECT {SUMMARY_COLUMNS} FROM issues WHERE number > ?2 {filter} AND ({READ_ACCESS}) ORDER BY number LIMIT {ISSUE_PAGE_SIZE}"),
            parameters,
        }).await?;
        let output = page
            .output
            .map(|rows| rows.iter().map(|row| summary(row)).collect())
            .transpose()
            .map_err(Invocation::NotStarted)?;
        Ok(Observed {
            output,
            receipt: page.receipt,
        })
    }

    /// Reads an issue only while the actor has repository access.
    ///
    /// Missing issues and missing access both return `None`.
    pub async fn issue<'a>(
        &self,
        actor: impl Into<ReadIdentity<'a>>,
        number: i64,
    ) -> Result<Observed<Option<Issue>>, Invocation> {
        let actor = actor.into();
        let mut parameters = actor_parameters(actor).map_err(Invocation::NotStarted)?;
        parameters.push(SqlValue::Integer(number));
        let observed = self.sql.query(None, SqlBatch { statements: vec![SqlStatement {
            sql: format!("SELECT {SUMMARY_COLUMNS}, body FROM issues WHERE number = ?2 AND ({READ_ACCESS})"),
            parameters,
        }] }).await?;
        let rows = observed
            .output
            .first()
            .ok_or(Invocation::NotStarted(Error::Command(
                "missing issue result",
            )))?;
        let output = rows
            .rows
            .first()
            .map(|row| {
                let Some(SqlValue::Text(body)) = row.get(8) else {
                    return Err(Error::Command("invalid issue body"));
                };
                Ok(Issue {
                    summary: summary(&row[..8])?,
                    body: body.clone(),
                })
            })
            .transpose()
            .map_err(Invocation::NotStarted)?;
        Ok(Observed {
            output,
            receipt: observed.receipt,
        })
    }

    /// Lists up to 16 comments on an accessible issue, after a comment number.
    ///
    /// Returns `None` for a missing issue or missing repository access.
    pub async fn issue_comments<'a>(
        &self,
        actor: impl Into<ReadIdentity<'a>>,
        number: i64,
        after: i64,
    ) -> Result<Observed<Option<Vec<IssueComment>>>, Invocation> {
        let actor = actor.into();
        if after < 0 {
            return Err(Invocation::NotStarted(Error::Command(
                "invalid comment cursor",
            )));
        }
        let mut parameters = actor_parameters(actor).map_err(Invocation::NotStarted)?;
        parameters.push(SqlValue::Integer(number));
        let check = SqlStatement {
            sql: format!(
                "SELECT ({READ_ACCESS}) AND EXISTS (SELECT 1 FROM issues WHERE number = ?2)"
            ),
            parameters: parameters.clone(),
        };
        parameters.push(SqlValue::Integer(after));
        let page = self.issue_rows(check, SqlStatement {
            sql: format!("SELECT number, id, author, body, version, created_ms, updated_ms FROM issue_comments WHERE issue_number = ?2 AND number > ?3 AND ({READ_ACCESS}) ORDER BY number LIMIT {COMMENT_PAGE_SIZE}"),
            parameters,
        }).await?;
        let output = page
            .output
            .map(|rows| rows.iter().map(|row| comment(row)).collect())
            .transpose()
            .map_err(Invocation::NotStarted)?;
        Ok(Observed {
            output,
            receipt: page.receipt,
        })
    }

    async fn issue_rows(
        &self,
        check: SqlStatement,
        page: SqlStatement,
    ) -> Result<Observed<Option<Vec<Vec<SqlValue>>>>, Invocation> {
        let observed = self
            .sql
            .query(
                None,
                SqlBatch {
                    statements: vec![check, page],
                },
            )
            .await?;
        let mut sets = observed.output.into_iter();
        let allowed = sets.next().ok_or(Invocation::NotStarted(Error::Command(
            "missing issue authorization",
        )))?;
        let output = match allowed.rows.first().map(Vec::as_slice) {
            Some([SqlValue::Integer(1)]) => Some(
                sets.next()
                    .ok_or(Invocation::NotStarted(Error::Command("missing issue page")))?
                    .rows,
            ),
            Some([SqlValue::Integer(0)]) => None,
            _ => {
                return Err(Invocation::NotStarted(Error::Command(
                    "invalid issue authorization",
                )));
            }
        };
        Ok(Observed {
            output,
            receipt: observed.receipt,
        })
    }
}

fn summary(row: &[SqlValue]) -> crab_cell_runtime::Result<IssueSummary> {
    let [
        SqlValue::Integer(number),
        SqlValue::Blob(id),
        SqlValue::Text(author),
        SqlValue::Text(title),
        SqlValue::Text(state),
        SqlValue::Integer(version),
        SqlValue::Integer(created),
        SqlValue::Integer(updated),
    ] = row
    else {
        return Err(Error::Command("invalid issue summary"));
    };
    let state = match state.as_str() {
        "open" => IssueState::Open,
        "closed" => IssueState::Closed,
        _ => return Err(Error::Command("invalid issue state")),
    };
    Ok(IssueSummary {
        number: *number,
        id: record_id(id)?,
        author: author.clone(),
        title: title.clone(),
        state,
        version: *version,
        created_at_ms: *created,
        updated_at_ms: *updated,
    })
}

fn comment(row: &[SqlValue]) -> crab_cell_runtime::Result<IssueComment> {
    let [
        SqlValue::Integer(number),
        SqlValue::Blob(id),
        SqlValue::Text(author),
        SqlValue::Text(body),
        SqlValue::Integer(version),
        SqlValue::Integer(created),
        SqlValue::Integer(updated),
    ] = row
    else {
        return Err(Error::Command("invalid issue comment"));
    };
    Ok(IssueComment {
        number: *number,
        id: record_id(id)?,
        author: author.clone(),
        body: body.clone(),
        version: *version,
        created_at_ms: *created,
        updated_at_ms: *updated,
    })
}

fn record_id(bytes: &[u8]) -> crab_cell_runtime::Result<String> {
    let id = bytes
        .try_into()
        .map_err(|_| Error::Command("invalid issue identity"))?;
    validate_repository_id(id)?;
    Ok(uuid::Uuid::from_bytes(id).to_string())
}
