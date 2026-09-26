//! Repository-local pull requests and immutable reviews tied to live ref versions.

use crate::ReadIdentity;

pub mod candidates;
pub mod merge;
mod mutations;
pub(crate) mod threads;

use crate::{
    RepositoryCell,
    default_branch::valid_default_branch,
    directory::validate_component,
    issues::{valid_body, valid_issue_text},
    validate_repository_id,
};
use cellule_runtime::{
    Committed, Error, InvocationError, MutationIdentity, Observed, SqlBatch, SqlResultSet,
    SqlStatement, SqlValue,
};
use serde::{Deserialize, Serialize};

type Invocation = InvocationError<Vec<SqlResultSet>>;
pub const PULL_PAGE_SIZE: usize = 32;
pub const REVIEW_PAGE_SIZE: usize = 16;
use crate::access::READ_ACCESS as ACCESS;
const WRITE: &str = "EXISTS (SELECT 1 FROM repository_identity WHERE owner = ?1) OR EXISTS (SELECT 1 FROM repository_members WHERE account = ?1 AND role = 'write')";
const JOINS: &str =
    "pull_requests p JOIN refs s ON s.name = p.source_ref JOIN refs b ON b.name = p.base_ref";
const COLUMNS: &str = "p.number, p.id, p.author, p.title, p.state, p.draft, p.version, p.source_ref, s.oid, s.version, p.base_ref, b.oid, b.version, p.created_ms, p.updated_ms";

// One eligibility predicate serves history, review policy reads and merge publication.
const APPLICABLE: &str = "r.kind != 'comment' AND p.state = 'open' AND p.draft = 0 AND r.pull_version = p.version AND r.source_oid = s.oid AND r.source_version = s.version AND r.base_oid = b.oid AND r.base_version = b.version AND r.reviewer != p.author AND (EXISTS (SELECT 1 FROM repository_identity WHERE owner = r.reviewer AND r.membership_version = 0) OR EXISTS (SELECT 1 FROM repository_members m JOIN membership_versions v ON v.account = m.account WHERE m.account = r.reviewer AND m.role = 'write' AND v.version = r.membership_version)) AND r.number = (SELECT review_number FROM pull_review_heads WHERE pull_number = p.number AND reviewer = r.reviewer)";

/// Pull lifecycle state; only merge publication can enter the terminal Merged state.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PullState {
    Open,
    Closed,
    Merged,
}
impl PullState {
    fn as_str(self) -> &'static str {
        match self {
            Self::Open => "open",
            Self::Closed => "closed",
            Self::Merged => "merged",
        }
    }
}
/// One branch's current live or deleted ref snapshot.
#[derive(Debug, PartialEq, Eq, Serialize)]
pub struct PullBranch {
    pub reference: String,
    pub oid: Option<String>,
    pub version: i64,
}
/// Bounded pull metadata observed with both branches in one Cell query.
#[derive(Debug, PartialEq, Eq, Serialize)]
pub struct PullSummary {
    pub number: i64,
    pub id: String,
    pub author: String,
    pub title: String,
    pub state: PullState,
    pub draft: bool,
    pub version: i64,
    pub source: PullBranch,
    pub base: PullBranch,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}
/// Pull details retain the original commit identities even after branch deletion.
#[derive(Debug, PartialEq, Eq, Serialize)]
pub struct PullRequest {
    #[serde(flatten)]
    pub summary: PullSummary,
    pub body: String,
    pub initial_source_oid: String,
    pub initial_base_oid: String,
    pub merge: Option<merge::MergeRecord>,
}
/// Exact pull/ref revision reviewed by a client; ref versions prevent ABA reuse.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PullRevision {
    pub pull_version: i64,
    pub source_oid: String,
    pub source_version: i64,
    pub base_oid: String,
    pub base_version: i64,
}
/// Immutable review intent; a comment does not replace an approval or objection.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReviewKind {
    Comment,
    Approve,
    RequestChanges,
}
impl ReviewKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Comment => "comment",
            Self::Approve => "approve",
            Self::RequestChanges => "request_changes",
        }
    }
}
/// One historical review and whether its decision applies to the current revision.
#[derive(Debug, PartialEq, Eq, Serialize)]
pub struct PullReview {
    pub number: i64,
    pub id: String,
    pub reviewer: String,
    pub kind: ReviewKind,
    pub body: String,
    pub revision: PullRevision,
    pub applicable: bool,
    pub created_at_ms: i64,
}
/// Immutable creation identity, contents and expected current branch tips.
pub struct NewPull<'a> {
    pub id: [u8; 16],
    pub title: &'a str,
    pub body: &'a str,
    pub draft: bool,
    pub source_ref: &'a str,
    pub source_oid: &'a str,
    pub base_ref: &'a str,
    pub base_oid: &'a str,
}
/// Replacement editorial content at the expected pull version; refs remain fixed.
pub struct PullEdit<'a> {
    pub expected_version: i64,
    pub title: &'a str,
    pub body: &'a str,
    pub state: PullState,
    pub draft: bool,
}
/// Review identity and exact revision supplied by a client that inspected the pull.
pub struct NewReview<'a> {
    pub id: [u8; 16],
    pub revision: &'a PullRevision,
    pub kind: ReviewKind,
    pub body: &'a str,
}
/// Published domain result; successful creation includes its durable number.
#[derive(Debug, PartialEq, Eq)]
pub enum PullChange {
    Applied(i64),
    NotFound,
    Forbidden,
    Conflict,
}

pub(crate) fn parse_oid(value: &str) -> Option<Vec<u8>> {
    if value.len() != 40
        || !value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return None;
    }
    hex::decode(value).ok()
}
pub(crate) fn valid_new(input: &NewPull<'_>) -> bool {
    valid_issue_text(input.title, input.body)
        && valid_default_branch(input.source_ref)
        && valid_default_branch(input.base_ref)
        && input.source_ref != input.base_ref
        && parse_oid(input.source_oid).is_some()
        && parse_oid(input.base_oid).is_some()
}
pub(crate) fn valid_review(input: &NewReview<'_>) -> bool {
    valid_body(input.body)
        && (input.kind != ReviewKind::Comment || !input.body.trim().is_empty())
        && input.revision.pull_version > 0
        && input.revision.source_version > 0
        && input.revision.base_version > 0
        && parse_oid(&input.revision.source_oid).is_some()
        && parse_oid(&input.revision.base_oid).is_some()
}

impl RepositoryCell {
    /// Lists 32 pull summaries with coherent current branches; missing access returns None.
    pub async fn pulls<'a>(
        &self,
        actor: impl Into<ReadIdentity<'a>>,
        after: i64,
        state: Option<PullState>,
    ) -> Result<Observed<Option<Vec<PullSummary>>>, Invocation> {
        let actor = actor.into();
        if after < 0 {
            return Err(invalid("invalid pull cursor"));
        }
        actor.validate().map_err(Invocation::NotStarted)?;
        let mut parameters = vec![actor.parameter(), SqlValue::Integer(after)];
        let filter = if let Some(state) = state {
            parameters.push(SqlValue::Text(state.as_str().into()));
            "AND p.state = ?3"
        } else {
            ""
        };
        let result = self.pull_rows(
            SqlStatement { sql: format!("SELECT ({ACCESS})"), parameters: vec![actor.parameter()] },
            SqlStatement { sql: format!("SELECT {COLUMNS} FROM {JOINS} WHERE p.number > ?2 {filter} AND ({ACCESS}) ORDER BY p.number LIMIT {PULL_PAGE_SIZE}"), parameters },
        ).await?;
        let output = result
            .output
            .map(|rows| rows.iter().map(|row| summary(row)).collect())
            .transpose()
            .map_err(Invocation::NotStarted)?;
        Ok(Observed {
            output,
            receipt: result.receipt,
        })
    }
    /// Reads a pull, original tips and live ref state under current read membership.
    pub async fn pull<'a>(
        &self,
        actor: impl Into<ReadIdentity<'a>>,
        number: i64,
    ) -> Result<Observed<Option<PullRequest>>, Invocation> {
        let actor = actor.into();
        actor.validate().map_err(Invocation::NotStarted)?;
        let result = self.sql.query(None, SqlBatch { statements: vec![SqlStatement {
            sql: format!("SELECT {COLUMNS}, p.body, p.initial_source_oid, p.initial_base_oid, merged.id, merged.pull_number, merged.oid, merged.merged_ms, merged.pull_version, merged.source_oid, merged.source_version, merged.base_oid, merged.base_version FROM {JOINS} LEFT JOIN pull_merges merged ON merged.pull_number = p.number WHERE p.number = ?2 AND ({ACCESS})"),
            parameters: vec![actor.parameter(), SqlValue::Integer(number)],
        }] }).await?;
        let rows = result
            .output
            .first()
            .ok_or_else(|| invalid("missing pull result"))?;
        let output = rows
            .rows
            .first()
            .map(|row| {
                let [
                    SqlValue::Text(body),
                    SqlValue::Blob(source),
                    SqlValue::Blob(base),
                ] = row
                    .get(15..18)
                    .ok_or(Error::Command("invalid pull details"))?
                else {
                    return Err(Error::Command("invalid pull details"));
                };
                Ok(PullRequest {
                    summary: summary(&row[..15])?,
                    body: body.clone(),
                    initial_source_oid: hex::encode(source),
                    initial_base_oid: hex::encode(base),
                    merge: match row.get(18..) {
                        Some(values)
                            if values.len() == 9
                                && values.iter().all(|value| matches!(value, SqlValue::Null)) =>
                        {
                            None
                        }
                        Some(values) => Some(merge::record(values)?),
                        None => return Err(Error::Command("missing pull merge details")),
                    },
                })
            })
            .transpose()
            .map_err(Invocation::NotStarted)?;
        Ok(Observed {
            output,
            receipt: result.receipt,
        })
    }
    /// Reads 16 immutable reviews with current applicability; missing pull/access returns None.
    pub async fn pull_reviews<'a>(
        &self,
        actor: impl Into<ReadIdentity<'a>>,
        number: i64,
        after: i64,
    ) -> Result<Observed<Option<Vec<PullReview>>>, Invocation> {
        let actor = actor.into();
        if after < 0 {
            return Err(invalid("invalid review cursor"));
        }
        actor.validate().map_err(Invocation::NotStarted)?;
        let result = self.pull_rows(
            SqlStatement { sql: format!("SELECT ({ACCESS}) AND EXISTS (SELECT 1 FROM pull_requests WHERE number = ?2)"), parameters: vec![actor.parameter(), SqlValue::Integer(number)] },
            SqlStatement { sql: format!("SELECT r.number, r.id, r.reviewer, r.kind, r.body, r.pull_version, r.source_oid, r.source_version, r.base_oid, r.base_version, coalesce(({APPLICABLE}), 0), r.created_ms FROM pull_reviews r JOIN pull_requests p ON p.number = r.pull_number JOIN refs s ON s.name = p.source_ref JOIN refs b ON b.name = p.base_ref WHERE r.pull_number = ?2 AND r.number > ?3 AND ({ACCESS}) ORDER BY r.number LIMIT {REVIEW_PAGE_SIZE}"), parameters: vec![actor.parameter(), SqlValue::Integer(number), SqlValue::Integer(after)] },
        ).await?;
        let output = result
            .output
            .map(|rows| rows.iter().map(|row| review(row)).collect())
            .transpose()
            .map_err(Invocation::NotStarted)?;
        Ok(Observed {
            output,
            receipt: result.receipt,
        })
    }
    pub(crate) async fn reviewed_revision<'a>(
        &self,
        actor: impl Into<ReadIdentity<'a>>,
        number: i64,
        review: i64,
    ) -> Result<Option<PullRevision>, Invocation> {
        let actor = actor.into();
        actor.validate().map_err(Invocation::NotStarted)?;
        let result = self.sql.query(None, SqlBatch { statements: vec![SqlStatement {
            sql: format!("SELECT pull_version, source_oid, source_version, base_oid, base_version FROM pull_reviews WHERE pull_number = ?2 AND number = ?3 AND ({ACCESS})"),
            parameters: vec![actor.parameter(), SqlValue::Integer(number), SqlValue::Integer(review)],
        }] }).await?;
        result
            .output
            .first()
            .ok_or_else(|| invalid("missing historical review result"))?
            .rows
            .first()
            .map(|row| stored_revision(row))
            .transpose()
            .map_err(Invocation::NotStarted)
    }
    async fn pull_rows(
        &self,
        check: SqlStatement,
        page: SqlStatement,
    ) -> Result<Observed<Option<Vec<Vec<SqlValue>>>>, Invocation> {
        let result = self
            .sql
            .query(
                None,
                SqlBatch {
                    statements: vec![check, page],
                },
            )
            .await?;
        let mut sets = result.output.into_iter();
        let access = sets.next().ok_or_else(|| invalid("missing pull access"))?;
        let output = match access.rows.first().map(Vec::as_slice) {
            Some([SqlValue::Integer(0)]) => None,
            Some([SqlValue::Integer(1)]) => Some(
                sets.next()
                    .ok_or_else(|| invalid("missing pull page"))?
                    .rows,
            ),
            _ => return Err(invalid("invalid pull access result")),
        };
        Ok(Observed {
            output,
            receipt: result.receipt,
        })
    }
}
fn invalid(message: &'static str) -> Invocation {
    Invocation::NotStarted(Error::Command(message))
}
fn summary(row: &[SqlValue]) -> cellule_runtime::Result<PullSummary> {
    let [
        SqlValue::Integer(number),
        SqlValue::Blob(id),
        SqlValue::Text(author),
        SqlValue::Text(title),
        SqlValue::Text(state),
        SqlValue::Integer(draft),
        SqlValue::Integer(version),
        SqlValue::Text(source),
        source_oid,
        SqlValue::Integer(source_version),
        SqlValue::Text(base),
        base_oid,
        SqlValue::Integer(base_version),
        SqlValue::Integer(created),
        SqlValue::Integer(updated),
    ] = row
    else {
        return Err(Error::Command("invalid pull summary"));
    };
    let state = match state.as_str() {
        "open" => PullState::Open,
        "closed" => PullState::Closed,
        "merged" => PullState::Merged,
        _ => return Err(Error::Command("invalid pull state")),
    };
    Ok(PullSummary {
        number: *number,
        id: record_id(id)?,
        author: author.clone(),
        title: title.clone(),
        state,
        draft: *draft == 1,
        version: *version,
        source: PullBranch {
            reference: source.clone(),
            oid: optional_oid(source_oid)?,
            version: *source_version,
        },
        base: PullBranch {
            reference: base.clone(),
            oid: optional_oid(base_oid)?,
            version: *base_version,
        },
        created_at_ms: *created,
        updated_at_ms: *updated,
    })
}
fn optional_oid(value: &SqlValue) -> cellule_runtime::Result<Option<String>> {
    match value {
        SqlValue::Null => Ok(None),
        SqlValue::Blob(oid) if oid.len() == 20 => Ok(Some(hex::encode(oid))),
        _ => Err(Error::Command("invalid pull tip")),
    }
}
fn record_id(value: &[u8]) -> cellule_runtime::Result<String> {
    let bytes = value
        .try_into()
        .map_err(|_| Error::Command("invalid pull record UUID"))?;
    validate_repository_id(bytes)?;
    Ok(uuid::Uuid::from_bytes(bytes).to_string())
}
fn review(row: &[SqlValue]) -> cellule_runtime::Result<PullReview> {
    let [
        SqlValue::Integer(number),
        SqlValue::Blob(id),
        SqlValue::Text(reviewer),
        SqlValue::Text(kind),
        SqlValue::Text(body),
        SqlValue::Integer(pull_version),
        SqlValue::Blob(source_oid),
        SqlValue::Integer(source_version),
        SqlValue::Blob(base_oid),
        SqlValue::Integer(base_version),
        SqlValue::Integer(applicable),
        SqlValue::Integer(created),
    ] = row
    else {
        return Err(Error::Command("invalid pull review"));
    };
    let kind = match kind.as_str() {
        "comment" => ReviewKind::Comment,
        "approve" => ReviewKind::Approve,
        "request_changes" => ReviewKind::RequestChanges,
        _ => return Err(Error::Command("invalid review kind")),
    };
    Ok(PullReview {
        number: *number,
        id: record_id(id)?,
        reviewer: reviewer.clone(),
        kind,
        body: body.clone(),
        revision: PullRevision {
            pull_version: *pull_version,
            source_oid: hex::encode(source_oid),
            source_version: *source_version,
            base_oid: hex::encode(base_oid),
            base_version: *base_version,
        },
        applicable: *applicable == 1,
        created_at_ms: *created,
    })
}

fn stored_revision(row: &[SqlValue]) -> cellule_runtime::Result<PullRevision> {
    let [
        SqlValue::Integer(pull_version),
        SqlValue::Blob(source_oid),
        SqlValue::Integer(source_version),
        SqlValue::Blob(base_oid),
        SqlValue::Integer(base_version),
    ] = row
    else {
        return Err(Error::Command("invalid stored pull revision"));
    };
    if *pull_version < 1
        || *source_version < 1
        || *base_version < 1
        || source_oid.len() != 20
        || base_oid.len() != 20
    {
        return Err(Error::Command("invalid stored pull revision fields"));
    }
    Ok(PullRevision {
        pull_version: *pull_version,
        source_oid: hex::encode(source_oid),
        source_version: *source_version,
        base_oid: hex::encode(base_oid),
        base_version: *base_version,
    })
}
