//! Line discussions retain verified file anchors independently of live branches.

use super::*;
use crate::ReadIdentity;
use crate::git_read::{ComparisonTarget, Side, patch::LineAnchor};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};

pub(crate) const PAGE: usize = 16;
const THREAD_COLUMNS: &str = "number, id, author, body, resolved, version, created_ms, updated_ms, pull_version, source_oid, source_version, base_oid, base_version, merge_base, path, side, line, blob_oid";

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ThreadIntent {
    pub id: [u8; 16],
    pub target: ComparisonTarget,
    pub path_base64: String,
    pub side: Side,
    pub line: i64,
    pub body: String,
}
impl ThreadIntent {
    pub(crate) fn digest(&self, actor: &str, pull: i64) -> Vec<u8> {
        let snapshot = match &self.target {
            ComparisonTarget::Current { revision: r } => format!(
                "current:{}:{}:{}:{}:{}",
                r.pull_version, r.source_oid, r.source_version, r.base_oid, r.base_version
            ),
            ComparisonTarget::Review { number } => format!("review:{number}"),
            ComparisonTarget::Merged {} => "merged".into(),
            ComparisonTarget::Thread { number } => format!("thread:{number}"),
        };
        mutations::binding(&[
            "thread",
            actor,
            &pull.to_string(),
            &snapshot,
            &self.path_base64,
            self.side.as_str(),
            &self.line.to_string(),
            &self.body,
        ])
    }
}
#[derive(Serialize)]
pub(crate) struct Thread {
    pub number: i64,
    pub id: String,
    pub author: String,
    pub body: String,
    pub resolved: bool,
    pub version: i64,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
    pub anchor: LineAnchor,
}
#[derive(Serialize)]
pub(crate) struct Comment {
    pub number: i64,
    pub id: String,
    pub author: String,
    pub body: String,
    pub created_at_ms: i64,
}

impl RepositoryCell {
    pub(crate) async fn threads<'a>(
        &self,
        actor: impl Into<ReadIdentity<'a>>,
        pull: i64,
        after: i64,
    ) -> Result<Option<Vec<Thread>>, Invocation> {
        let actor = actor.into();
        actor.validate().map_err(Invocation::NotStarted)?;
        if pull < 1 || after < 0 {
            return Err(invalid("invalid thread page"));
        }
        let result = self.pull_rows(
            SqlStatement { sql: format!("SELECT ({ACCESS}) AND EXISTS (SELECT 1 FROM pull_requests WHERE number = ?2)"), parameters: vec![actor.parameter(), SqlValue::Integer(pull)] },
            SqlStatement { sql: format!("SELECT {THREAD_COLUMNS} FROM pull_threads WHERE pull_number = ?2 AND number > ?3 AND ({ACCESS}) ORDER BY number LIMIT {PAGE}"), parameters: vec![actor.parameter(), SqlValue::Integer(pull), SqlValue::Integer(after)] },
        ).await?;
        result
            .output
            .map(|rows| rows.iter().map(|row| thread(row)).collect())
            .transpose()
            .map_err(Invocation::NotStarted)
    }
    pub(crate) async fn thread<'a>(
        &self,
        actor: impl Into<ReadIdentity<'a>>,
        pull: i64,
        number: i64,
    ) -> Result<Option<Thread>, Invocation> {
        let actor = actor.into();
        actor.validate().map_err(Invocation::NotStarted)?;
        let result = self.sql.query(None, SqlBatch { statements: vec![SqlStatement {
            sql: format!("SELECT {THREAD_COLUMNS} FROM pull_threads WHERE pull_number = ?2 AND number = ?3 AND ({ACCESS})"),
            parameters: vec![actor.parameter(), SqlValue::Integer(pull), SqlValue::Integer(number)],
        }] }).await?;
        result
            .output
            .first()
            .ok_or_else(|| invalid("missing thread result"))?
            .rows
            .first()
            .map(|row| thread(row))
            .transpose()
            .map_err(Invocation::NotStarted)
    }
    pub(crate) async fn thread_revision<'a>(
        &self,
        actor: impl Into<ReadIdentity<'a>>,
        pull: i64,
        number: i64,
    ) -> Result<Option<PullRevision>, Invocation> {
        let actor = actor.into();
        actor.validate().map_err(Invocation::NotStarted)?;
        let result = self.sql.query(None, SqlBatch { statements: vec![SqlStatement {
            sql: format!("SELECT pull_version, source_oid, source_version, base_oid, base_version FROM pull_threads WHERE pull_number = ?2 AND number = ?3 AND ({ACCESS})"),
            parameters: vec![actor.parameter(), SqlValue::Integer(pull), SqlValue::Integer(number)],
        }] }).await?;
        result
            .output
            .first()
            .ok_or_else(|| invalid("missing thread revision"))?
            .rows
            .first()
            .map(|row| stored_revision(row))
            .transpose()
            .map_err(Invocation::NotStarted)
    }
    // Check durable identity before traversing live refs. A lost-reply retry may
    // arrive after the original revision has moved; it must not create anew.
    pub(crate) async fn thread_retry(
        &self,
        actor: &str,
        pull: i64,
        input: &ThreadIntent,
    ) -> Result<Option<PullChange>, Invocation> {
        validate_component(actor).map_err(Invocation::NotStarted)?;
        let result = self.sql.query(None, SqlBatch { statements: vec![SqlStatement {
            sql: format!("SELECT CASE WHEN NOT ({ACCESS}) OR NOT EXISTS (SELECT 1 FROM pull_requests WHERE number = ?2) THEN -1 WHEN EXISTS (SELECT 1 FROM pull_threads WHERE id = ?3 AND (pull_number != ?2 OR creation_digest != ?4)) THEN -2 ELSE coalesce((SELECT number FROM pull_threads WHERE id = ?3), 0) END"),
            parameters: vec![SqlValue::Text(actor.into()), SqlValue::Integer(pull), SqlValue::Blob(input.id.to_vec()), SqlValue::Blob(input.digest(actor, pull))],
        }] }).await?;
        match result
            .output
            .first()
            .and_then(|set| set.rows.first())
            .map(Vec::as_slice)
        {
            Some([SqlValue::Integer(-1)]) => Ok(Some(PullChange::NotFound)),
            Some([SqlValue::Integer(-2)]) => Ok(Some(PullChange::Conflict)),
            Some([SqlValue::Integer(0)]) => Ok(None),
            Some([SqlValue::Integer(n)]) if *n > 0 => Ok(Some(PullChange::Applied(*n))),
            _ => Err(invalid("invalid thread retry result")),
        }
    }
    pub(crate) async fn create_thread(
        &self,
        identity: MutationIdentity,
        actor: &str,
        pull: i64,
        input: ThreadIntent,
        anchor: LineAnchor,
    ) -> Result<Committed<PullChange>, native::NativePullError> {
        self.native_create_thread(identity, actor, pull, input, anchor)
            .await
    }
    pub(crate) async fn resolve_thread(
        &self,
        identity: MutationIdentity,
        actor: &str,
        pull: i64,
        number: i64,
        version: i64,
        resolved: bool,
    ) -> Result<Committed<PullChange>, Invocation> {
        validate_component(actor).map_err(Invocation::NotStarted)?;
        if pull < 1 || number < 1 || !(1..i64::MAX).contains(&version) {
            return Err(invalid("invalid thread edit"));
        }
        let mut parameters = vec![
            SqlValue::Text(actor.into()),
            SqlValue::Integer(pull),
            SqlValue::Integer(number),
            SqlValue::Integer(version),
        ];
        let decision = format!(
            "CASE WHEN NOT ({ACCESS}) OR NOT EXISTS (SELECT 1 FROM pull_threads WHERE pull_number = ?2 AND number = ?3) THEN 'missing' WHEN NOT ({WRITE}) AND NOT EXISTS (SELECT 1 FROM pull_threads WHERE number = ?3 AND author = ?1) AND NOT EXISTS (SELECT 1 FROM pull_requests WHERE number = ?2 AND author = ?1) THEN 'forbidden' WHEN NOT EXISTS (SELECT 1 FROM pull_threads WHERE number = ?3 AND version = ?4) THEN 'conflict' ELSE 'applied' END"
        );
        let check = SqlStatement {
            sql: format!("SELECT {decision}"),
            parameters: parameters.clone(),
        };
        parameters.extend([
            SqlValue::Integer(i64::from(resolved)),
            SqlValue::Integer(identity.issued_at_ms),
        ]);
        self.pull_change(identity, vec![check, SqlStatement { sql: format!("UPDATE pull_threads SET resolved = ?5, version = version + 1, updated_ms = max(updated_ms, ?6) WHERE number = ?3 AND ({decision}) = 'applied'"), parameters },
            SqlStatement { sql: "SELECT number FROM pull_threads WHERE number = ?1".into(), parameters: vec![SqlValue::Integer(number)] }]).await
    }
    pub(crate) async fn thread_comments<'a>(
        &self,
        actor: impl Into<ReadIdentity<'a>>,
        pull: i64,
        number: i64,
        after: i64,
    ) -> Result<Option<Vec<Comment>>, Invocation> {
        let actor = actor.into();
        actor.validate().map_err(Invocation::NotStarted)?;
        if after < 0 {
            return Err(invalid("invalid comment cursor"));
        }
        let guard = format!(
            "({ACCESS}) AND EXISTS (SELECT 1 FROM pull_threads WHERE pull_number = ?2 AND number = ?3)"
        );
        let mut parameters = vec![
            actor.parameter(),
            SqlValue::Integer(pull),
            SqlValue::Integer(number),
        ];
        let check = SqlStatement {
            sql: format!("SELECT {guard}"),
            parameters: parameters.clone(),
        };
        parameters.push(SqlValue::Integer(after));
        let result = self.pull_rows(check, SqlStatement { sql: format!("SELECT number, id, author, body, created_ms FROM pull_thread_comments WHERE thread_number = ?3 AND number > ?4 AND {guard} ORDER BY number LIMIT {PAGE}"), parameters }).await?;
        result
            .output
            .map(|rows| rows.iter().map(|row| comment(row)).collect())
            .transpose()
            .map_err(Invocation::NotStarted)
    }
    pub(crate) async fn reply_thread(
        &self,
        identity: MutationIdentity,
        actor: &str,
        pull: i64,
        number: i64,
        input: crate::issues::NewComment<'_>,
    ) -> Result<Committed<PullChange>, Invocation> {
        validate_component(actor).map_err(Invocation::NotStarted)?;
        validate_repository_id(input.id).map_err(Invocation::NotStarted)?;
        if pull < 1 || number < 1 || !valid_body(input.body) || input.body.trim().is_empty() {
            return Err(invalid("invalid thread reply"));
        }
        let mut parameters = vec![
            SqlValue::Text(actor.into()),
            SqlValue::Integer(pull),
            SqlValue::Integer(number),
            SqlValue::Blob(input.id.to_vec()),
            SqlValue::Blob(mutations::binding(&[
                "thread-reply",
                actor,
                &pull.to_string(),
                &number.to_string(),
                input.body,
            ])),
        ];
        let decision = format!(
            "CASE WHEN NOT ({ACCESS}) OR NOT EXISTS (SELECT 1 FROM pull_threads WHERE pull_number = ?2 AND number = ?3) THEN 'missing' WHEN EXISTS (SELECT 1 FROM pull_thread_comments WHERE id = ?4 AND (thread_number != ?3 OR creation_digest != ?5)) THEN 'conflict' ELSE 'applied' END"
        );
        let check = SqlStatement {
            sql: format!("SELECT {decision}"),
            parameters: parameters.clone(),
        };
        parameters.extend([
            SqlValue::Text(input.body.into()),
            SqlValue::Integer(identity.issued_at_ms),
        ]);
        self.pull_change(identity, vec![check, SqlStatement { sql: format!("INSERT INTO pull_thread_comments (thread_number, id, creation_digest, author, body, created_ms) SELECT ?3, ?4, ?5, ?1, ?6, ?7 WHERE ({decision}) = 'applied' AND NOT EXISTS (SELECT 1 FROM pull_thread_comments WHERE id = ?4)"), parameters },
            SqlStatement { sql: "SELECT number FROM pull_thread_comments WHERE id = ?1".into(), parameters: vec![SqlValue::Blob(input.id.to_vec())] }]).await
    }
}
fn thread(row: &[SqlValue]) -> cellule_runtime::Result<Thread> {
    let [
        SqlValue::Integer(number),
        SqlValue::Blob(id),
        SqlValue::Text(author),
        SqlValue::Text(body),
        SqlValue::Integer(resolved),
        SqlValue::Integer(version),
        SqlValue::Integer(created),
        SqlValue::Integer(updated),
        _,
        _,
        _,
        _,
        _,
        SqlValue::Blob(base),
        SqlValue::Blob(path),
        SqlValue::Text(side),
        SqlValue::Integer(line),
        SqlValue::Blob(blob),
    ] = row
    else {
        return Err(Error::Command("invalid thread record"));
    };
    let side = match side.as_str() {
        "before" => Side::Before,
        "after" => Side::After,
        _ => return Err(Error::Command("invalid thread side")),
    };
    Ok(Thread {
        number: *number,
        id: record_id(id)?,
        author: author.clone(),
        body: body.clone(),
        resolved: *resolved == 1,
        version: *version,
        created_at_ms: *created,
        updated_at_ms: *updated,
        anchor: LineAnchor {
            revision: stored_revision(&row[8..13])?,
            merge_base: hex::encode(base),
            path_base64: URL_SAFE_NO_PAD.encode(path),
            side,
            line: *line,
            blob_oid: hex::encode(blob),
        },
    })
}
fn comment(row: &[SqlValue]) -> cellule_runtime::Result<Comment> {
    let [
        SqlValue::Integer(number),
        SqlValue::Blob(id),
        SqlValue::Text(author),
        SqlValue::Text(body),
        SqlValue::Integer(created),
    ] = row
    else {
        return Err(Error::Command("invalid thread comment"));
    };
    Ok(Comment {
        number: *number,
        id: record_id(id)?,
        author: author.clone(),
        body: body.clone(),
        created_at_ms: *created,
    })
}

pub(in crate::pulls) fn creation_statements(
    actor: &str,
    pull: i64,
    input: &ThreadIntent,
    anchor: &LineAnchor,
    now_ms: i64,
) -> cellule_runtime::Result<Vec<SqlStatement>> {
    validate_component(actor)?;
    validate_repository_id(input.id)?;
    if pull < 1
        || !valid_body(&input.body)
        || input.body.trim().is_empty()
        || input.path_base64 != anchor.path_base64
        || input.side != anchor.side
        || input.line != anchor.line
    {
        return Err(Error::Command("thread intent differs from verified anchor"));
    }
    let r = &anchor.revision;
    let mut parameters = vec![
        SqlValue::Text(actor.into()),
        SqlValue::Integer(pull),
        SqlValue::Blob(input.id.to_vec()),
        SqlValue::Blob(input.digest(actor, pull)),
        SqlValue::Integer(r.pull_version),
        SqlValue::Blob(parse_oid(&r.source_oid).ok_or(Error::Command("invalid thread source"))?),
        SqlValue::Integer(r.source_version),
        SqlValue::Blob(parse_oid(&r.base_oid).ok_or(Error::Command("invalid thread base"))?),
        SqlValue::Integer(r.base_version),
    ];
    let eligible = match &input.target {
            ComparisonTarget::Current { .. } => format!("EXISTS (SELECT 1 FROM {JOINS} WHERE p.number = ?2 AND p.version = ?5 AND s.oid = ?6 AND s.version = ?7 AND b.oid = ?8 AND b.version = ?9)"),
            ComparisonTarget::Review { number } => { parameters.push(SqlValue::Integer(*number)); "EXISTS (SELECT 1 FROM pull_reviews WHERE pull_number = ?2 AND number = ?10 AND pull_version = ?5 AND source_oid = ?6 AND source_version = ?7 AND base_oid = ?8 AND base_version = ?9)".into() },
            ComparisonTarget::Merged {} => "EXISTS (SELECT 1 FROM pull_merges WHERE pull_number = ?2 AND pull_version = ?5 AND source_oid = ?6 AND source_version = ?7 AND base_oid = ?8 AND base_version = ?9)".into(),
            ComparisonTarget::Thread { .. } => return Err(Error::Command("threads are not creation targets")),
        };
    let decision = format!(
        "CASE WHEN NOT ({ACCESS}) OR NOT EXISTS (SELECT 1 FROM pull_requests WHERE number = ?2) THEN 'missing' WHEN EXISTS (SELECT 1 FROM pull_threads WHERE id = ?3 AND (pull_number != ?2 OR creation_digest != ?4)) THEN 'conflict' WHEN EXISTS (SELECT 1 FROM pull_threads WHERE id = ?3) THEN 'applied' WHEN NOT ({eligible}) THEN 'conflict' ELSE 'applied' END"
    );
    let check = SqlStatement {
        sql: format!("SELECT {decision}"),
        parameters: parameters.clone(),
    };
    parameters.resize(10, SqlValue::Null);
    parameters.extend([
        SqlValue::Text(input.body.clone()),
        SqlValue::Blob(
            parse_oid(&anchor.merge_base).ok_or(Error::Command("invalid thread merge base"))?,
        ),
        SqlValue::Blob(
            URL_SAFE_NO_PAD
                .decode(&anchor.path_base64)
                .map_err(|_| Error::Command("invalid verified path"))?,
        ),
        SqlValue::Text(anchor.side.as_str().into()),
        SqlValue::Integer(anchor.line),
        SqlValue::Blob(parse_oid(&anchor.blob_oid).ok_or(Error::Command("invalid thread blob"))?),
        SqlValue::Integer(now_ms),
    ]);
    Ok(vec![
        check,
        SqlStatement {
            sql: format!(
                "INSERT INTO pull_threads (id, creation_digest, pull_number, author, body, resolved, version, pull_version, source_oid, source_version, base_oid, base_version, merge_base, path, side, line, blob_oid, created_ms, updated_ms) SELECT ?3, ?4, ?2, ?1, ?11, 0, 1, ?5, ?6, ?7, ?8, ?9, ?12, ?13, ?14, ?15, ?16, ?17, ?17 WHERE ({decision}) = 'applied' AND NOT EXISTS (SELECT 1 FROM pull_threads WHERE id = ?3)"
            ),
            parameters,
        },
        SqlStatement {
            sql: "SELECT number FROM pull_threads WHERE id = ?1".into(),
            parameters: vec![SqlValue::Blob(input.id.to_vec())],
        },
    ])
}
