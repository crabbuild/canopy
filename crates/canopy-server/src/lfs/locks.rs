use super::*;
use crate::access::{READ_ACCESS, ReadIdentity};
use serde::{Deserialize, Serialize};

const WRITE_ACCESS: &str = "EXISTS (SELECT 1 FROM repository_identity WHERE owner = ?1) OR EXISTS (SELECT 1 FROM repository_members WHERE account = ?1 AND role = 'write')";
const COLUMNS: &str = "id, path, locked_at, owner";

#[derive(Debug, Serialize)]
pub(crate) struct Lock {
    pub id: String,
    pub path: String,
    pub locked_at: String,
    pub owner: Owner,
}

#[derive(Debug, Serialize)]
pub(crate) struct Owner {
    pub name: String,
}

#[derive(Default, Deserialize)]
pub(crate) struct LockQuery {
    pub path: Option<String>,
    pub id: Option<String>,
    pub cursor: Option<String>,
    pub limit: Option<u32>,
}

pub(crate) struct LockPage {
    pub locks: Vec<Lock>,
    pub next_cursor: Option<String>,
}

impl LfsService {
    pub(crate) async fn create_lock(
        &self,
        actor: &str,
        path: &str,
    ) -> Result<(bool, Lock), LfsError> {
        validate_path(path)?;
        let identity = mutation_identity()?;
        let id = uuid::Uuid::new_v4().to_string();
        // The conflict row and insertion outcome share the mutation transaction.
        // A separate lookup could return a replacement lock after an unlock race.
        let result = self.repository.sql.batch(identity, SqlBatch { statements: vec![
            access_statement(actor),
            SqlStatement {
                sql: format!("INSERT INTO lfs_locks (id, path, locked_at, owner) SELECT ?2, ?3, strftime('%Y-%m-%dT%H:%M:%SZ', ?4 / 1000, 'unixepoch'), ?1 WHERE ({WRITE_ACCESS}) ON CONFLICT(path) DO NOTHING"),
                parameters: vec![SqlValue::Text(actor.into()), SqlValue::Text(id), SqlValue::Text(path.into()), SqlValue::Integer(identity.issued_at_ms)],
            },
            SqlStatement {
                sql: format!("SELECT {COLUMNS} FROM lfs_locks WHERE path = ?2 AND ({WRITE_ACCESS})"),
                parameters: vec![SqlValue::Text(actor.into()), SqlValue::Text(path.into())],
            },
        ] }).await.map_err(|error| LfsError::Cell(Box::new(error)))?;
        authorize(&result.output, TokenScope::Write)?;
        let created = result.output.get(1).ok_or(LfsError::Corrupt)?.rows_affected == 1;
        let row = result
            .output
            .get(2)
            .and_then(|set| set.rows.first())
            .ok_or(LfsError::Corrupt)?;
        Ok((created, decode(row)?))
    }

    pub(crate) async fn unlock(
        &self,
        actor: &str,
        id: &str,
        force: bool,
    ) -> Result<Lock, LfsError> {
        validate_id(id)?;
        // Read before deletion inside the same transaction: the response must
        // identify exactly the lock removed, and revoked writers cannot delete.
        let result = self.repository.sql.batch(mutation_identity()?, SqlBatch { statements: vec![
            access_statement(actor),
            SqlStatement {
                sql: format!("SELECT {COLUMNS} FROM lfs_locks WHERE id = ?2 AND ({WRITE_ACCESS})"),
                parameters: vec![SqlValue::Text(actor.into()), SqlValue::Text(id.into())],
            },
            SqlStatement {
                sql: format!("DELETE FROM lfs_locks WHERE id = ?2 AND (owner = ?1 OR ?3 = 1) AND ({WRITE_ACCESS})"),
                parameters: vec![SqlValue::Text(actor.into()), SqlValue::Text(id.into()), SqlValue::Integer(i64::from(force))],
            },
        ] }).await.map_err(|error| LfsError::Cell(Box::new(error)))?;
        authorize(&result.output, TokenScope::Write)?;
        let row = result
            .output
            .get(1)
            .ok_or(LfsError::Corrupt)?
            .rows
            .first()
            .ok_or(LfsError::NotFound)?;
        if result.output.get(2).ok_or(LfsError::Corrupt)?.rows_affected != 1 {
            return Err(LfsError::Forbidden);
        }
        decode(row)
    }

    pub(crate) async fn locks(
        &self,
        actor: ReadIdentity<'_>,
        query: LockQuery,
        required: TokenScope,
    ) -> Result<LockPage, LfsError> {
        let cursor = query
            .cursor
            .as_deref()
            .filter(|s| !s.is_empty())
            .map(|value| {
                value
                    .parse::<i64>()
                    .ok()
                    .filter(|n| *n > 0 && n.to_string() == value)
                    .ok_or(LfsError::InvalidLock("invalid cursor"))
            })
            .transpose()?
            .unwrap_or(0);
        let limit = query.limit.unwrap_or(100).clamp(1, 100) as usize;
        let access = if required >= TokenScope::Write {
            WRITE_ACCESS
        } else {
            READ_ACCESS
        };
        let mut sql =
            format!("SELECT sequence, {COLUMNS} FROM lfs_locks WHERE sequence > ?2 AND ({access})");
        let mut parameters = vec![actor.parameter(), SqlValue::Integer(cursor)];
        if let Some(path) = query.path.filter(|s| !s.is_empty()) {
            validate_path(&path)?;
            parameters.push(SqlValue::Text(path));
            sql.push_str(&format!(" AND path = ?{}", parameters.len()));
        }
        if let Some(id) = query.id.filter(|s| !s.is_empty()) {
            validate_id(&id)?;
            parameters.push(SqlValue::Text(id));
            sql.push_str(&format!(" AND id = ?{}", parameters.len()));
        }
        sql.push_str(&format!(" ORDER BY sequence LIMIT {}", limit + 1));
        let result = self
            .repository
            .sql
            .query(
                None,
                SqlBatch {
                    statements: vec![access_statement(actor), SqlStatement { sql, parameters }],
                },
            )
            .await
            .map_err(|error| LfsError::Cell(Box::new(error)))?;
        authorize(&result.output, required)?;
        let rows = &result.output.get(1).ok_or(LfsError::Corrupt)?.rows;
        let mut locks = Vec::with_capacity(rows.len().min(limit));
        let mut last = None;
        for row in rows.iter().take(limit) {
            let Some((SqlValue::Integer(sequence), lock)) = row.split_first() else {
                return Err(LfsError::Corrupt);
            };
            locks.push(decode(lock)?);
            last = Some(sequence.to_string());
        }
        Ok(LockPage {
            locks,
            next_cursor: if rows.len() > limit { last } else { None },
        })
    }
}

fn authorize(sets: &[SqlResultSet], required: TokenScope) -> Result<(), LfsError> {
    let access = decode_access(sets).map_err(|error| LfsError::Cell(Box::new(error)))?;
    if !access.is_some_and(|access| access >= required) {
        return Err(LfsError::Forbidden);
    }
    Ok(())
}

fn decode(row: &[SqlValue]) -> Result<Lock, LfsError> {
    let [
        SqlValue::Text(id),
        SqlValue::Text(path),
        SqlValue::Text(locked_at),
        SqlValue::Text(owner),
    ] = row
    else {
        return Err(LfsError::Corrupt);
    };
    Ok(Lock {
        id: id.clone(),
        path: path.clone(),
        locked_at: locked_at.clone(),
        owner: Owner {
            name: owner.clone(),
        },
    })
}

fn validate_path(path: &str) -> Result<(), LfsError> {
    if path.len() > 4096
        || path.contains('\0')
        || path.split('/').any(|part| matches!(part, "" | "." | ".."))
    {
        return Err(LfsError::InvalidLock(
            "path must be relative, canonical and at most 4096 bytes",
        ));
    }
    Ok(())
}

fn validate_id(id: &str) -> Result<(), LfsError> {
    if uuid::Uuid::parse_str(id).is_ok_and(|parsed| parsed.to_string() == id) {
        return Ok(());
    }
    Err(LfsError::InvalidLock("invalid lock ID"))
}
