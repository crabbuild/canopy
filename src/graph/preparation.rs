use super::*;
use crate::{
    RepositoryCell,
    directory::{TokenScope, validate_component},
};
use cellule_runtime::{InvocationError, MutationIdentity, identity::RequestId};
use std::{
    collections::{BTreeSet, HashMap, HashSet},
    time::{SystemTime, UNIX_EPOCH},
};

#[derive(Debug, thiserror::Error)]
enum PrepareError {
    #[error("graph query failed")]
    Query(#[from] InvocationError<Vec<SqlResultSet>>),
    #[error("graph certificate publication failed")]
    Certificate(#[from] InvocationError<bool>),
    #[error("graph data is invalid")]
    Invalid(#[from] Error),
    #[error("graph worker failed")]
    Worker(#[from] tokio::task::JoinError),
    #[error("graph clock failed")]
    Clock(#[from] std::time::SystemTimeError),
    #[error("graph clock is outside the supported range")]
    ClockRange(#[from] std::num::TryFromIntError),
}

enum Visit {
    Enter(Edge, Option<Status>),
    Edges(std::vec::IntoIter<Edge>),
    Leave(Oid, ObjectKind, u64),
}

impl RepositoryCell {
    pub(crate) async fn prepare_graph(&self, plan: &PushPlan) -> Result<(), InvocationError<bool>> {
        self.prepare_graph_inner(plan).await.map_err(|error| {
            InvocationError::NotStarted(Error::Facility {
                name: "Git graph preparation",
                source: Box::new(error),
            })
        })
    }

    async fn prepare_graph_inner(&self, plan: &PushPlan) -> Result<(), PrepareError> {
        // The authoritative ACL/version checks remain in finalization. Avoid doing
        // graph work for invalid or unauthorized plans before that transaction.
        if plan.updates.is_empty()
            || plan.updates.len() > crate::refs::MAX_UPDATES
            || validate_component(&plan.actor).is_err()
        {
            return Ok(());
        }
        if !self
            .access_level(&plan.actor, None)
            .await?
            .output
            .is_some_and(|level| level >= TokenScope::Write)
        {
            return Ok(());
        }
        let mut pending: Vec<_> = plan
            .updates
            .iter()
            .filter_map(|update| {
                update.new_oid.map(|oid| {
                    (
                        oid,
                        update
                            .name
                            .starts_with("refs/heads/")
                            .then_some(ObjectKind::Commit),
                    )
                })
            })
            .collect::<BTreeSet<_>>()
            .into_iter()
            .map(|edge| Visit::Enter(edge, None))
            .collect();
        let mut visiting = HashSet::new();
        let mut ready = HashMap::new();
        let mut batch = CertificateBatch::default();
        let mut bytes = 0;
        let mut indexes_warm = false;
        while let Some(visit) = pending.pop() {
            let (oid, expected, observed) = match visit {
                Visit::Enter((oid, expected), observed) => (oid, expected, observed),
                Visit::Edges(mut edges) => {
                    let page: Vec<_> = edges.by_ref().take(MAX_CERTIFICATES).collect();
                    if page.is_empty() {
                        continue;
                    }
                    let oids: Vec<_> = page.iter().map(|(oid, _)| *oid).collect();
                    let result = self.sql.query(None, status_query(&oids)).await?;
                    let states = statuses(&result.output)?;
                    pending.push(Visit::Edges(edges));
                    for (oid, expected) in page.into_iter().rev() {
                        let Some(state) = states.get(&oid) else {
                            return Ok(());
                        };
                        if expected.is_some_and(|expected| expected != state.kind) {
                            return Ok(());
                        }
                        if !state.certified {
                            pending.push(Visit::Enter((oid, expected), Some(state.clone())));
                        }
                    }
                    continue;
                }
                Visit::Leave(oid, kind, weight) => {
                    if !batch.0.is_empty() && weight > VERIFY_BATCH_BYTES.saturating_sub(bytes) {
                        self.certify_batch(std::mem::take(&mut batch)).await?;
                        bytes = 0;
                        ready.clear();
                    }
                    batch.0.push(oid);
                    bytes += weight;
                    ready.insert(oid, kind);
                    visiting.remove(&oid);
                    if batch.0.len() == MAX_CERTIFICATES || bytes >= VERIFY_BATCH_BYTES {
                        self.certify_batch(std::mem::take(&mut batch)).await?;
                        bytes = 0;
                        ready.clear();
                    }
                    continue;
                }
            };
            if let Some(kind) = ready.get(&oid) {
                if expected.is_some_and(|expected| expected != *kind) {
                    return Ok(());
                }
                continue;
            }
            let state = if let Some(state) = observed {
                state
            } else {
                let result = self.sql.query(None, status_query(&[oid])).await?;
                let Some(state) = statuses(&result.output)?.remove(&oid) else {
                    return Ok(());
                };
                state
            };
            if expected.is_some_and(|expected| expected != state.kind) {
                return Ok(());
            }
            if state.certified {
                continue;
            }
            if !visiting.insert(oid) {
                return Ok(());
            }
            if !indexes_warm {
                self.warm_graph_indexes().await?;
                indexes_warm = true;
            }
            let edges = if state.kind == ObjectKind::Blob {
                Vec::new()
            } else {
                let Some(edges) = self.dependencies(oid).await? else {
                    return Ok(());
                };
                edges
            };
            pending.push(Visit::Leave(oid, state.kind, state.bytes));
            pending.push(Visit::Edges(edges.into_iter()));
        }
        if !batch.0.is_empty() {
            self.certify_batch(batch).await?;
        }
        Ok(())
    }

    // Certification inserts into both reachability indexes. After recovery,
    // their old pages may still be remote. Pull them through bounded read-only
    // queries before the write transaction; this preserves large publication
    // batches without consuming its five-second SQL execution budget on I/O.
    async fn warm_graph_indexes(&self) -> Result<(), PrepareError> {
        for reverse in [false, true] {
            let mut cursor = (Vec::new(), Vec::new());
            let mut observed = None;
            loop {
                let result = self
                    .sql
                    .query(observed, index_page(reverse, &cursor))
                    .await?;
                let rows = &result
                    .output
                    .first()
                    .ok_or(Error::Command("missing graph index page"))?
                    .rows;
                if rows.is_empty() {
                    break;
                }
                for row in rows {
                    let [SqlValue::Blob(first), SqlValue::Blob(second)] = row.as_slice() else {
                        return Err(Error::Command("invalid graph index page").into());
                    };
                    if (first, second) <= (&cursor.0, &cursor.1) {
                        return Err(Error::Command("invalid graph index order").into());
                    }
                    cursor = (first.clone(), second.clone());
                }
                observed = Some(result.receipt);
            }
        }
        Ok(())
    }

    async fn certify_batch(&self, batch: CertificateBatch) -> Result<(), PrepareError> {
        let now = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis())?;
        let identity = MutationIdentity {
            request_id: RequestId::from_bytes(uuid::Uuid::new_v4().into_bytes()),
            issued_at_ms: now,
            expires_at_ms: now
                .checked_add(60_000)
                .ok_or(Error::Command("graph clock overflow"))?,
        };
        self.application
            .command::<CertifyObjects>(&self.target, identity, batch)
            .await?;
        Ok(())
    }
}

fn index_page(reverse: bool, cursor: &(Vec<u8>, Vec<u8>)) -> SqlBatch {
    let sql = if reverse {
        "SELECT child, parent FROM object_edges INDEXED BY object_edges_by_child WHERE (child, parent) > (?1, ?2) ORDER BY child, parent LIMIT 512"
    } else {
        "SELECT parent, child FROM object_edges WHERE (parent, child) > (?1, ?2) ORDER BY parent, child LIMIT 512"
    };
    SqlBatch {
        statements: vec![SqlStatement {
            sql: sql.into(),
            parameters: vec![
                SqlValue::Blob(cursor.0.clone()),
                SqlValue::Blob(cursor.1.clone()),
            ],
        }],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cellule_ltx::rusqlite::{Connection, StatementStatus, params};

    #[test]
    fn index_warming_seeks_every_pair_across_duplicate_cursor_keys()
    -> Result<(), Box<dyn std::error::Error>> {
        let db = Connection::open_in_memory()?;
        db.execute_batch("CREATE TABLE object_edges (parent BLOB, child BLOB, PRIMARY KEY(parent, child)) WITHOUT ROWID; CREATE INDEX object_edges_by_child ON object_edges(child, parent)")?;
        let mut expected = Vec::new();
        for n in 0_u32..2_049 {
            let parent = (n / 7).to_be_bytes().to_vec();
            let child = (n % 7).to_be_bytes().to_vec();
            db.execute(
                "INSERT INTO object_edges VALUES (?1, ?2)",
                params![&parent, &child],
            )?;
            expected.push((parent, child));
        }
        for reverse in [false, true] {
            let mut cursor = (Vec::new(), Vec::new());
            let mut seen = Vec::new();
            loop {
                let query = index_page(reverse, &cursor);
                let mut statement = db.prepare(&query.statements[0].sql)?;
                let rows: Vec<(Vec<u8>, Vec<u8>)> = statement
                    .query_map(params![&cursor.0, &cursor.1], |row| {
                        Ok((row.get(0)?, row.get(1)?))
                    })?
                    .collect::<Result<_, _>>()?;
                assert!(rows.len() <= 512);
                assert_eq!(statement.get_status(StatementStatus::FullscanStep), 0);
                if rows.is_empty() {
                    break;
                }
                assert!(rows[0] > cursor);
                cursor = rows.last().unwrap().clone();
                seen.extend(rows);
            }
            let mut ordered = expected.clone();
            if reverse {
                for pair in &mut ordered {
                    std::mem::swap(&mut pair.0, &mut pair.1);
                }
            }
            ordered.sort();
            assert_eq!(seen, ordered);
        }
        Ok(())
    }
}
