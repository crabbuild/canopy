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
    Enter(Edge),
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
        let started = std::time::Instant::now();
        // One preparer per process bounds aggregate memory while retaining no
        // arbitrary repository/object-count ceiling. Durable certificates still
        // recheck typed dependencies inside each bounded Cell transaction.
        static PREPARERS: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(1);
        let _preparer = PREPARERS
            .acquire()
            .await
            .map_err(|_| Error::Command("graph admission closed"))?;
        let mut nodes = HashMap::new();
        let mut ready = HashMap::new();
        let mut discovered = HashSet::new();
        let mut frontier: BTreeSet<_> = plan
            .updates
            .iter()
            .filter_map(|update| update.new_oid)
            .collect();
        while !frontier.is_empty() {
            let page: Vec<_> = frontier.iter().take(MAX_CERTIFICATES).copied().collect();
            for oid in &page {
                frontier.remove(oid);
                discovered.insert(*oid);
            }
            let result = self.sql.query(None, status_query(&page)).await?;
            let states = statuses(&result.output)?;
            let mut structure = BTreeSet::new();
            for oid in &page {
                let Some(state) = states.get(oid) else {
                    return Ok(());
                };
                if state.certified {
                    ready.insert(*oid, state.kind);
                    continue;
                }
                nodes.insert(*oid, (state.kind, state.bytes, Some(Vec::new())));
                if state.kind != ObjectKind::Blob {
                    structure.insert(*oid);
                }
            }
            while !structure.is_empty() {
                let ids: Vec<_> = structure
                    .iter()
                    .take(crate::object_batch::MAX_OBJECTS)
                    .copied()
                    .collect();
                let records = self.selected_objects(&ids).await?;
                if records.is_empty() {
                    return Err(Error::Command("empty graph body page").into());
                }
                for record in records {
                    structure.remove(&record.oid);
                    let oid = record.oid;
                    let kind = record.kind;
                    let body = match record.storage {
                        crate::ObjectStorage::Inline(body) => body,
                        crate::ObjectStorage::Chunked { .. } => {
                            self.object(oid, None)
                                .await?
                                .output
                                .ok_or(Error::Command("missing graph body"))?
                                .1
                        }
                        _ => return Err(Error::Command("invalid structural storage").into()),
                    };
                    let parsed =
                        tokio::task::spawn_blocking(move || edges(oid.format(), kind, &body))
                            .await?;
                    if let Some(edges) = &parsed {
                        frontier.extend(
                            edges
                                .iter()
                                .map(|(oid, _)| *oid)
                                .filter(|oid| !discovered.contains(oid)),
                        );
                    }
                    nodes
                        .get_mut(&oid)
                        .ok_or(Error::Command("missing graph node"))?
                        .2 = parsed;
                }
            }
        }
        drop(discovered);
        let loaded = nodes.len();
        let mut pending: Vec<_> = plan
            .updates
            .iter()
            .filter_map(|update| {
                update.new_oid.map(|oid| {
                    Visit::Enter((
                        oid,
                        update
                            .name
                            .starts_with("refs/heads/")
                            .then_some(ObjectKind::Commit),
                    ))
                })
            })
            .collect();
        let mut visiting = HashSet::new();
        let mut batch = CertificateBatch::default();
        let mut bytes = 0;
        while let Some(visit) = pending.pop() {
            match visit {
                Visit::Enter((oid, expected)) => {
                    if let Some(kind) = ready.get(&oid) {
                        if expected.is_some_and(|expected| expected != *kind) {
                            return Ok(());
                        }
                        continue;
                    }
                    // Dependencies outside the captured uncertified inventory
                    // must already be certified. The command checks this, and
                    // final publication always checks the roots again.
                    let Some((kind, weight, children)) = nodes.remove(&oid) else {
                        if visiting.contains(&oid) {
                            return Ok(());
                        }
                        continue;
                    };
                    if expected.is_some_and(|expected| expected != kind) || !visiting.insert(oid) {
                        return Ok(());
                    }
                    let Some(children) = children else {
                        return Ok(());
                    };
                    pending.push(Visit::Leave(oid, kind, weight));
                    for edge in children.into_iter().rev() {
                        pending.push(Visit::Enter(edge));
                    }
                }
                Visit::Leave(oid, kind, weight) => {
                    if !batch.0.is_empty() && weight > VERIFY_BATCH_BYTES.saturating_sub(bytes) {
                        self.certify_batch(std::mem::take(&mut batch)).await?;
                        bytes = 0;
                    }
                    batch.0.push(oid);
                    bytes += weight;
                    ready.insert(oid, kind);
                    visiting.remove(&oid);
                    if batch.0.len() == MAX_CERTIFICATES || bytes >= VERIFY_BATCH_BYTES {
                        self.certify_batch(std::mem::take(&mut batch)).await?;
                        bytes = 0;
                    }
                }
            }
        }
        if !batch.0.is_empty() {
            self.certify_batch(batch).await?;
        }
        tracing::info!(
            objects = loaded,
            elapsed_seconds = started.elapsed().as_secs_f64(),
            "prepared Git graph certificates"
        );
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
