//! Bounded heap, admitted disk traversal of certified commit-parent metadata.
//! The indexed queue and memoized pair results are disposable private scratch.
use super::*;
use crate::packs::{
    catalog::CatalogFiles,
    metadata::{AdmittedFile, TypedEdge},
};
use rusqlite::{Connection, OptionalExtension, params};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};

struct Cancellation(Arc<AtomicBool>);
impl Drop for Cancellation {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}

struct Scratch {
    connection: Connection,
    _file: AdmittedFile,
    canceled: Arc<AtomicBool>,
}
pub(super) struct Walker {
    scratch: Arc<Mutex<Scratch>>,
    _cancel: Cancellation,
}
impl Walker {
    pub(super) async fn new(
        root: &Path,
        budget: DiskBudget,
        limits: MetadataLimits,
    ) -> Result<Self, RefProofError> {
        let root = root.to_owned();
        let canceled = Arc::new(AtomicBool::new(false));
        let cancel = Cancellation(Arc::clone(&canceled));
        let scratch = tokio::task::spawn_blocking(move || {
            if canceled.load(Ordering::Acquire) { return Err(RefProofError::Canceled); }
            if limits.cache_kib == 0 || limits.cache_kib > i32::MAX as u32 || limits.max_file_bytes < 16 << 10 || limits.max_file_bytes > canopy_object_storage::external::MAX_ARTIFACT_BYTES || !limits.max_file_bytes.is_multiple_of(4096) { return Err(MetadataError::Limit.into()); }
            let reservation = budget.try_reserve(limits.max_file_bytes.checked_mul(3).ok_or(MetadataError::Limit)?).map_err(MetadataError::from)?;
            let workspace = Arc::new(tempfile::Builder::new().prefix("canopy-ref-ancestry-").tempdir_in(root).map_err(MetadataError::from)?);
            let file = tempfile::Builder::new().prefix("walk-").tempfile_in(workspace.path()).map_err(MetadataError::from)?;
            let mut admitted = AdmittedFile::new(file, reservation);
            admitted.retain_workspace(workspace);
            let connection = Connection::open(admitted.file().path()).map_err(MetadataError::from)?;
            connection.execute_batch("PRAGMA page_size=4096; PRAGMA journal_mode=DELETE; PRAGMA synchronous=OFF; PRAGMA trusted_schema=OFF; PRAGMA mmap_size=0;").map_err(MetadataError::from)?;
            connection.pragma_update(None, "cache_size", -(limits.cache_kib as i64)).map_err(MetadataError::from)?;
            connection.pragma_update(None, "max_page_count", limits.max_file_bytes / 4096).map_err(MetadataError::from)?;
            let interrupted = Arc::clone(&canceled);
            connection.progress_handler(10_000, Some(move || interrupted.load(Ordering::Acquire)));
            connection.execute_batch("CREATE TABLE visits(oid BLOB PRIMARY KEY,expanded INTEGER NOT NULL DEFAULT 0 CHECK(expanded IN (0,1))) WITHOUT ROWID; CREATE INDEX visits_ready ON visits(expanded,oid); CREATE TABLE answers(ancestor BLOB,descendant BLOB,result INTEGER NOT NULL CHECK(result IN(0,1)),PRIMARY KEY(ancestor,descendant)) WITHOUT ROWID;").map_err(MetadataError::from)?;
            Ok::<_, RefProofError>(Arc::new(Mutex::new(Scratch { connection, _file: admitted, canceled })))
        }).await??;
        Ok(Self {
            scratch,
            _cancel: cancel,
        })
    }
    async fn call<T: Send + 'static>(
        &self,
        f: impl FnOnce(&mut Scratch) -> Result<T, RefProofError> + Send + 'static,
    ) -> Result<T, RefProofError> {
        let scratch = Arc::clone(&self.scratch);
        tokio::task::spawn_blocking(move || {
            let mut scratch = scratch.lock().map_err(|_| RefProofError::Invalid)?;
            if scratch.canceled.load(Ordering::Acquire) {
                return Err(RefProofError::Canceled);
            }
            f(&mut scratch)
        })
        .await?
    }
    pub(super) async fn is_ancestor(
        &self,
        reader: &CatalogReader,
        files: &Arc<CatalogFiles>,
        old: ObjectId,
        new: ObjectId,
        base: &PreparationBaseResolver,
    ) -> Result<bool, RefProofError> {
        base.live_lease()?;
        let endpoints = reader.headers(&[old, new], &**files, &**files).await?;
        if endpoints.len() != 2
            || endpoints
                .iter()
                .any(|header| header.is_none_or(|h| h.object.kind != ObjectKind::Commit))
        {
            return Err(RefProofError::Invalid);
        }
        if old == new {
            return Ok(true);
        }
        if let Some(answer) = self
            .call(move |scratch| {
                scratch
                    .connection
                    .query_row(
                        "SELECT result FROM answers WHERE ancestor=?1 AND descendant=?2",
                        params![old.as_ref(), new.as_ref()],
                        |row| row.get::<_, bool>(0),
                    )
                    .optional()
                    .map_err(MetadataError::from)
                    .map_err(Into::into)
            })
            .await?
        {
            return Ok(answer);
        }
        self.call(move |scratch| {
            let tx = scratch
                .connection
                .transaction()
                .map_err(MetadataError::from)?;
            tx.execute("DELETE FROM visits", [])
                .map_err(MetadataError::from)?;
            tx.execute("INSERT INTO visits(oid) VALUES(?1)", [new.as_ref()])
                .map_err(MetadataError::from)?;
            tx.commit().map_err(MetadataError::from)?;
            Ok(())
        })
        .await?;
        let result = 'walk: loop {
            base.live_lease()?;
            let page = self
                .call(|scratch| {
                    scratch
                        .connection
                        .prepare_cached(
                            "SELECT oid FROM visits WHERE expanded=0 ORDER BY oid LIMIT ?1",
                        )
                        .map_err(MetadataError::from)?
                        .query_map([PAGE_OBJECTS as i64], |row| {
                            crate::packs::metadata::oid(row.get(0)?)
                        })
                        .map_err(MetadataError::from)?
                        .collect::<rusqlite::Result<Vec<_>>>()
                        .map_err(MetadataError::from)
                        .map_err(Into::into)
                })
                .await?;
            if page.is_empty() {
                break false;
            }
            for oid in page {
                base.live_lease()?;
                let object = reader
                    .lookup(oid, &**files, &**files)
                    .await?
                    .ok_or(RefProofError::Invalid)?;
                if object.entry.header.object.kind != ObjectKind::Commit {
                    return Err(RefProofError::Invalid);
                }
                let mut after = None;
                loop {
                    base.live_lease()?;
                    let metadata = Arc::clone(&object.source.metadata);
                    let edges =
                        tokio::task::spawn_blocking(move || metadata.edges_after(oid, after))
                            .await??;
                    if edges.is_empty() {
                        break;
                    }
                    after = edges.last().map(|edge| edge.child);
                    if edges
                        .iter()
                        .any(|edge| edge.expected_kind == ObjectKind::Commit && edge.child == old)
                    {
                        break 'walk true;
                    }
                    self.parents(edges).await?;
                }
                self.call(move |scratch| {
                    if scratch
                        .connection
                        .execute(
                            "UPDATE visits SET expanded=1 WHERE oid=?1 AND expanded=0",
                            [oid.as_ref()],
                        )
                        .map_err(MetadataError::from)?
                        != 1
                    {
                        return Err(RefProofError::Invalid);
                    }
                    Ok(())
                })
                .await?;
            }
        };
        base.live_lease()?;
        self.call(move |scratch| {
            scratch
                .connection
                .execute(
                    "INSERT INTO answers VALUES(?1,?2,?3)",
                    params![old.as_ref(), new.as_ref(), result],
                )
                .map_err(MetadataError::from)?;
            Ok(())
        })
        .await?;
        Ok(result)
    }
    async fn parents(&self, edges: Vec<TypedEdge>) -> Result<(), RefProofError> {
        if edges.len() > PAGE_OBJECTS {
            return Err(RefProofError::Invalid);
        }
        self.call(move |scratch| {
            let tx = scratch
                .connection
                .transaction()
                .map_err(MetadataError::from)?;
            {
                let mut insert = tx
                    .prepare_cached("INSERT OR IGNORE INTO visits(oid) VALUES(?1)")
                    .map_err(MetadataError::from)?;
                for edge in edges {
                    if edge.expected_kind == ObjectKind::Commit {
                        insert
                            .execute([edge.child.as_ref()])
                            .map_err(MetadataError::from)?;
                    }
                }
            }
            tx.commit().map_err(MetadataError::from)?;
            Ok(())
        })
        .await
    }
}

#[cfg(test)]
mod tests;
