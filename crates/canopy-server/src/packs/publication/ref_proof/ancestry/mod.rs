//! Bounded heap, admitted disk traversal of certified commit-parent metadata.
//! The indexed queue and memoized pair results are disposable private scratch.
use super::*;
use crate::packs::{
    catalog::CatalogFiles,
    metadata::{AdmittedFile, TypedEdge, growth},
};
use rusqlite::{Connection, OptionalExtension, params};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};

const CLEAR_PAGE: &str =
    "DELETE FROM visits WHERE oid IN (SELECT oid FROM visits ORDER BY oid LIMIT ?1)";

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
    limits: MetadataLimits,
}
impl Scratch {
    fn clear_page(&mut self) -> Result<usize, RefProofError> {
        self.write(|tx| {
            tx.execute(CLEAR_PAGE, [PAGE_OBJECTS as i64])
                .map_err(MetadataError::from)
                .map_err(Into::into)
        })
    }
    fn write<T>(
        &mut self,
        mut body: impl FnMut(&rusqlite::Transaction<'_>) -> Result<T, RefProofError>,
    ) -> Result<T, RefProofError> {
        let canceled = Arc::clone(&self.canceled);
        growth::transaction(
            &mut self.connection,
            &mut self._file,
            self.limits.max_file_bytes,
            |tx| {
                if canceled.load(Ordering::Acquire) {
                    return Err(RefProofError::Canceled);
                }
                let result = body(tx)?;
                if canceled.load(Ordering::Acquire) {
                    return Err(RefProofError::Canceled);
                }
                Ok(result)
            },
        )
    }
}
pub(in crate::packs::publication) struct Walker {
    scratch: Arc<Mutex<Scratch>>,
    _cancel: Cancellation,
    catalog: Option<crate::packs::catalog::StoredCatalog>,
    failed: bool,
}
impl Walker {
    pub(in crate::packs::publication) async fn new(
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
            let reservation = growth::reserve(&budget, limits.max_file_bytes)?;
            let workspace = Arc::new(tempfile::Builder::new().prefix("canopy-ref-ancestry-").tempdir_in(root).map_err(MetadataError::from)?);
            let file = tempfile::Builder::new().prefix("walk-").tempfile_in(workspace.path()).map_err(MetadataError::from)?;
            let mut admitted = AdmittedFile::new(file, reservation);
            admitted.retain_workspace(workspace);
            let connection = Connection::open(admitted.file().path()).map_err(MetadataError::from)?;
            connection.execute_batch("PRAGMA page_size=4096; PRAGMA journal_mode=DELETE; PRAGMA synchronous=OFF; PRAGMA trusted_schema=OFF; PRAGMA mmap_size=0;").map_err(MetadataError::from)?;
            connection.pragma_update(None, "cache_size", -(limits.cache_kib as i64)).map_err(MetadataError::from)?;
            growth::configure(&connection, &mut admitted)?;
            let interrupted = Arc::clone(&canceled);
            connection.progress_handler(10_000, Some(move || interrupted.load(Ordering::Acquire)));
            let mut scratch = Scratch { connection, _file: admitted, canceled, limits };
            scratch.write(|tx| {
                tx.execute_batch("CREATE TABLE visits(oid BLOB PRIMARY KEY,expanded INTEGER NOT NULL DEFAULT 0 CHECK(expanded IN (0,1))) WITHOUT ROWID; CREATE INDEX visits_ready ON visits(expanded,oid); CREATE TABLE answers(ancestor BLOB,descendant BLOB,result INTEGER NOT NULL CHECK(result IN(0,1)),PRIMARY KEY(ancestor,descendant)) WITHOUT ROWID;").map_err(MetadataError::from)?;
                Ok(())
            })?;
            Ok::<_, RefProofError>(Arc::new(Mutex::new(scratch)))
        }).await??;
        Ok(Self {
            scratch,
            _cancel: cancel,
            catalog: None,
            failed: false,
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
    pub(in crate::packs::publication) async fn is_ancestor(
        &mut self,
        reader: &CatalogReader,
        files: &Arc<CatalogFiles>,
        old: ObjectId,
        new: ObjectId,
        base: &PreparationBaseResolver,
    ) -> Result<bool, RefProofError> {
        // Exclusive borrowing spans the entire traversal, rather than just one
        // SQL callback. Failure/cancellation cannot leave a reusable queue or
        // let a queued old write contaminate a later pair's proof.
        if self.failed || self._cancel.0.load(Ordering::Acquire) {
            return Err(RefProofError::Canceled);
        }
        self.failed = true;
        let mut guard = WalkCancellation::new(Arc::clone(&self._cancel.0));
        let catalog = reader.stored();
        if self.catalog.is_some_and(|bound| bound != catalog)
            || old.format() != catalog.format
            || new.format() != catalog.format
        {
            return Err(RefProofError::Invalid);
        }
        self.catalog = Some(catalog);
        let result = self.walk(reader, files, old, new, base).await?;
        // This also covers identical tips and memo hits after asynchronous
        // membership/SQL work; neither may return an answer from an expired
        // preparation session.
        base.live_lease()?;
        guard.complete = true;
        self.failed = false;
        Ok(result)
    }

    /// Test a bounded set against one descendant traversal. This avoids one
    /// full base-history walk per original commit when verifying a rebase.
    pub(in crate::packs::publication) async fn ancestors_within(
        &mut self,
        reader: &CatalogReader,
        files: &Arc<CatalogFiles>,
        targets: &[ObjectId],
        descendant: ObjectId,
        base: &PreparationBaseResolver,
    ) -> Result<Vec<bool>, RefProofError> {
        if self.failed || self._cancel.0.load(Ordering::Acquire) {
            return Err(RefProofError::Canceled);
        }
        self.failed = true;
        let mut guard = WalkCancellation::new(Arc::clone(&self._cancel.0));
        let catalog = reader.stored();
        if targets.is_empty()
            || targets.len() > PAGE_OBJECTS
            || self.catalog.is_some_and(|c| c != catalog)
            || descendant.format() != catalog.format
            || targets.iter().any(|o| o.format() != catalog.format)
        {
            return Err(RefProofError::Invalid);
        }
        self.catalog = Some(catalog);
        base.live_lease()?;
        for ids in [targets, std::slice::from_ref(&descendant)] {
            let headers = reader.headers(ids, &**files, &**files).await?;
            if headers.len() != ids.len()
                || headers
                    .iter()
                    .any(|h| h.is_none_or(|h| h.object.kind != ObjectKind::Commit))
            {
                return Err(RefProofError::Invalid);
            }
        }
        loop {
            base.live_lease()?;
            if self.call(Scratch::clear_page).await? == 0 {
                break;
            }
        }
        self.call(move |scratch| {
            scratch.write(|tx| {
                tx.execute("INSERT INTO visits(oid) VALUES(?1)", [descendant.as_ref()])
                    .map_err(MetadataError::from)?;
                Ok(())
            })
        })
        .await?;
        let wanted: BTreeSet<_> = targets.iter().copied().collect();
        let mut reached = BTreeSet::new();
        'walk: loop {
            base.live_lease()?;
            let page = self
                .call(|scratch| {
                    scratch
                        .connection
                        .prepare_cached(
                            "SELECT oid FROM visits WHERE expanded=0 ORDER BY oid LIMIT ?1",
                        )
                        .map_err(MetadataError::from)?
                        .query_map([PAGE_OBJECTS as i64], |r| {
                            crate::packs::metadata::oid(r.get(0)?)
                        })
                        .map_err(MetadataError::from)?
                        .collect::<rusqlite::Result<Vec<_>>>()
                        .map_err(MetadataError::from)
                        .map_err(Into::into)
                })
                .await?;
            if page.is_empty() {
                break;
            }
            for oid in page {
                base.live_lease()?;
                if wanted.contains(&oid) {
                    reached.insert(oid);
                }
                if reached.len() == wanted.len() {
                    break 'walk;
                }
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
                    let metadata = object.source.metadata.clone();
                    let edges =
                        tokio::task::spawn_blocking(move || metadata.edges_after(oid, after))
                            .await??;
                    if edges.is_empty() {
                        break;
                    }
                    after = edges.last().map(|e| e.child);
                    self.parents(edges).await?;
                }
                self.call(move |scratch| {
                    scratch.write(|tx| {
                        if tx
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
                })
                .await?;
            }
        }
        base.live_lease()?;
        guard.complete = true;
        self.failed = false;
        Ok(targets.iter().map(|o| reached.contains(o)).collect())
    }
    async fn walk(
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
        // Reuse the same queue and indexes. Clear in bounded transactions so a
        // second pair does not build one history-sized rollback journal.
        loop {
            base.live_lease()?;
            let removed = self.call(Scratch::clear_page).await?;
            if removed == 0 {
                break;
            }
        }
        self.call(move |scratch| {
            scratch.write(|tx| {
                tx.execute("INSERT INTO visits(oid) VALUES(?1)", [new.as_ref()])
                    .map_err(MetadataError::from)?;
                Ok(())
            })
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
                    scratch.write(|tx| {
                        if tx
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
                })
                .await?;
            }
        };
        base.live_lease()?;
        self.call(move |scratch| {
            scratch.write(|tx| {
                tx.execute(
                    "INSERT INTO answers VALUES(?1,?2,?3)",
                    params![old.as_ref(), new.as_ref(), result],
                )
                .map_err(MetadataError::from)?;
                Ok(())
            })
        })
        .await?;
        Ok(result)
    }
    async fn parents(&self, edges: Vec<TypedEdge>) -> Result<(), RefProofError> {
        if edges.len() > PAGE_OBJECTS {
            return Err(RefProofError::Invalid);
        }
        self.call(move |scratch| {
            scratch.write(|tx| {
                {
                    let mut insert = tx
                        .prepare_cached("INSERT OR IGNORE INTO visits(oid) VALUES(?1)")
                        .map_err(MetadataError::from)?;
                    for edge in &edges {
                        if edge.expected_kind == ObjectKind::Commit {
                            insert
                                .execute([edge.child.as_ref()])
                                .map_err(MetadataError::from)?;
                        }
                    }
                }
                Ok(())
            })
        })
        .await
    }

    #[cfg(test)]
    pub(in crate::packs::publication) fn test_blocker(
        &self,
        entered: tokio::sync::oneshot::Sender<()>,
        release: std::sync::mpsc::Receiver<()>,
    ) -> tokio::task::JoinHandle<Result<(), RefProofError>> {
        let scratch = Arc::clone(&self.scratch);
        tokio::task::spawn_blocking(move || {
            let _owned = scratch.lock().map_err(|_| RefProofError::Invalid)?;
            let _ = entered.send(());
            release.recv().map_err(|_| RefProofError::Invalid)?;
            Ok(())
        })
    }
}

struct WalkCancellation {
    canceled: Arc<AtomicBool>,
    complete: bool,
}
impl WalkCancellation {
    fn new(canceled: Arc<AtomicBool>) -> Self {
        Self {
            canceled,
            complete: false,
        }
    }
}
impl Drop for WalkCancellation {
    fn drop(&mut self) {
        if !self.complete {
            self.canceled.store(true, Ordering::Release);
        }
    }
}

#[cfg(test)]
mod tests;
