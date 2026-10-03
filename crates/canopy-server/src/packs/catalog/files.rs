//! Service-owned authenticated SQLite files. Bounds apply to cached files,
//! borrowed readers and canceled blocking jobs together, not just cache entries.
use super::*;
use crate::packs::{
    directory::{DirectoryRun, StoredRun},
    metadata::{MetadataError, MetadataLimits, MetadataSegment, ReaderAdmission, StoredSegment},
};
use cellule_ltx::DiskBudget;
use std::{
    collections::VecDeque,
    path::Path,
    sync::{
        Mutex,
        atomic::{AtomicU64, Ordering},
    },
};
use tokio::sync::{Mutex as AsyncMutex, Semaphore};

const LOAD_STRIPES: usize = 16;
pub const MAX_OPEN_CATALOG_FILES: u32 = 128;

#[derive(Clone, Copy, Debug)]
pub struct CatalogFileLimits {
    /// Includes borrowed/evicted files and outstanding canceled worker jobs.
    pub open_files: u32,
    pub cached_files: usize,
    pub metadata: MetadataLimits,
}
impl Default for CatalogFileLimits {
    fn default() -> Self {
        Self {
            open_files: 32,
            cached_files: 16,
            metadata: MetadataLimits::default(),
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CatalogFileStats {
    pub open_files: u32,
    pub cached_files: usize,
    pub cache_hits: u64,
    pub downloaded_files: u64,
}

/// Shared across catalog generations in one worker. Creating a loader does not
/// certify its catalogs, grant access, or pin remote generations against GC.
pub struct CatalogFiles {
    root: Arc<tempfile::TempDir>,
    store: Arc<ArtifactStore>,
    format: ObjectFormat,
    budget: DiskBudget,
    limits: CatalogFileLimits,
    slots: Arc<Semaphore>,
    cache: Mutex<VecDeque<Entry>>,
    // Coalesce equal misses without serializing all unrelated transfers.
    // The fixed stripe array cannot grow with repository history.
    loads: [AsyncMutex<()>; LOAD_STRIPES],
    cache_hits: AtomicU64,
    downloaded_files: AtomicU64,
}
#[derive(Clone, Copy, PartialEq, Eq)]
enum Key {
    Run(StoredRun),
    Metadata(StoredSegment),
}
impl Key {
    fn identity(self) -> (u8, [u8; 16], [u8; 32]) {
        match self {
            Self::Run(stored) => (1, stored.run.operation, stored.artifact.digest),
            Self::Metadata(stored) => {
                (2, stored.segment.identity.operation, stored.artifact.digest)
            }
        }
    }
    fn stripe(self) -> usize {
        let (kind, operation, digest) = self.identity();
        let mut hash = blake3::Hasher::new();
        hash.update(&[kind]);
        hash.update(&operation);
        hash.update(&digest);
        usize::from(hash.finalize().as_bytes()[0]) % LOAD_STRIPES
    }
}
#[derive(Clone)]
enum Value {
    Run(Arc<DirectoryRun>),
    Metadata(Arc<MetadataSegment>),
}
impl Value {
    fn unborrowed(&self) -> bool {
        match self {
            Self::Run(run) => Arc::strong_count(run) == 1,
            Self::Metadata(segment) => Arc::strong_count(segment) == 1,
        }
    }
}
struct Entry {
    key: Key,
    value: Value,
}
impl CatalogFiles {
    pub fn new(
        workspace: &Path,
        budget: DiskBudget,
        store: Arc<ArtifactStore>,
        format: ObjectFormat,
        limits: CatalogFileLimits,
    ) -> Result<Self, MetadataError> {
        if limits.open_files == 0
            || limits.open_files > MAX_OPEN_CATALOG_FILES
            || limits.cached_files > limits.open_files as usize
            || limits.metadata.cache_kib == 0
            || limits.metadata.cache_kib > i32::MAX as u32
            || limits.metadata.max_file_bytes == 0
            || limits.metadata.max_file_bytes > canopy_object_storage::external::MAX_ARTIFACT_BYTES
        {
            return Err(MetadataError::Limit);
        }
        let root = tempfile::Builder::new()
            .prefix("canopy-catalog-files-")
            .tempdir_in(workspace)?;
        Ok(Self {
            root: Arc::new(root),
            store,
            format,
            budget,
            limits,
            slots: Arc::new(Semaphore::new(limits.open_files as usize)),
            cache: Mutex::new(VecDeque::new()),
            loads: std::array::from_fn(|_| AsyncMutex::new(())),
            cache_hits: AtomicU64::new(0),
            downloaded_files: AtomicU64::new(0),
        })
    }
    pub fn stats(&self) -> Result<CatalogFileStats, MetadataError> {
        Ok(CatalogFileStats {
            open_files: self.limits.open_files - self.slots.available_permits() as u32,
            cached_files: self
                .cache
                .lock()
                .map_err(|_| MetadataError::Integrity)?
                .len(),
            cache_hits: self.cache_hits.load(Ordering::Relaxed),
            downloaded_files: self.downloaded_files.load(Ordering::Relaxed),
        })
    }
    fn cached(&self, key: Key) -> Result<Option<Value>, MetadataError> {
        let mut cache = self.cache.lock().map_err(|_| MetadataError::Integrity)?;
        let Some(at) = cache
            .iter()
            .position(|entry| entry.key.identity() == key.identity())
        else {
            return Ok(None);
        };
        // A digest cache key must never hide a different descriptor/context.
        if cache[at].key != key {
            return Err(MetadataError::Integrity);
        }
        let entry = cache.remove(at).ok_or(MetadataError::Integrity)?;
        let value = entry.value.clone();
        cache.push_back(entry);
        self.cache_hits.fetch_add(1, Ordering::Relaxed);
        Ok(Some(value))
    }
    async fn admission(&self) -> Result<ReaderAdmission, MetadataError> {
        loop {
            if let Ok(slot) = Arc::clone(&self.slots).try_acquire_owned() {
                return Ok(ReaderAdmission {
                    _slot: slot,
                    _root: Arc::clone(&self.root),
                });
            }
            // Evict only an unborrowed file to make room. If all live slots
            // belong to callers/jobs, fail admission instead of deadlocking.
            let evicted = {
                let mut cache = self.cache.lock().map_err(|_| MetadataError::Integrity)?;
                cache
                    .iter()
                    .position(|entry| entry.value.unborrowed())
                    .and_then(|at| cache.remove(at))
            };
            let Some(evicted) = evicted else {
                return Err(MetadataError::Limit);
            };
            tokio::task::spawn_blocking(move || drop(evicted)).await?;
        }
    }
    async fn remember(&self, key: Key, value: Value) -> Result<(), MetadataError> {
        let evicted = {
            let mut cache = self.cache.lock().map_err(|_| MetadataError::Integrity)?;
            if self.limits.cached_files == 0 {
                return Ok(());
            }
            cache.push_back(Entry { key, value });
            if cache.len() > self.limits.cached_files {
                cache.pop_front()
            } else {
                None
            }
        };
        if let Some(evicted) = evicted {
            // Queued cleanup keeps the file's slot and disk charge until its
            // connection closes and private file is removed.
            tokio::task::spawn_blocking(move || drop(evicted)).await?;
        }
        Ok(())
    }
    async fn load_value(&self, key: Key) -> Result<Value, MetadataError> {
        match key {
            Key::Run(stored) => {
                stored.validate()?;
                if stored.run.repository != self.store.repository()
                    || stored.run.format != self.format
                {
                    return Err(MetadataError::Integrity);
                }
            }
            Key::Metadata(stored) => {
                if stored.segment.identity.repository != self.store.repository()
                    || stored.segment.identity.format != self.format
                    || stored.segment.size != stored.artifact.size
                    || stored.segment.digest != stored.artifact.digest
                {
                    return Err(MetadataError::Integrity);
                }
            }
        }
        if let Some(value) = self.cached(key)? {
            return Ok(value);
        }
        let _loading = self.loads[key.stripe()].lock().await;
        if let Some(value) = self.cached(key)? {
            return Ok(value);
        }
        let reader = self.admission().await?;
        let value = match key {
            Key::Run(stored) => Value::Run(
                DirectoryRun::download_for_reader(
                    self.root.path(),
                    self.budget.clone(),
                    &self.store,
                    stored,
                    self.limits.metadata,
                    Some(reader),
                )
                .await?,
            ),
            Key::Metadata(stored) => Value::Metadata(
                MetadataSegment::download_for_reader(
                    self.root.path(),
                    self.budget.clone(),
                    &self.store,
                    stored,
                    self.limits.metadata,
                    Some(reader),
                )
                .await?,
            ),
        };
        self.downloaded_files.fetch_add(1, Ordering::Relaxed);
        self.remember(key, value.clone()).await?;
        Ok(value)
    }
}
impl RunLoader for CatalogFiles {
    async fn load(&self, run: StoredRun) -> Result<Arc<DirectoryRun>, MetadataError> {
        run.validate()?;
        // Cache the complete authenticated file once across disjoint projections.
        // Coverage is certified by the catalog and explicitly folded by writers;
        // a different physical descriptor/manifest still rejects on a cache hit.
        let physical = StoredRun {
            coverage: run.run.coverage(),
            ..run
        };
        match self.load_value(Key::Run(physical)).await? {
            Value::Run(run) => Ok(run),
            Value::Metadata(_) => Err(MetadataError::Integrity),
        }
    }
}
impl SourceLoader for CatalogFiles {
    async fn load(&self, segment: StoredSegment) -> Result<Arc<MetadataSegment>, MetadataError> {
        match self.load_value(Key::Metadata(segment)).await? {
            Value::Metadata(segment) => Ok(segment),
            Value::Run(_) => Err(MetadataError::Integrity),
        }
    }
}
