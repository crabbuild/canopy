//! Bounded shared immutable pack copies. Callers supply certified selection and
//! a physical generation guard; these files do not themselves grant authority.
use super::*;
use crate::{
    git_cache::{CacheError, CacheOwnership, GitCache},
    git_objects::{GitObjects, ObjectReadError, ReadOwner},
    native_resources::NativeScope,
    packs::{metadata::MetadataError, sources::NativePackDescriptor},
};
use cellule_ltx::DiskBudget;
use std::{collections::VecDeque, sync::Mutex};
use tokio::sync::{Mutex as AsyncMutex, OwnedSemaphorePermit, Semaphore};

const OPEN_PACKS: usize = 8;
const CACHED_PACKS: usize = 4;
const LOAD_STRIPES: usize = 16;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NativeFileStats {
    pub open_files: usize,
    pub cached_files: usize,
    pub cache_hits: u64,
    pub downloaded_files: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum NativeReadError {
    #[error("native read service is unavailable")]
    Unavailable,
    #[error("native read file admission exhausted")]
    Capacity,
    #[error("native cache creation failed")]
    Cache(#[from] CacheError),
    #[error("native pack transfer failed")]
    Metadata(#[from] MetadataError),
    #[error("native pack binding failed")]
    Binding(#[from] IndexError),
    #[error("native object verification failed")]
    Object(#[from] ObjectReadError),
    #[error("native read job failed")]
    Task(#[from] tokio::task::JoinError),
}

pub(super) struct NativeFiles {
    root: Arc<tempfile::TempDir>,
    budget: DiskBudget,
    store: Arc<ArtifactStore>,
    format: ObjectFormat,
    native: NativeScope,
    slots: Arc<Semaphore>,
    cache: Mutex<VecDeque<(NativePackDescriptor, Arc<PackFile>)>>,
    loads: [AsyncMutex<()>; LOAD_STRIPES],
    hits: std::sync::atomic::AtomicU64,
    downloads: std::sync::atomic::AtomicU64,
}
struct FileAdmission {
    _root: Arc<tempfile::TempDir>,
    _slot: OwnedSemaphorePermit,
}
struct PackFile {
    cache: Arc<GitCache>,
    _admission: Arc<FileAdmission>,
}
impl NativeFiles {
    pub(super) fn new(
        root: Arc<tempfile::TempDir>,
        budget: DiskBudget,
        store: Arc<ArtifactStore>,
        format: ObjectFormat,
        native: NativeScope,
    ) -> Self {
        Self {
            root,
            budget,
            store,
            format,
            native,
            slots: Arc::new(Semaphore::new(OPEN_PACKS)),
            cache: Mutex::new(VecDeque::new()),
            loads: std::array::from_fn(|_| AsyncMutex::new(())),
            hits: std::sync::atomic::AtomicU64::new(0),
            downloads: std::sync::atomic::AtomicU64::new(0),
        }
    }
    pub(super) fn stats(&self) -> Result<NativeFileStats, NativeReadError> {
        Ok(NativeFileStats {
            open_files: OPEN_PACKS - self.slots.available_permits(),
            cached_files: self
                .cache
                .lock()
                .map_err(|_| NativeReadError::Capacity)?
                .len(),
            cache_hits: self.hits.load(std::sync::atomic::Ordering::Relaxed),
            downloaded_files: self.downloads.load(std::sync::atomic::Ordering::Relaxed),
        })
    }
    fn cached(
        &self,
        descriptor: NativePackDescriptor,
    ) -> Result<Option<Arc<PackFile>>, NativeReadError> {
        let mut cache = self.cache.lock().map_err(|_| NativeReadError::Capacity)?;
        let Some(at) = cache.iter().position(|(key, _)| *key == descriptor) else {
            return Ok(None);
        };
        let entry = cache.remove(at).ok_or(NativeReadError::Capacity)?;
        let file = Arc::clone(&entry.1);
        cache.push_back(entry);
        self.hits.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(Some(file))
    }
    async fn admit(
        &self,
        owner: ReadOwner,
        bytes: u64,
    ) -> Result<Arc<FileAdmission>, NativeReadError> {
        loop {
            if self.budget.available() >= bytes
                && let Ok(slot) = Arc::clone(&self.slots).try_acquire_owned()
            {
                return Ok(Arc::new(FileAdmission {
                    _root: Arc::clone(&self.root),
                    _slot: slot,
                }));
            }
            let evicted = {
                let mut cache = self.cache.lock().map_err(|_| NativeReadError::Capacity)?;
                cache
                    .iter()
                    .position(|(_, file)| Arc::strong_count(file) == 1)
                    .and_then(|at| cache.remove(at))
            };
            let Some(evicted) = evicted else {
                return Err(NativeReadError::Capacity);
            };
            let owner = Arc::clone(&owner);
            tokio::task::spawn_blocking(move || {
                let _owner = owner;
                drop(evicted);
            })
            .await?;
        }
    }
    async fn load(
        &self,
        descriptor: NativePackDescriptor,
        owner: ReadOwner,
    ) -> Result<Arc<PackFile>, NativeReadError> {
        descriptor.validate(self.store.repository(), self.format)?;
        if let Some(file) = self.cached(descriptor)? {
            return Ok(file);
        }
        let stripe = usize::from(descriptor.pack.digest[0]) % LOAD_STRIPES;
        let _loading = self.loads[stripe].lock().await;
        if let Some(file) = self.cached(descriptor)? {
            return Ok(file);
        }
        let bytes = descriptor
            .pack
            .size
            .checked_add(descriptor.index.size)
            .and_then(|size| size.checked_add(4096))
            .ok_or(NativeReadError::Capacity)?;
        if bytes > self.budget.capacity() {
            return Err(NativeReadError::Capacity);
        }
        let admission = self.admit(Arc::clone(&owner), bytes).await?;
        let lifetime: ReadOwner = Arc::new((Arc::clone(&owner), Arc::clone(&admission)));
        let cache = GitCache::create_owned(
            self.root.path().to_owned(),
            self.budget.clone(),
            "refs/heads/main",
            self.format,
            None,
            self.native.clone(),
            CacheOwnership {
                work: Arc::clone(&lifetime),
                cleanup: Some(admission.clone()),
            },
        )
        .await?;
        cache
            .download_native_owned(&self.store, descriptor, Arc::clone(&lifetime))
            .await?;
        let file = Arc::new(PackFile {
            cache,
            _admission: admission,
        });
        let verify = Arc::clone(&file);
        let claim = self
            .native
            .try_admit(crate::native_resources::NativeWork::Read)
            .map_err(ObjectReadError::from)?;
        tokio::task::spawn_blocking(move || {
            let (_owner, _claim) = (lifetime, claim);
            let pack = verify.cache.git_dir().join(format!(
                "objects/pack/pack-{}.pack",
                hex::encode(descriptor.git_checksum)
            ));
            descriptor.verify_files(&pack, &pack.with_extension("idx"))?;
            Ok::<_, IndexError>(())
        })
        .await??;
        self.downloads
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let evicted = {
            let mut cache = self.cache.lock().map_err(|_| NativeReadError::Capacity)?;
            cache.push_back((descriptor, Arc::clone(&file)));
            if cache.len() > CACHED_PACKS {
                cache.pop_front()
            } else {
                None
            }
        };
        if let Some(evicted) = evicted {
            tokio::task::spawn_blocking(move || {
                let _owner = owner;
                drop(evicted);
            })
            .await?;
        }
        Ok(file)
    }
    pub(super) async fn workspace(
        &self,
        owner: ReadOwner,
        cleanup: ReadOwner,
        head: String,
    ) -> Result<Arc<GitCache>, NativeReadError> {
        let admission = self.admit(owner.clone(), 4096).await?;
        let lifetime: ReadOwner = Arc::new((cleanup, admission));
        Ok(GitCache::create_owned(
            self.root.path().to_owned(),
            self.budget.clone(),
            &head,
            self.format,
            None,
            self.native.clone(),
            CacheOwnership {
                work: Arc::new((owner, lifetime.clone())),
                cleanup: Some(lifetime),
            },
        )
        .await?)
    }
    pub(super) async fn install(
        &self,
        cache: Arc<GitCache>,
        descriptor: NativePackDescriptor,
        owner: ReadOwner,
    ) -> Result<(), NativeReadError> {
        cache
            .download_native_owned(&self.store, descriptor, owner.clone())
            .await?;
        let claim = self
            .native
            .try_admit(crate::native_resources::NativeWork::Read)
            .map_err(ObjectReadError::from)?;
        tokio::task::spawn_blocking(move || {
            let (_owner, _claim) = (owner, claim);
            let pack = cache.git_dir().join(format!(
                "objects/pack/pack-{}.pack",
                hex::encode(descriptor.git_checksum)
            ));
            descriptor.verify_files(&pack, &pack.with_extension("idx"))?;
            Ok::<_, IndexError>(())
        })
        .await??;
        self.downloads
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }
    pub(super) async fn body(
        &self,
        object: ResolvedObject,
        limit: usize,
        owner: ReadOwner,
    ) -> Result<Vec<u8>, NativeReadError> {
        // Metadata was selected and verified through the certified catalog. No
        // native lookup is attempted for guessed or unpublished object IDs.
        let expected = object.entry.header.object;
        if expected.size > limit as u64 {
            return Err(ObjectReadError::TooLarge.into());
        }
        let file = self
            .load(object.source.record.native(), Arc::clone(&owner))
            .await?;
        let process_owner: ReadOwner = Arc::new((owner, Arc::clone(&file)));
        let mut objects =
            GitObjects::batch_owned(&file.cache.git_dir(), &self.native, process_owner)?;
        let body = objects.read_verified(expected, limit).await?;
        objects.finish().await?;
        Ok(body)
    }
}

#[cfg(test)]
mod tests;
