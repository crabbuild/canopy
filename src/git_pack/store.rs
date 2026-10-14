//! Immutable native pack publication and repository-scoped disposable copies.

use super::*;
use bytes::Bytes;
use cellule_ltx::{DiskBudget, DiskReservation};
use object_store::{ObjectStore, ObjectStoreExt, path::Path as ObjectPath};
use std::future::poll_fn;
use std::{collections::BTreeMap, path::PathBuf, sync::Arc};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Mutex;

const TRANSFER_BYTES: usize = 8 * 1024 * 1024;

/// Native files and their disk charge remain owned through background work.
pub struct OwnedPack {
    data: Option<GitPack>,
    directory: Option<tempfile::TempDir>,
    reservation: Option<DiskReservation>,
    native_owner: Option<Arc<crate::git_cache::GitCache>>,
}

#[cfg(test)]
mod tests;

impl OwnedPack {
    /// Takes exclusive ownership of `objects.pack` and `objects.idx`.
    pub fn open(
        directory: tempfile::TempDir,
        identity: PackIdentity,
        reservation: DiskReservation,
        max_index_bytes: usize,
        max_decode_bytes: usize,
    ) -> Result<Self, PackError> {
        let mut pack = Self::pending(directory, reservation);
        pack.verify(identity, max_index_bytes, max_decode_bytes)?;
        Ok(pack)
    }

    pub(super) fn pending(directory: tempfile::TempDir, reservation: DiskReservation) -> Self {
        Self {
            data: None,
            directory: Some(directory),
            reservation: Some(reservation),
            native_owner: None,
        }
    }

    pub(super) fn verify(
        &mut self,
        identity: PackIdentity,
        max_index_bytes: usize,
        max_decode_bytes: usize,
    ) -> Result<(), PackError> {
        let required = identity
            .pack_bytes
            .checked_add(identity.index_bytes)
            .ok_or(PackError::Limit)?;
        if self
            .reservation
            .as_ref()
            .ok_or(PackError::Integrity)?
            .bytes()
            < required
        {
            return Err(PackError::Limit);
        }
        let data = GitPack::open(
            &self.directory()?.join("objects.pack"),
            identity,
            max_index_bytes,
            max_decode_bytes,
        )?;
        self.data = Some(data);
        Ok(())
    }

    pub(super) fn with_native_owner(mut self, cache: Arc<crate::git_cache::GitCache>) -> Self {
        self.native_owner = Some(cache);
        self
    }

    pub(super) fn reservation(&self) -> Result<&DiskReservation, PackError> {
        self.reservation.as_ref().ok_or(PackError::Integrity)
    }

    pub fn data(&self) -> Result<&GitPack, PackError> {
        self.data.as_ref().ok_or(PackError::Integrity)
    }

    pub fn directory(&self) -> Result<&Path, PackError> {
        self.directory
            .as_ref()
            .map(|directory| directory.path())
            .ok_or(PackError::Integrity)
    }
}

impl Drop for OwnedPack {
    fn drop(&mut self) {
        if let Some(directory) = &self.directory
            && crate::native_git::idle_fence(directory.path()).is_err()
        {
            tracing::error!(
                "Git pack cleanup fenced by a live native worker; disk admission retained"
            );
            if let Some(reservation) = self.reservation.take() {
                std::mem::forget(reservation);
            }
            if let Some(cache) = self.native_owner.take() {
                std::mem::forget(cache);
            }
            if let Some(directory) = self.directory.as_mut() {
                directory.disable_cleanup(true);
            }
            return;
        }
        // Close mappings before unlinking files or releasing their disk charge.
        drop(self.data.take());
        if let Some(directory) = self.directory.take() {
            let path = directory.path().to_path_buf();
            if let Err(error) = directory.close() {
                tracing::error!(path = %path.display(), error = %error, "Git pack cleanup failed; disk admission retained until process restart");
                if let Some(reservation) = self.reservation.take() {
                    std::mem::forget(reservation);
                }
            }
        }
    }
}

/// Stores native compressed content separately from the transactional Cell.
#[derive(Clone)]
pub struct PackStore {
    store: Arc<dyn ObjectStore>,
    repository: [u8; 16],
    local: PathBuf,
    budget: DiskBudget,
    max_index_bytes: usize,
    max_decode_bytes: usize,
    cache: Arc<Mutex<BTreeMap<[u8; 32], Arc<OwnedPack>>>>,
}

impl PackStore {
    pub fn new(
        store: Arc<dyn ObjectStore>,
        repository: [u8; 16],
        local: PathBuf,
        budget: DiskBudget,
        max_index_bytes: usize,
        max_decode_bytes: usize,
    ) -> cellule_runtime::Result<Self> {
        crate::validate_repository_id(repository)?;
        if max_index_bytes == 0 || max_decode_bytes == 0 {
            return Err(cellule_runtime::Error::Command(
                "Git pack reader budgets must be nonzero",
            ));
        }
        Ok(Self {
            store,
            repository,
            local,
            budget,
            max_index_bytes,
            max_decode_bytes,
            cache: Arc::new(Mutex::new(BTreeMap::new())),
        })
    }

    pub fn repository_id(&self) -> [u8; 16] {
        self.repository
    }
    pub async fn verify(&self, identity: &PackIdentity) -> Result<(), PackError> {
        self.verify_file(
            &self.pack_path(identity),
            identity.pack_bytes,
            identity.pack_digest,
        )
        .await?;
        self.verify_file(
            &self.index_path(identity),
            identity.index_bytes,
            identity.index_digest,
        )
        .await
    }
    pub fn paths(&self, identity: &PackIdentity) -> [ObjectPath; 2] {
        [self.pack_path(identity), self.index_path(identity)]
    }
    pub(super) fn local_root(&self) -> &Path {
        &self.local
    }
    pub(super) fn disk_budget(&self) -> &DiskBudget {
        &self.budget
    }
    pub(super) fn index_limit(&self) -> usize {
        self.max_index_bytes
    }
    pub(super) fn decode_limit(&self) -> usize {
        self.max_decode_bytes
    }

    /// Publication owns source files through immutable copying and cleanup even
    /// when the request that started it disappears. Both files verify remotely
    /// before metadata may name this pair.
    pub async fn publish(&self, source: Arc<OwnedPack>) -> Result<PackIdentity, PackError> {
        let store = self.clone();
        tokio::spawn(async move {
            let identity = source.data()?.identity().clone();
            store
                .publish_file(
                    source.directory()?.join("objects.pack"),
                    store.pack_path(&identity),
                    identity.pack_bytes,
                    identity.pack_digest,
                )
                .await?;
            store
                .publish_file(
                    source.directory()?.join("objects.idx"),
                    store.index_path(&identity),
                    identity.index_bytes,
                    identity.index_digest,
                )
                .await?;
            Ok(identity)
        })
        .await?
    }

    /// Opens a verified local pair once per resident repository. The shared disk
    /// budget is charged before download; repository eviction releases the pair.
    pub async fn load(&self, identity: &PackIdentity) -> Result<Arc<OwnedPack>, PackError> {
        if identity.index_bytes > self.max_index_bytes as u64 {
            return Err(PackError::Limit);
        }
        // Waiters are cancellable before dispatch. One resident repository may
        // dispatch only one download; cancellation then retains its disk charge
        // and scratch until file I/O, verification and cache installation join.
        let mut cache = Arc::clone(&self.cache).lock_owned().await;
        if let Some(pack) = cache.get(&identity.pack_digest) {
            if pack.data()?.identity() != identity {
                return Err(PackError::Integrity);
            }
            return Ok(Arc::clone(pack));
        }
        let store = self.clone();
        let identity = identity.clone();
        tokio::spawn(async move { store.load_inner(&identity, &mut cache).await }).await?
    }

    async fn load_inner(
        &self,
        identity: &PackIdentity,
        cache: &mut BTreeMap<[u8; 32], Arc<OwnedPack>>,
    ) -> Result<Arc<OwnedPack>, PackError> {
        let required = identity
            .pack_bytes
            .checked_add(identity.index_bytes)
            .ok_or(PackError::Limit)?;
        let reservation = self.budget.try_reserve(required)?;
        let local = self.local.clone();
        let directory = tokio::task::spawn_blocking(move || {
            tempfile::Builder::new()
                .prefix("canopy-pack-")
                .tempdir_in(local)
        })
        .await??;
        let mut owned = OwnedPack::pending(directory, reservation);
        self.download_file(
            self.pack_path(identity),
            owned.directory()?.join("objects.pack"),
            identity.pack_bytes,
            identity.pack_digest,
        )
        .await?;
        self.download_file(
            self.index_path(identity),
            owned.directory()?.join("objects.idx"),
            identity.index_bytes,
            identity.index_digest,
        )
        .await?;
        let identity = identity.clone();
        let (max_index_bytes, max_decode_bytes) = (self.max_index_bytes, self.max_decode_bytes);
        let pack = Arc::new(
            tokio::task::spawn_blocking(move || {
                owned.verify(identity, max_index_bytes, max_decode_bytes)?;
                Ok::<_, PackError>(owned)
            })
            .await??,
        );
        cache.insert(pack.data()?.identity().pack_digest, Arc::clone(&pack));
        Ok(pack)
    }

    fn pack_path(&self, identity: &PackIdentity) -> ObjectPath {
        ObjectPath::from(format!(
            "repos/{}/git-packs/{}.pack",
            hex::encode(self.repository),
            hex::encode(identity.pack_digest)
        ))
    }

    fn index_path(&self, identity: &PackIdentity) -> ObjectPath {
        ObjectPath::from(format!(
            "repos/{}/git-packs/{}.idx",
            hex::encode(self.repository),
            hex::encode(identity.index_digest)
        ))
    }

    async fn publish_file(
        &self,
        source: PathBuf,
        destination: ObjectPath,
        size: u64,
        digest: [u8; 32],
    ) -> Result<(), PackError> {
        let staging = ObjectPath::from(format!(
            "repos/{}/git-pack-staging/{}",
            hex::encode(self.repository),
            uuid::Uuid::new_v4()
        ));
        let mut upload = self.store.put_multipart(&staging).await?;
        let result = async {
            let mut file = tokio::fs::File::open(source).await?;
            let mut buffer = vec![0; TRANSFER_BYTES];
            let mut hash = blake3::Hasher::new();
            let mut length = 0_u64;
            loop {
                let count = (size - length).min(TRANSFER_BYTES as u64) as usize;
                if count == 0 {
                    break;
                }
                file.read_exact(&mut buffer[..count]).await?;
                length = length.checked_add(count as u64).ok_or(PackError::Limit)?;
                if length > size {
                    return Err(PackError::Integrity);
                }
                hash.update(&buffer[..count]);
                upload
                    .put_part(Bytes::copy_from_slice(&buffer[..count]).into())
                    .await?;
            }
            if length != size || hash.finalize().as_bytes() != &digest {
                return Err(PackError::Integrity);
            }
            if file.read(&mut [0; 1]).await? != 0 {
                return Err(PackError::Integrity);
            }
            upload.complete().await?;
            match self.store.copy_if_not_exists(&staging, &destination).await {
                Ok(()) | Err(object_store::Error::AlreadyExists { .. }) => {}
                Err(error) => return Err(error.into()),
            }
            self.verify_file(&destination, size, digest).await
        }
        .await;
        if result.is_err()
            && let Err(error) = upload.abort().await
        {
            tracing::warn!(error = %error, "Git pack multipart cleanup failed");
        }
        let cleanup = self.store.delete(&staging).await;
        result?;
        match cleanup {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    async fn verify_file(
        &self,
        path: &ObjectPath,
        size: u64,
        digest: [u8; 32],
    ) -> Result<(), PackError> {
        let result = self.store.get(path).await?;
        if result.meta.size != size || result.range != (0..size) {
            return Err(PackError::Integrity);
        }
        let mut stream = result.into_stream();
        let mut length = 0_u64;
        let mut hash = blake3::Hasher::new();
        while let Some(bytes) = poll_fn(|cx| stream.as_mut().poll_next(cx)).await {
            let bytes = bytes?;
            length = length
                .checked_add(bytes.len() as u64)
                .ok_or(PackError::Limit)?;
            if length > size {
                return Err(PackError::Integrity);
            }
            hash.update(&bytes);
        }
        if length != size || hash.finalize().as_bytes() != &digest {
            return Err(PackError::Integrity);
        }
        Ok(())
    }

    async fn download_file(
        &self,
        source: ObjectPath,
        destination: PathBuf,
        size: u64,
        digest: [u8; 32],
    ) -> Result<(), PackError> {
        let result = self.store.get(&source).await?;
        if result.meta.size != size || result.range != (0..size) {
            return Err(PackError::Integrity);
        }
        let mut stream = result.into_stream();
        let mut file = tokio::fs::File::create(destination).await?;
        let mut length = 0_u64;
        let mut hash = blake3::Hasher::new();
        while let Some(bytes) = poll_fn(|cx| stream.as_mut().poll_next(cx)).await {
            let bytes = bytes?;
            length = length
                .checked_add(bytes.len() as u64)
                .ok_or(PackError::Limit)?;
            if length > size {
                return Err(PackError::Integrity);
            }
            hash.update(&bytes);
            file.write_all(&bytes).await?;
        }
        if length != size || hash.finalize().as_bytes() != &digest {
            return Err(PackError::Integrity);
        }
        file.sync_all().await?;
        Ok(())
    }
}
