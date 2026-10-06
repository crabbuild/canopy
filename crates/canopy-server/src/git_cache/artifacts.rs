//! Direct authenticated downloads into an unpublished native workspace.
use super::*;
use crate::packs::{metadata::MetadataError, sources::NativePackDescriptor};
use canopy_object_storage::artifact::{ArtifactKind, ArtifactStore};

struct Writer {
    file: File,
    _cache: Arc<GitCache>,
    _owner: crate::git_objects::ReadOwner,
}
impl GitCache {
    pub(crate) fn native_pack_path(&self, descriptor: NativePackDescriptor) -> PathBuf {
        self.git_dir().join(format!(
            "objects/pack/pack-{}.pack",
            hex::encode(descriptor.git_checksum)
        ))
    }

    /// Read the original index completely, retaining its manifest and native
    /// checksums. The sparse private decoder below grants no object authority.
    pub(crate) async fn native_index_owned(
        self: &Arc<Self>,
        store: &ArtifactStore,
        descriptor: NativePackDescriptor,
        owner: crate::git_objects::ReadOwner,
    ) -> Result<crate::git_format::pack_index::PackIndex, MetadataError> {
        descriptor
            .validate(store.repository(), self.object_format)
            .map_err(|_| MetadataError::Integrity)?;
        let cache = self.clone();
        let held = owner.clone();
        let mut writer = tokio::task::spawn_blocking(move || {
            let _owner = held;
            cache.reservation()?.try_grow(
                descriptor
                    .index
                    .size
                    .checked_add(4096)
                    .ok_or(MetadataError::Limit)?,
            )?;
            Ok::<_, MetadataError>(Writer {
                file: File::create_new(cache.native_pack_path(descriptor).with_extension("idx"))?,
                _cache: cache,
                _owner,
            })
        })
        .await??;
        let mut input = store
            .read_owned(
                descriptor
                    .key(ArtifactKind::Index)
                    .map_err(|_| MetadataError::Integrity)?,
                descriptor.index,
                owner.clone(),
            )
            .await?;
        while let Some(bytes) = input.next().await? {
            writer = tokio::task::spawn_blocking(move || {
                writer.file.write_all(&bytes)?;
                Ok::<_, MetadataError>(writer)
            })
            .await??;
        }
        tokio::task::spawn_blocking(move || {
            let index = crate::git_format::pack_index::PackIndex::open(
                writer
                    ._cache
                    .native_pack_path(descriptor)
                    .with_extension("idx"),
                writer._cache.object_format,
            )?;
            if index.len() != descriptor.object_count
                || index.pack_checksum() != descriptor.git_checksum
            {
                return Err(MetadataError::Integrity);
            }
            Ok(index)
        })
        .await?
    }

    /// This is an incomplete private decoder file, never an accepted pack.
    /// Its framing comes from the certified descriptor; selected payloads are
    /// filled only from authenticated provider parts. Every extracted object
    /// still needs canonical verification before leaving this workspace.
    pub(crate) async fn sparse_native_owned(
        self: &Arc<Self>,
        descriptor: NativePackDescriptor,
        owner: crate::git_objects::ReadOwner,
    ) -> Result<(), MetadataError> {
        let cache = self.clone();
        tokio::task::spawn_blocking(move || {
            use std::io::{Seek, SeekFrom};
            let _owner = owner;
            let tail = descriptor
                .pack
                .size
                .checked_sub(descriptor.git_checksum.len() as u64)
                .filter(|tail| *tail >= 12)
                .ok_or(MetadataError::Integrity)?;
            cache.reservation()?.try_grow(8192)?;
            let mut file = File::create_new(cache.native_pack_path(descriptor))?;
            file.set_len(descriptor.pack.size)?;
            file.write_all(b"PACK")?;
            file.write_all(&2_u32.to_be_bytes())?;
            file.write_all(&descriptor.object_count.to_be_bytes())?;
            file.seek(SeekFrom::Start(tail))?;
            file.write_all(descriptor.git_checksum.as_ref())?;
            Ok::<_, MetadataError>(())
        })
        .await?
    }

    pub(crate) async fn sparse_payload_owned(
        self: &Arc<Self>,
        descriptor: NativePackDescriptor,
        offset: u64,
        bytes: bytes::Bytes,
        owner: crate::git_objects::ReadOwner,
    ) -> Result<(), MetadataError> {
        let cache = self.clone();
        tokio::task::spawn_blocking(move || {
            use std::io::{Seek, SeekFrom};
            let _owner = owner;
            let end = offset
                .checked_add(bytes.len() as u64)
                .ok_or(MetadataError::Limit)?;
            let tail = descriptor
                .pack
                .size
                .checked_sub(descriptor.git_checksum.len() as u64)
                .ok_or(MetadataError::Integrity)?;
            if offset < 12 || end > tail {
                return Err(MetadataError::Integrity);
            }
            // Include alignment overhead before allocating sparse file blocks.
            let charge = (bytes.len() as u64)
                .div_ceil(4096)
                .checked_add(1)
                .and_then(|pages| pages.checked_mul(4096))
                .ok_or(MetadataError::Limit)?;
            cache.reservation()?.try_grow(charge)?;
            let mut file = OpenOptions::new()
                .write(true)
                .open(cache.native_pack_path(descriptor))?;
            file.seek(SeekFrom::Start(offset))?;
            file.write_all(&bytes)?;
            Ok::<_, MetadataError>(())
        })
        .await?
    }

    pub(crate) fn reserve_native_spool(&self, bytes: u64) -> Result<(), MetadataError> {
        self.reservation()?.try_grow(bytes)?;
        Ok(())
    }

    /// The isolated verifier calls this exactly once on its fresh private cache.
    /// Reserve the complete pair before creating files or reading the provider.
    /// No second pack copy or blob-as-artifact wrapper is involved.
    #[cfg(test)]
    pub(crate) async fn download_native(
        self: &Arc<Self>,
        store: &ArtifactStore,
        descriptor: NativePackDescriptor,
    ) -> Result<(), MetadataError> {
        self.download_native_owned(store, descriptor, Arc::new(()))
            .await
    }

    pub(crate) async fn download_native_owned(
        self: &Arc<Self>,
        store: &ArtifactStore,
        descriptor: NativePackDescriptor,
        owner: crate::git_objects::ReadOwner,
    ) -> Result<(), MetadataError> {
        descriptor
            .validate(store.repository(), self.object_format)
            .map_err(|_| MetadataError::Integrity)?;
        if self.objects.is_some() {
            return Err(MetadataError::Integrity);
        }
        let cache = Arc::clone(self);
        let admission = Arc::clone(&owner);
        tokio::task::spawn_blocking(move || {
            let _owner = admission;
            let size = descriptor
                .pack
                .size
                .checked_add(descriptor.index.size)
                .ok_or(MetadataError::Limit)?;
            cache.reservation()?.try_grow(size)?;
            Ok::<_, MetadataError>(())
        })
        .await??;
        for (kind, artifact, extension) in [
            (ArtifactKind::Pack, descriptor.pack, "pack"),
            (ArtifactKind::Index, descriptor.index, "idx"),
        ] {
            let cache = Arc::clone(self);
            let admission = Arc::clone(&owner);
            let mut writer = tokio::task::spawn_blocking(move || {
                let path = cache.git_dir().join(format!(
                    "objects/pack/pack-{}.{}",
                    hex::encode(descriptor.git_checksum),
                    extension
                ));
                Ok::<_, MetadataError>(Writer {
                    file: File::create_new(path)?,
                    _cache: cache,
                    _owner: admission,
                })
            })
            .await??;
            let mut input = store
                .read_owned(
                    descriptor.key(kind).map_err(|_| MetadataError::Integrity)?,
                    artifact,
                    owner.clone(),
                )
                .await?;
            while let Some(bytes) = input.next().await? {
                writer = tokio::task::spawn_blocking(move || {
                    writer.file.write_all(&bytes)?;
                    Ok::<_, MetadataError>(writer)
                })
                .await??;
            }
            tokio::task::spawn_blocking(move || {
                writer.file.sync_all()?;
                Ok::<_, MetadataError>(())
            })
            .await??;
        }
        Ok(())
    }
}
