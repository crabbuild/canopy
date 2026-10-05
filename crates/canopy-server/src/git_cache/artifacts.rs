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
