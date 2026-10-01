//! Authenticated immutable Git artifacts, scoped to a creating operation.
//!
//! Reusing a content digest after collection uses a different operation path,
//! so a delayed provider delete cannot remove a later artifact incarnation.

use crate::external::{self, MAX_ARTIFACT_BYTES, PART_BYTES};
use bytes::Bytes;
use object_store::{ObjectStore, path::Path};
use std::{sync::Arc, time::Duration};
use tokio::io::{AsyncRead, AsyncReadExt};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ArtifactKind {
    Pack,
    Index,
    Metadata,
    DirectoryRun,
    CatalogNode,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ArtifactKey {
    pub operation: [u8; 16],
    /// Parent pack digest for pack/index/metadata; the artifact's own digest
    /// for directory runs and catalog nodes.
    pub binding_digest: [u8; 32],
    pub kind: ArtifactKind,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ArtifactDescriptor {
    pub size: u64,
    pub digest: [u8; 32],
    pub manifest_digest: [u8; 32],
}

#[derive(Debug, thiserror::Error)]
pub enum ArtifactError {
    #[error("artifact store failed")]
    Store(#[source] object_store::Error),
    #[error("artifact input failed")]
    Io(#[from] std::io::Error),
    #[error("artifact hashing worker failed")]
    Task(#[from] tokio::task::JoinError),
    #[error("artifact bytes disagree with the authenticated descriptor")]
    Corrupt,
    #[error("artifact transfer timed out")]
    Timeout,
    #[error("artifact exceeds the bounded manifest limit")]
    TooLarge,
}

#[derive(Clone)]
pub struct ArtifactStore {
    store: Arc<dyn ObjectStore>,
    repository: [u8; 16],
}

impl From<object_store::Error> for ArtifactError {
    fn from(error: object_store::Error) -> Self {
        if external::is_corruption(&error) {
            Self::Corrupt
        } else {
            Self::Store(error)
        }
    }
}
impl ArtifactStore {
    pub fn new(store: Arc<dyn ObjectStore>, repository: [u8; 16]) -> Self {
        Self { store, repository }
    }

    pub fn repository(&self) -> [u8; 16] {
        self.repository
    }
    /// In-process capability identity. Separately constructed providers are
    /// never assumed equivalent from a display name or a matching repository.
    /// Clone the service-owned store when carrying a local verification proof.
    pub fn same_binding(&self, other: &Self) -> bool {
        self.repository == other.repository && Arc::ptr_eq(&self.store, &other.store)
    }

    pub fn path(&self, key: ArtifactKey, digest: [u8; 32]) -> Result<Path, ArtifactError> {
        let root = format!(
            "repos/{}/git-packs/{}/{}",
            hex::encode(self.repository),
            uuid::Uuid::from_bytes(key.operation),
            hex::encode(key.binding_digest)
        );
        match key.kind {
            ArtifactKind::Pack if digest != key.binding_digest => Err(ArtifactError::Corrupt),
            ArtifactKind::Pack => Ok(Path::from(format!("{root}/pack"))),
            ArtifactKind::Index => Ok(Path::from(format!("{root}/index/{}", hex::encode(digest)))),
            ArtifactKind::Metadata => Ok(Path::from(format!(
                "{root}/metadata/{}",
                hex::encode(digest)
            ))),
            ArtifactKind::DirectoryRun | ArtifactKind::CatalogNode
                if key.binding_digest != digest =>
            {
                Err(ArtifactError::Corrupt)
            }
            ArtifactKind::DirectoryRun | ArtifactKind::CatalogNode => {
                let kind = if key.kind == ArtifactKind::DirectoryRun {
                    "directory"
                } else {
                    "nodes"
                };
                Ok(Path::from(format!(
                    "repos/{}/git-catalogs/{}/{kind}/{}",
                    hex::encode(self.repository),
                    uuid::Uuid::from_bytes(key.operation),
                    hex::encode(digest)
                )))
            }
        }
    }

    /// Read exactly one frame and compare its verified digest before publication.
    /// A canceled writer can leave unregistered staging; its operation owns it.
    pub async fn put(
        &self,
        key: ArtifactKey,
        size: u64,
        digest: [u8; 32],
        input: &mut (impl AsyncRead + Unpin),
    ) -> Result<ArtifactDescriptor, ArtifactError> {
        if size > MAX_ARTIFACT_BYTES {
            return Err(ArtifactError::TooLarge);
        }
        let path = self.path(key, digest)?;
        let family = match key.kind {
            ArtifactKind::DirectoryRun | ArtifactKind::CatalogNode => "git-catalogs",
            _ => "git-packs",
        };
        let stage = Path::from(format!(
            "repos/{}/{family}/{}/staging/{}",
            hex::encode(self.repository),
            uuid::Uuid::from_bytes(key.operation),
            uuid::Uuid::new_v4()
        ));
        let mut upload = external::Upload::new(Arc::clone(&self.store), stage).await?;
        let result = async {
            let mut hash = blake3::Hasher::new();
            let mut remaining = size;
            let mut parts = Vec::with_capacity(size.div_ceil(PART_BYTES as u64).max(1) as usize);
            loop {
                let length = remaining.min(PART_BYTES as u64) as usize;
                let mut bytes = vec![0; length];
                tokio::time::timeout(Duration::from_secs(120), input.read_exact(&mut bytes))
                    .await
                    .map_err(|_| ArtifactError::Timeout)??;
                let (next, part, bytes) = tokio::task::spawn_blocking(move || {
                    hash.update(&bytes);
                    let part = *blake3::hash(&bytes).as_bytes();
                    (hash, part, Bytes::from(bytes))
                })
                .await?;
                hash = next;
                parts.push(part);
                upload.write(bytes).await?;
                remaining -= length as u64;
                if remaining == 0 {
                    break;
                }
            }
            if hash.finalize().as_bytes() != &digest {
                return Err(ArtifactError::Corrupt);
            }
            let manifest_digest = upload.publish_hashed(&path, size, &parts).await?;
            Ok(ArtifactDescriptor {
                size,
                digest,
                manifest_digest,
            })
        }
        .await;
        let cleanup = upload.cleanup().await;
        match result {
            Ok(descriptor) => {
                cleanup?;
                Ok(descriptor)
            }
            Err(error) => {
                if let Err(cleanup) = cleanup {
                    tracing::warn!(error = %cleanup,"artifact staging cleanup failed");
                }
                Err(error)
            }
        }
    }

    pub async fn read(
        &self,
        key: ArtifactKey,
        descriptor: ArtifactDescriptor,
    ) -> Result<ArtifactRead, ArtifactError> {
        if descriptor.size > MAX_ARTIFACT_BYTES {
            return Err(ArtifactError::TooLarge);
        }
        let path = self.path(key, descriptor.digest)?;
        let manifest = external::open_hashed(
            self.store.as_ref(),
            &path,
            descriptor.size,
            descriptor.manifest_digest,
        )
        .await?;
        if descriptor.size == 0
            && (descriptor.digest != *blake3::hash(b"").as_bytes()
                || manifest.part_digest(0) != Some(*blake3::hash(b"").as_bytes()))
        {
            return Err(ArtifactError::Corrupt);
        }
        Ok(ArtifactRead {
            store: Arc::clone(&self.store),
            path,
            manifest,
            descriptor,
            offset: 0,
            hash: Some(blake3::Hasher::new()),
        })
    }
}

/// One bounded authenticated part at a time. Errors/cancellation poison the
/// reader, preventing continuation with an incomplete checksum history.
pub struct ArtifactRead {
    store: Arc<dyn ObjectStore>,
    path: Path,
    manifest: external::Manifest,
    descriptor: ArtifactDescriptor,
    offset: u64,
    hash: Option<blake3::Hasher>,
}
impl ArtifactRead {
    pub fn descriptor(&self) -> ArtifactDescriptor {
        self.descriptor
    }
    pub async fn next(&mut self) -> Result<Option<Bytes>, ArtifactError> {
        if self.offset == self.descriptor.size {
            return Ok(None);
        }
        let mut hash = self.hash.take().ok_or(ArtifactError::Corrupt)?;
        let bytes = external::read(
            self.store.as_ref(),
            &self.path,
            &self.manifest,
            self.descriptor.size,
            self.offset,
        )
        .await?;
        let expected = self
            .manifest
            .part_digest(self.offset / PART_BYTES as u64)
            .ok_or(ArtifactError::Corrupt)?;
        let (next, valid, bytes) = tokio::task::spawn_blocking(move || {
            let valid = blake3::hash(&bytes).as_bytes() == &expected;
            hash.update(&bytes);
            (hash, valid, bytes)
        })
        .await?;
        if !valid {
            return Err(ArtifactError::Corrupt);
        }
        let end = self.offset + bytes.len() as u64;
        if end == self.descriptor.size && next.finalize().as_bytes() != &self.descriptor.digest {
            return Err(ArtifactError::Corrupt);
        }
        self.hash = Some(next);
        self.offset = end;
        Ok(Some(bytes))
    }
}

#[cfg(test)]
mod tests;
