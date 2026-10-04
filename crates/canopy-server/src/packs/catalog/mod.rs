//! One immutable catalog binding directory and source roots. Root upload is
//! preparation, never the ref commit point or a canonical/closure certificate.

use super::{
    directory::{
        DirectoryEntry,
        index::{IndexError, RangeIndex},
        snapshot::{DirectorySnapshot, RunLoader, StoredSnapshot},
    },
    sources::{ResolvedSource, SourceIndex, SourceLoader, SourceRoot},
};
use crate::{ObjectFormat, ObjectId};
use canopy_object_storage::artifact::{
    ArtifactDescriptor, ArtifactKey, ArtifactKind, ArtifactStore,
};
use std::sync::Arc;

mod codec;
mod native;
pub use native::{NativeFileStats, NativeReadError};
mod files;
pub use files::{CatalogFileLimits, CatalogFileStats, CatalogFiles, MAX_OPEN_CATALOG_FILES};
mod reader;
pub use reader::{CatalogIndexes, CatalogReader, ResolvedObject};

/// Exactly two root descriptors; history is not a linked list of prior roots.
pub const CATALOG_BYTES: u32 = 1024;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CatalogSnapshot {
    pub directory: StoredSnapshot,
    pub sources: Option<SourceRoot>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StoredCatalog {
    pub repository: [u8; 16],
    pub operation: [u8; 16],
    pub format: ObjectFormat,
    pub artifact: ArtifactDescriptor,
}
impl StoredCatalog {
    pub fn validate(self) -> Result<(), IndexError> {
        if self.artifact.size == 0 || self.artifact.size > u64::from(CATALOG_BYTES) {
            return Err(IndexError::Integrity);
        }
        Ok(())
    }
    fn key(self) -> ArtifactKey {
        ArtifactKey {
            operation: self.operation,
            binding_digest: self.artifact.digest,
            kind: ArtifactKind::CatalogNode,
        }
    }
}
impl CatalogSnapshot {
    pub fn validate(self) -> Result<(), IndexError> {
        if self.directory.artifact.size == 0
            || self.directory.artifact.size > u64::from(super::directory::index::NODE_BYTES)
        {
            return Err(IndexError::Integrity);
        }
        if let Some(root) = self.sources {
            root.validate(self.directory.format)?;
        }
        Ok(())
    }
    /// Persist prepared root bytes. The Cell later publishes this descriptor
    /// together with verified facts, refs and exact durable outcomes under CAS.
    pub async fn upload(
        self,
        store: &ArtifactStore,
        operation: [u8; 16],
    ) -> Result<StoredCatalog, IndexError> {
        self.validate()?;
        if self.directory.repository != store.repository() {
            return Err(IndexError::Integrity);
        }
        let bytes = self.encode(operation)?;
        let digest = *blake3::hash(&bytes).as_bytes();
        let key = ArtifactKey {
            operation,
            binding_digest: digest,
            kind: ArtifactKind::CatalogNode,
        };
        let artifact = store
            .put(key, bytes.len() as u64, digest, &mut bytes.as_slice())
            .await?;
        Ok(StoredCatalog {
            repository: store.repository(),
            operation,
            format: self.directory.format,
            artifact,
        })
    }
    pub async fn download(
        store: &ArtifactStore,
        stored: StoredCatalog,
    ) -> Result<Self, IndexError> {
        stored.validate()?;
        if stored.repository != store.repository() {
            return Err(IndexError::Integrity);
        }
        let mut reader = store.read(stored.key(), stored.artifact).await?;
        let bytes = reader.next().await?.ok_or(IndexError::Integrity)?;
        if reader.next().await?.is_some() {
            return Err(IndexError::Integrity);
        }
        let (snapshot, operation) = Self::decode(&bytes)?;
        if snapshot.directory.repository != stored.repository
            || snapshot.directory.format != stored.format
            || operation != stored.operation
        {
            return Err(IndexError::Integrity);
        }
        Ok(snapshot)
    }
}

#[cfg(test)]
pub(in crate::packs) mod tests;

#[cfg(test)]
pub(crate) mod serving_fixture;
