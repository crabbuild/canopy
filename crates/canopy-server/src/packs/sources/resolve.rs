use super::super::{
    directory::DirectoryEntry,
    metadata::{MetadataError, MetadataSegment},
};
use super::*;
use std::{future::Future, sync::Arc};

/// Service-owned admitted loading/caching. Implementations must retain file
/// admission and verify whole authenticated metadata bytes before opening SQL.
pub trait SourceLoader: Sync {
    fn load(
        &self,
        segment: StoredSegment,
    ) -> impl Future<Output = Result<Arc<MetadataSegment>, MetadataError>> + Send;
}

/// Pins the exact preferred metadata file, not a publication/closure proof.
pub struct ResolvedSource {
    pub record: SourceRecord,
    pub metadata: Arc<MetadataSegment>,
}
impl SourceIndex {
    /// Resolve an already selected directory entry through its pinned source
    /// root. The caller supplies authorization and retains the catalog reader
    /// lease throughout this call and subsequent artifact reads.
    pub async fn resolve(
        &self,
        root: Option<SourceRoot>,
        entry: DirectoryEntry,
        loader: &impl SourceLoader,
    ) -> Result<ResolvedSource, IndexError> {
        if entry.header.object.oid.format() != self.format() {
            return Err(IndexError::Integrity);
        }
        let record = self
            .find(root, entry.source)
            .await?
            .ok_or(IndexError::Integrity)?;
        let metadata = loader.load(record.metadata).await?;
        tokio::task::spawn_blocking(move || {
            record.verify_directory_entry(&metadata, entry)?;
            Ok(ResolvedSource { record, metadata })
        })
        .await
        .map_err(MetadataError::from)?
    }
}
