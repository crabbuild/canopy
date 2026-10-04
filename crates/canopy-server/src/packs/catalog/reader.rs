use super::*;

/// One repository/format's bounded node clients, retained by a read worker
/// across catalog publications. New generations reuse unchanged authenticated
/// nodes rather than allocating new caches per snapshot/request.
pub struct CatalogIndexes {
    store: Arc<ArtifactStore>,
    ranges: RangeIndex,
    sources: Arc<SourceIndex>,
    inputs: super::super::sources::NativeInputIndex,
    refs: super::super::ref_state::RefStateIndex,
}
impl CatalogIndexes {
    pub fn new(store: Arc<ArtifactStore>, format: ObjectFormat) -> Self {
        Self {
            ranges: RangeIndex::new(Arc::clone(&store), format),
            sources: Arc::new(SourceIndex::new(Arc::clone(&store), format)),
            inputs: super::super::sources::NativeInputIndex::new(Arc::clone(&store), format),
            refs: super::super::ref_state::RefStateIndex::new(Arc::clone(&store), format),
            store,
        }
    }
    pub(in crate::packs) fn store(&self) -> Arc<ArtifactStore> {
        Arc::clone(&self.store)
    }
    pub(in crate::packs) fn sources(&self) -> Arc<SourceIndex> {
        Arc::clone(&self.sources)
    }
    pub(in crate::packs) fn inputs(&self) -> &super::super::sources::NativeInputIndex {
        &self.inputs
    }
    pub(in crate::packs) fn refs(&self) -> &super::super::ref_state::RefStateIndex {
        &self.refs
    }
    pub fn input_stats(&self) -> super::super::directory::index::ReadStats {
        self.inputs.stats()
    }
    pub(in crate::packs) fn ranges(&self) -> &RangeIndex {
        &self.ranges
    }
    pub fn stats(
        &self,
    ) -> (
        super::super::directory::index::ReadStats,
        super::super::directory::index::ReadStats,
    ) {
        (self.ranges.stats(), self.sources.stats())
    }
}

/// Immutable reader for one complete catalog binding. The service supplies
/// authorization, closure certificates, generation leases and loader admission.
/// Opening verifies root context; it does not certify every descendant.
pub struct CatalogReader {
    stored: StoredCatalog,
    directory: DirectorySnapshot,
    indexes: Arc<CatalogIndexes>,
    source_root: Option<SourceRoot>,
}
pub struct ResolvedObject {
    pub entry: DirectoryEntry,
    pub source: ResolvedSource,
}
impl CatalogReader {
    pub async fn open(
        indexes: Arc<CatalogIndexes>,
        stored: StoredCatalog,
    ) -> Result<Self, IndexError> {
        if stored.repository != indexes.store.repository()
            || stored.format != indexes.sources.format()
        {
            return Err(IndexError::Integrity);
        }
        let snapshot = CatalogSnapshot::download(&indexes.store, stored).await?;
        let directory = DirectorySnapshot::download(&indexes.store, snapshot.directory).await?;
        let nonempty =
            !directory.level_zero.is_empty() || directory.levels.iter().any(Option::is_some);
        if nonempty && snapshot.sources.is_none() {
            return Err(IndexError::Integrity);
        }
        // At most 48 directory roots and one source root, independent of
        // the number of objects/artifacts. Node clients retain bounded caches.
        for root in directory
            .level_zero
            .iter()
            .chain(directory.levels.iter().flatten())
        {
            indexes.ranges.validate_root(*root).await?;
        }
        if let Some(root) = snapshot.sources {
            indexes.sources.validate_root(root).await?;
        }
        Ok(Self {
            stored,
            directory,
            indexes,
            source_root: snapshot.sources,
        })
    }
    pub fn stored(&self) -> StoredCatalog {
        self.stored
    }
    pub(in crate::packs) fn directory(&self) -> DirectorySnapshot {
        self.directory.clone()
    }
    pub(in crate::packs) fn source_root(&self) -> Option<SourceRoot> {
        self.source_root
    }
    pub async fn lookup(
        &self,
        oid: ObjectId,
        runs: &impl RunLoader,
        metadata: &impl SourceLoader,
    ) -> Result<Option<ResolvedObject>, IndexError> {
        let Some(entry) = self
            .directory
            .lookup(&self.indexes.ranges, runs, oid)
            .await?
        else {
            return Ok(None);
        };
        let source = self
            .indexes
            .sources
            .resolve(self.source_root, entry, metadata)
            .await?;
        Ok(Some(ResolvedObject { entry, source }))
    }
    /// Authenticated canonical headers in request order, at most 512. This
    /// verifies preferred source bindings; it does not grant closure authority.
    /// Each metadata file is released before the next group is opened.
    pub async fn headers(
        &self,
        ids: &[ObjectId],
        runs: &impl RunLoader,
        metadata: &impl SourceLoader,
    ) -> Result<Vec<Option<super::super::metadata::ObjectHeader>>, IndexError> {
        let entries = self
            .directory
            .lookup_batch(&self.indexes.ranges, runs, ids)
            .await?;
        let mut groups = std::collections::BTreeMap::<
            super::super::directory::SegmentKey,
            Vec<(usize, DirectoryEntry)>,
        >::new();
        for (at, entry) in entries.into_iter().enumerate() {
            if let Some(entry) = entry {
                groups.entry(entry.source).or_default().push((at, entry));
            }
        }
        let mut output = vec![None; ids.len()];
        for (key, group) in groups {
            let record = self
                .indexes
                .sources
                .find(self.source_root, key)
                .await?
                .ok_or(IndexError::Integrity)?;
            let segment = metadata.load(record.metadata).await?;
            let requested: Vec<_> = group.iter().map(|(_, entry)| *entry).collect();
            tokio::task::spawn_blocking(move || {
                record.verify_directory_entries(&segment, &requested)
            })
            .await
            .map_err(super::super::metadata::MetadataError::from)??;
            for (at, entry) in group {
                output[at] = Some(entry.header);
            }
        }
        Ok(output)
    }
}
