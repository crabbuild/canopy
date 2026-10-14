//! Repository-scoped typed artifact inventory for pinned backup snapshots.
use super::{
    catalog::{CatalogIndexes, CatalogSnapshot, StoredCatalog},
    directory::{
        StoredRun,
        index::{IndexVisitor, WalkResult},
        snapshot::DirectorySnapshot,
    },
    ref_state::{RefStateRecord, RefStateSnapshotRoot},
    sources::{NativeInputRoot, NativePackDescriptor, SourceRecord},
};
use crate::ObjectFormat;
use canopy_object_storage::artifact::{
    ArtifactDescriptor, ArtifactKey, ArtifactKind, ArtifactStore,
};
use cellule_runtime::{
    CellTarget,
    codec::{BoundedDecoder, WireValue},
};
use std::sync::Arc;

pub(crate) use super::directory::index::ArtifactVisitor;

pub(crate) struct Inventory<'a> {
    store: Arc<ArtifactStore>,
    format: ObjectFormat,
    indexes: Arc<CatalogIndexes>,
    visitor: &'a mut dyn ArtifactVisitor,
    target: CellTarget,
}
impl<'a> Inventory<'a> {
    pub(crate) fn new(
        store: Arc<ArtifactStore>,
        format: ObjectFormat,
        target: CellTarget,
        visitor: &'a mut dyn ArtifactVisitor,
    ) -> WalkResult<Self> {
        if crate::repository_target(target.tenant(), target.application(), store.repository())?
            != target
        {
            return Err(super::directory::index::IndexError::Integrity.into());
        }
        Ok(Self {
            indexes: Arc::new(CatalogIndexes::new(store.clone(), format)),
            store,
            format,
            visitor,
            target,
        })
    }
    pub(crate) fn target(&self) -> &CellTarget {
        &self.target
    }
    pub(crate) fn store(&self) -> Arc<ArtifactStore> {
        self.store.clone()
    }
    pub(crate) fn format(&self) -> ObjectFormat {
        self.format
    }
    pub(crate) async fn artifact(
        &mut self,
        key: ArtifactKey,
        descriptor: ArtifactDescriptor,
    ) -> WalkResult<bool> {
        self.visitor.artifact(key, descriptor).await
    }
    pub(crate) async fn input(
        &mut self,
        operation: [u8; 16],
        descriptor: ArtifactDescriptor,
    ) -> WalkResult<bool> {
        self.artifact(
            ArtifactKey {
                operation,
                binding_digest: descriptor.digest,
                kind: ArtifactKind::InputRoot,
            },
            descriptor,
        )
        .await
    }
    pub(crate) async fn body(
        &mut self,
        operation: [u8; 16],
        descriptor: ArtifactDescriptor,
    ) -> WalkResult<()> {
        self.artifact(
            ArtifactKey {
                operation,
                binding_digest: descriptor.digest,
                kind: ArtifactKind::InputBody,
            },
            descriptor,
        )
        .await?;
        Ok(())
    }
    pub(crate) async fn catalog(&mut self, stored: StoredCatalog) -> WalkResult<()> {
        if stored.format != self.format {
            return Err(super::directory::index::IndexError::Integrity.into());
        }
        let snapshot = CatalogSnapshot::download(&self.store, stored).await?;
        self.artifact(
            ArtifactKey {
                operation: stored.operation,
                binding_digest: stored.artifact.digest,
                kind: ArtifactKind::CatalogNode,
            },
            stored.artifact,
        )
        .await?;
        let directory = DirectorySnapshot::download(&self.store, snapshot.directory).await?;
        self.artifact(
            ArtifactKey {
                operation: snapshot.directory.operation,
                binding_digest: snapshot.directory.artifact.digest,
                kind: ArtifactKind::CatalogNode,
            },
            snapshot.directory.artifact,
        )
        .await?;
        let indexes = self.indexes.clone();
        for root in directory
            .level_zero
            .iter()
            .chain(directory.levels.iter().flatten())
        {
            indexes.ranges().visit(*root, self).await?;
        }
        if let Some(root) = snapshot.sources {
            indexes.sources().visit(root, self).await?;
        }
        Ok(())
    }
    /// Closed audit needs the selected metadata headers, without retaining a
    /// superseded catalog's physical object graph as permanent Git history.
    pub(crate) async fn catalog_headers(&mut self, stored: StoredCatalog) -> WalkResult<()> {
        if stored.format != self.format {
            return Err(super::directory::index::IndexError::Integrity.into());
        }
        let snapshot = CatalogSnapshot::download(&self.store, stored).await?;
        DirectorySnapshot::download(&self.store, snapshot.directory).await?;
        self.artifact(
            ArtifactKey {
                operation: stored.operation,
                binding_digest: stored.artifact.digest,
                kind: ArtifactKind::CatalogNode,
            },
            stored.artifact,
        )
        .await?;
        self.artifact(
            ArtifactKey {
                operation: snapshot.directory.operation,
                binding_digest: snapshot.directory.artifact.digest,
                kind: ArtifactKind::CatalogNode,
            },
            snapshot.directory.artifact,
        )
        .await?;
        Ok(())
    }
    pub(crate) async fn refs(&mut self, root: RefStateSnapshotRoot) -> WalkResult<()> {
        let snapshot = root.read(&self.store).await?;
        if snapshot.format != self.format {
            return Err(super::directory::index::IndexError::Integrity.into());
        }
        if !self.input(root.operation(), root.artifact()).await? {
            return Ok(());
        }
        if let Some(root) = snapshot.root {
            self.indexes.clone().refs().visit(root, self).await?;
        }
        Ok(())
    }
    pub(crate) async fn native_inputs(&mut self, root: NativeInputRoot) -> WalkResult<()> {
        self.indexes.clone().inputs().visit(root, self).await
    }
    async fn pack(&mut self, pack: NativePackDescriptor) -> WalkResult<()> {
        pack.validate(self.store.repository(), self.format)?;
        self.artifact(pack.key(ArtifactKind::Pack)?, pack.pack)
            .await?;
        self.artifact(pack.key(ArtifactKind::Index)?, pack.index)
            .await?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl ArtifactVisitor for Inventory<'_> {
    async fn artifact(
        &mut self,
        key: ArtifactKey,
        descriptor: ArtifactDescriptor,
    ) -> WalkResult<bool> {
        self.visitor.artifact(key, descriptor).await
    }
}
#[async_trait::async_trait]
impl IndexVisitor<StoredRun> for Inventory<'_> {
    async fn record(&mut self, record: &StoredRun) -> WalkResult<()> {
        self.artifact(
            ArtifactKey {
                operation: record.run.operation,
                binding_digest: record.artifact.digest,
                kind: ArtifactKind::DirectoryRun,
            },
            record.artifact,
        )
        .await?;
        Ok(())
    }
}
#[async_trait::async_trait]
impl IndexVisitor<SourceRecord> for Inventory<'_> {
    async fn record(&mut self, record: &SourceRecord) -> WalkResult<()> {
        self.artifact(record.metadata_key(), record.metadata.artifact)
            .await?;
        self.pack(record.native()).await
    }
}
#[async_trait::async_trait]
impl IndexVisitor<NativePackDescriptor> for Inventory<'_> {
    async fn record(&mut self, record: &NativePackDescriptor) -> WalkResult<()> {
        self.pack(*record).await
    }
}
#[async_trait::async_trait]
impl IndexVisitor<RefStateRecord> for Inventory<'_> {
    async fn record(&mut self, _: &RefStateRecord) -> WalkResult<()> {
        Ok(())
    }
}

pub(crate) fn decode<T: WireValue>(bytes: &[u8], limit: u32) -> WalkResult<T> {
    let mut decoder = BoundedDecoder::new(bytes, limit)?;
    let value = T::decode(&mut decoder)?;
    decoder.finish()?;
    Ok(value)
}
