//! Construct catalog changes from complete physical witnesses and the queried
//! certified base. Callers cannot supply directory/source roots or closure bits.
use super::*;
use crate::packs::{
    catalog::CatalogSnapshot,
    closure::{ClosureError, ClosureVerifier, RetainedClosure},
    directory::{DirectoryBuilder, StoredRun, index::IndexError, snapshot::DirectorySnapshot},
    metadata::{MetadataError, MetadataLimits, MetadataSegment},
    sources::{NativePackDescriptor, SourceIndex, SourceRecord, SourceRoot},
    verification::{PhysicalError, PhysicalPackWitness},
};
use canopy_object_storage::artifact::ArtifactStore;
use cellule_ltx::DiskBudget;
use std::{path::Path, sync::Arc};
use tokio::time::timeout_at;

#[derive(Debug, thiserror::Error)]
pub enum CatalogPreparationError {
    #[error("preparation base is inactive or inconsistent")]
    Base(#[from] PreparationBaseError),
    #[error("preparation closure failed")]
    Closure(#[from] ClosureError),
    #[error("preparation metadata failed")]
    Metadata(#[from] MetadataError),
    #[error("preparation physical input failed")]
    Physical(#[from] PhysicalError),
    #[error("preparation catalog failed")]
    Catalog(#[from] IndexError),
    #[error("preparation worker failed")]
    Task(#[from] tokio::task::JoinError),
    #[error("catalog preparation is incomplete, failed or inconsistent")]
    Integrity,
}

/// Private construction is conditional verification, not the commit point.
/// The final publisher must check the actual fence, attempt and base generation,
/// current policy/ref expectations and the complete durability gate.
pub struct PreparedCatalog {
    pub(super) base: Arc<PreparationBaseResolver>,
    catalog: StoredCatalog,
    object_count: u64,
    edge_count: u64,
    input_count: u64,
    inputs_digest: [u8; 32],
    inventory_digest: [u8; 32],
    incoming_run: Option<StoredRun>,
    incoming_sources: Option<SourceRoot>,
    closure: Arc<RetainedClosure>,
}
impl PreparedCatalog {
    pub fn token(&self) -> PreparationToken {
        self.base.context_token()
    }
    pub fn base(&self) -> GenerationFact {
        self.base.generation_fact()
    }
    pub fn catalog(&self) -> StoredCatalog {
        self.catalog
    }
    pub fn object_count(&self) -> u64 {
        self.object_count
    }
    pub fn edge_count(&self) -> u64 {
        self.edge_count
    }
    pub fn input_count(&self) -> u64 {
        self.input_count
    }
    pub fn inputs_digest(&self) -> [u8; 32] {
        self.inputs_digest
    }
    pub fn inventory_digest(&self) -> [u8; 32] {
        self.inventory_digest
    }
    pub fn ensure_live(&self) -> Result<(), PreparationBaseError> {
        self.base.live_lease().map(|_| ())
    }
    /// Reuse exact physical inputs and the verified incoming DAG. Only incoming
    /// overlaps/external anchors are read from the newly queried certified base;
    /// no full pack decode or historical graph scan runs again.
    pub async fn reconcile(&self) -> Result<Self, CatalogPreparationError> {
        let (_, deadline) = self.base.live_lease()?;
        timeout_at(deadline, async {
            let base = Arc::new(self.base.select_current().await?);
            let catalog = if base.generation_fact() == self.base() {
                self.catalog
            } else {
                self.closure.reconcile(base.context(), &*base).await?;
                let (mut directory, sources) = base.catalog_parts();
                if let Some(run) = self.incoming_run {
                    directory.append(run)?;
                }
                let indexes = base.indexes();
                let store = indexes.store();
                let sources = merge_sources(
                    &indexes.sources(),
                    sources,
                    self.incoming_sources,
                    base.context_token().artifact_operation,
                )
                .await?;
                CatalogSnapshot {
                    directory: directory
                        .upload(&store, base.context_token().artifact_operation)
                        .await?,
                    sources,
                }
                .upload(&store, base.context_token().artifact_operation)
                .await?
            };
            base.live_lease()?;
            Ok(Self {
                base,
                catalog,
                object_count: self.object_count,
                edge_count: self.edge_count,
                input_count: self.input_count,
                inputs_digest: self.inputs_digest,
                inventory_digest: self.inventory_digest,
                incoming_run: self.incoming_run,
                incoming_sources: self.incoming_sources,
                closure: Arc::clone(&self.closure),
            })
        })
        .await
        .map_err(|_| PreparationBaseError::Inactive)?
    }
}

async fn merge_sources(
    sources: &SourceIndex,
    mut base: Option<SourceRoot>,
    incoming: Option<SourceRoot>,
    operation: [u8; 16],
) -> Result<Option<SourceRoot>, IndexError> {
    // Both roots are private assembler outputs. Initial preparation already
    // validated every incoming source; avoid copying its tree into itself.
    if base.is_none() {
        return Ok(incoming);
    }
    if incoming.is_none() || base == incoming {
        return Ok(base);
    }
    let mut cursor = sources.cursor(incoming, None)?;
    while let Some(record) = cursor.next().await? {
        if record.native().operation != operation {
            return Err(IndexError::Integrity);
        }
        base = Some(sources.insert(base, operation, record).await?);
    }
    Ok(base)
}

/// One operation's disk-backed incoming inventory. Existing certified source
/// subtrees are reused; only exact verified incoming shards insert new leaves.
/// Failure/cancellation poisons the operation and never yields PreparedCatalog.
pub struct CatalogPreparation {
    base: Arc<PreparationBaseResolver>,
    store: Arc<ArtifactStore>,
    sources: Arc<SourceIndex>,
    source_root: Option<SourceRoot>,
    incoming_sources: Option<SourceRoot>,
    snapshot: DirectorySnapshot,
    directory: Option<DirectoryBuilder>,
    closure: Option<ClosureVerifier>,
    active: Option<NativePackDescriptor>,
    failed: bool,
}
impl CatalogPreparation {
    pub async fn new(
        root: &Path,
        budget: DiskBudget,
        base: Arc<PreparationBaseResolver>,
        limits: MetadataLimits,
    ) -> Result<Self, CatalogPreparationError> {
        let (lease, deadline) = base.live_lease()?;
        let indexes = base.indexes();
        let (snapshot, source_root) = base.catalog_parts();
        let root = root.to_owned();
        let work = async {
            let workspace = tokio::task::spawn_blocking(move || {
                tempfile::Builder::new()
                    .prefix("canopy-catalog-preparation-")
                    .tempdir_in(root)
                    .map(Arc::new)
                    .map_err(MetadataError::from)
            })
            .await??;
            let context = base.context();
            let closure = ClosureVerifier::new_in_workspace(
                Arc::clone(&workspace),
                budget.clone(),
                context,
                limits,
            )
            .await?;
            let directory = tokio::task::spawn_blocking(move || {
                let mut builder = DirectoryBuilder::new(
                    workspace.path(),
                    budget,
                    lease.token.repository,
                    lease.token.artifact_operation,
                    lease.format,
                    limits,
                )?;
                builder.retain_workspace(workspace);
                Ok::<_, MetadataError>(builder)
            })
            .await??;
            base.live_lease()?;
            Ok(Self {
                base,
                store: indexes.store(),
                sources: indexes.sources(),
                source_root,
                incoming_sources: None,
                snapshot,
                directory: Some(directory),
                closure: Some(closure),
                active: None,
                failed: false,
            })
        };
        timeout_at(deadline, work)
            .await
            .map_err(|_| PreparationBaseError::Inactive)?
    }
    fn start(&mut self) -> Result<tokio::time::Instant, CatalogPreparationError> {
        if self.failed {
            return Err(CatalogPreparationError::Integrity);
        }
        self.failed = true;
        Ok(self.base.live_lease()?.1)
    }
    pub fn begin_pack(
        &mut self,
        witness: PhysicalPackWitness,
    ) -> Result<(), CatalogPreparationError> {
        self.start()?;
        if self.active.is_some() {
            return Err(CatalogPreparationError::Integrity);
        }
        let native = witness.native();
        witness.verify_store(&self.store)?;
        self.closure
            .as_mut()
            .ok_or(CatalogPreparationError::Integrity)?
            .begin_pack(witness)?;
        self.active = Some(native);
        self.failed = false;
        Ok(())
    }
    pub async fn add_segment(
        &mut self,
        segment: Arc<MetadataSegment>,
    ) -> Result<(), CatalogPreparationError> {
        let deadline = self.start()?;
        timeout_at(deadline, self.add_inner(segment))
            .await
            .map_err(|_| PreparationBaseError::Inactive)??;
        self.base.live_lease()?;
        self.failed = false;
        Ok(())
    }
    async fn add_inner(
        &mut self,
        segment: Arc<MetadataSegment>,
    ) -> Result<(), CatalogPreparationError> {
        let native = self.active.ok_or(CatalogPreparationError::Integrity)?;
        self.closure
            .as_mut()
            .ok_or(CatalogPreparationError::Integrity)?
            .add_segment(Arc::clone(&segment))
            .await?;
        let directory = self
            .directory
            .take()
            .ok_or(CatalogPreparationError::Integrity)?;
        let pinned = Arc::clone(&segment);
        self.directory = Some(
            tokio::task::spawn_blocking(move || {
                let mut directory = directory;
                directory.add_segment(&pinned)?;
                Ok::<_, MetadataError>(directory)
            })
            .await??,
        );
        let metadata = segment.upload(&self.store).await?;
        let record = SourceRecord {
            metadata,
            pack: native.pack,
            index: native.index,
            pack_object_count: native.object_count,
        };
        record.validate(native.repository, native.format)?;
        if record.native() != native {
            return Err(CatalogPreparationError::Integrity);
        }
        self.incoming_sources = Some(
            self.sources
                .insert(self.incoming_sources, native.operation, record)
                .await?,
        );
        Ok(())
    }
    pub async fn finish_pack(&mut self) -> Result<(), CatalogPreparationError> {
        let deadline = self.start()?;
        if self.active.is_none() {
            return Err(CatalogPreparationError::Integrity);
        }
        timeout_at(
            deadline,
            self.closure
                .as_mut()
                .ok_or(CatalogPreparationError::Integrity)?
                .finish_pack(),
        )
        .await
        .map_err(|_| PreparationBaseError::Inactive)??;
        self.base.live_lease()?;
        self.active = None;
        self.failed = false;
        Ok(())
    }
    pub async fn finish(mut self) -> Result<PreparedCatalog, CatalogPreparationError> {
        let deadline = self.start()?;
        if self.active.is_some() {
            return Err(CatalogPreparationError::Integrity);
        }
        timeout_at(deadline, self.finish_inner())
            .await
            .map_err(|_| PreparationBaseError::Inactive)?
    }
    async fn finish_inner(mut self) -> Result<PreparedCatalog, CatalogPreparationError> {
        let context = self.base.context();
        let (witness, closure) = self
            .closure
            .take()
            .ok_or(CatalogPreparationError::Integrity)?
            .finish_retained(context.base.map(|_| &*self.base))
            .await?;
        let directory = self
            .directory
            .take()
            .ok_or(CatalogPreparationError::Integrity)?;
        let run = tokio::task::spawn_blocking(move || {
            if witness.object_count() == 0 {
                drop(directory);
                Ok((None, witness))
            } else {
                let run = directory.seal()?;
                witness.verify_run(run.descriptor())?;
                Ok::<_, CatalogPreparationError>((Some(Arc::new(run)), witness))
            }
        })
        .await??;
        let (run, witness) = run;
        let incoming_run = match run {
            Some(run) => {
                let stored = run.upload(&self.store).await?;
                self.snapshot.append(stored)?;
                Some(stored)
            }
            None => None,
        };
        self.source_root = merge_sources(
            &self.sources,
            self.source_root,
            self.incoming_sources,
            context.operation,
        )
        .await?;
        let snapshot = CatalogSnapshot {
            directory: self.snapshot.upload(&self.store, context.operation).await?,
            sources: self.source_root,
        };
        let catalog = snapshot.upload(&self.store, context.operation).await?;
        self.base.live_lease()?;
        Ok(PreparedCatalog {
            base: self.base,
            catalog,
            object_count: witness.object_count(),
            edge_count: witness.edge_count(),
            input_count: witness.input_count(),
            inputs_digest: witness.inputs_digest(),
            inventory_digest: witness.inventory_digest(),
            incoming_run,
            incoming_sources: self.incoming_sources,
            closure: Arc::new(closure),
        })
    }
}
