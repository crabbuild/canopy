//! Construct catalog changes from complete physical witnesses and the queried
//! certified base. Callers cannot supply directory/source roots or closure bits.
use super::*;
use crate::packs::{
    catalog::CatalogSnapshot,
    closure::{ClosureError, ClosureVerifier, RetainedClosure},
    directory::{
        DirectoryBuilder, DirectoryPartitioner, RUN_TARGET_BYTES,
        index::{IndexError, NodeRef},
        snapshot::DirectorySnapshot,
    },
    metadata::{MetadataError, MetadataLimits, MetadataSegment, StoredSegment},
    sources::{NativePackDescriptor, SourceIndex, SourceRecord, SourceRoot},
    verification::{PhysicalError, PhysicalPackWitness, StagedNativeMetadata},
};
use canopy_object_storage::artifact::ArtifactStore;
use cellule_ltx::DiskBudget;
use std::{path::Path, sync::Arc};
use tokio::time::timeout_at;

#[derive(Debug, thiserror::Error)]
pub enum CatalogPreparationError {
    #[error("preparation encoding failed")]
    Codec(#[from] CodecError),
    #[error("preparation base is inactive or inconsistent")]
    Base(#[from] PreparationBaseError),
    #[error("preparation closure failed")]
    Closure(#[from] ClosureError),
    #[error("preparation input custody failed")]
    Inputs(#[from] InputCheckpointError),
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
    incoming_root: Option<NodeRef>,
    incoming_sources: Option<SourceRoot>,
    closure: Arc<RetainedClosure>,
    pub(super) input_checkpoint_digest: Option<[u8; 32]>,
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
                let indexes = base.indexes();
                if let Some(root) = self.incoming_root {
                    directory.append(indexes.ranges(), root).await?;
                }
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
                incoming_root: self.incoming_root,
                incoming_sources: self.incoming_sources,
                closure: Arc::clone(&self.closure),
                input_checkpoint_digest: self.input_checkpoint_digest,
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
        base = Some(sources.insert(base, operation, record).await?);
    }
    Ok(base)
}

/// One operation's disk-backed incoming inventory. Existing certified source
/// subtrees are reused; only exact verified incoming shards insert new leaves.
/// Failure/cancellation poisons the operation and never yields PreparedCatalog.
pub struct CatalogPreparation {
    staging: Option<StagingContext>,
    base: Arc<PreparationBaseResolver>,
    store: Arc<ArtifactStore>,
    sources: Arc<SourceIndex>,
    source_root: Option<SourceRoot>,
    incoming_sources: Option<SourceRoot>,
    snapshot: DirectorySnapshot,
    directory: Option<DirectoryBuilder>,
    budget: DiskBudget,
    input_limits: MetadataLimits,
    output_limits: MetadataLimits,
    closure: Option<ClosureVerifier>,
    active: Option<NativePackDescriptor>,
    input_checkpoint_digest: Option<[u8; 32]>,
    failed: bool,
}
impl CatalogPreparation {
    pub async fn new(
        root: &Path,
        budget: DiskBudget,
        base: Arc<PreparationBaseResolver>,
        limits: MetadataLimits,
    ) -> Result<Self, CatalogPreparationError> {
        Self::new_with_run_limits(
            root,
            budget,
            base,
            limits,
            MetadataLimits {
                max_file_bytes: limits.max_file_bytes.min(RUN_TARGET_BYTES),
                ..limits
            },
        )
        .await
    }
    /// Separate the incoming verification spool class from bounded immutable
    /// output runs. Larger spool admission never raises the point-lookup bound.
    pub async fn new_with_run_limits(
        root: &Path,
        budget: DiskBudget,
        base: Arc<PreparationBaseResolver>,
        limits: MetadataLimits,
        output_limits: MetadataLimits,
    ) -> Result<Self, CatalogPreparationError> {
        Self::new_inner(root, budget, base, limits, output_limits, None).await
    }
    /// Production construction retains the admitted worker through every
    /// detached assembler job, while the finished private proof owns no worker.
    pub async fn new_staged(
        context: &StagingContext,
        root: &Path,
        budget: DiskBudget,
        base: Arc<PreparationBaseResolver>,
        limits: MetadataLimits,
    ) -> Result<Self, CatalogPreparationError> {
        context.ensure_live().map_err(PhysicalError::from)?;
        if context.token().map_err(PhysicalError::from)? != base.context_token()
            || context.format() != base.context().format
        {
            return Err(CatalogPreparationError::Integrity);
        }
        Self::new_inner(
            root,
            budget,
            base,
            limits,
            MetadataLimits {
                max_file_bytes: limits.max_file_bytes.min(RUN_TARGET_BYTES),
                ..limits
            },
            Some(context.clone()),
        )
        .await
    }
    async fn new_inner(
        root: &Path,
        budget: DiskBudget,
        base: Arc<PreparationBaseResolver>,
        limits: MetadataLimits,
        output_limits: MetadataLimits,
        staging: Option<StagingContext>,
    ) -> Result<Self, CatalogPreparationError> {
        DirectoryPartitioner::validate_limits(output_limits)?;
        let (lease, deadline) = base.live_lease()?;
        let indexes = base.indexes();
        let (snapshot, source_root) = base.catalog_parts();
        let root = root.to_owned();
        let activity = staging.as_ref().map_or_else(
            || Arc::new(()) as crate::git_objects::ReadOwner,
            StagingContext::physical_owner,
        );
        let work = async {
            let workspace_activity = activity.clone();
            let workspace = tokio::task::spawn_blocking(move || {
                let _activity = workspace_activity;
                tempfile::Builder::new()
                    .prefix("canopy-catalog-preparation-")
                    .tempdir_in(root)
                    .map(Arc::new)
                    .map_err(MetadataError::from)
            })
            .await??;
            let context = base.context();
            let closure = ClosureVerifier::new_in_workspace_owned(
                Arc::clone(&workspace),
                budget.clone(),
                context,
                limits,
                activity.clone(),
            )
            .await?;
            let directory_budget = budget.clone();
            let directory = tokio::task::spawn_blocking(move || {
                let _activity = activity;
                let mut builder = DirectoryBuilder::new(
                    workspace.path(),
                    directory_budget,
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
                staging,
                base,
                store: indexes.store(),
                sources: indexes.sources(),
                source_root,
                incoming_sources: None,
                snapshot,
                directory: Some(directory),
                budget,
                input_limits: limits,
                output_limits,
                closure: Some(closure),
                active: None,
                input_checkpoint_digest: None,
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
        if let Some(context) = &self.staging {
            context.ensure_live().map_err(PhysicalError::from)?;
        }
        Ok(self.base.live_lease()?.1)
    }
    fn physical_owner(&self) -> crate::git_objects::ReadOwner {
        self.staging.as_ref().map_or_else(
            || Arc::new(()) as crate::git_objects::ReadOwner,
            StagingContext::physical_owner,
        )
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
    /// Accept a complete physical witness from the exact authenticated input
    /// checkpoint retained by this attempt. Raw descriptors cannot bypass the
    /// public begin_pack namespace guard or construct the private custody proof.
    pub async fn begin_retained_pack(
        &mut self,
        witness: PhysicalPackWitness,
    ) -> Result<(), CatalogPreparationError> {
        let deadline = self.start()?;
        if self.active.is_some() {
            return Err(CatalogPreparationError::Integrity);
        }
        let native = witness.native();
        witness.verify_store(&self.store)?;
        let custody = timeout_at(
            deadline,
            inputs::RetainedNativeInput::open(&self.base, native),
        )
        .await
        .map_err(|_| PreparationBaseError::Inactive)??;
        let digest = custody.digest();
        if self
            .input_checkpoint_digest
            .is_some_and(|old| old != digest)
        {
            return Err(CatalogPreparationError::Integrity);
        }
        self.closure
            .as_mut()
            .ok_or(CatalogPreparationError::Integrity)?
            .begin_retained_pack(witness, custody)?;
        self.input_checkpoint_digest = Some(digest);
        self.active = Some(native);
        self.failed = false;
        Ok(())
    }
    pub async fn add_segment(
        &mut self,
        segment: Arc<MetadataSegment>,
    ) -> Result<(), CatalogPreparationError> {
        let deadline = self.start()?;
        timeout_at(deadline, self.add_inner(segment, None))
            .await
            .map_err(|_| PreparationBaseError::Inactive)??;
        self.base.live_lease()?;
        self.failed = false;
        Ok(())
    }
    async fn add_inner(
        &mut self,
        segment: Arc<MetadataSegment>,
        stored: Option<StoredSegment>,
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
        let activity = self.physical_owner();
        self.directory = Some(
            tokio::task::spawn_blocking(move || {
                let _activity = activity;
                let mut directory = directory;
                directory.add_segment(&pinned)?;
                Ok::<_, MetadataError>(directory)
            })
            .await??,
        );
        let metadata = match stored {
            Some(stored) if stored.segment == segment.descriptor() => stored,
            Some(_) => return Err(CatalogPreparationError::Integrity),
            None => {
                segment
                    .upload_owned(&self.store, self.physical_owner())
                    .await?
            }
        };
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
                .insert(
                    self.incoming_sources,
                    self.base.context_token().artifact_operation,
                    record,
                )
                .await?,
        );
        Ok(())
    }
    /// Consume the complete physical witness and its admitted ordinal replay.
    /// Download/authenticate/copy one shard at a time; reuse each uploaded
    /// SourceRecord instead of uploading metadata again during bound assembly.
    pub async fn add_staged_pack(
        &mut self,
        input: StagedNativeMetadata,
    ) -> Result<(), CatalogPreparationError> {
        let deadline = self.start()?;
        let context = self
            .staging
            .clone()
            .ok_or(CatalogPreparationError::Integrity)?;
        // The complete operation is poisoned on cancellation, including a
        // failed/absent provider artifact or an incomplete descriptor replay.
        let result = timeout_at(deadline, async {
            let native = input.witness.native();
            self.failed = false;
            self.begin_retained_pack(input.witness).await?;
            self.failed = true;
            let workspace = self
                .directory
                .as_ref()
                .ok_or(CatalogPreparationError::Integrity)?
                .workspace();
            let root = workspace
                .as_ref()
                .ok_or(CatalogPreparationError::Integrity)?
                .path();
            let mut offset = 0;
            while let Some((stored, end)) = input.replay.next(&context, native, offset).await? {
                context.ensure_live().map_err(PhysicalError::from)?;
                let segment = MetadataSegment::download_owned(
                    root,
                    self.budget.clone(),
                    &self.store,
                    stored,
                    self.input_limits,
                    None,
                    context.physical_owner(),
                )
                .await?;
                self.add_inner(segment, Some(stored)).await?;
                offset = end;
            }
            context.ensure_live().map_err(PhysicalError::from)?;
            self.failed = false;
            self.finish_pack().await
        })
        .await
        .map_err(|_| PreparationBaseError::Inactive)?;
        if result.is_err() {
            self.failed = true;
        }
        result
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
        let budget = self.budget.clone();
        let limits = self.output_limits;
        let activity = self.physical_owner();
        let (partitioner, witness) = tokio::task::spawn_blocking(move || {
            let _activity = activity;
            if witness.object_count() == 0 {
                drop(directory);
                Ok((None, witness))
            } else {
                let run = directory.seal()?;
                witness.verify_run(run.descriptor())?;
                let partitioner = DirectoryPartitioner::new(Arc::new(run), budget, limits)?;
                Ok::<_, CatalogPreparationError>((Some(partitioner), witness))
            }
        })
        .await??;
        let indexes = self.base.indexes();
        let index = indexes.ranges();
        let mut incoming_root = None;
        if let Some(mut partitioner) = partitioner {
            loop {
                let activity = self.physical_owner();
                let (next, retained) = tokio::task::spawn_blocking(move || {
                    let _activity = activity;
                    let next = partitioner.next_run()?;
                    Ok::<_, MetadataError>((next, partitioner))
                })
                .await??;
                partitioner = retained;
                let Some(run) = next else {
                    break;
                };
                let stored = run.upload_owned(&self.store, self.physical_owner()).await?;
                incoming_root = Some(
                    index
                        .insert(incoming_root, context.operation, stored)
                        .await?,
                );
            }
            // Exhaustion checked the exact canonical inventory before any root
            // can escape this private preparation. Earlier uploads grant no authority.
        }
        match (incoming_root, witness.object_count()) {
            (Some(root), count) if count > 0 && root.object_count == count => {
                self.snapshot.append(index, root).await?;
            }
            (None, 0) => {}
            _ => return Err(CatalogPreparationError::Integrity),
        }
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
            incoming_root,
            incoming_sources: self.incoming_sources,
            closure: Arc::new(closure),
            input_checkpoint_digest: self.input_checkpoint_digest,
        })
    }
}
