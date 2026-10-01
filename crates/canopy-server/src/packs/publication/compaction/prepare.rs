use super::*;
use crate::packs::directory::snapshot::RunLoader;

impl PreparedCompaction {
    /// Merge bounded selected ingress roots from the authoritative base. Caller
    /// indices select from that base; raw input/output descriptors are not accepted.
    pub async fn prepare(
        root: &Path,
        budget: DiskBudget,
        base: Arc<PreparationBaseResolver>,
        selected: &[usize],
        limits: CompactionLimits,
    ) -> Result<Self, CatalogPreparationError> {
        limits.validate()?;
        let (_, deadline) = base.live_lease()?;
        let (directory, _) = base.catalog_parts();
        directory.validate()?;
        if selected.len() < 2 || selected.len() > LEVEL_ZERO_ROOTS {
            return Err(MetadataError::Limit.into());
        }
        let mut inputs = Vec::with_capacity(selected.len());
        for &at in selected {
            let root = *directory
                .level_zero
                .get(at)
                .ok_or(CatalogPreparationError::Integrity)?;
            if inputs.contains(&root) {
                return Err(CatalogPreparationError::Integrity);
            }
            inputs.push(root);
        }
        // Canonical selection order makes replay/binding independent of caller
        // index order and never materializes the indexed run inventory.
        inputs.sort_by_key(|root| (root.operation, root.artifact.digest));
        let root = root.to_owned();
        timeout_at(deadline, async {
            let (client, target, check) = base.capability();
            let sql = cellule_runtime::primitives::sql::SqlCell::<RepositoryModule>::new(
                client.clone(),
                target.clone(),
            )
            .map_err(|_| PreparationBaseError::Context)?;
            let role = sql
                .query(
                    None,
                    SqlBatch {
                        statements: vec![access_statement(&check.actor)],
                    },
                )
                .await
                .map_err(|_| PreparationBaseError::Inactive)?;
            if !decode_access(&role.output)
                .map_err(|_| PreparationBaseError::Context)?
                .is_some_and(|role| role >= TokenScope::Admin)
            {
                return Err(PreparationBaseError::Inactive.into());
            }
            Self::prepare_inner(root, budget, base, inputs, limits).await
        })
        .await
        .map_err(|_| PreparationBaseError::Inactive)?
    }
    async fn prepare_inner(
        root: std::path::PathBuf,
        budget: DiskBudget,
        base: Arc<PreparationBaseResolver>,
        selected: Vec<NodeRef>,
        limits: CompactionLimits,
    ) -> Result<Self, CatalogPreparationError> {
        let context = base.context();
        let builder_budget = budget.clone();
        let mut builder = tokio::task::spawn_blocking(move || {
            let workspace = Arc::new(
                tempfile::Builder::new()
                    .prefix("canopy-compaction-")
                    .tempdir_in(root)?,
            );
            let mut builder = DirectoryBuilder::new(
                workspace.path(),
                builder_budget,
                context.repository,
                context.operation,
                context.format,
                limits.spool,
            )?;
            builder.retain_workspace(workspace);
            Ok::<_, MetadataError>(builder)
        })
        .await??;
        let mut binding = BoundedEncoder::new(16 << 10)?;
        binding.write_bytes(b"canopy.ingress-compaction.v1\0")?;
        binding.write_bytes(&context.repository)?;
        binding.write_u8(context.format.bytes() as u8)?;
        binding.write_count(selected.len())?;
        let indexes = base.indexes();
        let files = base.files();
        let mut input_count = 0_u64;
        let mut input_bytes = 0_u64;
        for &root in &selected {
            reference(&mut binding, root)?;
            let mut runs = 0_u64;
            let mut objects = 0_u64;
            let mut first = None;
            let mut last = None;
            let mut cursor = indexes.ranges().cursor(Some(root), None)?;
            while let Some(stored) = cursor.next().await? {
                runs = runs.checked_add(1).ok_or(MetadataError::Limit)?;
                objects = objects
                    .checked_add(stored.run.object_count)
                    .ok_or(MetadataError::Limit)?;
                first.get_or_insert(stored.run.first_oid);
                last = Some(stored.run.last_oid);
                input_count = input_count.checked_add(1).ok_or(MetadataError::Limit)?;
                input_bytes = input_bytes
                    .checked_add(stored.run.size)
                    .ok_or(MetadataError::Limit)?;
                if input_count > u64::from(limits.input_runs) || input_bytes > limits.input_bytes {
                    return Err(MetadataError::Limit.into());
                }
                let run = RunLoader::load(&*files, stored).await?;
                builder = tokio::task::spawn_blocking(move || {
                    if run.descriptor() != stored.run {
                        return Err(MetadataError::Integrity);
                    }
                    builder.add_run(&run)?;
                    Ok::<_, MetadataError>(builder)
                })
                .await??;
                base.live_lease()?;
            }
            // Summaries cover one disjoint run set, not the union of overlapping
            // roots. Per-file canonical folding and the merged spool establish
            // that union independently.
            if runs != root.record_count
                || objects != root.object_count
                || first != Some(root.first_key)
                || last != Some(root.last_key)
            {
                return Err(CatalogPreparationError::Integrity);
            }
        }
        let (mut partitioner, descriptor, edge_count) = tokio::task::spawn_blocking(move || {
            let (run, edges) = builder.seal_with_edges()?;
            let descriptor = run.descriptor();
            Ok::<_, MetadataError>((
                DirectoryPartitioner::new(Arc::new(run), budget, limits.output)?,
                descriptor,
                edges,
            ))
        })
        .await??;
        let store = indexes.store();
        let mut output = None;
        loop {
            let (next, retained) = tokio::task::spawn_blocking(move || {
                let next = partitioner.next_run()?;
                Ok::<_, MetadataError>((next, partitioner))
            })
            .await??;
            partitioner = retained;
            let Some(run) = next else {
                break;
            };
            output = Some(
                indexes
                    .ranges()
                    .insert(output, context.operation, run.upload(&store).await?)
                    .await?,
            );
            base.live_lease()?;
        }
        let output = output.ok_or(CatalogPreparationError::Integrity)?;
        if output.object_count != descriptor.object_count
            || output.first_key != descriptor.first_oid
            || output.last_key != descriptor.last_oid
        {
            return Err(CatalogPreparationError::Integrity);
        }
        let catalog = replacement(&base, &selected, output).await?;
        base.live_lease()?;
        Ok(Self {
            base,
            selected,
            output,
            catalog,
            object_count: descriptor.object_count,
            edge_count,
            input_count,
            inputs_digest: *blake3::hash(&binding.finish()).as_bytes(),
            inventory_digest: descriptor.inventory_digest,
        })
    }
}
