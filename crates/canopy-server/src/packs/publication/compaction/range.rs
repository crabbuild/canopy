use super::*;
use crate::{
    ObjectId,
    packs::directory::{
        StoredRun,
        index::{IndexError, RangeIndex, codec::write_run},
        snapshot::{DirectorySnapshot, MAX_LEVELS, RunLoader},
    },
};

/// Caller chooses a catalog position, never supplies authoritative descriptors.
/// Ingress is promoted to level 0; a level is promoted to its adjacent successor.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CompactionSource {
    Ingress(usize),
    Level(usize),
}
#[derive(Clone)]
enum Source {
    Ingress(NodeRef),
    Level(usize),
}
#[derive(Clone)]
pub(super) struct RangeSelection {
    source: Source,
    run: StoredRun,
    portion: StoredRun,
    remainder: Option<StoredRun>,
    target: usize,
    overlaps: Vec<StoredRun>,
}
impl PreparedCompaction {
    /// Move one source run plus every intersecting target run into the next
    /// level. If overlaps exceed the input budget, move a verified source prefix
    /// and retain its exact verified suffix in the same physical file. `after`
    /// is the exclusive last OID of a previously processed source portion; None starts at the first run. None output means the source is empty
    /// or exhausted. A source/target file exceeding the physical input budget
    /// rejects; selected target windows never truncate coverage silently.
    /// Unchanged disjoint files are verified and promoted without rewriting.
    pub async fn prepare_range(
        root: &Path,
        budget: DiskBudget,
        base: Arc<PreparationBaseResolver>,
        source: CompactionSource,
        after: Option<ObjectId>,
        limits: CompactionLimits,
    ) -> Result<Option<Self>, CatalogPreparationError> {
        limits.validate()?;
        let (_, deadline) = base.live_lease()?;
        let root = root.to_owned();
        timeout_at(deadline, async {
            prepare::check_admin(&base).await?;
            let (directory, _) = base.catalog_parts();
            directory.validate()?;
            let (source, source_root, target) = match source {
                CompactionSource::Ingress(at) => {
                    let selected = *directory.level_zero.get(at).ok_or(IndexError::Stale)?;
                    (Source::Ingress(selected), Some(selected), 0)
                }
                CompactionSource::Level(at) if at < MAX_LEVELS - 1 => (
                    Source::Level(at),
                    directory.levels.get(at).copied().flatten(),
                    at + 1,
                ),
                CompactionSource::Level(_) => return Err(IndexError::Limit.into()),
            };
            let indexes = base.indexes();
            let mut cursor = indexes.ranges().cursor(source_root, after)?;
            let Some(run) = cursor.next().await? else {
                return Ok(None);
            };
            // Include the source in both budgets before selecting targets. The
            // interval seek includes target ranges enclosing either endpoint.
            if run.run.size > limits.input_bytes {
                return Err(MetadataError::Limit.into());
            }
            let (overlaps, through) = select_window(
                indexes.ranges(),
                directory.levels.get(target).copied().flatten(),
                run,
                limits,
            )
            .await?;
            let file = base.files().load(run).await?;
            let (portion, remainder, portion_edges) = tokio::task::spawn_blocking(move || {
                if file.descriptor() != run.run {
                    return Err(MetadataError::Integrity);
                }
                let (left, right, edges) = file.split_coverage(run.coverage, through)?;
                let portion = StoredRun {
                    coverage: left.ok_or(MetadataError::Integrity)?,
                    ..run
                };
                let remainder = right.map(|coverage| StoredRun { coverage, ..run });
                Ok::<_, MetadataError>((portion, remainder, edges))
            })
            .await??;
            let selection = RangeSelection {
                source,
                run,
                portion,
                remainder,
                target,
                overlaps,
            };
            let input_count = selection.overlaps.len() as u64 + 1;
            let inputs_digest =
                selection.digest(base.context().repository, base.context().format)?;
            let (output, descriptor, edge_count) =
                if selection.overlaps.is_empty() && run.run.size <= limits.output.max_file_bytes {
                    let output = indexes
                        .ranges()
                        .insert(None, base.context().operation, portion)
                        .await?;
                    (output, portion.coverage, portion_edges)
                } else {
                    let mut builder =
                        prepare::new_builder(root, budget.clone(), &base, limits.spool).await?;
                    for input in std::iter::once(&portion).chain(selection.overlaps.iter()) {
                        builder = prepare::copy_run(builder, &base, *input).await?;
                        base.live_lease()?;
                    }
                    let (output, descriptor, edges) =
                        prepare::finish_output(builder, budget, &base, limits.output).await?;
                    (output, descriptor.coverage(), edges)
                };
            let selected = Selection::Range(Box::new(selection));
            let catalog = replacement(&base, &selected, output).await?;
            base.live_lease()?;
            Ok(Some(Self {
                base,
                selected,
                output,
                catalog,
                object_count: descriptor.object_count,
                edge_count,
                input_count,
                inputs_digest,
                inventory_digest: descriptor.inventory_digest,
            }))
        })
        .await
        .map_err(|_| PreparationBaseError::Inactive)?
    }
}
impl RangeSelection {
    fn digest(&self, repository: [u8; 16], format: ObjectFormat) -> Result<[u8; 32], CodecError> {
        let mut hash = blake3::Hasher::new();
        let mut e = BoundedEncoder::new(1024)?;
        e.write_bytes(b"canopy.range-compaction.v2\0")?;
        e.write_bytes(&repository)?;
        e.write_u8(format.bytes() as u8)?;
        match self.source {
            Source::Ingress(root) => {
                e.write_u8(0)?;
                reference(&mut e, root)?;
            }
            Source::Level(at) => {
                e.write_u8(1)?;
                e.write_u8(at as u8)?;
            }
        }
        e.write_u8(self.target as u8)?;
        e.write_count(self.overlaps.len())?;
        write_run(&mut e, self.run)?;
        hash.update(&e.finish());
        let mut e = BoundedEncoder::new(1024)?;
        e.write_bool(self.remainder.is_some())?;
        if let Some(remainder) = self.remainder {
            write_run(&mut e, remainder)?;
        }
        hash.update(&e.finish());
        for run in std::iter::once(&self.portion).chain(self.overlaps.iter()) {
            let mut e = BoundedEncoder::new(1024)?;
            write_run(&mut e, *run)?;
            hash.update(&e.finish());
        }
        Ok(*hash.finalize().as_bytes())
    }
    pub(super) async fn replace(
        &self,
        directory: &mut DirectorySnapshot,
        index: &RangeIndex,
        operation: [u8; 16],
        output: NodeRef,
    ) -> Result<(), IndexError> {
        directory.validate()?;
        let source_at = match self.source {
            Source::Ingress(root) => directory
                .level_zero
                .iter()
                .position(|candidate| *candidate == root)
                .ok_or(IndexError::Stale)?,
            Source::Level(at) => at,
        };
        let source_root = match self.source {
            Source::Ingress(root) => Some(root),
            Source::Level(at) => directory.levels.get(at).copied().flatten(),
        };
        if index.find(source_root, self.run.coverage.first_oid).await? != Some(self.run) {
            return Err(IndexError::Stale);
        }
        let first = self
            .overlaps
            .iter()
            .map(|run| run.coverage.first_oid)
            .chain(std::iter::once(self.portion.coverage.first_oid))
            .min()
            .ok_or(IndexError::Integrity)?;
        let last = self
            .overlaps
            .iter()
            .map(|run| run.coverage.last_oid)
            .chain(std::iter::once(self.portion.coverage.last_oid))
            .max()
            .ok_or(IndexError::Integrity)?;
        if output.first_key != first || output.last_key != last {
            return Err(IndexError::Integrity);
        }
        index.validate_root(output).await?;
        let target_root = directory.levels.get(self.target).copied().flatten();
        let current = index
            .overlapping(target_root, first, last, self.overlaps.len())
            .await;
        // Any newly inserted overlapping target, changed preferred placement or
        // missing selected target invalidates reuse. Out-of-range path updates
        // remain eligible; never rebuild a level from an old root.
        match current {
            Ok(runs) if runs == self.overlaps => {}
            Ok(_) | Err(IndexError::Limit) => return Err(IndexError::Stale),
            Err(error) => return Err(error),
        }
        let mut remaining = index.remove(source_root, operation, self.run).await?;
        if let Some(remainder) = self.remainder {
            remaining = Some(index.insert(remaining, operation, remainder).await?);
        }
        match self.source {
            Source::Ingress(_) => {
                if let Some(root) = remaining {
                    directory.level_zero[source_at] = root;
                } else {
                    directory.level_zero.remove(source_at);
                }
            }
            Source::Level(_) => directory.levels[source_at] = remaining,
        }
        let mut target = target_root;
        for run in &self.overlaps {
            target = index.remove(target, operation, *run).await?;
        }
        if target.is_none() {
            // The complete verified output tree already has the right context.
            // Reuse it rather than reuploading identical intermediate nodes.
            target = Some(output);
        } else {
            let mut cursor = index.cursor(Some(output), None)?;
            while let Some(run) = cursor.next().await? {
                target = Some(index.insert(target, operation, run).await?);
            }
        }
        if directory.levels.len() <= self.target {
            directory.levels.resize(self.target + 1, None);
        }
        directory.levels[self.target] = target;
        directory.validate()
    }
}

/// A bounded consecutive target prefix. A later target outside the returned
/// interval is never read/copied or removed by this operation.
async fn select_window(
    index: &RangeIndex,
    target: Option<NodeRef>,
    source: StoredRun,
    limits: CompactionLimits,
) -> Result<(Vec<StoredRun>, ObjectId), CatalogPreparationError> {
    let mut selected = Vec::new();
    let mut bytes = source.run.size;
    let mut physical = std::collections::BTreeMap::new();
    physical.insert(
        (source.run.operation, source.artifact.digest),
        (source.run, source.artifact),
    );
    let mut next = index.successor(target, source.coverage.first_oid).await?;
    let mut cursor = if let Some(first) = next {
        Some(index.cursor(target, Some(first.coverage.first_oid))?)
    } else {
        None
    };
    while let Some(run) = next {
        if run.coverage.first_oid > source.coverage.last_oid {
            break;
        }
        let key = (run.run.operation, run.artifact.digest);
        let extra = match physical.get(&key) {
            Some(descriptor) if *descriptor == (run.run, run.artifact) => 0,
            Some(_) => return Err(MetadataError::Integrity.into()),
            None => run.run.size,
        };
        let total = bytes.checked_add(extra).ok_or(MetadataError::Limit)?;
        if selected.len() + 1 >= limits.input_runs as usize || total > limits.input_bytes {
            let through = if let Some(last) = selected.last() {
                let last: &StoredRun = last;
                last.coverage.last_oid.min(source.coverage.last_oid)
            } else if run.coverage.first_oid > source.coverage.first_oid {
                predecessor(run.coverage.first_oid)?
            } else {
                return Err(MetadataError::Limit.into());
            };
            return Ok((selected, through));
        }
        bytes = total;
        physical.insert(key, (run.run, run.artifact));
        selected.push(run);
        if run.coverage.last_oid >= source.coverage.last_oid {
            break;
        }
        next = match &mut cursor {
            Some(cursor) => cursor.next().await?,
            None => None,
        };
    }
    Ok((selected, source.coverage.last_oid))
}
fn predecessor(oid: ObjectId) -> Result<ObjectId, MetadataError> {
    let mut bytes = oid.to_vec();
    for byte in bytes.iter_mut().rev() {
        if *byte != 0 {
            *byte -= 1;
            return bytes.try_into().map_err(|_| MetadataError::Integrity);
        }
        *byte = 255;
    }
    Err(MetadataError::Integrity)
}
