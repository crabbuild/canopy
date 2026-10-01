//! Bounded selection and canonical conflict checking across directory levels.
//! This snapshot is an input to catalog publication, not an authorization token.

use super::{
    index::{IndexError, NodeRef, RangeIndex},
    *,
};
use std::future::Future;

mod codec;
pub use codec::StoredSnapshot;

pub const LEVEL_ZERO_ROOTS: usize = 32;
pub const MAX_LEVELS: usize = 16;
pub const MAX_SELECTED_RUNS: usize = LEVEL_ZERO_ROOTS + MAX_LEVELS;

/// Every root indexes disjoint OID ranges. Different level-zero roots may
/// overlap, but a partitioned ingress batch consumes only one root slot.
/// No point lookup materializes every run descriptor in a root.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DirectorySnapshot {
    pub repository: [u8; 16],
    pub format: ObjectFormat,
    pub level_zero: Vec<NodeRef>,
    pub levels: Vec<Option<NodeRef>>,
}
pub trait RunLoader: Sync {
    fn load(
        &self,
        run: StoredRun,
    ) -> impl Future<Output = Result<Arc<DirectoryRun>, MetadataError>> + Send;
}
impl DirectorySnapshot {
    pub fn empty(repository: [u8; 16], format: ObjectFormat) -> Self {
        Self {
            repository,
            format,
            level_zero: Vec::new(),
            levels: Vec::new(),
        }
    }
    pub fn validate(&self) -> Result<(), IndexError> {
        if self.level_zero.len() > LEVEL_ZERO_ROOTS || self.levels.len() > MAX_LEVELS {
            return Err(IndexError::Limit);
        }
        for (at, root) in self.level_zero.iter().enumerate() {
            root.validate(self.format)?;
            if self.level_zero[..at].contains(root) {
                return Err(IndexError::Integrity);
            }
        }
        for root in self.levels.iter().flatten() {
            root.validate(self.format)?;
        }
        Ok(())
    }
    /// Bounded ingress. When full, preparation must wait/reject until admitted
    /// compaction publishes a replacement root; it cannot append extra roots.
    /// Authenticate the run-set root in this repository before accepting it.
    pub async fn append(&mut self, index: &RangeIndex, root: NodeRef) -> Result<(), IndexError> {
        self.validate()?;
        if index.repository() != self.repository || index.format() != self.format {
            return Err(IndexError::Integrity);
        }
        index.validate_root(root).await?;
        if self.level_zero.contains(&root) {
            return Ok(());
        }
        if self.level_zero.len() == LEVEL_ZERO_ROOTS {
            return Err(IndexError::Limit);
        }
        self.level_zero.push(root);
        Ok(())
    }
    pub async fn selected_runs(
        &self,
        index: &RangeIndex,
        oid: ObjectId,
    ) -> Result<Vec<StoredRun>, IndexError> {
        self.validate()?;
        if self.repository != index.repository() || self.format != index.format() {
            return Err(IndexError::Integrity);
        }
        if oid.format() != self.format {
            return Ok(Vec::new());
        }
        let mut selected = Vec::new();
        for root in self
            .level_zero
            .iter()
            .copied()
            .map(Some)
            .chain(self.levels.iter().copied())
        {
            if let Some(run) = index.find(root, oid).await?
                && !selected.contains(&run)
            {
                selected.push(run);
            }
        }
        debug_assert!(selected.len() <= MAX_SELECTED_RUNS);
        Ok(selected)
    }
    /// Every matching header must agree, even when its physical source has been
    /// superseded. A newer placement never conceals an OID/graph conflict.
    pub async fn lookup(
        &self,
        index: &RangeIndex,
        loader: &impl RunLoader,
        oid: ObjectId,
    ) -> Result<Option<DirectoryEntry>, IndexError> {
        Ok(self
            .lookup_batch(index, loader, &[oid])
            .await?
            .pop()
            .flatten())
    }
    /// Group only requested IDs by selected file. At most 512 IDs and 48 file
    /// candidates per ID are retained; no historical inventory is loaded.
    pub async fn lookup_batch(
        &self,
        index: &RangeIndex,
        loader: &impl RunLoader,
        ids: &[ObjectId],
    ) -> Result<Vec<Option<DirectoryEntry>>, IndexError> {
        if ids.len() > PAGE_OBJECTS {
            return Err(IndexError::Limit);
        }
        self.validate()?;
        if self.repository != index.repository() || self.format != index.format() {
            return Err(IndexError::Integrity);
        }
        let mut groups = std::collections::BTreeMap::<SegmentKey, (StoredRun, Vec<usize>)>::new();
        for (at, oid) in ids.iter().enumerate() {
            for stored in self.selected_runs(index, *oid).await? {
                let key = SegmentKey {
                    operation: stored.run.operation,
                    digest: stored.artifact.digest,
                };
                let group = groups.entry(key).or_insert_with(|| (stored, Vec::new()));
                if group.0 != stored {
                    return Err(IndexError::Integrity);
                }
                group.1.push(at);
            }
        }
        let mut chosen: Vec<Option<DirectoryEntry>> = vec![None; ids.len()];
        for (_, (stored, positions)) in groups {
            let run = loader.load(stored).await?;
            let requested: Vec<_> = positions.iter().map(|at| ids[*at]).collect();
            let candidates = tokio::task::spawn_blocking(move || {
                if run.descriptor() != stored.run {
                    return Err(MetadataError::Integrity);
                }
                run.find_batch(&requested)
            })
            .await
            .map_err(MetadataError::from)??;
            if candidates.len() != positions.len() {
                return Err(IndexError::Integrity);
            }
            for (at, candidate) in positions.into_iter().zip(candidates) {
                let Some(candidate) = candidate else {
                    continue;
                };
                if let Some(current) = chosen[at] {
                    if current.header != candidate.header {
                        return Err(MetadataError::IdentityConflict.into());
                    }
                    if candidate.location_version > current.location_version
                        || (candidate.location_version == current.location_version
                            && candidate.source < current.source)
                    {
                        chosen[at] = Some(candidate);
                    }
                } else {
                    chosen[at] = Some(candidate);
                }
            }
        }
        Ok(chosen)
    }
}
