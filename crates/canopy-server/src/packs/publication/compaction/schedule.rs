//! Bounded advisory selection. Catalog-derived preparation and publication
//! remain the authority; local rotation state grants no retention/delete rights.
use super::*;
use crate::{
    ObjectId,
    packs::directory::{
        index::IndexError,
        snapshot::{DirectorySnapshot, MAX_LEVELS},
    },
};

#[derive(Clone, Copy, Debug)]
pub struct CompactionPolicy {
    /// Logical object target for the first nonoverlapping level.
    pub base_objects: u64,
    pub level_ratio: u32,
    /// Begin urgent ingress dispatch at this many overlapping roots.
    pub ingress_high_water: usize,
    /// At most this many urgent jobs before an eligible higher-level job.
    pub urgent_burst: u8,
}
impl Default for CompactionPolicy {
    fn default() -> Self {
        Self {
            base_objects: 1 << 18,
            level_ratio: 4,
            ingress_high_water: 8,
            urgent_burst: 3,
        }
    }
}
impl CompactionPolicy {
    fn targets(self) -> Result<[u64; MAX_LEVELS], IndexError> {
        if self.base_objects == 0
            || self.base_objects > i64::MAX as u64
            || !(2..=16).contains(&self.level_ratio)
            || !(1..=LEVEL_ZERO_ROOTS).contains(&self.ingress_high_water)
            || !(1..=32).contains(&self.urgent_burst)
        {
            return Err(IndexError::Limit);
        }
        let mut result = [0; MAX_LEVELS];
        let mut next = self.base_objects;
        for target in &mut result {
            *target = next;
            next = next
                .saturating_mul(u64::from(self.level_ratio))
                .min(i64::MAX as u64);
        }
        Ok(result)
    }
    /// Logical counts are exact within a disjoint level, but may overlap counts
    /// in other levels. They are scheduling pressure, never a global inventory.
    pub fn pressure(self, directory: &DirectorySnapshot) -> Result<CompactionPressure, IndexError> {
        directory.validate()?;
        let targets = self.targets()?;
        let mut objects = [0; MAX_LEVELS];
        for (at, root) in directory.levels.iter().enumerate() {
            objects[at] = root.map_or(0, |root| root.object_count);
        }
        // The final level has no adjacent successor. Never silently select a
        // nonexistent level or claim that this pressure has been drained.
        if objects[MAX_LEVELS - 1] > targets[MAX_LEVELS - 1] {
            return Err(IndexError::Limit);
        }
        Ok(CompactionPressure {
            ingress_roots: directory.level_zero.len(),
            level_objects: objects,
            level_targets: targets,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CompactionPressure {
    pub ingress_roots: usize,
    pub level_objects: [u64; MAX_LEVELS],
    pub level_targets: [u64; MAX_LEVELS],
}

/// One repository's local advisory maintenance cursor. O(MAX_LEVELS) state;
/// restart resets rotation, without changing authority or dropping catalog work.
/// Selection progresses after successful private preparation, not a durable ACK.
/// The caller must retain/recover uncertain publication before releasing inputs.
pub struct CompactionPlanner {
    policy: CompactionPolicy,
    context: Option<([u8; 16], ObjectFormat)>,
    next_class: usize,
    next_ingress: usize,
    urgent_streak: u8,
    after: [Option<ObjectId>; MAX_LEVELS - 1],
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Choice {
    source: CompactionSource,
    class: usize,
    urgent_streak: u8,
}
impl CompactionPlanner {
    pub fn new(policy: CompactionPolicy) -> Result<Self, IndexError> {
        policy.targets()?;
        Ok(Self {
            policy,
            context: None,
            next_class: 0,
            next_ingress: 0,
            urgent_streak: 0,
            after: [None; MAX_LEVELS - 1],
        })
    }
    fn choose(&self, directory: &DirectorySnapshot) -> Result<Option<Choice>, IndexError> {
        let pressure = self.policy.pressure(directory)?;
        if self
            .context
            .is_some_and(|context| context != (directory.repository, directory.format))
        {
            return Err(IndexError::Integrity);
        }
        let level_ready = |at: usize| pressure.level_objects[at] > pressure.level_targets[at];
        let higher_ready = (0..MAX_LEVELS - 1).any(level_ready);
        let ingress = || CompactionSource::Ingress(self.next_ingress % pressure.ingress_roots);
        if pressure.ingress_roots >= self.policy.ingress_high_water
            && (!higher_ready || self.urgent_streak < self.policy.urgent_burst)
        {
            return Ok(Some(Choice {
                source: ingress(),
                class: 0,
                urgent_streak: self.urgent_streak.saturating_add(1),
            }));
        }
        // Class zero is ingress; classes 1..MAX_LEVELS are promotable levels.
        // After a full urgent burst, give one eligible level a turn even if
        // ingress occupies the current rotation position.
        for offset in 0..MAX_LEVELS {
            let class = (self.next_class + offset) % MAX_LEVELS;
            let source = if class == 0 {
                if pressure.ingress_roots == 0
                    || (higher_ready && self.urgent_streak >= self.policy.urgent_burst)
                {
                    continue;
                }
                ingress()
            } else if level_ready(class - 1) {
                CompactionSource::Level(class - 1)
            } else {
                continue;
            };
            return Ok(Some(Choice {
                source,
                class,
                urgent_streak: 0,
            }));
        }
        Ok(None)
    }
    fn advance(&mut self, directory: &DirectorySnapshot, choice: Choice, through: ObjectId) {
        self.context = Some((directory.repository, directory.format));
        if choice.urgent_streak == 0 {
            self.next_class = (choice.class + 1) % MAX_LEVELS;
        }
        self.urgent_streak = choice.urgent_streak;
        match choice.source {
            CompactionSource::Ingress(at) => self.next_ingress = at + 1,
            CompactionSource::Level(at) => self.after[at] = Some(through),
        }
    }
    /// Select geometric pressure from this queried base and prepare one verified
    /// bounded window. No history scan or raw caller-supplied run descriptor.
    /// Tail ingress is eligible even below the urgent high-water mark.
    pub async fn prepare_next(
        &mut self,
        root: &Path,
        budget: DiskBudget,
        base: Arc<PreparationBaseResolver>,
        limits: CompactionLimits,
    ) -> Result<Option<PreparedCompaction>, CatalogPreparationError> {
        limits.validate()?;
        let (_, deadline) = base.live_lease()?;
        timeout_at(deadline, async {
            let (directory, _) = base.catalog_parts();
            let Some(choice) = self.choose(&directory)? else {
                prepare::check_admin(&base).await?;
                return Ok(None);
            };
            let after = match choice.source {
                CompactionSource::Ingress(_) => None,
                CompactionSource::Level(at) => self.after[at],
            };
            let mut prepared = PreparedCompaction::prepare_range(
                root,
                budget.clone(),
                Arc::clone(&base),
                choice.source,
                after,
                limits,
            )
            .await?;
            if prepared.is_none() && after.is_some() {
                // Wrap only after indexed exhaustion, so newly inserted lower
                // ranges and retained prefixes cannot be skipped indefinitely.
                prepared = PreparedCompaction::prepare_range(
                    root,
                    budget,
                    base,
                    choice.source,
                    None,
                    limits,
                )
                .await?;
            }
            let prepared = prepared.ok_or(IndexError::Integrity)?;
            let Selection::Range(selection) = &prepared.selected else {
                return Err(IndexError::Integrity.into());
            };
            self.advance(&directory, choice, selection.last_moved());
            Ok(Some(prepared))
        })
        .await
        .map_err(|_| PreparationBaseError::Inactive)?
    }
}

#[cfg(test)]
mod tests;
