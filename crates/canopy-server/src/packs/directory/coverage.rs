use super::*;

/// A contiguous logical projection of an authenticated immutable run. Its fold
/// uses the existing canonical encoding and excludes physical placement, exactly
/// like a whole run. It is not independently an authorization/closure proof.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RunCoverage {
    pub object_count: u64,
    pub first_oid: ObjectId,
    pub last_oid: ObjectId,
    pub inventory_digest: [u8; 32],
}
impl RunDescriptor {
    pub fn coverage(self) -> RunCoverage {
        RunCoverage {
            object_count: self.object_count,
            first_oid: self.first_oid,
            last_oid: self.last_oid,
            inventory_digest: self.inventory_digest,
        }
    }
}
impl RunCoverage {
    pub fn validate(self, run: RunDescriptor) -> Result<(), MetadataError> {
        if self.object_count == 0
            || self.object_count > run.object_count
            || self.first_oid.format() != run.format
            || self.last_oid.format() != run.format
            || self.first_oid < run.first_oid
            || self.last_oid > run.last_oid
            || self.first_oid > self.last_oid
            || (self.object_count == 1) != (self.first_oid == self.last_oid)
            || ((self.object_count == run.object_count
                || (self.first_oid == run.first_oid && self.last_oid == run.last_oid))
                && self != run.coverage())
        {
            return Err(MetadataError::Integrity);
        }
        Ok(())
    }
}
struct Fold {
    count: u64,
    edges: u64,
    inventory: [u8; 32],
    first: Option<ObjectId>,
    last: Option<ObjectId>,
}
impl Fold {
    fn new(format: ObjectFormat) -> Self {
        Self {
            count: 0,
            edges: 0,
            inventory: inventory_seed(format),
            first: None,
            last: None,
        }
    }
    fn add(&mut self, entry: DirectoryEntry) -> Result<(), MetadataError> {
        let oid = entry.header.object.oid;
        if self.last.is_some_and(|last| last >= oid) {
            return Err(MetadataError::Integrity);
        }
        self.inventory = fold_header(self.inventory, self.count, entry.header);
        self.count = self.count.checked_add(1).ok_or(MetadataError::Limit)?;
        self.edges = self
            .edges
            .checked_add(entry.header.edge_count)
            .ok_or(MetadataError::Limit)?;
        self.first.get_or_insert(oid);
        self.last = Some(oid);
        Ok(())
    }
    fn coverage(&self) -> Option<RunCoverage> {
        Some(RunCoverage {
            object_count: self.count,
            first_oid: self.first?,
            last_oid: self.last?,
            inventory_digest: self.inventory,
        })
    }
}
impl DirectoryRun {
    fn entries_in_coverage(
        &self,
        coverage: RunCoverage,
        after: Option<ObjectId>,
    ) -> Result<Vec<DirectoryEntry>, MetadataError> {
        let connection = self.connection()?;
        let mut statement = connection.prepare_cached("SELECT oid,kind,size,digest,edge_count,edge_digest,source_operation,source_digest,location_version FROM objects WHERE oid>=?1 AND oid<=?2 AND oid>?3 ORDER BY oid LIMIT ?4")?;
        Ok(statement
            .query_map(
                params![
                    coverage.first_oid.as_ref(),
                    coverage.last_oid.as_ref(),
                    after.as_ref().map_or(&[][..], AsRef::<[u8]>::as_ref),
                    PAGE_OBJECTS as i64
                ],
                entry,
            )?
            .collect::<rusqlite::Result<_>>()?)
    }
    #[cfg(test)]
    pub(in crate::packs) fn verify_coverage(
        &self,
        coverage: RunCoverage,
    ) -> Result<u64, MetadataError> {
        self.copy_coverage(coverage, |_| Ok(()))
    }
    pub(super) fn copy_coverage(
        &self,
        coverage: RunCoverage,
        mut consume: impl FnMut(&[DirectoryEntry]) -> Result<(), MetadataError>,
    ) -> Result<u64, MetadataError> {
        coverage.validate(self.descriptor)?;
        let mut fold = Fold::new(self.descriptor.format);
        loop {
            let page = self.entries_in_coverage(coverage, fold.last)?;
            if page.is_empty() {
                break;
            }
            for value in &page {
                fold.add(*value)?;
            }
            consume(&page)?;
        }
        if fold.coverage() != Some(coverage) {
            return Err(MetadataError::Integrity);
        }
        Ok(fold.edges)
    }
    /// Verify the complete input projection while folding both resulting parts
    /// in that same bounded pass. Empty parts are absent. Neither part escapes
    /// until exact parent inventory/count/endpoints have matched.
    pub(in crate::packs) fn split_coverage(
        &self,
        coverage: RunCoverage,
        through: ObjectId,
    ) -> Result<(Option<RunCoverage>, Option<RunCoverage>, u64), MetadataError> {
        coverage.validate(self.descriptor)?;
        if through.format() != self.descriptor.format {
            return Err(MetadataError::Integrity);
        }
        let mut parent = Fold::new(self.descriptor.format);
        let mut left = Fold::new(self.descriptor.format);
        let mut right = Fold::new(self.descriptor.format);
        loop {
            let page = self.entries_in_coverage(coverage, parent.last)?;
            if page.is_empty() {
                break;
            }
            for value in page {
                parent.add(value)?;
                if value.header.object.oid <= through {
                    left.add(value)?;
                } else {
                    right.add(value)?;
                }
            }
        }
        if parent.coverage() != Some(coverage) {
            return Err(MetadataError::Integrity);
        }
        Ok((left.coverage(), right.coverage(), left.edges))
    }
}
