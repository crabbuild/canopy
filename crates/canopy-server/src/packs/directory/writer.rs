use super::*;

/// Disk-backed canonical merge. Sources are sealed shards/runs, not arbitrary
/// user headers. A failed source copy poisons the builder until it is dropped.
pub struct DirectoryBuilder {
    connection: Connection,
    admitted: AdmittedFile,
    repository: [u8; 16],
    operation: [u8; 16],
    format: ObjectFormat,
    limits: MetadataLimits,
    failed: bool,
}
impl DirectoryBuilder {
    pub(in crate::packs) fn retain_workspace(&mut self, workspace: Arc<tempfile::TempDir>) {
        self.admitted.retain_workspace(workspace);
    }
    pub fn new(
        root: &Path,
        budget: DiskBudget,
        repository: [u8; 16],
        operation: [u8; 16],
        format: ObjectFormat,
        limits: MetadataLimits,
    ) -> Result<Self, MetadataError> {
        if limits.cache_kib == 0
            || limits.cache_kib > i32::MAX as u32
            || limits.max_file_bytes < 16 << 10
            || limits.max_file_bytes > canopy_object_storage::external::MAX_ARTIFACT_BYTES
            || !limits.max_file_bytes.is_multiple_of(4096)
        {
            return Err(MetadataError::Limit);
        }
        let reservation = budget.try_reserve(
            limits
                .max_file_bytes
                .checked_mul(3)
                .ok_or(MetadataError::Limit)?,
        )?;
        let file = tempfile::Builder::new()
            .prefix("canopy-directory-")
            .tempfile_in(root)?;
        let admitted = AdmittedFile::new(file, reservation);
        let connection = Connection::open(admitted.file().path())?;
        connection.execute_batch("PRAGMA page_size=4096; PRAGMA journal_mode=DELETE; PRAGMA synchronous=FULL; PRAGMA foreign_keys=ON; PRAGMA trusted_schema=OFF; PRAGMA mmap_size=0;")?;
        connection.pragma_update(None, "cache_size", -(limits.cache_kib as i64))?;
        connection.pragma_update(None, "max_page_count", limits.max_file_bytes / 4096)?;
        connection.execute_batch(SCHEMA)?;
        Ok(Self {
            connection,
            admitted,
            repository,
            operation,
            format,
            limits,
            failed: false,
        })
    }
    pub fn add_segment(&mut self, segment: &MetadataSegment) -> Result<(), MetadataError> {
        let descriptor = segment.descriptor();
        if self.failed
            || descriptor.identity.repository != self.repository
            || descriptor.identity.format != self.format
        {
            return Err(MetadataError::Integrity);
        }
        self.failed = true;
        let source = SegmentKey {
            operation: descriptor.identity.operation,
            digest: descriptor.digest,
        };
        let mut after = None;
        let mut copied = 0_u64;
        loop {
            let headers = segment.headers_after(after)?;
            if headers.is_empty() {
                break;
            }
            copied = copied
                .checked_add(headers.len() as u64)
                .ok_or(MetadataError::Limit)?;
            after = headers.last().map(|header| header.object.oid);
            self.put_entries(
                &headers
                    .into_iter()
                    .map(|header| DirectoryEntry {
                        header,
                        source,
                        location_version: 1,
                    })
                    .collect::<Vec<_>>(),
            )?;
        }
        if copied != u64::from(descriptor.identity.object_count) {
            return Err(MetadataError::Integrity);
        }
        self.failed = false;
        Ok(())
    }
    /// Copy a certified run during directory compaction. Duplicate identities
    /// preserve their full canonical/graph inventory and choose a stable source.
    pub fn add_run(&mut self, run: &DirectoryRun) -> Result<(), MetadataError> {
        let descriptor = run.descriptor();
        if self.failed
            || descriptor.repository != self.repository
            || descriptor.format != self.format
        {
            return Err(MetadataError::Integrity);
        }
        self.failed = true;
        let mut after = None;
        let mut copied = 0_u64;
        loop {
            let entries = run.entries_after(after)?;
            if entries.is_empty() {
                break;
            }
            copied = copied
                .checked_add(entries.len() as u64)
                .ok_or(MetadataError::Limit)?;
            after = entries.last().map(|entry| entry.header.object.oid);
            self.put_entries(&entries)?;
        }
        if copied != descriptor.object_count {
            return Err(MetadataError::Integrity);
        }
        self.failed = false;
        Ok(())
    }
    /// Switch a bounded batch in a private compaction spool. The caller copies
    /// certified input ranges first. Publication still CASes that input root;
    /// this local CAS cannot substitute for the admitted Cell owner fence.
    pub fn relocate(
        &mut self,
        replacement: &MetadataSegment,
        expected: &[DirectoryEntry],
    ) -> Result<(), MetadataError> {
        let descriptor = replacement.descriptor();
        if self.failed
            || descriptor.identity.repository != self.repository
            || descriptor.identity.format != self.format
        {
            return Err(MetadataError::Integrity);
        }
        if expected.is_empty() || expected.len() > PAGE_OBJECTS {
            return Err(MetadataError::Limit);
        }
        let source = SegmentKey {
            operation: descriptor.identity.operation,
            digest: descriptor.digest,
        };
        let mut versions = Vec::with_capacity(expected.len());
        for entry in expected {
            if entry.header.object.oid.format() != self.format || entry.location_version == 0 {
                return Err(MetadataError::Integrity);
            }
            if entry.source == source {
                return Err(MetadataError::PlacementConflict);
            }
            if replacement.header(entry.header.object.oid)? != Some(entry.header) {
                return Err(MetadataError::IdentityConflict);
            }
            versions.push(
                entry
                    .location_version
                    .checked_add(1)
                    .filter(|version| *version <= i64::MAX as u64)
                    .ok_or(MetadataError::Limit)?,
            );
        }
        let transaction = self.connection.transaction()?;
        {
            let mut existing = transaction.prepare_cached("SELECT oid,kind,size,digest,edge_count,edge_digest,source_operation,source_digest,location_version FROM objects WHERE oid=?1")?;
            let mut update = transaction.prepare_cached("UPDATE objects SET source_operation=?2,source_digest=?3,location_version=?4 WHERE oid=?1")?;
            for (entry, version) in expected.iter().zip(versions) {
                let current = existing
                    .query_row([entry.header.object.oid.as_ref()], super::entry)
                    .optional()?
                    .ok_or(MetadataError::PlacementConflict)?;
                if current.header != entry.header {
                    return Err(MetadataError::IdentityConflict);
                }
                if current != *entry {
                    return Err(MetadataError::PlacementConflict);
                }
                update.execute(params![
                    entry.header.object.oid.as_ref(),
                    source.operation.as_slice(),
                    source.digest.as_slice(),
                    version as i64
                ])?;
            }
        }
        transaction.commit()?;
        Ok(())
    }
    pub(super) fn put_entries(&mut self, entries: &[DirectoryEntry]) -> Result<(), MetadataError> {
        if entries.is_empty() || entries.len() > PAGE_OBJECTS {
            return Err(MetadataError::Limit);
        }
        if entries.iter().any(|entry| {
            entry.header.object.oid.format() != self.format
                || entry.header.object.oid.is_zero()
                || entry.header.object.size > i64::MAX as u64
                || entry.header.edge_count > i64::MAX as u64
                || entry.location_version == 0
                || entry.location_version > i64::MAX as u64
        }) {
            return Err(MetadataError::Integrity);
        }
        let transaction = self.connection.transaction()?;
        {
            let mut insert = transaction.prepare_cached(
                "INSERT INTO objects VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9) ON CONFLICT DO NOTHING",
            )?;
            let mut existing = transaction.prepare_cached("SELECT oid,kind,size,digest,edge_count,edge_digest,source_operation,source_digest,location_version FROM objects WHERE oid=?1")?;
            let mut relocate = transaction.prepare_cached(
                "UPDATE objects SET source_operation=?2,source_digest=?3,location_version=?4 WHERE oid=?1",
            )?;
            for entry in entries {
                let header = entry.header;
                insert.execute(params![
                    header.object.oid.as_ref(),
                    header.object.kind.git_name(),
                    header.object.size as i64,
                    header.object.digest.as_slice(),
                    header.edge_count as i64,
                    header.edge_digest.as_slice(),
                    entry.source.operation.as_slice(),
                    entry.source.digest.as_slice(),
                    entry.location_version as i64
                ])?;
                let stored = existing.query_row([header.object.oid.as_ref()], super::entry)?;
                if stored.header != header {
                    return Err(MetadataError::IdentityConflict);
                }
                if entry.location_version > stored.location_version
                    || (entry.location_version == stored.location_version
                        && entry.source < stored.source)
                {
                    relocate.execute(params![
                        header.object.oid.as_ref(),
                        entry.source.operation.as_slice(),
                        entry.source.digest.as_slice(),
                        entry.location_version as i64
                    ])?;
                }
            }
        }
        transaction.commit()?;
        Ok(())
    }
    pub fn seal(mut self) -> Result<DirectoryRun, MetadataError> {
        if self.failed {
            return Err(MetadataError::Integrity);
        }
        let mut inventory = inventory_seed(self.format);
        let mut after = Vec::new();
        let mut count = 0_u64;
        let mut first = None;
        let mut last = None;
        loop {
            let entries = {
                let mut statement = self.connection.prepare_cached("SELECT oid,kind,size,digest,edge_count,edge_digest,source_operation,source_digest,location_version FROM objects WHERE oid>?1 ORDER BY oid LIMIT ?2")?;
                statement
                    .query_map(params![after, PAGE_OBJECTS as i64], entry)?
                    .collect::<rusqlite::Result<Vec<_>>>()?
            };
            if entries.is_empty() {
                break;
            }
            for entry in entries {
                inventory = fold_header(inventory, count, entry.header);
                count = count.checked_add(1).ok_or(MetadataError::Limit)?;
                first.get_or_insert(entry.header.object.oid);
                last = Some(entry.header.object.oid);
                after = entry.header.object.oid.to_vec();
            }
        }
        let first_oid = first.ok_or(MetadataError::Integrity)?;
        let last_oid = last.ok_or(MetadataError::Integrity)?;
        self.connection.execute(
            "INSERT INTO directory_identity VALUES (1,?1,?2,?3,?4,?5,?6,?7)",
            params![
                self.repository.as_slice(),
                self.operation.as_slice(),
                self.format.as_str(),
                i64::try_from(count).map_err(|_| MetadataError::Limit)?,
                first_oid.as_ref(),
                last_oid.as_ref(),
                inventory.as_slice()
            ],
        )?;
        self.connection
            .close()
            .map_err(|(_, error)| MetadataError::Sql(error))?;
        self.admitted.clean_journal()?;
        self.admitted.file().as_file().sync_all()?;
        let size = self.admitted.file().as_file().metadata()?.len();
        if size > self.limits.max_file_bytes {
            return Err(MetadataError::Limit);
        }
        let descriptor = RunDescriptor {
            repository: self.repository,
            operation: self.operation,
            format: self.format,
            object_count: count,
            first_oid,
            last_oid,
            inventory_digest: inventory,
            size,
            digest: file_digest(self.admitted.file().path(), size)?,
        };
        self.admitted.reservation().resize(size)?;
        DirectoryRun::open_admitted(self.admitted, descriptor, self.limits.cache_kib)
    }
}
