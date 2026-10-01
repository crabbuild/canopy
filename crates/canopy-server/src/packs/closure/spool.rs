use super::*;

pub(super) struct Spool {
    // SQLite closes before the journal/file are deleted and admission released.
    pub(super) connection: Connection,
    pub(super) _admitted: AdmittedFile,
    pub(super) context: ClosureContext,
    pub(super) canceled: Arc<AtomicBool>,
}
impl Spool {
    pub(super) fn new(
        root: &Path,
        budget: DiskBudget,
        context: ClosureContext,
        limits: MetadataLimits,
        canceled: Arc<AtomicBool>,
    ) -> Result<Self, ClosureError> {
        if canceled.load(Ordering::Acquire) {
            return Err(ClosureError::Canceled);
        }
        context.validate()?;
        if limits.cache_kib == 0
            || limits.cache_kib > i32::MAX as u32
            || limits.max_file_bytes < 16 << 10
            || limits.max_file_bytes > canopy_object_storage::external::MAX_ARTIFACT_BYTES
            || !limits.max_file_bytes.is_multiple_of(4096)
        {
            return Err(MetadataError::Limit.into());
        }
        let reservation = budget
            .try_reserve(
                limits
                    .max_file_bytes
                    .checked_mul(3)
                    .ok_or(MetadataError::Limit)?,
            )
            .map_err(MetadataError::from)?;
        let file = tempfile::Builder::new()
            .prefix("canopy-closure-")
            .tempfile_in(root)
            .map_err(MetadataError::from)?;
        let admitted = AdmittedFile::new(file, reservation);
        let connection = Connection::open(admitted.file().path())?;
        // No scratch state is an acknowledgement or recovery root. After a
        // crash, rebuild it from authenticated inputs; never reopen this file.
        connection.execute_batch("PRAGMA page_size=4096; PRAGMA journal_mode=DELETE; PRAGMA synchronous=OFF; PRAGMA foreign_keys=ON; PRAGMA trusted_schema=OFF; PRAGMA mmap_size=0;")?;
        connection.pragma_update(None, "cache_size", -(limits.cache_kib as i64))?;
        connection.pragma_update(None, "max_page_count", limits.max_file_bytes / 4096)?;
        let interrupt = Arc::clone(&canceled);
        connection.progress_handler(10_000, Some(move || interrupt.load(Ordering::Acquire)));
        let spool = Self {
            connection,
            _admitted: admitted,
            context,
            canceled,
        };
        spool.check_cancel()?;
        spool.connection.execute_batch(include_str!("schema.sql"))?;
        Ok(spool)
    }
    pub(super) fn check_cancel(&self) -> Result<(), ClosureError> {
        if self.canceled.load(Ordering::Acquire) {
            Err(ClosureError::Canceled)
        } else {
            Ok(())
        }
    }
    pub(super) fn input(&mut self, digest: [u8; 32]) -> Result<(), ClosureError> {
        self.check_cancel()?;
        self.connection.execute(
            "INSERT INTO inputs VALUES(?1) ON CONFLICT DO NOTHING",
            [digest.as_slice()],
        )?;
        Ok(())
    }
    pub(super) fn prepare_lookups(&mut self) -> Result<(), ClosureError> {
        self.check_cancel()?;
        let tx = self.connection.transaction()?;
        tx.execute("INSERT INTO lookups(oid) SELECT oid FROM objects", [])?;
        tx.execute(
            "INSERT OR IGNORE INTO lookups(oid) SELECT child FROM object_edges",
            [],
        )?;
        tx.commit()?;
        Ok(())
    }
    pub(super) fn lookup_page(&self) -> Result<Vec<ObjectId>, ClosureError> {
        self.check_cancel()?;
        Ok(self
            .connection
            .prepare_cached("SELECT oid FROM lookups WHERE resolved=0 ORDER BY oid LIMIT ?1")?
            .query_map([PAGE_OBJECTS as i64], |row| metadata::oid(row.get(0)?))?
            .collect::<rusqlite::Result<_>>()?)
    }
    pub(super) fn apply_base(
        &mut self,
        ids: &[ObjectId],
        batch: BaseBatch,
    ) -> Result<(), ClosureError> {
        self.check_cancel()?;
        if self.context.base != Some(batch.base)
            || ids.is_empty()
            || ids.len() > PAGE_OBJECTS
            || batch.objects.len() != ids.len()
        {
            return Err(ClosureError::Integrity);
        }
        let tx = self.connection.transaction()?;
        for (oid, found) in ids.iter().zip(batch.objects) {
            if let Some(found) = found {
                if found.header.object.oid != *oid {
                    return Err(ClosureError::Integrity);
                }
                validate_header(found.header, self.context.format)?;
                let local = tx.query_row("SELECT oid,kind,size,digest,edge_count,edge_digest FROM objects WHERE oid=?1", [oid.as_ref()], metadata::header).optional()?;
                if local.is_some_and(|header| header != found.header) {
                    return Err(MetadataError::IdentityConflict.into());
                }
                if !found.certified {
                    return Err(ClosureError::Uncertified(*oid));
                }
                let h = found.header;
                tx.execute(
                    "INSERT INTO base_objects VALUES(?1,?2,?3,?4,?5,?6)",
                    params![
                        oid.as_ref(),
                        h.object.kind.git_name(),
                        h.object.size as i64,
                        h.object.digest.as_slice(),
                        h.edge_count as i64,
                        h.edge_digest.as_slice()
                    ],
                )?;
            }
            if tx.execute(
                "UPDATE lookups SET resolved=1 WHERE oid=?1 AND resolved=0",
                [oid.as_ref()],
            )? != 1
            {
                return Err(ClosureError::Integrity);
            }
        }
        tx.commit()?;
        Ok(())
    }
}
pub(super) fn validate_header(
    header: ObjectHeader,
    format: ObjectFormat,
) -> Result<(), ClosureError> {
    if header.object.oid.format() != format
        || header.object.oid.is_zero()
        || header.object.size > i64::MAX as u64
        || header.edge_count > i64::MAX as u64
        || (header.object.kind == ObjectKind::Blob && header.edge_count != 0)
    {
        return Err(ClosureError::Integrity);
    }
    Ok(())
}
