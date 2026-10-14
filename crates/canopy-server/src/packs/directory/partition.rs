//! Stream disjoint immutable runs from an admitted canonical spool. Callers must
//! exhaust the stream before publishing its root: exhaustion checks the entire
//! output inventory against the input, including count and range endpoints.
use super::*;

pub struct DirectoryPartitioner {
    input: Arc<DirectoryRun>,
    budget: DiskBudget,
    limits: MetadataLimits,
    pending: Vec<DirectoryEntry>,
    position: usize,
    after: Option<ObjectId>,
    count: u64,
    inventory: [u8; 32],
    first: Option<ObjectId>,
    last: Option<ObjectId>,
    finished: bool,
    failed: bool,
}
impl DirectoryPartitioner {
    pub(in crate::packs) fn validate_limits(limits: MetadataLimits) -> Result<(), MetadataError> {
        if limits.max_file_bytes < 16 << 10
            || limits.max_file_bytes > RUN_TARGET_BYTES
            || !limits.max_file_bytes.is_multiple_of(4096)
            || limits.cache_kib == 0
            || limits.cache_kib > i32::MAX as u32
        {
            return Err(MetadataError::Limit);
        }
        Ok(())
    }
    pub fn new(
        input: Arc<DirectoryRun>,
        budget: DiskBudget,
        limits: MetadataLimits,
    ) -> Result<Self, MetadataError> {
        Self::validate_limits(limits)?;
        Ok(Self {
            inventory: inventory_seed(input.descriptor.format),
            input,
            budget,
            limits,
            pending: Vec::new(),
            position: 0,
            after: None,
            count: 0,
            first: None,
            last: None,
            finished: false,
            failed: false,
        })
    }
    /// Blocking disk work. At most one input page (512 entries) and one output
    /// builder are live. Input/output workspace pins follow queued work and files.
    /// A failed call permanently poisons the stream; no partial root is complete.
    pub fn next_run(&mut self) -> Result<Option<Arc<DirectoryRun>>, MetadataError> {
        if self.failed {
            return Err(MetadataError::Integrity);
        }
        if self.finished {
            return Ok(None);
        }
        self.failed = true;
        let result = self.next_inner()?;
        self.failed = false;
        Ok(result)
    }
    fn next_inner(&mut self) -> Result<Option<Arc<DirectoryRun>>, MetadataError> {
        // Small pushes reuse their verified file without copying or reserving a
        // second builder. Its exact descriptor already binds the whole inventory.
        if self.input.descriptor.size <= self.limits.max_file_bytes {
            self.finished = true;
            return Ok(Some(Arc::clone(&self.input)));
        }
        let descriptor = self.input.descriptor;
        let root = self.input.path().parent().ok_or(MetadataError::Integrity)?;
        let mut output = DirectoryBuilder::new(
            root,
            self.budget.clone(),
            descriptor.repository,
            descriptor.operation,
            descriptor.format,
            self.limits,
        )?;
        if let Some(workspace) = self.input.admitted.workspace() {
            output.retain_workspace(workspace);
        }
        let mut copied = 0_u64;
        loop {
            if self.position == self.pending.len() {
                self.pending = self.input.entries_after(self.after)?;
                self.position = 0;
                self.after = self.pending.last().map(|entry| entry.header.object.oid);
                if self.pending.is_empty() {
                    if self.count != descriptor.object_count
                        || self.inventory != descriptor.inventory_digest
                        || self.first != Some(descriptor.first_oid)
                        || self.last != Some(descriptor.last_oid)
                    {
                        return Err(MetadataError::Integrity);
                    }
                    self.finished = true;
                    return if copied == 0 {
                        Ok(None)
                    } else {
                        Ok(Some(Arc::new(output.seal()?)))
                    };
                }
            }
            let entries = &self.pending[self.position..];
            let mut length = entries.len();
            loop {
                match output.put_entries(&entries[..length]) {
                    Ok(()) => break,
                    // SQLite rolls back the entire attempted transaction when
                    // max_page_count is reached. Retry bounded smaller prefixes;
                    // advance only entries that actually committed. Ordinary
                    // pages use one transaction, rather than one per object.
                    Err(MetadataError::Limit) if length > 1 => length /= 2,
                    Err(MetadataError::Limit) if copied > 0 => {
                        return Ok(Some(Arc::new(output.seal()?)));
                    }
                    Err(error) => return Err(error),
                }
            }
            for entry in &entries[..length] {
                let oid = entry.header.object.oid;
                if self.last.is_some_and(|last| last >= oid) {
                    return Err(MetadataError::Integrity);
                }
                self.inventory = fold_header(self.inventory, self.count, entry.header);
                self.count = self.count.checked_add(1).ok_or(MetadataError::Limit)?;
                self.first.get_or_insert(oid);
                self.last = Some(oid);
            }
            copied += length as u64;
            self.position += length;
        }
    }
}
