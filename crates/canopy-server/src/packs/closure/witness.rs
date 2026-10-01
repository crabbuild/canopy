use super::*;

/// Complete incoming closure conditional on the bound certified base. Reuse the
/// directory's canonical inventory encoding to bind the incoming directory.
/// The coordinator must verify catalog inputs/coverage and CAS context.base
/// under the admitted owner fence before any durable acknowledgement.
pub struct ClosureWitness {
    context: ClosureContext,
    object_count: u64,
    edge_count: u64,
    input_count: u64,
    inputs_digest: [u8; 32],
    inventory_digest: [u8; 32],
    first: Option<ObjectId>,
    last: Option<ObjectId>,
}
impl ClosureWitness {
    pub fn context(&self) -> ClosureContext {
        self.context
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
    pub fn verify_run(&self, run: RunDescriptor) -> Result<(), ClosureError> {
        run.validate()?;
        if run.repository != self.context.repository
            || run.operation != self.context.operation
            || run.format != self.context.format
            || run.object_count != self.object_count
            || run.inventory_digest != self.inventory_digest
            || Some(run.first_oid) != self.first
            || Some(run.last_oid) != self.last
        {
            return Err(ClosureError::Integrity);
        }
        Ok(())
    }
    /// For partitioned incoming directories, stream their complete canonical
    /// union in raw OID order. This does not verify descriptor/source provenance.
    pub fn verify_headers(
        &self,
        headers: impl IntoIterator<Item = ObjectHeader>,
    ) -> Result<(), ClosureError> {
        let mut count = 0_u64;
        let mut chain = directory::inventory_seed(self.context.format);
        let mut last = None;
        for header in headers {
            spool::validate_header(header, self.context.format)?;
            if last.is_some_and(|oid| oid >= header.object.oid) {
                return Err(ClosureError::Integrity);
            }
            chain = metadata::fold_header(chain, count, header);
            count = count.checked_add(1).ok_or(MetadataError::Limit)?;
            last = Some(header.object.oid);
        }
        if count != self.object_count || chain != self.inventory_digest {
            return Err(ClosureError::Integrity);
        }
        Ok(())
    }
}
impl Spool {
    pub(super) fn witness(&self) -> Result<ClosureWitness, ClosureError> {
        self.check_cancel()?;
        let mut after = Vec::new();
        let mut count = 0_u64;
        let mut edges = 0_u64;
        let mut first = None;
        let mut last = None;
        let mut inventory = directory::inventory_seed(self.context.format);
        loop {
            self.check_cancel()?;
            let headers: Vec<ObjectHeader> = self.connection.prepare_cached("SELECT oid,kind,size,digest,edge_count,edge_digest FROM objects WHERE oid>?1 ORDER BY oid LIMIT ?2")?
                .query_map(params![after,PAGE_OBJECTS as i64], metadata::header)?.collect::<rusqlite::Result<_>>()?;
            if headers.is_empty() {
                break;
            }
            for header in headers {
                spool::validate_header(header, self.context.format)?;
                inventory = metadata::fold_header(inventory, count, header);
                edges = edges
                    .checked_add(header.edge_count)
                    .ok_or(MetadataError::Limit)?;
                count = count.checked_add(1).ok_or(MetadataError::Limit)?;
                first.get_or_insert(header.object.oid);
                last = Some(header.object.oid);
                after = header.object.oid.to_vec();
            }
        }
        let actual_edges: u64 =
            self.connection
                .query_row("SELECT count(*) FROM object_edges", [], |row| row.get(0))?;
        if edges != actual_edges {
            return Err(ClosureError::Integrity);
        }
        let mut inputs = blake3::Hasher::new();
        inputs.update(b"canopy.closure-inputs.v1\0");
        inputs.update(&self.context.repository);
        inputs.update(&self.context.operation);
        inputs.update(&[self.context.format.bytes() as u8]);
        let mut rows = self
            .connection
            .prepare_cached("SELECT digest FROM inputs ORDER BY digest")?;
        let mut cursor = rows.query([])?;
        let mut input_count = 0;
        while let Some(row) = cursor.next()? {
            self.check_cancel()?;
            let digest = metadata::digest(row.get(0)?)?;
            inputs.update(&digest);
            input_count += 1;
        }
        Ok(ClosureWitness {
            context: self.context,
            object_count: count,
            edge_count: edges,
            input_count,
            inputs_digest: *inputs.finalize().as_bytes(),
            inventory_digest: inventory,
            first,
            last,
        })
    }
}
