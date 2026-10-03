use super::*;
use spool::validate_header;

impl Spool {
    pub(super) fn copy_segment(
        &mut self,
        segment: &MetadataSegment,
        operation: [u8; 16],
    ) -> Result<(), ClosureError> {
        self.check_cancel()?;
        let descriptor = segment.descriptor();
        let identity = descriptor.identity;
        if identity.repository != self.context.repository
            || identity.operation != operation
            || identity.format != self.context.format
            || metadata::file_digest_with::<ClosureError>(segment.path(), descriptor.size, || {
                self.check_cancel()
            })? != descriptor.digest
        {
            return Err(ClosureError::Integrity);
        }
        let mut after = None;
        let mut count = 0_u64;
        let mut edge_count = 0_u64;
        let mut first = None;
        let mut inventory = metadata::inventory_seed(identity);
        loop {
            self.check_cancel()?;
            let headers = segment.headers_after(after)?;
            if headers.is_empty() {
                break;
            }
            let canceled = Arc::clone(&self.canceled);
            let (next_first, next_after, next_inventory, next_count, next_edges) = self.write(|tx| {
                let mut first = first;
                let mut after = after;
                let mut inventory = inventory;
                let mut count = count;
                let mut edge_count = edge_count;
            for &header in &headers {
                validate_header(header, identity.format)?;
                let h = header.object;
                tx.execute("INSERT INTO objects(oid,kind,size,digest,edge_count,edge_digest) VALUES(?1,?2,?3,?4,?5,?6) ON CONFLICT DO NOTHING", params![h.oid.as_ref(),h.kind.git_name(),h.size as i64,h.digest.as_slice(),header.edge_count as i64,header.edge_digest.as_slice()])?;
                let current = tx.query_row(
                    "SELECT oid,kind,size,digest,edge_count,edge_digest FROM objects WHERE oid=?1",
                    [h.oid.as_ref()],
                    metadata::header,
                )?;
                if current != header {
                    return Err(MetadataError::IdentityConflict.into());
                }
                verify_edges(tx, segment, header, &canceled)?;
                first.get_or_insert(h.oid);
                after = Some(h.oid);
                inventory = metadata::fold_header(inventory, count, header);
                count = count.checked_add(1).ok_or(MetadataError::Limit)?;
                edge_count = edge_count
                    .checked_add(header.edge_count)
                    .ok_or(MetadataError::Limit)?;
            }
                Ok((first, after, inventory, count, edge_count))
            })?;
            first = next_first;
            after = next_after;
            inventory = next_inventory;
            count = next_count;
            edge_count = next_edges;
        }
        if count != u64::from(identity.object_count)
            || edge_count != descriptor.edge_count
            || first != Some(descriptor.first_oid)
            || after != Some(descriptor.last_oid)
            || inventory != descriptor.inventory_digest
        {
            return Err(ClosureError::Integrity);
        }
        Ok(())
    }
}
fn verify_edges(
    tx: &rusqlite::Transaction<'_>,
    segment: &MetadataSegment,
    header: ObjectHeader,
    canceled: &AtomicBool,
) -> Result<(), ClosureError> {
    let parent = header.object;
    let mut after = None;
    let mut count = 0_u64;
    let mut trees = 0;
    let mut chain = metadata::edge_seed(parent.oid);
    let mut insert =
        tx.prepare_cached("INSERT INTO object_edges VALUES(?1,?2,?3) ON CONFLICT DO NOTHING")?;
    let mut existing =
        tx.prepare_cached("SELECT expected_kind FROM object_edges WHERE parent=?1 AND child=?2")?;
    loop {
        if canceled.load(Ordering::Acquire) {
            return Err(ClosureError::Canceled);
        }
        let page = segment.edges_after(parent.oid, after)?;
        if page.is_empty() {
            break;
        }
        for edge in page {
            if edge.child.format() != parent.oid.format()
                || edge.child.is_zero()
                || after.is_some_and(|oid| oid >= edge.child)
                || match parent.kind {
                    ObjectKind::Blob => true,
                    ObjectKind::Tree => {
                        !matches!(edge.expected_kind, ObjectKind::Tree | ObjectKind::Blob)
                    }
                    ObjectKind::Commit => {
                        !matches!(edge.expected_kind, ObjectKind::Tree | ObjectKind::Commit)
                    }
                    ObjectKind::Tag => false,
                }
            {
                return Err(ClosureError::Integrity);
            }
            insert.execute(params![
                parent.oid.as_ref(),
                edge.child.as_ref(),
                edge.expected_kind.git_name()
            ])?;
            if existing.query_row(params![parent.oid.as_ref(), edge.child.as_ref()], |row| {
                metadata::kind(&row.get::<_, String>(0)?)
            })? != edge.expected_kind
            {
                return Err(MetadataError::IdentityConflict.into());
            }
            let mut record = [0; 33];
            let width = edge.child.len();
            record[..width].copy_from_slice(&edge.child);
            record[width] = metadata::kind_code(edge.expected_kind);
            chain = metadata::fold(chain, count, &record[..width + 1]);
            count = count.checked_add(1).ok_or(MetadataError::Limit)?;
            trees += u64::from(edge.expected_kind == ObjectKind::Tree);
            after = Some(edge.child);
        }
    }
    if count != header.edge_count
        || chain != header.edge_digest
        || (parent.kind == ObjectKind::Tag && count != 1)
        || (parent.kind == ObjectKind::Commit && trees != 1)
    {
        return Err(ClosureError::Integrity);
    }
    Ok(())
}
