use super::*;

/// Disk-backed verification spool. Header/edge batches are atomic and bounded.
/// Supplied canonical headers must come from the trusted streaming verifier.
pub struct MetadataBuilder {
    // Close SQLite (and its journal) before deleting the spool or releasing
    // admission on a failed or canceled builder.
    connection: Connection,
    admitted: AdmittedFile,
    identity: SegmentIdentity,
    limits: MetadataLimits,
    failed: bool,
}
impl MetadataBuilder {
    pub fn new(
        root: &Path,
        budget: DiskBudget,
        identity: SegmentIdentity,
        limits: MetadataLimits,
    ) -> Result<Self, MetadataError> {
        validate_identity(identity)?;
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
            .prefix("canopy-metadata-")
            .tempfile_in(root)?;
        let admitted = AdmittedFile::new(file, reservation);
        let connection = Connection::open(admitted.file().path())?;
        connection.execute_batch("PRAGMA page_size=4096; PRAGMA journal_mode=DELETE; PRAGMA synchronous=FULL; PRAGMA foreign_keys=ON; PRAGMA trusted_schema=OFF; PRAGMA mmap_size=0;")?;
        connection.pragma_update(None, "cache_size", -(limits.cache_kib as i64))?;
        connection.pragma_update(None, "max_page_count", limits.max_file_bytes / 4096)?;
        connection.execute_batch(SCHEMA)?;
        Ok(Self {
            admitted,
            connection,
            identity,
            limits,
            failed: false,
        })
    }

    pub fn put_objects(&mut self, objects: &[CanonicalObject]) -> Result<(), MetadataError> {
        self.check_healthy()?;
        if objects.is_empty() || objects.len() > PAGE_OBJECTS {
            return Err(MetadataError::Limit);
        }
        if objects.iter().any(|object| {
            object.oid.format() != self.identity.format
                || object.oid.is_zero()
                || object.size > i64::MAX as u64
        }) {
            return Err(MetadataError::Integrity);
        }
        let transaction = self.connection.transaction()?;
        {
            let mut insert = transaction.prepare_cached("INSERT INTO objects(oid, kind, size, digest) VALUES (?1,?2,?3,?4) ON CONFLICT DO NOTHING")?;
            let mut existing = transaction
                .prepare_cached("SELECT oid, kind, size, digest FROM objects WHERE oid = ?1")?;
            for object in objects {
                insert.execute(params![
                    object.oid.as_ref(),
                    object.kind.git_name(),
                    object.size as i64,
                    object.digest.as_slice()
                ])?;
                if existing.query_row([object.oid.as_ref()], canonical)? != *object {
                    return Err(MetadataError::IdentityConflict);
                }
            }
        }
        transaction.commit()?;
        Ok(())
    }

    pub fn put_edges(
        &mut self,
        parent: ObjectId,
        edges: &[TypedEdge],
    ) -> Result<(), MetadataError> {
        self.check_healthy()?;
        if edges.is_empty() || edges.len() > PAGE_OBJECTS {
            return Err(MetadataError::Limit);
        }
        if parent.format() != self.identity.format
            || parent.is_zero()
            || edges
                .iter()
                .any(|edge| edge.child.format() != self.identity.format || edge.child.is_zero())
        {
            return Err(MetadataError::Integrity);
        }
        let transaction = self.connection.transaction()?;
        let parent_kind = transaction
            .query_row(
                "SELECT kind FROM objects WHERE oid = ?1",
                [parent.as_ref()],
                |row| kind(&row.get::<_, String>(0)?),
            )
            .optional()?
            .ok_or(MetadataError::Integrity)?;
        if parent_kind == ObjectKind::Blob
            || edges.iter().any(|edge| match parent_kind {
                ObjectKind::Tree => {
                    !matches!(edge.expected_kind, ObjectKind::Blob | ObjectKind::Tree)
                }
                ObjectKind::Commit => {
                    !matches!(edge.expected_kind, ObjectKind::Commit | ObjectKind::Tree)
                }
                _ => false,
            })
        {
            return Err(MetadataError::Integrity);
        }
        {
            let mut insert = transaction.prepare_cached("INSERT INTO object_edges(parent,child,expected_kind) VALUES (?1,?2,?3) ON CONFLICT DO NOTHING")?;
            let mut existing = transaction.prepare_cached(
                "SELECT expected_kind FROM object_edges WHERE parent = ?1 AND child = ?2",
            )?;
            for edge in edges {
                insert.execute(params![
                    parent.as_ref(),
                    edge.child.as_ref(),
                    edge.expected_kind.git_name()
                ])?;
                if existing.query_row(params![parent.as_ref(), edge.child.as_ref()], |row| {
                    kind(&row.get::<_, String>(0)?)
                })? != edge.expected_kind
                {
                    return Err(MetadataError::IdentityConflict);
                }
            }
        }
        transaction.commit()?;
        Ok(())
    }

    fn check_healthy(&self) -> Result<(), MetadataError> {
        if self.failed {
            Err(MetadataError::Integrity)
        } else {
            Ok(())
        }
    }

    /// Atomically import one complete decoded witness. No header or dependency
    /// survives a failed replay, and failure permanently prevents sealing.
    /// Physical pack binding and global typed closure are separate requirements.
    pub fn put_verified(
        &mut self,
        witness: super::super::verification::VerifiedObject,
    ) -> Result<(), MetadataError> {
        self.put_verified_batch(vec![witness])
    }

    /// Bound retained witnesses and amortize SQLite durability over up to one
    /// page. Any failure rolls back the whole batch and poisons this builder.
    /// Run on a blocking worker that owns the admitted builder and witnesses
    /// until it finishes, even when the requesting async future is canceled.
    pub fn put_verified_batch(
        &mut self,
        witnesses: Vec<super::super::verification::VerifiedObject>,
    ) -> Result<(), MetadataError> {
        self.check_healthy()?;
        self.failed = true;
        if witnesses.is_empty() || witnesses.len() > PAGE_OBJECTS {
            return Err(MetadataError::Limit);
        }
        let transaction = self.connection.transaction()?;
        for witness in witnesses {
            let object = witness.object();
            if object.oid.format() != self.identity.format
                || object.oid.is_zero()
                || object.size > i64::MAX as u64
            {
                return Err(MetadataError::Integrity);
            }
            transaction.execute("INSERT INTO objects(oid, kind, size, digest) VALUES (?1,?2,?3,?4) ON CONFLICT DO NOTHING", params![object.oid.as_ref(),object.kind.git_name(),object.size as i64,object.digest.as_slice()])?;
            let existing = transaction.query_row(
                "SELECT oid, kind, size, digest FROM objects WHERE oid = ?1",
                [object.oid.as_ref()],
                canonical,
            )?;
            if existing != object {
                return Err(MetadataError::IdentityConflict);
            }
            {
                let mut insert = transaction.prepare_cached("INSERT INTO object_edges(parent,child,expected_kind) VALUES (?1,?2,?3) ON CONFLICT DO NOTHING")?;
                let mut existing = transaction.prepare_cached(
                    "SELECT expected_kind FROM object_edges WHERE parent = ?1 AND child = ?2",
                )?;
                witness.replay(|edges| {
                    for edge in edges {
                        if edge.child.format() != self.identity.format
                            || edge.child.is_zero()
                            || match object.kind {
                                ObjectKind::Blob => true,
                                ObjectKind::Tree => !matches!(
                                    edge.expected_kind,
                                    ObjectKind::Tree | ObjectKind::Blob
                                ),
                                ObjectKind::Commit => !matches!(
                                    edge.expected_kind,
                                    ObjectKind::Tree | ObjectKind::Commit
                                ),
                                ObjectKind::Tag => false,
                            }
                        {
                            return Err(MetadataError::Integrity);
                        }
                        insert.execute(params![
                            object.oid.as_ref(),
                            edge.child.as_ref(),
                            edge.expected_kind.git_name()
                        ])?;
                        if existing
                            .query_row(params![object.oid.as_ref(), edge.child.as_ref()], |row| {
                                kind(&row.get::<_, String>(0)?)
                            })?
                            != edge.expected_kind
                        {
                            return Err(MetadataError::IdentityConflict);
                        }
                    }
                    Ok(())
                })?;
            }
        }
        transaction.commit()?;
        self.failed = false;
        Ok(())
    }

    pub fn seal(mut self, index: &PackIndex) -> Result<MetadataSegment, MetadataError> {
        self.check_healthy()?;
        let end = self
            .identity
            .first_ordinal
            .checked_add(self.identity.object_count)
            .ok_or(MetadataError::Integrity)?;
        if index.format() != self.identity.format
            || index.pack_checksum() != self.identity.git_checksum
            || end > index.len()
        {
            return Err(MetadataError::Integrity);
        }
        let count: u64 = self
            .connection
            .query_row("SELECT count(*) FROM objects", [], |row| row.get(0))?;
        if count != u64::from(self.identity.object_count) {
            return Err(MetadataError::Integrity);
        }
        let wrong_child: bool = self.connection.query_row("SELECT EXISTS(SELECT 1 FROM object_edges e JOIN objects o ON o.oid = e.child WHERE o.kind != e.expected_kind)", [], |row| row.get(0))?;
        if wrong_child {
            return Err(MetadataError::Integrity);
        }
        let mut inventory = inventory_seed(self.identity);
        let mut native = index.ids_from(self.identity.first_ordinal)?;
        let mut after = Vec::new();
        let mut ordinal = 0;
        let mut total_edges = 0_u64;
        let mut first_oid = None;
        let mut last_oid = None;
        loop {
            let objects = {
                let mut statement = self.connection.prepare_cached("SELECT oid, kind, size, digest FROM objects WHERE oid > ?1 ORDER BY oid LIMIT ?2")?;
                statement
                    .query_map(params![after, PAGE_OBJECTS as i64], canonical)?
                    .collect::<rusqlite::Result<Vec<_>>>()?
            };
            if objects.is_empty() {
                break;
            }
            let transaction = self.connection.transaction()?;
            for object in objects {
                if native.next().transpose()? != Some(object.oid) {
                    return Err(MetadataError::Integrity);
                }
                let mut chain = edge_seed(object.oid);
                let mut edge_count = 0_u64;
                let mut tree_count = 0_u64;
                {
                    let mut statement = transaction.prepare_cached("SELECT child, expected_kind FROM object_edges WHERE parent = ?1 ORDER BY child")?;
                    let mut rows = statement.query([object.oid.as_ref()])?;
                    while let Some(row) = rows.next()? {
                        let child = oid(row.get(0)?)?;
                        let expected_kind = kind(&row.get::<_, String>(1)?)?;
                        let mut record = [0; 33];
                        let width = child.len();
                        record[..width].copy_from_slice(&child);
                        record[width] = kind_code(expected_kind);
                        chain = fold(chain, edge_count, &record[..width + 1]);
                        edge_count = edge_count.checked_add(1).ok_or(MetadataError::Limit)?;
                        tree_count += u64::from(expected_kind == ObjectKind::Tree);
                    }
                }
                if (object.kind == ObjectKind::Blob && edge_count != 0)
                    || (object.kind == ObjectKind::Tag && edge_count != 1)
                    || (object.kind == ObjectKind::Commit && tree_count != 1)
                {
                    return Err(MetadataError::Integrity);
                }
                let edge_count_sql = i64::try_from(edge_count).map_err(|_| MetadataError::Limit)?;
                transaction.execute(
                    "UPDATE objects SET edge_count = ?2, edge_digest = ?3 WHERE oid = ?1",
                    params![object.oid.as_ref(), edge_count_sql, chain.as_slice()],
                )?;
                inventory = fold_header(
                    inventory,
                    ordinal,
                    ObjectHeader {
                        object,
                        edge_count,
                        edge_digest: chain,
                    },
                );
                ordinal += 1;
                total_edges = total_edges
                    .checked_add(edge_count)
                    .ok_or(MetadataError::Limit)?;
                first_oid.get_or_insert(object.oid);
                last_oid = Some(object.oid);
                after = object.oid.to_vec();
            }
            transaction.commit()?;
        }
        if ordinal != count {
            return Err(MetadataError::Integrity);
        }
        let identity = self.identity;
        self.connection.execute(
            "INSERT INTO segment_identity VALUES(1,?1,?2,?3,?4,?5,?6,?7,?8,?9)",
            params![
                identity.repository.as_slice(),
                identity.operation.as_slice(),
                identity.format.as_str(),
                identity.pack_digest.as_slice(),
                identity.git_checksum.as_ref(),
                identity.first_ordinal,
                identity.object_count,
                i64::try_from(total_edges).map_err(|_| MetadataError::Limit)?,
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
        let descriptor = SegmentDescriptor {
            identity,
            edge_count: total_edges,
            inventory_digest: inventory,
            first_oid: first_oid.ok_or(MetadataError::Integrity)?,
            last_oid: last_oid.ok_or(MetadataError::Integrity)?,
            size,
            digest: file_digest(self.admitted.file().path(), size)?,
        };
        self.admitted.reservation().resize(size)?;
        MetadataSegment::open_admitted(self.admitted, descriptor, self.limits.cache_kib)
    }
}
