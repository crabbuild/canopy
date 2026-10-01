use super::*;

impl Spool {
    pub(super) fn certify_graph(&mut self) -> Result<(), ClosureError> {
        self.check_cancel()?;
        let wrong = self.connection.query_row("SELECT e.child,e.expected_kind,COALESCE(o.kind,b.kind) FROM object_edges e LEFT JOIN objects o ON o.oid=e.child LEFT JOIN base_objects b ON b.oid=e.child WHERE COALESCE(o.kind,b.kind) IS NULL OR e.expected_kind != COALESCE(o.kind,b.kind) LIMIT 1", [], |row| {
            Ok((metadata::oid(row.get(0)?)?,metadata::kind(&row.get::<_,String>(1)?)?,row.get::<_,Option<String>>(2)?))
        }).optional()?;
        if let Some((oid, expected, kind)) = wrong {
            return match kind {
                Some(kind) => Err(ClosureError::Kind {
                    oid,
                    expected,
                    actual: metadata::kind(&kind)?,
                }),
                None => Err(ClosureError::Missing(oid)),
            };
        }
        // Only incoming vertices need topological processing. Certified external
        // dependencies are anchors; historical graph edges never enter scratch.
        self.connection.execute("UPDATE objects SET pending=(SELECT count(*) FROM object_edges e JOIN objects c ON c.oid=e.child WHERE e.parent=objects.oid)", [])?;
        // Reuse the partial ready index as a disk-backed queue. A transaction
        // performs at most 512 vertex/edge updates, including newly-ready
        // vertices, so long chains do not create a journal per vertex. One wide
        // reverse fanout may span transactions; its cursor has constant size.
        let mut active = None;
        loop {
            self.check_cancel()?;
            let tx = self.connection.transaction()?;
            let exhausted = advance(&tx, &mut active, &self.canceled)?;
            tx.commit()?;
            if exhausted {
                break;
            }
        }
        let unfinished: bool = self.connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM objects WHERE done=0)",
            [],
            |row| row.get(0),
        )?;
        if unfinished {
            return Err(ClosureError::Cycle);
        }
        Ok(())
    }
}

fn advance(
    tx: &rusqlite::Transaction<'_>,
    active: &mut Option<(ObjectId, Vec<u8>)>,
    canceled: &AtomicBool,
) -> Result<bool, ClosureError> {
    let mut updates = 0;
    let mut ready = tx.prepare_cached(
        "SELECT oid FROM objects WHERE pending=0 AND done=0 ORDER BY oid LIMIT 1",
    )?;
    let mut complete =
        tx.prepare_cached("UPDATE objects SET done=1 WHERE oid=?1 AND pending=0 AND done=0")?;
    let mut reverse = tx.prepare_cached(
        "SELECT parent FROM object_edges WHERE child=?1 AND parent>?2 ORDER BY parent LIMIT ?3",
    )?;
    let mut decrement = tx.prepare_cached(
        "UPDATE objects SET pending=pending-1 WHERE oid=?1 AND pending>0 AND done=0",
    )?;
    while updates < PAGE_OBJECTS {
        if canceled.load(Ordering::Acquire) {
            return Err(ClosureError::Canceled);
        }
        if active.is_none() {
            let Some(child) = ready
                .query_row([], |row| metadata::oid(row.get(0)?))
                .optional()?
            else {
                return Ok(true);
            };
            if complete.execute([child.as_ref()])? != 1 {
                return Err(ClosureError::Integrity);
            }
            *active = Some((child, Vec::new()));
            updates += 1;
            if updates == PAGE_OBJECTS {
                break;
            }
        }
        let (child, after) = active.as_mut().ok_or(ClosureError::Integrity)?;
        let limit = PAGE_OBJECTS - updates;
        let parents: Vec<ObjectId> = reverse
            .query_map(
                params![child.as_ref(), after.as_slice(), limit as i64],
                |row| metadata::oid(row.get(0)?),
            )?
            .collect::<rusqlite::Result<_>>()?;
        let exhausted = parents.len() < limit;
        for parent in parents {
            if decrement.execute([parent.as_ref()])? != 1 {
                return Err(ClosureError::Integrity);
            }
            *after = parent.to_vec();
            updates += 1;
        }
        if exhausted {
            *active = None;
        }
    }
    Ok(false)
}
