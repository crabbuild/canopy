use super::*;

const RECONCILE_PAGE: &str = "SELECT l.oid,COALESCE(o.kind,b.kind),COALESCE(o.size,b.size),COALESCE(o.digest,b.digest),COALESCE(o.edge_count,b.edge_count),COALESCE(o.edge_digest,b.edge_digest),o.oid IS NOT NULL FROM lookups l LEFT JOIN objects o ON o.oid=l.oid LEFT JOIN base_objects b ON b.oid=l.oid WHERE l.oid>?1 ORDER BY l.oid LIMIT ?2";

/// Constructed only after incoming physical partitions, canonical overlap and
/// topology checks finish. Retains the already admitted scratch; no heap OID set
/// or historical graph is copied. Queued workers keep that scratch admitted.
pub(in crate::packs) struct RetainedClosure {
    pub(super) spool: Arc<Mutex<Spool>>,
    pub(super) context: ClosureContext,
    pub(super) canceled: Arc<AtomicBool>,
}
impl RetainedClosure {
    pub(in crate::packs) async fn reconcile(
        &self,
        context: ClosureContext,
        resolver: &impl BaseResolver,
    ) -> Result<(), ClosureError> {
        context.validate()?;
        if context.repository != self.context.repository
            || context.operation != self.context.operation
            || context.format != self.context.format
            || context.base.map_or(0, |base| base.generation)
                < self.context.base.map_or(0, |base| base.generation)
            || (context.base.map_or(0, |base| base.generation)
                == self.context.base.map_or(0, |base| base.generation)
                && context.base != self.context.base)
        {
            return Err(ClosureError::Integrity);
        }
        if context == self.context {
            return Ok(());
        }
        let base = context.base.ok_or(ClosureError::Integrity)?;
        let canceled = Arc::new(AtomicBool::new(false));
        let mut guard = CancelGuard::new(Arc::clone(&canceled));
        let mut after = Vec::new();
        loop {
            let spool = Arc::clone(&self.spool);
            let token = Arc::clone(&canceled);
            let cursor = after.clone();
            let page = tokio::task::spawn_blocking(move || {
                if token.load(Ordering::Acquire) {
                    return Err(ClosureError::Canceled);
                }
                let spool = spool.lock().map_err(|_| ClosureError::Integrity)?;
                spool.check_cancel()?;
                let result = spool
                    .connection
                    .prepare_cached(RECONCILE_PAGE)?
                    .query_map(params![cursor, PAGE_OBJECTS as i64], |row| {
                        Ok((metadata::header(row)?, row.get::<_, bool>(6)?))
                    })?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                if token.load(Ordering::Acquire) {
                    return Err(ClosureError::Canceled);
                }
                Ok::<_, ClosureError>(result)
            })
            .await??;
            if page.is_empty() {
                break;
            }
            let ids = page
                .iter()
                .map(|(header, _)| header.object.oid)
                .collect::<Vec<_>>();
            let found = resolver.resolve(base, &ids).await?;
            if found.base != base || found.objects.len() != page.len() {
                return Err(ClosureError::Integrity);
            }
            for ((expected, incoming), actual) in page.into_iter().zip(found.objects) {
                match actual {
                    Some(actual) if !actual.certified => {
                        return Err(ClosureError::Uncertified(expected.object.oid));
                    }
                    Some(actual) if actual.header != expected => {
                        return Err(MetadataError::IdentityConflict.into());
                    }
                    None if !incoming => return Err(ClosureError::Missing(expected.object.oid)),
                    _ => {}
                }
                after = expected.object.oid.to_vec();
            }
        }
        // Every external anchor has the identical canonical body and graph
        // digest in a certified new base. The already verified incoming DAG can
        // therefore be reused; no topological processing of history is needed.
        guard.complete();
        Ok(())
    }
}

impl Drop for RetainedClosure {
    fn drop(&mut self) {
        self.canceled.store(true, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;
    struct Empty;
    impl BaseResolver for Empty {
        async fn resolve(
            &self,
            base: ClosureBase,
            ids: &[ObjectId],
        ) -> std::result::Result<BaseBatch, ClosureError> {
            Ok(BaseBatch {
                base,
                objects: vec![None; ids.len()],
            })
        }
    }
    #[test]
    fn reconciliation_pages_use_indexed_incoming_and_anchor_lookups() -> Result {
        let root = tempfile::TempDir::new()?;
        let budget = DiskBudget::new(128 << 20);
        let context = ClosureContext {
            repository: [1; 16],
            operation: [2; 16],
            format: ObjectFormat::Sha256,
            base: None,
        };
        let mut spool = Spool::new(
            root.path(),
            budget.clone(),
            context,
            metadata::tests::limits(),
            Arc::new(AtomicBool::new(false)),
        )?;
        {
            spool.write(|tx| {
            for n in 1u64..=10000 {
                let mut oid = [0; 32];
                oid[24..].copy_from_slice(&n.to_be_bytes());
                tx.execute("INSERT INTO lookups VALUES(?1,1)", [oid.as_slice()])?;
                if n.is_multiple_of(2) {
                    tx.execute("INSERT INTO objects(oid,kind,size,digest,edge_count,edge_digest,done) VALUES(?1,'blob',0,zeroblob(32),0,zeroblob(32),1)",[oid.as_slice()])?;
                } else {
                    tx.execute(
                        "INSERT INTO base_objects VALUES(?1,'blob',0,zeroblob(32),0,zeroblob(32))",
                        [oid.as_slice()],
                    )?;
                }
            }
            Ok(())
            })?;
        }
        let mut after = [0; 32];
        after[24..].copy_from_slice(&9000u64.to_be_bytes());
        let explain = format!("EXPLAIN QUERY PLAN {RECONCILE_PAGE}");
        let plan = spool
            .connection
            .prepare(&explain)?
            .query_map(params![after.as_slice(), PAGE_OBJECTS as i64], |row| {
                row.get::<_, String>(3)
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        for alias in ["l", "o", "b"] {
            assert!(
                plan.iter()
                    .any(|line| line.contains(&format!("SEARCH {alias} USING PRIMARY KEY"))),
                "{plan:?}"
            );
            assert!(
                !plan
                    .iter()
                    .any(|line| line.contains(&format!("SCAN {alias}"))),
                "{plan:?}"
            );
        }
        let page = spool
            .connection
            .prepare(RECONCILE_PAGE)?
            .query_map(params![after.as_slice(), PAGE_OBJECTS as i64], |row| {
                Ok((metadata::header(row)?, row.get::<_, bool>(6)?))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        assert_eq!(page.len(), PAGE_OBJECTS);
        for (at, (header, incoming)) in page.into_iter().enumerate() {
            let n = 9001 + at as u64;
            assert_eq!(&header.object.oid[24..], &n.to_be_bytes());
            assert_eq!(incoming, n.is_multiple_of(2));
        }
        drop(spool);
        assert_eq!(budget.used(), 0);
        assert_eq!(std::fs::read_dir(root.path())?.count(), 0);
        Ok(())
    }
    #[test]
    fn canceled_reconciliation_keeps_admission_until_queued_reader_drains() -> Result {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .max_blocking_threads(1)
            .build()?;
        runtime.block_on(async {
            let root = tempfile::TempDir::new()?;
            let budget = DiskBudget::new(128 << 20);
            let context = ClosureContext {
                repository: [1; 16],
                operation: [2; 16],
                format: ObjectFormat::Sha1,
                base: None,
            };
            let verifier = ClosureVerifier::new(
                root.path(),
                budget.clone(),
                context,
                metadata::tests::limits(),
            )
            .await?;
            let (_, retained) = verifier.finish_retained(None::<&Empty>).await?;
            let stored = StoredCatalog {
                repository: context.repository,
                operation: [3; 16],
                format: context.format,
                artifact: canopy_object_storage::artifact::ArtifactDescriptor {
                    size: 1,
                    digest: [4; 32],
                    manifest_digest: [5; 32],
                },
            };
            let selected = ClosureContext {
                base: Some(ClosureBase {
                    generation: 1,
                    catalog: stored,
                }),
                ..context
            };
            let (release, receiver) = std::sync::mpsc::channel();
            let started = Arc::new(tokio::sync::Notify::new());
            let notify = Arc::clone(&started);
            let worker = tokio::task::spawn_blocking(move || {
                notify.notify_one();
                receiver.recv().unwrap();
            });
            started.notified().await;
            let mut pending = Box::pin(retained.reconcile(selected, &Empty));
            assert!(
                tokio::time::timeout(std::time::Duration::from_millis(25), &mut pending)
                    .await
                    .is_err()
            );
            drop(pending);
            // Cancellation of one attempt does not poison the private original
            // inventory. It may be retried after its queued reader drains.
            assert!(!retained.canceled.load(Ordering::Acquire));
            let held = budget.used();
            assert_eq!(held, metadata::growth::INITIAL_BYTES * 3);
            release.send(())?;
            worker.await?;
            retained.reconcile(selected, &Empty).await?;
            let weak = Arc::downgrade(&retained.spool);
            let (release, receiver) = std::sync::mpsc::channel();
            let started = Arc::new(tokio::sync::Notify::new());
            let notify = Arc::clone(&started);
            let worker = tokio::task::spawn_blocking(move || {
                notify.notify_one();
                receiver.recv().unwrap();
            });
            started.notified().await;
            let mut pending = Box::pin(retained.reconcile(selected, &Empty));
            assert!(
                tokio::time::timeout(std::time::Duration::from_millis(25), &mut pending)
                    .await
                    .is_err()
            );
            drop(pending);
            drop(retained);
            // Now no inventory owner remains. The queued canceled worker alone
            // must keep SQLite, its file and the reservation alive until drain.
            assert!(weak.upgrade().is_some());
            assert_eq!(budget.used(), held);
            release.send(())?;
            worker.await?;
            tokio::time::timeout(std::time::Duration::from_secs(5), async {
                while budget.used() != 0 {
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
            })
            .await?;
            assert!(weak.upgrade().is_none());
            assert_eq!(std::fs::read_dir(root.path())?.count(), 0);
            Ok::<_, Box<dyn std::error::Error>>(())
        })
    }
}
