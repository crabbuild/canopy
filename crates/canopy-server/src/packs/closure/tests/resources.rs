use super::*;
use std::sync::mpsc;

#[tokio::test]
async fn disk_admission_precedes_files_and_sqlite_full_releases_the_spool() -> Result {
    let root = tempfile::TempDir::new()?;
    let budget = DiskBudget::new(1);
    assert!(
        ClosureVerifier::new(
            root.path(),
            budget.clone(),
            context(ObjectFormat::Sha256),
            limits()
        )
        .await
        .is_err()
    );
    assert_eq!(budget.used(), 0);
    assert_eq!(std::fs::read_dir(root.path())?.count(), 0);
    let limits = MetadataLimits {
        max_file_bytes: 64 << 10,
        cache_kib: 16,
    };
    let budget = DiskBudget::new(limits.max_file_bytes * 3);
    let mut spool = Spool::new(
        root.path(),
        budget.clone(),
        context(ObjectFormat::Sha256),
        limits,
        Arc::new(AtomicBool::new(false)),
    )?;
    let page: Vec<_> = (1..=512)
        .map(|n| graph::synthetic(ObjectFormat::Sha256, n, ObjectKind::Blob, &[]))
        .collect();
    let error = graph::insert(&mut spool, &page).unwrap_err();
    assert!(matches!(
        error.downcast_ref::<ClosureError>(),
        Some(ClosureError::Metadata(MetadataError::Limit))
    ));
    let pages: u64 = spool
        .connection
        .pragma_query_value(None, "page_count", |r| r.get(0))?;
    assert!(pages * 4096 <= limits.max_file_bytes);
    assert_eq!(budget.used(), limits.max_file_bytes * 3);
    drop(spool);
    cleanup(root.path(), &budget).await?;
    Ok(())
}

#[test]
fn canceled_queued_work_retains_ownership_and_admission_until_the_worker_exits() -> Result {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()?;
    runtime.block_on(async {
        let root = tempfile::TempDir::new()?;
        let budget = DiskBudget::new(64 << 20);
        let closure = ClosureVerifier::new(
            root.path(),
            budget.clone(),
            context(ObjectFormat::Sha256),
            limits(),
        )
        .await?;
        let (weak, canceled) = closure.test_ownership();
        let used = budget.used();
        assert!(used > 0);
        let (release_tx, release_rx) = mpsc::channel();
        let started = Arc::new(tokio::sync::Notify::new());
        let notify = started.clone();
        let blocked = tokio::task::spawn_blocking(move || {
            notify.notify_one();
            release_rx.recv().unwrap();
        });
        started.notified().await;
        let mut finish = Box::pin(closure.finish(None::<&Resolver>));
        assert!(
            tokio::time::timeout(Duration::from_millis(25), &mut finish)
                .await
                .is_err()
        );
        drop(finish);
        assert!(canceled.load(Ordering::Acquire));
        assert!(weak.upgrade().is_some());
        assert_eq!(budget.used(), used);
        release_tx.send(())?;
        blocked.await?;
        cleanup(root.path(), &budget).await?;
        assert!(weak.upgrade().is_none());
        Ok::<_, Box<dyn std::error::Error>>(())
    })
}

#[tokio::test]
async fn cancellation_interrupts_active_sql_and_bounded_file_hashing() -> Result {
    let root = tempfile::TempDir::new()?;
    let budget = DiskBudget::new(64 << 20);
    let canceled = Arc::new(AtomicBool::new(false));
    let spool = Spool::new(
        root.path(),
        budget.clone(),
        context(ObjectFormat::Sha256),
        limits(),
        canceled.clone(),
    )?;
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let work = tokio::task::spawn_blocking(move || {
        let _ = started_tx.send(());
        // A long SQLite VM loop avoids unbounded fixture allocation and proves
        // cancellation interrupts within a query, rather than only between pages.
        let result=spool.connection.query_row("WITH RECURSIVE numbers(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM numbers WHERE n<1000000000) SELECT sum(n) FROM numbers",[],|row|row.get::<_,i64>(0));
        assert!(
            matches!(result,Err(rusqlite::Error::SqliteFailure(code,_)) if code.code==rusqlite::ErrorCode::OperationInterrupted)
        );
        drop(spool);
    });
    started_rx.await?;
    canceled.store(true, Ordering::Release);
    tokio::time::timeout(Duration::from_secs(5), work).await??;
    cleanup(root.path(), &budget).await?;
    let file = tempfile::NamedTempFile::new()?;
    std::fs::write(file.path(), vec![5; 1 << 20])?;
    let mut checkpoints = 0;
    let result = metadata::file_digest_with::<ClosureError>(file.path(), 1 << 20, || {
        checkpoints += 1;
        if checkpoints == 5 {
            Err(ClosureError::Canceled)
        } else {
            Ok(())
        }
    });
    assert!(matches!(result, Err(ClosureError::Canceled)));
    assert_eq!(checkpoints, 5);
    assert_eq!(
        metadata::file_digest(file.path(), 1 << 20)?,
        *blake3::hash(&vec![5; 1 << 20]).as_bytes()
    );
    Ok(())
}
