use super::*;
type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;
#[tokio::test]
async fn large_ancestry_queue_uses_an_index_and_releases_admitted_scratch() -> Result {
    let root = tempfile::TempDir::new()?;
    let budget = DiskBudget::new(64 << 20);
    let walker = Walker::new(
        root.path(),
        budget.clone(),
        MetadataLimits {
            max_file_bytes: 32 << 20,
            cache_kib: 64,
        },
    )
    .await?;
    assert_eq!(budget.used(), growth::INITIAL_BYTES * 3);
    for start in (0..100_001u64).step_by(PAGE_OBJECTS) {
        walker
            .call(move |scratch| {
                scratch.write(|tx| {
                    {
                        let mut insert = tx
                            .prepare_cached("INSERT INTO visits(oid,expanded) VALUES(?1,?2)")
                            .map_err(MetadataError::from)?;
                        for n in start..(start + PAGE_OBJECTS as u64).min(100_001) {
                            let mut oid = [1; 20];
                            oid[..8].copy_from_slice(&n.to_be_bytes());
                            insert
                                .execute(params![oid.as_slice(), n < 100_000])
                                .map_err(MetadataError::from)?;
                        }
                    }
                    Ok(())
                })
            })
            .await?;
    }
    walker.call(|scratch| {
        let plans=scratch.connection.prepare("EXPLAIN QUERY PLAN SELECT oid FROM visits WHERE expanded=0 ORDER BY oid LIMIT 512").map_err(MetadataError::from)?.query_map([],|row|row.get::<_,String>(3)).map_err(MetadataError::from)?.collect::<rusqlite::Result<Vec<_>>>().map_err(MetadataError::from)?;
        assert!(plans.iter().any(|plan|plan.contains("SEARCH")&&plan.contains("visits_ready")),"{plans:?}");
        let n:i64=scratch.connection.query_row("SELECT count(*) FROM (SELECT oid FROM visits WHERE expanded=0 ORDER BY oid LIMIT 512)",[],|row|row.get(0)).map_err(MetadataError::from)?;
        assert_eq!(n,1);
        Ok(())
    }).await?;
    walker
        .call(|scratch| {
            let plans = scratch
                .connection
                .prepare(&format!("EXPLAIN QUERY PLAN {CLEAR_PAGE}"))
                .map_err(MetadataError::from)?
                .query_map([PAGE_OBJECTS as i64], |r| r.get::<_, String>(3))
                .map_err(MetadataError::from)?
                .collect::<rusqlite::Result<Vec<_>>>()
                .map_err(MetadataError::from)?;
            assert!(
                plans
                    .iter()
                    .any(|p| p.contains("SEARCH visits USING PRIMARY KEY")),
                "{plans:?}"
            );
            assert!(
                !plans.iter().any(|p| p.contains("TEMP B-TREE")),
                "{plans:?}"
            );
            scratch.write(|tx| {
                tx.execute(
                    "INSERT INTO answers VALUES(?1,?2,1)",
                    params![[1u8; 20].as_slice(), [2u8; 20].as_slice()],
                )
                .map_err(MetadataError::from)?;
                Ok(())
            })
        })
        .await?;
    let mut removed = 0;
    loop {
        let n = walker.call(Scratch::clear_page).await?;
        assert!(n <= PAGE_OBJECTS);
        removed += n;
        if n == 0 {
            break;
        }
    }
    assert_eq!(removed, 100_001);
    walker
        .parents(vec![TypedEdge {
            child: ObjectId::Sha1([3; 20]),
            expected_kind: ObjectKind::Commit,
        }])
        .await?;
    walker
        .call(|scratch| {
            assert_eq!(
                scratch
                    .connection
                    .query_row("SELECT count(*) FROM visits", [], |r| r.get::<_, usize>(0))
                    .map_err(MetadataError::from)?,
                1
            );
            assert_eq!(
                scratch
                    .connection
                    .query_row("SELECT count(*) FROM answers", [], |r| r.get::<_, usize>(0))
                    .map_err(MetadataError::from)?,
                1
            );
            Ok(())
        })
        .await?;
    assert!(budget.used() > growth::INITIAL_BYTES * 3);
    assert!(budget.used() <= 64 << 20);
    drop(walker);
    assert_eq!(budget.used(), 0);
    assert_eq!(std::fs::read_dir(root)?.count(), 0);
    Ok(())
}
#[tokio::test]
async fn cancellation_retains_a_running_scratch_worker_until_it_drains() -> Result {
    let root = tempfile::TempDir::new()?;
    let budget = DiskBudget::new(32 << 20);
    let walker = Arc::new(
        Walker::new(
            root.path(),
            budget.clone(),
            MetadataLimits {
                max_file_bytes: 4 << 20,
                cache_kib: 64,
            },
        )
        .await?,
    );
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let work = Arc::clone(&walker);
    let task = tokio::spawn(async move {
        work.call(move |_| {
            let _ = entered_tx.send(());
            release_rx.recv().map_err(|_| RefProofError::Invalid)?;
            Ok(())
        })
        .await
    });
    entered_rx.await?;
    task.abort();
    assert!(task.await.is_err());
    drop(walker);
    assert_eq!(budget.used(), growth::INITIAL_BYTES * 3);
    assert_eq!(std::fs::read_dir(root.path())?.count(), 1);
    release_tx.send(())?;
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while budget.used() != 0 {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await?;
    assert_eq!(std::fs::read_dir(root.path())?.count(), 0);
    Ok(())
}
