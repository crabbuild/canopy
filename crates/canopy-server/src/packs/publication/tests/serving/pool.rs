//! Bounded pooling and actual lifecycle ownership, including head races.
use super::*;

pub(super) fn pooled(
    f: &Fixture,
    store: Arc<ArtifactStore>,
    root: &tempfile::TempDir,
    tasks: TaskTracker,
    q: PublicationCoordinator,
) -> Result<ServingPool> {
    Ok(ServingPool::new(
        ServingContext::new(
            f.client(),
            f.target.clone(),
            f.authority(),
            Arc::new(CatalogIndexes::new(store.clone(), f.format)),
            Arc::new(CatalogFiles::new(
                root.path(),
                DiskBudget::new(64 << 20),
                store,
                f.format,
                CatalogFileLimits::default(),
            )?),
            ServingReadBudget::new(32, tasks)?,
            "owner".into(),
        )?,
        q,
        ServingPoolLimits::default(),
    )?)
}
pub(super) fn queue(f: &Fixture) -> Result<PublicationCoordinator> {
    Ok(PublicationCoordinator::new(
        f.target.clone(),
        PublicationLimits::default(),
        f.publication_budget.clone(),
    )?)
}
async fn advance(f: &Fixture, generation: u64) -> Result {
    // Copied certified roots isolate selection/lifecycle semantics; this is not
    // native publication, changing Git content, or a full-history benchmark.
    edit(f, &format!("INSERT INTO catalog_generations(generation,catalog,certificate,refs) SELECT {generation},catalog,certificate,refs FROM catalog_generations WHERE generation=1; UPDATE catalog_state SET generation={generation} WHERE singleton=1")).await
}
pub(super) async fn finish(
    f: &Fixture,
    pool: &ServingPool,
    q: &PublicationCoordinator,
    tasks: TaskTracker,
) -> Result {
    timeout(Duration::from_secs(8), pool.close_and_drain()).await?;
    assert_eq!(pin_count(f).await?, 0);
    assert!(q.close_and_drain().await.is_empty());
    tasks.close();
    tasks.wait().await;
    Ok(())
}

#[tokio::test]
async fn concurrent_viewers_share_one_generation_and_every_cached_borrow_checks_access() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let f = Fixture::new(format).await?;
        let store = Arc::new(ArtifactStore::new(Arc::new(InMemory::new()), f.repository));
        initialize(&f, store.clone()).await?;
        edit(
            &f,
            "INSERT INTO repository_members(account,role) VALUES('viewer','read')",
        )
        .await?;
        let q = queue(&f)?;
        let root = tempfile::TempDir::new()?;
        let tasks = TaskTracker::new();
        let pool = pooled(&f, store, &root, tasks.clone(), q.clone())?;
        let before = f.counts().await?;
        let snapshots =
            futures_util::future::join_all((0..12).map(|_| pool.snapshot(Some("viewer".into()))))
                .await
                .into_iter()
                .collect::<std::result::Result<Vec<_>, _>>()?;
        assert_eq!(pin_count(&f).await?, 1);
        assert_eq!(pool.owners_for_test().await.len(), 1);
        assert!(snapshots.iter().all(|s| s.fact() == snapshots[0].fact()));
        assert_eq!(f.counts().await?, before);
        assert!(pool.snapshot(Some("other".into())).await.is_err());
        assert!(pool.snapshot(None).await.is_err());
        edit(&f, "UPDATE ref_generation SET visibility='public'").await?;
        let anonymous = pool.snapshot(None).await?;
        assert_eq!(anonymous.fact(), snapshots[0].fact());
        assert_eq!(pin_count(&f).await?, 1);
        edit(&f, "DELETE FROM repository_members WHERE account='viewer'; UPDATE ref_generation SET visibility='private'").await?;
        assert!(pool.snapshot(Some("viewer".into())).await.is_err());
        assert!(snapshots[0].headers(&[missing(&f)?]).await.is_err());
        assert!(anonymous.headers(&[missing(&f)?]).await.is_err());
        drop((snapshots, anonymous));
        finish(&f, &pool, &q, tasks).await?;
        f.runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn canceled_cold_observer_and_lost_ack_keep_one_owned_acquisition() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let f = Fixture::new(format).await?;
        let store = Arc::new(ArtifactStore::new(Arc::new(InMemory::new()), f.repository));
        initialize(&f, store.clone()).await?;
        let q = queue(&f)?;
        let root = tempfile::TempDir::new()?;
        let tasks = TaskTracker::new();
        let pool = pooled(&f, store, &root, tasks.clone(), q.clone())?;
        let (dispatch, entered) = q.pause_for_test().await;
        q.fault_for_test(2);
        let work = pool.clone();
        let observer = tokio::spawn(async move { work.snapshot(Some("owner".into())).await });
        timeout(Duration::from_secs(8), entered).await??;
        observer.abort();
        assert!(
            observer
                .await
                .err()
                .ok_or("completed early")?
                .is_cancelled()
        );
        dispatch.send(()).map_err(|_| "dispatch gone")?;
        let snapshot =
            timeout(Duration::from_secs(8), pool.snapshot(Some("owner".into()))).await??;
        assert_eq!(snapshot.fact().generation, 1);
        assert_eq!(pin_count(&f).await?, 1);
        assert_eq!(pool.owners_for_test().await.len(), 1);
        drop(snapshot);
        finish(&f, &pool, &q, tasks).await?;
        f.runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn sequential_generations_roll_over_idle_slots_without_client_retries() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let f = Fixture::new(format).await?;
        let store = Arc::new(ArtifactStore::new(Arc::new(InMemory::new()), f.repository));
        initialize(&f, store.clone()).await?;
        let q = queue(&f)?;
        let root = tempfile::TempDir::new()?;
        let tasks = TaskTracker::new();
        let pool = pooled(&f, store, &root, tasks.clone(), q.clone())?;
        for generation in 1..=12 {
            if generation > 1 {
                advance(&f, generation).await?;
            }
            let snapshot =
                timeout(Duration::from_secs(8), pool.snapshot(Some("owner".into()))).await??;
            assert_eq!(snapshot.fact().generation, generation);
            assert_eq!(snapshot.headers(&[missing(&f)?]).await?, vec![None]);
            assert!(pool.owners_for_test().await.len() <= 4);
            assert!(pin_count(&f).await? <= 4);
            drop(snapshot);
        }
        finish(&f, &pool, &q, tasks).await?;
        f.runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn rollover_waiters_share_release_after_observer_cancellation_and_lost_ack() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let f = Fixture::new(format).await?;
        let store = Arc::new(ArtifactStore::new(Arc::new(InMemory::new()), f.repository));
        initialize(&f, store.clone()).await?;
        let q = queue(&f)?;
        let root = tempfile::TempDir::new()?;
        let tasks = TaskTracker::new();
        let pool = pooled(&f, store, &root, tasks.clone(), q.clone())?;
        let mut old_views = Vec::new();
        for generation in 1..=4 {
            if generation > 1 {
                advance(&f, generation).await?;
            }
            old_views.push(pool.snapshot(Some("owner".into())).await?);
        }
        let old = pool.owners_for_test().await.remove(0);
        drop(old_views.remove(0));
        advance(&f, 5).await?;
        let (dispatch, entered) = q.pause_for_test().await;
        q.fault_for_test(2);
        let work = pool.clone();
        let observer = tokio::spawn(async move { work.snapshot(Some("owner".into())).await });
        timeout(Duration::from_secs(8), entered).await??;
        observer.abort();
        assert!(
            observer
                .await
                .err()
                .ok_or("rollover finished early")?
                .is_cancelled()
        );
        assert!(
            timeout(Duration::from_millis(20), old.drain_observer().wait())
                .await
                .is_err()
        );
        assert_eq!(pool.owners_for_test().await.len(), 4);
        assert_eq!(pin_count(&f).await?, 4);
        let work = pool.clone();
        let viewers = tokio::spawn(async move {
            futures_util::future::join_all((0..12).map(|_| work.snapshot(Some("owner".into()))))
                .await
                .into_iter()
                .collect::<std::result::Result<Vec<_>, _>>()
        });
        dispatch.send(()).map_err(|_| "release dispatch gone")?;
        let snapshots = timeout(Duration::from_secs(8), viewers).await???;
        assert!(
            snapshots
                .iter()
                .all(|snapshot| snapshot.fact().generation == 5)
        );
        assert_eq!(old.stats().phase, ServingOwnerPhase::Released);
        assert_eq!(pool.owners_for_test().await.len(), 4);
        assert_eq!(pin_count(&f).await?, 4);
        for view in &old_views {
            assert_eq!(view.headers(&[missing(&f)?]).await?, vec![None]);
        }
        drop((old_views, snapshots));
        finish(&f, &pool, &q, tasks).await?;
        f.runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn rollover_timeout_retains_the_slot_and_exact_release_for_retry() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let f = Fixture::new(format).await?;
        let store = Arc::new(ArtifactStore::new(Arc::new(InMemory::new()), f.repository));
        initialize(&f, store.clone()).await?;
        let q = queue(&f)?;
        let root = tempfile::TempDir::new()?;
        let tasks = TaskTracker::new();
        let pool = pooled(&f, store, &root, tasks.clone(), q.clone())?;
        let mut old_views = Vec::new();
        for generation in 1..=4 {
            if generation > 1 {
                advance(&f, generation).await?;
            }
            old_views.push(pool.snapshot(Some("owner".into())).await?);
        }
        let old = pool.owners_for_test().await.remove(0);
        drop(old_views.remove(0));
        advance(&f, 5).await?;
        let (dispatch, entered) = q.pause_for_test().await;
        let work = pool.clone();
        let observer = tokio::spawn(async move { work.snapshot(Some("owner".into())).await });
        timeout(Duration::from_secs(8), entered).await??;
        assert!(matches!(
            timeout(Duration::from_secs(8), observer).await??,
            Err(ServingOwnerError::Read(ServingReadError::Capability(
                Error::Capacity("repository serving generations")
            )))
        ));
        assert!(
            timeout(Duration::from_millis(20), old.drain_observer().wait())
                .await
                .is_err()
        );
        assert_eq!(pool.owners_for_test().await.len(), 4);
        assert_eq!(pin_count(&f).await?, 4);
        dispatch.send(()).map_err(|_| "release dispatch gone")?;
        let fifth = timeout(Duration::from_secs(8), pool.snapshot(Some("owner".into()))).await??;
        assert_eq!(fifth.fact().generation, 5);
        assert_eq!(pin_count(&f).await?, 4);
        for view in &old_views {
            assert_eq!(view.headers(&[missing(&f)?]).await?, vec![None]);
        }
        drop((old_views, fifth));
        finish(&f, &pool, &q, tasks).await?;
        f.runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn four_generation_bound_retains_borrows_and_reuses_only_actually_drained_slots() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let f = Fixture::new(format).await?;
        let store = Arc::new(ArtifactStore::new(Arc::new(InMemory::new()), f.repository));
        initialize(&f, store.clone()).await?;
        let q = queue(&f)?;
        let root = tempfile::TempDir::new()?;
        let tasks = TaskTracker::new();
        let pool = pooled(&f, store, &root, tasks.clone(), q.clone())?;
        let mut snapshots = Vec::new();
        for generation in 1..=4 {
            if generation > 1 {
                advance(&f, generation).await?;
            }
            let snapshot = pool.snapshot(Some("owner".into())).await?;
            assert_eq!(snapshot.fact().generation, generation);
            snapshots.push(snapshot);
        }
        advance(&f, 5).await?;
        assert!(matches!(
            pool.snapshot(Some("owner".into())).await,
            Err(ServingOwnerError::Read(ServingReadError::Capability(
                Error::Capacity("repository serving generations")
            )))
        ));
        assert_eq!(pin_count(&f).await?, 4);
        let old = pool.owners_for_test().await.remove(0);
        drop(snapshots.remove(0));
        let fifth = timeout(Duration::from_secs(8), pool.snapshot(Some("owner".into()))).await??;
        assert_eq!(
            timeout(Duration::from_secs(8), old.drain_observer().wait())
                .await?
                .phase,
            ServingOwnerPhase::Released
        );
        assert_eq!(fifth.fact().generation, 5);
        assert_eq!(pin_count(&f).await?, 4);
        for snapshot in &snapshots {
            assert_eq!(snapshot.headers(&[missing(&f)?]).await?, vec![None]);
        }
        drop((snapshots, fifth));
        finish(&f, &pool, &q, tasks).await?;
        f.runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn acquisition_head_race_returns_actual_accepted_fact_and_reuses_it() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let f = Fixture::new(format).await?;
        let store = Arc::new(ArtifactStore::new(Arc::new(InMemory::new()), f.repository));
        initialize(&f, store.clone()).await?;
        let q = queue(&f)?;
        let root = tempfile::TempDir::new()?;
        let tasks = TaskTracker::new();
        let pool = pooled(&f, store, &root, tasks.clone(), q.clone())?;
        let (dispatch, entered) = q.pause_for_test().await;
        let work = pool.clone();
        let observer = tokio::spawn(async move { work.snapshot(Some("owner".into())).await });
        timeout(Duration::from_secs(8), entered).await??;
        advance(&f, 2).await?;
        dispatch.send(()).map_err(|_| "dispatch gone")?;
        let snapshot = timeout(Duration::from_secs(8), observer).await???;
        assert_eq!(snapshot.fact().generation, 2);
        let second = pool.snapshot(Some("owner".into())).await?;
        assert_eq!(second.fact(), snapshot.fact());
        assert_eq!(pin_count(&f).await?, 1);
        assert_eq!(pool.owners_for_test().await.len(), 1);
        drop((snapshot, second));
        finish(&f, &pool, &q, tasks).await?;
        f.runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn busy_eviction_resumes_and_canceled_exclusive_drain_joins_exact_uncertain_release() -> Result
{
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let f = Fixture::new(format).await?;
        let store = Arc::new(ArtifactStore::new(Arc::new(InMemory::new()), f.repository));
        initialize(&f, store.clone()).await?;
        let q = queue(&f)?;
        let root = tempfile::TempDir::new()?;
        let tasks = TaskTracker::new();
        let pool = pooled(&f, store, &root, tasks.clone(), q.clone())?;
        let snapshot = pool.snapshot(Some("owner".into())).await?;
        assert!(!pool.quiesce().await?);
        assert_eq!(snapshot.headers(&[missing(&f)?]).await?, vec![None]);
        drop(snapshot);
        let held = q.try_reserve(
            ReadyServingCommand::acquire(
                f.client(),
                f.target.clone(),
                f.begin([233; 16]),
                identity()?,
                f.authority(),
            )
            .await?,
        )?;
        assert!(!pool.quiesce().await?);
        assert!(!q.stats().await.closed);
        drop(pool.snapshot(Some("owner".into())).await?);
        held.discard_held().await?;
        let (dispatch, entered) = q.pause_for_test().await;
        q.fault_for_test(2);
        let work = pool.clone();
        let observed = tokio::spawn(async move { work.quiesce().await });
        timeout(Duration::from_secs(8), entered).await??;
        observed.abort();
        assert!(observed.await.err().ok_or("drained early")?.is_cancelled());
        assert_eq!(pin_count(&f).await?, 1);
        dispatch.send(()).map_err(|_| "dispatch gone")?;
        timeout(Duration::from_secs(8), pool.close_and_drain()).await?;
        assert_eq!(pin_count(&f).await?, 0);
        assert!(q.stats().await.closed);
        assert!(pool.quiesce().await?);
        assert!(pool.snapshot(Some("owner".into())).await.is_err());
        finish(&f, &pool, &q, tasks).await?;
        f.runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn blocked_old_generation_does_not_block_other_release_or_allow_early_eviction() -> Result {
    use std::sync::atomic::Ordering;
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let f = Fixture::new(format).await?;
        let provider = Arc::new(super::blocked::Gate::new());
        let store = Arc::new(ArtifactStore::new(provider.clone(), f.repository));
        initialize(&f, store.clone()).await?;
        let q = queue(&f)?;
        let root = tempfile::TempDir::new()?;
        let tasks = TaskTracker::new();
        let pool = pooled(&f, store, &root, tasks.clone(), q.clone())?;
        let snapshot = pool.snapshot(Some("owner".into())).await?;
        provider.armed.store(true, Ordering::Release);
        let oid = missing(&f)?;
        let observer = tokio::spawn(async move { snapshot.headers(&[oid]).await });
        timeout(Duration::from_secs(8), provider.entered.acquire())
            .await??
            .forget();
        observer.abort();
        assert!(
            observer
                .await
                .err()
                .ok_or("provider finished")?
                .is_cancelled()
        );
        assert!(!timeout(Duration::from_secs(1), pool.quiesce()).await??);
        advance(&f, 2).await?;
        let other = pool.snapshot(Some("owner".into())).await?;
        let owners = pool.owners_for_test().await;
        let first = owners[0].drain_observer();
        let second = owners[1].drain_observer();
        pool.close();
        drop(other);
        assert_eq!(
            timeout(Duration::from_secs(8), second.wait()).await?.phase,
            ServingOwnerPhase::Released
        );
        assert_eq!(pin_count(&f).await?, 1);
        assert!(
            timeout(Duration::from_millis(50), first.wait())
                .await
                .is_err()
        );
        assert!(root.path().exists());
        provider.proceed.add_permits(1);
        finish(&f, &pool, &q, tasks).await?;
        f.runtime.shutdown().await?;
    }
    Ok(())
}
