//! Typed graph reads retain the same owned lifetime as headers and bodies.
use super::*;
use std::sync::atomic::Ordering;

#[tokio::test]
async fn canceled_edge_page_keeps_generation_until_actual_provider_drain() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let f = Fixture::new(format).await?;
        let provider = Arc::new(super::blocked::Gate::new());
        let (native, _) = super::body::catalog(&f, provider.clone()).await?;
        let root = tempfile::TempDir::new()?;
        let tasks = TaskTracker::new();
        let q = super::pool::queue(&f)?;
        let (context, _) =
            super::body::serving_context(&f, native.store.clone(), &root, tasks.clone())?;
        let pool = ServingPool::new(context, q.clone(), ServingPoolLimits::default())?;
        let snapshot = pool.snapshot(Some("owner".into())).await?;
        let id = *native.fixture.objects.keys().next().ok_or("object")?;
        provider.armed.store(true, Ordering::Release);
        let observer = tokio::spawn(async move { snapshot.edges_page(&[id], None).await });
        timeout(Duration::from_secs(8), provider.entered.acquire())
            .await??
            .forget();
        observer.abort();
        assert!(observer.await.err().ok_or("observer")?.is_cancelled());
        assert!(!pool.quiesce().await?);
        let mut drain = tokio::spawn({
            let pool = pool.clone();
            async move { pool.close_and_drain().await }
        });
        assert!(
            timeout(Duration::from_millis(50), &mut drain)
                .await
                .is_err()
        );
        assert_eq!(pin_count(&f).await?, 1);
        provider.proceed.add_permits(1);
        timeout(Duration::from_secs(8), drain).await??;
        super::pool::finish(&f, &pool, &q, tasks).await?;
        f.runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn edge_pages_discard_revoked_in_flight_results_and_recheck_cache_hits() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let f = Fixture::new(format).await?;
        let provider = Arc::new(super::blocked::Gate::new());
        let (native, _) = super::body::catalog(&f, provider.clone()).await?;
        edit(
            &f,
            "INSERT INTO repository_members(account,role) VALUES('viewer','read')",
        )
        .await?;
        let root = tempfile::TempDir::new()?;
        let tasks = TaskTracker::new();
        let q = super::pool::queue(&f)?;
        let (context, _) =
            super::body::serving_context(&f, native.store.clone(), &root, tasks.clone())?;
        let pool = ServingPool::new(context, q.clone(), ServingPoolLimits::default())?;
        let viewer = pool.snapshot(Some("viewer".into())).await?;
        let id = native
            .fixture
            .objects
            .iter()
            .find(|(_, (_, edges))| !edges.is_empty())
            .ok_or("object with edges")?
            .0;
        let id = *id;
        provider.armed.store(true, Ordering::Release);
        let observer = tokio::spawn({
            let viewer = viewer.clone();
            async move { viewer.edges_page(&[id], None).await }
        });
        timeout(Duration::from_secs(8), provider.entered.acquire())
            .await??
            .forget();
        edit(&f, "DELETE FROM repository_members WHERE account='viewer'").await?;
        provider.proceed.add_permits(1);
        assert!(matches!(
            timeout(Duration::from_secs(8), observer).await??,
            Err(ServingReadError::Inactive)
        ));
        let owner = pool.snapshot(Some("owner".into())).await?;
        assert!(!owner.edges_page(&[id], None).await?.edges.is_empty());
        assert!(matches!(
            viewer.edges_page(&[id], None).await,
            Err(ServingReadError::Inactive)
        ));
        drop((owner, viewer));
        super::pool::finish(&f, &pool, &q, tasks).await?;
        f.runtime.shutdown().await?;
    }
    Ok(())
}
