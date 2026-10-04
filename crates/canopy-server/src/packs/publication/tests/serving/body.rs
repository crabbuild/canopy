//! Actual native packs/metadata with trusted catalog installation to isolate
//! serving semantics. This is not end-to-end producer publication qualification.
use super::*;
use crate::packs::{
    catalog::{CatalogSnapshot, StoredCatalog},
    metadata::tests::limits,
    verification::physical::tests::{Prepared, prepared_for_store},
};
use object_store::ObjectStore;

pub(super) async fn catalog(
    f: &Fixture,
    provider: Arc<dyn ObjectStore>,
) -> Result<(Prepared, StoredCatalog)> {
    let store = Arc::new(ArtifactStore::new(provider.clone(), f.repository));
    let (base, _, _) = super::super::prepare::opened(f, [71; 16], store.clone()).await?;
    let native =
        prepared_for_store(f.format, 32, base.context().operation, provider, store).await?;
    let root = tempfile::TempDir::new()?;
    let budget = DiskBudget::new(64 << 20);
    let mut builder = CatalogPreparation::new(root.path(), budget.clone(), base, limits()).await?;
    let (witness, segments) =
        super::super::prepare::physical(&native, root.path(), budget.clone()).await?;
    builder.begin_pack(witness)?;
    for segment in segments {
        builder.add_segment(segment).await?;
    }
    builder.finish_pack().await?;
    let prepared = builder.finish().await?;
    let stored = prepared.catalog();
    drop(prepared);
    super::super::prepare::cleaned(root.path(), &budget).await?;
    let fact = initialize(f, native.store.clone()).await?;
    f.install_generation(2, stored, fact.refs).await?;
    Ok((native, stored))
}
pub(super) fn serving_context(
    f: &Fixture,
    store: Arc<ArtifactStore>,
    root: &tempfile::TempDir,
    tasks: TaskTracker,
) -> Result<(ServingContext, Arc<CatalogFiles>)> {
    let files = CatalogFiles::new(
        root.path(),
        DiskBudget::new(64 << 20),
        store.clone(),
        f.format,
        CatalogFileLimits::default(),
    )?
    .with_native(
        crate::native_resources::NativeResources::default()
            .scope(crate::native_resources::NativeClass::Foreground),
    );
    let files = Arc::new(files);
    Ok((
        ServingContext::new(
            f.client(),
            f.target.clone(),
            f.authority(),
            Arc::new(CatalogIndexes::new(store, f.format)),
            files.clone(),
            ServingReadBudget::new(32, tasks)?,
            "owner".into(),
        )?,
        files,
    ))
}

#[tokio::test]
async fn certified_bodies_share_one_pack_across_parallel_readers_and_recheck_cached_access()
-> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let f = Fixture::new(format).await?;
        let (native, _) = catalog(&f, Arc::new(InMemory::new())).await?;
        edit(
            &f,
            "INSERT INTO repository_members(account,role) VALUES('viewer','read')",
        )
        .await?;
        let q = super::pool::queue(&f)?;
        let root = tempfile::TempDir::new()?;
        let tasks = TaskTracker::new();
        let (ctx, files) = serving_context(&f, native.store.clone(), &root, tasks.clone())?;
        let pool = ServingPool::new(ctx, q.clone(), ServingPoolLimits::default())?;
        let view = pool.snapshot(Some("viewer".into())).await?;
        let ids: Vec<_> = native.fixture.objects.keys().copied().collect();
        let results =
            futures_util::future::join_all(ids.iter().take(12).map(|oid| view.body(*oid, 1 << 20)))
                .await;
        for (oid, body) in ids.iter().zip(results) {
            let body = body?.ok_or("body missing")?;
            let expected = native.fixture.objects[oid].0;
            assert_eq!(crate::object_id(format, expected.kind, &body), *oid);
            assert_eq!(blake3::hash(&body).as_bytes(), &expected.digest);
        }
        for (oid, (expected, _)) in &native.fixture.objects {
            let body = view.body(*oid, 1 << 20).await?.ok_or("body missing")?;
            assert_eq!(body.len() as u64, expected.size);
        }
        let stats = files.native_stats()?.ok_or("native stats")?;
        assert_eq!(stats.downloaded_files, 1);
        assert_eq!(stats.open_files, 1);
        assert_eq!(stats.cached_files, 1);
        assert!(stats.cache_hits >= 12);
        assert_eq!(view.body(missing(&f)?, 1024).await?, None);
        let blob = native
            .fixture
            .objects
            .values()
            .find(|(o, _)| o.kind == crate::ObjectKind::Blob)
            .ok_or("blob")?
            .0;
        assert!(matches!(
            view.body(blob.oid, 1).await,
            Err(ServingReadError::TooLarge)
        ));
        assert!(matches!(
            view.body(blob.oid, 65 << 20).await,
            Err(ServingReadError::TooLarge)
        ));
        assert_eq!(
            files
                .native_stats()?
                .ok_or("native stats")?
                .downloaded_files,
            1
        );
        edit(&f, "DELETE FROM repository_members WHERE account='viewer'").await?;
        assert!(matches!(
            view.body(blob.oid, 1024).await,
            Err(ServingReadError::Inactive)
        ));
        drop(view);
        super::pool::finish(&f, &pool, &q, tasks).await?;
        f.runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn cached_pack_never_exposes_objects_absent_from_the_selected_catalog() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let f = Fixture::new(format).await?;
        let (native, _) = catalog(&f, Arc::new(InMemory::new())).await?;
        let q = super::pool::queue(&f)?;
        let root = tempfile::TempDir::new()?;
        let tasks = TaskTracker::new();
        let (ctx, _) = serving_context(&f, native.store.clone(), &root, tasks.clone())?;
        let pool = ServingPool::new(ctx, q.clone(), ServingPoolLimits::default())?;
        let old = pool.snapshot(Some("owner".into())).await?;
        let oid = *native.fixture.objects.keys().next().ok_or("object")?;
        assert!(old.body(oid, 1 << 20).await?.is_some());
        let directory =
            crate::packs::directory::snapshot::DirectorySnapshot::empty(f.repository, format)
                .upload(&native.store, [181; 16])
                .await?;
        let empty = CatalogSnapshot {
            directory,
            sources: None,
        }
        .upload(&native.store, [182; 16])
        .await?;
        f.install_generation(3, empty, old.fact().refs).await?;
        let current = pool.snapshot(Some("owner".into())).await?;
        assert_eq!(current.body(oid, 1 << 20).await?, None);
        assert!(old.body(oid, 1 << 20).await?.is_some());
        drop((old, current));
        super::pool::finish(&f, &pool, &q, tasks).await?;
        f.runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn blocked_native_pack_download_retains_pin_after_observer_cancellation() -> Result {
    use std::sync::atomic::Ordering;
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let f = Fixture::new(format).await?;
        let provider = Arc::new(super::blocked::Gate::new());
        let (native, _) = catalog(&f, provider.clone()).await?;
        let q = super::pool::queue(&f)?;
        let root = tempfile::TempDir::new()?;
        let tasks = TaskTracker::new();
        let (ctx, _) = serving_context(&f, native.store.clone(), &root, tasks.clone())?;
        let pool = ServingPool::new(ctx, q.clone(), ServingPoolLimits::default())?;
        let view = pool.snapshot(Some("owner".into())).await?;
        let oid = *native.fixture.objects.keys().next().ok_or("object")?;
        // Warm all metadata; the armed provider suspension is actual pack I/O.
        assert!(view.headers(&[oid]).await?[0].is_some());
        provider.armed.store(true, Ordering::Release);
        let observer = tokio::spawn(async move { view.body(oid, 1 << 20).await });
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
async fn native_download_drain_finishes_but_body_is_refused_after_access_revocation() -> Result {
    use std::sync::atomic::Ordering;
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let f = Fixture::new(format).await?;
        let provider = Arc::new(super::blocked::Gate::new());
        let (native, _) = catalog(&f, provider.clone()).await?;
        edit(
            &f,
            "INSERT INTO repository_members(account,role) VALUES('viewer','read')",
        )
        .await?;
        let q = super::pool::queue(&f)?;
        let root = tempfile::TempDir::new()?;
        let tasks = TaskTracker::new();
        let (ctx, files) = serving_context(&f, native.store.clone(), &root, tasks.clone())?;
        let pool = ServingPool::new(ctx, q.clone(), ServingPoolLimits::default())?;
        let view = pool.snapshot(Some("viewer".into())).await?;
        let oid = *native.fixture.objects.keys().next().ok_or("object")?;
        assert!(view.headers(&[oid]).await?[0].is_some());
        provider.armed.store(true, Ordering::Release);
        let observer = tokio::spawn(async move { view.body(oid, 1 << 20).await });
        timeout(Duration::from_secs(8), provider.entered.acquire())
            .await??
            .forget();
        edit(&f, "DELETE FROM repository_members WHERE account='viewer'").await?;
        provider.proceed.add_permits(1);
        assert!(matches!(
            timeout(Duration::from_secs(8), observer).await??,
            Err(ServingReadError::Inactive)
        ));
        let authorized = pool.snapshot(Some("owner".into())).await?;
        assert!(authorized.body(oid, 1 << 20).await?.is_some());
        assert_eq!(
            files
                .native_stats()?
                .ok_or("native stats")?
                .downloaded_files,
            1
        );
        drop(authorized);
        super::pool::finish(&f, &pool, &q, tasks).await?;
        f.runtime.shutdown().await?;
    }
    Ok(())
}
