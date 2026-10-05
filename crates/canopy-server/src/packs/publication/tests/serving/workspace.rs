//! Native forward closures use physically verified packs. Trusted generation
//! installation isolates serving from the still-incomplete live publisher.
use super::*;

#[tokio::test]
async fn native_write_base_streams_catalog_inputs_and_isolates_new_native_outputs() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let f = Fixture::new(format).await?;
        let (native, _) = super::body::catalog(&f, Arc::new(InMemory::new())).await?;
        let q = super::pool::queue(&f)?;
        let root = tempfile::TempDir::new()?;
        let tasks = TaskTracker::new();
        let (ctx, files) =
            super::body::serving_context(&f, native.store.clone(), &root, tasks.clone())?;
        let pool = ServingPool::new(ctx, q.clone(), ServingPoolLimits::default())?;
        let view = pool.snapshot(Some("owner".into())).await?;
        let backend = view.native_base().await?;
        assert!(backend.cache.pack_sources().await?.is_empty());
        assert_eq!(files.native_stats()?.ok_or("stats")?.open_files, 2);
        let mut objects = crate::git_objects::GitObjects::batch_owned(
            &backend.git_dir(),
            &backend.cache.native,
            backend.cache.clone(),
        )?;
        for (id, (expected, _)) in &native.fixture.objects {
            let body = objects.read_verified(*expected, 1 << 20).await?;
            assert_eq!(crate::object_id(format, expected.kind, &body), *id);
        }
        objects.finish().await?;
        let path = backend.git_dir();
        assert_eq!(std::fs::read_dir(path.join("objects/pack"))?.count(), 0);
        assert!(path.join("objects/info/alternates").exists());
        let body = b"new generated native object";
        let written = crate::packs::metadata::tests::git(
            &path,
            &["hash-object", "-w", "--stdin"],
            Some(body.to_vec()),
        )
        .await?;
        let id = crate::object_id(format, ObjectKind::Blob, body);
        assert_eq!(String::from_utf8(written)?.trim(), hex::encode(id));
        let input = format!("{}\n", hex::encode(id));
        let pack = crate::packs::metadata::tests::git(
            &path,
            &["pack-objects", "--stdout"],
            Some(input.into_bytes()),
        )
        .await?;
        crate::packs::metadata::tests::git(&path, &["index-pack", "--stdin"], Some(pack)).await?;
        let incoming = backend.cache.pack_sources().await?;
        assert_eq!(incoming.len(), 1);
        assert_eq!(incoming[0].3, [id]); // baseline history must never be ingested as the new pack
        drop(view);
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
        drop(backend);
        timeout(Duration::from_secs(8), drain).await??;
        super::pool::finish(&f, &pool, &q, tasks).await?;
        assert!(!path.exists());
        assert_eq!(files.native_stats()?.ok_or("stats")?.open_files, 0);
        f.runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn native_write_base_cancellation_and_revocation_retain_real_provider_work_until_drain()
-> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        for cancel in [false, true] {
            let f = Fixture::new(format).await?;
            let provider = Arc::new(super::blocked::Gate::new());
            let (native, _) = super::body::catalog(&f, provider.clone()).await?;
            edit(
                &f,
                "INSERT INTO repository_members(account,role) VALUES('viewer','read')",
            )
            .await?;
            let q = super::pool::queue(&f)?;
            let root = tempfile::TempDir::new()?;
            let tasks = TaskTracker::new();
            let (ctx, files) =
                super::body::serving_context(&f, native.store.clone(), &root, tasks.clone())?;
            let pool = ServingPool::new(ctx, q.clone(), ServingPoolLimits::default())?;
            let view = pool.snapshot(Some("viewer".into())).await?;
            assert!(
                view.headers(&[*native.fixture.objects.keys().next().ok_or("object")?])
                    .await?[0]
                    .is_some()
            );
            // Warm immutable ref-root metadata so the gate below suspends the
            // native pack transfer after file admission, not ref-root loading.
            view.resolve_ref(None).await?;
            provider.armed.store(true, Ordering::Release);
            let observer = tokio::spawn(async move { view.native_base().await });
            timeout(Duration::from_secs(8), provider.entered.acquire())
                .await??
                .forget();
            if cancel {
                observer.abort();
                assert!(observer.await.err().ok_or("cancelled")?.is_cancelled());
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
                assert_eq!(files.native_stats()?.ok_or("stats")?.open_files, 1);
                provider.proceed.add_permits(1);
                timeout(Duration::from_secs(8), drain).await??;
            } else {
                edit(&f, "DELETE FROM repository_members WHERE account='viewer'").await?;
                assert_eq!(pin_count(&f).await?, 1);
                provider.proceed.add_permits(1);
                assert!(matches!(
                    timeout(Duration::from_secs(8), observer).await??,
                    Err(ServingReadError::Inactive)
                ));
            }
            super::pool::finish(&f, &pool, &q, tasks).await?;
            assert_eq!(files.native_stats()?.ok_or("stats")?.open_files, 0);
            f.runtime.shutdown().await?;
        }
    }
    Ok(())
}
use crate::packs::catalog::serving_fixture::{operation, prepare};
use crate::{ObjectId, ObjectKind};
use std::collections::BTreeSet;
use std::sync::atomic::Ordering;

#[tokio::test]
async fn complete_native_history_crosses_shards_and_wide_parent_pages_without_loose_copies()
-> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let f = Fixture::new(format).await?;
        let provider = Arc::new(InMemory::new());
        // Wide fixtures exercise metadata shards, large trees and >512 parents.
        let fixture = prepare(format, provider.clone(), f.repository, 600)
            .await
            .map_err(|e| e.to_string())?;
        let store = Arc::new(ArtifactStore::new(provider, f.repository));
        initialize(&f, store.clone()).await?;
        f.install_generation(2, fixture.catalog, Some(fixture.refs))
            .await?;
        let q = super::pool::queue(&f)?;
        let root = tempfile::TempDir::new()?;
        let tasks = TaskTracker::new();
        let (ctx, files) = super::body::serving_context(&f, store, &root, tasks.clone())?;
        let pool = ServingPool::new(ctx, q.clone(), ServingPoolLimits::default())?;
        let view = pool.snapshot(Some("owner".into())).await?;
        let wide = fixture.wide.ok_or("wide merge")?;
        let workspace = timeout(
            Duration::from_secs(60),
            view.workspace(&[wide], WorkspaceLimits::default()),
        )
        .await??;
        let mut pending = vec![wide];
        let mut expected = BTreeSet::new();
        while let Some(id) = pending.pop() {
            if expected.insert(id) {
                pending.extend(fixture.edges[&id].iter().map(|e| e.child));
            }
        }
        assert_eq!(workspace.stats().objects, expected.len() as u64);
        assert_eq!(workspace.stats().packs, 1);
        for page in expected.into_iter().collect::<Vec<_>>().chunks(512) {
            assert!(
                workspace
                    .contains(page)
                    .await?
                    .iter()
                    .all(|present| *present)
            );
        }
        assert_eq!(
            workspace
                .contains(&[fixture.tag, fixture.main, missing(&f)?])
                .await?,
            [false, false, false]
        );
        assert!(workspace.body(fixture.tag, 1024).await?.is_none());
        assert_eq!(files.native_stats()?.ok_or("stats")?.open_files, 1);
        let path = workspace.git_dir();
        assert_eq!(std::fs::read_dir(path.join("objects/pack"))?.count(), 2);
        assert_eq!(std::fs::read_dir(path.join("objects"))?.count(), 2); // info + pack, no loose shards
        // A native walk needs all ordered parents, not just the tip pack object.
        let commits = crate::packs::metadata::tests::git(
            &path,
            &["rev-list", "--count", &hex::encode(wide)],
            None,
        )
        .await?;
        assert_eq!(String::from_utf8(commits)?.trim(), "533");
        let body = workspace.body(wide, 64 << 10).await?.ok_or("wide body")?;
        assert_eq!(crate::object_id(format, ObjectKind::Commit, &body), wide);
        drop(view);
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
        drop(workspace);
        timeout(Duration::from_secs(8), drain).await??;
        super::pool::finish(&f, &pool, &q, tasks).await?;
        assert!(!path.exists());
        f.runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn closure_reads_reject_unselected_physical_objects_limits_and_revoked_access() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let f = Fixture::new(format).await?;
        let (native, _) = super::body::catalog(&f, Arc::new(InMemory::new())).await?;
        edit(
            &f,
            "INSERT INTO repository_members(account,role) VALUES('viewer','read')",
        )
        .await?;
        let q = super::pool::queue(&f)?;
        let root = tempfile::TempDir::new()?;
        let tasks = TaskTracker::new();
        let (ctx, _) =
            super::body::serving_context(&f, native.store.clone(), &root, tasks.clone())?;
        let pool = ServingPool::new(ctx, q.clone(), ServingPoolLimits::default())?;
        let view = pool.snapshot(Some("viewer".into())).await?;
        let (id, (expected, _)) = native
            .fixture
            .objects
            .iter()
            .find(|(_, (o, _))| o.kind == ObjectKind::Blob)
            .ok_or("blob")?;
        let workspace = view.workspace(&[*id], WorkspaceLimits::default()).await?;
        assert_eq!(workspace.stats().objects, 1);
        let other = *native
            .fixture
            .objects
            .keys()
            .find(|oid| *oid != id)
            .ok_or("other")?;
        assert_eq!(
            workspace.contains(&[*id, other, *id, missing(&f)?]).await?,
            [true, false, true, false]
        );
        assert_eq!(workspace.body(other, 1 << 20).await?, None);
        assert_eq!(
            workspace.body(*id, 1 << 20).await?,
            view.body(*id, 1 << 20).await?
        );
        assert!(matches!(
            workspace.body(*id, expected.size as usize - 1).await,
            Err(ServingReadError::TooLarge)
        ));
        assert!(matches!(
            workspace.body(*id, 65 << 20).await,
            Err(ServingReadError::TooLarge)
        ));
        let wrong =
            ObjectId::try_from(vec![1; if format == ObjectFormat::Sha1 { 32 } else { 20 }])?;
        assert!(matches!(
            workspace.contains(&[wrong]).await,
            Err(ServingReadError::Context)
        ));
        assert!(matches!(
            view.workspace(&[*id, *id], WorkspaceLimits::default())
                .await,
            Err(ServingReadError::Context)
        ));
        assert!(matches!(
            view.workspace(&[], WorkspaceLimits::default()).await,
            Err(ServingReadError::Context)
        ));
        assert!(matches!(
            view.workspace(
                &[*id],
                WorkspaceLimits {
                    cache_kib: 257,
                    ..WorkspaceLimits::default()
                }
            )
            .await,
            Err(ServingReadError::Context)
        ));
        assert!(matches!(
            view.workspace(&[missing(&f)?], WorkspaceLimits::default())
                .await,
            Err(ServingReadError::Context)
        ));
        edit(&f, "DELETE FROM repository_members WHERE account='viewer'").await?;
        assert!(matches!(
            workspace.contains(&[*id]).await,
            Err(ServingReadError::Inactive)
        ));
        assert!(matches!(
            workspace.body(*id, 1 << 20).await,
            Err(ServingReadError::Inactive)
        ));
        drop((view, workspace));
        super::pool::finish(&f, &pool, &q, tasks).await?;
        f.runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn cancelled_construction_keeps_pin_and_admission_until_suspended_provider_drains() -> Result
{
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let f = Fixture::new(format).await?;
        let provider = Arc::new(super::blocked::Gate::new());
        let (native, _) = super::body::catalog(&f, provider.clone()).await?;
        let q = super::pool::queue(&f)?;
        let root = tempfile::TempDir::new()?;
        let tasks = TaskTracker::new();
        let (ctx, files) =
            super::body::serving_context(&f, native.store.clone(), &root, tasks.clone())?;
        let pool = ServingPool::new(ctx, q.clone(), ServingPoolLimits::default())?;
        let view = pool.snapshot(Some("owner".into())).await?;
        let oid = native
            .fixture
            .objects
            .iter()
            .find(|(_, (o, _))| o.kind == ObjectKind::Commit)
            .ok_or("commit")?
            .0;
        let oid = *oid;
        assert!(view.headers(&[oid]).await?[0].is_some());
        provider.armed.store(true, Ordering::Release);
        let observer =
            tokio::spawn(async move { view.workspace(&[oid], WorkspaceLimits::default()).await });
        timeout(Duration::from_secs(8), provider.entered.acquire())
            .await??
            .forget();
        observer.abort();
        assert!(observer.await.err().ok_or("cancelled")?.is_cancelled());
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
        assert_eq!(files.native_stats()?.ok_or("stats")?.open_files, 1);
        provider.proceed.add_permits(1);
        timeout(Duration::from_secs(8), drain).await??;
        super::pool::finish(&f, &pool, &q, tasks).await?;
        assert_eq!(files.native_stats()?.ok_or("stats")?.open_files, 0);
        f.runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn closed_producer_renews_during_long_construction_and_returned_workspace_lifetime() -> Result
{
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let f = Fixture::new(format).await?;
        let provider = Arc::new(super::blocked::Gate::new());
        let (native, _) = super::body::catalog(&f, provider.clone()).await?;
        let q = super::pool::queue(&f)?;
        let root = tempfile::TempDir::new()?;
        let tasks = TaskTracker::new();
        let (ctx, _) =
            super::body::serving_context(&f, native.store.clone(), &root, tasks.clone())?;
        // Warm immutable metadata before acquiring the short lease. Cold file
        // hashing is covered by the separate construction/cancellation cases.
        let oid = *native.fixture.objects.keys().next().ok_or("object")?;
        let warm =
            ServingOwner::start(ctx.clone(), q.clone(), f.begin([118; 16]), identity()?).await?;
        timeout(Duration::from_secs(8), async {
            while warm.stats().phase != ServingOwnerPhase::Ready {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .map_err(|error| {
            format!(
                "{format:?}: warm owner readiness: {error}; {:?}",
                warm.stats()
            )
        })?;
        let view = warm.snapshot(Some("owner".into())).await?;
        assert!(view.headers(&[oid]).await?[0].is_some());
        drop(view);
        assert_eq!(
            warm.close_and_drain().await.phase,
            ServingOwnerPhase::Released
        );
        let mut input = f.begin([119; 16]);
        input.lease_ms = RENEWAL_LEASE_MS;
        let owner = ServingOwner::start(ctx, q.clone(), input, identity()?).await?;
        timeout(Duration::from_secs(8), async {
            while owner.stats().phase != ServingOwnerPhase::Ready {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .map_err(|error| {
            format!(
                "{format:?}: short-lease readiness: {error}; {:?}",
                owner.stats()
            )
        })?;
        let view = owner.snapshot(Some("owner".into())).await?;
        assert!(
            view.headers(&[oid])
                .await
                .map_err(|e| format!("warm short-lease headers: {e}"))?[0]
                .is_some()
        );
        provider.armed.store(true, Ordering::Release);
        let observer =
            tokio::spawn(async move { view.workspace(&[oid], WorkspaceLimits::default()).await });
        timeout(Duration::from_secs(8), provider.entered.acquire())
            .await
            .map_err(|error| {
                format!(
                    "{format:?}: constructor provider entry: {error}; {:?}",
                    owner.stats()
                )
            })??
            .forget();
        owner.close();
        tokio::time::sleep(Duration::from_millis(RENEWAL_LEASE_MS * 3 / 2)).await;
        timeout(Duration::from_secs(8), async {
            while owner.stats().renewals < 2 {
                assert_eq!(
                    owner.stats().phase,
                    ServingOwnerPhase::Ready,
                    "{:?}",
                    owner.stats()
                );
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .map_err(|error| {
            format!(
                "{format:?}: two closed-owner renewals: {error}; {:?}",
                owner.stats()
            )
        })?;
        assert!(owner.stats().renewals >= 2, "{:?}", owner.stats());
        provider.proceed.add_permits(1);
        let workspace = timeout(Duration::from_secs(8), observer)
            .await
            .map_err(|error| {
                format!(
                    "{format:?}: renewed constructor completion: {error}; {:?}",
                    owner.stats()
                )
            })??
            .map_err(|e| format!("renewed constructor: {e}"))?;
        // Construction's final fresh authority check proves the complete result;
        // short body/membership reads are exercised with their own deadline tests.
        assert!(workspace.stats().objects > 0);
        assert_eq!(pin_count(&f).await?, 1);
        let mut drain = tokio::spawn({
            let owner = owner.clone();
            async move { owner.close_and_drain().await }
        });
        assert!(
            timeout(Duration::from_millis(50), &mut drain)
                .await
                .is_err()
        );
        drop(workspace);
        assert_eq!(
            timeout(Duration::from_secs(8), drain)
                .await
                .map_err(|error| format!(
                    "{format:?}: returned workspace drain: {error}; {:?}",
                    owner.stats()
                ))??
                .phase,
            ServingOwnerPhase::Released
        );
        assert_eq!(pin_count(&f).await?, 0);
        assert!(q.close_and_drain().await.is_empty());
        tasks.close();
        tasks.wait().await;
        f.runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn reachability_stops_at_live_refs_without_scanning_other_history() -> Result {
    use crate::git_gateway::{GatewayError, GitGateway};
    use crate::packs::ref_state::{RefStateSnapshot, RefStateSnapshotRoot};
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let f = Fixture::new(format).await?;
        let provider = Arc::new(InMemory::new());
        let fixture = prepare(format, provider.clone(), f.repository, 600)
            .await
            .map_err(|e| e.to_string())?;
        let store = Arc::new(ArtifactStore::new(provider, f.repository));
        initialize(&f, store.clone()).await?;
        f.install_generation(2, fixture.catalog, Some(fixture.refs))
            .await?;
        let q = super::pool::queue(&f)?;
        let root = tempfile::TempDir::new()?;
        let tasks = TaskTracker::new();
        let (ctx, _) = super::body::serving_context(&f, store.clone(), &root, tasks.clone())?;
        let pool = ServingPool::new(ctx, q.clone(), ServingPoolLimits::default())?;
        let old = pool.snapshot(Some("owner".into())).await?;
        let workspace = old.ref_workspace(WorkspaceLimits::default()).await?;
        assert!(fixture.edges.len() as u64 > workspace.stats().objects + 500);
        let wants = BTreeSet::from([fixture.main, fixture.root, fixture.side, fixture.tree]);
        GitGateway::validate_wants(&workspace, &wants).await?;
        for unrelated in [fixture.tag, fixture.wide.ok_or("wide")?, missing(&f)?] {
            assert!(matches!(
                GitGateway::validate_wants(&workspace, &BTreeSet::from([unrelated])).await,
                Err(GatewayError::UnreachableWant)
            ));
        }
        let refs = crate::packs::metadata::tests::git(
            &workspace.git_dir(),
            &["for-each-ref", "--format=%(refname)"],
            None,
        )
        .await?;
        assert_eq!(
            String::from_utf8(refs)?,
            "refs/heads/main\nrefs/heads/side\n"
        );
        let empty = RefStateSnapshotRoot::upload(
            &store,
            operation(122),
            RefStateSnapshot {
                repository: f.repository,
                format,
                generation: 2,
                default_branch: "refs/heads/main".into(),
                root: None,
            },
        )
        .await?;
        f.install_generation(3, fixture.catalog, Some(empty))
            .await?;
        let current = pool.snapshot(Some("owner".into())).await?;
        let empty_workspace = current.ref_workspace(WorkspaceLimits::default()).await?;
        assert_eq!(empty_workspace.stats().objects, 0);
        assert_eq!(empty_workspace.stats().packs, 0);
        assert!(matches!(
            GitGateway::validate_wants(&empty_workspace, &wants).await,
            Err(GatewayError::UnreachableWant)
        ));
        GitGateway::validate_wants(&workspace, &wants).await?; // admitted old snapshot retains its roots
        drop((old, current, workspace, empty_workspace));
        super::pool::finish(&f, &pool, &q, tasks).await?;
        f.runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn returned_workspace_releases_read_credit_while_retaining_physical_generation() -> Result {
    let f = Fixture::new(ObjectFormat::Sha256).await?;
    let (native, _) = super::body::catalog(&f, Arc::new(InMemory::new())).await?;
    let q = super::pool::queue(&f)?;
    let root = tempfile::TempDir::new()?;
    let tasks = TaskTracker::new();
    let (_, files) = super::body::serving_context(&f, native.store.clone(), &root, tasks.clone())?;
    let ctx = ServingContext::new(
        f.client(),
        f.target.clone(),
        f.authority(),
        Arc::new(CatalogIndexes::new(native.store.clone(), f.format)),
        files,
        ServingReadBudget::new(2, tasks.clone())?,
        "owner".into(),
    )?;
    let pool = ServingPool::new(ctx, q.clone(), ServingPoolLimits::default())?;
    let view = pool.snapshot(Some("owner".into())).await?;
    let oid = *native.fixture.objects.keys().next().ok_or("object")?;
    let workspace = view.workspace(&[oid], WorkspaceLimits::default()).await?;
    assert_eq!(workspace.contains(&[oid]).await?, [true]);
    assert!(workspace.body(oid, 1 << 20).await?.is_some());
    drop(view);
    assert_eq!(workspace.contains(&[oid]).await?, [true]);
    assert_eq!(pin_count(&f).await?, 1);
    drop(workspace);
    super::pool::finish(&f, &pool, &q, tasks).await?;
    f.runtime.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn suspended_construction_refuses_revoked_access_after_real_transfer_finishes() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let f = Fixture::new(format).await?;
        let provider = Arc::new(super::blocked::Gate::new());
        let (native, _) = super::body::catalog(&f, provider.clone()).await?;
        edit(
            &f,
            "INSERT INTO repository_members(account,role) VALUES('viewer','read')",
        )
        .await?;
        let q = super::pool::queue(&f)?;
        let root = tempfile::TempDir::new()?;
        let tasks = TaskTracker::new();
        let (ctx, files) =
            super::body::serving_context(&f, native.store.clone(), &root, tasks.clone())?;
        let pool = ServingPool::new(ctx, q.clone(), ServingPoolLimits::default())?;
        let view = pool.snapshot(Some("viewer".into())).await?;
        let oid = *native.fixture.objects.keys().next().ok_or("object")?;
        assert!(view.headers(&[oid]).await?[0].is_some());
        provider.armed.store(true, Ordering::Release);
        let observer =
            tokio::spawn(async move { view.workspace(&[oid], WorkspaceLimits::default()).await });
        timeout(Duration::from_secs(8), provider.entered.acquire())
            .await??
            .forget();
        edit(&f, "DELETE FROM repository_members WHERE account='viewer'").await?;
        assert_eq!(pin_count(&f).await?, 1);
        provider.proceed.add_permits(1);
        assert!(matches!(
            timeout(Duration::from_secs(8), observer).await??,
            Err(ServingReadError::Inactive)
        ));
        super::pool::finish(&f, &pool, &q, tasks).await?;
        assert_eq!(files.native_stats()?.ok_or("stats")?.open_files, 0);
        f.runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn expired_lease_does_not_resurrect_or_release_suspended_construction() -> Result {
    let f = Fixture::new(ObjectFormat::Sha256).await?;
    let provider = Arc::new(super::blocked::Gate::new());
    let (native, _) = super::body::catalog(&f, provider.clone()).await?;
    let q = super::pool::queue(&f)?;
    let root = tempfile::TempDir::new()?;
    let tasks = TaskTracker::new();
    let (ctx, files) =
        super::body::serving_context(&f, native.store.clone(), &root, tasks.clone())?;
    let mut input = f.begin([123; 16]);
    input.lease_ms = 1000;
    let owner = ServingOwner::start(ctx, q.clone(), input, identity()?).await?;
    timeout(Duration::from_secs(8), async {
        while owner.stats().phase != ServingOwnerPhase::Ready {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    let view = owner.snapshot(Some("owner".into())).await?;
    let oid = *native.fixture.objects.keys().next().ok_or("object")?;
    assert!(view.headers(&[oid]).await?[0].is_some());
    let (dispatch, entered) = q.pause_for_test().await;
    provider.armed.store(true, Ordering::Release);
    let observer =
        tokio::spawn(async move { view.workspace(&[oid], WorkspaceLimits::default()).await });
    timeout(Duration::from_secs(8), provider.entered.acquire())
        .await??
        .forget();
    timeout(Duration::from_secs(8), entered).await??;
    tokio::time::sleep(Duration::from_millis(1100)).await;
    assert_eq!(owner.stats().renewals, 0);
    assert_eq!(pin_count(&f).await?, 1);
    assert_eq!(files.native_stats()?.ok_or("stats")?.open_files, 1);
    provider.proceed.add_permits(1);
    assert!(matches!(
        timeout(Duration::from_secs(8), observer).await??,
        Err(ServingReadError::Inactive)
    ));
    dispatch.send(()).map_err(|_| "dispatcher lost")?;
    assert_eq!(
        timeout(Duration::from_secs(8), owner.close_and_drain())
            .await?
            .phase,
        ServingOwnerPhase::Released
    );
    assert_eq!(pin_count(&f).await?, 0);
    assert!(q.close_and_drain().await.is_empty());
    tasks.close();
    tasks.wait().await;
    f.runtime.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn joint_ref_pages_stream_ten_thousand_names_without_an_object_root_limit() -> Result {
    use crate::packs::ref_state::{
        RefStateRecord, RefStateSnapshot, RefStateSnapshotRoot, RefStateTree,
    };
    let f = Fixture::new(ObjectFormat::Sha256).await?;
    let (native, catalog) = super::body::catalog(&f, Arc::new(InMemory::new())).await?;
    let oid = *native
        .fixture
        .objects
        .iter()
        .find(|(_, (o, _))| o.kind == ObjectKind::Blob)
        .ok_or("blob")?
        .0;
    let refs = RefStateTree::new(native.store.clone(), f.format)
        .build_sorted(
            operation(124),
            (0..10_000).map(|n| {
                RefStateRecord::new(
                    &format!("refs/tags/item-{n:05}"),
                    crate::RefExpectation {
                        oid: Some(oid),
                        version: 1,
                    },
                    f.format,
                )
            }),
        )
        .await?;
    let refs = RefStateSnapshotRoot::upload(
        &native.store,
        operation(125),
        RefStateSnapshot {
            repository: f.repository,
            format: f.format,
            generation: 2,
            default_branch: "refs/heads/main".into(),
            root: refs,
        },
    )
    .await?;
    f.install_generation(3, catalog, Some(refs)).await?;
    let q = super::pool::queue(&f)?;
    let root = tempfile::TempDir::new()?;
    let tasks = TaskTracker::new();
    let (ctx, _) = super::body::serving_context(&f, native.store.clone(), &root, tasks.clone())?;
    let pool = ServingPool::new(ctx, q.clone(), ServingPoolLimits::default())?;
    let view = pool.snapshot(Some("owner".into())).await?;
    let workspace = timeout(
        Duration::from_secs(30),
        view.ref_workspace(WorkspaceLimits::default()),
    )
    .await??;
    assert_eq!(workspace.stats().objects, 1);
    assert_eq!(workspace.stats().packs, 1);
    let names = crate::packs::metadata::tests::git(
        &workspace.git_dir(),
        &["for-each-ref", "--format=%(refname)"],
        None,
    )
    .await?;
    let names = String::from_utf8(names)?;
    assert_eq!(names.lines().count(), 10_000);
    assert_eq!(names.lines().next(), Some("refs/tags/item-00000"));
    assert_eq!(names.lines().last(), Some("refs/tags/item-09999"));
    drop((view, workspace));
    super::pool::finish(&f, &pool, &q, tasks).await?;
    f.runtime.shutdown().await?;
    Ok(())
}
