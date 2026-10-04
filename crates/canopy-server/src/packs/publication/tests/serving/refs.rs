//! Immutable joint refs, bounded pagination and real retained provider work.
use super::pool::{finish, pooled, queue};
use super::*;
use crate::RefExpectation;
use crate::packs::ref_state::{
    RefStateRecord, RefStateSnapshot, RefStateSnapshotRoot, RefStateTree,
};

fn operation(sequence: u64) -> [u8; 16] {
    let mut operation = *b"CANOPY0100000000";
    operation[8..].copy_from_slice(&sequence.to_be_bytes());
    operation
}

async fn install(
    f: &Fixture,
    store: &ArtifactStore,
    base: GenerationFact,
    generation: u64,
    snapshot: RefStateSnapshot,
) -> Result {
    // Trusted root injection isolates reader behavior; it is not native graph
    // closure or evidence that the production publisher accepts these tips.
    let root = RefStateSnapshotRoot::upload(store, operation(1_000 + generation), snapshot).await?;
    f.install_generation(
        generation,
        base.catalog.ok_or("catalog absent")?,
        Some(root),
    )
    .await
}
fn snapshot(f: &Fixture, generation: u64) -> RefStateSnapshot {
    RefStateSnapshot {
        repository: f.repository,
        format: f.format,
        generation,
        default_branch: "refs/heads/main".into(),
        root: None,
    }
}

#[tokio::test]
async fn immutable_ref_pages_preserve_versions_skip_deleted_subtrees_and_recheck_cached_access()
-> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let f = Fixture::new(format).await?;
        let store = Arc::new(ArtifactStore::new(Arc::new(InMemory::new()), f.repository));
        let base = initialize(&f, store.clone()).await?;
        let oid = missing(&f)?;
        let mut records = Vec::new();
        for n in 0..512 {
            records.push(RefStateRecord::new(
                &format!("refs/heads/deleted/{n:04}"),
                RefExpectation {
                    oid: None,
                    version: 2,
                },
                format,
            )?);
        }
        for n in 0..300 {
            records.push(RefStateRecord::new(
                &format!("refs/heads/live/{n:04}"),
                RefExpectation {
                    oid: Some(oid),
                    version: 1,
                },
                format,
            )?);
        }
        let tree = RefStateTree::new(store.clone(), format);
        let mut old = snapshot(&f, 1);
        old.default_branch = "refs/heads/live/0000".into();
        old.root = tree
            .build_sorted(operation(141), records.into_iter().map(Ok))
            .await?;
        install(&f, &store, base, 2, old).await?;
        edit(
            &f,
            "INSERT INTO repository_members(account,role) VALUES('viewer','read')",
        )
        .await?;
        let q = queue(&f)?;
        let root = tempfile::TempDir::new()?;
        let tasks = TaskTracker::new();
        let pool = pooled(&f, store.clone(), &root, tasks.clone(), q.clone())?;
        let view = pool.snapshot(Some("viewer".into())).await?;
        let resolved = view.resolve_ref(None).await?;
        assert_eq!(resolved.generation, 1);
        assert_eq!(resolved.reference, "refs/heads/live/0000");
        assert_eq!(
            resolved.state,
            Some(RefExpectation {
                oid: Some(oid),
                version: 1
            })
        );
        assert!(
            view.resolve_ref(Some("refs/heads/absent"))
                .await?
                .state
                .is_none()
        );
        assert_eq!(
            view.resolve_ref(Some("refs/heads/deleted/0000"))
                .await?
                .state,
            Some(RefExpectation {
                oid: None,
                version: 2
            })
        );
        let first = view.refs_page("", None, true).await?;
        assert_eq!(first.refs.len(), 256);
        assert!(first.has_more);
        assert_eq!(first.refs[0].0, "refs/heads/live/0000");
        let after = &first.refs.last().ok_or("empty first page")?.0;
        let last = view.refs_page(after, Some(first.generation), true).await?;
        assert_eq!(last.refs.len(), 44);
        assert!(!last.has_more);
        assert_eq!(
            last.refs.last().ok_or("empty last page")?.0,
            "refs/heads/live/0299"
        );
        let all = view.refs_page("", None, false).await?;
        assert_eq!(all.refs.len(), 256);
        assert!(all.refs.iter().all(|(_, state)| state.oid.is_none()));
        assert!(matches!(
            view.refs_page(after, Some(0), true).await,
            Err(ServingReadError::Changed)
        ));
        assert!(view.refs_page(after, None, true).await.is_err());
        assert!(
            view.resolve_ref(Some("refs/heads/bad..name"))
                .await
                .is_err()
        );
        let mut new = snapshot(&f, 2);
        new.default_branch = "refs/heads/new".into();
        install(&f, &store, base, 3, new).await?;
        let current = pool.snapshot(Some("owner".into())).await?;
        assert_eq!(current.resolve_ref(None).await?.reference, "refs/heads/new");
        assert_eq!(
            view.resolve_ref(None).await?.reference,
            "refs/heads/live/0000"
        );
        assert!(matches!(
            current.refs_page(after, Some(1), true).await,
            Err(ServingReadError::Changed)
        ));
        edit(&f, "UPDATE ref_generation SET visibility='public'").await?;
        let public = pool.snapshot(None).await?;
        assert_eq!(public.resolve_ref(None).await?.generation, 2);
        edit(&f, "DELETE FROM repository_members WHERE account='viewer'; UPDATE ref_generation SET visibility='private'").await?;
        assert!(view.resolve_ref(None).await.is_err());
        assert!(view.refs_page("", None, true).await.is_err());
        assert!(public.resolve_ref(None).await.is_err());
        drop((view, current, public));
        finish(&f, &pool, &q, tasks).await?;
        f.runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn ref_page_byte_bound_continues_exactly_after_long_names_without_losing_entries() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let f = Fixture::new(format).await?;
        let store = Arc::new(ArtifactStore::new(Arc::new(InMemory::new()), f.repository));
        let base = initialize(&f, store.clone()).await?;
        let oid = missing(&f)?;
        let names: Vec<_> = (0..9)
            .map(|n| format!("refs/heads/{n:02}-{}", "x".repeat(65_000)))
            .collect();
        let records = names
            .iter()
            .map(|name| {
                RefStateRecord::new(
                    name,
                    RefExpectation {
                        oid: Some(oid),
                        version: 1,
                    },
                    format,
                )
            })
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let mut refs = snapshot(&f, 1);
        refs.root = RefStateTree::new(store.clone(), format)
            .build_sorted(operation(142), records.into_iter().map(Ok))
            .await?;
        install(&f, &store, base, 2, refs).await?;
        let q = queue(&f)?;
        let root = tempfile::TempDir::new()?;
        let tasks = TaskTracker::new();
        let pool = pooled(&f, store, &root, tasks.clone(), q.clone())?;
        let view = pool.snapshot(Some("owner".into())).await?;
        let first = view.refs_page("", None, true).await?;
        assert_eq!(first.refs.len(), 8);
        assert!(first.has_more);
        assert!(
            first
                .refs
                .iter()
                .map(|(name, _)| name.len() + 64)
                .sum::<usize>()
                <= 512 * 1024
        );
        let after = &first.refs.last().ok_or("first page")?.0;
        let last = view.refs_page(after, Some(first.generation), true).await?;
        assert_eq!(last.refs.len(), 1);
        assert!(!last.has_more);
        assert_eq!(
            first
                .refs
                .into_iter()
                .chain(last.refs)
                .map(|(name, _)| name)
                .collect::<Vec<_>>(),
            names
        );
        drop(view);
        finish(&f, &pool, &q, tasks).await?;
        f.runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn blocked_ref_download_survives_canceled_observation_and_refuses_early_pin_release() -> Result
{
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
        let view = pool.snapshot(Some("owner".into())).await?;
        provider.armed.store(true, Ordering::Release);
        let observer = tokio::spawn(async move { view.resolve_ref(None).await });
        timeout(Duration::from_secs(8), provider.entered.acquire())
            .await??
            .forget();
        observer.abort();
        assert!(
            observer
                .await
                .err()
                .ok_or("ref observer finished")?
                .is_cancelled()
        );
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
        finish(&f, &pool, &q, tasks).await?;
        f.runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn ref_download_drains_but_never_returns_a_result_after_viewer_revocation() -> Result {
    use std::sync::atomic::Ordering;
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let f = Fixture::new(format).await?;
        let provider = Arc::new(super::blocked::Gate::new());
        let store = Arc::new(ArtifactStore::new(provider.clone(), f.repository));
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
        let view = pool.snapshot(Some("viewer".into())).await?;
        provider.armed.store(true, Ordering::Release);
        let observer = tokio::spawn(async move { view.resolve_ref(None).await });
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
        assert_eq!(
            authorized.resolve_ref(None).await?.reference,
            "refs/heads/main"
        );
        assert!(pool.snapshot(Some("viewer".into())).await.is_err());
        drop(authorized);
        finish(&f, &pool, &q, tasks).await?;
        f.runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn ref_snapshot_context_mismatch_never_falls_back_to_legacy_sql() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let f = Fixture::new(format).await?;
        let store = Arc::new(ArtifactStore::new(Arc::new(InMemory::new()), f.repository));
        let base = initialize(&f, store.clone()).await?;
        let q = queue(&f)?;
        let root = tempfile::TempDir::new()?;
        let tasks = TaskTracker::new();
        let pool = pooled(&f, store.clone(), &root, tasks.clone(), q.clone())?;
        for (generation, other_format, ref_generation) in [
            (2, format, 3),
            (
                3,
                if format == ObjectFormat::Sha1 {
                    ObjectFormat::Sha256
                } else {
                    ObjectFormat::Sha1
                },
                1,
            ),
        ] {
            let mut bad = snapshot(&f, ref_generation);
            bad.format = other_format;
            install(&f, &store, base, generation, bad).await?;
            let view = pool.snapshot(Some("owner".into())).await?;
            assert!(matches!(
                view.resolve_ref(None).await,
                Err(ServingReadError::Context)
            ));
            assert!(matches!(
                view.refs_page("", None, true).await,
                Err(ServingReadError::Context)
            ));
            drop(view);
        }
        finish(&f, &pool, &q, tasks).await?;
        f.runtime.shutdown().await?;
    }
    Ok(())
}
