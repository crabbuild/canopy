//! Release the initial floor without losing the exact original command result.
use super::*;
use super::{
    initialization::empty,
    prepare::cleaned,
    publishing::{edit, edit_handle},
    terminal_retention::maintenance,
};
use crate::packs::catalog::CatalogSnapshot;
use canopy_object_storage::artifact::{ArtifactKey, ArtifactKind, ArtifactStore};
use cellule_runtime::Resolution;
use object_store::{ObjectStore, ObjectStoreExt};
use tokio::time::{Duration, timeout};

#[tokio::test]
async fn initialized_repository_releases_zero_floor_and_recovers_original_receipt() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let f = Fixture::new(format).await?;
        let store = Arc::new(ArtifactStore::new(Arc::new(InMemory::new()), f.repository));
        let (prepared, root, budget) = empty(&f, [235; 16], store.clone()).await?;
        let prepared = Arc::new(prepared);
        let ready = prepared.ready_initialization(identity()?).await?;
        let registered = ready.persist_recovery(&store, identity()?).await?;
        let admin = maintenance(&f.handle, f.repository).await?;
        assert!(
            registered
                .ready_terminal_release(f.client(), &store, admin.clone(), identity()?)
                .await
                .is_err()
        );
        let original = ready.complete(&registered, &store).await?;
        let discovered = RegisteredRootRecovery::load_initialization(
            &f.client(),
            &f.target,
            &store,
            &f.begin([235; 16]),
        )
        .await?
        .ok_or("closed initial pin not discovered")?;
        assert_eq!(discovered.evidence(), registered.evidence());
        let result = registered
            .ready_terminal_release(f.client(), &store, admin, identity()?)
            .await?
            .complete()
            .await?;
        assert_eq!(result.output, TerminalReleaseReply::Released);
        f.handle.query(0, 128, |db| {
            assert_eq!(db.query_row("SELECT count(*) FROM catalog_leases", [], |r| r.get::<_,u64>(0))?, 0);
            assert_eq!(db.query_row("SELECT count(*) FROM catalog_initialization", [], |r| r.get::<_,u64>(0))?, 1);
            assert_eq!(db.query_row("SELECT count(*) FROM catalog_recovery_receipts", [], |r| r.get::<_,u64>(0))?, 1);
            for sql in [
                "UPDATE catalog_recovery_receipts SET operation=zeroblob(16)",
                "UPDATE catalog_recovery_receipts SET recovery_phase=x'01'",
                "UPDATE catalog_recovery_receipts SET recovery_release=x'01'",
                "INSERT OR REPLACE INTO catalog_recovery_receipts SELECT * FROM catalog_recovery_receipts",
                "DELETE FROM catalog_recovery_receipts",
            ] {
                assert!(db.execute(sql, []).is_err(), "{sql}");
            }
            Ok(Vec::new())
        }).await?;
        let check = check(registered.token());
        drop(discovered);
        drop(registered);
        drop(prepared);
        cleaned(root.path(), &budget).await?;
        let loaded = RegisteredRootRecovery::load(&f.client(), &f.target, &store, &check)
            .await?
            .ok_or("initial receipt archive missing")?;
        let recovered = loaded
            .recover_initialization(&f.client(), &store, &f.authority())
            .await?;
        assert_eq!(
            (recovered.output, recovered.receipt),
            (original.output, original.receipt)
        );
        let mut wrong = check;
        wrong.token.attempt += 1;
        assert!(
            RegisteredRootRecovery::load(&f.client(), &f.target, &store, &wrong)
                .await?
                .is_none()
        );
        f.runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn initialization_retirement_checks_actual_authority_and_rolls_back_the_last_write() -> Result
{
    let f = Fixture::new(ObjectFormat::Sha256).await?;
    let store = Arc::new(ArtifactStore::new(Arc::new(InMemory::new()), f.repository));
    let (prepared, root, budget) = empty(&f, [236; 16], store.clone()).await?;
    let prepared = Arc::new(prepared);
    let ready = prepared.ready_initialization(identity()?).await?;
    let saved = ready.persist_recovery(&store, identity()?).await?;
    ready.complete(&saved, &store).await?;
    let admin = maintenance(&f.handle, f.repository).await?;
    for wrong_owner in [true, false] {
        let mut wrong = admin.clone();
        if wrong_owner {
            wrong.owner.epoch += 1;
        } else {
            wrong.actor = "outsider".into();
        }
        let release = saved
            .ready_terminal_release(f.client(), &store, wrong, identity()?)
            .await?;
        assert!(
            matches!(release.complete().await, Err(PublicationError::TerminalRelease(InvocationError::Rejected(ref value))) if value.output == TerminalReleaseReply::Denied(PreparationDenial::Unauthorized))
        );
    }
    let release = saved
        .ready_terminal_release(f.client(), &store, admin, identity()?)
        .await?;
    edit_handle(&f.handle, "CREATE TRIGGER initialization_release_late_fault BEFORE DELETE ON catalog_leases WHEN OLD.recovery IS NOT NULL BEGIN SELECT RAISE(ABORT,'late initialization release fault'); END").await?;
    let failed = release.clone().complete().await;
    assert!(
        matches!(failed, Err(PublicationError::TerminalRelease(InvocationError::NotStarted(Error::Sqlite(rusqlite::Error::SqliteFailure(_,Some(ref message)))))) if message == "late initialization release fault"),
        "{failed:?}"
    );
    assert!(matches!(
        f.client().resolve(&release.evidence_for_test()).await?,
        Resolution::Absent
    ));
    f.handle.query(0,128,|db| {
        assert_eq!(db.query_row("SELECT count(*) FROM catalog_recovery_receipts",[],|r| r.get::<_,u64>(0))?,0);
        assert_eq!(db.query_row("SELECT count(*) FROM catalog_leases WHERE recovery IS NOT NULL AND recovery_phase IS NOT NULL",[],|r| r.get::<_,u64>(0))?,1);
        Ok(Vec::new())
    }).await?;
    edit_handle(&f.handle, "DROP TRIGGER initialization_release_late_fault").await?;
    assert_eq!(
        release.complete().await?.output,
        TerminalReleaseReply::Released
    );
    drop(prepared);
    cleaned(root.path(), &budget).await?;
    f.runtime.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn missing_typed_initial_metadata_cannot_authorize_retirement() -> Result {
    for missing in 0..3 {
        let f = Fixture::new(ObjectFormat::Sha256).await?;
        let provider = Arc::new(InMemory::new());
        let store = Arc::new(ArtifactStore::new(provider.clone(), f.repository));
        let (prepared, root, budget) = empty(&f, [237; 16], store.clone()).await?;
        let prepared = Arc::new(prepared);
        let ready = prepared.ready_initialization(identity()?).await?;
        let saved = ready.persist_recovery(&store, identity()?).await?;
        let original = ready.complete(&saved, &store).await?;
        let InitializationReply::Initialized(fact) = &original.output else {
            return Err("initialization reply".into());
        };
        let catalog = fact.catalog.ok_or("catalog absent")?;
        let directory = CatalogSnapshot::download(&store, catalog).await?.directory;
        let refs = fact.refs.ok_or("refs absent")?;
        let (operation, kind, artifact) = match missing {
            0 => (
                catalog.operation,
                ArtifactKind::CatalogNode,
                catalog.artifact,
            ),
            1 => (
                directory.operation,
                ArtifactKind::CatalogNode,
                directory.artifact,
            ),
            _ => (refs.operation(), ArtifactKind::InputRoot, refs.artifact()),
        };
        let path = store.path(
            ArtifactKey {
                operation,
                binding_digest: artifact.digest,
                kind,
            },
            artifact.digest,
        )?;
        provider.head(&path).await?;
        provider.delete(&path).await?;
        assert!(
            saved
                .ready_terminal_release(
                    f.client(),
                    &store,
                    maintenance(&f.handle, f.repository).await?,
                    identity()?
                )
                .await
                .is_err()
        );
        assert_eq!(
            saved
                .recover_initialization(&f.client(), &store, &f.authority(),)
                .await?
                .receipt,
            original.receipt
        );
        f.handle
            .query(0, 128, |db| {
                assert_eq!(
                    db.query_row(
                        "SELECT count(*) FROM catalog_leases WHERE recovery IS NOT NULL",
                        [],
                        |r| r.get::<_, u64>(0)
                    )?,
                    1
                );
                assert_eq!(
                    db.query_row("SELECT count(*) FROM catalog_recovery_receipts", [], |r| {
                        r.get::<_, u64>(0)
                    })?,
                    0
                );
                Ok(Vec::new())
            })
            .await?;
        drop(prepared);
        cleaned(root.path(), &budget).await?;
        f.runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn denied_initial_attempt_retires_only_after_claim_and_keeps_its_receipt_after_success()
-> Result {
    let f = Fixture::new(ObjectFormat::Sha1).await?;
    let store = Arc::new(ArtifactStore::new(Arc::new(InMemory::new()), f.repository));
    let operation = [238; 16];
    let (prepared, root, budget) = empty(&f, operation, store.clone()).await?;
    let prepared = Arc::new(prepared);
    let ready = prepared.ready_initialization(identity()?).await?;
    let saved = ready.persist_recovery(&store, identity()?).await?;
    let old = check(saved.token());
    edit(
        &f,
        "UPDATE catalog_operations SET expires_at_ms=0; UPDATE catalog_leases SET expires_at_ms=0",
    )
    .await?;
    let denied = match saved
        .recover_initialization(&f.client(), &store, &f.authority())
        .await
    {
        Err(PublicationError::Initialization(InvocationError::Rejected(value))) => value,
        other => return Err(format!("expected original expiry: {other:?}").into()),
    };
    assert_eq!(
        denied.output,
        InitializationReply::Denied(PreparationDenial::Expired)
    );
    let admin = maintenance(&f.handle, f.repository).await?;
    assert!(
        saved
            .ready_terminal_release(f.client(), &store, admin.clone(), identity()?)
            .await
            .is_err()
    );
    let queue = PublicationCoordinator::new(
        f.target.clone(),
        PublicationLimits::default(),
        f.publication_budget.clone(),
    )?;
    let supervisor = RecoverySupervisor::start_retiring(
        f.client(),
        f.target.clone(),
        (*store).clone(),
        queue.clone(),
        RecoveryScanLimits {
            page: 1,
            interval: Duration::from_secs(1),
        },
        f.authority(),
        admin.clone(),
    )?;
    timeout(Duration::from_secs(10), async {
        while supervisor.stats().scanned == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    let scan = supervisor.shutdown().await?;
    assert_eq!(scan.release_submitted, 0);
    assert!(scan.deferred > 0);
    assert_eq!(queue.stats().await.admitted, 0);
    f.client()
        .command::<ClaimPreparation>(
            &f.target,
            identity()?,
            LeaseRequest {
                check: old.clone(),
                lease_ms: DEFAULT_LEASE_MS,
            },
        )
        .await?;
    let release = saved
        .ready_terminal_release(f.client(), &store, admin.clone(), identity()?)
        .await?;
    release.complete().await?;
    drop(ready);
    drop(prepared);
    cleaned(root.path(), &budget).await?;
    let (prepared, root, budget) = empty(&f, operation, store.clone()).await?;
    let prepared = Arc::new(prepared);
    let ready = prepared.ready_initialization(identity()?).await?;
    let current = ready.persist_recovery(&store, identity()?).await?;
    assert_ne!(
        current.token().artifact_operation,
        saved.token().artifact_operation
    );
    assert_eq!(
        RegisteredRootRecovery::load_initialization(
            &f.client(),
            &f.target,
            &store,
            &f.begin(operation)
        )
        .await?
        .ok_or("new attempt absent")?
        .evidence(),
        current.evidence()
    );
    ready.complete(&current, &store).await?;
    current
        .ready_terminal_release(f.client(), &store, admin, identity()?)
        .await?
        .complete()
        .await?;
    let old = RegisteredRootRecovery::load(&f.client(), &f.target, &store, &old)
        .await?
        .ok_or("old denied archive absent")?;
    assert!(matches!(old.recover_initialization(&f.client(),
&store,
&f.authority(),).await, Err(PublicationError::Initialization(InvocationError::Rejected(ref value))) if value.output == denied.output && value.receipt == denied.receipt));
    f.handle
        .query(0, 128, |db| {
            assert_eq!(
                db.query_row("SELECT count(*) FROM catalog_recovery_receipts", [], |r| {
                    r.get::<_, u64>(0)
                })?,
                2
            );
            assert_eq!(
                db.query_row("SELECT count(*) FROM catalog_leases", [], |r| r
                    .get::<_, u64>(0))?,
                0
            );
            Ok(Vec::new())
        })
        .await?;
    assert!(queue.close_and_drain().await.is_empty());
    drop(prepared);
    cleaned(root.path(), &budget).await?;
    f.runtime.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn lost_initial_retirement_ack_keeps_original_receipts_after_expiry_body_loss_and_restore()
-> Result {
    let f = Fixture::new(ObjectFormat::Sha256).await?;
    let provider: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let store = Arc::new(ArtifactStore::new(provider.clone(), f.repository));
    let (prepared, root, budget) = empty(&f, [239; 16], store.clone()).await?;
    let prepared = Arc::new(prepared);
    let mut mutation = identity()?;
    mutation.expires_at_ms = mutation.issued_at_ms + 8_000;
    let ready = prepared.ready_initialization(mutation).await?;
    let saved = ready.persist_recovery(&store, identity()?).await?;
    let original = ready.complete(&saved, &store).await?;
    let check = check(saved.token());
    let mut release_identity = identity()?;
    release_identity.expires_at_ms = release_identity.issued_at_ms + 8_000;
    let release = saved
        .ready_terminal_release(
            f.client(),
            &store,
            maintenance(&f.handle, f.repository).await?,
            release_identity,
        )
        .await?;
    let rival = saved
        .ready_terminal_release(
            f.client(),
            &store,
            maintenance(&f.handle, f.repository).await?,
            identity()?,
        )
        .await?;
    let evidence = release.evidence_for_test();
    assert!(matches!(
        release.clone().dispatch(false, 2).await,
        Err(PublicationError::TerminalRelease(InvocationError::Pending(
            _
        )))
    ));
    let released = release.clone().complete().await?;
    assert!(
        matches!(rival.complete().await, Err(PublicationError::TerminalRelease(InvocationError::Rejected(ref value))) if value.output == TerminalReleaseReply::Denied(PreparationDenial::Missing))
    );
    for (key, descriptor) in saved.command_bodies_for_test() {
        let path = store.path(key, descriptor.digest)?;
        provider.head(&path).await?;
        provider.delete(&path).await?;
    }
    drop(prepared);
    cleaned(root.path(), &budget).await?;
    edit(&f, "UPDATE repository_identity SET owner='replacement'").await?;
    let (runtime, handle, client) = super::durable_recovery::restore_owner(&f, &check).await?;
    assert_ne!(handle.owner_fence(), check.token.owner);
    loop {
        let now = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis())?;
        if now > release_identity.expires_at_ms.max(mutation.expires_at_ms) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(u64::try_from(
            release_identity.expires_at_ms.max(mutation.expires_at_ms) - now + 1,
        )?))
        .await;
    }
    assert!(matches!(
        client.resolve(&evidence).await?,
        Resolution::Expired
    ));
    let restored = RegisteredRootRecovery::load(&client, &f.target, &store, &check)
        .await?
        .ok_or("restored archive absent")?;
    let result = restored
        .recover_initialization(&client, &store, &f.authority())
        .await?;
    assert_eq!(
        (result.output, result.receipt),
        (original.output, original.receipt)
    );
    let result = release.with_client_for_test(client).complete().await?;
    assert_eq!(
        (result.output, result.receipt),
        (released.output, released.receipt)
    );
    runtime.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn automatic_initialization_retirement_recovers_uncertainty_after_pin_disappears() -> Result {
    let f = Fixture::new(ObjectFormat::Sha256).await?;
    let store = Arc::new(ArtifactStore::new(Arc::new(InMemory::new()), f.repository));
    let (prepared, root, budget) = empty(&f, [240; 16], store.clone()).await?;
    let prepared = Arc::new(prepared);
    let ready = prepared.ready_initialization(identity()?).await?;
    let saved = ready.persist_recovery(&store, identity()?).await?;
    ready.complete(&saved, &store).await?;
    let queue = PublicationCoordinator::new(
        f.target.clone(),
        PublicationLimits::default(),
        f.publication_budget.clone(),
    )?;
    queue.fault_for_test(2);
    let scanner = RecoverySupervisor::start_retiring(
        f.client(),
        f.target.clone(),
        (*store).clone(),
        queue.clone(),
        RecoveryScanLimits {
            page: 1,
            interval: Duration::from_secs(1),
        },
        f.authority(),
        maintenance(&f.handle, f.repository).await?,
    )?;
    let observer = timeout(Duration::from_secs(10), async {
        loop {
            if let Some(observer) = queue.pending(saved.token().operation).await {
                break observer;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    assert!(matches!(
        timeout(Duration::from_secs(10), observer.wait()).await?,
        PublicationState::Uncertain(_)
    ));
    let scan = scanner.shutdown().await?;
    assert_eq!(scan.release_submitted, 1);
    assert_eq!(queue.stats().await.admitted, 1);
    f.handle
        .query(0, 128, |db| {
            assert_eq!(
                db.query_row("SELECT count(*) FROM catalog_leases", [], |r| r
                    .get::<_, u64>(0))?,
                0
            );
            Ok(Vec::new())
        })
        .await?;
    assert_eq!(queue.close_and_drain().await.len(), 1);
    let scanner = RecoverySupervisor::start_retiring(
        f.client(),
        f.target.clone(),
        (*store).clone(),
        queue.clone(),
        RecoveryScanLimits {
            page: 1,
            interval: Duration::from_secs(1),
        },
        f.authority(),
        maintenance(&f.handle, f.repository).await?,
    )?;
    timeout(Duration::from_secs(10), async {
        while scanner.stats().release_recovered == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await?;
    assert!(
        matches!(timeout(Duration::from_secs(10), observer.wait()).await?, PublicationState::Finished(Ok(PublicationOutcome::TerminalRelease(ref value))) if value.output==TerminalReleaseReply::Released)
    );
    let scan = scanner.shutdown().await?;
    assert_eq!(scan.release_submitted, 0);
    assert_eq!(queue.stats().await.admitted, 0);
    assert!(queue.close_and_drain().await.is_empty());
    drop(prepared);
    cleaned(root.path(), &budget).await?;
    f.runtime.shutdown().await?;
    Ok(())
}
