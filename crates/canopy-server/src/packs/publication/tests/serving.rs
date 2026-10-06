//! Serving pins protect real immutable roots through worker and receipt loss.
use super::*;
use super::{initialization::empty, publishing::edit};
use crate::packs::catalog::{CatalogFileLimits, CatalogFiles, CatalogIndexes};
use canopy_object_storage::artifact::ArtifactStore;
use cellule_ltx::DiskBudget;
use cellule_runtime::{Committed, PreparedCommand};
use tokio::time::{Duration, timeout};
use tokio_util::task::TaskTracker;
mod body;
mod custody;
mod edges;
mod lifecycle;
mod pool;
mod refs;
mod selection_drain;
mod workspace;

// Allow real SQL/provider callbacks under concurrent load. Renewal cases
// retain borrowers beyond this initial lease; expiry cases use their own clocks.
const RENEWAL_LEASE_MS: u64 = 5_000;

async fn initialize(f: &Fixture, store: Arc<ArtifactStore>) -> Result<GenerationFact> {
    let (prepared, root, budget) = Box::pin(empty(f, [241; 16], store.clone())).await?;
    let prepared = Arc::new(prepared);
    let ready = prepared.ready_initialization(identity()?).await?;
    let registered = ready.persist_recovery(&store, identity()?).await?;
    let InitializationReply::Initialized(fact) = ready.complete(&registered, &store).await?.output
    else {
        return Err("initialization not committed".into());
    };
    let admin = super::terminal_retention::maintenance(&f.handle, f.repository).await?;
    assert_eq!(
        registered
            .ready_terminal_release(f.client(), &store, admin, identity()?)
            .await?
            .complete()
            .await?
            .output,
        TerminalReleaseReply::Released
    );
    drop(prepared);
    super::prepare::cleaned(root.path(), &budget).await?;
    Ok(*fact)
}
fn request(f: &Fixture, actor: Option<&str>, reader: u8, lease_ms: u64) -> AcquireServingRequest {
    AcquireServingRequest {
        repository: f.repository,
        reader: [reader; 16],
        actor: actor.map(str::to_owned),
        lease_ms,
    }
}
fn granted(reply: ServingReply) -> Result<ServingLease> {
    match reply {
        ServingReply::Granted(lease) => Ok(*lease),
        other => Err(format!("unexpected {other:?}").into()),
    }
}
async fn acquire(
    f: &Fixture,
    actor: Option<&str>,
    reader: u8,
    lease_ms: u64,
) -> Result<(
    ServingLease,
    PreparedCommand<AcquireServingPin>,
    Committed<ServingReply>,
)> {
    let command = f
        .client()
        .prepare_command::<AcquireServingPin>(
            &f.target,
            identity()?,
            request(f, actor, reader, lease_ms),
        )
        .await?;
    let committed = command.clone().execute().await?;
    Ok((granted(committed.output.clone())?, command, committed))
}
async fn pin_count(f: &Fixture) -> Result<u64> {
    let bytes = f
        .handle
        .query(0, 8, |db| {
            let count: u64 =
                db.query_row("SELECT count(*) FROM catalog_serving_pins", [], |row| {
                    row.get(0)
                })?;
            Ok(count.to_be_bytes().to_vec())
        })
        .await?;
    Ok(u64::from_be_bytes(bytes.as_slice().try_into()?))
}
fn context(
    f: &Fixture,
    store: Arc<ArtifactStore>,
    root: &tempfile::TempDir,
    tasks: TaskTracker,
) -> Result<ServingContext> {
    Ok(ServingContext::new(
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
        ServingReadBudget::new(4, tasks)?,
        "owner".into(),
    )?)
}
fn missing(f: &Fixture) -> Result<crate::ObjectId> {
    Ok(match f.format {
        ObjectFormat::Sha1 => crate::ObjectId::Sha1([7; 20]),
        ObjectFormat::Sha256 => crate::ObjectId::Sha256([7; 32]),
    })
}
async fn release(f: &Fixture, pin: &ServingPin) -> Result<Committed<ServingReleaseReply>> {
    let ready = pin.ready_release(identity()?).await?;
    let coordinator = PublicationCoordinator::new(
        f.target.clone(),
        PublicationLimits::default(),
        f.publication_budget.clone(),
    )?;
    let ticket = coordinator.submit(ready).await?;
    let state = timeout(Duration::from_secs(5), ticket.wait()).await?;
    let PublicationState::Finished(Ok(PublicationOutcome::ServingRelease(result))) = state else {
        return Err(format!("unexpected serving release {state:?}").into());
    };
    assert!(coordinator.close_and_drain().await.is_empty());
    Ok(result)
}

#[tokio::test]
async fn grants_require_read_and_joint_initialization_without_allocating_namespaces() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let f = Fixture::new(format).await?;
        for (actor, reason) in [
            (Some("owner"), ServingDenial::Uninitialized),
            (None, ServingDenial::Unauthorized),
            (Some("other"), ServingDenial::Unauthorized),
        ] {
            let result = f
                .client()
                .command::<AcquireServingPin>(
                    &f.target,
                    identity()?,
                    request(&f, actor, 242, DEFAULT_LEASE_MS),
                )
                .await;
            assert!(
                matches!(result, Err(InvocationError::Rejected(value)) if value.output==ServingReply::Denied(reason))
            );
        }
        let store = Arc::new(ArtifactStore::new(Arc::new(InMemory::new()), f.repository));
        let fact = initialize(&f, store.clone()).await?;
        edit(
            &f,
            "INSERT INTO repository_members(account,role) VALUES('viewer','read')",
        )
        .await?;
        let before = f.counts().await?;
        let (lease, original, receipt) = acquire(&f, Some("viewer"), 243, DEFAULT_LEASE_MS).await?;
        assert_eq!(lease.fact, fact);
        assert_eq!(lease.token.owner, f.handle.owner_fence());
        assert_eq!(f.counts().await?, before);
        assert_eq!(pin_count(&f).await?, 1);
        let duplicate = f
            .client()
            .command::<AcquireServingPin>(
                &f.target,
                identity()?,
                request(&f, Some("viewer"), 243, DEFAULT_LEASE_MS),
            )
            .await;
        assert!(
            matches!(duplicate, Err(InvocationError::Rejected(value)) if value.output==ServingReply::Denied(ServingDenial::Conflict))
        );
        let root = tempfile::TempDir::new()?;
        let tasks = TaskTracker::new();
        let pin = ServingPin::open(
            context(&f, store.clone(), &root, tasks.clone())?,
            lease.token,
            Some("viewer".into()),
        )
        .await?;
        assert_eq!(
            pin.headers(Some("viewer".into()), &[missing(&f)?]).await?,
            vec![None]
        );
        assert_eq!(
            release(&f, &pin).await?.output,
            ServingReleaseReply::Released
        );
        assert_eq!(pin_count(&f).await?, 0);
        assert!(pin.ready_release(identity()?).await.is_err());
        // SDK replay is immutable evidence, not a fresh serving capability.
        let replay = original.execute().await?;
        assert_eq!(
            (replay.output, replay.receipt),
            (receipt.output, receipt.receipt)
        );
        assert!(
            ServingPin::open(
                context(&f, store, &root, tasks.clone())?,
                lease.token,
                Some("viewer".into())
            )
            .await
            .is_err()
        );
        tasks.close();
        timeout(Duration::from_secs(5), tasks.wait()).await?;
        f.runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn anonymous_public_reads_revocation_and_token_scope_are_rechecked() -> Result {
    let f = Fixture::new(ObjectFormat::Sha256).await?;
    let store = Arc::new(ArtifactStore::new(Arc::new(InMemory::new()), f.repository));
    initialize(&f, store.clone()).await?;
    edit(&f, "UPDATE ref_generation SET visibility='public'").await?;
    let (lease, _, _) = acquire(&f, None, 244, DEFAULT_LEASE_MS).await?;
    let root = tempfile::TempDir::new()?;
    let tasks = TaskTracker::new();
    let pin =
        ServingPin::open(context(&f, store, &root, tasks.clone())?, lease.token, None).await?;
    assert_eq!(pin.headers(None, &[missing(&f)?]).await?, vec![None]);
    for field in 0..5 {
        let mut token = lease.token;
        match field {
            0 => token.repository[15] ^= 1,
            1 => token.reader[15] ^= 1,
            2 => token.owner.epoch += 1,
            3 => token.admission_sequence += 1,
            _ => token.generation += 1,
        }
        assert!(
            f.client()
                .query::<CheckServingPin>(&f.target, None, ServingCheck { token, actor: None })
                .await?
                .output
                .is_none()
        );
    }
    edit(&f, "UPDATE ref_generation SET visibility='private'").await?;
    assert!(matches!(
        pin.headers(None, &[missing(&f)?]).await,
        Err(ServingReadError::Inactive)
    ));
    assert_eq!(pin_count(&f).await?, 1);
    assert_eq!(
        release(&f, &pin).await?.output,
        ServingReleaseReply::Released
    );
    tasks.close();
    tasks.wait().await;
    f.runtime.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn expiry_refuses_reads_and_renewal_but_retains_generation_until_drained_release() -> Result {
    let f = Fixture::new(ObjectFormat::Sha1).await?;
    let store = Arc::new(ArtifactStore::new(Arc::new(InMemory::new()), f.repository));
    let fact = initialize(&f, store.clone()).await?;
    let (lease, _, _) = acquire(&f, Some("owner"), 245, 1_000).await?;
    let root = tempfile::TempDir::new()?;
    let tasks = TaskTracker::new();
    let pin = ServingPin::open(
        context(&f, store, &root, tasks.clone())?,
        lease.token,
        Some("owner".into()),
    )
    .await?;
    // Fixture injection qualifies retention, not publication of a native graph.
    f.install_generation(2, fact.catalog.ok_or("catalog")?, fact.refs)
        .await?;
    tokio::time::sleep(Duration::from_millis(1_100)).await;
    assert!(matches!(
        pin.headers(Some("owner".into()), &[missing(&f)?]).await,
        Err(ServingReadError::Inactive)
    ));
    let renewal = f
        .client()
        .command::<RenewServingPin>(
            &f.target,
            identity()?,
            RenewServingRequest {
                check: ServingCheck {
                    token: lease.token,
                    actor: Some("owner".into()),
                },
                lease_ms: DEFAULT_LEASE_MS,
            },
        )
        .await;
    assert!(
        matches!(renewal, Err(InvocationError::Rejected(value)) if value.output==ServingReply::Denied(ServingDenial::Expired))
    );
    // Expire abandoned preparation work; its floor must not mask serving retention.
    edit(
        &f,
        "UPDATE catalog_operations SET expires_at_ms=0; UPDATE catalog_leases SET expires_at_ms=0",
    )
    .await?;
    let maintenance = super::terminal_retention::maintenance(&f.handle, f.repository).await?;
    f.client()
        .command::<ReapPreparation>(&f.target, identity()?, maintenance.clone())
        .await?;
    assert_eq!(pin_count(&f).await?, 1);
    assert!(
        edit(&f, "DELETE FROM catalog_generations WHERE generation=1")
            .await
            .is_err()
    );
    assert_eq!(
        release(&f, &pin).await?.output,
        ServingReleaseReply::Released
    );
    f.client()
        .command::<ReapPreparation>(&f.target, identity()?, maintenance)
        .await?;
    let count = f
        .handle
        .query(0, 1, |db| {
            let count: u8 = db.query_row(
                "SELECT count(*) FROM catalog_generations WHERE generation=1",
                [],
                |row| row.get(0),
            )?;
            Ok(vec![count])
        })
        .await?;
    assert_eq!(count, vec![0]);
    tasks.close();
    tasks.wait().await;
    f.runtime.shutdown().await?;
    Ok(())
}

#[test]
fn schema_bounds_serving_pins_and_rejects_identity_replacement() -> Result {
    let db = rusqlite::Connection::open_in_memory()?;
    db.execute_batch("PRAGMA foreign_keys=ON")?;
    db.execute_batch(SCHEMA)?;
    db.execute_batch("INSERT INTO catalog_generations(generation,catalog,certificate) VALUES(1,x'01',zeroblob(32)),(2,x'02',zeroblob(32)); INSERT INTO catalog_serving_pins VALUES(randomblob(16),zeroblob(16),1,x'0000000000000001',1,100)")?;
    for sql in [
        "UPDATE catalog_serving_pins SET reader=randomblob(16)",
        "UPDATE catalog_serving_pins SET incarnation=randomblob(16)",
        "UPDATE catalog_serving_pins SET admission_sequence=2",
        "UPDATE catalog_serving_pins SET owner_epoch=x'0000000000000002'",
        "UPDATE catalog_serving_pins SET generation=2",
        "UPDATE catalog_serving_pins SET expires_at_ms=99",
        "INSERT OR REPLACE INTO catalog_serving_pins SELECT * FROM catalog_serving_pins",
        "DELETE FROM catalog_generations WHERE generation=1",
    ] {
        assert!(db.execute_batch(sql).is_err(), "{sql}");
    }
    db.execute_batch("UPDATE catalog_serving_pins SET expires_at_ms=101; WITH RECURSIVE n(x) AS (VALUES(2) UNION ALL SELECT x+1 FROM n WHERE x<4096) INSERT INTO catalog_serving_pins SELECT randomblob(16),zeroblob(16),x,x'0000000000000001',1,0 FROM n")?;
    assert!(db.execute_batch("INSERT INTO catalog_serving_pins VALUES(randomblob(16),zeroblob(16),4097,x'0000000000000001',1,0)").is_err());
    db.execute_batch(
        "DELETE FROM catalog_serving_pins; DELETE FROM catalog_generations WHERE generation=1",
    )?;
    Ok(())
}

#[tokio::test]
async fn release_uncertainty_reuses_original_command_and_receipt_after_caller_loss() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        for fault in 1..=3 {
            let f = Fixture::new(format).await?;
            let store = Arc::new(ArtifactStore::new(Arc::new(InMemory::new()), f.repository));
            initialize(&f, store.clone()).await?;
            let (lease, _, _) = acquire(&f, Some("owner"), 246, DEFAULT_LEASE_MS).await?;
            let root = tempfile::TempDir::new()?;
            let tasks = TaskTracker::new();
            let pin = ServingPin::open(
                context(&f, store, &root, tasks.clone())?,
                lease.token,
                Some("owner".into()),
            )
            .await?;
            let ready = pin.ready_release(identity()?).await?;
            let evidence = ready.evidence().clone();
            assert_eq!(pin.ready_release(identity()?).await?.evidence(), &evidence);
            let coordinator = PublicationCoordinator::new(
                f.target.clone(),
                PublicationLimits::default(),
                f.publication_budget.clone(),
            )?;
            coordinator.fault_for_test(fault);
            let ticket = coordinator.submit(ready).await?;
            let state = timeout(Duration::from_secs(5), ticket.wait()).await?;
            assert!(matches!(state, PublicationState::Uncertain(_)), "{state:?}");
            drop(ticket);
            assert_eq!(pin.ready_release(identity()?).await?.evidence(), &evidence);
            coordinator.fault_for_test(0);
            let ticket = coordinator
                .pending_serving_release(lease.token.reader)
                .await
                .ok_or("retained release")?;
            ticket.recover().await?;
            let state = timeout(Duration::from_secs(5), ticket.wait()).await?;
            assert!(
                matches!(state, PublicationState::Finished(Ok(PublicationOutcome::ServingRelease(ref result))) if result.output==ServingReleaseReply::Released),
                "{state:?}"
            );
            assert_eq!(pin_count(&f).await?, 0);
            assert!(pin.ready_release(identity()?).await.is_err());
            assert!(coordinator.close_and_drain().await.is_empty());
            tasks.close();
            tasks.wait().await;
            f.runtime.shutdown().await?;
        }
    }
    Ok(())
}

mod blocked;

#[tokio::test]
async fn renewal_preserves_snapshot_and_cannot_shorten_or_revive_an_existing_pin() -> Result {
    let f = Fixture::new(ObjectFormat::Sha256).await?;
    let store = Arc::new(ArtifactStore::new(Arc::new(InMemory::new()), f.repository));
    let fact = initialize(&f, store).await?;
    let (lease, _, _) = acquire(&f, Some("owner"), 249, DEFAULT_LEASE_MS).await?;
    f.install_generation(2, fact.catalog.ok_or("catalog")?, fact.refs)
        .await?;
    let input = RenewServingRequest {
        check: ServingCheck {
            token: lease.token,
            actor: Some("owner".into()),
        },
        lease_ms: 1,
    };
    let original = f
        .client()
        .prepare_command::<RenewServingPin>(&f.target, identity()?, input.clone())
        .await?;
    let renewed = original.clone().execute().await?;
    let grant = granted(renewed.output.clone())?;
    assert_eq!(grant.token, lease.token);
    assert_eq!(grant.fact, fact);
    assert_eq!(grant.expires_at_ms, lease.expires_at_ms);
    let replay = original.execute().await?;
    assert_eq!(
        (replay.output, replay.receipt),
        (renewed.output, renewed.receipt)
    );
    edit(&f, "UPDATE repository_identity SET owner='replacement'").await?;
    let result = f
        .client()
        .command::<RenewServingPin>(&f.target, identity()?, input)
        .await;
    assert!(
        matches!(result, Err(InvocationError::Rejected(value)) if value.output==ServingReply::Denied(ServingDenial::Unauthorized))
    );
    assert_eq!(pin_count(&f).await?, 1);
    f.runtime.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn owner_restoration_cannot_convert_an_old_pin_dto_into_fresh_serving_authority() -> Result {
    let f = Fixture::new(ObjectFormat::Sha1).await?;
    let store = Arc::new(ArtifactStore::new(Arc::new(InMemory::new()), f.repository));
    initialize(&f, store.clone()).await?;
    let (lease, _, _) = acquire(&f, Some("owner"), 250, DEFAULT_LEASE_MS).await?;
    let (runtime, handle, client) =
        super::durable_recovery::restore_owner_fence(&f, lease.token.owner).await?;
    let root = tempfile::TempDir::new()?;
    let tasks = TaskTracker::new();
    // Use the actual restored client, not an old local handle or decoded fence.
    let restored = ServingContext::new(
        client.clone(),
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
        ServingReadBudget::new(4, tasks.clone())?,
        "owner".into(),
    )?;
    assert!(matches!(
        ServingPin::open(restored, lease.token, Some("owner".into())).await,
        Err(ServingReadError::Authority(_))
    ));
    let result = client
        .command::<RenewServingPin>(
            &f.target,
            identity()?,
            RenewServingRequest {
                check: ServingCheck {
                    token: lease.token,
                    actor: Some("owner".into()),
                },
                lease_ms: DEFAULT_LEASE_MS,
            },
        )
        .await;
    assert!(
        matches!(result, Err(InvocationError::Rejected(value)) if value.output==ServingReply::Denied(ServingDenial::Stale))
    );
    let bytes = handle
        .query(0, 8, |db| {
            let count: u64 =
                db.query_row("SELECT count(*) FROM catalog_serving_pins", [], |row| {
                    row.get(0)
                })?;
            Ok(count.to_be_bytes().to_vec())
        })
        .await?;
    assert_eq!(u64::from_be_bytes(bytes.as_slice().try_into()?), 1);
    tasks.close();
    tasks.wait().await;
    runtime.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn serving_release_and_preparation_share_budgets_without_colliding_logical_ids() -> Result {
    let f = Fixture::new(ObjectFormat::Sha256).await?;
    let store = Arc::new(ArtifactStore::new(Arc::new(InMemory::new()), f.repository));
    initialize(&f, store.clone()).await?;
    let (base, _, _) = super::prepare::opened(&f, [251; 16], store.clone()).await?;
    let (lease, _, _) = acquire(&f, Some("owner"), 251, DEFAULT_LEASE_MS).await?;
    let root = tempfile::TempDir::new()?;
    let tasks = TaskTracker::new();
    let pin = ServingPin::open(
        context(&f, store, &root, tasks.clone())?,
        lease.token,
        Some("owner".into()),
    )
    .await?;
    let queue = PublicationCoordinator::new(
        f.target.clone(),
        PublicationLimits::default(),
        f.publication_budget.clone(),
    )?;
    let preparation = queue.try_reserve(
        Arc::new(base.session.clone())
            .ready_renew(identity()?, DEFAULT_LEASE_MS)
            .await?,
    )?;
    assert!(matches!(preparation.state(), PublicationState::Held));
    let reader = queue.submit(pin.ready_release(identity()?).await?).await?;
    assert!(
        matches!(timeout(Duration::from_secs(5), reader.wait()).await?, PublicationState::Finished(Ok(PublicationOutcome::ServingRelease(ref value))) if value.output==ServingReleaseReply::Released)
    );
    assert!(queue.pending([251; 16]).await.is_some());
    preparation.activate().await?;
    assert!(matches!(
        timeout(Duration::from_secs(5), preparation.wait()).await?,
        PublicationState::Finished(Ok(PublicationOutcome::Preparation(_)))
    ));
    assert!(queue.close_and_drain().await.is_empty());
    assert_eq!(queue.stats().await.command_bytes, 0);
    tasks.close();
    tasks.wait().await;
    f.runtime.shutdown().await?;
    Ok(())
}
