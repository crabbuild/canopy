//! Restart scanning exercises real Cell queries, indexed paging and native work.
use super::*;
use super::{publishing::edit, root_outcome::Context};
use canopy_object_storage::artifact::ArtifactStore;
use tokio::time::{Duration, timeout};

fn scan_limits(page: u16) -> RecoveryScanLimits {
    RecoveryScanLimits {
        page,
        interval: Duration::from_millis(10),
    }
}
async fn scanned(
    service: &RecoverySupervisor,
    accept: impl Fn(&RecoveryScanStats) -> bool,
) -> Result<RecoveryScanStats> {
    Ok(timeout(Duration::from_secs(10), async {
        loop {
            let stats = service.stats();
            if accept(&stats) {
                return stats;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await?)
}

#[tokio::test]
async fn restart_scan_seeks_bounded_keys_and_revisits_corrupt_pins_without_starving_later_rows()
-> Result {
    let f = Fixture::new(ObjectFormat::Sha256).await?;
    // Far more unrelated pins than a page. Only 300 retained recovery heads
    // belong in the scan, including expired attempts with no active operation.
    edit(&f, "WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<4300) INSERT INTO catalog_leases(incarnation,admission_sequence,operation,owner_epoch,artifact_operation,expires_at_ms,recovery) SELECT zeroblob(16),x,zeroblob(16),x'0000000000000001',randomblob(16),0,CASE WHEN x>4000 THEN x'01' ELSE NULL END FROM n").await?;
    f.handle.query(0, 4096, |db| {
        let mut query = db.prepare("EXPLAIN QUERY PLAN SELECT incarnation,admission_sequence FROM catalog_leases INDEXED BY catalog_leases_recovery_scan WHERE recovery IS NOT NULL AND (incarnation,admission_sequence)>(?1,?2) ORDER BY incarnation,admission_sequence LIMIT ?3")?;
        let details: Vec<String> = query.query_map(rusqlite::params![vec![0u8;16], 0, 128], |row| row.get(3))?.collect::<std::result::Result<_,_>>()?;
        assert!(details.iter().any(|value| value.contains("SEARCH") && value.contains("catalog_leases_recovery_scan")), "{details:?}");
        assert!(details.iter().all(|value| !value.contains("TEMP B-TREE")), "{details:?}");
        Ok(Vec::new())
    }).await?;
    let queue = PublicationCoordinator::new(
        f.target.clone(),
        PublicationLimits::default(),
        f.publication_budget.clone(),
    )?;
    let store = ArtifactStore::new(Arc::new(InMemory::new()), f.repository);
    let service = RecoverySupervisor::start(
        f.client(),
        f.target.clone(),
        store.clone(),
        queue.clone(),
        f.scans(scan_limits(17)),
        f.authority(),
    )?;
    let stats = scanned(&service, |stats| stats.passes >= 2).await?;
    assert!(stats.scanned >= 600);
    assert_eq!(stats.scanned, stats.failures);
    assert_eq!(stats.submitted, 0);
    assert_eq!(stats.recovered, 0);
    assert!(stats.last_error.is_some());
    // A recovery registration behind the current key must be found on a later
    // pass. Keyset scans are bounded live walks, not a snapshot/high-water mark.
    edit(&f, "UPDATE catalog_leases SET recovery=x'01' WHERE incarnation=zeroblob(16) AND admission_sequence=1").await?;
    // A progress snapshot can already be part-way through its next pass.
    // Sample after registration and allow that partial pass plus two complete
    // passes, without changing the test timeout or scanner resource bounds.
    let registered = service.stats();
    let later = scanned(&service, |later| later.passes >= registered.passes + 3).await?;
    assert!(later.scanned >= registered.scanned + 601);
    assert_eq!(later.scanned, later.failures);
    assert_eq!(queue.stats().await.admitted, 0);
    service.shutdown().await?;
    assert!(queue.close_and_drain().await.is_empty());
    // Invalid scope must fail before starting a task or performing artifact I/O.
    let foreign = ArtifactStore::new(Arc::new(InMemory::new()), *uuid::Uuid::new_v4().as_bytes());
    assert!(matches!(
        RecoverySupervisor::start(
            f.client(),
            f.target.clone(),
            foreign,
            queue.clone(),
            f.scans(scan_limits(1)),
            f.authority(),
        ),
        Err(RootRecoveryError::Context)
    ));
    f.runtime.shutdown().await?;
    Ok(())
}

pub(super) async fn leaves_live_owner(
    f: &Fixture,
    store: &ArtifactStore,
    queue: &PublicationCoordinator,
) -> Result {
    let service = RecoverySupervisor::start_retiring(
        f.client(),
        f.target.clone(),
        store.clone(),
        queue.clone(),
        f.scans(scan_limits(1)),
        f.authority(),
        super::terminal_retention::maintenance(&f.handle, f.repository).await?,
    )?;
    let stats = scanned(&service, |stats| stats.deferred > 0).await?;
    assert_eq!(stats.submitted, 0);
    assert_eq!(stats.recovered, 0);
    assert_eq!(stats.release_submitted, 0);
    assert_eq!(stats.release_recovered, 0);
    assert_eq!(queue.stats().await.uncertain, 1);
    assert_eq!(queue.stats().await.command_bytes, 544 << 10);
    service.shutdown().await?;
    Ok(())
}

pub(super) async fn qualify(context: Context<'_>, fault: u8) -> Result {
    let Context {
        fixture: f,
        store,
        staging,
        ticket,
        root,
        budget,
        request,
    } = context;
    let session = ticket.bound_session()?;
    let ready = session
        .ready_root_outcome(identity()?, store, root, budget.clone(), None)
        .await?;
    let original = ready.evidence_for_test();
    let registered = ready.persist_recovery(store, identity()?).await?;
    let check = session.check.clone();
    assert_eq!(registered.evidence(), &original);
    drop(ready);
    drop(registered);
    drop(session);
    assert!(matches!(
        f.client().resolve(&original).await?,
        cellule_runtime::Resolution::Absent
    ));
    if fault == 2 {
        // Correctly MAC-sealed bytes copied into a different pin must fail
        // identity binding before bundle I/O; the later valid head still runs.
        edit(f, "INSERT INTO catalog_leases(incarnation,admission_sequence,operation,owner_epoch,artifact_operation,expires_at_ms,recovery) SELECT zeroblob(16),1,zeroblob(16),x'0000000000000001',randomblob(16),0,recovery FROM catalog_leases WHERE recovery IS NOT NULL LIMIT 1").await?;
    }
    let queue = PublicationCoordinator::new(
        f.target.clone(),
        PublicationLimits::default(),
        f.publication_budget.clone(),
    )?;
    queue.fault_for_test(fault);
    let (release, entered) = queue.pause_for_test().await;
    let service = RecoverySupervisor::start(
        f.client(),
        f.target.clone(),
        store.clone(),
        queue.clone(),
        f.scans(RecoveryScanLimits {
            page: 1,
            interval: Duration::from_secs(1),
        }),
        f.authority(),
    )?;
    timeout(Duration::from_secs(10), entered).await??;
    let observer = queue
        .pending(check.token.operation)
        .await
        .ok_or("discovered admission lost")?;
    assert_eq!(queue.stats().await.command_bytes, 32 << 10);
    release
        .send(())
        .map_err(|_| "discovered dispatch disappeared")?;
    assert!(matches!(
        timeout(Duration::from_secs(10), observer.wait()).await?,
        PublicationState::Uncertain(_)
    ));
    assert_eq!(queue.stats().await.command_bytes, 32 << 10);
    assert_eq!(queue.stats().await.admitted, 1);
    let service = if fault == 3 {
        // Stop the discovery owner while acceptance is unknown. The service
        // coordinator must keep the original command and its credits; a new
        // scanner joins that same ticket, without a second admission.
        service.shutdown().await?;
        assert_eq!(queue.stats().await.command_bytes, 32 << 10);
        assert_eq!(queue.stats().await.uncertain, 1);
        RecoverySupervisor::start(
            f.client(),
            f.target.clone(),
            store.clone(),
            queue.clone(),
            f.scans(RecoveryScanLimits {
                page: 1,
                interval: Duration::from_secs(1),
            }),
            f.authority(),
        )?
    } else {
        service
    };
    // The caller never requests recovery. A later scan finds the retained cold
    // ticket and retries its same original SDK identity through the fair queue.
    scanned(&service, |stats| stats.recovered > 0).await?;
    let completed = match timeout(Duration::from_secs(10), observer.wait()).await? {
        PublicationState::Finished(Ok(PublicationOutcome::RootPush(value))) => value,
        state => return Err(format!("automatic recovery: {state:?}").into()),
    };
    assert!(matches!(
        completed.output,
        RootCompletionReply::Completed(_)
    ));
    assert_eq!(queue.stats().await.command_bytes, 0);
    assert_eq!(queue.stats().await.admitted, 0);
    let stats = scanned(&service, |stats| stats.settled > 0).await?;
    assert_eq!(stats.submitted, u64::from(fault != 3));
    assert_eq!(stats.continuation, 0);
    assert_eq!(stats.failures > 0, fault == 2);
    service.shutdown().await?;
    assert!(queue.close_and_drain().await.is_empty());
    drop(observer);
    assert!(staging.close_and_drain().await.is_empty());
    // Destroy local SQL and factory state. A new owner's scanner recognizes
    // the settled head without dispatching or claiming an old-owner command.
    let (runtime, _, client) = super::durable_recovery::restore_owner(f, &check).await?;
    let queue = PublicationCoordinator::new(
        f.target.clone(),
        PublicationLimits::default(),
        f.publication_budget.clone(),
    )?;
    let service = RecoverySupervisor::start(
        client.clone(),
        f.target.clone(),
        store.clone(),
        queue.clone(),
        f.scans(scan_limits(1)),
        f.authority(),
    )?;
    let stats = scanned(&service, |stats| stats.settled > 0).await?;
    assert_eq!(stats.submitted, 0);
    assert_eq!(stats.failures > 0, fault == 2);
    let restored = RegisteredRootRecovery::load(&client, &f.target, store, &check)
        .await?
        .ok_or("settled pin lost on owner restore")?;
    assert_eq!(restored.evidence(), &original);
    let recovered = restored.dispatch(&client, store, &f.authority()).await?;
    assert_eq!(recovered.receipt, completed.receipt);
    assert_eq!(recovered.output, completed.output);
    let lookup = BeginRequest {
        repository: f.repository,
        operation: check.token.operation,
        request_digest: check.token.request_digest,
        actor: check.actor.clone(),
        lease_ms: DEFAULT_LEASE_MS,
    };
    let response =
        replay_root_push_response(&client, &f.target, lookup, Some(completed.receipt), store)
            .await?
            .ok_or("saved response missing")?;
    assert_eq!(
        super::durable_recovery::read_response(response).await?,
        request.response
    );
    service.shutdown().await?;
    assert!(queue.close_and_drain().await.is_empty());
    runtime.shutdown().await?;
    Ok(())
}

pub(super) async fn advanced_head(
    f: &Fixture,
    client: &cellule_runtime::CellClient,
    store: &ArtifactStore,
    original: &RegisteredRootRecovery,
    expected: &cellule_runtime::Committed<RefPolicyReply>,
) -> Result {
    let queue = PublicationCoordinator::new(
        f.target.clone(),
        PublicationLimits::default(),
        f.publication_budget.clone(),
    )?;
    queue.fault_for_test(2);
    let observer = queue
        .submit(
            original
                .clone()
                .ready(client.clone(), store.clone(), f.authority())?,
        )
        .await
        .map_err(|error| format!("historical admission: {:?}", error.reason))?;
    assert!(matches!(
        observer.wait().await,
        PublicationState::Uncertain(_)
    ));
    assert_eq!(queue.stats().await.command_bytes, 544 << 10);
    let service = RecoverySupervisor::start(
        client.clone(),
        f.target.clone(),
        store.clone(),
        queue.clone(),
        f.scans(scan_limits(1)),
        f.authority(),
    )?;
    let stats = scanned(&service, |stats| stats.recovered > 0 || stats.deferred > 0).await?;
    assert!(
        stats.recovered > 0,
        "advanced head stranded original cold ticket: {stats:?}"
    );
    let PublicationState::Finished(Ok(PublicationOutcome::PolicyPage(recovered))) =
        observer.wait().await
    else {
        return Err("advanced-head recovery lost original page".into());
    };
    assert_eq!(recovered.receipt, expected.receipt);
    assert_eq!(recovered.output, expected.output);
    assert_eq!(queue.stats().await.command_bytes, 0);
    service.shutdown().await?;
    assert!(queue.close_and_drain().await.is_empty());
    Ok(())
}

// Keep large qualifier construction outside the shared native fixture's poll
// frame. The boxed future is polled with the same test stack and resource caps.
pub(super) fn qualify_native<'a>(
    context: Context<'a>,
    mode: super::root_completion::CompletionMode,
    provider: Arc<InMemory>,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result> + 'a>> {
    match mode {
        super::root_completion::CompletionMode::TerminalRetention { fault } => {
            Box::pin(super::terminal_retention::qualify(context, fault, provider))
        }
        super::root_completion::CompletionMode::Discovery { fault } => {
            Box::pin(qualify(context, fault))
        }
        super::root_completion::CompletionMode::Durable { fault, revoked } => {
            Box::pin(super::durable_recovery::qualify(context, fault, revoked))
        }
        _ => unreachable!("native recovery qualifier role"),
    }
}

#[tokio::test]
async fn resident_scanners_pause_independently_and_resume_live_indexed_discovery() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let f = Fixture::new(format).await?;
        edit(&f, "WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<30) INSERT INTO catalog_leases(incarnation,admission_sequence,operation,owner_epoch,artifact_operation,expires_at_ms,recovery) SELECT zeroblob(16),x,zeroblob(16),x'0000000000000001',randomblob(16),0,x'01' FROM n").await?;
        edit(&f, "WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<30) INSERT INTO catalog_custody_commands(operation,step,incarnation,request_id,intent) SELECT CAST(printf('%016d',x) AS BLOB),0,zeroblob(16),CAST(printf('%016d',x) AS BLOB),x'01' FROM n").await?;
        let queue = PublicationCoordinator::new(
            f.target.clone(),
            PublicationLimits::default(),
            f.publication_budget.clone(),
        )?;
        let roots = RecoverySupervisor::start(
            f.client(),
            f.target.clone(),
            ArtifactStore::new(Arc::new(InMemory::new()), f.repository),
            queue.clone(),
            f.scans(scan_limits(1)),
            f.authority(),
        )?;
        let custody = CustodySupervisor::start(
            f.client(),
            f.target.clone(),
            queue.clone(),
            f.scans(scan_limits(1)),
            f.authority(),
        )?;
        scanned(&roots, |stats| stats.scanned > 0).await?;
        tokio::join!(roots.pause(), custody.pause());
        let root_before = roots.stats();
        let custody_before = custody.stats();
        assert_eq!(f.scan_budget.in_flight(), 0);
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(roots.stats().scanned, root_before.scanned);
        assert_eq!(custody.stats().scanned, custody_before.scanned);
        assert_eq!(roots.stats().passes, root_before.passes);
        assert_eq!(custody.stats().passes, custody_before.passes);
        assert_eq!(queue.stats().await.admitted, 0);
        // Resuming one owner must not restart its independently paused sibling.
        roots.resume();
        scanned(&roots, |stats| stats.scanned > root_before.scanned).await?;
        assert_eq!(custody.stats().scanned, custody_before.scanned);
        roots.pause().await;
        let paused = roots.stats();
        custody.resume();
        timeout(Duration::from_secs(10), async {
            while custody.stats().scanned <= custody_before.scanned {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await?;
        assert_eq!(roots.stats().scanned, paused.scanned);
        custody.pause().await;
        assert_eq!(f.scan_budget.in_flight(), 0);
        roots.resume();
        custody.resume();
        scanned(&roots, |stats| stats.passes > root_before.passes).await?;
        timeout(Duration::from_secs(10), async {
            while custody.stats().passes <= custody_before.passes {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await?;
        let (root_final, custody_final) = tokio::join!(roots.shutdown(), custody.shutdown());
        let root_final = root_final?;
        let custody_final = custody_final?;
        assert_eq!(root_final.scanned, root_final.failures);
        assert_eq!(custody_final.scanned, custody_final.failures);
        assert_eq!(f.scan_budget.in_flight(), 0);
        assert!(queue.close_and_drain().await.is_empty());
        f.runtime.shutdown().await?;
    }
    Ok(())
}
