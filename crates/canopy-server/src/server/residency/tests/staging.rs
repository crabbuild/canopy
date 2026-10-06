//! Production residency owns staging independently of request observers.
use super::recovery::{create, loaded, server};
use super::*;
use crate::packs::publication::{
    BeginRequest, DEFAULT_LEASE_MS, ReadyStaging, StagingError, StagingState, StagingTicket,
};
use crate::{ObjectFormat, server::mutation_identity};
use tokio::{
    sync::oneshot,
    time::{Duration, timeout},
};
type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

async fn ready(
    repository: &RepositoryCell,
    client: CellClient,
    operation: u8,
) -> Result<ReadyStaging> {
    Ok(ReadyStaging::new(
        client,
        repository.target.clone(),
        BeginRequest {
            repository: repository.id,
            operation: [operation; 16],
            request_digest: [operation; 32],
            actor: "canopy".into(),
            lease_ms: DEFAULT_LEASE_MS,
        },
        mutation_identity()?,
    )
    .await?)
}
async fn active(ticket: &StagingTicket) -> Result {
    match timeout(Duration::from_secs(10), ticket.wait()).await? {
        StagingState::Active(_) => Ok(()),
        other => Err(format!("production staging refused: {other:?}").into()),
    }
}

struct Release(Option<std::sync::mpsc::Sender<()>>);
impl Drop for Release {
    fn drop(&mut self) {
        if let Some(sender) = self.0.take() {
            let _ = sender.send(());
        }
    }
}

struct DriverDropped(Arc<std::sync::atomic::AtomicBool>);
impl Drop for DriverDropped {
    fn drop(&mut self) {
        self.0.store(true, std::sync::atomic::Ordering::Release);
    }
}
fn driver_request(repository: &RepositoryCell, operation: u8) -> BeginRequest {
    BeginRequest {
        repository: repository.id,
        operation: [operation; 16],
        request_digest: [operation; 32],
        actor: "canopy".into(),
        lease_ms: DEFAULT_LEASE_MS,
    }
}

struct DriverOwnedFailure {
    _pin: crate::git_objects::ReadOwner,
    message: String,
}
impl std::fmt::Debug for DriverOwnedFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DriverOwnedFailure").finish_non_exhaustive()
    }
}
impl std::fmt::Display for DriverOwnedFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}
impl std::error::Error for DriverOwnedFailure {}

#[tokio::test]
async fn production_push_driver_survives_observer_loss_binds_and_joins_before_resident_release()
-> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let (server, _files) = server().await?;
        let manager = server.repositories.clone();
        let entry = create(&manager, "driver-bind", format).await?;
        let (repository, _, service) = loaded(&manager, entry.repository_id).await?;
        let coordinator = repository.staging_coordinator()?;
        let request = driver_request(&repository, 180);
        let ready = coordinator
            .ready_request(request.clone(), mutation_identity()?)
            .await?;
        let ticket = coordinator.submit(ready).map_err(|(error, _)| error)?;
        // Join identity is checked even before Begin has produced a token.
        assert!(coordinator.join_request(&request)?.is_some());
        let mut wrong = request.clone();
        wrong.request_digest[0] ^= 1;
        assert!(coordinator.join_request(&wrong).is_err());
        let mut wrong = request.clone();
        wrong.actor = "another-account".into();
        assert!(coordinator.join_request(&wrong).is_err());
        let mut foreign = request.clone();
        foreign.repository = *uuid::Uuid::new_v4().as_bytes();
        assert!(coordinator.join_request(&foreign).is_err());
        let (bound, bound_observer) = oneshot::channel();
        let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let owned = DriverDropped(dropped.clone());
        ticket.drive(move |ticket, publication| async move {
            let _owned = owned;
            if !matches!(ticket.wait().await, StagingState::Active(_)) {
                return Err(StagingError::Context);
            }
            if publication.stats().await.closed {
                return Err(StagingError::Closed);
            }
            let work = ticket.spawn(|context| async move { context.token() })?;
            let _token = work
                .wait()
                .await
                .map_err(|error| StagingError::Input(Box::new(error)))?;
            ticket.seal()?;
            if !matches!(ticket.wait_terminal().await, StagingState::Bound(_)) {
                return Err(StagingError::Context);
            }
            let _ = bound.send(());
            std::future::pending::<std::result::Result<(), StagingError>>().await
        })?;
        let called = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let attempt = called.clone();
        assert!(matches!(
            ticket.drive(move |_, _| async move {
                attempt.store(true, std::sync::atomic::Ordering::Release);
                Ok(())
            }),
            Err(StagingError::Duplicate)
        ));
        drop(ticket);
        timeout(Duration::from_secs(10), bound_observer).await??;
        assert!(!called.load(std::sync::atomic::Ordering::Acquire));
        assert!(!dropped.load(std::sync::atomic::Ordering::Acquire));
        assert_eq!(
            coordinator.stats().workers,
            0,
            "controller must not prevent its own Bind"
        );
        assert_eq!(coordinator.stats().admitted, 1);
        assert!(!service.quiesce().await);
        timeout(Duration::from_secs(10), server.shutdown()).await??;
        assert!(dropped.load(std::sync::atomic::Ordering::Acquire));
        assert_eq!(coordinator.stats().admitted, 0);
        assert_eq!(manager.staging_budget.available(), (32, 64));
        assert!(matches!(
            coordinator
                .ready_request(request.clone(), mutation_identity()?)
                .await,
            Err(StagingError::Closed)
        ));
        assert!(coordinator.join_request(&request)?.is_none());
    }
    Ok(())
}

#[tokio::test]
async fn production_push_driver_cancellation_keeps_detached_physical_worker_and_node_owned_until_drain()
-> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let (server, files) = server().await?;
        let manager = server.repositories.clone();
        let entry = create(&manager, "driver-physical", format).await?;
        let (repository, _, service) = loaded(&manager, entry.repository_id).await?;
        let coordinator = repository.staging_coordinator()?;
        let ready = coordinator
            .ready_request(driver_request(&repository, 181), mutation_identity()?)
            .await?;
        let ticket = coordinator.submit(ready).map_err(|(error, _)| error)?;
        let (release, wait) = std::sync::mpsc::channel();
        let release = Release(Some(release));
        let (entered, running) = oneshot::channel();
        let (transferred, transfer) = oneshot::channel();
        let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let owned = DriverDropped(dropped.clone());
        ticket.drive(move |ticket, _| async move {
            let _owned = owned;
            if !matches!(ticket.wait().await, StagingState::Active(_)) {
                return Err(StagingError::Context);
            }
            let work = ticket.spawn(move |context| async move {
                let owner = context.physical_owner();
                Ok(tokio::task::spawn_blocking(move || {
                    let _owner = owner;
                    let _ = entered.send(());
                    let _ = wait.recv();
                }))
            })?;
            let _detached = work
                .wait()
                .await
                .map_err(|error| StagingError::Input(Box::new(error)))?;
            let _ = transferred.send(());
            std::future::pending::<std::result::Result<(), StagingError>>().await
        })?;
        drop(ticket);
        timeout(Duration::from_secs(5), running).await??;
        timeout(Duration::from_secs(5), transfer).await??;
        let node = server.node.clone();
        let mut shutdown = tokio::spawn(server.shutdown());
        timeout(Duration::from_secs(5), async {
            while !dropped.load(std::sync::atomic::Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        assert!(
            timeout(Duration::from_millis(50), &mut shutdown)
                .await
                .is_err()
        );
        assert!(!node.is_shutting_down());
        assert!(!service.coordinator.stats().await.closed);
        assert_eq!(manager.staging_budget.available(), (31, 63));
        assert!(
            crate::server::workspace::Workspace::open(&files.path().join("node"))
                .is_err_and(|error| error.kind() == std::io::ErrorKind::WouldBlock)
        );
        drop(release);
        timeout(Duration::from_secs(10), shutdown).await???;
        assert_eq!(manager.staging_budget.available(), (32, 64));
    }
    Ok(())
}

#[tokio::test]
async fn production_push_driver_close_preserves_exact_uncertain_begin_and_panic_returns_credit()
-> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        for fault in [1, 2, 3] {
            let (server, _files) = server().await?;
            let manager = server.repositories.clone();
            let entry = create(&manager, "driver-exact", format).await?;
            let (repository, _, service) = loaded(&manager, entry.repository_id).await?;
            let coordinator = repository.staging_coordinator()?;
            let ready = coordinator
                .ready_request(driver_request(&repository, 182), mutation_identity()?)
                .await?;
            coordinator.fault_for_test(fault);
            let ticket = coordinator.submit(ready).map_err(|(error, _)| error)?;
            let (entered, running) = oneshot::channel();
            let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let owned = DriverDropped(dropped.clone());
            ticket.drive(move |_, _| async move {
                let _owned = owned;
                let _ = entered.send(());
                std::future::pending::<std::result::Result<(), StagingError>>().await
            })?;
            timeout(Duration::from_secs(5), running).await??;
            assert!(matches!(
                timeout(Duration::from_secs(10), ticket.wait_terminal()).await?,
                StagingState::Uncertain(_)
            ));
            let original = ticket
                .custody_evidence_for_test()
                .ok_or("original missing")?;
            // Both drains await the same workflow join, without taking its
            // handle away from another observer or returning its quota early.
            let (first, second) = timeout(Duration::from_secs(5), async {
                tokio::join!(coordinator.close_and_drain(), coordinator.close_and_drain())
            })
            .await?;
            assert_eq!((first.len(), second.len()), (1, 1));
            assert!(dropped.load(std::sync::atomic::Ordering::Acquire));
            assert_eq!(ticket.custody_evidence_for_test(), Some(original));
            assert_eq!(manager.staging_budget.available(), (31, 64));
            assert!(!service.coordinator.stats().await.closed);
            timeout(Duration::from_secs(10), server.shutdown()).await??;
            assert_eq!(manager.staging_budget.available(), (32, 64));
        }
        let (server, _files) = server().await?;
        let manager = server.repositories.clone();
        let entry = create(&manager, "driver-panic", format).await?;
        let (repository, _, _) = loaded(&manager, entry.repository_id).await?;
        let coordinator = repository.staging_coordinator()?;
        let ready = coordinator
            .ready_request(driver_request(&repository, 183), mutation_identity()?)
            .await?;
        let ticket = coordinator.submit(ready).map_err(|(error, _)| error)?;
        active(&ticket).await?;
        ticket.drive(|_, _| async { panic!("owned workflow panic") })?;
        timeout(Duration::from_secs(5), async {
            while coordinator.stats().admitted != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        assert!(
            matches!(ticket.state(), StagingState::Fenced(error) if matches!(&*error, StagingError::DriverFailure(message) if message.contains("staging worker panicked")))
        );
        assert_eq!(manager.staging_budget.available(), (32, 64));
        timeout(Duration::from_secs(10), server.shutdown()).await??;
    }
    Ok(())
}

#[tokio::test]
async fn production_push_driver_failure_remains_observable_before_and_after_bind() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        for bind in [false, true] {
            for owned in [false, true] {
                let (server, _files) = server().await?;
                let manager = server.repositories.clone();
                let entry = create(&manager, "driver-failure", format).await?;
                let (repository, _, service) = loaded(&manager, entry.repository_id).await?;
                let coordinator = repository.staging_coordinator()?;
                let request = driver_request(&repository, 184);
                let ready = coordinator
                    .ready_request(request.clone(), mutation_identity()?)
                    .await?;
                let ticket = coordinator.submit(ready).map_err(|(error, _)| error)?;
                let (entered, running) = oneshot::channel();
                let (release, proceed) = oneshot::channel();
                ticket.drive(move |ticket, _| async move {
                    if !matches!(ticket.wait().await, StagingState::Active(_)) {
                        return Err(StagingError::Context);
                    }
                    if bind {
                        ticket.seal()?;
                        if !matches!(ticket.wait_terminal().await, StagingState::Bound(_)) {
                            return Err(StagingError::Context);
                        }
                    }
                    let pin = if owned {
                        let task = if bind {
                            ticket.spawn_bound(|_, context| async move {
                                Ok(context.physical_owner())
                            })?
                        } else {
                            ticket.spawn(|context| async move { Ok(context.physical_owner()) })?
                        };
                        Some(
                            task.wait()
                                .await
                                .map_err(|error| StagingError::Input(Box::new(error)))?,
                        )
                    } else {
                        None
                    };
                    let _ = entered.send(());
                    let _ = proceed.await;
                    Err(match pin {
                        Some(pin) => StagingError::Input(Box::new(DriverOwnedFailure {
                            _pin: pin,
                            message: format!("worker-owned failure {}", "λ".repeat(4096)),
                        })),
                        None => StagingError::Clock,
                    })
                })?;
                timeout(Duration::from_secs(5), running).await??;
                let original_bound = ticket.bound_result();
                assert_eq!(original_bound.is_some(), bind);
                assert_eq!(
                    manager.staging_budget.available(),
                    (31, if owned { 63 } else { 64 })
                );
                drop(ticket);
                let observer = coordinator.join_request(&request)?.expect("owned driver");
                let _ = release.send(());
                let outcome = timeout(Duration::from_secs(5), observer.wait_completion()).await?;
                let StagingState::Fenced(error) = outcome else {
                    return Err(format!("controller failure was lost: {outcome:?}").into());
                };
                let StagingError::DriverFailure(message) = &*error else {
                    return Err(format!("unexpected controller failure: {error:?}").into());
                };
                assert!(message.len() <= 4096);
                assert!(message.contains(if owned {
                    "worker-owned failure"
                } else {
                    "staging clock failed"
                }));
                assert_eq!(observer.bound_result().is_some(), bind);
                if let Some(original) = original_bound {
                    assert!(Arc::ptr_eq(
                        &original,
                        &observer.bound_result().expect("historical Bind")
                    ));
                }
                assert!(observer.pending_publication().is_none());
                timeout(Duration::from_secs(5), async {
                    while coordinator.stats().admitted != 0 {
                        tokio::task::yield_now().await;
                    }
                })
                .await?;
                assert_eq!(manager.staging_budget.available(), (32, 64));
                assert!(!service.coordinator.stats().await.closed);
                timeout(Duration::from_secs(10), server.shutdown()).await??;
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn production_staging_blocks_eviction_and_shutdown_until_detached_physical_worker_drains()
-> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let (server, files) = server().await?;
        let manager = server.repositories.clone();
        let entry = create(&manager, "staging-physical", format).await?;
        let (repository, client, service) = loaded(&manager, entry.repository_id).await?;
        let coordinator = repository.staging_coordinator()?;
        assert!(Arc::ptr_eq(&coordinator, &service.staging));
        let ticket = coordinator
            .submit(ready(&repository, client.clone(), 91).await?)
            .map_err(|(error, _)| error)?;
        active(&ticket).await?;
        let (release, wait) = std::sync::mpsc::channel();
        let release = Release(Some(release));
        let (entered, running) = oneshot::channel();
        let work = ticket.spawn(move |context| async move {
            let owner = context.physical_owner();
            Ok(tokio::task::spawn_blocking(move || {
                let _owner = owner;
                let _ = entered.send(());
                let _ = wait.recv();
            }))
        })?;
        let worker = work.wait().await.map_err(|error| error.to_string())?;
        timeout(Duration::from_secs(5), running).await??;
        assert_eq!(manager.staging_budget.available(), (31, 63));
        assert!(!service.quiesce().await);
        assert!(!coordinator.stats().closed);
        assert!(!service.coordinator.stats().await.closed);
        drop(ticket);
        let node = server.node.clone();
        let directory = server.directory.clone();
        let session = server.advertisement.lock().await.advertisement().session();
        let mut shutdown = tokio::spawn(server.shutdown());
        timeout(Duration::from_secs(5), async {
            while !coordinator.stats().closed {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        assert!(
            timeout(Duration::from_millis(50), &mut shutdown)
                .await
                .is_err()
        );
        assert!(repository.staging_coordinator().is_err());
        assert!(!node.is_shutting_down());
        assert!(
            directory
                .is_live(session, crate::server::unix_now_ms()?)
                .await?
        );
        assert!(!service.coordinator.stats().await.closed);
        assert_eq!(manager.staging_budget.available(), (31, 63));
        assert!(
            crate::server::workspace::Workspace::open(&files.path().join("node"))
                .is_err_and(|error| error.kind() == std::io::ErrorKind::WouldBlock)
        );
        // A cached handle also refuses new admission after the node barrier.
        let (error, _) = coordinator
            .submit(ready(&repository, client, 92).await?)
            .err()
            .ok_or("cached coordinator admitted during shutdown")?;
        assert!(matches!(error, StagingError::Closed));
        drop(release);
        timeout(Duration::from_secs(5), worker).await??;
        timeout(Duration::from_secs(10), shutdown).await???;
        assert_eq!(manager.staging_budget.available(), (32, 64));
        assert_eq!(coordinator.stats().admitted, 0);
        assert!(node.is_shutting_down());
    }
    Ok(())
}

#[tokio::test]
async fn production_staging_account_capacity_is_shared_across_resident_repositories() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let (server, _files) = server().await?;
        let manager = server.repositories.clone();
        let mut tickets = Vec::new();
        for (repo, name) in ["stage-one", "stage-two", "stage-three"].iter().enumerate() {
            let entry = create(&manager, name, format).await?;
            let (repository, client, service) = loaded(&manager, entry.repository_id).await?;
            let coordinator = repository.staging_coordinator()?;
            for index in 0..8 {
                let input =
                    ready(&repository, client.clone(), (100 + repo * 8 + index) as u8).await?;
                match coordinator.submit(input) {
                    Ok(ticket) => {
                        active(&ticket).await?;
                        tickets.push(ticket);
                    }
                    Err((error, retained)) => {
                        assert_eq!(repo, 2);
                        assert!(matches!(error, StagingError::Capacity));
                        assert_eq!(service.staging.stats().admitted, 0);
                        assert_eq!(manager.staging_budget.available(), (16, 64));
                        // Quota refusal did not consume the prepared request. The
                        // exact value can be submitted after an owner drains.
                        if index == 0 {
                            let stopped = tickets.remove(0);
                            stopped.stop();
                            timeout(Duration::from_secs(5), async {
                                while manager.staging_budget.available().0 != 17 {
                                    tokio::task::yield_now().await;
                                }
                            })
                            .await?;
                            let retry = coordinator.submit(retained).map_err(|(error, _)| error)?;
                            active(&retry).await?;
                            tickets.push(retry);
                        }
                        break;
                    }
                }
            }
        }
        assert_eq!(tickets.len(), 16);
        drop(tickets); // Observers do not return credits; the resident owns them.
        assert_eq!(manager.staging_budget.available(), (16, 64));
        timeout(Duration::from_secs(15), server.shutdown()).await??;
        assert_eq!(manager.staging_budget.available(), (32, 64));
    }
    Ok(())
}

#[tokio::test]
async fn production_staging_eviction_refusal_restores_admission_and_closed_eviction_is_idempotent()
-> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let (server, _files) = server().await?;
        let manager = server.repositories.clone();
        let entry = create(&manager, "stage-resume", format).await?;
        let (repository, client, service) = loaded(&manager, entry.repository_id).await?;
        let snapshot = repository
            .serving_snapshot(ReadIdentity::Account("canopy"))
            .await?;
        assert!(!service.quiesce().await);
        let coordinator = repository.staging_coordinator()?;
        let ticket = coordinator
            .submit(ready(&repository, client, 93).await?)
            .map_err(|(error, _)| error)?;
        active(&ticket).await?;
        assert!(!service.quiesce().await);
        ticket.stop();
        drop(snapshot);
        timeout(Duration::from_secs(5), async {
            while coordinator.stats().admitted != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        assert!(timeout(Duration::from_secs(5), service.quiesce()).await?);
        assert!(service.quiesce().await);
        assert!(matches!(
            repository.staging_coordinator(),
            Err(StagingError::Closed)
        ));
        assert_eq!(manager.staging_budget.available(), (32, 64));
        server.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn production_shutdown_recovers_exact_staging_while_another_repository_holds_physical_work()
-> Result {
    use crate::packs::publication::RegisteredCustody;
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        for fault in [1, 2, 3] {
            let (server, _files) = server().await?;
            let manager = server.repositories.clone();
            let held = create(&manager, "stage-held", format).await?;
            let uncertain = create(&manager, "stage-recovery", format).await?;
            let (held_repo, held_client, held_service) =
                loaded(&manager, held.repository_id).await?;
            let (repository, client, service) = loaded(&manager, uncertain.repository_id).await?;
            let ticket = held_service
                .staging
                .submit(ready(&held_repo, held_client, 94).await?)
                .map_err(|(error, _)| error)?;
            active(&ticket).await?;
            let (release, wait) = std::sync::mpsc::channel();
            let release = Release(Some(release));
            let (entered, running) = oneshot::channel();
            let worker = ticket
                .spawn(move |context| async move {
                    let owner = context.physical_owner();
                    Ok(tokio::task::spawn_blocking(move || {
                        let _owner = owner;
                        let _ = entered.send(());
                        let _ = wait.recv();
                    }))
                })?
                .wait()
                .await
                .map_err(|error| error.to_string())?;
            timeout(Duration::from_secs(5), running).await??;
            service.staging.fault_for_test(fault);
            let original_ticket = service
                .staging
                .submit(ready(&repository, client.clone(), 95).await?)
                .map_err(|(error, _)| error)?;
            assert!(matches!(
                timeout(Duration::from_secs(10), original_ticket.wait_terminal()).await?,
                StagingState::Uncertain(_)
            ));
            let original = RegisteredCustody::load_latest(&client, &repository.target, [95; 16])
                .await?
                .ok_or("uncertain staging registration absent")?
                .evidence()
                .clone();
            let prior = match client.resolve(&original).await? {
                cellule_runtime::Resolution::Absent => None,
                cellule_runtime::Resolution::Committed(outcome) => Some(outcome.commit_sequence()),
                other => return Err(format!("unexpected original state: {other:?}").into()),
            };
            assert_eq!(prior.is_some(), fault != 1);
            drop((ticket, original_ticket));
            let node = server.node.clone();
            let mut shutdown = tokio::spawn(server.shutdown());
            timeout(Duration::from_secs(10), async {
                while service.staging.stats().admitted != 0
                    || service.staging.stats().retirement_running
                {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await?;
            assert!(
                timeout(Duration::from_millis(30), &mut shutdown)
                    .await
                    .is_err()
            );
            assert!(!node.is_shutting_down());
            assert_eq!(manager.staging_budget.available(), (31, 63));
            assert!(!service.coordinator.stats().await.closed);
            let saved = RegisteredCustody::load_latest(&client, &repository.target, [95; 16])
                .await?
                .ok_or("exact registration lost during shutdown")?;
            assert_eq!(saved.evidence(), &original);
            assert!(saved.settled());
            let replay = saved.recover(&client).await?;
            if let Some(sequence) = prior {
                assert_eq!(replay.receipt.commit_sequence, sequence);
            }
            drop(release);
            timeout(Duration::from_secs(5), worker).await??;
            timeout(Duration::from_secs(10), shutdown).await???;
            assert!(!service.staging.stats().retirement_running);
            assert_eq!(manager.staging_budget.available(), (32, 64));
        }
    }
    Ok(())
}
