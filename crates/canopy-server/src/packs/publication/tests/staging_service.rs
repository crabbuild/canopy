mod bound;
mod publication;
use super::*;
use tokio::{
    sync::oneshot,
    time::{Duration, timeout},
};

async fn submit(
    fixture: &Fixture,
    coordinator: &StagingCoordinator,
    operation: [u8; 16],
    actor: &str,
) -> Result<StagingTicket> {
    let mut input = fixture.begin(operation);
    input.actor = actor.into();
    let ready =
        ReadyStaging::new(fixture.client(), fixture.target.clone(), input, identity()?).await?;
    coordinator.submit(ready).map_err(|(e, _)| e.into())
}
async fn active(ticket: &StagingTicket) -> Result<StagingLease> {
    match timeout(Duration::from_secs(10), ticket.wait()).await? {
        StagingState::Active(lease) => Ok(lease),
        other => Err(format!("unexpected {other:?}").into()),
    }
}
async fn terminal(ticket: &StagingTicket) -> Result<StagingState> {
    Ok(timeout(Duration::from_secs(10), ticket.wait_terminal()).await?)
}
async fn changed_lease(ticket: &StagingTicket, old: i64) -> Result<StagingLease> {
    timeout(Duration::from_secs(10), async {
        loop {
            match ticket.state() {
                StagingState::Active(l) | StagingState::Draining(l) if l.expires_at_ms > old => {
                    return Ok(l);
                }
                StagingState::Fenced(e) | StagingState::Uncertain(e) => {
                    return Err(format!("unexpected {e:?}").into());
                }
                _ => tokio::task::yield_now().await,
            }
        }
    })
    .await?
}

#[tokio::test]
async fn staged_service_canceled_observers_keep_workers_and_results_until_single_handoff() -> Result
{
    let fixture = Fixture::new(ObjectFormat::Sha256).await?;
    let coordinator = StagingCoordinator::new(fixture.target.clone(), StagingLimits::default())?;
    let ticket = submit(&fixture, &coordinator, [220; 16], "owner").await?;
    let initial = active(&ticket).await?;
    let (release, wait) = oneshot::channel();
    let (entered, started) = oneshot::channel();
    let work = ticket.spawn(move |ctx| async move {
        let token = ctx.token()?;
        let _ = entered.send(());
        wait.await.map_err(|_| StagingError::Worker)?;
        ctx.ensure_live()?;
        Ok(token)
    })?;
    let work_id = work.id();
    timeout(Duration::from_secs(10), started).await??;
    drop(work);
    drop(ticket);
    let retained = coordinator.pending([220; 16]).ok_or("lost job")?;
    assert_eq!(coordinator.stats().workers, 1);
    retained.renew_for_test();
    let renewed = changed_lease(&retained, initial.expires_at_ms).await?;
    assert_eq!(renewed.token, initial.token);
    retained.seal()?;
    assert!(matches!(
        retained.spawn(|_| async { Ok(()) }),
        Err(StagingError::Inactive)
    ));
    release.send(()).map_err(|_| "worker disappeared")?;
    let result = retained
        .pending_task::<PreparationToken>(work_id)
        .ok_or("lost result")?;
    assert!(retained.pending_task::<u64>(work_id).is_none());
    let token = result.wait().await.map_err(|e| format!("work {e}"))?;
    assert_eq!(token, initial.token);
    assert!(matches!(result.wait().await,Err(e) if matches!(*e,StagingError::NotReady)));
    let StagingState::Bound(bound) = terminal(&retained).await? else {
        return Err("not bound".into());
    };
    assert_eq!(bound.lease.token, initial.token);
    assert_eq!(bound.lease.expires_at_ms, renewed.expires_at_ms);
    assert_eq!(coordinator.stats().workers, 0);
    assert!(coordinator.close_and_drain().await.is_empty());
    assert_eq!(coordinator.stats().admitted, 0);
    assert_eq!(fixture.counts().await?, (1, 1));
    fixture.runtime.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn staged_service_resolves_begin_renew_and_bind_exactly_after_absence_lost_ack_or_panic()
-> Result {
    for fault in [1, 2, 3] {
        let fixture = Fixture::new(ObjectFormat::Sha256).await?;
        let coordinator =
            StagingCoordinator::new(fixture.target.clone(), StagingLimits::default())?;
        coordinator.fault_for_test(fault);
        let ticket = submit(&fixture, &coordinator, [221; 16], "owner").await?;
        assert!(matches!(
            terminal(&ticket).await?,
            StagingState::Uncertain(_)
        ));
        assert_eq!(coordinator.stats().admitted, 1);
        assert_eq!(coordinator.stats().command_bytes, 8192);
        coordinator.recover(&ticket)?;
        let original = active(&ticket).await?;
        assert_eq!(original.token.artifact_operation, artifact_number(1));
        assert_eq!(fixture.counts().await?, (1, 1));
        coordinator.fault_for_test(fault);
        ticket.renew_for_test();
        assert!(matches!(
            terminal(&ticket).await?,
            StagingState::Uncertain(_)
        ));
        coordinator.recover(&ticket)?;
        // wait observes Resolving and then a queried live Active result.
        let renewed = active(&ticket).await?;
        assert_eq!(renewed.token, original.token);
        coordinator.fault_for_test(fault);
        ticket.seal()?;
        let StagingState::Uncertain(error) = terminal(&ticket).await? else {
            return Err("not uncertain bind".into());
        };
        let StagingError::Bind(ref error) = *error else {
            return Err("wrong operation".into());
        };
        let InvocationError::Pending(evidence) = error.as_ref() else {
            return Err("missing evidence".into());
        };
        let evidence = (**evidence).clone();
        let pending = timeout(Duration::from_secs(10), coordinator.close_and_drain()).await?;
        assert_eq!(pending.len(), 1);
        assert_eq!(coordinator.stats().command_bytes, 8192);
        coordinator.recover(&pending[0])?;
        let StagingState::Bound(bound) = terminal(&pending[0]).await? else {
            return Err("bind did not resolve".into());
        };
        let cellule_runtime::Resolution::Committed(outcome) =
            fixture.client().resolve(&evidence).await?
        else {
            return Err("missing committed outcome".into());
        };
        assert_eq!(bound.receipt.commit_sequence, outcome.commit_sequence());
        assert_eq!(bound.lease.token, original.token);
        assert_eq!(fixture.counts().await?, (1, 1));
        assert!(coordinator.close_and_drain().await.is_empty());
        fixture.runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn staged_service_replayed_renewal_is_not_a_new_clock_or_permission_after_revocation()
-> Result {
    let fixture = Fixture::new(ObjectFormat::Sha256).await?;
    super::publishing::edit(
        &fixture,
        "INSERT INTO repository_members VALUES('writer','write')",
    )
    .await?;
    let coordinator = StagingCoordinator::new(fixture.target.clone(), StagingLimits::default())?;
    let ticket = submit(&fixture, &coordinator, [222; 16], "writer").await?;
    active(&ticket).await?;
    let (entered, started) = oneshot::channel();
    let work = ticket.spawn(move |_| async move {
        let _ = entered.send(());
        std::future::pending::<std::result::Result<(), StagingError>>().await
    })?;
    timeout(Duration::from_secs(10), started).await??;
    coordinator.fault_for_test(2);
    ticket.renew_for_test();
    assert!(matches!(
        terminal(&ticket).await?,
        StagingState::Uncertain(_)
    ));
    super::publishing::edit(
        &fixture,
        "UPDATE repository_members SET role='read' WHERE account='writer'",
    )
    .await?;
    coordinator.recover(&ticket)?;
    assert!(matches!(terminal(&ticket).await?, StagingState::Fenced(_)));
    assert!(work.wait().await.is_err());
    assert!(
        timeout(Duration::from_secs(10), coordinator.close_and_drain())
            .await?
            .is_empty()
    );
    assert_eq!(coordinator.stats().workers, 0);
    assert_eq!(coordinator.stats().admitted, 0);
    fixture.runtime.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn staged_service_account_operation_and_worker_bounds_preserve_rejected_ready_requests()
-> Result {
    let fixture = Fixture::new(ObjectFormat::Sha1).await?;
    super::publishing::edit(
        &fixture,
        "INSERT INTO repository_members VALUES('writer','write')",
    )
    .await?;
    let coordinator = StagingCoordinator::new(
        fixture.target.clone(),
        StagingLimits {
            operations: 3,
            per_actor: 1,
            workers: 2,
            workers_per_actor: 1,
            ..StagingLimits::default()
        },
    )?;
    let first = submit(&fixture, &coordinator, [223; 16], "owner").await?;
    active(&first).await?;
    let ready = ReadyStaging::new(
        fixture.client(),
        fixture.target.clone(),
        fixture.begin([224; 16]),
        identity()?,
    )
    .await?;
    let (error, ready) = coordinator
        .submit(ready)
        .err()
        .ok_or("actor over-admitted")?;
    assert!(matches!(error, StagingError::Capacity));
    let second = submit(&fixture, &coordinator, [225; 16], "writer").await?;
    active(&second).await?;
    let (release, wait) = oneshot::channel();
    let work = first.spawn(move |_| async move {
        wait.await.map_err(|_| StagingError::Worker)?;
        Ok(7u64)
    })?;
    assert!(matches!(
        first.spawn(|_| async { Ok(()) }),
        Err(StagingError::Capacity)
    ));
    let writer = second.spawn(|_| async { Ok(8u64) })?;
    assert_eq!(coordinator.stats().workers, 2);
    assert!(matches!(
        second.spawn(|_| async { Ok(()) }),
        Err(StagingError::Capacity)
    ));
    assert_eq!(writer.wait().await.map_err(|e| format!("writer {e}"))?, 8);
    first.stop();
    release.send(()).map_err(|_| "worker vanished")?;
    assert_eq!(work.wait().await.map_err(|e| format!("work {e}"))?, 7);
    assert!(matches!(terminal(&first).await?, StagingState::Stopped));
    let admitted = coordinator
        .submit(ready)
        .map_err(|(e, _)| format!("retry {e}"))?;
    active(&admitted).await?;
    assert_eq!(coordinator.stats().admitted, 2);
    assert_eq!(coordinator.stats().accounts, 2);
    assert!(
        timeout(Duration::from_secs(10), coordinator.close_and_drain())
            .await?
            .is_empty()
    );
    fixture.runtime.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn staged_service_worker_failure_and_panic_fence_before_binding_and_release_admission()
-> Result {
    for panic in [false, true] {
        let fixture = Fixture::new(ObjectFormat::Sha1).await?;
        let coordinator =
            StagingCoordinator::new(fixture.target.clone(), StagingLimits::default())?;
        let ticket = submit(&fixture, &coordinator, [226; 16], "owner").await?;
        let lease = active(&ticket).await?;
        let work = ticket.spawn(move |_| async move {
            assert!(!panic, "injected producer panic");
            Err::<(), _>(StagingError::Context)
        })?;
        assert!(work.wait().await.is_err());
        assert!(matches!(terminal(&ticket).await?, StagingState::Fenced(_)));
        assert!(coordinator.close_and_drain().await.is_empty());
        assert!(
            fixture
                .client()
                .query::<CheckPreparation>(&fixture.target, None, check(lease.token))
                .await?
                .output
                .is_none()
        );
        assert_eq!(coordinator.stats().workers, 0);
        fixture.runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn staged_service_owned_native_verification_hands_off_to_the_existing_private_catalog_pipeline()
-> Result {
    use crate::packs::{
        catalog::{CatalogFileLimits, CatalogFiles, CatalogIndexes},
        metadata::tests::limits,
        verification::physical::tests::prepared_for_store,
    };
    use canopy_object_storage::artifact::ArtifactStore;
    use cellule_ltx::DiskBudget;
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let fixture = Fixture::new(format).await?;
        let coordinator =
            StagingCoordinator::new(fixture.target.clone(), StagingLimits::default())?;
        let ticket = submit(&fixture, &coordinator, [227; 16], "owner").await?;
        let initial = active(&ticket).await?;
        let provider: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let store = Arc::new(ArtifactStore::new(
            Arc::clone(&provider),
            fixture.repository,
        ));
        let native = Arc::new(
            prepared_for_store(
                format,
                32,
                initial.token.artifact_operation,
                provider,
                Arc::clone(&store),
            )
            .await?,
        );
        let root = Arc::new(tempfile::TempDir::new()?);
        let budget = DiskBudget::new(256 << 20);
        let work = {
            let root = Arc::clone(&root);
            let budget = budget.clone();
            let native = Arc::clone(&native);
            ticket.spawn(move |ctx| async move {
                ctx.ensure_live()?;
                let result = super::prepare::physical(&native, root.path(), budget)
                    .await
                    .map_err(|e| StagingError::Input(e.to_string().into()))?;
                ctx.ensure_live()?;
                Ok(result)
            })?
        };
        let (witness, segments) = work.wait().await.map_err(|e| format!("native {e}"))?;
        ticket.seal()?;
        assert!(matches!(terminal(&ticket).await?, StagingState::Bound(_)));
        let indexes = Arc::new(CatalogIndexes::new(Arc::clone(&store), format));
        let files = Arc::new(CatalogFiles::new(
            fixture.root.path(),
            DiskBudget::new(64 << 20),
            store,
            format,
            CatalogFileLimits::default(),
        )?);
        let base = Arc::new(ticket.open_base(indexes, files).await?);
        let bound_work = {
            let root = root.clone();
            let budget = budget.clone();
            ticket.spawn_bound(move |_| async move {
                async {
                    let mut assembler =
                        CatalogPreparation::new(root.path(), budget.clone(), base, limits())
                            .await?;
                    assembler.begin_pack(witness)?;
                    for segment in segments {
                        assembler.add_segment(segment).await?;
                    }
                    assembler.finish_pack().await?;
                    let proof = assembler.finish().await?;
                    Ok(proof)
                }
                .await
                .map_err(|e: CatalogPreparationError| StagingError::Input(Box::new(e)))
            })?
        };
        let proof = bound_work.wait().await.map_err(|e| e.to_string())?;
        assert_eq!(proof.token(), initial.token);
        assert_eq!(proof.object_count(), native.fixture.objects.len() as u64);
        assert!(matches!(
            proof.attest(identity()?).await?.output,
            AttestationOutcome::Registered(_)
        ));
        drop(proof);
        super::prepare::cleaned(root.path(), &budget).await?;
        assert!(coordinator.close_and_drain().await.is_empty());
        fixture.runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn staged_service_automatic_renewal_runs_without_an_observer_or_manual_tick() -> Result {
    let fixture = Fixture::new(ObjectFormat::Sha256).await?;
    let coordinator = StagingCoordinator::new(
        fixture.target.clone(),
        StagingLimits {
            renew_before_ms: DEFAULT_LEASE_MS - 1000,
            ..StagingLimits::default()
        },
    )?;
    let ticket = submit(&fixture, &coordinator, [228; 16], "owner").await?;
    let lease = active(&ticket).await?;
    drop(ticket);
    let retained = coordinator.pending([228; 16]).ok_or("lost automatic job")?;
    let renewed = changed_lease(&retained, lease.expires_at_ms).await?;
    assert_eq!(renewed.token, lease.token);
    assert!(
        fixture
            .client()
            .query::<CheckPreparation>(&fixture.target, None, check(lease.token))
            .await?
            .output
            .is_none()
    );
    assert_eq!(fixture.counts().await?, (1, 1));
    retained.stop();
    assert!(matches!(terminal(&retained).await?, StagingState::Stopped));
    assert!(coordinator.close_and_drain().await.is_empty());
    fixture.runtime.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn staged_service_rejects_invalid_profiles_foreign_targets_and_duplicate_logical_requests()
-> Result {
    let fixture = Fixture::new(ObjectFormat::Sha1).await?;
    for limits in [
        StagingLimits {
            operations: 0,
            ..StagingLimits::default()
        },
        StagingLimits {
            per_actor: 32,
            ..StagingLimits::default()
        },
        StagingLimits {
            workers: 0,
            ..StagingLimits::default()
        },
        StagingLimits {
            workers_per_actor: 0,
            ..StagingLimits::default()
        },
        StagingLimits {
            workers_per_actor: 65,
            ..StagingLimits::default()
        },
        StagingLimits {
            workers: MAX_GENERATION_LEASES as usize + 1,
            ..StagingLimits::default()
        },
        StagingLimits {
            lease_ms: 0,
            ..StagingLimits::default()
        },
        StagingLimits {
            lease_ms: MAX_LEASE_MS + 1,
            ..StagingLimits::default()
        },
        StagingLimits {
            renew_before_ms: DEFAULT_LEASE_MS,
            ..StagingLimits::default()
        },
        StagingLimits {
            lifetime_ms: 1,
            ..StagingLimits::default()
        },
        StagingLimits {
            lifetime_ms: u64::MAX,
            ..StagingLimits::default()
        },
    ] {
        assert!(matches!(
            StagingCoordinator::new(fixture.target.clone(), limits),
            Err(StagingError::InvalidLimits)
        ));
    }
    let coordinator = StagingCoordinator::new(fixture.target.clone(), StagingLimits::default())?;
    let ticket = submit(&fixture, &coordinator, [229; 16], "owner").await?;
    active(&ticket).await?;
    let ready = ReadyStaging::new(
        fixture.client(),
        fixture.target.clone(),
        fixture.begin([229; 16]),
        identity()?,
    )
    .await?;
    let (error, ready) = coordinator
        .submit(ready)
        .err()
        .ok_or("duplicate accepted")?;
    assert!(matches!(error, StagingError::Duplicate));
    let foreign = Fixture::new(ObjectFormat::Sha256).await?;
    let other = StagingCoordinator::new(foreign.target.clone(), StagingLimits::default())?;
    let (error, _) = other.submit(ready).err().ok_or("foreign accepted")?;
    assert!(matches!(error, StagingError::Foreign));
    assert!(matches!(other.recover(&ticket), Err(StagingError::Foreign)));
    assert!(coordinator.close_and_drain().await.is_empty());
    let ready = ReadyStaging::new(
        fixture.client(),
        fixture.target.clone(),
        fixture.begin([230; 16]),
        identity()?,
    )
    .await?;
    assert!(matches!(
        coordinator.submit(ready),
        Err((StagingError::Closed, _))
    ));
    foreign.runtime.shutdown().await?;
    fixture.runtime.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn staged_service_revocation_drops_completed_owned_results_before_releasing_worker_credit()
-> Result {
    use std::sync::atomic::{AtomicBool, Ordering};
    struct OwnedInput {
        coordinator: StagingCoordinator,
        dropped: Arc<AtomicBool>,
        wrong_order: Arc<AtomicBool>,
    }
    impl Drop for OwnedInput {
        fn drop(&mut self) {
            if self.coordinator.stats().workers != 1 {
                self.wrong_order.store(true, Ordering::Release);
            }
            self.dropped.store(true, Ordering::Release);
        }
    }
    let fixture = Fixture::new(ObjectFormat::Sha256).await?;
    super::publishing::edit(
        &fixture,
        "INSERT INTO repository_members VALUES('writer','write')",
    )
    .await?;
    let coordinator = StagingCoordinator::new(fixture.target.clone(), StagingLimits::default())?;
    let ticket = submit(&fixture, &coordinator, [231; 16], "writer").await?;
    active(&ticket).await?;
    let dropped = Arc::new(AtomicBool::new(false));
    let wrong_order = Arc::new(AtomicBool::new(false));
    let resource = OwnedInput {
        coordinator: coordinator.clone(),
        dropped: Arc::clone(&dropped),
        wrong_order: Arc::clone(&wrong_order),
    };
    let (done, finished) = oneshot::channel();
    let work = ticket.spawn(move |_| async move {
        let _ = done.send(());
        Ok(resource)
    })?;
    timeout(Duration::from_secs(10), finished).await??;
    // Retain the observer deliberately. Neither completed input ownership nor
    // its credit may depend on dropping an external ticket after revocation.
    assert_eq!(coordinator.stats().workers, 1);
    super::publishing::edit(
        &fixture,
        "UPDATE repository_members SET role='read' WHERE account='writer'",
    )
    .await?;
    ticket.renew_for_test();
    assert!(matches!(terminal(&ticket).await?, StagingState::Fenced(_)));
    assert!(
        timeout(Duration::from_secs(10), coordinator.close_and_drain())
            .await?
            .is_empty()
    );
    assert!(dropped.load(Ordering::Acquire));
    assert!(!wrong_order.load(Ordering::Acquire));
    assert_eq!(coordinator.stats().workers, 0);
    assert!(work.wait().await.is_err());
    fixture.runtime.shutdown().await?;
    Ok(())
}
