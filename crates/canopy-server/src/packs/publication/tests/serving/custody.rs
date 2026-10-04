//! Serving uses the same exact intent protocol without gaining write custody.
use super::*;
use cellule_runtime::{PendingMutation, Resolution};

fn queue(f: &Fixture) -> Result<PublicationCoordinator> {
    Ok(PublicationCoordinator::new(
        f.target.clone(),
        PublicationLimits::default(),
        f.publication_budget.clone(),
    )?)
}
async fn ready(f: &Fixture, reader: u8, actor: &str) -> Result<ReadyServingCommand> {
    let mut request = f.begin([reader; 16]);
    request.actor = actor.into();
    Ok(ReadyServingCommand::acquire(
        f.client(),
        f.target.clone(),
        request,
        identity()?,
        f.authority(),
    )
    .await?)
}
async fn result(ticket: &PublicationTicket) -> Result<Committed<ServingReply>> {
    match timeout(Duration::from_secs(10), ticket.wait()).await? {
        PublicationState::Finished(Ok(PublicationOutcome::ServingCommand(value))) => Ok(value),
        other => Err(format!("serving command: {other:?}").into()),
    }
}
async fn saved(f: &Fixture, reader: u8) -> Result<RegisteredCustody> {
    Ok(RegisteredCustody::load_for(
        &f.client(),
        &f.target,
        CustodyPurpose::Serving,
        [reader; 16],
    )
    .await?
    .ok_or("serving intent missing")?)
}
async fn expired(evidence: &PendingMutation) -> Result {
    let now = sql::now(0)?;
    if now <= evidence.identity().expires_at_ms {
        tokio::time::sleep(Duration::from_millis(u64::try_from(
            evidence.identity().expires_at_ms - now + 1,
        )?))
        .await;
    }
    Ok(())
}

#[tokio::test]
async fn readonly_serving_and_creating_same_id_keep_distinct_originals_and_no_namespace() -> Result
{
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let f = Fixture::new(format).await?;
        let store = Arc::new(ArtifactStore::new(Arc::new(InMemory::new()), f.repository));
        let fact = initialize(&f, store.clone()).await?;
        edit(
            &f,
            "INSERT INTO repository_members(account,role) VALUES('viewer','read')",
        )
        .await?;
        let creating = PreparedCustody::prepare(
            &f.client(),
            &f.target,
            CustodyAction::BeginPreparation(f.begin([220; 16])),
            identity()?,
        )
        .await?
        .register(&f.client(), identity()?)
        .await?;
        let before = f.counts().await?;
        let q = queue(&f)?;
        let serving = ready(&f, 220, "viewer").await?;
        let evidence = serving.evidence().clone();
        assert_ne!(&evidence, creating.evidence());
        let committed = result(&q.submit(serving).await?).await?;
        let lease = granted(committed.output.clone())?;
        assert_eq!(lease.fact, fact);
        assert_eq!(
            lease.token.admission_sequence,
            committed.receipt.commit_sequence
        );
        assert_eq!(f.counts().await?, before);
        assert_eq!(pin_count(&f).await?, 1);
        assert_eq!(saved(&f, 220).await?.evidence(), &evidence);
        assert_eq!(
            saved(&f, 220).await?.recover_serving(&f.client()).await?,
            committed
        );
        assert_eq!(
            RegisteredCustody::load_latest(&f.client(), &f.target, [220; 16])
                .await?
                .ok_or("creating intent lost")?
                .evidence(),
            creating.evidence()
        );
        let root = tempfile::TempDir::new()?;
        let tasks = TaskTracker::new();
        let pin = ServingPin::open(
            context(&f, store, &root, tasks.clone())?,
            lease.token,
            Some("viewer".into()),
        )
        .await?;
        assert_eq!(
            release(&f, &pin).await?.output,
            ServingReleaseReply::Released
        );
        // Historical knowledge is unchanged by physical release.
        assert_eq!(
            saved(&f, 220).await?.recover_serving(&f.client()).await?,
            committed
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
async fn acquisition_six_transport_faults_recover_exact_original_after_observer_loss() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        for fault in 1..=6 {
            let f = Fixture::new(format).await?;
            let store = Arc::new(ArtifactStore::new(Arc::new(InMemory::new()), f.repository));
            initialize(&f, store.clone()).await?;
            let before = f.counts().await?;
            let q = queue(&f)?;
            let original = ready(&f, 221, "owner").await?;
            let evidence = original.evidence().clone();
            q.fault_for_test(fault);
            let ticket = q.submit(original).await?;
            assert!(
                matches!(
                    timeout(Duration::from_secs(10), ticket.wait()).await?,
                    PublicationState::Uncertain(_)
                ),
                "fault {fault}"
            );
            assert_eq!(pin_count(&f).await?, u64::from(matches!(fault, 2 | 3)));
            assert_eq!(
                q.stats().await.command_bytes,
                crate::packs::publication::custody::RESERVATION
            );
            assert_eq!(q.stats().await.foreground, 1);
            drop(ticket);
            let ticket = q
                .pending_serving_command([221; 16])
                .await
                .ok_or("original lost")?;
            assert_eq!(q.close_and_drain().await.len(), 1);
            ticket.recover().await?;
            let committed = result(&ticket).await?;
            let lease = granted(committed.output.clone())?;
            let saved = saved(&f, 221).await?;
            assert_eq!(saved.evidence(), &evidence);
            assert_eq!(saved.recover_serving(&f.client()).await?, committed);
            assert_eq!(pin_count(&f).await?, 1);
            assert_eq!(f.counts().await?, before);
            assert_eq!(q.stats().await.command_bytes, 0);
            let root = tempfile::TempDir::new()?;
            let tasks = TaskTracker::new();
            let pin = ServingPin::open(
                context(&f, store, &root, tasks.clone())?,
                lease.token,
                Some("owner".into()),
            )
            .await?;
            release(&f, &pin).await?;
            assert!(q.close_and_drain().await.is_empty());
            tasks.close();
            tasks.wait().await;
            f.runtime.shutdown().await?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn renewal_retains_physical_drain_guard_across_all_uncertain_transports() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        for fault in 1..=6 {
            let f = Fixture::new(format).await?;
            let store = Arc::new(ArtifactStore::new(Arc::new(InMemory::new()), f.repository));
            initialize(&f, store.clone()).await?;
            let q = queue(&f)?;
            let lease = granted(
                result(&q.submit(ready(&f, 222, "owner").await?).await?)
                    .await?
                    .output,
            )?;
            let root = tempfile::TempDir::new()?;
            let tasks = TaskTracker::new();
            let pin = ServingPin::open(
                context(&f, store, &root, tasks.clone())?,
                lease.token,
                Some("owner".into()),
            )
            .await?;
            let renewal = pin
                .ready_renew("owner".into(), [17; 32], identity()?, DEFAULT_LEASE_MS)
                .await?;
            let evidence = renewal.evidence().clone();
            let held = q.try_reserve(renewal)?;
            let observer = held.clone();
            let observing = tokio::spawn(async move { observer.wait().await });
            tokio::task::yield_now().await;
            observing.abort();
            assert!(observing.await.unwrap_err().is_cancelled());
            drop(held);
            let closing_pin = pin.clone();
            let closing = tokio::spawn(async move { closing_pin.close_and_drain().await });
            tokio::task::yield_now().await;
            assert!(!closing.is_finished());
            let held = q
                .pending_serving_command([222; 16])
                .await
                .ok_or("held renewal lost")?;
            assert_eq!(q.close_and_drain().await.len(), 1);
            q.fault_for_test(fault);
            held.activate().await?;
            assert!(matches!(
                timeout(Duration::from_secs(10), held.wait()).await?,
                PublicationState::Uncertain(_)
            ));
            assert!(!closing.is_finished());
            assert_eq!(pin_count(&f).await?, 1);
            assert_eq!(
                q.stats().await.command_bytes,
                crate::packs::publication::custody::RESERVATION
            );
            held.recover().await?;
            let renewed = granted(result(&held).await?.output)?;
            assert_eq!(renewed.token, lease.token);
            assert_eq!(renewed.fact, lease.fact);
            assert!(renewed.expires_at_ms >= lease.expires_at_ms);
            assert_eq!(saved(&f, 222).await?.evidence(), &evidence);
            timeout(Duration::from_secs(5), closing).await??;
            assert!(
                pin.ready_renew("owner".into(), [17; 32], identity()?, 1)
                    .await
                    .is_err()
            );
            release(&f, &pin).await?;
            assert_eq!(pin_count(&f).await?, 0);
            assert!(q.close_and_drain().await.is_empty());
            tasks.close();
            tasks.wait().await;
            f.runtime.shutdown().await?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn discarded_unexecuted_renewal_releases_guard_and_preserves_acquisition_head() -> Result {
    let f = Fixture::new(ObjectFormat::Sha256).await?;
    let store = Arc::new(ArtifactStore::new(Arc::new(InMemory::new()), f.repository));
    initialize(&f, store.clone()).await?;
    let q = queue(&f)?;
    let acquisition = result(&q.submit(ready(&f, 223, "owner").await?).await?).await?;
    let original = saved(&f, 223).await?.evidence().clone();
    let root = tempfile::TempDir::new()?;
    let tasks = TaskTracker::new();
    let pin = ServingPin::open(
        context(&f, store, &root, tasks.clone())?,
        granted(acquisition.output)?.token,
        Some("owner".into()),
    )
    .await?;
    let renewal = pin
        .ready_renew("owner".into(), [17; 32], identity()?, 1)
        .await?;
    let evidence = renewal.evidence().clone();
    let held = q.try_reserve(renewal)?;
    let closing_pin = pin.clone();
    let closing = tokio::spawn(async move { closing_pin.close_and_drain().await });
    tokio::task::yield_now().await;
    assert!(!closing.is_finished());
    held.discard_held().await?;
    timeout(Duration::from_secs(5), closing).await??;
    assert!(matches!(
        f.client().resolve(&evidence).await?,
        Resolution::Absent
    ));
    assert_eq!(saved(&f, 223).await?.evidence(), &original);
    release(&f, &pin).await?;
    assert!(q.close_and_drain().await.is_empty());
    tasks.close();
    tasks.wait().await;
    f.runtime.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn late_serving_phase_fault_rolls_back_pin_and_sdk_acceptance_before_exact_retry() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        for ignore in [false, true] {
            let f = Fixture::new(format).await?;
            let store = Arc::new(ArtifactStore::new(Arc::new(InMemory::new()), f.repository));
            initialize(&f, store).await?;
            let prepared = PreparedCustody::prepare(
                &f.client(),
                &f.target,
                CustodyAction::AcquireServing(f.begin([224; 16])),
                identity()?,
            )
            .await?;
            let registered = prepared.register(&f.client(), identity()?).await?;
            let evidence = registered.evidence().clone();
            let raised = if ignore {
                "IGNORE"
            } else {
                "ABORT,'late serving phase fault'"
            };
            edit(&f, &format!("CREATE TRIGGER serving_phase_fault BEFORE UPDATE OF phase ON catalog_custody_commands WHEN NEW.purpose=1 BEGIN SELECT RAISE({raised}); END")).await?;
            assert!(matches!(
                registered.recover_serving(&f.client()).await,
                Err(InvocationError::NotStarted(_))
            ));
            assert_eq!(pin_count(&f).await?, 0);
            assert!(matches!(
                f.client().resolve(&evidence).await?,
                Resolution::Absent
            ));
            assert!(!saved(&f, 224).await?.settled());
            edit(&f, "DROP TRIGGER serving_phase_fault").await?;
            let committed = registered.recover_serving(&f.client()).await?;
            assert_eq!(
                granted(committed.output.clone())?.token.admission_sequence,
                committed.receipt.commit_sequence
            );
            assert_eq!(pin_count(&f).await?, 1);
            assert_eq!(registered.recover_serving(&f.client()).await?, committed);
            f.runtime.shutdown().await?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn revoked_read_is_recorded_as_original_denial_and_never_rewritten_by_restored_access()
-> Result {
    let f = Fixture::new(ObjectFormat::Sha1).await?;
    let store = Arc::new(ArtifactStore::new(Arc::new(InMemory::new()), f.repository));
    initialize(&f, store).await?;
    edit(
        &f,
        "INSERT INTO repository_members(account,role) VALUES('viewer','read')",
    )
    .await?;
    let mut request = f.begin([225; 16]);
    request.actor = "viewer".into();
    let prepared = PreparedCustody::prepare(
        &f.client(),
        &f.target,
        CustodyAction::AcquireServing(request),
        identity()?,
    )
    .await?;
    let registered = prepared.register(&f.client(), identity()?).await?;
    edit(&f, "DELETE FROM repository_members WHERE account='viewer'").await?;
    let Err(InvocationError::Rejected(first)) = registered.recover_serving(&f.client()).await
    else {
        return Err("revoked read was not recorded as denied".into());
    };
    assert_eq!(
        first.output,
        ServingReply::Denied(ServingDenial::Unauthorized)
    );
    assert!(saved(&f, 225).await?.settled());
    assert_eq!(pin_count(&f).await?, 0);
    edit(
        &f,
        "INSERT INTO repository_members(account,role) VALUES('viewer','read')",
    )
    .await?;
    let q = queue(&f)?;
    let restored =
        ReadyServingCommand::restore(f.client(), f.target.clone(), [225; 16], f.authority())
            .await?;
    assert_eq!(restored.evidence(), registered.evidence());
    let state = timeout(Duration::from_secs(5), q.submit(restored).await?.wait()).await?;
    assert!(matches!(state, PublicationState::Finished(Err(ref error))
        if matches!(&**error, PublicationError::ServingCommand(InvocationError::Rejected(value)) if **value==*first)));
    assert_eq!(pin_count(&f).await?, 0);
    assert!(q.close_and_drain().await.is_empty());
    f.runtime.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn cold_owner_restoration_preserves_grant_receipt_but_refuses_old_physical_authority()
-> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let f = Fixture::new(format).await?;
        let store = Arc::new(ArtifactStore::new(Arc::new(InMemory::new()), f.repository));
        initialize(&f, store.clone()).await?;
        let q = queue(&f)?;
        let mut mutation = identity()?;
        mutation.expires_at_ms = mutation.issued_at_ms + 1_000;
        let original = ReadyServingCommand::acquire(
            f.client(),
            f.target.clone(),
            f.begin([226; 16]),
            mutation,
            f.authority(),
        )
        .await?;
        let committed = result(&q.submit(original).await?).await?;
        let lease = granted(committed.output.clone())?;
        let evidence = saved(&f, 226).await?.evidence().clone();
        assert!(q.close_and_drain().await.is_empty());
        let (runtime, handle, client) =
            super::super::durable_recovery::restore_owner_fence(&f, lease.token.owner).await?;
        assert_ne!(handle.owner_fence(), lease.token.owner);
        expired(&evidence).await?;
        assert!(matches!(
            client.resolve(&evidence).await?,
            Resolution::Expired
        ));
        let recovered =
            RegisteredCustody::load_for(&client, &f.target, CustodyPurpose::Serving, [226; 16])
                .await?
                .ok_or("durable serving history lost")?;
        assert_eq!(recovered.evidence(), &evidence);
        assert_eq!(recovered.recover_serving(&client).await?, committed);
        let cold_q = queue(&f)?;
        let restored = ReadyServingCommand::restore(
            client.clone(),
            f.target.clone(),
            [226; 16],
            f.authority(),
        )
        .await?;
        assert_eq!(result(&cold_q.submit(restored).await?).await?, committed);
        let root = tempfile::TempDir::new()?;
        let tasks = TaskTracker::new();
        let ctx = ServingContext::new(
            client,
            f.target.clone(),
            f.authority(),
            Arc::new(CatalogIndexes::new(store.clone(), format)),
            Arc::new(CatalogFiles::new(
                root.path(),
                DiskBudget::new(64 << 20),
                store,
                format,
                CatalogFileLimits::default(),
            )?),
            ServingReadBudget::new(4, tasks.clone())?,
            "owner".into(),
        )?;
        assert!(matches!(
            ServingPin::open(ctx, lease.token, Some("owner".into())).await,
            Err(ServingReadError::Authority(PreparationBaseError::Inactive))
        ));
        handle
            .query(0, 8, |db| {
                assert_eq!(
                    db.query_row("SELECT count(*) FROM catalog_serving_pins", [], |row| row
                        .get::<_, u64>(
                        0
                    ))?,
                    1
                );
                Ok(Vec::new())
            })
            .await?;
        assert!(cold_q.close_and_drain().await.is_empty());
        tasks.close();
        tasks.wait().await;
        runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn scanner_retires_both_purposes_with_same_id_without_removing_accepted_serving_root()
-> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let f = Fixture::new(format).await?;
        let store = Arc::new(ArtifactStore::new(Arc::new(InMemory::new()), f.repository));
        initialize(&f, store.clone()).await?;
        let q = queue(&f)?;
        let live = granted(
            result(&q.submit(ready(&f, 228, "owner").await?).await?)
                .await?
                .output,
        )?;
        let mut records = Vec::new();
        for purpose in [CustodyPurpose::Creating, CustodyPurpose::Serving] {
            let mut mutation = identity()?;
            mutation.expires_at_ms = mutation.issued_at_ms + 200;
            let action = if purpose == CustodyPurpose::Creating {
                CustodyAction::BeginPreparation(f.begin([227; 16]))
            } else {
                CustodyAction::AcquireServing(f.begin([227; 16]))
            };
            let registered = PreparedCustody::prepare(&f.client(), &f.target, action, mutation)
                .await?
                .register(&f.client(), identity()?)
                .await?;
            records.push((purpose, registered));
        }
        for (_, record) in &records {
            expired(record.evidence()).await?;
        }
        let service = CustodySupervisor::start(
            f.client(),
            f.target.clone(),
            q.clone(),
            f.scans(RecoveryScanLimits {
                page: 1,
                interval: Duration::from_millis(10),
            }),
            f.authority(),
        )?;
        timeout(Duration::from_secs(10), async {
            loop {
                let mut complete = true;
                for (purpose, _) in &records {
                    complete &=
                        RegisteredCustody::load_for(&f.client(), &f.target, *purpose, [227; 16])
                            .await?
                            .ok_or("scope disappeared")?
                            .stop_fact()
                            .is_some();
                }
                if complete {
                    return Ok::<_, Box<dyn std::error::Error>>(());
                }
                tokio::task::yield_now().await;
            }
        })
        .await??;
        let stats = service.shutdown().await?;
        assert_eq!(stats.submitted, 2);
        assert_eq!(stats.failures, 0);
        for (purpose, original) in records {
            let saved = RegisteredCustody::load_for(&f.client(), &f.target, purpose, [227; 16])
                .await?
                .ok_or("stopped scope lost")?;
            assert_eq!(saved.evidence(), original.evidence());
            assert!(!saved.settled());
            assert!(saved.closed());
            assert!(matches!(saved.recover(&f.client()).await,
                Err(InvocationError::Pending(value)) if *value==*original.evidence()));
        }
        assert_eq!(pin_count(&f).await?, 1);
        let root = tempfile::TempDir::new()?;
        let tasks = TaskTracker::new();
        let pin = ServingPin::open(
            context(&f, store, &root, tasks.clone())?,
            live.token,
            Some("owner".into()),
        )
        .await?;
        release(&f, &pin).await?;
        assert!(q.close_and_drain().await.is_empty());
        tasks.close();
        tasks.wait().await;
        f.runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn serving_journal_identity_is_immutable_and_pending_quota_is_shared() -> Result {
    let f = Fixture::new(ObjectFormat::Sha256).await?;
    let prepared = PreparedCustody::prepare(
        &f.client(),
        &f.target,
        CustodyAction::AcquireServing(f.begin([229; 16])),
        identity()?,
    )
    .await?;
    let registered = prepared.register(&f.client(), identity()?).await?;
    for statement in [
        "UPDATE catalog_custody_commands SET purpose=0 WHERE purpose=1",
        "INSERT OR REPLACE INTO catalog_custody_commands SELECT * FROM catalog_custody_commands",
        "INSERT INTO catalog_custody_commands(purpose,operation,step,incarnation,request_id,intent) SELECT 0,operation,step,incarnation,request_id,intent FROM catalog_custody_commands",
        "INSERT INTO catalog_custody_commands(operation,step,incarnation,request_id,intent) VALUES(zeroblob(16),0,zeroblob(16),zeroblob(16),x'01')",
        "DELETE FROM catalog_custody_commands",
    ] {
        assert!(edit(&f, statement).await.is_err(), "{statement}");
    }
    assert_eq!(saved(&f, 229).await?.evidence(), registered.evidence());
    assert!(
        RegisteredCustody::load_latest(&f.client(), &f.target, [229; 16])
            .await?
            .is_none()
    );
    // Trusted invalid heads qualify the bounded quota probe, not recovery.
    edit(&f, "WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<1023) INSERT INTO catalog_custody_commands(purpose,operation,step,incarnation,request_id,intent) SELECT 0,CAST(printf('%016d',x) AS BLOB),0,zeroblob(16),CAST(printf('%016d',x) AS BLOB),x'01' FROM n").await?;
    for action in [
        CustodyAction::AcquireServing(f.begin([230; 16])),
        CustodyAction::BeginPreparation(f.begin([230; 16])),
    ] {
        let original =
            PreparedCustody::prepare(&f.client(), &f.target, action, identity()?).await?;
        assert!(matches!(original.register(&f.client(), identity()?).await,
            Err(CustodyError::Registration(error)) if matches!(&*error,
                InvocationError::Rejected(value) if value.output==RootRecoveryReply::Denied(PreparationDenial::Capacity))));
        assert!(matches!(
            f.client().resolve(original.evidence()).await?,
            Resolution::Absent
        ));
    }
    assert_eq!(f.counts().await?, (0, 0));
    assert_eq!(pin_count(&f).await?, 0);
    f.runtime.shutdown().await?;
    Ok(())
}
