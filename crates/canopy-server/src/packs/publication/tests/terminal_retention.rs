//! Real native closure, atomic transfer, capacity release and original receipts.
use super::*;
use super::{publishing::edit_handle, root_outcome::Context};
use canopy_object_storage::artifact::ArtifactStore;
use cellule_runtime::{CellClient, Committed, Resolution};
use tokio::time::{Duration, timeout};

async fn unrelated_recovery_pins(handle: &CellHandle, token: PreparationToken) -> Result<Vec<u8>> {
    Ok(handle.query(0, 64 << 10, move |db| {
        let mut statement = db.prepare("SELECT incarnation,admission_sequence,recovery,recovery_phase,recovery_phase_revision FROM catalog_leases WHERE recovery IS NOT NULL AND NOT(incarnation=?1 AND admission_sequence=?2) ORDER BY incarnation,admission_sequence")?;
        let pins = statement.query_map(rusqlite::params![token.owner.incarnation.as_bytes().as_slice(), token.attempt], |row| Ok((
            row.get::<_, Vec<u8>>(0)?, row.get::<_, u64>(1)?, row.get::<_, Vec<u8>>(2)?, row.get::<_, Option<Vec<u8>>>(3)?, row.get::<_, u64>(4)?
        )))?.collect::<rusqlite::Result<Vec<_>>>()?;
        serde_json::to_vec(&pins).map_err(|_| Error::Command("fixture unrelated recovery pins"))
    }).await?)
}

pub(super) async fn maintenance(
    handle: &CellHandle,
    repository: [u8; 16],
) -> std::io::Result<MaintenanceRequest> {
    let actor = handle
        .query(0, 256, |db| {
            Ok(db
                .query_row(
                    "SELECT owner FROM repository_identity WHERE singleton=1",
                    [],
                    |row| row.get::<_, String>(0),
                )?
                .into_bytes())
        })
        .await
        .map_err(std::io::Error::other)?;
    Ok(MaintenanceRequest {
        repository,
        actor: String::from_utf8(actor).map_err(std::io::Error::other)?,
        owner: handle.owner_fence(),
    })
}

// Keep this complete qualifier off the shared native setup poll stack while
// retaining the original service, ticket, artifact storage and workspace.
pub(super) fn spawn_qualify(
    fixture: Fixture,
    store: Arc<ArtifactStore>,
    staging: StagingCoordinator,
    ticket: StagingTicket,
    work: (Arc<tempfile::TempDir>, cellule_ltx::DiskBudget),
    request: PushCompletionRequest,
    case: (u8, Arc<InMemory>),
) -> tokio::task::JoinHandle<std::result::Result<(), String>> {
    tokio::spawn(async move {
        let (root, budget) = work;
        let (fault, provider) = case;
        Box::pin(qualify(
            Context {
                fixture: &fixture,
                store: &store,
                staging: &staging,
                ticket: &ticket,
                root: root.path(),
                budget,
                request,
            },
            fault,
            provider,
        ))
        .await
        .map_err(|error| error.to_string())
    })
}

pub(super) async fn qualify(context: Context<'_>, fault: u8, provider: Arc<InMemory>) -> Result {
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
    let check = session.check.clone();
    let mut mutation = identity()?;
    mutation.expires_at_ms = mutation.issued_at_ms + 8_000;
    let artifacts = store.clone();
    let directory = root.to_path_buf();
    let ready = ticket
        .spawn_bound(move |original| async move {
            original
                .ready_root_outcome(mutation, &artifacts, &directory, budget, None)
                .await
                .map_err(|error| StagingError::Input(Box::new(error)))
        })?
        .wait()
        .await
        .map_err(|error| format!("owned terminal native outcome: {error:?}"))?;
    let original = ready.evidence_for_test();
    let registered = ready.persist_recovery(store, identity()?).await?;
    // Unknown is not a closed audit root and cannot produce a release proof.
    assert!(
        registered
            .ready_terminal_release(
                f.client(),
                store,
                maintenance(&f.handle, f.repository).await?,
                identity()?
            )
            .await
            .is_err()
    );
    let completed = registered
        .dispatch(&f.client(), store, &f.authority())
        .await?;
    drop(ready);
    drop(session);
    assert!(staging.close_and_drain().await.is_empty());
    // Fill admission with unrelated pins to prove the actual quota consequence,
    // not a large-team capacity claim. No resource limit is increased.
    if fault == 1 {
        edit_handle(&f.handle, "WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<4095) INSERT INTO catalog_leases(incarnation,admission_sequence,operation,owner_epoch,artifact_operation,expires_at_ms) SELECT zeroblob(16),x,zeroblob(16),x'0000000000000001',randomblob(16),9223372036854775807 FROM n").await?;
        assert!(
            matches!(f.client().command::<BeginPreparation>(&f.target, identity()?, f.begin([191;16])).await,
            Err(InvocationError::Rejected(value)) if value.output == PreparationReply::Denied(PreparationDenial::Capacity))
        );
    }
    // Verify the selected response manifest before any release proof is minted.
    // The provider fault changes bytes/presence only, never SQL or authority.
    let RootCompletionReply::Completed(selected) = &completed.output else {
        return Err("native terminal root role".into());
    };
    let (key, body) =
        super::super::root_completion::tests::selected_body_for_test(selected.root, store).await?;
    let path = store.path(key, body.digest)?;
    use object_store::ObjectStoreExt;
    let manifest = provider.get(&path).await?.bytes().await?;
    provider.delete(&path).await?;
    assert!(
        registered
            .ready_terminal_release(
                f.client(),
                store,
                maintenance(&f.handle, f.repository).await?,
                identity()?
            )
            .await
            .is_err()
    );
    provider.put(&path, manifest.into()).await?;
    // Prepare the competitor before release, retaining its original capability.
    // Owned execution keeps this additional factory out of the shared native
    // fixture's poll frame, with the same worker stacks and resource limits.
    let competing = if fault == 1 {
        let head = registered.clone();
        let client = f.client();
        let artifacts = store.clone();
        let admin = maintenance(&f.handle, f.repository).await?;
        let mutation = identity()?;
        Some(
            tokio::spawn(async move {
                head.ready_terminal_release(client, &artifacts, admin, mutation)
                    .await
            })
            .await??,
        )
    } else {
        None
    };
    let released = Box::pin(archive(
        f,
        &f.client(),
        &f.handle,
        store,
        &registered,
        &completed,
        fault,
    ))
    .await?;
    if let Some(competing) = competing {
        assert_ne!(competing.evidence_for_test(), released.evidence_for_test());
        assert!(
            matches!(tokio::spawn(async move { competing.dispatch(false, 0).await }).await?,
            Err(PublicationError::TerminalRelease(InvocationError::Rejected(value))) if value.output == TerminalReleaseReply::Denied(PreparationDenial::Missing))
        );
    }
    if fault == 1 {
        let admitted = lease(
            f.client()
                .command::<BeginPreparation>(&f.target, identity()?, f.begin([191; 16]))
                .await?
                .output,
        )?;
        assert_eq!(admitted.token.operation, [191; 16]);
    }
    let PublicationOutcome::TerminalRelease(initial_release) =
        released.clone().dispatch(true, 0).await?
    else {
        return Err("original release role".into());
    };
    // Missing old command bodies cannot erase original receipt recovery after
    // closure. Header/audit artifacts remain; this test is not deletion authority.
    for (key, body) in registered.command_bodies_for_test() {
        let path = store.path(key, body.digest)?;
        provider.delete(&path).await?;
        for index in 0..body
            .size
            .div_ceil(canopy_object_storage::external::PART_BYTES as u64)
            .max(1)
        {
            provider
                .delete(&canopy_object_storage::external::part(&path, index))
                .await?;
        }
    }
    // Expiry of both SDK identities cannot replace the archived receipt with a
    // newly minted identity, sequence or owner fence.
    let expires = released
        .evidence_for_test()
        .identity()
        .expires_at_ms
        .max(original.identity().expires_at_ms);
    // Tokio waits on a monotonic clock; SDK expiration uses wall time. Check
    // the actual boundary after each wait rather than assuming the two clocks
    // advanced by precisely the same amount during a long receipt lifetime.
    loop {
        let now = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis())?;
        if now > expires {
            break;
        }
        tokio::time::sleep(Duration::from_millis(u64::try_from(expires - now + 1)?)).await;
    }
    assert!(matches!(
        f.client().resolve(&original).await?,
        Resolution::Expired
    ));
    assert!(matches!(
        f.client().resolve(&released.evidence_for_test()).await?,
        Resolution::Expired
    ));
    let (runtime, handle, client) = super::durable_recovery::restore_owner(f, &check).await?;
    let loaded = RegisteredRootRecovery::load(&client, &f.target, store, &check)
        .await?
        .ok_or("archived recovery missing after owner restore")?;
    assert_eq!(loaded.evidence(), &original);
    let recovered = loaded.dispatch(&client, store, &f.authority()).await?;
    assert_eq!(
        (&recovered.output, recovered.receipt),
        (&completed.output, completed.receipt)
    );
    let response = replay_root_push_response(
        &client,
        &f.target,
        BeginRequest {
            repository: f.repository,
            operation: check.token.operation,
            request_digest: check.token.request_digest,
            actor: check.actor.clone(),
            lease_ms: DEFAULT_LEASE_MS,
        },
        Some(completed.receipt),
        store,
    )
    .await?
    .ok_or("archived response missing")?;
    assert_eq!(
        super::durable_recovery::read_response(response).await?,
        request.response
    );
    // Repeat release recovery with a fresh owner/client and the ORIGINAL SDK
    // command. The app receipt wins before the now-expired SDK window/fence.
    let recovered_release = released
        .with_client_for_test(client.clone())
        .dispatch(true, 0)
        .await?;
    let PublicationOutcome::TerminalRelease(recovered_release) = recovered_release else {
        return Err("release receipt role".into());
    };
    assert_eq!(
        (&recovered_release.output, recovered_release.receipt),
        (&initial_release.output, initial_release.receipt)
    );
    assert!(
        loaded
            .ready_terminal_release(
                client.clone(),
                store,
                maintenance(&handle, f.repository).await?,
                identity()?
            )
            .await
            .is_err()
    );
    edit_handle(
        &handle,
        "UPDATE repository_identity SET owner='revoked' WHERE singleton=1",
    )
    .await?;
    assert_eq!(
        loaded
            .dispatch(&client, store, &f.authority(),)
            .await?
            .receipt,
        completed.receipt
    );
    assert!(matches!(
        replay_root_push_response(
            &client,
            &f.target,
            BeginRequest {
                repository: f.repository,
                operation: check.token.operation,
                request_digest: check.token.request_digest,
                actor: check.actor,
                lease_ms: DEFAULT_LEASE_MS,
            },
            None,
            store
        )
        .await,
        Err(RootPushReplayError::Denied(PreparationDenial::Unauthorized))
    ));
    runtime.shutdown().await?;
    Ok(())
}

pub(super) async fn archive(
    f: &Fixture,
    client: &CellClient,
    handle: &CellHandle,
    store: &ArtifactStore,
    head: &RegisteredRootRecovery,
    expected: &Committed<RootCompletionReply>,
    fault: u8,
) -> Result<ReadyTerminalRelease> {
    let token = head.token();
    let unrelated = unrelated_recovery_pins(handle, token).await?;
    let admin = maintenance(handle, f.repository).await?;
    // The certificate binds the original actor, but release needs CURRENT
    // repository administration and actual admitted owner custody separately.
    let mut wrong = admin.clone();
    wrong.actor = "untrusted".into();
    let denied = head
        .ready_terminal_release(client.clone(), store, wrong, identity()?)
        .await?;
    assert!(matches!(denied.dispatch(false, 0).await,
        Err(PublicationError::TerminalRelease(InvocationError::Rejected(value))) if value.output == TerminalReleaseReply::Denied(PreparationDenial::Unauthorized)));
    let mut wrong = admin.clone();
    wrong.owner.epoch += 1;
    let denied = head
        .ready_terminal_release(client.clone(), store, wrong, identity()?)
        .await?;
    assert!(matches!(denied.dispatch(false, 0).await,
        Err(PublicationError::TerminalRelease(InvocationError::Rejected(value))) if value.output == TerminalReleaseReply::Denied(PreparationDenial::Unauthorized)));
    let mut mutation = identity()?;
    mutation.expires_at_ms = mutation.issued_at_ms + 8_000;
    let mut ready = head
        .ready_terminal_release(client.clone(), store, admin.clone(), mutation)
        .await?;
    // Force the LAST write to fail. Archive, pin and SDK acceptance all roll
    // back together; the same original command can execute after repair.
    edit_handle(handle, "CREATE TRIGGER terminal_release_late_fault BEFORE DELETE ON catalog_leases WHEN OLD.recovery IS NOT NULL BEGIN SELECT RAISE(ABORT,'late release fault'); END").await?;
    assert!(ready.clone().dispatch(false, 0).await.is_err());
    assert!(matches!(
        client.resolve(&ready.evidence_for_test()).await?,
        Resolution::Absent
    ));
    handle
        .query(0, 128, move |db| {
            assert_eq!(
                db.query_row(
                    "SELECT count(*) FROM catalog_recovery_receipts WHERE incarnation=?1 AND admission_sequence=?2",
                    rusqlite::params![token.owner.incarnation.as_bytes().as_slice(), token.attempt],
                    |r| r.get::<_, u64>(0)
                )?,
                0
            );
            assert_eq!(
                db.query_row(
                    "SELECT count(*) FROM catalog_leases WHERE incarnation=?1 AND admission_sequence=?2 AND recovery IS NOT NULL",
                    rusqlite::params![token.owner.incarnation.as_bytes().as_slice(), token.attempt],
                    |r| r.get::<_, u64>(0)
                )?,
                1
            );
            Ok(Vec::new())
        })
        .await?;
    edit_handle(handle, "DROP TRIGGER terminal_release_late_fault").await?;
    let queue = PublicationCoordinator::new(f.target.clone(), PublicationLimits::default())?;
    queue.fault_for_test(if fault == 4 { 2 } else { fault });
    let limits = RecoveryScanLimits {
        page: 1,
        interval: Duration::from_secs(1),
    };
    let observer = if fault == 4 {
        let (release, entered) = queue.pause_for_test().await;
        let service = RecoverySupervisor::start_retiring(
            client.clone(),
            f.target.clone(),
            store.clone(),
            queue.clone(),
            limits,
            f.authority(),
            admin.clone(),
        )?;
        timeout(Duration::from_secs(10), entered).await??;
        let observer = queue
            .pending(head.token().operation)
            .await
            .ok_or("automatic retirement admission lost")?;
        let original = ready.evidence_for_test();
        ready = observer
            .terminal_release_for_test()
            .await
            .ok_or("automatic retirement role")?;
        assert_ne!(ready.evidence_for_test(), original);
        let stats = service.shutdown().await?;
        assert_eq!(stats.release_submitted, 1);
        assert_eq!(stats.submitted, 0);
        release
            .send(())
            .map_err(|_| "retirement dispatch disappeared")?;
        observer
    } else {
        queue
            .submit(ready.clone())
            .await
            .map_err(|error| format!("terminal release admission: {:?}", error.reason))?
    };
    if fault != 0 {
        assert!(matches!(
            timeout(Duration::from_secs(10), observer.wait()).await?,
            PublicationState::Uncertain(_)
        ));
        assert_eq!(queue.stats().await.command_bytes, 8192);
        assert_eq!(queue.close_and_drain().await.len(), 1);
        if fault == 3 {
            let service = RecoverySupervisor::start_retiring(
                client.clone(),
                f.target.clone(),
                store.clone(),
                queue.clone(),
                limits,
                f.authority(),
                admin.clone(),
            )?;
            service.shutdown().await?;
            assert_eq!(queue.stats().await.command_bytes, 8192);
        }
        let service = RecoverySupervisor::start_retiring(
            client.clone(),
            f.target.clone(),
            store.clone(),
            queue.clone(),
            limits,
            f.authority(),
            admin.clone(),
        )?;
        // No caller recovery request: the supervisor retries the original
        // factory-owned maintenance command even when no SQL pin remains.
        timeout(Duration::from_secs(10), async {
            while service.stats().release_recovered == 0 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await?;
        let stats = service.shutdown().await?;
        assert!(stats.release_recovered > 0);
        assert_eq!(stats.failures, 0);
        if fault != 1 {
            // A retired push has no pin. Other settled intermediate heads
            // must not admit another publication or release.
            assert_eq!(stats.scanned, stats.settled);
            assert_eq!(stats.submitted, 0);
        }
    }
    let PublicationState::Finished(Ok(PublicationOutcome::TerminalRelease(released))) =
        timeout(Duration::from_secs(10), observer.wait()).await?
    else {
        return Err("release outcome missing".into());
    };
    assert_eq!(released.output, TerminalReleaseReply::Released);
    assert_eq!(queue.stats().await.command_bytes, 0);
    assert_eq!(queue.stats().await.admitted, 0);
    assert!(queue.close_and_drain().await.is_empty());
    let PublicationOutcome::TerminalRelease(recovered) = ready.clone().dispatch(true, 0).await?
    else {
        return Err("release replay role".into());
    };
    assert_eq!(
        (&recovered.output, recovered.receipt),
        (&released.output, released.receipt)
    );
    handle.query(0, 128, move |db| {
        assert_eq!(db.query_row("SELECT count(*) FROM catalog_leases WHERE incarnation=?1 AND admission_sequence=?2 AND recovery IS NOT NULL", rusqlite::params![token.owner.incarnation.as_bytes().as_slice(), token.attempt], |r| r.get::<_, u64>(0))?, 0);
        assert_eq!(db.query_row("SELECT count(*) FROM catalog_recovery_receipts WHERE incarnation=?1 AND admission_sequence=?2", rusqlite::params![token.owner.incarnation.as_bytes().as_slice(), token.attempt], |r| r.get::<_, u64>(0))?, 1);
        for sql in ["UPDATE catalog_recovery_receipts SET recovery=NULL,recovery_phase=NULL,recovery_release=NULL WHERE recovery IS NOT NULL", "UPDATE catalog_recovery_receipts SET recovery_phase=x'01' WHERE recovery IS NOT NULL", "UPDATE catalog_recovery_receipts SET recovery_release=x'01' WHERE recovery IS NOT NULL", "DELETE FROM catalog_recovery_receipts WHERE recovery IS NOT NULL"] {
            assert!(db.execute(sql, []).is_err(), "{sql}");
        }
        Ok(Vec::new())
    }).await?;
    assert_eq!(unrelated_recovery_pins(handle, token).await?, unrelated);
    let loaded = RegisteredRootRecovery::load(
        client,
        &f.target,
        store,
        &LeaseCheck {
            token: head.token(),
            actor: "owner".into(),
        },
    )
    .await?
    .ok_or("closed head archive missing")?;
    let original = loaded.clone();
    let artifacts = store.clone();
    let reader = client.clone();
    let authority = f.authority();
    let actual = tokio::spawn(async move {
        original
            .dispatch_any(
                &reader,
                &artifacts,
                &authority,
                &std::sync::atomic::AtomicBool::new(false),
            )
            .await
    })
    .await??;
    let PublicationOutcome::RootPush(actual) = actual else {
        return Err("archived head role".into());
    };
    assert_eq!(
        (&actual.output, actual.receipt),
        (&expected.output, expected.receipt)
    );
    assert!(
        head.ready_terminal_release(client.clone(), store, admin, identity()?)
            .await
            .is_err()
    );
    Ok(ready)
}
