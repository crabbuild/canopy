//! Destroy factory outputs and local SQLite, then recover solely from the pin.
use super::*;
use super::{publishing::edit, root_outcome::Context};
use crate::git_http::GitHttpResponse;
use cellule_runtime::Resolution;

pub(super) async fn qualify(context: Context<'_>, fault: u8, revoked: bool) -> Result {
    let session = context.ticket.bound_session()?;
    let ready = session
        .ready_root_outcome(
            identity()?,
            context.store,
            context.root,
            context.budget.clone(),
            None,
        )
        .await?;
    let loser = session
        .ready_root_outcome(
            identity()?,
            context.store,
            context.root,
            context.budget.clone(),
            None,
        )
        .await?;
    drop(session);
    qualify_ready(context, ready, loser, fault, revoked, None).await
}
pub(super) async fn qualify_publish(
    context: super::root_dispatch::Context<'_>,
    fault: u8,
) -> Result {
    let super::root_dispatch::Context {
        fixture,
        prepared,
        store,
        staging,
        ticket,
        root,
        budget,
        request,
    } = context;
    let pending = prepared
        .ref_policy_preparation(
            request.plan.clone().ok_or("durable publishing plan")?,
            root,
            budget.clone(),
            crate::packs::metadata::tests::limits(),
        )
        .await?;
    let previous = Box::pin(super::mandatory_registration::register_native_pages(
        fixture,
        &prepared,
        &pending,
        store,
        root,
        budget.clone(),
    ))
    .await?;
    let guard = pending.ready(&prepared).await?;
    let refusal = Arc::new(
        Arc::new(prepared.base.session.clone())
            .ready_root_refusal(identity()?, store, root, budget.clone(), None)
            .await?,
    );
    let ready = prepared
        .ready_root_push(
            identity()?,
            &guard,
            root,
            budget.clone(),
            crate::packs::metadata::tests::limits(),
            None,
        )
        .await?
        .with_refusal(refusal.clone())?;
    let loser = prepared
        .ready_root_push(
            identity()?,
            &guard,
            root,
            budget.clone(),
            crate::packs::metadata::tests::limits(),
            None,
        )
        .await?
        .with_refusal(refusal)?;
    drop(pending);
    drop(guard);
    drop(prepared);
    qualify_ready(
        Context {
            fixture,
            store,
            staging,
            ticket,
            root,
            budget,
            request,
        },
        ready,
        loser,
        fault,
        false,
        Some(previous),
    )
    .await
}
async fn qualify_ready(
    context: Context<'_>,
    ready: ReadyRootPush,
    loser: ReadyRootPush,
    fault: u8,
    revoked: bool,
    previous: Option<RegisteredRootRecovery>,
) -> Result {
    let publishing = previous.is_some();
    let Context {
        fixture: f,
        store,
        staging,
        ticket,
        request,
        ..
    } = context;
    let session = ticket.bound_session()?;
    let check = session.check.clone();
    let lookup = BeginRequest {
        repository: f.repository,
        operation: check.token.operation,
        request_digest: check.token.request_digest,
        actor: check.actor.clone(),
        lease_ms: DEFAULT_LEASE_MS,
    };
    let original = ready.evidence_for_test();
    let loaded = RegisteredRootRecovery::load(&f.client(), &f.target, store, &check).await?;
    assert_eq!(
        loaded.as_ref().map(RegisteredRootRecovery::evidence),
        previous.as_ref().map(RegisteredRootRecovery::evidence),
    );
    let registered = if fault == 1 {
        assert!(
            matches!(persist_ready(&ready, store, previous.as_ref(), identity()?, 2).await,
            Err(RootRecoveryError::Registration(error)) if matches!(*error, InvocationError::Pending(_)))
        );
        assert!(matches!(
            f.client().resolve(&original).await?,
            Resolution::Absent
        ));
        RegisteredRootRecovery::load(&f.client(), &f.target, store, &check)
            .await?
            .ok_or("uncertain registration did not persist its winner")?
    } else {
        persist_ready(&ready, store, previous.as_ref(), identity()?, 0).await?
    };
    assert_eq!(registered.evidence(), &original);
    assert_eq!(
        persist_ready(&ready, store, previous.as_ref(), identity()?, 0)
            .await?
            .evidence(),
        &original
    );
    let token = check.token;
    let persisted = f
        .handle
        .query(0, 1024, move |db| {
            Ok(db.query_row(
                "SELECT recovery FROM catalog_leases WHERE incarnation=?1 AND admission_sequence=?2 AND recovery IS NOT NULL",
                rusqlite::params![token.owner.incarnation.as_bytes().as_slice(), token.attempt],
                |row| row.get::<_, Vec<u8>>(0),
            )?)
        })
        .await?;
    let mut corrupted = persisted.clone();
    *corrupted.last_mut().ok_or("recovery certificate framing")? ^= 1;
    let mut d = BoundedDecoder::new(&corrupted, CERTIFICATE_BYTES)?;
    let certificate = RootRecoveryCertificate::decode(&mut d)?;
    d.finish()?;
    assert!(
        matches!(f.client().command::<RegisterRootRecovery>(&f.target, identity()?, certificate).await,
        Err(InvocationError::Rejected(value)) if value.output == RootRecoveryReply::Denied(PreparationDenial::Unauthorized))
    );
    // A different final command cannot replace the registered exact identity.
    assert!(
        matches!(persist_ready(&loser, store, previous.as_ref(), identity()?, 0).await,
        Err(RootRecoveryError::Registration(error)) if matches!(&*error, InvocationError::Rejected(value) if value.output == RootRecoveryReply::Denied(PreparationDenial::Conflict)))
    );
    drop(loser);
    let mut wrong_actor = check.clone();
    wrong_actor.actor = "another".into();
    assert!(
        RegisteredRootRecovery::load(&f.client(), &f.target, store, &wrong_actor)
            .await
            .is_err()
    );
    let original_result = if fault == 2 {
        Some(
            registered
                .dispatch(&f.client(), store, &f.authority())
                .await?,
        )
    } else {
        None
    };
    // Factory outputs and SDK snapshots are dropped before recovery. The
    // stopped lifecycle session is never passed to the new owner. Only this
    // check identifies the original pin; it cannot select a root or grant ACK.
    drop(registered);
    drop(loaded);
    drop(previous);
    drop(ready);
    drop(session);
    if revoked {
        edit(
            f,
            "UPDATE repository_identity SET owner='replacement' WHERE singleton=1",
        )
        .await?;
    }
    if fault == 1 {
        assert!(matches!(
            f.client().resolve(&original).await?,
            Resolution::Absent
        ));
    }
    assert!(staging.close_and_drain().await.is_empty());
    let (runtime, handle, client) = restore_owner(f, &check).await?;
    let loaded = RegisteredRootRecovery::load(&client, &f.target, store, &check)
        .await?
        .ok_or("durable record missing after restore")?;
    assert_eq!(loaded.evidence(), &original);
    let result = loaded.dispatch(&client, store, &f.authority()).await;
    if fault == 2 {
        let result = result?;
        assert!(
            matches!(&result.output, RootCompletionReply::Completed(value) if value.completion.publication.is_some() == publishing)
        );
        let expected = original_result.ok_or("original outcome missing")?;
        let queue = PublicationCoordinator::new(
            f.target.clone(),
            PublicationLimits::default(),
            f.publication_budget.clone(),
        )?;
        queue.fault_for_test(2);
        let observer = queue
            .submit(
                loaded
                    .clone()
                    .ready(client.clone(), store.clone(), f.authority())?,
            )
            .await
            .map_err(|failure| format!("durable admission: {:?}", failure.reason))?;
        assert!(
            matches!(tokio::time::timeout(std::time::Duration::from_secs(10), observer.wait()).await?,
            PublicationState::Uncertain(error) if matches!(&*error, PublicationError::RootPush(InvocationError::Pending(evidence)) if **evidence == original))
        );
        // Publishing bundles retain both the 32 KiB publication and its
        // 16 KiB frozen refusal; ref-free outcomes retain one command.
        assert_eq!(
            queue.stats().await.command_bytes,
            if publishing { 48 << 10 } else { 32 << 10 }
        );
        assert_eq!(queue.close_and_drain().await.len(), 1);
        observer.recover().await?;
        assert!(
            matches!(tokio::time::timeout(std::time::Duration::from_secs(10), observer.wait()).await?,
            PublicationState::Finished(Ok(PublicationOutcome::RootPush(ref recovered))) if recovered.receipt == expected.receipt && recovered.output == expected.output)
        );
        assert_eq!(queue.stats().await.command_bytes, 0);
        assert_eq!(queue.stats().await.admitted, 0);
        assert!(queue.close_and_drain().await.is_empty());
        assert_eq!(
            (result.output.clone(), result.receipt),
            (expected.output, expected.receipt)
        );
        assert_eq!(
            loaded
                .dispatch(&client, store, &f.authority(),)
                .await?
                .receipt,
            result.receipt
        );
        if revoked {
            assert!(matches!(
                replay_root_push_response(
                    &client,
                    &f.target,
                    lookup.clone(),
                    Some(result.receipt),
                    store
                )
                .await,
                Err(RootPushReplayError::Denied(PreparationDenial::Unauthorized))
            ));
            // Known results still resolve; this grants no current response read.
            assert_eq!(
                loaded
                    .dispatch(&client, store, &f.authority(),)
                    .await?
                    .receipt,
                result.receipt
            );
        } else {
            let value =
                replay_root_push_response(&client, &f.target, lookup, Some(result.receipt), store)
                    .await?
                    .ok_or("durable selected response")?;
            assert_eq!(read_response(value).await?, request.response);
        }
    } else {
        // Proven absence under a new owner cannot publish a stale old command.
        let denied = match result {
            Err(PublicationError::RootPush(InvocationError::Rejected(value))) => *value,
            Err(PublicationError::RootPush(InvocationError::NotStarted(_))) => {
                assert!(
                    client
                        .query::<CheckCompletedRootPush>(&f.target, None, lookup)
                        .await?
                        .output
                        .is_none()
                );
                assert_pin_retained(&handle, check.token).await?;
                runtime.shutdown().await?;
                return Ok(());
            }
            other => return Err(format!("stale absence published: {other:?}").into()),
        };
        assert_eq!(
            denied.output,
            RootCompletionReply::Denied(PreparationDenial::Stale)
        );
        assert!(matches!(loaded.dispatch(&client,
store,
&f.authority(),).await,
            Err(PublicationError::RootPush(InvocationError::Rejected(replayed))) if replayed.receipt == denied.receipt && replayed.output == denied.output));
        assert!(
            client
                .query::<CheckCompletedRootPush>(&f.target, None, lookup)
                .await?
                .output
                .is_none()
        );
    }
    assert_pin_retained(&handle, check.token).await?;
    runtime.shutdown().await?;
    Ok(())
}
pub(super) async fn read_response(
    mut value: GitHttpResponse<canopy_object_storage::artifact::ArtifactRead>,
) -> Result<GitHttpResponse> {
    let mut body = Vec::new();
    while let Some(part) = value.body.next().await? {
        body.extend_from_slice(&part);
    }
    Ok(GitHttpResponse {
        status: value.status,
        headers: value.headers,
        body,
    })
}
async fn assert_pin_retained(handle: &CellHandle, token: PreparationToken) -> Result {
    handle
        .query(0, 32, move |db| {
            assert_eq!(
                db.query_row(
                    "SELECT count(*) FROM catalog_leases WHERE incarnation=?1 AND admission_sequence=?2 AND recovery IS NOT NULL",
                    rusqlite::params![token.owner.incarnation.as_bytes().as_slice(), token.attempt],
                    |row| row.get::<_, u64>(0)
                )?,
                1
            );
            assert!(
                db.execute(
                    "UPDATE catalog_leases SET recovery=NULL WHERE incarnation=?1 AND admission_sequence=?2 AND recovery IS NOT NULL",
                    rusqlite::params![token.owner.incarnation.as_bytes().as_slice(), token.attempt]
                )
                .is_err()
            );
            assert!(
                db.execute("DELETE FROM catalog_leases WHERE incarnation=?1 AND admission_sequence=?2 AND recovery IS NOT NULL", rusqlite::params![token.owner.incarnation.as_bytes().as_slice(), token.attempt])
                    .is_err()
            );
            Ok(Vec::new())
        })
        .await?;
    Ok(())
}

pub(super) async fn restore_owner(
    f: &Fixture,
    check: &LeaseCheck,
) -> Result<(CellRuntime, CellHandle, CellClient)> {
    restore_owner_fence(f, check.token.owner).await
}
pub(super) async fn restore_owner_fence(
    f: &Fixture,
    old: OwnerFence,
) -> Result<(CellRuntime, CellHandle, CellClient)> {
    f.handle.drain().await?;
    f.runtime.shutdown().await?;
    let old_sqlite = f.root.path().join("a.sqlite");
    if old_sqlite.exists() {
        std::fs::remove_file(&old_sqlite)?;
    }
    for suffix in ["-wal", "-shm"] {
        let sidecar = f.root.path().join(format!("a.sqlite{suffix}"));
        if sidecar.exists() {
            std::fs::remove_file(sidecar)?;
        }
    }
    let session_id = SessionId::from_bytes([249; 16]);
    let runtime = CellRuntime::new(SqlWorkerPool::new(1, 4)?, 64 << 20, session_id)?;
    let authority = CellAuthority::new(f.layout.clone());
    let idle = authority
        .load(f.target.cell_id())
        .await?
        .ok_or("durable idle owner")?;
    let provision = CellCatalog::new(f.layout.clone(), f.target.tenant())
        .lookup(f.target.cell_id())
        .await?
        .ok_or("durable provision")?;
    let handle = runtime
        .acquire_idle_restored(
            provision,
            f.replica.clone(),
            authority,
            idle,
            f.root.path().join("durable-restored.sqlite"),
            Owner {
                session: session_id,
                endpoint: "https://durable-restored.invalid".into(),
            },
        )
        .await?;
    assert!(handle.owner_fence().epoch > old.epoch);
    let client = CellClient::local(f.registry.clone(), handle.clone());
    Ok((runtime, handle, client))
}

// Registration retries retain the same final command and settled predecessor.
async fn persist_ready(
    ready: &ReadyRootPush,
    store: &canopy_object_storage::artifact::ArtifactStore,
    previous: Option<&RegisteredRootRecovery>,
    mutation: MutationIdentity,
    fault: u8,
) -> std::result::Result<RegisteredRootRecovery, RootRecoveryError> {
    match previous {
        Some(previous) => {
            ready
                .persist_recovery_after_for_test(store, mutation, previous, fault)
                .await
        }
        None => {
            ready
                .persist_recovery_for_test(store, mutation, fault)
                .await
        }
    }
}
