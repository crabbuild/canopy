//! Real native pages, atomic denial phases and fresh-owner original receipts.
use super::publishing::edit;
use super::root_dispatch::Context;
use super::*;
use crate::packs::metadata::tests::limits;
use cellule_runtime::Resolution;

pub(super) async fn qualify(context: Context<'_>, refusal_case: bool, late_write: bool) -> Result {
    let Context {
        fixture: f,
        prepared,
        store,
        staging,
        ticket,
        root,
        budget,
        request,
    } = context;
    let session = Arc::new(ticket.bound_session()?);
    let check = session.check.clone();
    let intent = Arc::new(
        prepared
            .ref_policy_preparation(
                request.plan.clone().ok_or("native policy plan")?,
                root,
                budget.clone(),
                limits(),
            )
            .await?,
    );
    assert_eq!(intent.plan().updates.len(), 257);
    let refusal = Arc::new(
        session
            .ready_root_refusal(identity()?, store, root, budget.clone(), None)
            .await?,
    );
    let fallback_evidence = refusal.evidence_for_test();
    let mut first_identity = identity()?;
    first_identity.expires_at_ms = first_identity.issued_at_ms + 8_000;
    let page = intent
        .ready_page(&prepared, first_identity, 0)
        .await?
        .with_refusal(refusal.clone())?;
    let first_evidence = page.evidence_for_test();
    let first = page.persist_recovery(store, identity()?, None).await?;
    let flag = std::sync::atomic::AtomicBool::new(false);
    // A successor cannot be registered while the first page remains unknown.
    let next = intent
        .ready_page(&prepared, identity()?, 128)
        .await?
        .with_refusal(refusal.clone())?;
    assert!(
        next.persist_recovery(store, identity()?, Some(&first))
            .await
            .is_err()
    );
    // Premature refusal cannot consume the pre-frozen SDK identity.
    let queue = PublicationCoordinator::new(f.target.clone(), PublicationLimits::default())?;
    let observer = queue
        .submit(refusal.clone())
        .await
        .map_err(|error| format!("premature refusal admission: {:?}", error.reason))?;
    assert!(matches!(
        observer.wait().await,
        PublicationState::Finished(Err(_))
    ));
    assert!(matches!(
        f.client().resolve(&fallback_evidence).await?,
        Resolution::Absent
    ));
    assert!(queue.close_and_drain().await.is_empty());
    if refusal_case {
        edit(
            f,
            "INSERT INTO branch_rules VALUES('refs/heads/main',1,1,0,0,0,0)",
        )
        .await?;
    } else {
        edit(f, "CREATE TRIGGER phase_late_fault BEFORE UPDATE OF recovery_phase ON catalog_leases WHEN NEW.recovery_phase IS NOT NULL BEGIN SELECT RAISE(ABORT,'late phase fault'); END;").await?;
        assert!(first.dispatch_any(&f.client(), store, &flag).await.is_err());
        assert!(matches!(
            f.client().resolve(&first_evidence).await?,
            Resolution::Absent
        ));
        f.handle
            .query(0, 128, |db| {
                assert_eq!(
                    db.query_row("SELECT count(*) FROM ref_policy_guards", [], |row| row
                        .get::<_, u64>(0))?,
                    0
                );
                assert_eq!(
                    db.query_row(
                        "SELECT count(*) FROM catalog_leases WHERE recovery_phase IS NOT NULL",
                        [],
                        |row| row.get::<_, u64>(0)
                    )?,
                    0
                );
                Ok(Vec::new())
            })
            .await?;
        edit(f, "DROP TRIGGER phase_late_fault").await?;
    }
    let original = first.dispatch_any(&f.client(), store, &flag).await?;
    let mut head = first.clone();
    let mut saved_first = None;
    let expected = if refusal_case {
        let PublicationOutcome::RootPush(value) = original else {
            return Err("policy denial did not select refusal".into());
        };
        let RootCompletionReply::Completed(completed) = &value.output else {
            return Err("native refusal did not complete".into());
        };
        assert!(completed.completion.rejected);
        assert!(completed.completion.publication.is_none());
        assert!(
            next.persist_recovery(store, identity()?, Some(&head))
                .await
                .is_err()
        );
        value
    } else {
        let PublicationOutcome::PolicyPage(value) = original else {
            return Err("first page did not complete".into());
        };
        assert!(
            matches!(value.output, RefPolicyReply::Registered(progress) if progress.next==128 && progress.valid)
        );
        saved_first = Some(value);
        for offset in [128, 256] {
            let page = intent
                .ready_page(&prepared, identity()?, offset)
                .await?
                .with_refusal(refusal.clone())?;
            let registered = page
                .persist_recovery(store, identity()?, Some(&head))
                .await?;
            let PublicationOutcome::PolicyPage(value) =
                registered.dispatch_any(&f.client(), store, &flag).await?
            else {
                return Err("successor page did not complete".into());
            };
            assert!(matches!(value.output, RefPolicyReply::Registered(progress) if progress.valid));
            head = registered;
        }
        if late_write {
            let context = Context {
                fixture: f,
                prepared: prepared.clone(),
                store,
                staging,
                ticket,
                root,
                budget: budget.clone(),
                request,
            };
            let (registered, result) = Box::pin(late_write_case(
                &context, &session, &intent, &head, &refusal,
            ))
            .await?;
            head = registered;
            result
        } else {
            let guard = intent.ready(&prepared).await?;
            let mut root_identity = identity()?;
            root_identity.expires_at_ms = first_identity.expires_at_ms;
            let owner = prepared.clone();
            let directory = root.to_path_buf();
            let disk = budget.clone();
            let ready = ticket
                .spawn_bound(move |_| async move {
                    owner
                        .ready_root_push(root_identity, &guard, &directory, disk, limits(), None)
                        .await
                        .map_err(|error| StagingError::Input(Box::new(error)))
                })?
                .wait()
                .await
                .map_err(|error| format!("owned durable positive root: {error:?}"))?;
            head = ready
                .persist_recovery_after(store, identity()?, &head)
                .await?;
            head.dispatch(&f.client(), store).await?
        }
    };
    // Return the same settled page through its retained predecessor frame.
    if let Some(expected) = &saved_first {
        let PublicationOutcome::PolicyPage(actual) =
            first.dispatch_any(&f.client(), store, &flag).await?
        else {
            return Err("original page history missing".into());
        };
        assert_eq!(
            (&actual.output, actual.receipt),
            (&expected.output, expected.receipt)
        );
    }
    let head_evidence = head.evidence().clone();
    drop(next);
    drop(page);
    drop(intent);
    drop(prepared);
    drop(session);
    drop(refusal);
    drop(head);
    assert!(staging.close_and_drain().await.is_empty());
    let (runtime, handle, client) = super::durable_recovery::restore_owner(f, &check).await?;
    // Wall-clock expiry is real SDK behavior, not a synthetic transport result.
    let now = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis())?;
    if now <= first_identity.expires_at_ms {
        tokio::time::sleep(std::time::Duration::from_millis(u64::try_from(
            first_identity.expires_at_ms - now + 1,
        )?))
        .await;
    }
    assert!(matches!(
        client.resolve(&first_evidence).await?,
        Resolution::Expired
    ));
    if !refusal_case && !late_write {
        assert!(matches!(
            client.resolve(&head_evidence).await?,
            Resolution::Expired
        ));
    }
    let loaded = RegisteredRootRecovery::load(&client, &f.target, store, &check)
        .await?
        .ok_or("restored durable phase")?;
    let PublicationOutcome::RootPush(actual) = loaded.dispatch_any(&client, store, &flag).await?
    else {
        return Err("restored terminal phase missing".into());
    };
    assert_eq!(
        (&actual.output, actual.receipt),
        (&expected.output, expected.receipt)
    );
    if late_write {
        let lookup = BeginRequest {
            repository: f.repository,
            operation: check.token.operation,
            request_digest: check.token.request_digest,
            actor: check.actor.clone(),
            lease_ms: DEFAULT_LEASE_MS,
        };
        assert!(matches!(
            replay_root_push_response(&client, &f.target, lookup, Some(actual.receipt), store)
                .await,
            Err(RootPushReplayError::Denied(PreparationDenial::Unauthorized))
        ));
    }
    if let Some(expected) = saved_first {
        let PublicationOutcome::PolicyPage(actual) =
            first.dispatch_any(&client, store, &flag).await?
        else {
            return Err("expired predecessor reply missing".into());
        };
        assert_eq!(
            (actual.output, actual.receipt),
            (expected.output, expected.receipt)
        );
    }
    handle
        .query(0, 128, |db| {
            assert_eq!(
                db.query_row(
                    "SELECT count(*) FROM pushes WHERE response_root IS NOT NULL",
                    [],
                    |row| { row.get::<_, u64>(0) }
                )?,
                1
            );
            assert_eq!(
                db.query_row("SELECT count(*) FROM catalog_operations", [], |row| row
                    .get::<_, u64>(0))?,
                0
            );
            Ok(Vec::new())
        })
        .await?;
    Box::pin(query_failure(&loaded, &client, &handle, store, &expected)).await?;
    runtime.shutdown().await?;
    Ok(())
}

async fn query_failure(
    loaded: &RegisteredRootRecovery,
    client: &CellClient,
    handle: &CellHandle,
    store: &canopy_object_storage::artifact::ArtifactStore,
    expected: &cellule_runtime::Committed<RootCompletionReply>,
) -> Result {
    // The service is stopped and the original outcome has settled. Hide the
    // phase table to inject a real private-query failure without changing data.
    super::publishing::edit_handle(
        handle,
        "ALTER TABLE catalog_leases RENAME TO phase_query_fault",
    )
    .await?;
    let ready = loaded.clone().ready(client.clone(), store.clone())?;
    let reservation = ready.reservation();
    let queue = PublicationCoordinator::new(
        loaded.evidence().target().clone(),
        PublicationLimits::default(),
    )?;
    let observer = queue
        .submit(ready)
        .await
        .map_err(|error| format!("phase query failure admission: {:?}", error.reason))?;
    let state = tokio::time::timeout(std::time::Duration::from_secs(10), observer.wait()).await?;
    assert!(matches!(state, PublicationState::Uncertain(error)
        if matches!(&*error, PublicationError::Recovery { evidence, source }
            if **evidence == *loaded.evidence()
                && matches!(&**source, RootRecoveryError::Query(_)))));
    assert_eq!(queue.stats().await.command_bytes, reservation);
    assert_eq!(queue.stats().await.admitted, 1);
    assert_eq!(queue.close_and_drain().await.len(), 1);
    super::publishing::edit_handle(
        handle,
        "ALTER TABLE phase_query_fault RENAME TO catalog_leases",
    )
    .await?;
    observer.recover().await?;
    let state = tokio::time::timeout(std::time::Duration::from_secs(10), observer.wait()).await?;
    let PublicationState::Finished(Ok(PublicationOutcome::RootPush(actual))) = state else {
        return Err(format!("phase query recovery: {state:?}").into());
    };
    assert_eq!(
        (actual.output, actual.receipt),
        (expected.output.clone(), expected.receipt)
    );
    assert_eq!(queue.stats().await.command_bytes, 0);
    assert_eq!(queue.stats().await.admitted, 0);
    assert!(queue.close_and_drain().await.is_empty());
    Ok(())
}

async fn late_write_case(
    context: &Context<'_>,
    session: &Arc<PreparationSession>,
    intent: &RefPolicyPreparation,
    head: &RegisteredRootRecovery,
    refusal: &ReadyRootPush,
) -> Result<(
    RegisteredRootRecovery,
    cellule_runtime::Committed<RootCompletionReply>,
)> {
    let Context {
        fixture: f,
        prepared,
        store,
        root,
        budget,
        ..
    } = context;
    let check = &session.check;
    let guard = intent.ready(prepared).await?;
    let owner = prepared.clone();
    let original = session.clone();
    let artifacts = (*store).clone();
    let directory = root.to_path_buf();
    let disk = budget.clone();
    let positive_identity = identity()?;
    let negative_identity = identity()?;
    // The lifecycle owns expensive preparation. Awaiting its typed result
    // keeps the original factory/custody checks without nesting the complete
    // native receive fixture on the producer's poll stack.
    let worker = context.ticket.spawn_bound(move |_| async move {
        let publishing = owner
            .ready_root_push(
                positive_identity,
                &guard,
                &directory,
                disk.clone(),
                limits(),
                None,
            )
            .await
            .map_err(|error| StagingError::Input(Box::new(error)))?;
        let refusal = original
            .ready_root_refusal(negative_identity, &artifacts, &directory, disk, None)
            .await
            .map_err(|error| StagingError::Input(Box::new(error)))?;
        Ok((publishing, refusal))
    })?;
    let (publishing, different_refusal) = worker
        .wait()
        .await
        .map_err(|error| format!("late-write candidates: {error:?}"))?;
    edit(
        f,
        "UPDATE repository_identity SET owner='replacement' WHERE singleton=1",
    )
    .await?;
    for candidate in [&publishing, &different_refusal] {
        assert!(
            matches!(candidate.persist_recovery_after(store, identity()?, head).await,
                    Err(RootRecoveryError::Registration(error))
                    if matches!(&*error, InvocationError::Rejected(value)
                        if value.output == RootRecoveryReply::Denied(PreparationDenial::Unauthorized)))
        );
    }
    assert_eq!(
        RegisteredRootRecovery::load(&f.client(), &f.target, store, check)
            .await?
            .ok_or("canonical page after refused candidates")?
            .evidence(),
        head.evidence()
    );
    let registered = refusal
        .persist_recovery_after(store, identity()?, head)
        .await?;
    let result = registered.dispatch(&f.client(), store).await?;
    assert!(
        matches!(&result.output, RootCompletionReply::Completed(value)
                if value.completion.rejected && value.completion.publication.is_none())
    );
    Ok((registered, result))
}
