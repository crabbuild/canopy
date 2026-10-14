//! Real native publication through original lifecycle-bound durable recovery.
use super::publishing::edit;
use super::root_dispatch::Context;
use super::*;
use crate::packs::metadata::tests::limits;
use cellule_runtime::Resolution;
use tokio::time::{Duration, timeout};

async fn settled(ticket: &StagingTicket) -> Result<StagingState> {
    Ok(timeout(Duration::from_secs(10), async {
        loop {
            let state = ticket.state();
            if matches!(
                state,
                StagingState::Bound(_) | StagingState::Published(_) | StagingState::Fenced(_)
            ) {
                return state;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .map_err(|error| format!("staging settle: {error}; state={:?}", ticket.state()))?)
}

pub(super) async fn qualify(
    context: Context<'_>,
    fault: u8,
    refusal_case: bool,
    late_write: bool,
) -> Result {
    let Context {
        fixture: f,
        mut prepared,
        store,
        staging,
        ticket,
        root,
        budget,
        request,
    } = context;
    let session = Arc::new(ticket.bound_session()?);
    let check = session.check.clone();
    let mut intent = Arc::new(
        prepared
            .ref_policy_preparation(
                request.plan.ok_or("native policy plan")?,
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
    let queue = PublicationCoordinator::new(
        f.target.clone(),
        PublicationLimits::default(),
        f.publication_budget.clone(),
    )?;
    let mut head = None;
    let mut terminal = None;
    for offset in [0, 128, 256] {
        let page_identity = identity()?;
        let page = intent
            .ready_page(&prepared, page_identity, offset)
            .await?
            .with_refusal(refusal.clone())?;
        let evidence = page.evidence_for_test();
        let registered = page
            .persist_recovery(store, identity()?, head.as_ref())
            .await?;
        // Equal operation/session is insufficient: both original primary and
        // fallback SDK identities/digests must match the registered bundle.
        let other = intent
            .ready_page(&prepared, identity()?, offset)
            .await?
            .with_refusal(refusal.clone())?;
        let other_evidence = other.evidence_for_test();
        let failure = other
            .bind_recovery(registered.clone(), store)
            .err()
            .ok_or("different original page bound")?;
        assert_eq!(failure.original.evidence_for_test(), other_evidence);
        assert_eq!(failure.registered.evidence(), &evidence);
        drop(failure);
        if offset == 0 {
            let different = session
                .ready_root_refusal(identity()?, store, root, budget.clone(), None)
                .await?;
            let other = intent
                .ready_page(&prepared, page_identity, offset)
                .await?
                .with_refusal(different)?;
            // Identical primary evidence with a different pre-frozen fallback
            // must still fail binding and return both original capabilities.
            assert_eq!(other.evidence_for_test(), evidence);
            let failure = other
                .bind_recovery(registered.clone(), store)
                .err()
                .ok_or("different fallback bound")?;
            assert_eq!(failure.original.evidence_for_test(), evidence);
            assert_eq!(failure.registered.evidence(), &evidence);
        }
        let cold = registered
            .clone()
            .ready(f.client(), store.clone(), f.authority())?;
        let failure = ticket
            .publish(&queue, cold)
            .err()
            .ok_or("cold recovery joined a live lifecycle")?;
        assert!(matches!(failure.reason, StagingError::Context));
        assert!(matches!(failure.ready, ReadyPublication::RootRecovery(_)));
        let foreign_store = canopy_object_storage::artifact::ArtifactStore::new(
            Arc::new(InMemory::new()),
            *uuid::Uuid::new_v4().as_bytes(),
        );
        let failure = page
            .bind_recovery(registered.clone(), &foreign_store)
            .err()
            .ok_or("foreign artifact namespace bound")?;
        assert_eq!(failure.registered.evidence(), &evidence);
        let page = failure.original;
        let bound = page.bind_recovery(registered.clone(), store)?;
        let failure = ticket
            .publish(&queue, bound)
            .err()
            .ok_or("durable page entered final role")?;
        assert!(matches!(failure.reason, StagingError::Context));
        let ReadyPublication::BoundRecovery(bound) = failure.ready else {
            return Err("bound page lost on role refusal".into());
        };
        if offset == 0 {
            queue.fault_for_test(fault);
            if refusal_case {
                edit(
                    f,
                    "INSERT INTO branch_rules VALUES('refs/heads/main',1,1,0,0,0,0)",
                )
                .await?;
            }
        }
        let (release, wait) = tokio::sync::oneshot::channel();
        let (entered, running) = tokio::sync::oneshot::channel();
        let worker = if offset == 0 {
            Some(ticket.spawn_bound(move |_, _context| async move {
                let _ = entered.send(());
                wait.await.map_err(|_| StagingError::Worker)?;
                Ok(42u64)
            })?)
        } else {
            None
        };
        if worker.is_some() {
            timeout(Duration::from_secs(10), running).await??;
        }
        let observer = ticket.register_policy_page(&queue, bound)?;
        if let Some(worker) = worker {
            assert!(matches!(observer.state(), PublicationState::Held));
            assert_eq!(queue.stats().await.command_bytes, 544 << 10);
            assert!(matches!(
                f.client().resolve(&evidence).await?,
                Resolution::Absent
            ));
            let task_id = worker.id();
            drop(worker);
            drop(observer);
            release.send(()).map_err(|_| "held producer disappeared")?;
            assert_eq!(
                ticket
                    .pending_task::<u64>(task_id)
                    .ok_or("owned result lost")?
                    .wait()
                    .await
                    .map_err(|error| format!("owned result: {error:?}"))?,
                42
            );
        } else {
            drop(observer);
        }
        let observer = ticket
            .pending_publication()
            .ok_or("canceled durable observer lost")?;
        if offset == 0 && fault != 0 {
            assert!(matches!(
                observer.wait().await,
                PublicationState::Uncertain(_)
            ));
            assert_eq!(queue.stats().await.command_bytes, 544 << 10);
            assert_eq!(queue.stats().await.admitted, 1);
            let weak_prepared = Arc::downgrade(&prepared);
            let weak_intent = Arc::downgrade(&intent);
            drop(prepared);
            drop(intent);
            prepared = weak_prepared
                .upgrade()
                .ok_or("original catalog custody released while unknown")?;
            intent = weak_intent
                .upgrade()
                .ok_or("original policy intent released while unknown")?;
            Box::pin(super::recovery_discovery::leaves_live_owner(
                f, store, &queue,
            ))
            .await?;
            // Recovery owns the same fence/clock and original policy intent.
            staging.recover(ticket)?;
            let resumed = settled(ticket).await?;
            assert!(matches!(
                resumed,
                StagingState::Bound(_) | StagingState::Published(_)
            ));
        }
        let observed = timeout(Duration::from_secs(10), observer.wait()).await?;
        let PublicationState::Finished(Ok(result)) = observed else {
            return Err(format!("durable page offset={offset} fault={fault}: {observed:?}").into());
        };
        assert_eq!(queue.stats().await.command_bytes, 0);
        assert_eq!(queue.stats().await.admitted, 0);
        if refusal_case {
            let PublicationOutcome::RootPush(value) = result else {
                return Err("refused page resumed Bound".into());
            };
            terminal = Some(value);
            assert!(matches!(settled(ticket).await?, StagingState::Published(_)));
            break;
        }
        let PublicationOutcome::PolicyPage(value) = result else {
            return Err("valid page ended lifecycle".into());
        };
        assert!(
            matches!(value.output, RefPolicyReply::Registered(progress) if progress.valid && progress.next == (offset + 128).min(257) as u64)
        );
        assert!(matches!(settled(ticket).await?, StagingState::Bound(_)));
        let actual = registered
            .dispatch_any(
                &f.client(),
                store,
                &f.authority(),
                &std::sync::atomic::AtomicBool::new(false),
            )
            .await?;
        let PublicationOutcome::PolicyPage(actual) = actual else {
            return Err("original page receipt lost".into());
        };
        assert_eq!(
            (actual.output, actual.receipt),
            (value.output, value.receipt)
        );
        head = Some(registered);
    }
    if !refusal_case {
        let head = head.ok_or("last registered policy page")?;
        let ready = if late_write {
            edit(
                f,
                "UPDATE repository_identity SET owner='replacement' WHERE singleton=1",
            )
            .await?;
            Arc::try_unwrap(refusal).map_err(|_| "refusal retained after known pages")?
        } else {
            // Root preparation uses the same owned producer boundary as native
            // input work; the returned private output retains the original base.
            let owner = prepared.clone();
            let policy = intent.clone();
            let directory = root.to_path_buf();
            let disk = budget.clone();
            let mutation = identity()?;
            ticket
                .spawn_bound(move |_, _context| async move {
                    let guard = policy
                        .ready(&owner)
                        .await
                        .map_err(|error| StagingError::Input(Box::new(error)))?;
                    owner
                        .ready_root_push(mutation, &guard, &directory, disk, limits(), None)
                        .await
                        .map_err(|error| StagingError::Input(Box::new(error)))
                })?
                .wait()
                .await
                .map_err(|error| format!("owned durable root: {error:?}"))?
        };
        let registered = ready
            .persist_recovery_after(store, identity()?, &head)
            .await?;
        let evidence = registered.evidence().clone();
        let bound = ready.bind_recovery(registered.clone(), store)?;
        let failure = ticket
            .register_policy_page(&queue, bound)
            .err()
            .ok_or("root entered policy role")?;
        assert!(matches!(failure.reason, StagingError::Context));
        let ReadyPublication::BoundRecovery(bound) = failure.ready else {
            return Err("bound root lost on role refusal".into());
        };
        queue.fault_for_test(fault);
        drop(ticket.publish(&queue, bound)?);
        let observer = ticket.pending_publication().ok_or("root observer lost")?;
        if fault != 0 {
            assert!(matches!(
                observer.wait().await,
                PublicationState::Uncertain(_)
            ));
            assert_eq!(queue.stats().await.command_bytes, 32 << 10);
            assert_eq!(queue.stats().await.admitted, 1);
            if fault != 1 {
                assert_eq!(staging.close_and_drain().await.len(), 1);
                assert_eq!(queue.close_and_drain().await.len(), 1);
            }
            queue
                .pending(check.token.operation)
                .await
                .ok_or("uncertain bound root lost")?
                .recover()
                .await?;
            assert!(matches!(settled(ticket).await?, StagingState::Published(_)));
        }
        let PublicationState::Finished(Ok(PublicationOutcome::RootPush(value))) =
            timeout(Duration::from_secs(10), observer.wait()).await?
        else {
            return Err("bound root did not settle".into());
        };
        let loaded = RegisteredRootRecovery::load(&f.client(), &f.target, store, &check)
            .await?
            .ok_or("durable terminal record")?;
        assert_eq!(loaded.evidence(), &evidence);
        let actual = loaded.dispatch(&f.client(), store, &f.authority()).await?;
        assert_eq!(
            (&actual.output, actual.receipt),
            (&value.output, value.receipt)
        );
        if late_write {
            assert!(matches!(
                observer.root_response(store).await,
                Err(RootPushReplayError::Denied(PreparationDenial::Unauthorized))
            ));
        }
        terminal = Some(value);
    }
    let terminal = terminal.ok_or("durable root terminal result")?;
    let RootCompletionReply::Completed(value) = terminal.output else {
        return Err("durable root completion denied".into());
    };
    assert_eq!(value.completion.rejected, refusal_case || late_write);
    assert_eq!(
        value.completion.publication.is_some(),
        !refusal_case && !late_write
    );
    assert!(matches!(settled(ticket).await?, StagingState::Published(_)));
    assert_eq!(staging.stats().admitted, 0);
    assert_eq!(staging.stats().workers, 0);
    assert_eq!(queue.stats().await.admitted, 0);
    assert_eq!(queue.stats().await.command_bytes, 0);
    assert!(staging.close_and_drain().await.is_empty());
    assert!(queue.close_and_drain().await.is_empty());
    Ok(())
}

pub(super) async fn qualify_fence(context: Context<'_>) -> Result {
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
    let intent = Arc::new(
        prepared
            .ref_policy_preparation(
                request.plan.ok_or("native policy plan")?,
                root,
                budget.clone(),
                limits(),
            )
            .await?,
    );
    let refusal = session
        .ready_root_refusal(identity()?, store, root, budget, None)
        .await?;
    let page = intent
        .ready_page(&prepared, identity()?, 0)
        .await?
        .with_refusal(refusal)?;
    let evidence = page.evidence_for_test();
    let registered = page.persist_recovery(store, identity()?, None).await?;
    let bound = page.bind_recovery(registered, store)?;
    session.fence();
    let queue = PublicationCoordinator::new(
        f.target.clone(),
        PublicationLimits::default(),
        f.publication_budget.clone(),
    )?;
    let observer = queue
        .submit(bound)
        .await
        .map_err(|error| format!("fenced original admission: {error:?}"))?;
    let result = observer.wait().await;
    assert!(
        matches!(result, PublicationState::Finished(Err(error)) if matches!(&*error, PublicationError::PolicyPage(InvocationError::NotStarted(_))))
    );
    assert!(matches!(
        f.client().resolve(&evidence).await?,
        Resolution::Absent
    ));
    assert_eq!(queue.stats().await.admitted, 0);
    assert!(queue.close_and_drain().await.is_empty());
    ticket.stop();
    assert!(staging.close_and_drain().await.is_empty());
    Ok(())
}

pub(super) async fn qualify_revoked(context: Context<'_>, root_case: bool) -> Result {
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
    let intent = Arc::new(
        prepared
            .ref_policy_preparation(
                request.plan.ok_or("native policy plan")?,
                root,
                budget.clone(),
                limits(),
            )
            .await?,
    );
    let refusal = Arc::new(
        session
            .ready_root_refusal(identity()?, store, root, budget.clone(), None)
            .await?,
    );
    let queue = PublicationCoordinator::new(
        f.target.clone(),
        PublicationLimits::default(),
        f.publication_budget.clone(),
    )?;
    let mut head = None;
    let mut observer = None;
    for offset in [0, 128, 256] {
        let page = intent
            .ready_page(&prepared, identity()?, offset)
            .await?
            .with_refusal(refusal.clone())?;
        let evidence = page.evidence_for_test();
        let registered = page
            .persist_recovery(store, identity()?, head.as_ref())
            .await?;
        let bound = page.bind_recovery(registered.clone(), store)?;
        if !root_case {
            queue.fault_for_test(1);
        }
        let current = ticket.register_policy_page(&queue, bound)?;
        if !root_case {
            assert!(matches!(
                current.wait().await,
                PublicationState::Uncertain(_)
            ));
            assert!(matches!(
                f.client().resolve(&evidence).await?,
                Resolution::Absent
            ));
            observer = Some(current);
            break;
        }
        assert!(
            matches!(current.wait().await, PublicationState::Finished(Ok(PublicationOutcome::PolicyPage(value))) if matches!(value.output, RefPolicyReply::Registered(progress) if progress.valid))
        );
        assert!(matches!(settled(ticket).await?, StagingState::Bound(_)));
        head = Some(registered);
    }
    if root_case {
        let owner = prepared.clone();
        let directory = root.to_path_buf();
        let disk = budget;
        let mutation = identity()?;
        let ready = ticket
            .spawn_bound(move |_, _context| async move {
                let guard = intent
                    .ready(&owner)
                    .await
                    .map_err(|error| StagingError::Input(Box::new(error)))?;
                owner
                    .ready_root_push(mutation, &guard, &directory, disk, limits(), None)
                    .await
                    .map_err(|error| StagingError::Input(Box::new(error)))
            })?
            .wait()
            .await
            .map_err(|error| format!("revoked root factory: {error:?}"))?;
        let registered = ready
            .persist_recovery_after(store, identity()?, &head.ok_or("completed policy head")?)
            .await?;
        let evidence = registered.evidence().clone();
        let bound = ready.bind_recovery(registered, store)?;
        queue.fault_for_test(1);
        let current = ticket.publish(&queue, bound)?;
        assert!(matches!(
            current.wait().await,
            PublicationState::Uncertain(_)
        ));
        assert!(matches!(
            f.client().resolve(&evidence).await?,
            Resolution::Absent
        ));
        observer = Some(current);
    }
    let observer = observer.ok_or("revoked original observer")?;
    timeout(Duration::from_secs(10), async {
        while !matches!(ticket.state(), StagingState::Uncertain(_)) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    edit(
        f,
        "UPDATE repository_identity SET owner='replacement' WHERE singleton=1",
    )
    .await?;
    staging.recover(ticket)?;
    let state = settled(ticket).await?;
    assert!(
        matches!(state, StagingState::Published(_)),
        "revoked original root_case={root_case}: {state:?}"
    );
    let result = observer.wait().await;
    let PublicationState::Finished(Ok(PublicationOutcome::RootPush(value))) = result else {
        return Err(format!("revoked original root_case={root_case}: {result:?}").into());
    };
    assert!(
        matches!(value.output, RootCompletionReply::Completed(output) if output.completion.rejected && output.completion.publication.is_none())
    );
    assert!(matches!(
        observer.root_response(store).await,
        Err(RootPushReplayError::Denied(PreparationDenial::Unauthorized))
    ));
    assert_eq!(staging.stats().admitted, 0);
    assert_eq!(queue.stats().await.admitted, 0);
    assert_eq!(queue.stats().await.command_bytes, 0);
    assert!(staging.close_and_drain().await.is_empty());
    assert!(queue.close_and_drain().await.is_empty());
    Ok(())
}
