mod preparation;
use super::*;
use super::{
    prepare::cleaned,
    publishing::{assembled, edit, plan, update},
};
use crate::{
    git_http::GitHttpResponse,
    packs::{
        catalog::{CatalogFileLimits, CatalogFiles, CatalogIndexes},
        metadata::tests::limits,
    },
};
use canopy_object_storage::artifact::ArtifactStore;
use cellule_ltx::DiskBudget;
use tokio::time::{Duration, timeout};

pub(super) fn refused() -> GitHttpResponse {
    GitHttpResponse {
        status: 400,
        headers: vec![("X-Native-Trace".into(), "exact-refusal".into())],
        body: b"native input refused\n".to_vec(),
    }
}
pub(super) fn request(response: GitHttpResponse) -> PushCompletionRequest {
    PushCompletionRequest {
        plan: None,
        response,
        options: vec!["canopy.note=queue".into()],
        certificate: None,
    }
}
fn accepted(tip: crate::ObjectId, name: &str) -> PushCompletionRequest {
    let mut report = Vec::new();
    for body in [b"unpack ok\n".to_vec(), format!("ok {name}\n").into_bytes()] {
        report.extend_from_slice(format!("{:04x}", body.len() + 4).as_bytes());
        report.extend_from_slice(&body);
    }
    report.extend_from_slice(b"0000");
    PushCompletionRequest {
        plan: Some(plan(vec![update(name, None, Some(tip))])),
        response: GitHttpResponse {
            status: 200,
            headers: vec![("X-Native-Trace".into(), "exact-acceptance".into())],
            body: report,
        },
        options: vec![],
        certificate: None,
    }
}
pub(super) fn finished(
    state: PublicationState,
) -> Result<cellule_runtime::Committed<CatalogCompletionReply>> {
    match state {
        PublicationState::Finished(Ok(PublicationOutcome::Push(value))) => Ok(value),
        other => Err(format!("unexpected {other:?}").into()),
    }
}
async fn empty(
    fixture: &Fixture,
    operation: [u8; 16],
    actor: &str,
) -> Result<(Arc<PreparedCatalog>, tempfile::TempDir, DiskBudget)> {
    let store = Arc::new(ArtifactStore::new(
        Arc::new(InMemory::new()),
        fixture.repository,
    ));
    empty_in_store(fixture, operation, actor, store).await
}
pub(super) async fn empty_in_store(
    fixture: &Fixture,
    operation: [u8; 16],
    actor: &str,
    store: Arc<ArtifactStore>,
) -> Result<(Arc<PreparedCatalog>, tempfile::TempDir, DiskBudget)> {
    let mut input = fixture.begin(operation);
    input.actor = actor.into();
    let started = fixture
        .client()
        .command::<BeginPreparation>(&fixture.target, identity()?, input)
        .await?;
    let token = lease(started.output)?.token;
    let root = tempfile::TempDir::new()?;
    let budget = DiskBudget::new(256 << 20);
    let indexes = Arc::new(CatalogIndexes::new(Arc::clone(&store), fixture.format));
    let files = Arc::new(CatalogFiles::new(
        root.path(),
        budget.clone(),
        store,
        fixture.format,
        CatalogFileLimits::default(),
    )?);
    let base = Arc::new(
        PreparationBaseResolver::open(
            fixture.client(),
            fixture.target.clone(),
            LeaseCheck {
                token,
                actor: actor.into(),
            },
            indexes,
            files,
            Some(started.receipt),
        )
        .await?,
    );
    let prepared = Arc::new(
        CatalogPreparation::new(root.path(), budget.clone(), base, limits())
            .await?
            .finish()
            .await?,
    );
    Ok((prepared, root, budget))
}

#[tokio::test]
async fn canceled_observer_does_not_cancel_admitted_native_publication_or_release_scratch() -> Result
{
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let fixture = Fixture::new(format).await?;
        let graph = assembled(&fixture, [60; 16], 0).await?;
        let prepared = Arc::new(graph.prepared);
        let weak = Arc::downgrade(&prepared);
        let ready = Box::pin(prepared.ready_push(
            identity()?,
            accepted(graph.initial, "refs/heads/queued"),
            graph.root.path(),
            graph.budget.clone(),
            limits(),
        ))
        .await?;
        let coordinator =
            PublicationCoordinator::new(fixture.target.clone(), PublicationLimits::default())?;
        let (release, entered) = coordinator.pause_for_test().await;
        let ticket = coordinator.submit(ready).await?;
        timeout(Duration::from_secs(5), entered).await??;
        assert!(matches!(ticket.state(), PublicationState::Running));
        assert_eq!(coordinator.reservations_for_test().await, (1, 8 << 20, 1));
        drop(ticket);
        drop(prepared);
        assert!(weak.upgrade().is_some());
        assert_eq!(
            graph.budget.used(),
            crate::packs::metadata::growth::INITIAL_BYTES * 3
        );
        release.send(()).map_err(|_| "worker disappeared")?;
        assert!(
            timeout(Duration::from_secs(10), coordinator.close_and_drain())
                .await?
                .is_empty()
        );
        assert_eq!(coordinator.reservations_for_test().await, (0, 0, 0));
        assert!(weak.upgrade().is_none());
        assert_eq!(
            replay_push_response(
                &fixture.client(),
                &fixture.target,
                fixture.begin([60; 16]),
                None
            )
            .await?,
            Some(accepted(graph.initial, "refs/heads/queued").response)
        );
        cleaned(graph.root.path(), &graph.budget).await?;
        fixture.runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn admission_accounts_for_running_and_queued_work_without_losing_rejected_ready_input()
-> Result {
    let fixture = Fixture::new(ObjectFormat::Sha256).await?;
    edit(
        &fixture,
        "INSERT INTO repository_members VALUES('writer','write')",
    )
    .await?;
    let coordinator = PublicationCoordinator::new(
        fixture.target.clone(),
        PublicationLimits {
            operations: 5,
            per_actor: 2,
            command_bytes: (24 << 20) + (8 << 10),
            in_flight: 1,
            maintenance_operations: 1,
            maintenance_in_flight: 1,
            foreground_burst: 3,
        },
    )?;
    let (release, entered) = coordinator.pause_for_test().await;
    let mut attempts = Vec::new();
    for (n, actor) in [
        (61, "owner"),
        (62, "owner"),
        (63, "owner"),
        (64, "writer"),
        (65, "writer"),
    ] {
        let (prepared, root, budget) = empty(&fixture, [n; 16], actor).await?;
        let ready = Box::pin(prepared.ready_push(
            identity()?,
            request(refused()),
            root.path(),
            budget.clone(),
            limits(),
        ))
        .await?;
        attempts.push((prepared, root, budget, Some(ReadyPublication::from(ready))));
    }
    let first = coordinator
        .submit(attempts[0].3.take().ok_or("ready")?)
        .await?;
    timeout(Duration::from_secs(5), entered).await??;
    let duplicate = Box::pin(attempts[0].0.ready_push(
        identity()?,
        request(refused()),
        attempts[0].1.path(),
        attempts[0].2.clone(),
        limits(),
    ))
    .await?;
    assert_eq!(
        coordinator
            .submit(duplicate)
            .await
            .err()
            .ok_or("duplicate admitted")?
            .reason,
        PublicationScheduleError::Duplicate
    );
    let second = coordinator
        .submit(attempts[1].3.take().ok_or("ready")?)
        .await?;
    let failure = coordinator
        .submit(attempts[2].3.take().ok_or("ready")?)
        .await
        .err()
        .ok_or("account quota")?;
    assert_eq!(failure.reason, PublicationScheduleError::Capacity);
    attempts[2].3 = Some(failure.ready);
    let third = coordinator
        .submit(attempts[3].3.take().ok_or("ready")?)
        .await?;
    let failure = coordinator
        .submit(attempts[4].3.take().ok_or("ready")?)
        .await
        .err()
        .ok_or("byte quota")?;
    assert_eq!(failure.reason, PublicationScheduleError::Capacity);
    attempts[4].3 = Some(failure.ready);
    assert_eq!(coordinator.reservations_for_test().await, (3, 24 << 20, 2));
    let foreign = PublicationCoordinator::new(
        crate::repository_target(
            fixture.target.tenant(),
            fixture.target.application(),
            uuid::Uuid::new_v4().into_bytes(),
        )?,
        PublicationLimits::default(),
    )?;
    let failure = foreign
        .submit(attempts[2].3.take().ok_or("ready")?)
        .await
        .err()
        .ok_or("foreign admitted")?;
    assert_eq!(failure.reason, PublicationScheduleError::Foreign);
    attempts[2].3 = Some(failure.ready);
    release.send(()).map_err(|_| "worker disappeared")?;
    for ticket in [first, second, third] {
        finished(timeout(Duration::from_secs(10), ticket.wait()).await?)?;
        assert_eq!(ticket.response().await?, refused());
    }
    assert_eq!(coordinator.reservations_for_test().await, (0, 0, 0));
    let retry = coordinator
        .submit(attempts[2].3.take().ok_or("retained ready")?)
        .await?;
    finished(timeout(Duration::from_secs(10), retry.wait()).await?)?;
    assert!(coordinator.close_and_drain().await.is_empty());
    let failure = coordinator
        .submit(attempts[4].3.take().ok_or("ready")?)
        .await
        .err()
        .ok_or("closed admitted")?;
    assert_eq!(failure.reason, PublicationScheduleError::Closed);
    drop(failure);
    for (prepared, root, budget, ready) in attempts {
        drop(ready);
        drop(prepared);
        cleaned(root.path(), &budget).await?;
    }
    fixture.runtime.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn unknown_absent_lost_ack_and_worker_panic_recover_exact_native_command() -> Result {
    for fault in [1, 2, 3] {
        let fixture = Fixture::new(ObjectFormat::Sha256).await?;
        let graph = assembled(&fixture, [70 + fault; 16], 0).await?;
        let prepared = Arc::new(graph.prepared);
        let weak = Arc::downgrade(&prepared);
        let native = accepted(graph.initial, "refs/heads/recover");
        let expected = native.response.clone();
        let ready = Box::pin(prepared.ready_push(
            identity()?,
            native,
            graph.root.path(),
            graph.budget.clone(),
            limits(),
        ))
        .await?;
        let coordinator =
            PublicationCoordinator::new(fixture.target.clone(), PublicationLimits::default())?;
        coordinator.fault_for_test(fault);
        let ticket = coordinator.submit(ready).await?;
        drop(prepared);
        let uncertain = timeout(Duration::from_secs(10), ticket.wait()).await?;
        assert!(matches!(uncertain, PublicationState::Uncertain(_)));
        let PublicationState::Uncertain(error) = &uncertain else {
            return Err("no retained evidence".into());
        };
        let PublicationError::Push(InvocationError::Pending(evidence)) = error.as_ref() else {
            return Err("no exact mutation evidence".into());
        };
        let original_sequence = match fixture.client().resolve(evidence).await? {
            cellule_runtime::Resolution::Committed(value) => Some(value.commit_sequence()),
            cellule_runtime::Resolution::Absent => None,
            other => return Err(format!("unexpected resolution {other:?}").into()),
        };
        assert_eq!(original_sequence.is_some(), fault != 1);
        // Advance unrelated SQL after a committed result. Recovery must retain
        // the original result's receipt, not substitute this later observation.
        edit(
            &fixture,
            "UPDATE ref_generation SET visibility='public' WHERE singleton=1",
        )
        .await?;
        let stats = coordinator.stats().await;
        assert_eq!(
            (
                stats.admitted,
                stats.queued,
                stats.in_flight,
                stats.uncertain
            ),
            (1, 0, 0, 1)
        );
        assert_eq!(coordinator.reservations_for_test().await, (1, 8 << 20, 1));
        assert_eq!(
            graph.budget.used(),
            crate::packs::metadata::growth::INITIAL_BYTES * 3
        );
        assert!(weak.upgrade().is_some());
        drop(ticket);
        let retained = coordinator
            .pending([70 + fault; 16])
            .await
            .ok_or("uncertain input lost")?;
        let drained = coordinator.close_and_drain().await;
        assert_eq!(drained.len(), 1);
        let before = replay_push_response(
            &fixture.client(),
            &fixture.target,
            fixture.begin([70 + fault; 16]),
            None,
        )
        .await?;
        assert_eq!(before.is_some(), fault != 1);
        coordinator.recover(&retained).await?;
        let completed = finished(timeout(Duration::from_secs(10), retained.wait()).await?)?;
        if let Some(sequence) = original_sequence {
            assert_eq!(completed.receipt.commit_sequence, sequence);
        }
        assert_eq!(retained.response().await?, expected);
        assert!(matches!(
            completed.output,
            CatalogCompletionReply::Completed(CompletedCatalogPush {
                rejected: false,
                publication: Some(PublishedRefs {
                    generation: 1,
                    ref_generation: 1,
                    ..
                }),
                ..
            })
        ));
        assert_eq!(coordinator.reservations_for_test().await, (0, 0, 0));
        assert!(weak.upgrade().is_none());
        assert_eq!(
            coordinator.recover(&retained).await,
            Err(PublicationScheduleError::NotUncertain)
        );
        // Resolved tickets carry only small receipt/read context, not scratch.
        assert!(coordinator.close_and_drain().await.is_empty());
        cleaned(graph.root.path(), &graph.budget).await?;
        fixture.runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn stale_ready_command_has_durable_conflict_then_reconciliation_can_reenter_queue() -> Result
{
    let fixture = Fixture::new(ObjectFormat::Sha256).await?;
    let provider: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let store = Arc::new(ArtifactStore::new(
        Arc::clone(&provider),
        fixture.repository,
    ));
    let first = super::reconcile::graph(
        &fixture,
        Arc::clone(&provider),
        Arc::clone(&store),
        [80; 16],
        4,
    )
    .await?;
    let second = super::reconcile::graph(&fixture, provider, store, [81; 16], 5).await?;
    let a = Arc::new(first.prepared);
    let b = Arc::new(second.prepared);
    let a_ready = Box::pin(a.ready_push(
        identity()?,
        accepted(first.initial, "refs/heads/a"),
        first.root.path(),
        first.budget.clone(),
        limits(),
    ))
    .await?;
    let b_ready = Box::pin(b.ready_push(
        identity()?,
        accepted(second.initial, "refs/heads/b"),
        second.root.path(),
        second.budget.clone(),
        limits(),
    ))
    .await?;
    let coordinator = PublicationCoordinator::new(
        fixture.target.clone(),
        PublicationLimits {
            in_flight: 1,
            maintenance_in_flight: 1,
            ..PublicationLimits::default()
        },
    )?;
    let (release, entered) = coordinator.pause_for_test().await;
    let a_ticket = coordinator.submit(a_ready).await?;
    entered.await?;
    let old_ticket = coordinator.submit(b_ready).await?;
    release.send(()).map_err(|_| "worker disappeared")?;
    finished(a_ticket.wait().await)?;
    let old = old_ticket.wait().await;
    assert!(
        matches!(old, PublicationState::Finished(Err(ref error)) if matches!(error.as_ref(), PublicationError::Push(InvocationError::Rejected(value)) if value.output==CatalogCompletionReply::Denied(PreparationDenial::Conflict)))
    );
    assert_eq!(
        replay_push_response(
            &fixture.client(),
            &fixture.target,
            fixture.begin([81; 16]),
            None
        )
        .await?,
        None
    );
    let reconciled = Arc::new(b.reconcile().await?);
    assert_eq!(reconciled.base().generation, 1);
    assert_eq!(reconciled.token(), b.token());
    let ready = Box::pin(reconciled.ready_push(
        identity()?,
        accepted(second.initial, "refs/heads/b"),
        second.root.path(),
        second.budget.clone(),
        limits(),
    ))
    .await?;
    let published = coordinator.submit(ready).await?;
    let value = finished(published.wait().await)?;
    assert!(matches!(
        value.output,
        CatalogCompletionReply::Completed(CompletedCatalogPush {
            publication: Some(PublishedRefs { generation: 2, .. }),
            ..
        })
    ));
    assert_eq!(
        published.response().await?,
        accepted(second.initial, "refs/heads/b").response
    );
    assert!(coordinator.close_and_drain().await.is_empty());
    drop(a);
    drop(b);
    drop(reconciled);
    cleaned(first.root.path(), &first.budget).await?;
    cleaned(second.root.path(), &second.budget).await?;
    fixture.runtime.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn queued_publication_still_evaluates_current_authorization() -> Result {
    let fixture = Fixture::new(ObjectFormat::Sha256).await?;
    let graph = assembled(&fixture, [90; 16], 0).await?;
    let prepared = Arc::new(graph.prepared);
    let mut native = accepted(graph.initial, "refs/heads/revoked");
    // report-status was declined; revocation must become an explicit HTTP
    // failure rather than an empty success response.
    native.response.body.clear();
    let ready = Box::pin(prepared.ready_push(
        identity()?,
        native,
        graph.root.path(),
        graph.budget.clone(),
        limits(),
    ))
    .await?;
    let coordinator =
        PublicationCoordinator::new(fixture.target.clone(), PublicationLimits::default())?;
    let (release, entered) = coordinator.pause_for_test().await;
    let ticket = coordinator.submit(ready).await?;
    entered.await?;
    edit(&fixture, "UPDATE repository_identity SET owner='other'; INSERT INTO repository_members VALUES('owner','read')").await?;
    release.send(()).map_err(|_| "worker disappeared")?;
    let completed = finished(ticket.wait().await)?;
    assert!(matches!(
        completed.output,
        CatalogCompletionReply::Completed(CompletedCatalogPush {
            rejected: true,
            publication: None,
            ..
        })
    ));
    assert_eq!(ticket.response().await?.status, 409);
    assert!(coordinator.close_and_drain().await.is_empty());
    drop(prepared);
    cleaned(graph.root.path(), &graph.budget).await?;
    fixture.runtime.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn bounded_dispatch_allows_another_command_to_progress_before_first_outcome() -> Result {
    let fixture = Fixture::new(ObjectFormat::Sha256).await?;
    let first = empty(&fixture, [91; 16], "owner").await?;
    let second = empty(&fixture, [92; 16], "owner").await?;
    let coordinator = PublicationCoordinator::new(
        fixture.target.clone(),
        PublicationLimits {
            in_flight: 2,
            maintenance_in_flight: 1,
            ..PublicationLimits::default()
        },
    )?;
    let (release, entered) = coordinator.pause_for_test().await;
    let ready = Box::pin(first.0.ready_push(
        identity()?,
        request(refused()),
        first.1.path(),
        first.2.clone(),
        limits(),
    ))
    .await?;
    let a = coordinator.submit(ready).await?;
    entered.await?;
    let ready = Box::pin(second.0.ready_push(
        identity()?,
        request(refused()),
        second.1.path(),
        second.2.clone(),
        limits(),
    ))
    .await?;
    let b = coordinator.submit(ready).await?;
    finished(timeout(Duration::from_secs(10), b.wait()).await?)?;
    assert!(matches!(a.state(), PublicationState::Running));
    assert_eq!(coordinator.reservations_for_test().await, (1, 8 << 20, 1));
    release.send(()).map_err(|_| "worker disappeared")?;
    finished(a.wait().await)?;
    assert!(coordinator.close_and_drain().await.is_empty());
    for (prepared, root, budget) in [first, second] {
        drop(prepared);
        cleaned(root.path(), &budget).await?;
    }
    fixture.runtime.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn oversized_inline_completion_fails_before_dispatch_while_inventory_stays_reusable() -> Result
{
    let fixture = Fixture::new(ObjectFormat::Sha256).await?;
    let (prepared, root, budget) = empty(&fixture, [93; 16], "owner").await?;
    let mut native = request(refused());
    native.response.body.resize(5 << 20, b'x');
    let error =
        Box::pin(prepared.ready_push(identity()?, native, root.path(), budget.clone(), limits()))
            .await
            .err()
            .ok_or("oversize prepared")?;
    assert!(matches!(error, PushCompletionProofError::Codec(_)));
    assert_eq!(
        replay_push_response(
            &fixture.client(),
            &fixture.target,
            fixture.begin([93; 16]),
            None
        )
        .await?,
        None
    );
    prepared.ensure_live()?;
    assert_eq!(
        budget.used(),
        crate::packs::metadata::growth::INITIAL_BYTES * 3
    );
    let ready = Box::pin(prepared.ready_push(
        identity()?,
        request(refused()),
        root.path(),
        budget.clone(),
        limits(),
    ))
    .await?;
    let coordinator =
        PublicationCoordinator::new(fixture.target.clone(), PublicationLimits::default())?;
    let ticket = coordinator.submit(ready).await?;
    finished(ticket.wait().await)?;
    assert!(coordinator.close_and_drain().await.is_empty());
    drop(prepared);
    cleaned(root.path(), &budget).await?;
    fixture.runtime.shutdown().await?;
    Ok(())
}
