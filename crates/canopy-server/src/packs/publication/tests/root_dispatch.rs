use super::publishing::{edit, state};
use super::*;
use crate::git_http::GitHttpResponse;
use crate::packs::metadata::tests::limits;
use canopy_object_storage::artifact::{ArtifactRead, ArtifactStore};
use cellule_ltx::DiskBudget;
use cellule_runtime::{Committed, PendingMutation, Resolution};
use std::path::Path;
use tokio::time::{Duration, timeout};

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Loss {
    None,
    Write,
    Expiry,
}
pub(super) struct Context<'a> {
    pub fixture: &'a Fixture,
    pub prepared: Arc<PreparedCatalog>,
    pub store: &'a ArtifactStore,
    pub staging: &'a StagingCoordinator,
    pub ticket: &'a StagingTicket,
    pub root: &'a Path,
    pub budget: DiskBudget,
    pub request: PushCompletionRequest,
}
async fn response(mut value: GitHttpResponse<ArtifactRead>) -> Result<GitHttpResponse> {
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
async fn stage(ticket: &StagingTicket, allow_uncertain: bool) -> Result<StagingState> {
    Ok(timeout(Duration::from_secs(10), async {
        loop {
            let value = ticket.state();
            if matches!(value, StagingState::Published(_) | StagingState::Fenced(_))
                || allow_uncertain && matches!(value, StagingState::Uncertain(_))
            {
                return value;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?)
}
async fn resolved(
    f: &Fixture,
    evidence: &PendingMutation,
) -> Result<Option<Committed<RootCompletionReply>>> {
    Ok(match f.client().resolve(evidence).await? {
        Resolution::Absent => None,
        Resolution::Committed(value) => {
            let mut decoder = BoundedDecoder::new(value.result(), 512)?;
            let output = RootCompletionReply::decode(&mut decoder)?;
            decoder.finish()?;
            Some(Committed {
                output,
                receipt: cellule_runtime::Receipt {
                    cell: evidence.target().cell_id(),
                    incarnation: evidence.incarnation(),
                    commit_sequence: value.commit_sequence(),
                },
            })
        }
        other => return Err(format!("unexpected root resolution {other:?}").into()),
    })
}
pub(super) async fn qualify(context: Context<'_>, fault: u8, loss: Loss) -> Result {
    let Context {
        fixture: f,
        prepared,
        store,
        staging: c,
        ticket,
        root,
        budget,
        request,
    } = context;
    assert!(
        prepared
            .base
            .session
            .root_outcome_completion(store, root, budget.clone(), None)
            .await
            .is_err(),
        "publishing native intent must not become a ref-free outcome"
    );
    let token = prepared.token();
    let operation = token.operation;
    let lookup = BeginRequest {
        repository: f.repository,
        operation,
        request_digest: prepared.token().request_digest,
        actor: "owner".into(),
        lease_ms: DEFAULT_LEASE_MS,
    };
    let expected = request.response;
    let pending = prepared
        .ref_policy_preparation(
            request.plan.ok_or("native plan")?,
            root,
            budget.clone(),
            limits(),
        )
        .await?;
    let head = Box::pin(super::mandatory_registration::register_native_pages(
        f,
        &prepared,
        &pending,
        store,
        root,
        budget.clone(),
    ))
    .await?;
    let guard = pending.ready(&prepared).await?;
    drop(pending);
    let weak = Arc::downgrade(&prepared);
    let directory = root.to_owned();
    let producer_store = store.clone();
    let producer = ticket.spawn_bound(move |session| async move {
        assert!(Arc::ptr_eq(
            &prepared.base.session.deadline,
            &session.deadline
        ));
        let ready = prepared
            .ready_root_push(
                identity().map_err(|_| StagingError::Worker)?,
                &guard,
                &directory,
                budget,
                limits(),
                None,
            )
            .await
            .map_err(|error| StagingError::Input(Box::new(error)))?;
        let registered = Box::pin(ready.persist_recovery_after(
            &producer_store,
            identity().map_err(|_| StagingError::Worker)?,
            &head,
        ))
        .await
        .map_err(|error| StagingError::Input(Box::new(error)))?;
        ready
            .bind_recovery(registered, &producer_store)
            .map_err(|error| StagingError::Input(error))
    })?;
    let ready = producer.wait().await.map_err(|error| error.to_string())?;
    drop(producer);
    let evidence = ready.evidence_for_test();
    let foreign = PublicationCoordinator::new(
        crate::repository_target(
            TenantId::from_bytes([99; 16]),
            f.target.application(),
            f.repository,
        )?,
        PublicationLimits::default(),
        f.publication_budget.clone(),
    )?;
    let failure = ticket
        .publish(&foreign, ready)
        .err()
        .ok_or("foreign root admission accepted")?;
    assert!(matches!(
        failure.reason,
        StagingError::PublicationAdmission(PublicationScheduleError::Foreign)
    ));
    let ReadyPublication::BoundRecovery(ref retained) = failure.ready else {
        return Err("root ready type lost".into());
    };
    assert_eq!(retained.evidence_for_test(), evidence);
    assert_eq!(foreign.stats().await.admitted, 0);

    let p = PublicationCoordinator::new(
        f.target.clone(),
        PublicationLimits::default(),
        f.publication_budget.clone(),
    )?;
    p.fault_for_test(fault);
    let (release, wait) = tokio::sync::oneshot::channel();
    let (entered, running) = tokio::sync::oneshot::channel();
    let worker = ticket.spawn_bound(move |_| async move {
        let _ = entered.send(());
        wait.await.map_err(|_| StagingError::Worker)?;
        Ok(42u64)
    })?;
    let work_id = worker.id();
    timeout(Duration::from_secs(10), running).await??;
    ticket.renew_for_test();
    let observer = ticket.publish(&p, failure.ready)?;
    assert!(matches!(observer.state(), PublicationState::Held));
    assert!(observer.root_response(store).await.is_err());
    drop(observer);
    drop(worker);
    assert!(weak.upgrade().is_some());
    assert_eq!(p.stats().await.command_bytes, 32 << 10);
    assert_eq!(p.stats().await.held, 1);
    let closing = c.clone();
    let mut close = Some(tokio::spawn(async move { closing.close_and_drain().await }));
    timeout(Duration::from_secs(10), async {
        while !c.stats().closed || ticket.bound_renewal().is_none() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    let renewal = ticket.bound_renewal().ok_or("ordered root renewal")?;
    assert!(!close.as_ref().unwrap().is_finished());
    assert_eq!(p.close_and_drain().await.len(), 1);
    assert!(ticket.spawn_bound(|_| async { Ok(()) }).is_err());
    release.send(()).map_err(|_| "drained worker disappeared")?;
    assert_eq!(
        ticket
            .pending_task::<u64>(work_id)
            .ok_or("owned result lost")?
            .wait()
            .await
            .map_err(|error| error.to_string())?,
        42
    );
    let original = if fault == 0 {
        None
    } else {
        let StagingState::Uncertain(error) = stage(ticket, true).await? else {
            return Err("root lifecycle uncertainty lost".into());
        };
        let StagingError::Publication(error) = &*error else {
            return Err("root uncertainty kind".into());
        };
        let PublicationError::RootPush(InvocationError::Pending(actual)) = &**error else {
            return Err("root exact evidence lost".into());
        };
        assert_eq!(actual.as_ref(), &evidence);
        assert!(weak.upgrade().is_some());
        assert_eq!(p.stats().await.command_bytes, 32 << 10);
        let known = resolved(f, &evidence).await?;
        assert_eq!(known.is_some(), fault != 1);
        match loss {
            Loss::None => {}
            Loss::Write => {
                edit(
                    f,
                    "UPDATE repository_identity SET owner='replacement' WHERE singleton=1",
                )
                .await?
            }
            Loss::Expiry => {
                // Cellule's SQL deadlines use the real monotonic clock. Wait
                // for this shared local ceiling without shifting Tokio time.
                let deadline = ticket.expire_bound_for_test()?;
                tokio::time::sleep_until(deadline + Duration::from_millis(1)).await;
                assert!(
                    weak.upgrade()
                        .ok_or("root custody lost")?
                        .ensure_live()
                        .is_err()
                );
            }
        }
        assert_eq!(
            timeout(Duration::from_secs(10), close.take().unwrap())
                .await??
                .len(),
            1
        );
        assert_eq!(p.close_and_drain().await.len(), 1);
        let before = state(&f.handle)
            .await
            .map_err(|error| format!("root oracle after custody change: {error}"))?;
        // Shared-coordinator recovery must wake the lifecycle without a
        // second caller request or reconstructing the root/response identity.
        p.pending(operation)
            .await
            .ok_or("exact root command lost")?
            .recover()
            .await?;
        let stage = stage(ticket, false).await?;
        assert!(matches!(stage, StagingState::Published(_)), "{stage:?}");
        if known.is_some() {
            assert_eq!(state(&f.handle).await?, before);
        }
        known
    };
    let observer = ticket
        .pending_publication()
        .ok_or("canceled root observer lost")?;
    match observer.wait().await {
        PublicationState::Finished(Ok(PublicationOutcome::RootPush(committed))) => {
            if let Some(original) = original {
                assert_eq!(
                    (committed.output.clone(), committed.receipt),
                    (original.output, original.receipt)
                );
            }
            assert!(committed.receipt.commit_sequence > renewal.receipt.commit_sequence);
            let RootCompletionReply::Completed(ref output) = committed.output else {
                return Err("root completion denied".into());
            };
            let rejected = fault == 1 && loss == Loss::Write;
            assert_eq!(output.completion.rejected, rejected);
            assert_eq!(output.completion.publication.is_some(), !rejected);
            if loss == Loss::Write {
                assert!(matches!(
                    observer.root_response(store).await,
                    Err(RootPushReplayError::Denied(PreparationDenial::Unauthorized))
                ));
                edit(
                    f,
                    "UPDATE repository_identity SET owner='owner' WHERE singleton=1",
                )
                .await?;
            }
            let expected = if rejected {
                crate::push::report::rejected_report(&expected, crate::push::report::REJECTED)?
            } else {
                expected
            };
            assert_eq!(
                response(observer.root_response(store).await?).await?,
                expected
            );
            assert_eq!(f.counts_for(token).await?, (0, 1));
            let saved = f
                .client()
                .query::<CheckCompletedRootPush>(&f.target, Some(committed.receipt), lookup.clone())
                .await?
                .output;
            assert_eq!(saved, Some(committed.output));
        }
        PublicationState::Finished(Err(error)) if fault == 1 && loss == Loss::Expiry => {
            assert!(matches!(
                &*error,
                PublicationError::RootPush(InvocationError::NotStarted(_))
            ));
            assert!(observer.root_response(store).await.is_err());
            assert_eq!(f.counts_for(token).await?, (1, 1));
            assert!(
                f.client()
                    .query::<CheckCompletedRootPush>(&f.target, None, lookup.clone())
                    .await?
                    .output
                    .is_none()
            );
        }
        other => return Err(format!("unexpected root outcome {other:?}").into()),
    }
    if let Some(close) = close {
        assert!(timeout(Duration::from_secs(10), close).await??.is_empty());
    }
    assert!(weak.upgrade().is_none());
    assert_eq!(c.stats().admitted, 0);
    assert_eq!(c.stats().workers, 0);
    assert_eq!(p.reservations_for_test().await, (0, 0, 0));
    assert!(c.close_and_drain().await.is_empty());
    assert!(p.close_and_drain().await.is_empty());
    assert!(foreign.close_and_drain().await.is_empty());
    Ok(())
}
