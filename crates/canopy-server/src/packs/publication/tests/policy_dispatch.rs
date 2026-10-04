use super::publishing::{edit, state};
use super::root_dispatch::Context;
use super::*;
use crate::packs::metadata::tests::limits;
use cellule_runtime::{PendingMutation, Resolution};
use tokio::time::{Duration, timeout};

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Loss {
    None,
    Write,
    Epoch,
    Check,
    Expiry,
    Close,
    HeldStop,
}
async fn settled(ticket: &StagingTicket, uncertain: bool) -> Result<StagingState> {
    Ok(timeout(Duration::from_secs(10), async {
        loop {
            let value = ticket.state();
            if matches!(
                value,
                StagingState::Bound(_) | StagingState::Fenced(_) | StagingState::Published(_)
            ) || uncertain && matches!(value, StagingState::Uncertain(_))
            {
                return value;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?)
}
fn roots_unchanged(before: &[u8], after: &[u8]) -> Result {
    let before: Vec<serde_json::Value> = serde_json::from_slice(before)?;
    let after: Vec<serde_json::Value> = serde_json::from_slice(after)?;
    assert_eq!(before[..6], after[..6]);
    Ok(())
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
    let expected = request.response;
    let plan = request.plan.ok_or("native plan")?;
    if loss == Loss::Check {
        let oid = plan.updates[0].new_oid.ok_or("native check target")?;
        edit(f, &format!("INSERT INTO check_contexts VALUES('ci','owner',1,1); INSERT INTO branch_rules VALUES('refs/heads/main',1,1,0,1,0,0); INSERT INTO branch_required_checks VALUES('refs/heads/main','ci'); INSERT INTO check_runs(id,oid,context,context_version,reporter,state,version,summary,created_ms,updated_ms) VALUES(X'{}',X'{}','ci',1,'owner','success',1,'',0,0)", hex::encode(uuid::Uuid::new_v4().as_bytes()), hex::encode(oid))).await?;
    }
    let pending = Arc::new(
        prepared
            .ref_policy_preparation(plan, root, budget.clone(), limits())
            .await?,
    );
    let session = Arc::new(ticket.bound_session()?);
    let refusal = Arc::new(
        Box::pin(session.ready_root_refusal(identity()?, store, root, budget.clone(), None))
            .await?,
    );
    let refusal_evidence = refusal.evidence_for_test();
    let operation = prepared.token().operation;
    let p = PublicationCoordinator::new(f.target.clone(), PublicationLimits::default())?;
    let before = state(&f.handle).await?;
    let mut head = None;
    let mut offset = 0;
    while offset < 257 {
        let last = offset == 256;
        let ready = pending
            .ready_page(&prepared, identity()?, offset)
            .await?
            .with_refusal(refusal.clone())?;
        let evidence = ready.evidence_for_test();
        let registered = ready
            .persist_recovery(store, identity()?, head.as_ref())
            .await?;
        let ready = ready.bind_recovery(registered.clone(), store)?;
        // A page cannot enter through the final-completion API.
        let failure = ticket
            .publish(&p, ready)
            .err()
            .ok_or("policy page accepted as final")?;
        assert!(matches!(failure.reason, StagingError::Context));
        let ReadyPublication::BoundRecovery(ready) = failure.ready else {
            return Err("policy ready type lost".into());
        };
        assert_eq!(ready.evidence_for_test(), evidence);
        if last {
            p.fault_for_test(fault);
        }
        let (release, wait) = tokio::sync::oneshot::channel();
        let (entered, running) = tokio::sync::oneshot::channel();
        let worker = if offset == 0 {
            Some(ticket.spawn_bound(move |_| async move {
                let _ = entered.send(());
                wait.await.map_err(|_| StagingError::Worker)?;
                Ok(42u64)
            })?)
        } else {
            None
        };
        if worker.is_some() {
            timeout(Duration::from_secs(10), running).await??;
            ticket.renew_for_test();
        }
        let observer = ticket.register_policy_page(&p, ready)?;
        if let Some(worker) = worker {
            assert!(matches!(observer.state(), PublicationState::Held));
            assert_eq!(p.stats().await.command_bytes, 544 << 10);
            assert!(ticket.bound_session().is_err());
            let id = worker.id();
            drop(worker);
            drop(observer);
            timeout(Duration::from_secs(10), async {
                while ticket.bound_renewal().is_none() {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await?;
            if loss == Loss::HeldStop {
                ticket.stop();
                release.send(()).map_err(|_| "held worker disappeared")?;
                assert_eq!(
                    ticket
                        .pending_task::<u64>(id)
                        .ok_or("worker result lost")?
                        .wait()
                        .await
                        .map_err(|error| error.to_string())?,
                    42
                );
                assert!(matches!(
                    settled(ticket, false).await?,
                    StagingState::Fenced(_)
                ));
                assert!(matches!(
                    ticket
                        .pending_publication()
                        .ok_or("held page observer lost")?
                        .state(),
                    PublicationState::Discarded
                ));
                assert!(matches!(
                    f.client().resolve(&evidence).await?,
                    Resolution::Absent
                ));
                assert_eq!(state(&f.handle).await?, before);
                assert!(c.close_and_drain().await.is_empty());
                assert!(p.close_and_drain().await.is_empty());
                return Ok(());
            }
            release.send(()).map_err(|_| "held worker disappeared")?;
            assert_eq!(
                ticket
                    .pending_task::<u64>(id)
                    .ok_or("worker result lost")?
                    .wait()
                    .await
                    .map_err(|error| error.to_string())?,
                42
            );
        } else {
            drop(observer);
        }
        if last && fault != 0 {
            let StagingState::Uncertain(error) = settled(ticket, true).await? else {
                return Err("policy uncertainty lost".into());
            };
            let StagingError::Publication(error) = &*error else {
                return Err("policy uncertainty kind".into());
            };
            assert!(matches!(&**error,
                PublicationError::PolicyPage(InvocationError::Pending(actual)) if actual.as_ref() == &evidence));
            assert_eq!(p.stats().await.command_bytes, 544 << 10);
            let known = match f.client().resolve(&evidence).await? {
                Resolution::Absent => None,
                Resolution::Committed(value) => {
                    Some((value.result().to_vec(), value.commit_sequence()))
                }
                other => return Err(format!("unexpected policy resolution {other:?}").into()),
            };
            assert_eq!(known.is_some(), fault != 1);
            let weak_prepared = Arc::downgrade(&prepared);
            let weak_intent = Arc::downgrade(&pending);
            // At this boundary only the registered page job retains the inputs.
            drop(prepared);
            drop(pending);
            let prepared = weak_prepared.upgrade().ok_or("policy inputs lost")?;
            let pending = weak_intent.upgrade().ok_or("original policy intent lost")?;
            // SQL readiness cannot bypass the original page's lifecycle barrier.
            // Retain this exact registered final command after the refused handoff;
            // recovery must not replace it with a freshly minted SDK identity.
            let final_ready = if known.is_some() {
                let guard = pending.ready(&prepared).await?;
                let final_ready = Box::pin(prepared.ready_root_push(
                    identity()?,
                    &guard,
                    root,
                    budget.clone(),
                    limits(),
                    None,
                ))
                .await?;
                let final_registered = final_ready
                    .persist_recovery_after(store, identity()?, &registered)
                    .await?;
                let final_ready = final_ready.bind_recovery(final_registered, store)?;
                let failure = ticket
                    .publish(&p, final_ready)
                    .err()
                    .ok_or("uncertain policy allowed final handoff")?;
                assert!(matches!(failure.reason, StagingError::Duplicate));
                let ReadyPublication::BoundRecovery(ready) = failure.ready else {
                    return Err("registered final command lost on refused handoff".into());
                };
                Some(ready)
            } else {
                None
            };
            match loss {
                Loss::Write => edit(f, "UPDATE repository_identity SET owner='replacement' WHERE singleton=1").await?,
                Loss::Epoch => edit(f, "INSERT INTO branch_rules VALUES('refs/heads/main',1,1,0,0,0,0)").await?,
                Loss::Check => edit(f, "UPDATE check_runs SET state='failure',version=2 WHERE context='ci'").await?,
                Loss::Expiry => edit(f, "UPDATE catalog_operations SET expires_at_ms=0; UPDATE catalog_leases SET expires_at_ms=0;").await?,
                Loss::Close => {
                    assert_eq!(c.close_and_drain().await.len(), 1);
                    assert_eq!(p.close_and_drain().await.len(), 1);
                }
                Loss::None => {}
                Loss::HeldStop => unreachable!(),
            }
            let recovery_before = state(&f.handle).await?;
            p.pending(operation)
                .await
                .ok_or("policy exact job lost")?
                .recover()
                .await?;
            let observer = ticket.pending_publication().ok_or("policy observer lost")?;
            match observer.wait().await {
                PublicationState::Finished(Ok(PublicationOutcome::PolicyPage(committed))) => {
                    if let Some((bytes, sequence)) = known {
                        let mut e = BoundedEncoder::new(512)?;
                        committed.output.encode(&mut e)?;
                        assert_eq!(e.finish(), bytes);
                        assert_eq!(committed.receipt.commit_sequence, sequence);
                        assert_eq!(state(&f.handle).await?, recovery_before);
                    }
                    assert!(
                        matches!(committed.output, RefPolicyReply::Registered(progress) if progress.ready() && progress.total == 257)
                    );
                    assert!(observer.root_response(store).await.is_err());
                    let stage = settled(ticket, false).await?;
                    if loss == Loss::None {
                        assert!(matches!(stage, StagingState::Bound(_)));
                        complete(
                            f,
                            &prepared,
                            &pending,
                            Finalization {
                                coordinator: &p,
                                ticket,
                                previous: &registered,
                                ready: final_ready,
                            },
                            store,
                            (root, budget),
                            expected,
                        )
                        .await?;
                    } else {
                        assert!(pending.ready(&prepared).await.is_err() || loss == Loss::Close);
                        assert!(c.close_and_drain().await.is_empty());
                        assert!(p.close_and_drain().await.is_empty());
                        roots_unchanged(&before, &state(&f.handle).await?)?;
                    }
                }
                PublicationState::Finished(Ok(PublicationOutcome::RootPush(committed))) => {
                    assert!(known.is_none());
                    assert!(matches!(loss, Loss::Write | Loss::Epoch | Loss::Check));
                    assert!(
                        matches!(&committed.output, RootCompletionReply::Completed(value)
                        if value.completion.rejected && value.completion.publication.is_none())
                    );
                    assert!(matches!(
                        settled(ticket, false).await?,
                        StagingState::Published(Ok(_))
                    ));
                    assert_denied_page(f, &evidence, loss).await?;
                    let Resolution::Committed(original) =
                        f.client().resolve(&refusal_evidence).await?
                    else {
                        return Err("original frozen refusal receipt missing".into());
                    };
                    let mut encoded = BoundedEncoder::new(512)?;
                    committed.output.encode(&mut encoded)?;
                    assert_eq!(original.result(), encoded.finish());
                    assert_eq!(
                        original.commit_sequence(),
                        committed.receipt.commit_sequence
                    );
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
                    let rejected = crate::push::report::rejected_report(
                        &expected,
                        crate::push::report::REJECTED,
                    )?;
                    let mut actual = observer.root_response(store).await?;
                    let mut body = Vec::new();
                    while let Some(part) = actual.body.next().await? {
                        body.extend_from_slice(&part);
                    }
                    assert_eq!(actual.status, rejected.status);
                    assert_eq!(actual.headers, rejected.headers);
                    assert_eq!(body, rejected.body);
                    assert_eq!(body.windows(3).filter(|part| *part == b"ng ").count(), 257);
                    assert!(!body.windows(3).any(|part| part == b"ok "));
                    assert_eq!(f.counts_for(prepared.token()).await?, (0, 1));
                    roots_unchanged(&before, &state(&f.handle).await?)?;
                }
                PublicationState::Finished(Err(error)) => {
                    assert!(known.is_none());
                    assert!(loss == Loss::Expiry);
                    assert!(matches!(&*error,
                        PublicationError::RootPush(InvocationError::Rejected(value))
                        if value.output == RootCompletionReply::Denied(PreparationDenial::Expired)));
                    assert!(matches!(
                        settled(ticket, false).await?,
                        StagingState::Published(Err(_))
                    ));
                    assert_denied_page(f, &evidence, loss).await?;
                    assert!(observer.root_response(store).await.is_err());
                    assert_eq!(f.counts_for(prepared.token()).await?, (1, 1));
                    roots_unchanged(&before, &state(&f.handle).await?)?;
                }
                other => return Err(format!("unexpected policy result {other:?}").into()),
            }
            assert!(c.close_and_drain().await.is_empty());
            assert!(p.close_and_drain().await.is_empty());
            assert_eq!(p.reservations_for_test().await, (0, 0, 0));
            assert_eq!(c.stats().admitted, 0);
            return Ok(());
        }
        let observer = ticket.pending_publication().ok_or("page observer lost")?;
        let PublicationState::Finished(Ok(PublicationOutcome::PolicyPage(committed))) =
            observer.wait().await
        else {
            return Err("policy page not registered".into());
        };
        let RefPolicyReply::Registered(progress) = committed.output else {
            return Err("policy page denied".into());
        };
        assert!(progress.valid);
        assert_eq!(progress.total, 257);
        assert_eq!(progress.next, (offset + 128).min(257) as u64);
        assert!(matches!(
            settled(ticket, false).await?,
            StagingState::Bound(_)
        ));
        assert_eq!(p.reservations_for_test().await, (0, 0, 0));
        roots_unchanged(&before, &state(&f.handle).await?)?;
        assert!(matches!(
            f.client().resolve(&refusal_evidence).await?,
            Resolution::Absent
        ));
        head = Some(registered);
        offset = progress.next as usize;
    }
    complete(
        f,
        &prepared,
        &pending,
        Finalization {
            coordinator: &p,
            ticket,
            previous: head
                .as_ref()
                .ok_or("settled policy recovery head missing")?,
            ready: None,
        },
        store,
        (root, budget),
        expected,
    )
    .await?;
    assert!(c.close_and_drain().await.is_empty());
    assert!(p.close_and_drain().await.is_empty());
    Ok(())
}
async fn assert_denied_page(f: &Fixture, evidence: &PendingMutation, loss: Loss) -> Result {
    let reason = match loss {
        Loss::Write => PreparationDenial::Unauthorized,
        Loss::Epoch | Loss::Check => PreparationDenial::Conflict,
        Loss::Expiry => PreparationDenial::Expired,
        _ => return Err("unexpected negative page authority".into()),
    };
    let Resolution::Committed(original) = f.client().resolve(evidence).await? else {
        return Err("original registered negative page receipt missing".into());
    };
    assert!(matches!(
        original,
        cellule_runtime::cell::executor::StoredOutcome::Success { .. }
    ));
    let mut decoder = BoundedDecoder::new(original.result(), 512)?;
    assert_eq!(
        RefPolicyReply::decode(&mut decoder)?,
        RefPolicyReply::Denied(reason)
    );
    decoder.finish()?;
    Ok(())
}
struct Finalization<'a> {
    coordinator: &'a PublicationCoordinator,
    ticket: &'a StagingTicket,
    previous: &'a RegisteredRootRecovery,
    ready: Option<ReadyBoundRecovery>,
}
// The real final factory and current-authorized streaming reader qualify that
// a resumed page pipeline can publish exactly once, not merely register pages.
async fn complete(
    f: &Fixture,
    prepared: &Arc<PreparedCatalog>,
    pending: &RefPolicyPreparation,
    dispatch: Finalization<'_>,
    store: &canopy_object_storage::artifact::ArtifactStore,
    work: (&std::path::Path, cellule_ltx::DiskBudget),
    expected: crate::git_http::GitHttpResponse,
) -> Result {
    let Finalization {
        coordinator: p,
        ticket,
        previous,
        ready,
    } = dispatch;
    let (root, budget) = work;
    let ready = if let Some(ready) = ready {
        ready
    } else {
        let guard = pending.ready(prepared).await?;
        let ready =
            Box::pin(prepared.ready_root_push(identity()?, &guard, root, budget, limits(), None))
                .await?;
        let registered = ready
            .persist_recovery_after(store, identity()?, previous)
            .await?;
        ready.bind_recovery(registered, store)?
    };
    let observer = ticket.publish(p, ready)?;
    let PublicationState::Finished(Ok(PublicationOutcome::RootPush(committed))) =
        observer.wait().await
    else {
        return Err("resumed root push did not complete".into());
    };
    assert!(
        matches!(committed.output, RootCompletionReply::Completed(value) if !value.completion.rejected && value.completion.publication.is_some())
    );
    let mut actual = observer.root_response(store).await?;
    assert_eq!(actual.status, expected.status);
    assert_eq!(actual.headers, expected.headers);
    let mut body = Vec::new();
    while let Some(part) = actual.body.next().await? {
        body.extend_from_slice(&part);
    }
    assert_eq!(body, expected.body);
    // Initialization and the native attempt each retain an independent pin.
    assert_eq!(f.counts_for(prepared.token()).await?, (0, 1));
    f.handle
        .query(0, 1024, |db| {
            assert_eq!(
                db.query_row("SELECT count(*) FROM pushes", [], |r| r.get::<_, u64>(0))?,
                1
            );
            assert_eq!(
                db.query_row("SELECT generation FROM catalog_state", [], |r| r
                    .get::<_, u64>(0))?,
                2
            );
            Ok(Vec::new())
        })
        .await?;
    assert!(matches!(
        timeout(Duration::from_secs(10), ticket.wait_terminal()).await?,
        StagingState::Published(Ok(_))
    ));
    Ok(())
}
