use super::publishing::{edit, state};
use super::root_dispatch::Context;
use super::*;
use crate::packs::metadata::tests::limits;
use cellule_runtime::Resolution;
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
            if matches!(value, StagingState::Bound(_) | StagingState::Fenced(_))
                || uncertain && matches!(value, StagingState::Uncertain(_))
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
    let operation = prepared.token().operation;
    let p = PublicationCoordinator::new(f.target.clone(), PublicationLimits::default())?;
    let before = state(&f.handle).await?;
    let mut offset = 0;
    while offset < 257 {
        let last = offset == 256;
        let ready = pending.ready_page(&prepared, identity()?, offset).await?;
        let evidence = ready.evidence_for_test();
        // A page cannot enter through the final-completion API.
        let failure = ticket
            .publish(&p, ready)
            .err()
            .ok_or("policy page accepted as final")?;
        assert!(matches!(failure.reason, StagingError::Context));
        let ReadyPublication::PolicyPage(ready) = failure.ready else {
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
            assert_eq!(p.stats().await.command_bytes, 512 << 10);
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
            assert_eq!(p.stats().await.command_bytes, 512 << 10);
            let known = match f.client().resolve(&evidence).await? {
                Resolution::Absent => None,
                Resolution::Committed(value) => {
                    Some((value.result().to_vec(), value.commit_sequence()))
                }
                other => return Err(format!("unexpected policy resolution {other:?}").into()),
            };
            assert_eq!(known.is_some(), fault != 1);
            // Even complete historical SQL readiness cannot bypass the page's
            // lifecycle barrier while its original result remains uncertain.
            if known.is_some() {
                let guard = pending.ready(&prepared).await?;
                let final_ready = prepared
                    .ready_root_push(identity()?, &guard, root, budget.clone(), limits(), None)
                    .await?;
                let failure = ticket
                    .publish(&p, final_ready)
                    .err()
                    .ok_or("uncertain policy allowed final handoff")?;
                assert!(matches!(failure.reason, StagingError::Duplicate));
                drop(failure);
            }
            let weak_prepared = Arc::downgrade(&prepared);
            let weak_intent = Arc::downgrade(&pending);
            // Drop caller ownership; only the exact registered job retains them.
            drop(prepared);
            drop(pending);
            let prepared = weak_prepared.upgrade().ok_or("policy inputs lost")?;
            let pending = weak_intent.upgrade().ok_or("original policy intent lost")?;
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
                            (&p, ticket),
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
                PublicationState::Finished(Err(error)) => {
                    assert!(known.is_none());
                    let reason = match loss {
                        Loss::Write => PreparationDenial::Unauthorized,
                        Loss::Epoch | Loss::Check => PreparationDenial::Conflict,
                        Loss::Expiry => PreparationDenial::Expired,
                        _ => return Err("live absent page did not register".into()),
                    };
                    assert!(
                        matches!(&*error, PublicationError::PolicyPage(InvocationError::Rejected(value)) if value.output == RefPolicyReply::Denied(reason))
                    );
                    assert!(matches!(
                        settled(ticket, false).await?,
                        StagingState::Fenced(_)
                    ));
                    assert_eq!(state(&f.handle).await?, recovery_before);
                    assert!(c.close_and_drain().await.is_empty());
                    assert!(p.close_and_drain().await.is_empty());
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
        offset = progress.next as usize;
    }
    complete(
        f,
        &prepared,
        &pending,
        (&p, ticket),
        store,
        (root, budget),
        expected,
    )
    .await?;
    assert!(c.close_and_drain().await.is_empty());
    assert!(p.close_and_drain().await.is_empty());
    Ok(())
}
// The real final factory and current-authorized streaming reader qualify that
// a resumed page pipeline can publish exactly once, not merely register pages.
async fn complete(
    f: &Fixture,
    prepared: &Arc<PreparedCatalog>,
    pending: &RefPolicyPreparation,
    dispatch: (&PublicationCoordinator, &StagingTicket),
    store: &canopy_object_storage::artifact::ArtifactStore,
    work: (&std::path::Path, cellule_ltx::DiskBudget),
    expected: crate::git_http::GitHttpResponse,
) -> Result {
    let (p, ticket) = dispatch;
    let (root, budget) = work;
    let guard = pending.ready(prepared).await?;
    let ready = prepared
        .ready_root_push(identity()?, &guard, root, budget, limits(), None)
        .await?;
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
    assert_eq!(f.counts().await?, (0, 2));
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
