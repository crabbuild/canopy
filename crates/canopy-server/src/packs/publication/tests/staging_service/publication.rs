use super::super::coordinator::{finished, refused, request};
use super::super::publishing::edit;
use super::*;

async fn ready(session: &Arc<PreparationSession>) -> Result<ReadyCatalogPush> {
    Ok(session
        .ready_outcome(identity()?, request(refused()))
        .await?)
}

#[tokio::test]
async fn bound_final_waits_for_exact_renewal_and_adopted_checkpoint_recovery_before_dispatch()
-> Result {
    use canopy_object_storage::artifact::ArtifactStore;
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        for fault in [1, 2, 3] {
            for renewal in [false, true] {
                let f = Fixture::new(format).await?;
                let (source, first) = super::super::inputs::active(&f, [235; 16]).await?;
                let store = Arc::new(ArtifactStore::new(Arc::new(InMemory::new()), f.repository));
                let prior = super::super::inputs::seal(&f, &first, store.clone(), 3).await?;
                first
                    .register_inputs(prior.clone(), identity()?)
                    .map_err(|(e, _)| e)?
                    .wait()
                    .await
                    .map_err(|e| e.to_string())?;
                first.seal()?;
                let StagingState::Bound(original) = terminal(&first).await? else {
                    return Err("source binding lost".into());
                };
                assert!(source.close_and_drain().await.is_empty());
                let c = StagingCoordinator::new(
                    f.target.clone(),
                    StagingLimits::default(),
                    f.authority(),
                )?;
                let p = PublicationCoordinator::new(
                    f.target.clone(),
                    PublicationLimits::default(),
                    f.publication_budget.clone(),
                )?;
                let ticket = super::bound::claim(&f, &c, original.lease.token, identity()?).await?;
                assert!(matches!(terminal(&ticket).await?, StagingState::Bound(_)));
                let session = ticket.bound_session()?;
                let adopted = session.adopt_native_inputs(store, &prior).await?;
                assert_eq!(adopted.root()?, prior.root()?);
                let input = ready(&session).await?;
                c.fault_for_test(fault);
                if renewal {
                    ticket.renew_for_test();
                }
                let checkpoint = ticket
                    .register_inputs(adopted, identity()?)
                    .map_err(|(e, _)| e)?;
                let observer = ticket.publish(&p, input)?;
                let StagingState::Uncertain(error) = wait_for(&ticket, |s| {
                    matches!(
                        s,
                        StagingState::Uncertain(_)
                            | StagingState::Fenced(_)
                            | StagingState::Published(_)
                    )
                })
                .await?
                else {
                    return Err("pre-publication command uncertainty lost".into());
                };
                if renewal {
                    assert!(matches!(error.as_ref(), StagingError::BoundRenew(_)));
                } else {
                    assert!(matches!(error.as_ref(), StagingError::Checkpoint(_)));
                }
                assert!(matches!(observer.state(), PublicationState::Held));
                assert_eq!(
                    c.stats().command_bytes,
                    super::super::super::custody::RESERVATION + 4096
                );
                assert_eq!(p.stats().await.held, 1);
                assert_eq!(c.close_and_drain().await.len(), 1);
                assert_eq!(p.close_and_drain().await.len(), 1);
                c.recover(&ticket)?;
                let registration = timeout(Duration::from_secs(10), checkpoint.wait())
                    .await?
                    .map_err(|e| e.to_string())?;
                let completed = finished(timeout(Duration::from_secs(10), observer.wait()).await?)?;
                assert!(completed.receipt.commit_sequence > registration.commit_sequence);
                if renewal {
                    assert!(
                        registration.commit_sequence
                            > ticket
                                .bound_renewal()
                                .ok_or("original renewal outcome lost")?
                                .receipt
                                .commit_sequence
                    );
                }
                assert!(matches!(
                    terminal(&ticket).await?,
                    StagingState::Published(Ok(_))
                ));
                assert_eq!(observer.response().await?, refused());
                assert_eq!(p.reservations_for_test().await, (0, 0, 0));
                assert!(c.close_and_drain().await.is_empty());
                assert!(p.close_and_drain().await.is_empty());
                f.runtime.shutdown().await?;
            }
        }
    }
    Ok(())
}
async fn wait_for(
    ticket: &StagingTicket,
    predicate: impl Fn(&StagingState) -> bool,
) -> Result<StagingState> {
    timeout(Duration::from_secs(10), async {
        loop {
            let state = ticket.state();
            if predicate(&state) {
                return state;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .map_err(Into::into)
}

#[tokio::test]
async fn bound_final_publication_drains_retained_work_and_due_renewal_through_closed_admission()
-> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let f = Fixture::new(format).await?;
        let c = StagingCoordinator::new(f.target.clone(), StagingLimits::default(), f.authority())?;
        let p = PublicationCoordinator::new(
            f.target.clone(),
            PublicationLimits::default(),
            f.publication_budget.clone(),
        )?;
        let ticket = super::bound::bind(&f, &c, [231; 16], "owner").await?;
        let session = ticket.bound_session()?;
        let input = ready(&session).await?;
        let (release, blocked) = oneshot::channel();
        let (entered, running) = oneshot::channel();
        let work = ticket.spawn_bound(move |_, _context| async move {
            let _ = entered.send(());
            blocked.await.map_err(|_| StagingError::Worker)?;
            Ok(42u64)
        })?;
        let work_id = work.id();
        timeout(Duration::from_secs(10), running).await??;
        ticket.renew_for_test();
        let publication = ticket.publish(&p, input)?;
        drop(publication);
        drop(work);
        assert!(matches!(ticket.state(), StagingState::Finishing));
        assert!(ticket.spawn_bound(|_, _context| async { Ok(()) }).is_err());
        assert!(ticket.bound_session().is_err());
        assert_eq!(p.stats().await.held, 1);
        assert_eq!(c.stats().workers, 1);
        timeout(Duration::from_secs(10), async {
            while ticket.bound_renewal().is_none() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await?;
        let renewed = ticket.bound_renewal().ok_or("serialized renewal")?;
        assert!(matches!(ticket.state(), StagingState::Finishing));
        assert_eq!(p.close_and_drain().await.len(), 1);
        let closing = c.clone();
        let closed = tokio::spawn(async move { closing.close_and_drain().await });
        timeout(Duration::from_secs(10), async {
            while !c.stats().closed {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        assert!(!closed.is_finished());
        release.send(()).map_err(|_| "producer disappeared")?;
        let retained = ticket
            .pending_task::<u64>(work_id)
            .ok_or("retained result lost")?;
        assert_eq!(retained.wait().await.map_err(|e| e.to_string())?, 42);
        let publication = ticket.pending_publication().ok_or("final observer lost")?;
        let value = finished(timeout(Duration::from_secs(10), publication.wait()).await?)?;
        assert!(value.receipt.commit_sequence > renewed.receipt.commit_sequence);
        assert!(matches!(
            terminal(&ticket).await?,
            StagingState::Published(Ok(PublicationOutcome::Push(_)))
        ));
        assert_eq!(publication.response().await?, refused());
        assert!(timeout(Duration::from_secs(10), closed).await??.is_empty());
        assert!(session.live_lease().is_err());
        assert!(
            f.client()
                .query::<CheckPreparation>(
                    &f.target,
                    Some(value.receipt),
                    check(session.lease.token)
                )
                .await?
                .output
                .is_none()
        );
        assert_eq!(c.stats().admitted, 0);
        assert_eq!(c.stats().workers, 0);
        assert_eq!(p.reservations_for_test().await, (0, 0, 0));
        assert!(p.close_and_drain().await.is_empty());
        f.runtime.shutdown().await?;
    }
    Ok(())
}

#[test]
fn bound_final_exact_recovery_preserves_commits_and_refuses_absence_after_custody_loss() -> Result {
    // Virtual time belongs to one scenario: offsets otherwise accumulate and
    // cross unrelated SDK deadlines expressed using the real monotonic clock.
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        for fault in [1, 2, 3] {
            for expired in [false, true] {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()?
                    .block_on(exact_case(format, fault, expired))?;
            }
        }
    }
    Ok(())
}
async fn exact_case(format: ObjectFormat, fault: u8, expired: bool) -> Result {
    let f = Fixture::new(format).await?;
    let c = StagingCoordinator::new(f.target.clone(), StagingLimits::default(), f.authority())?;
    let p = PublicationCoordinator::new(
        f.target.clone(),
        PublicationLimits::default(),
        f.publication_budget.clone(),
    )?;
    let ticket = super::bound::bind(&f, &c, [232; 16], "owner").await?;
    // Start the short ceiling in the publication phase, after Bind setup.
    ticket.limit_bound_ceiling_for_test(Duration::from_millis(1000))?;
    let session = ticket.bound_session()?;
    p.fault_for_test(fault);
    let observer = ticket.publish(&p, ready(&session).await?)?;
    drop(observer);
    let StagingState::Uncertain(error) = wait_for(&ticket, |s| {
        matches!(
            s,
            StagingState::Uncertain(_) | StagingState::Published(_) | StagingState::Fenced(_)
        )
    })
    .await?
    else {
        return Err("final uncertainty lost".into());
    };
    let StagingError::Publication(error) = error.as_ref() else {
        return Err("wrong lifecycle uncertainty".into());
    };
    let PublicationError::Push(InvocationError::Pending(evidence)) = error.as_ref() else {
        return Err("wrong final evidence".into());
    };
    let original = match f.client().resolve(evidence).await? {
        cellule_runtime::Resolution::Committed(value) => Some(value.commit_sequence()),
        cellule_runtime::Resolution::Absent => None,
        other => return Err(format!("unexpected resolution {other:?}").into()),
    };
    assert_eq!(original.is_some(), fault != 1);
    assert_eq!(
        c.stats().command_bytes,
        super::super::super::custody::RESERVATION
    );
    assert_eq!(p.stats().await.command_bytes, 8 << 20);
    if expired {
        tokio::time::pause();
        tokio::time::advance(Duration::from_millis(1001)).await;
        tokio::time::resume();
        assert!(session.live_lease().is_err());
    } else {
        edit(&f, "UPDATE repository_identity SET owner='replacement'; UPDATE ref_generation SET visibility='private'").await?;
    }
    assert_eq!(c.close_and_drain().await.len(), 1);
    assert_eq!(p.close_and_drain().await.len(), 1);
    let retained = c.pending([232; 16]).ok_or("lifecycle command lost")?;
    c.recover(&retained)?;
    let state = terminal(&retained).await?;
    let StagingState::Published(outcome) = state else {
        return Err(format!(
            "final outcome {state:?}; format {format:?}, fault {fault}, expired {expired}"
        )
        .into());
    };
    let observer = retained
        .pending_publication()
        .ok_or("final command observer lost")?;
    match outcome {
        Ok(PublicationOutcome::Push(value)) if fault != 1 => {
            assert_eq!(Some(value.receipt.commit_sequence), original);
            if !expired {
                assert_eq!(observer.response().await?, refused());
                assert!(matches!(
                    replay_push_response(&f.client(), &f.target, f.begin([232; 16]), None).await,
                    Err(CatalogPushResponseError::Denied(
                        PreparationDenial::Unauthorized
                    ))
                ));
                edit(&f, "UPDATE repository_identity SET owner='owner'").await?;
            }
            assert_eq!(observer.response().await?, refused());
        }
        Err(error) if fault == 1 && expired => assert!(matches!(
            error.as_ref(),
            PublicationError::Push(InvocationError::NotStarted(_))
        )),
        Err(error) if fault == 1 => assert!(
            matches!(error.as_ref(), PublicationError::Push(InvocationError::Rejected(value))
        if value.output == CatalogCompletionReply::Denied(PreparationDenial::Unauthorized))
        ),
        other => return Err(format!("unexpected exact result {other:?}").into()),
    }
    assert!(session.live_lease().is_err());
    assert_eq!(c.stats().admitted, 0);
    assert_eq!(p.reservations_for_test().await, (0, 0, 0));
    assert!(c.close_and_drain().await.is_empty());
    assert!(p.close_and_drain().await.is_empty());
    f.runtime.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn bound_final_ceiling_discards_held_proof_and_drops_result_before_worker_credit() -> Result {
    use std::sync::atomic::{AtomicBool, Ordering};
    struct Owned {
        c: StagingCoordinator,
        dropped: Arc<AtomicBool>,
        wrong: Arc<AtomicBool>,
    }
    impl Drop for Owned {
        fn drop(&mut self) {
            if self.c.stats().workers != 1 {
                self.wrong.store(true, Ordering::Release);
            }
            self.dropped.store(true, Ordering::Release);
        }
    }
    let f = Fixture::new(ObjectFormat::Sha256).await?;
    let c = StagingCoordinator::new(f.target.clone(), StagingLimits::default(), f.authority())?;
    let p = PublicationCoordinator::new(
        f.target.clone(),
        PublicationLimits::default(),
        f.publication_budget.clone(),
    )?;
    let ticket = super::bound::bind(&f, &c, [233; 16], "owner").await?;
    // Start the short ceiling in the publication phase, after Bind setup.
    ticket.limit_bound_ceiling_for_test(Duration::from_millis(1000))?;
    let session = ticket.bound_session()?;
    let input = ready(&session).await?;
    let dropped = Arc::new(AtomicBool::new(false));
    let wrong = Arc::new(AtomicBool::new(false));
    let owned = Owned {
        c: c.clone(),
        dropped: dropped.clone(),
        wrong: wrong.clone(),
    };
    let (done, completed) = oneshot::channel();
    let work = ticket.spawn_bound(move |_, _context| async move {
        let _ = done.send(());
        Ok(owned)
    })?;
    timeout(Duration::from_secs(10), completed).await??;
    let observer = ticket.publish(&p, input)?;
    assert!(matches!(observer.state(), PublicationState::Held));
    tokio::time::pause();
    tokio::time::advance(Duration::from_millis(1001)).await;
    tokio::time::resume();
    assert!(matches!(terminal(&ticket).await?, StagingState::Fenced(_)));
    assert!(matches!(observer.wait().await, PublicationState::Discarded));
    assert!(work.wait().await.is_err());
    assert!(dropped.load(Ordering::Acquire));
    assert!(!wrong.load(Ordering::Acquire));
    assert!(session.live_lease().is_err());
    assert_eq!(
        super::super::completion::completed_pushes(&f.handle).await?,
        0
    );
    assert_eq!(p.reservations_for_test().await, (0, 0, 0));
    assert!(c.close_and_drain().await.is_empty());
    assert!(p.close_and_drain().await.is_empty());
    f.runtime.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn bound_final_refusals_keep_exact_ready_and_require_shared_session_and_final_kind() -> Result
{
    let f = Fixture::new(ObjectFormat::Sha256).await?;
    let c = StagingCoordinator::new(f.target.clone(), StagingLimits::default(), f.authority())?;
    let p = PublicationCoordinator::new(
        f.target.clone(),
        PublicationLimits::default(),
        f.publication_budget.clone(),
    )?;
    let ticket = super::bound::bind(&f, &c, [234; 16], "owner").await?;
    let session = ticket.bound_session()?;
    let unrelated = Arc::new(
        PreparationSession::open(
            f.client(),
            f.target.clone(),
            check(session.lease.token),
            None,
            f.authority(),
        )
        .await?,
    );
    let failure = ticket
        .publish(&p, ready(&unrelated).await?)
        .err()
        .ok_or("independent clock accepted")?;
    assert!(matches!(failure.reason, StagingError::Context));
    drop(failure);
    let renewal = session.ready_renew(identity()?, DEFAULT_LEASE_MS).await?;
    let failure = ticket
        .publish(&p, renewal)
        .err()
        .ok_or("nonfinal renewal accepted")?;
    assert!(matches!(failure.reason, StagingError::Context));
    drop(failure);
    assert_eq!(p.stats().await.admitted, 0);
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
        .publish(&foreign, ready(&session).await?)
        .err()
        .ok_or("foreign final coordinator accepted")?;
    assert!(matches!(
        failure.reason,
        StagingError::PublicationAdmission(PublicationScheduleError::Foreign)
    ));
    assert!(matches!(ticket.state(), StagingState::Bound(_)));
    let second = ready(&session).await?;
    let (release, entered) = p.pause_for_test().await;
    let observer = ticket.publish(&p, failure.ready)?;
    let failure = ticket
        .publish(&p, second)
        .err()
        .ok_or("duplicate final accepted")?;
    assert!(matches!(failure.reason, StagingError::Duplicate));
    drop(failure);
    timeout(Duration::from_secs(10), entered).await??;
    release.send(()).map_err(|_| "final dispatch disappeared")?;
    finished(timeout(Duration::from_secs(10), observer.wait()).await?)?;
    assert!(matches!(
        terminal(&ticket).await?,
        StagingState::Published(Ok(_))
    ));
    assert_eq!(observer.response().await?, refused());
    assert!(c.close_and_drain().await.is_empty());
    assert!(p.close_and_drain().await.is_empty());
    f.runtime.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn bound_final_queued_transport_rechecks_ceiling_before_initial_execution() -> Result {
    let f = Fixture::new(ObjectFormat::Sha256).await?;
    let c = StagingCoordinator::new(f.target.clone(), StagingLimits::default(), f.authority())?;
    let p = PublicationCoordinator::new(
        f.target.clone(),
        PublicationLimits::default(),
        f.publication_budget.clone(),
    )?;
    let ticket = super::bound::bind(&f, &c, [236; 16], "owner").await?;
    // Start the short ceiling in the publication phase, after Bind setup.
    ticket.limit_bound_ceiling_for_test(Duration::from_millis(1000))?;
    let session = ticket.bound_session()?;
    let (release, entered) = p.pause_for_test().await;
    let observer = ticket.publish(&p, ready(&session).await?)?;
    timeout(Duration::from_secs(10), entered).await??;
    assert!(matches!(observer.state(), PublicationState::Running));
    tokio::time::pause();
    tokio::time::advance(Duration::from_millis(1001)).await;
    tokio::time::resume();
    release.send(()).map_err(|_| "transport disappeared")?;
    assert!(
        matches!(terminal(&ticket).await?, StagingState::Published(Err(error))
        if matches!(error.as_ref(), PublicationError::Push(InvocationError::NotStarted(_))))
    );
    assert!(observer.response().await.is_err());
    assert_eq!(
        super::super::completion::completed_pushes(&f.handle).await?,
        0
    );
    assert_eq!(p.reservations_for_test().await, (0, 0, 0));
    assert!(session.live_lease().is_err());
    assert!(c.close_and_drain().await.is_empty());
    assert!(p.close_and_drain().await.is_empty());
    f.runtime.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn bound_final_observes_shared_coordinator_recovery_without_losing_lifecycle_admission()
-> Result {
    let f = Fixture::new(ObjectFormat::Sha256).await?;
    let c = StagingCoordinator::new(f.target.clone(), StagingLimits::default(), f.authority())?;
    let p = PublicationCoordinator::new(
        f.target.clone(),
        PublicationLimits::default(),
        f.publication_budget.clone(),
    )?;
    let ticket = super::bound::bind(&f, &c, [237; 16], "owner").await?;
    let session = ticket.bound_session()?;
    p.fault_for_test(2);
    let observer = ticket.publish(&p, ready(&session).await?)?;
    assert!(matches!(
        terminal(&ticket).await?,
        StagingState::Uncertain(_)
    ));
    assert_eq!(c.stats().admitted, 1);
    assert_eq!(c.close_and_drain().await.len(), 1);
    assert_eq!(p.close_and_drain().await.len(), 1);
    let exact = p.pending([237; 16]).await.ok_or("shared command lost")?;
    exact.recover().await?;
    // The lifecycle observes the shared ticket transition itself. No second
    // recovery call, replacement command or caller-owned result is necessary.
    assert!(matches!(
        timeout(
            Duration::from_secs(10),
            wait_for(&ticket, |s| matches!(s, StagingState::Published(_)))
        )
        .await??,
        StagingState::Published(Ok(_))
    ));
    assert_eq!(observer.response().await?, refused());
    assert_eq!(c.stats().admitted, 0);
    assert_eq!(p.reservations_for_test().await, (0, 0, 0));
    assert!(c.close_and_drain().await.is_empty());
    assert!(p.close_and_drain().await.is_empty());
    f.runtime.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn bound_publication_waits_for_contended_admission_without_losing_original_command() -> Result
{
    use std::{
        future::Future,
        task::{Context, Poll, Waker},
    };
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let f = Fixture::new(format).await?;
        let c = StagingCoordinator::new(f.target.clone(), StagingLimits::default(), f.authority())?;
        let p = PublicationCoordinator::new(
            f.target.clone(),
            PublicationLimits::default(),
            f.publication_budget.clone(),
        )?;
        let ticket = super::bound::bind(&f, &c, [236; 16], "owner").await?;
        let session = ticket.bound_session()?;
        let input = ready(&session).await?;
        let future = ticket.publish_wait(&p, input);
        tokio::pin!(future);
        p.with_admission_for_test(|| {
            let mut context = Context::from_waker(Waker::noop());
            assert!(
                matches!(future.as_mut().poll(&mut context), Poll::Pending),
                "mutex contention must wait without consuming the prepared command"
            );
            assert!(ticket.pending_publication().is_none());
            assert!(matches!(ticket.state(), StagingState::Bound(_)));
        })
        .await;
        let observer = timeout(Duration::from_secs(10), future).await??;
        finished(timeout(Duration::from_secs(10), observer.wait()).await?)?;
        assert_eq!(observer.response().await?, refused());
        assert!(matches!(
            terminal(&ticket).await?,
            StagingState::Published(Ok(_))
        ));
        assert!(c.close_and_drain().await.is_empty());
        assert!(p.close_and_drain().await.is_empty());
    }
    Ok(())
}

#[tokio::test]
async fn bound_publication_wait_ceiling_never_admits_or_executes_the_retained_command() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let f = Fixture::new(format).await?;
        let c = StagingCoordinator::new(f.target.clone(), StagingLimits::default(), f.authority())?;
        let p = PublicationCoordinator::new(
            f.target.clone(),
            PublicationLimits::default(),
            f.publication_budget.clone(),
        )?;
        let ticket = super::bound::bind(&f, &c, [237; 16], "owner").await?;
        let session = ticket.bound_session()?;
        let input = ready(&session).await?;
        ticket.expire_bound_for_test()?;
        let failure = timeout(
            Duration::from_secs(10),
            p.with_admission_async_for_test(ticket.publish_wait(&p, input)),
        )
        .await?
        .err()
        .ok_or("publication admitted under a locked mutex")?;
        assert!(matches!(failure.reason, StagingError::Inactive));
        assert!(ticket.pending_publication().is_none());
        assert_eq!(p.stats().await.admitted, 0);
        assert_eq!(
            super::super::completion::completed_pushes(&f.handle).await?,
            0
        );
        drop(failure);
        assert!(matches!(terminal(&ticket).await?, StagingState::Fenced(_)));
        assert!(c.close_and_drain().await.is_empty());
        assert!(p.close_and_drain().await.is_empty());
        f.runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn bound_publication_wait_keeps_real_quota_refusal_immediate_and_unexecuted() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let f = Fixture::new(format).await?;
        let c = StagingCoordinator::new(f.target.clone(), StagingLimits::default(), f.authority())?;
        let p = PublicationCoordinator::new(
            f.target.clone(),
            PublicationLimits {
                per_actor: 1,
                ..PublicationLimits::default()
            },
            f.publication_budget.clone(),
        )?;
        let first = super::bound::bind(&f, &c, [238; 16], "owner").await?;
        let second = super::bound::bind(&f, &c, [239; 16], "owner").await?;
        let first_input = ready(&first.bound_session()?).await?;
        let second_input = ready(&second.bound_session()?).await?;
        let (release, entered) = p.pause_for_test().await;
        let observer = first.publish_wait(&p, first_input).await?;
        timeout(Duration::from_secs(10), entered).await??;
        let failure = timeout(
            Duration::from_secs(1),
            second.publish_wait(&p, second_input),
        )
        .await?
        .err()
        .ok_or("actor quota exceeded")?;
        assert!(matches!(
            failure.reason,
            StagingError::PublicationAdmission(PublicationScheduleError::Capacity)
        ));
        assert!(second.pending_publication().is_none());
        assert!(matches!(second.state(), StagingState::Bound(_)));
        assert_eq!(p.stats().await.admitted, 1);
        drop(failure);
        release
            .send(())
            .map_err(|_| "held publication disappeared")?;
        finished(timeout(Duration::from_secs(10), observer.wait()).await?)?;
        assert_eq!(
            super::super::completion::completed_pushes(&f.handle).await?,
            1
        );
        assert!(c.close_and_drain().await.is_empty());
        assert!(p.close_and_drain().await.is_empty());
        f.runtime.shutdown().await?;
    }
    Ok(())
}
