use super::*;

async fn outcomes(handle: &CellHandle) -> Result<u64> {
    Ok(super::super::completion::counts(handle).await?[0])
}

#[tokio::test]
async fn held_native_proof_survives_canceled_observation_and_closed_activation() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let fixture = Fixture::new(format).await?;
        let graph = assembled(&fixture, [60; 16], 0).await?;
        let prepared = Arc::new(graph.prepared);
        let weak = Arc::downgrade(&prepared);
        let response = accepted(graph.initial, "refs/heads/held");
        let ready = Box::pin(prepared.ready_push(
            identity()?,
            accepted(graph.initial, "refs/heads/held"),
            graph.root.path(),
            graph.budget.clone(),
            limits(),
        ))
        .await?;
        let coordinator = PublicationCoordinator::new(
            fixture.target.clone(),
            PublicationLimits::default(),
            fixture.publication_budget.clone(),
        )?;
        let ticket = coordinator.try_reserve(ready)?;
        drop(prepared);
        let observer = ticket.clone();
        let waiter = tokio::spawn(async move { observer.wait().await });
        tokio::task::yield_now().await;
        waiter.abort();
        assert!(waiter.await.unwrap_err().is_cancelled());
        drop(ticket);
        let ticket = coordinator
            .pending([60; 16])
            .await
            .ok_or("held admission lost")?;
        assert!(matches!(ticket.state(), PublicationState::Held));
        let stats = coordinator.stats().await;
        assert_eq!(
            (
                stats.held,
                stats.queued,
                stats.in_flight,
                stats.command_bytes
            ),
            (1, 0, 0, 8 << 20)
        );
        assert!(weak.upgrade().is_some());
        assert_eq!(outcomes(&fixture.handle).await?, 0);
        assert_eq!(
            graph.budget.used(),
            crate::packs::metadata::growth::INITIAL_BYTES * 3
        );
        assert!(ticket.response().await.is_err());
        assert_eq!(
            ticket.recover().await,
            Err(PublicationScheduleError::NotUncertain)
        );
        let closed = coordinator.close_and_drain().await;
        assert_eq!(closed.len(), 1);
        assert!(matches!(closed[0].state(), PublicationState::Held));
        assert_eq!(coordinator.reservations_for_test().await, (1, 8 << 20, 1));
        let (release, entered) = coordinator.pause_for_test().await;
        let (a, b) = tokio::join!(ticket.activate(), ticket.activate());
        a?;
        b?;
        timeout(Duration::from_secs(5), entered).await??;
        assert_eq!(
            ticket.discard_held().await,
            Err(PublicationScheduleError::NotHeld)
        );
        release.send(()).map_err(|_| "dispatch disappeared")?;
        let committed = finished(timeout(Duration::from_secs(10), ticket.wait()).await?)?;
        assert!(matches!(
            committed.output,
            CatalogCompletionReply::Completed(_)
        ));
        ticket.activate().await?;
        assert_eq!(outcomes(&fixture.handle).await?, 1);
        assert_eq!(ticket.response().await?, response.response);
        assert!(weak.upgrade().is_none());
        assert_eq!(coordinator.reservations_for_test().await, (0, 0, 0));
        cleaned(graph.root.path(), &graph.budget).await?;
        assert!(coordinator.close_and_drain().await.is_empty());
        fixture.runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn held_discard_drops_native_proof_before_credit_and_never_executes() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let fixture = Fixture::new(format).await?;
        let graph = assembled(&fixture, [60; 16], 0).await?;
        let prepared = Arc::new(graph.prepared);
        let weak = Arc::downgrade(&prepared);
        let ready = Box::pin(prepared.ready_push(
            identity()?,
            accepted(graph.initial, "refs/heads/discarded"),
            graph.root.path(),
            graph.budget.clone(),
            limits(),
        ))
        .await?;
        let coordinator = PublicationCoordinator::new(
            fixture.target.clone(),
            PublicationLimits::default(),
            fixture.publication_budget.clone(),
        )?;
        let ticket = coordinator.try_reserve(ready)?;
        drop(prepared);
        assert_eq!(coordinator.close_and_drain().await.len(), 1);
        ticket.discard_held().await?;
        assert!(matches!(ticket.wait().await, PublicationState::Discarded));
        assert!(weak.upgrade().is_none());
        let node = fixture.publication_budget.stats();
        assert_eq!(
            (node.foreground, node.command_bytes, node.accounts),
            (0, 0, 0)
        );
        cleaned(graph.root.path(), &graph.budget).await?;
        assert_eq!(coordinator.reservations_for_test().await, (0, 0, 0));
        assert!(coordinator.pending([60; 16]).await.is_none());
        assert_eq!(
            ticket.activate().await,
            Err(PublicationScheduleError::NotHeld)
        );
        assert_eq!(
            ticket.discard_held().await,
            Err(PublicationScheduleError::NotHeld)
        );
        assert_eq!(
            ticket.recover().await,
            Err(PublicationScheduleError::NotUncertain)
        );
        assert!(ticket.response().await.is_err());
        assert_eq!(outcomes(&fixture.handle).await?, 0);
        assert!(coordinator.close_and_drain().await.is_empty());
        fixture.runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn held_admission_uses_existing_account_bytes_and_returns_refused_ready() -> Result {
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
        fixture.publication_budget.clone(),
    )?;
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
    let foreign = PublicationCoordinator::new(
        crate::repository_target(
            TenantId::from_bytes([99; 16]),
            fixture.target.application(),
            fixture.repository,
        )?,
        PublicationLimits::default(),
        fixture.publication_budget.clone(),
    )?;
    let failure = foreign
        .try_reserve(attempts[0].3.take().unwrap())
        .err()
        .ok_or("refused admission unexpectedly accepted")?;
    assert_eq!(failure.reason, PublicationScheduleError::Foreign);
    let first = coordinator.try_reserve(failure.ready)?;
    let failure = coordinator
        .with_admission_for_test(|| coordinator.try_reserve(attempts[1].3.take().unwrap()))
        .await
        .err()
        .ok_or("contended synchronous admission unexpectedly accepted")?;
    assert_eq!(failure.reason, PublicationScheduleError::Busy);
    attempts[1].3 = Some(failure.ready);
    // The same logical operation cannot have a second held/executing slot.
    let duplicate = Box::pin(attempts[0].0.ready_push(
        identity()?,
        request(refused()),
        attempts[0].1.path(),
        attempts[0].2.clone(),
        limits(),
    ))
    .await?;
    let failure = coordinator
        .try_reserve(duplicate)
        .err()
        .ok_or("refused admission unexpectedly accepted")?;
    assert_eq!(failure.reason, PublicationScheduleError::Duplicate);
    drop(failure);
    let second = coordinator.try_reserve(attempts[1].3.take().unwrap())?;
    let failure = coordinator
        .try_reserve(attempts[2].3.take().unwrap())
        .err()
        .ok_or("refused admission unexpectedly accepted")?;
    assert_eq!(failure.reason, PublicationScheduleError::Capacity);
    attempts[2].3 = Some(failure.ready);
    let third = coordinator.try_reserve(attempts[3].3.take().unwrap())?;
    let failure = coordinator
        .try_reserve(attempts[4].3.take().unwrap())
        .err()
        .ok_or("refused admission unexpectedly accepted")?;
    assert_eq!(failure.reason, PublicationScheduleError::Capacity);
    attempts[4].3 = Some(failure.ready);
    assert_eq!(coordinator.reservations_for_test().await, (3, 24 << 20, 2));
    assert_eq!(coordinator.stats().await.held, 3);
    first.discard_held().await?;
    let retry = coordinator.try_reserve(attempts[2].3.take().unwrap())?;
    assert_eq!(coordinator.reservations_for_test().await, (3, 24 << 20, 2));
    let (release, entered) = coordinator.pause_for_test().await;
    second.activate().await?;
    timeout(Duration::from_secs(5), entered).await??;
    third.activate().await?;
    retry.activate().await?;
    release.send(()).map_err(|_| "dispatch disappeared")?;
    for ticket in [second, third, retry] {
        finished(timeout(Duration::from_secs(10), ticket.wait()).await?)?;
    }
    assert_eq!(outcomes(&fixture.handle).await?, 3);
    assert_eq!(coordinator.reservations_for_test().await, (0, 0, 0));
    assert!(coordinator.close_and_drain().await.is_empty());
    let failure = coordinator
        .try_reserve(attempts[4].3.take().unwrap())
        .err()
        .ok_or("refused admission unexpectedly accepted")?;
    assert_eq!(failure.reason, PublicationScheduleError::Closed);
    let successor = PublicationCoordinator::new(
        fixture.target.clone(),
        PublicationLimits::default(),
        fixture.publication_budget.clone(),
    )?;
    let ticket = successor.try_reserve(failure.ready)?;
    ticket.activate().await?;
    finished(timeout(Duration::from_secs(10), ticket.wait()).await?)?;
    assert_eq!(outcomes(&fixture.handle).await?, 4);
    assert!(successor.close_and_drain().await.is_empty());
    drop(attempts);
    fixture.runtime.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn activated_held_command_recovers_exact_receipt_or_checks_absent_authority_after_close()
-> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        for fault in [1, 2, 3] {
            let fixture = Fixture::new(format).await?;
            let (prepared, root, budget) = empty(&fixture, [66; 16], "owner").await?;
            let ready = Box::pin(prepared.ready_push(
                identity()?,
                request(refused()),
                root.path(),
                budget.clone(),
                limits(),
            ))
            .await?;
            let coordinator = PublicationCoordinator::new(
                fixture.target.clone(),
                PublicationLimits::default(),
                fixture.publication_budget.clone(),
            )?;
            let ticket = coordinator.try_reserve(ready)?;
            assert!(matches!(ticket.state(), PublicationState::Held));
            coordinator.fault_for_test(fault);
            ticket.activate().await?;
            let PublicationState::Uncertain(error) =
                timeout(Duration::from_secs(10), ticket.wait()).await?
            else {
                return Err("exact uncertainty lost".into());
            };
            let PublicationError::Push(InvocationError::Pending(evidence)) = error.as_ref() else {
                return Err("wrong exact evidence".into());
            };
            let original = match fixture.client().resolve(evidence).await? {
                cellule_runtime::Resolution::Committed(value) => Some(value.commit_sequence()),
                cellule_runtime::Resolution::Absent => None,
                other => return Err(format!("unexpected resolution {other:?}").into()),
            };
            assert_eq!(original.is_some(), fault != 1);
            let stats = coordinator.stats().await;
            assert_eq!(
                (stats.held, stats.uncertain, stats.command_bytes),
                (0, 1, 8 << 20)
            );
            // Activation never retries an ambiguous command, and discard cannot
            // erase it. Only explicit recovery joins the exact fair queue.
            ticket.activate().await?;
            assert!(matches!(ticket.state(), PublicationState::Uncertain(_)));
            assert_eq!(
                ticket.discard_held().await,
                Err(PublicationScheduleError::NotHeld)
            );
            edit(
                &fixture,
                "UPDATE repository_identity SET owner='replacement'; UPDATE ref_generation SET visibility='private'",
            )
            .await?;
            drop(ticket);
            let closed = coordinator.close_and_drain().await;
            assert_eq!(closed.len(), 1);
            let retained = coordinator
                .pending([66; 16])
                .await
                .ok_or("exact held command lost")?;
            retained.recover().await?;
            match timeout(Duration::from_secs(10), retained.wait()).await? {
                PublicationState::Finished(Ok(PublicationOutcome::Push(value))) if fault != 1 => {
                    assert_eq!(Some(value.receipt.commit_sequence), original);
                    assert!(matches!(value.output, CatalogCompletionReply::Completed(_)));
                    assert_eq!(outcomes(&fixture.handle).await?, 1);
                    assert_eq!(retained.response().await?, refused());
                    assert!(matches!(
                        replay_push_response(
                            &fixture.client(),
                            &fixture.target,
                            fixture.begin([66; 16]),
                            None
                        )
                        .await,
                        Err(CatalogPushResponseError::Denied(
                            PreparationDenial::Unauthorized
                        ))
                    ));
                    edit(&fixture, "UPDATE repository_identity SET owner='owner'").await?;
                    assert_eq!(retained.response().await?, refused());
                }
                PublicationState::Finished(Err(error)) if fault == 1 => {
                    assert!(
                        matches!(error.as_ref(), PublicationError::Push(InvocationError::Rejected(value))
                        if value.output == CatalogCompletionReply::Denied(PreparationDenial::Unauthorized))
                    );
                    assert_eq!(outcomes(&fixture.handle).await?, 0);
                    assert!(retained.response().await.is_err());
                }
                other => return Err(format!("unexpected recovery {other:?}").into()),
            }
            assert_eq!(coordinator.reservations_for_test().await, (0, 0, 0));
            assert!(coordinator.close_and_drain().await.is_empty());
            drop(prepared);
            cleaned(root.path(), &budget).await?;
            fixture.runtime.shutdown().await?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn held_activation_and_discard_race_selects_one_exact_disposition() -> Result {
    let fixture = Fixture::new(ObjectFormat::Sha256).await?;
    let coordinator = PublicationCoordinator::new(
        fixture.target.clone(),
        PublicationLimits::default(),
        fixture.publication_budget.clone(),
    )?;
    let mut published = 0;
    for n in 100..108 {
        let (prepared, root, budget) = empty(&fixture, [n; 16], "owner").await?;
        let ready = Box::pin(prepared.ready_push(
            identity()?,
            request(refused()),
            root.path(),
            budget.clone(),
            limits(),
        ))
        .await?;
        let ticket = coordinator.try_reserve(ready)?;
        let activator = ticket.clone();
        let discard = ticket.clone();
        let barrier = Arc::new(tokio::sync::Barrier::new(3));
        let a_barrier = Arc::clone(&barrier);
        let d_barrier = Arc::clone(&barrier);
        let a = tokio::spawn(async move {
            a_barrier.wait().await;
            activator.activate().await
        });
        let d = tokio::spawn(async move {
            d_barrier.wait().await;
            discard.discard_held().await
        });
        barrier.wait().await;
        let (a, d) = (a.await?, d.await?);
        match (a, d) {
            (Ok(()), Err(PublicationScheduleError::NotHeld)) => {
                finished(timeout(Duration::from_secs(10), ticket.wait()).await?)?;
                published += 1;
            }
            (Err(PublicationScheduleError::NotHeld), Ok(())) => {
                assert!(matches!(ticket.wait().await, PublicationState::Discarded))
            }
            other => return Err(format!("contradictory disposition {other:?}").into()),
        }
        assert_eq!(outcomes(&fixture.handle).await?, published);
        assert_eq!(coordinator.reservations_for_test().await, (0, 0, 0));
        drop(prepared);
        cleaned(root.path(), &budget).await?;
    }
    assert!(coordinator.close_and_drain().await.is_empty());
    fixture.runtime.shutdown().await?;
    Ok(())
}
