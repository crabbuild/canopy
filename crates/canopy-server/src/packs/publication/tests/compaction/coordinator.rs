use super::super::coordinator::{empty_in_store, finished, refused, request};
use super::*;
use tokio::time::{Duration, timeout};

fn compacted(state: PublicationState) -> Result<cellule_runtime::Committed<CompactionReply>> {
    match state {
        PublicationState::Finished(Ok(PublicationOutcome::Compaction(value))) => Ok(value),
        other => Err(format!("unexpected {other:?}").into()),
    }
}

#[tokio::test]
async fn maintenance_final_publication_uses_shared_bound_lifecycle_and_reserved_fair_dispatch()
-> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let fixture = Fixture::new(format).await?;
        let inventory = seed(&fixture, 2).await?;
        let before_refs = refs(&fixture.handle).await?;
        let stages = StagingCoordinator::new(
            fixture.target.clone(),
            StagingLimits::default(),
            fixture.authority(),
        )?;
        let ready = ReadyStaging::new(
            fixture.client(),
            fixture.target.clone(),
            fixture.begin([184; 16]),
            identity()?,
        )
        .await?;
        let ticket = stages.submit(ready).map_err(|(e, _)| e)?;
        assert!(matches!(
            timeout(Duration::from_secs(10), ticket.wait()).await?,
            StagingState::Active(_)
        ));
        ticket.seal()?;
        assert!(matches!(
            timeout(Duration::from_secs(10), ticket.wait_terminal()).await?,
            StagingState::Bound(_)
        ));
        let session = ticket.bound_session()?;
        let root = Arc::new(tempfile::TempDir::new()?);
        let budget = DiskBudget::new(128 << 20);
        let indexes = Arc::new(CatalogIndexes::new(inventory.store.clone(), format));
        let files = Arc::new(CatalogFiles::new(
            fixture.root.path(),
            DiskBudget::new(64 << 20),
            inventory.store.clone(),
            format,
            crate::packs::catalog::CatalogFileLimits::default(),
        )?);
        let base = Arc::new(ticket.open_base(indexes, files).await?);
        let owned_root = root.clone();
        let owned_budget = budget.clone();
        let mutation = identity()?;
        let work = ticket.spawn_bound(move |_| async move {
            let prepared = Arc::new(
                PreparedCompaction::prepare(
                    owned_root.path(),
                    owned_budget,
                    base,
                    &[0, 1],
                    CompactionLimits {
                        spool: limits(),
                        output: crate::packs::metadata::MetadataLimits {
                            max_file_bytes: 16 << 10,
                            cache_kib: 16,
                        },
                        ..CompactionLimits::default()
                    },
                )
                .await
                .map_err(|e| StagingError::Input(Box::new(e)))?,
            );
            let weak = Arc::downgrade(&prepared);
            let ready = prepared
                .ready_compaction(mutation)
                .await
                .map_err(|e| StagingError::Input(Box::new(e)))?;
            Ok((ready, weak))
        })?;
        let (ready, weak) = work.wait().await.map_err(|e| e.to_string())?;
        let publications =
            PublicationCoordinator::new(fixture.target.clone(), PublicationLimits::default())?;
        let (release, entered) = publications.pause_for_test().await;
        let observer = ticket.publish(&publications, ready)?;
        timeout(Duration::from_secs(10), entered).await??;
        let stats = publications.stats().await;
        assert_eq!(
            (stats.foreground, stats.maintenance, stats.command_bytes),
            (0, 1, 8 << 10)
        );
        assert!(weak.upgrade().is_some());
        assert_eq!(outcomes(&fixture.handle).await?, 0);
        release
            .send(())
            .map_err(|_| "maintenance transport disappeared")?;
        let completed = compacted(timeout(Duration::from_secs(10), observer.wait()).await?)?;
        assert!(matches!(
            completed.output,
            CompactionReply::Published(PublishedCompaction { generation: 3, .. })
        ));
        assert!(matches!(
            timeout(Duration::from_secs(10), ticket.wait_terminal()).await?,
            StagingState::Published(Ok(PublicationOutcome::Compaction(_)))
        ));
        assert!(observer.response().await.is_err());
        assert!(session.live_lease().is_err());
        assert_eq!(refs(&fixture.handle).await?, before_refs);
        assert_eq!(outcomes(&fixture.handle).await?, 1);
        assert!(weak.upgrade().is_none());
        cleaned(root.path(), &budget).await?;
        assert!(stages.close_and_drain().await.is_empty());
        assert!(publications.close_and_drain().await.is_empty());
        fixture.runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn uncertain_compaction_retains_exact_command_and_recovers_original_receipt() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        for fault in [1, 2, 3] {
            let fixture = Fixture::new(format).await?;
            let inventory = seed(&fixture, 2).await?;
            let before_refs = refs(&fixture.handle).await?;
            let prepared = prepare_compaction(&fixture, &inventory, 180, &[0, 1]).await?;
            let compact = Arc::new(prepared.compact);
            let weak = Arc::downgrade(&compact);
            let ready = compact.ready_compaction(identity()?).await?;
            let coordinator =
                PublicationCoordinator::new(fixture.target.clone(), PublicationLimits::default())?;
            coordinator.fault_for_test(fault);
            let ticket = coordinator.try_reserve(ready)?;
            assert_eq!(ticket.class(), PublicationClass::Maintenance);
            assert_eq!(coordinator.stats().await.held, 1);
            assert_eq!(outcomes(&fixture.handle).await?, 0);
            drop(compact);
            assert!(weak.upgrade().is_some());
            ticket.activate().await?;
            let uncertain = timeout(Duration::from_secs(10), ticket.wait()).await?;
            let PublicationState::Uncertain(error) = uncertain else {
                return Err("uncertainty lost".into());
            };
            let PublicationError::Compaction(InvocationError::Pending(evidence)) = error.as_ref()
            else {
                return Err("wrong evidence kind".into());
            };
            let original = match fixture.client().resolve(evidence).await? {
                cellule_runtime::Resolution::Committed(value) => Some(value.commit_sequence()),
                cellule_runtime::Resolution::Absent => None,
                other => return Err(format!("unexpected resolution {other:?}").into()),
            };
            assert_eq!(original.is_some(), fault != 1);
            let stats = coordinator.stats().await;
            assert_eq!(
                (
                    stats.foreground,
                    stats.maintenance,
                    stats.uncertain,
                    stats.command_bytes
                ),
                (0, 1, 1, 8 << 10)
            );
            assert!(weak.upgrade().is_some());
            assert_eq!(
                fixture
                    .client()
                    .query::<CheckCompletedCompaction>(
                        &fixture.target,
                        None,
                        fixture.begin([180; 16])
                    )
                    .await?
                    .output
                    .is_some(),
                fault != 1
            );
            edit(
                &fixture,
                "UPDATE ref_generation SET visibility='public' WHERE singleton=1",
            )
            .await?;
            drop(ticket);
            let retained = coordinator.pending([180; 16]).await.ok_or("input lost")?;
            let drained = coordinator.close_and_drain().await;
            assert_eq!(drained.len(), 1);
            assert_eq!(drained[0].class(), PublicationClass::Maintenance);
            retained.recover().await?;
            let completed = compacted(timeout(Duration::from_secs(10), retained.wait()).await?)?;
            if let Some(sequence) = original {
                assert_eq!(completed.receipt.commit_sequence, sequence);
            }
            assert!(matches!(
                completed.output,
                CompactionReply::Published(PublishedCompaction { generation: 3, .. })
            ));
            assert_eq!(
                fixture
                    .client()
                    .query::<CheckCompletedCompaction>(
                        &fixture.target,
                        None,
                        fixture.begin([180; 16])
                    )
                    .await?
                    .output,
                Some(completed.output)
            );
            assert_eq!(refs(&fixture.handle).await?, before_refs);
            assert_eq!(outcomes(&fixture.handle).await?, 1);
            assert!(retained.response().await.is_err());
            assert!(weak.upgrade().is_none());
            assert_eq!(coordinator.reservations_for_test().await, (0, 0, 0));
            assert!(coordinator.close_and_drain().await.is_empty());
            cleaned(prepared.root.path(), &prepared.budget).await?;
            fixture.runtime.shutdown().await?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn reserved_classes_and_actor_quotas_keep_mixed_admission_bounded() -> Result {
    let fixture = Fixture::new(ObjectFormat::Sha256).await?;
    let inventory = seed(&fixture, 2).await?;
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
            command_bytes: (24 << 20) + (16 << 10),
            in_flight: 1,
            maintenance_operations: 2,
            maintenance_in_flight: 1,
            foreground_burst: 3,
        },
    )?;
    let (release, entered) = coordinator.pause_for_test().await;
    let mut entered = Some(entered);
    let mut foreground = Vec::new();
    let mut tickets = Vec::new();
    for (n, actor) in [(90, "owner"), (91, "owner"), (92, "writer")] {
        let (prepared, root, budget) =
            empty_in_store(&fixture, [n; 16], actor, Arc::clone(&inventory.store)).await?;
        let ready = Box::pin(prepared.ready_push(
            identity()?,
            request(refused()),
            root.path(),
            budget.clone(),
            limits(),
        ))
        .await?;
        tickets.push(coordinator.submit(ready).await?);
        if let Some(entered) = entered.take() {
            timeout(Duration::from_secs(5), entered).await??;
        }
        foreground.push((prepared, root, budget));
    }
    let before_refs = refs(&fixture.handle).await?;
    let mut maintenance = Vec::new();
    for operation in [180, 181] {
        let prepared = prepare_compaction(&fixture, &inventory, operation, &[0, 1]).await?;
        let compact = Arc::new(prepared.compact);
        tickets.push(coordinator.try_reserve(compact.ready_compaction(identity()?).await?)?);
        maintenance.push((compact, prepared.root, prepared.budget));
    }
    let stats = coordinator.stats().await;
    assert_eq!((stats.held, stats.in_flight, stats.queued), (2, 1, 2));
    assert_eq!(
        (
            stats.admitted,
            stats.foreground,
            stats.maintenance,
            stats.accounts,
            stats.command_bytes
        ),
        (5, 3, 2, 2, (24 << 20) + (16 << 10))
    );
    let extra = prepare_compaction(&fixture, &inventory, 182, &[0, 1]).await?;
    let extra = Arc::new(extra.compact);
    let failure = coordinator
        .submit(extra.ready_compaction(identity()?).await?)
        .await
        .err()
        .ok_or("maintenance overflow")?;
    assert_eq!(failure.reason, PublicationScheduleError::Capacity);
    assert!(matches!(failure.ready, ReadyPublication::Compaction(_)));
    let duplicate = coordinator
        .submit(maintenance[0].0.ready_compaction(identity()?).await?)
        .await
        .err()
        .ok_or("duplicate")?;
    assert_eq!(duplicate.reason, PublicationScheduleError::Duplicate);
    // The logical ID namespace is shared even when trusted preparation can
    // construct a different purpose-bound command under that same lease.
    let push_root = tempfile::TempDir::new()?;
    let push_budget = DiskBudget::new(256 << 20);
    let push_base = Arc::new(maintenance[0].0.preparation_base().select_current().await?);
    let push = Arc::new(
        CatalogPreparation::new(push_root.path(), push_budget.clone(), push_base, limits())
            .await?
            .finish()
            .await?,
    );
    let ready = Box::pin(push.ready_push(
        identity()?,
        request(refused()),
        push_root.path(),
        push_budget.clone(),
        limits(),
    ))
    .await?;
    let cross_class = coordinator
        .submit(ready)
        .await
        .err()
        .ok_or("cross-class duplicate")?;
    assert_eq!(cross_class.reason, PublicationScheduleError::Duplicate);
    assert!(matches!(cross_class.ready, ReadyPublication::Push(_)));
    for ticket in &tickets {
        ticket.activate().await?;
    }
    release.send(()).map_err(|_| "worker disappeared")?;
    for ticket in tickets {
        let state = timeout(Duration::from_secs(10), ticket.wait()).await?;
        match ticket.class() {
            PublicationClass::Foreground => {
                finished(state)?;
                assert_eq!(ticket.response().await?, refused());
            }
            PublicationClass::Maintenance => match state {
                PublicationState::Finished(Ok(PublicationOutcome::Compaction(value))) => {
                    assert!(matches!(value.output, CompactionReply::Published(_)))
                }
                PublicationState::Finished(Err(error)) => assert!(
                    matches!(error.as_ref(), PublicationError::Compaction(InvocationError::Rejected(value)) if value.output==CompactionReply::Denied(PreparationDenial::Conflict))
                ),
                other => return Err(format!("unexpected {other:?}").into()),
            },
        }
    }
    assert_eq!(refs(&fixture.handle).await?, before_refs);
    assert_eq!(outcomes(&fixture.handle).await?, 1);
    assert!(coordinator.close_and_drain().await.is_empty());
    assert_eq!(coordinator.reservations_for_test().await, (0, 0, 0));
    drop(failure);
    drop(duplicate);
    drop(extra);
    drop(cross_class);
    drop(push);
    cleaned(push_root.path(), &push_budget).await?;
    for (prepared, root, budget) in foreground {
        drop(prepared);
        cleaned(root.path(), &budget).await?;
    }
    for (compact, root, budget) in maintenance {
        drop(compact);
        cleaned(root.path(), &budget).await?;
    }
    fixture.runtime.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn queued_compaction_rechecks_admin_and_canceled_observer_cannot_cancel_publication() -> Result
{
    for revoke in [false, true] {
        let fixture = Fixture::new(ObjectFormat::Sha1).await?;
        let inventory = seed(&fixture, 2).await?;
        let prepared = prepare_compaction(&fixture, &inventory, 180, &[0, 1]).await?;
        let compact = Arc::new(prepared.compact);
        let weak = Arc::downgrade(&compact);
        let coordinator =
            PublicationCoordinator::new(fixture.target.clone(), PublicationLimits::default())?;
        let (release, entered) = coordinator.pause_for_test().await;
        let ticket = coordinator
            .submit(compact.ready_compaction(identity()?).await?)
            .await?;
        timeout(Duration::from_secs(5), entered).await??;
        drop(ticket);
        drop(compact);
        assert!(weak.upgrade().is_some());
        if revoke {
            edit(&fixture, "UPDATE repository_identity SET owner='other'; INSERT INTO repository_members VALUES('owner','write')").await?;
        }
        let before = refs(&fixture.handle).await?;
        let observer = coordinator.pending([180; 16]).await.ok_or("job lost")?;
        release.send(()).map_err(|_| "worker disappeared")?;
        let outcome = timeout(Duration::from_secs(10), observer.wait()).await?;
        if revoke {
            assert!(
                matches!(outcome, PublicationState::Finished(Err(ref error)) if matches!(error.as_ref(), PublicationError::Compaction(InvocationError::Rejected(value)) if value.output==CompactionReply::Denied(PreparationDenial::Unauthorized)))
            );
            assert_eq!(outcomes(&fixture.handle).await?, 0);
        } else {
            compacted(outcome)?;
            assert_eq!(outcomes(&fixture.handle).await?, 1);
        }
        assert_eq!(refs(&fixture.handle).await?, before);
        assert!(weak.upgrade().is_none());
        assert!(coordinator.close_and_drain().await.is_empty());
        cleaned(prepared.root.path(), &prepared.budget).await?;
        fixture.runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn maintenance_concurrency_cap_keeps_foreground_progressing() -> Result {
    let fixture = Fixture::new(ObjectFormat::Sha256).await?;
    let inventory = seed(&fixture, 2).await?;
    let first = prepare_compaction(&fixture, &inventory, 180, &[0, 1]).await?;
    let second = prepare_compaction(&fixture, &inventory, 181, &[0, 1]).await?;
    let first_compact = Arc::new(first.compact);
    let second_compact = Arc::new(second.compact);
    let foreground =
        empty_in_store(&fixture, [90; 16], "owner", Arc::clone(&inventory.store)).await?;
    let coordinator = PublicationCoordinator::new(
        fixture.target.clone(),
        PublicationLimits {
            in_flight: 2,
            maintenance_in_flight: 1,
            ..PublicationLimits::default()
        },
    )?;
    let (release, entered) = coordinator.pause_for_test().await;
    let a = coordinator
        .submit(first_compact.ready_compaction(identity()?).await?)
        .await?;
    timeout(Duration::from_secs(5), entered).await??;
    let b = coordinator
        .submit(second_compact.ready_compaction(identity()?).await?)
        .await?;
    let ready = Box::pin(foreground.0.ready_push(
        identity()?,
        request(refused()),
        foreground.1.path(),
        foreground.2.clone(),
        limits(),
    ))
    .await?;
    let push = coordinator.submit(ready).await?;
    finished(timeout(Duration::from_secs(10), push.wait()).await?)?;
    assert_eq!(push.response().await?, refused());
    assert!(matches!(a.state(), PublicationState::Running));
    assert!(matches!(b.state(), PublicationState::Queued));
    assert_eq!(coordinator.reservations_for_test().await, (2, 16 << 10, 1));
    release.send(()).map_err(|_| "worker disappeared")?;
    compacted(timeout(Duration::from_secs(10), a.wait()).await?)?;
    assert!(
        matches!(timeout(Duration::from_secs(10), b.wait()).await?, PublicationState::Finished(Err(ref error)) if matches!(error.as_ref(), PublicationError::Compaction(InvocationError::Rejected(value)) if value.output==CompactionReply::Denied(PreparationDenial::Conflict)))
    );
    assert_eq!(outcomes(&fixture.handle).await?, 1);
    assert!(coordinator.close_and_drain().await.is_empty());
    drop(first_compact);
    drop(second_compact);
    drop(foreground.0);
    cleaned(first.root.path(), &first.budget).await?;
    cleaned(second.root.path(), &second.budget).await?;
    cleaned(foreground.1.path(), &foreground.2).await?;
    fixture.runtime.shutdown().await?;
    Ok(())
}
