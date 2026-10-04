//! Separate logical retirement, actual receiver races and bounded maintenance.
use super::{publishing::edit, staging_service::restore::head_expiring, *};
use cellule_runtime::Resolution;
use tokio::time::{Duration, timeout};

async fn expired(value: &cellule_runtime::PendingMutation) -> Result {
    let now = sql::now(0)?;
    if now <= value.identity().expires_at_ms {
        tokio::time::sleep(Duration::from_millis(
            (value.identity().expires_at_ms - now + 1) as u64,
        ))
        .await;
    }
    Ok(())
}
async fn registered(f: &Fixture, kind: u8) -> Result<RegisteredCustody> {
    Ok(
        RegisteredCustody::load_latest(&f.client(), &f.target, [230 + kind; 16])
            .await?
            .ok_or("original missing")?,
    )
}
fn stopped(state: PublicationState) -> Result<CustodyStopOutcome> {
    match state {
        PublicationState::Finished(Ok(PublicationOutcome::CustodyStop(value))) => Ok(*value),
        other => Err(format!("stop outcome: {other:?}").into()),
    }
}
async fn observed(ticket: &PublicationTicket) -> Result<PublicationState> {
    Ok(timeout(Duration::from_secs(10), ticket.wait()).await?)
}

#[tokio::test]
async fn stop_all_seven_expired_originals_preserves_unknown_outcomes_and_allows_explicit_successors()
-> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        for kind in 0..7 {
            let f = Fixture::new(format).await?;
            let (evidence, _) = head_expiring(&f, kind, false, true).await?;
            expired(&evidence).await?;
            let original = registered(&f, kind).await?;
            let counts = f.counts().await?;
            let ready = original
                .ready_stop(f.client(), identity()?, &f.authority())
                .await?;
            let stop_evidence = ready.evidence().clone();
            let queue = PublicationCoordinator::new(
                f.target.clone(),
                PublicationLimits::default(),
                f.publication_budget.clone(),
            )?;
            let ticket = queue.submit(ready).await?;
            let outcome = stopped(observed(&ticket).await?)?;
            assert_eq!(outcome.original, evidence);
            assert_eq!(outcome.invocation, stop_evidence);
            assert_eq!(
                outcome
                    .committed
                    .as_ref()
                    .ok_or("stop invocation lost")?
                    .output,
                CustodyStopReply::Stopped
            );
            let fact = outcome.stop.ok_or("stop fact missing")?;
            assert_eq!(
                fact.receipt,
                outcome.committed.ok_or("stop receipt missing")?.receipt
            );
            assert_eq!(fact.owner, f.handle.owner_fence());
            assert_eq!(f.counts().await?, counts);
            let loaded = registered(&f, kind).await?;
            assert_eq!(loaded.evidence(), &evidence);
            assert!(loaded.closed());
            assert!(!loaded.settled());
            assert_eq!(loaded.stop_fact(), Some(fact.clone()));
            assert!(
                matches!(loaded.recover(&f.client()).await, Err(InvocationError::Pending(value)) if *value == evidence)
            );
            assert!(matches!(
                f.client().resolve(&evidence).await?,
                Resolution::Expired
            ));
            assert!(
                matches!(loaded.ready_stop(f.client(), identity()?, &f.authority()).await, Err(CustodyError::Stopped(value)) if *value == fact)
            );
            let staging =
                StagingCoordinator::new(f.target.clone(), StagingLimits::default(), f.authority())?;
            let cold = staging
                .submit(
                    ReadyStaging::restore(f.client(), f.target.clone(), [230 + kind; 16]).await?,
                )
                .map_err(|(error, _)| error)?;
            let StagingState::Fenced(error) =
                timeout(Duration::from_secs(10), cold.wait_terminal()).await?
            else {
                return Err("stopped cold stage not fenced".into());
            };
            assert!(
                matches!(&*error, StagingError::Custody { source, .. } if matches!(&**source, CustodyError::Stopped(_)))
            );
            assert_eq!(cold.restored_evidence(), Some(&evidence));
            assert!(cold.restored_outcome().is_none());
            assert_eq!(staging.stats().command_bytes, 0);
            assert!(staging.close_and_drain().await.is_empty());
            // A deliberate successor keeps logical actor/digest continuity and
            // does not erase or assign an invented result to the stopped original.
            let next =
                PreparedCustody::prepare(&f.client(), &f.target, loaded.action()?, identity()?)
                    .await?;
            assert_ne!(next.evidence(), &evidence);
            let next = next.register(&f.client(), identity()?).await?;
            assert!(!next.closed());
            assert_eq!(original.stop_fact(), None); // Old DTO is not fresh state.
            assert!(queue.close_and_drain().await.is_empty());
            f.runtime.shutdown().await?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn stop_receiver_refuses_live_forged_and_stale_owner_proofs_and_preserves_accepted_originals()
-> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let f = Fixture::new(format).await?;
        let (evidence, _) = head_expiring(&f, 0, false, false).await?;
        let original = registered(&f, 0).await?;
        let ready = original
            .ready_stop(f.client(), identity()?, &f.authority())
            .await?;
        assert!(matches!(ready.command_for_test().execute().await,
            Err(InvocationError::Rejected(value)) if value.output == CustodyStopReply::Denied(PreparationDenial::Conflict)));
        assert!(!registered(&f, 0).await?.closed());
        let forged = f
            .client()
            .prepare_command::<StopCustodyIntent>(
                &f.target,
                identity()?,
                ready.input_for_test()?.tamper_for_test(),
            )
            .await?;
        assert!(matches!(forged.execute().await,
            Err(InvocationError::Rejected(value)) if value.output == CustodyStopReply::Denied(PreparationDenial::Unauthorized)));
        let ready = original
            .ready_stop(f.client(), identity()?, &f.authority())
            .await?;
        let expected = original.recover(&f.client()).await?;
        // Execute won before stop. Even revoked access must not rewrite history.
        edit(&f, "UPDATE repository_identity SET owner='other'").await?;
        let command = ready.command_for_test();
        assert_eq!(command.execute().await?.output, CustodyStopReply::Settled);
        assert_eq!(
            registered(&f, 0).await?.recover(&f.client()).await?,
            expected
        );
        assert!(registered(&f, 0).await?.stop_fact().is_none());
        assert!(matches!(
            f.client().resolve(&evidence).await?,
            Resolution::Committed(_)
        ));
        f.runtime.shutdown().await?;

        let f = Fixture::new(format).await?;
        let (evidence, _) = head_expiring(&f, 0, false, true).await?;
        let original = registered(&f, 0).await?;
        let ready = original
            .ready_stop(f.client(), identity()?, &f.authority())
            .await?;
        let input = ready.input_for_test()?;
        let (runtime, handle, client) =
            super::durable_recovery::restore_owner_fence(&f, f.handle.owner_fence()).await?;
        expired(&evidence).await?;
        let stale = client
            .prepare_command::<StopCustodyIntent>(&f.target, identity()?, input)
            .await?;
        assert!(matches!(stale.execute().await,
            Err(InvocationError::Rejected(value)) if value.output == CustodyStopReply::Denied(PreparationDenial::Stale)));
        let original = RegisteredCustody::load_latest(&client, &f.target, [230; 16])
            .await?
            .ok_or("restored head")?;
        assert!(!original.closed());
        let fresh = original
            .ready_stop(client.clone(), identity()?, &f.authority())
            .await?;
        assert_eq!(
            fresh.command_for_test().execute().await?.output,
            CustodyStopReply::Stopped
        );
        assert_eq!(
            RegisteredCustody::load_latest(&client, &f.target, [230; 16])
                .await?
                .ok_or("stopped head")?
                .stop_fact()
                .ok_or("actual stop")?
                .owner,
            handle.owner_fence()
        );
        runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn stop_first_writer_fact_is_immutable_and_is_not_an_unexecuted_invocations_receipt() -> Result
{
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let f = Fixture::new(format).await?;
        let (evidence, _) = head_expiring(&f, 4, false, true).await?;
        expired(&evidence).await?;
        let original = registered(&f, 4).await?;
        let first = original
            .ready_stop(f.client(), identity()?, &f.authority())
            .await?;
        let second = original
            .ready_stop(f.client(), identity()?, &f.authority())
            .await?;
        let first_receipt = first.command_for_test().execute().await?.receipt;
        let invocation = second.evidence().clone();
        let queue = PublicationCoordinator::new(
            f.target.clone(),
            PublicationLimits::default(),
            f.publication_budget.clone(),
        )?;
        let outcome = stopped(observed(&queue.submit(second).await?).await?)?;
        assert_eq!(outcome.invocation, invocation);
        assert!(outcome.committed.is_none());
        assert_eq!(
            outcome.stop.ok_or("first writer fact")?.receipt,
            first_receipt
        );
        assert!(matches!(
            f.client().resolve(&invocation).await?,
            Resolution::Absent
        ));
        assert!(matches!(
            f.client().resolve(&evidence).await?,
            Resolution::Expired
        ));
        for sql in [
            "UPDATE catalog_custody_commands SET stopped=NULL",
            "UPDATE catalog_custody_commands SET stopped=x'01'",
            "UPDATE catalog_custody_commands SET phase=x'01'",
            "DELETE FROM catalog_custody_commands",
        ] {
            assert!(edit(&f, sql).await.is_err(), "{sql}");
        }
        assert!(queue.close_and_drain().await.is_empty());
        f.runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn stop_late_failure_and_ignored_write_rollback_marker_and_sdk_acceptance_before_exact_retry()
-> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        for ignored in [false, true] {
            let f = Fixture::new(format).await?;
            let (evidence, _) = head_expiring(&f, 0, false, true).await?;
            expired(&evidence).await?;
            let original = registered(&f, 0).await?;
            let ready = original
                .ready_stop(f.client(), identity()?, &f.authority())
                .await?;
            let command = ready.command_for_test();
            let action = if ignored {
                "RAISE(IGNORE)"
            } else {
                "RAISE(ABORT,'late stop fault')"
            };
            edit(&f, &format!("CREATE TRIGGER stop_fault BEFORE UPDATE OF stopped ON catalog_custody_commands BEGIN SELECT {action}; END")).await?;
            assert!(matches!(
                command.clone().execute().await,
                Err(InvocationError::NotStarted(_))
            ));
            assert!(matches!(
                f.client().resolve(command.evidence()).await?,
                Resolution::Absent
            ));
            assert!(!registered(&f, 0).await?.closed());
            edit(&f, "DROP TRIGGER stop_fault").await?;
            let result = command.clone().execute().await?;
            assert_eq!(result.output, CustodyStopReply::Stopped);
            assert_eq!(
                registered(&f, 0)
                    .await?
                    .stop_fact()
                    .ok_or("stop missing")?
                    .receipt,
                result.receipt
            );
            assert!(matches!(
                f.client().resolve(&evidence).await?,
                Resolution::Expired
            ));
            f.runtime.shutdown().await?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn stop_service_reply_loss_and_panic_keep_bounded_originals_through_closed_recovery() -> Result
{
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        for fault in 0..=3 {
            let f = Fixture::new(format).await?;
            let (evidence, _) = head_expiring(&f, 0, false, true).await?;
            expired(&evidence).await?;
            let mut invocation_identity = identity()?;
            if fault > 1 {
                invocation_identity.expires_at_ms = invocation_identity.issued_at_ms + 1_000;
            }
            let ready = registered(&f, 0)
                .await?
                .ready_stop(f.client(), invocation_identity, &f.authority())
                .await?;
            let invocation = ready.evidence().clone();
            let queue = PublicationCoordinator::new(
                f.target.clone(),
                PublicationLimits::default(),
                f.publication_budget.clone(),
            )?;
            if fault == 0 {
                edit(
                    &f,
                    "ALTER TABLE catalog_custody_commands RENAME TO custody_stop_query_fault",
                )
                .await?;
            } else {
                queue.fault_for_test(fault);
            }
            let ticket = queue.submit(ready).await?;
            assert!(matches!(
                observed(&ticket).await?,
                PublicationState::Uncertain(_)
            ));
            let stats = queue.stats().await;
            assert_eq!(stats.maintenance, 1);
            assert_eq!(stats.foreground, 0);
            assert_eq!(stats.command_bytes, 8 << 10);
            drop(ticket);
            let ticket = queue
                .pending_custody_stop([230; 16])
                .await
                .ok_or("lost observer")?;
            assert_eq!(queue.close_and_drain().await.len(), 1);
            if fault == 0 {
                assert!(matches!(
                    f.client().resolve(&invocation).await?,
                    Resolution::Absent
                ));
                edit(
                    &f,
                    "ALTER TABLE custody_stop_query_fault RENAME TO catalog_custody_commands",
                )
                .await?;
            }
            if fault > 1 {
                expired(&invocation).await?;
                edit(&f, "UPDATE repository_identity SET owner='other'").await?;
                assert!(matches!(
                    f.client().resolve(&invocation).await?,
                    Resolution::Expired
                ));
            }
            queue.recover(&ticket).await?;
            let outcome = stopped(observed(&ticket).await?)?;
            assert_eq!(outcome.original, evidence);
            assert_eq!(outcome.invocation, invocation);
            assert_eq!(
                outcome.committed.ok_or("stop result lost")?.output,
                CustodyStopReply::Stopped
            );
            assert!(outcome.stop.is_some());
            assert_eq!(queue.stats().await.command_bytes, 0);
            assert!(queue.close_and_drain().await.is_empty());
            f.runtime.shutdown().await?;
        }
    }
    Ok(())
}

async fn junk(f: &Fixture, count: usize) -> Result {
    edit(f, &format!("WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<{count}) INSERT INTO catalog_custody_commands(operation,step,incarnation,request_id,intent) SELECT CAST(printf('%016d',x) AS BLOB),0,zeroblob(16),CAST(printf('%016d',x) AS BLOB),x'01' FROM n")).await?;
    Ok(())
}

#[tokio::test]
async fn stop_reclaims_only_pending_quota_and_retains_original_history() -> Result {
    let f = Fixture::new(ObjectFormat::Sha256).await?;
    let (evidence, _) = head_expiring(&f, 0, false, true).await?;
    junk(&f, 1023).await?;
    let next = PreparedCustody::prepare(
        &f.client(),
        &f.target,
        CustodyAction::BeginPreparation(f.begin([248; 16])),
        identity()?,
    )
    .await?;
    assert!(next.register(&f.client(), identity()?).await.is_err());
    expired(&evidence).await?;
    let ready = registered(&f, 0)
        .await?
        .ready_stop(f.client(), identity()?, &f.authority())
        .await?;
    ready.command_for_test().execute().await?;
    next.register(&f.client(), identity()?).await?;
    f.handle.query(0, 4096, |db| {
        assert_eq!(db.query_row("SELECT count(*) FROM catalog_custody_commands WHERE phase IS NULL AND stopped IS NULL", [], |r| r.get::<_,i64>(0))?,1024);
        assert_eq!(db.query_row("SELECT count(*) FROM catalog_custody_commands", [], |r| r.get::<_,i64>(0))?,1025);
        Ok(Vec::new())
    }).await?;
    assert_eq!(registered(&f, 0).await?.evidence(), &evidence);
    f.runtime.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn custody_scan_uses_bounded_indexed_pages_and_revisits_corruption_without_starving_tail_heads()
-> Result {
    let f = Fixture::new(ObjectFormat::Sha256).await?;
    junk(&f, 300).await?;
    let (evidence, _) = head_expiring(&f, 0, false, true).await?;
    expired(&evidence).await?;
    f.handle.query(0,4096,|db| {
        let mut query = db.prepare("EXPLAIN QUERY PLAN SELECT operation FROM catalog_custody_commands INDEXED BY catalog_custody_pending WHERE phase IS NULL AND stopped IS NULL AND operation>?1 ORDER BY operation LIMIT ?2")?;
        let details: Vec<String> = query.query_map(rusqlite::params![vec![0u8;16],17], |r| r.get(3))?.collect::<std::result::Result<_,_>>()?;
        assert!(details.iter().any(|v|v.contains("SEARCH") && v.contains("catalog_custody_pending")),"{details:?}");
        assert!(details.iter().all(|v|!v.contains("TEMP B-TREE")),"{details:?}");
        Ok(Vec::new())
    }).await?;
    let queue = PublicationCoordinator::new(
        f.target.clone(),
        PublicationLimits::default(),
        f.publication_budget.clone(),
    )?;
    let limits = RecoveryScanLimits {
        page: 17,
        interval: Duration::from_millis(10),
    };
    let service = CustodySupervisor::start(
        f.client(),
        f.target.clone(),
        queue.clone(),
        f.scans(limits),
        f.authority(),
    )?;
    timeout(Duration::from_secs(10), async {
        loop {
            if service.stats().passes >= 2 && registered(&f, 0).await?.stop_fact().is_some() {
                return Ok::<_, Box<dyn std::error::Error>>(());
            }
            tokio::task::yield_now().await;
        }
    })
    .await??;
    let stats = service.stats();
    assert!(stats.failures >= 600);
    assert!(stats.last_error.is_some());
    assert_eq!(stats.submitted, 1);
    // A head behind the current cursor must be revisited on a later pass.
    let next = PreparedCustody::prepare(
        &f.client(),
        &f.target,
        CustodyAction::BeginStaging(f.begin([1; 16])),
        identity()?,
    )
    .await?;
    next.register(&f.client(), identity()?).await?;
    let passes = service.stats().passes;
    timeout(Duration::from_secs(10), async {
        while service.stats().passes < passes + 3 {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    assert!(service.stats().deferred >= 1);
    service.shutdown().await?;
    assert!(queue.close_and_drain().await.is_empty());
    assert_eq!(queue.stats().await.command_bytes, 0);
    for invalid in [0, 129] {
        assert!(
            CustodySupervisor::start(
                f.client(),
                f.target.clone(),
                queue.clone(),
                f.scans(RecoveryScanLimits {
                    page: invalid,
                    ..limits
                }),
                f.authority()
            )
            .is_err()
        );
    }
    f.runtime.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn stopped_originals_survive_real_owner_restore_and_stop_sdk_expiry_without_current_permission()
-> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let f = Fixture::new(format).await?;
        let (evidence, _) = head_expiring(&f, 4, false, true).await?;
        expired(&evidence).await?;
        let original = registered(&f, 4).await?;
        let mut mutation = identity()?;
        mutation.expires_at_ms = mutation.issued_at_ms + 1_000;
        let ready = original
            .ready_stop(f.client(), mutation, &f.authority())
            .await?;
        let invocation = ready.evidence().clone();
        let accepted = ready.command_for_test().execute().await?;
        let expected = registered(&f, 4)
            .await?
            .stop_fact()
            .ok_or("durable retirement")?;
        assert_eq!(expected.receipt, accepted.receipt);
        drop(ready);
        drop(original);
        edit(&f, "UPDATE repository_identity SET owner='other'").await?;
        let (runtime, handle, client) =
            super::durable_recovery::restore_owner_fence(&f, f.handle.owner_fence()).await?;
        assert_ne!(handle.owner_fence(), expected.owner);
        expired(&invocation).await?;
        assert!(matches!(
            client.resolve(&invocation).await?,
            Resolution::Expired
        ));
        let loaded = RegisteredCustody::load_latest(&client, &f.target, [234; 16])
            .await?
            .ok_or("restored original")?;
        assert_eq!(loaded.evidence(), &evidence);
        assert!(!loaded.settled());
        assert!(loaded.closed());
        assert_eq!(loaded.stop_fact(), Some(expected));
        assert!(
            matches!(loaded.recover(&client).await,Err(InvocationError::Pending(value)) if *value==evidence)
        );
        let stage =
            StagingCoordinator::new(f.target.clone(), StagingLimits::default(), f.authority())?;
        let ticket = stage
            .submit(ReadyStaging::restore(client, f.target.clone(), [234; 16]).await?)
            .map_err(|(error, _)| error)?;
        assert!(matches!(
            timeout(Duration::from_secs(10), ticket.wait_terminal()).await?,
            StagingState::Fenced(_)
        ));
        assert_eq!(ticket.restored_evidence(), Some(&evidence));
        assert!(ticket.restored_outcome().is_none());
        assert_eq!(stage.stats().command_bytes, 0);
        assert!(stage.close_and_drain().await.is_empty());
        runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn custody_scan_recovers_exact_maintenance_commands_after_their_pending_keys_disappear()
-> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        for fault in 1..=3 {
            let f = Fixture::new(format).await?;
            let (evidence, _) = head_expiring(&f, 0, false, true).await?;
            expired(&evidence).await?;
            let staging =
                StagingCoordinator::new(f.target.clone(), StagingLimits::default(), f.authority())?;
            let stage = staging
                .submit(ReadyStaging::restore(f.client(), f.target.clone(), [230; 16]).await?)
                .map_err(|(error, _)| error)?;
            assert!(matches!(
                timeout(Duration::from_secs(10), stage.wait_terminal()).await?,
                StagingState::Uncertain(_)
            ));
            let queue = PublicationCoordinator::new(
                f.target.clone(),
                PublicationLimits::default(),
                f.publication_budget.clone(),
            )?;
            queue.fault_for_test(fault);
            let service = CustodySupervisor::start(
                f.client(),
                f.target.clone(),
                queue.clone(),
                f.scans(RecoveryScanLimits {
                    page: 1,
                    interval: Duration::from_millis(100),
                }),
                f.authority(),
            )?;
            let invocation = timeout(Duration::from_secs(10), async {
                loop {
                    if let Some(ticket) = queue.pending_custody_stop([230; 16]).await
                        && let PublicationState::Uncertain(error) = ticket.state()
                        && let PublicationError::CustodyStop(InvocationError::Pending(value)) =
                            &*error
                    {
                        return Ok::<_, Box<dyn std::error::Error>>((**value).clone());
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await??;
            timeout(Duration::from_secs(10), async {
                loop {
                    if registered(&f, 0).await?.stop_fact().is_some()
                        && queue.stats().await.command_bytes == 0
                    {
                        return Ok::<_, Box<dyn std::error::Error>>(());
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await??;
            let stats = service.shutdown().await?;
            assert_eq!(stats.submitted, 1);
            assert!(stats.recovered >= 1);
            assert!(matches!(
                f.client().resolve(&invocation).await?,
                Resolution::Committed(_)
            ));
            assert!(matches!(
                f.client().resolve(&evidence).await?,
                Resolution::Expired
            ));
            let fact = registered(&f, 0)
                .await?
                .stop_fact()
                .ok_or("stopped original")?;
            assert!(fact.receipt.commit_sequence > 0);
            // No waiter or manual recovery is needed to observe logical closure.
            timeout(Duration::from_secs(5), async {
                while !matches!(stage.state(), StagingState::Fenced(_)) {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await?;
            let StagingState::Fenced(error) = stage.state() else {
                return Err("retired stage not fenced".into());
            };
            assert!(
                matches!(&*error, StagingError::Custody { evidence: original, source }
                if **original == evidence && matches!(&**source, CustodyError::Stopped(_)))
            );
            assert_eq!(stage.restored_evidence(), Some(&evidence));
            assert!(stage.restored_outcome().is_none());
            assert_eq!(staging.stats().command_bytes, 0);
            assert!(staging.close_and_drain().await.is_empty());
            assert!(queue.close_and_drain().await.is_empty());
            f.runtime.shutdown().await?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn authenticated_stop_records_cannot_be_transplanted_to_another_original() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let f = Fixture::new(format).await?;
        let (evidence, _) = head_expiring(&f, 0, false, true).await?;
        let (other, _) = head_expiring(&f, 4, false, true).await?;
        expired(&evidence).await?;
        expired(&other).await?;
        let old = registered(&f, 0).await?;
        for kind in [0, 4] {
            registered(&f, kind)
                .await?
                .ready_stop(f.client(), identity()?, &f.authority())
                .await?
                .command_for_test()
                .execute()
                .await?;
        }
        edit(&f, "DROP TRIGGER catalog_custody_stop_immutable").await?;
        edit(&f, "UPDATE catalog_custody_commands SET stopped=(SELECT stopped FROM catalog_custody_commands WHERE operation=x'eaeaeaeaeaeaeaeaeaeaeaeaeaeaeaea') WHERE operation=x'e6e6e6e6e6e6e6e6e6e6e6e6e6e6e6e6'").await?;
        assert!(
            RegisteredCustody::load_latest(&f.client(), &f.target, [230; 16])
                .await
                .is_err()
        );
        assert!(
            matches!(old.recover(&f.client()).await, Err(InvocationError::Pending(value)) if *value==evidence)
        );
        assert!(
            ReadyStaging::restore(f.client(), f.target.clone(), [230; 16])
                .await
                .is_err()
        );
        assert!(registered(&f, 4).await?.stop_fact().is_some());
        assert!(matches!(
            f.client().resolve(&evidence).await?,
            Resolution::Expired
        ));
        f.runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn custody_stop_cannot_be_blocked_by_its_own_uncertain_preparation_and_fences_the_shared_session()
-> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let f = Fixture::new(format).await?;
        let operation = [230; 16];
        let begin = PreparedCustody::prepare(
            &f.client(),
            &f.target,
            CustodyAction::BeginPreparation(f.begin(operation)),
            identity()?,
        )
        .await?
        .register(&f.client(), identity()?)
        .await?
        .recover_preparation(&f.client())
        .await?;
        let lease = lease(begin.output)?;
        let session = Arc::new(
            PreparationSession::open(
                f.client(),
                f.target.clone(),
                check(lease.token),
                Some(begin.receipt),
                f.authority(),
            )
            .await?,
        );
        let mut mutation = identity()?;
        mutation.expires_at_ms = mutation.issued_at_ms + 1_000;
        let ready = session.ready_renew(mutation, DEFAULT_LEASE_MS).await?;
        let queue = PublicationCoordinator::new(
            f.target.clone(),
            PublicationLimits::default(),
            f.publication_budget.clone(),
        )?;
        queue.fault_for_test(1);
        let renewal = queue.submit(ready).await?;
        assert!(matches!(
            observed(&renewal).await?,
            PublicationState::Uncertain(_)
        ));
        let original = registered(&f, 0).await?;
        expired(original.evidence()).await?;
        assert!(session.live_lease().is_ok());
        let service = CustodySupervisor::start(
            f.client(),
            f.target.clone(),
            queue.clone(),
            f.scans(RecoveryScanLimits {
                page: 1,
                interval: Duration::from_millis(10),
            }),
            f.authority(),
        )?;
        timeout(Duration::from_secs(10), async {
            while queue.stats().await.command_bytes != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        let stats = service.shutdown().await?;
        assert_eq!(stats.submitted, 1);
        let PublicationState::Finished(Err(error)) = renewal.state() else {
            return Err("renewal not closed by its own stop".into());
        };
        assert!(
            matches!(&*error, PublicationError::Custody { evidence, source } if **evidence==*original.evidence() && matches!(&**source,CustodyError::Stopped(_)))
        );
        assert!(session.live_lease().is_err());
        assert!(registered(&f, 0).await?.closed());
        assert!(!registered(&f, 0).await?.settled());
        assert!(matches!(
            f.client().resolve(original.evidence()).await?,
            Resolution::Expired
        ));
        assert!(queue.close_and_drain().await.is_empty());
        f.runtime.shutdown().await?;
    }
    Ok(())
}
