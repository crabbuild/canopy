//! Automatic closure must preserve original knowledge and resource ownership.
use super::super::publishing::edit;
use super::*;
use cellule_runtime::{PendingMutation, Resolution};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

async fn expired(evidence: &PendingMutation) -> Result {
    let now = crate::packs::publication::sql::now(0)?;
    if now <= evidence.identity().expires_at_ms {
        tokio::time::sleep(Duration::from_millis(
            (evidence.identity().expires_at_ms - now + 1) as u64,
        ))
        .await;
    }
    Ok(())
}
async fn until(c: &StagingCoordinator, predicate: impl Fn(StagingStats) -> bool) -> Result {
    timeout(Duration::from_secs(10), async {
        while !predicate(c.stats()) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    Ok(())
}
async fn head(f: &Fixture, operation: [u8; 16]) -> Result<RegisteredCustody> {
    Ok(
        RegisteredCustody::load_latest(&f.client(), &f.target, operation)
            .await?
            .ok_or("custody head missing")?,
    )
}
async fn stop(f: &Fixture, original: &RegisteredCustody) -> Result {
    let ready = original
        .ready_stop(f.client(), identity()?, &f.authority())
        .await?;
    assert_eq!(
        ready.command_for_test().execute().await?.output,
        CustodyStopReply::Stopped
    );
    Ok(())
}

#[tokio::test]
async fn automatic_retirement_closes_all_seven_cold_originals_after_observer_drop_and_service_close()
-> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        for kind in 0..7 {
            let f = Fixture::new(format).await?;
            let (evidence, _) = restore::head_expiring(&f, kind, false, true).await?;
            expired(&evidence).await?;
            let c =
                StagingCoordinator::new(f.target.clone(), StagingLimits::default(), f.authority())?;
            let ticket = c
                .submit(
                    ReadyStaging::restore(f.client(), f.target.clone(), [230 + kind; 16]).await?,
                )
                .map_err(|(error, _)| error)?;
            assert!(matches!(
                terminal(&ticket).await?,
                StagingState::Uncertain(_)
            ));
            assert_eq!(c.close_and_drain().await.len(), 1);
            drop(ticket);
            let original = head(&f, [230 + kind; 16]).await?;
            let counts = f.counts().await?;
            stop(&f, &original).await?;
            until(&c, |s| s.admitted == 0 && !s.retirement_running).await?;
            assert!(c.pending([230 + kind; 16]).is_none());
            assert_eq!(c.stats().command_bytes, 0);
            assert_eq!(c.stats().retirement_recoveries, 1);
            assert_eq!(f.counts().await?, counts);
            let saved = head(&f, [230 + kind; 16]).await?;
            assert_eq!(saved.evidence(), &evidence);
            assert!(saved.closed());
            assert!(!saved.settled());
            assert!(matches!(saved.recover(&f.client()).await,
                Err(InvocationError::Pending(value)) if *value == evidence));
            assert!(matches!(
                f.client().resolve(&evidence).await?,
                Resolution::Expired
            ));
            assert!(c.close_and_drain().await.is_empty());
            f.runtime.shutdown().await?;
        }
    }
    Ok(())
}

struct Owned {
    coordinator: StagingCoordinator,
    dropped: Arc<AtomicUsize>,
    wrong: Arc<AtomicBool>,
}
impl Drop for Owned {
    fn drop(&mut self) {
        let stats = self.coordinator.stats();
        if stats.workers == 0 || stats.admitted == 0 || stats.command_bytes == 0 {
            self.wrong.store(true, Ordering::Release);
        }
        self.dropped.fetch_add(1, Ordering::AcqRel);
    }
}

#[tokio::test]
async fn automatic_retirement_joins_live_callbacks_and_drops_retained_results_before_releasing_credits()
-> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        for bound in [false, true] {
            let f = Fixture::new(format).await?;
            let c =
                StagingCoordinator::new(f.target.clone(), StagingLimits::default(), f.authority())?;
            let operation = [220; 16];
            let ticket = if bound {
                bound::bind(&f, &c, operation, "owner").await?
            } else {
                let t = submit(&f, &c, operation, "owner").await?;
                active(&t).await?;
                t
            };
            let session = if bound {
                Some(ticket.bound_session()?)
            } else {
                None
            };
            let dropped = Arc::new(AtomicUsize::new(0));
            let wrong = Arc::new(AtomicBool::new(false));
            let owned = || Owned {
                coordinator: c.clone(),
                dropped: dropped.clone(),
                wrong: wrong.clone(),
            };
            let finished = owned();
            let completed = if bound {
                ticket.spawn_bound(move |_, _context| async move { Ok(finished) })?
            } else {
                ticket.spawn(move |_| async move { Ok(finished) })?
            };
            let running = owned();
            let (entered, start) = oneshot::channel();
            let worker = if bound {
                ticket.spawn_bound(move |_, _context| async move {
                    let _ = entered.send(());
                    std::future::pending::<()>().await;
                    drop(running);
                    Ok(())
                })?
            } else {
                ticket.spawn(move |_| async move {
                    let _ = entered.send(());
                    std::future::pending::<()>().await;
                    drop(running);
                    Ok(())
                })?
            };
            timeout(Duration::from_secs(10), start).await??;
            let mut mutation = identity()?;
            mutation.expires_at_ms = mutation.issued_at_ms + 1_000;
            c.fault_for_test(1); // Registered original, execution never started.
            ticket.renew_with_identity_for_test(mutation).await?;
            until(&c, |s| s.uncertain == 1).await?;
            assert!(matches!(ticket.state(), StagingState::Uncertain(_)));
            let original = head(&f, operation).await?;
            let evidence = original.evidence().clone();
            assert!(matches!(
                f.client().resolve(&evidence).await?,
                Resolution::Absent
            ));
            expired(&evidence).await?;
            assert_eq!(c.stats().workers, 2); // Includes the untransferred completed result.
            assert_eq!(dropped.load(Ordering::Acquire), 0);
            if let Some(session) = &session {
                assert!(session.live_lease().is_ok());
            }
            drop(completed);
            drop(worker);
            drop(ticket);
            stop(&f, &original).await?;
            until(&c, |s| s.admitted == 0 && !s.retirement_running).await?;
            assert_eq!(dropped.load(Ordering::Acquire), 2);
            assert!(!wrong.load(Ordering::Acquire));
            assert_eq!(c.stats().workers, 0);
            assert_eq!(c.stats().command_bytes, 0);
            assert_eq!(c.stats().retirement_recoveries, 1);
            if let Some(session) = session {
                assert!(session.live_lease().is_err());
            }
            assert!(matches!(
                f.client().resolve(&evidence).await?,
                Resolution::Expired
            ));
            assert!(!head(&f, operation).await?.settled());
            assert!(c.close_and_drain().await.is_empty());
            f.runtime.shutdown().await?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn automatic_retirement_does_not_execute_absent_commands_or_retry_known_phases_without_a_stop()
-> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let f = Fixture::new(format).await?;
        let (evidence, _) = restore::head_expiring(&f, 0, false, false).await?;
        let ready = ReadyStaging::restore(f.client(), f.target.clone(), [230; 16]).await?;
        let c = StagingCoordinator::new(f.target.clone(), StagingLimits::default(), f.authority())?;
        edit(
            &f,
            "ALTER TABLE catalog_custody_commands RENAME TO private_query_unavailable",
        )
        .await?;
        let ticket = c.submit(ready).map_err(|(error, _)| error)?;
        assert!(matches!(
            terminal(&ticket).await?,
            StagingState::Uncertain(_)
        ));
        until(&c, |s| s.retirement_failures >= 2).await?;
        assert_eq!(f.counts().await?, (0, 0));
        assert_eq!(c.stats().admitted, 1);
        assert_eq!(c.stats().retirement_recoveries, 0);
        assert!(matches!(
            f.client().resolve(&evidence).await?,
            Resolution::Absent
        ));
        edit(
            &f,
            "ALTER TABLE private_query_unavailable RENAME TO catalog_custody_commands",
        )
        .await?;
        let known = head(&f, [230; 16]).await?.recover(&f.client()).await?;
        let probes = c.stats().retirement_probes;
        until(&c, |s| s.retirement_probes >= probes + 2).await?;
        assert!(matches!(ticket.state(), StagingState::Uncertain(_)));
        assert_eq!(c.stats().retirement_recoveries, 0);
        assert!(ticket.restored_outcome().is_none());
        assert_eq!(c.close_and_drain().await.len(), 1);
        c.recover(&ticket)?;
        until(&c, |s| s.admitted == 0 && !s.retirement_running).await?;
        assert_eq!(
            *ticket.restored_outcome().ok_or("known outcome lost")?,
            known
        );
        assert!(c.close_and_drain().await.is_empty());
        f.runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn exact_retirement_probe_keeps_old_ordinal_after_successor_and_rejects_corrupt_facts()
-> Result {
    use crate::packs::publication::custody::OwnedCustody;
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let f = Fixture::new(format).await?;
        let (evidence, _) = restore::head_expiring(&f, 0, false, true).await?;
        let probe = OwnedCustody::restore(&f.client(), &f.target, [230; 16])
            .await?
            .stop_probe()?;
        assert!(!probe.observed(&f.client()).await?);
        expired(&evidence).await?;
        let original = head(&f, [230; 16]).await?;
        stop(&f, &original).await?;
        let successor =
            PreparedCustody::prepare(&f.client(), &f.target, original.action()?, identity()?)
                .await?
                .register(&f.client(), identity()?)
                .await?;
        assert_ne!(head(&f, [230; 16]).await?.evidence(), &evidence);
        assert_eq!(head(&f, [230; 16]).await?.evidence(), successor.evidence());
        assert!(probe.observed(&f.client()).await?);
        let fact = f
            .handle
            .query(0, 4096, |db| {
                Ok(db.query_row(
                    "SELECT stopped FROM catalog_custody_commands WHERE operation=?1 AND step=0",
                    [vec![230u8; 16]],
                    |row| row.get::<_, Vec<u8>>(0),
                )?)
            })
            .await?;
        edit(&f, "DROP TRIGGER catalog_custody_stop_immutable").await?;
        edit(
            &f,
            "UPDATE catalog_custody_commands SET stopped=x'01' WHERE step=0",
        )
        .await?;
        assert!(probe.observed(&f.client()).await.is_err());
        edit(
            &f,
            &format!(
                "UPDATE catalog_custody_commands SET stopped=x'{}' WHERE step=0",
                hex::encode(fact)
            ),
        )
        .await?;
        assert!(probe.observed(&f.client()).await?);
        assert!(matches!(
            f.client().resolve(&evidence).await?,
            Resolution::Expired
        ));
        f.runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn automatic_retirement_skips_bad_heads_and_revisits_them_after_repair() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let f = Fixture::new(format).await?;
        let (bad, _) = restore::head_expiring(&f, 0, false, true).await?;
        let (good, _) = restore::head_expiring(&f, 4, false, true).await?;
        expired(&bad).await?;
        expired(&good).await?;
        let bad_ready = ReadyStaging::restore(f.client(), f.target.clone(), [230; 16]).await?;
        let good_ready = ReadyStaging::restore(f.client(), f.target.clone(), [234; 16]).await?;
        let bad_original = head(&f, [230; 16]).await?;
        let good_original = head(&f, [234; 16]).await?;
        let intent = f
            .handle
            .query(0, 4096, |db| {
                Ok(db.query_row(
                    "SELECT intent FROM catalog_custody_commands WHERE operation=?1 AND step=0",
                    [vec![230u8; 16]],
                    |row| row.get::<_, Vec<u8>>(0),
                )?)
            })
            .await?;
        edit(&f, "DROP TRIGGER catalog_custody_identity_immutable").await?;
        edit(&f, "UPDATE catalog_custody_commands SET intent=x'01' WHERE operation=x'e6e6e6e6e6e6e6e6e6e6e6e6e6e6e6e6'").await?;
        let c = StagingCoordinator::new(f.target.clone(), StagingLimits::default(), f.authority())?;
        let bad_ticket = c.submit(bad_ready).map_err(|(error, _)| error)?;
        let good_ticket = c.submit(good_ready).map_err(|(error, _)| error)?;
        assert!(matches!(
            terminal(&bad_ticket).await?,
            StagingState::Uncertain(_)
        ));
        assert!(matches!(
            terminal(&good_ticket).await?,
            StagingState::Uncertain(_)
        ));
        until(&c, |s| s.retirement_failures > 0).await?;
        stop(&f, &good_original).await?;
        until(&c, |s| s.admitted == 1 && s.retirement_recoveries == 1).await?;
        assert!(matches!(good_ticket.state(), StagingState::Fenced(_)));
        assert!(matches!(bad_ticket.state(), StagingState::Uncertain(_)));
        assert_eq!(
            c.stats().command_bytes,
            crate::packs::publication::custody::RESERVATION
        );
        edit(&f, &format!("UPDATE catalog_custody_commands SET intent=x'{}' WHERE operation=x'e6e6e6e6e6e6e6e6e6e6e6e6e6e6e6e6'", hex::encode(intent))).await?;
        stop(&f, &bad_original).await?;
        until(&c, |s| s.admitted == 0 && !s.retirement_running).await?;
        assert_eq!(c.stats().retirement_recoveries, 2);
        assert!(bad_ticket.restored_outcome().is_none());
        assert!(good_ticket.restored_outcome().is_none());
        assert!(matches!(
            f.client().resolve(&bad).await?,
            Resolution::Expired
        ));
        assert!(matches!(
            f.client().resolve(&good).await?,
            Resolution::Expired
        ));
        assert!(c.close_and_drain().await.is_empty());
        f.runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn automatic_retirement_does_not_apply_old_custody_closure_to_an_input_checkpoint() -> Result
{
    use canopy_object_storage::artifact::ArtifactStore;
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let f = Fixture::new(format).await?;
        let (old, _) = restore::head_expiring(&f, 0, false, true).await?;
        expired(&old).await?;
        stop(&f, &head(&f, [230; 16]).await?).await?;
        let c = StagingCoordinator::new(f.target.clone(), StagingLimits::default(), f.authority())?;
        PreparedCustody::prepare(
            &f.client(),
            &f.target,
            CustodyAction::BeginStaging(f.begin([230; 16])),
            identity()?,
        )
        .await?
        .register(&f.client(), identity()?)
        .await?;
        let ticket = c
            .submit(ReadyStaging::restore(f.client(), f.target.clone(), [230; 16]).await?)
            .map_err(|(e, _)| e)?;
        active(&ticket).await?;
        let store = Arc::new(ArtifactStore::new(Arc::new(InMemory::new()), f.repository));
        let proof = super::super::inputs::seal(&f, &ticket, store, 2).await?;
        c.fault_for_test(1);
        let checkpoint = ticket
            .register_inputs(proof, identity()?)
            .map_err(|(e, _)| e)?;
        assert!(matches!(
            terminal(&ticket).await?,
            StagingState::Uncertain(_)
        ));
        let (other, _) = restore::head_expiring(&f, 4, false, true).await?;
        expired(&other).await?;
        let other_ticket = c
            .submit(ReadyStaging::restore(f.client(), f.target.clone(), [234; 16]).await?)
            .map_err(|(e, _)| e)?;
        assert!(matches!(
            terminal(&other_ticket).await?,
            StagingState::Uncertain(_)
        ));
        until(&c, |s| s.retirement_probes >= 2).await?;
        stop(&f, &head(&f, [234; 16]).await?).await?;
        until(&c, |s| s.admitted == 1 && !s.retirement_running).await?;
        assert!(matches!(ticket.state(), StagingState::Uncertain(_)));
        assert_eq!(c.stats().retirement_recoveries, 1);
        assert_eq!(
            c.stats().command_bytes,
            crate::packs::publication::custody::RESERVATION + 4096
        );
        assert!(checkpoint.wait().await.is_err());
        c.recover(&ticket)?;
        checkpoint.wait().await.map_err(|e| e.to_string())?;
        assert!(c.close_and_drain().await.is_empty());
        assert!(matches!(
            f.client().resolve(&old).await?,
            Resolution::Expired
        ));
        f.runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn automatic_retirement_spans_more_than_one_page_and_restarts_at_earlier_keys_after_idle()
-> Result {
    let f = Fixture::new(ObjectFormat::Sha256).await?;
    let c = StagingCoordinator::new(
        f.target.clone(),
        StagingLimits {
            operations: 256,
            per_actor: 192,
            ..StagingLimits::default()
        },
        f.authority(),
    )?;
    let mut originals = Vec::new();
    for n in 1u16..=130 {
        let mut operation = [0; 16];
        operation[14..].copy_from_slice(&n.to_be_bytes());
        let mut mutation = identity()?;
        mutation.expires_at_ms = mutation.issued_at_ms + 1_000;
        let saved = PreparedCustody::prepare(
            &f.client(),
            &f.target,
            CustodyAction::BeginStaging(f.begin(operation)),
            mutation,
        )
        .await?
        .register(&f.client(), identity()?)
        .await?;
        originals.push((operation, saved));
    }
    expired(originals.last().ok_or("no originals")?.1.evidence()).await?;
    for (operation, _) in &originals {
        drop(
            c.submit(ReadyStaging::restore(f.client(), f.target.clone(), *operation).await?)
                .map_err(|(error, _)| error)?,
        );
    }
    until(&c, |s| s.uncertain == 130).await?;
    for (_, saved) in &originals {
        stop(&f, saved).await?;
    }
    until(&c, |s| s.admitted == 0 && !s.retirement_running).await?;
    assert_eq!(c.stats().retirement_recoveries, 130);
    assert_eq!(c.stats().command_bytes, 0);
    assert_eq!(f.counts().await?, (0, 0));
    // A new unknown original below the previous cursor starts a fresh singleton.
    let mut mutation = identity()?;
    mutation.expires_at_ms = mutation.issued_at_ms + 1_000;
    let operation = originals[0].0;
    let saved = PreparedCustody::prepare(
        &f.client(),
        &f.target,
        CustodyAction::BeginStaging(f.begin(operation)),
        mutation,
    )
    .await?
    .register(&f.client(), identity()?)
    .await?;
    expired(saved.evidence()).await?;
    let ticket = c
        .submit(ReadyStaging::restore(f.client(), f.target.clone(), operation).await?)
        .map_err(|(error, _)| error)?;
    assert!(matches!(
        terminal(&ticket).await?,
        StagingState::Uncertain(_)
    ));
    drop(ticket);
    stop(&f, &saved).await?;
    until(&c, |s| s.admitted == 0 && !s.retirement_running).await?;
    assert_eq!(c.stats().retirement_recoveries, 131);
    assert!(c.close_and_drain().await.is_empty());
    f.runtime.shutdown().await?;
    Ok(())
}
