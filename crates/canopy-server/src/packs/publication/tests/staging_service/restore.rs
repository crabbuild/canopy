//! Real durable command/owner recovery; no old coordinator or usable session.
use super::*;
use cellule_runtime::{Committed, PendingMutation, Resolution};

async fn execute(f: &Fixture, action: CustodyAction) -> Result<Committed<CustodyReply>> {
    let ready = PreparedCustody::prepare(&f.client(), &f.target, action, identity()?).await?;
    Ok(ready
        .register(&f.client(), identity()?)
        .await?
        .recover(&f.client())
        .await?)
}
fn granted_token(value: &CustodyReply) -> Result<PreparationToken> {
    match value {
        CustodyReply::Preparation(PreparationReply::Granted(lease)) => Ok(lease.token),
        CustodyReply::Staging(StagingReply::Granted(lease)) => Ok(lease.token),
        _ => Err("custody grant missing".into()),
    }
}
async fn head(
    f: &Fixture,
    kind: u8,
    execute_original: bool,
) -> Result<(PendingMutation, Option<Committed<CustodyReply>>)> {
    head_expiring(f, kind, execute_original, execute_original).await
}
pub(in crate::packs::publication::tests) async fn head_expiring(
    f: &Fixture,
    kind: u8,
    execute_original: bool,
    short_expiry: bool,
) -> Result<(PendingMutation, Option<Committed<CustodyReply>>)> {
    let input = f.begin([230 + kind; 16]);
    let action = match kind {
        0 => CustodyAction::BeginStaging(input),
        4 => CustodyAction::BeginPreparation(input),
        _ => {
            let source = if kind < 4 {
                CustodyAction::BeginStaging(input)
            } else {
                CustodyAction::BeginPreparation(input)
            };
            let previous = granted_token(&execute(f, source).await?.output)?;
            let lease = LeaseRequest {
                check: check(previous),
                lease_ms: DEFAULT_LEASE_MS,
            };
            match kind {
                1 => CustodyAction::ClaimStaging(lease),
                2 => CustodyAction::RenewStaging(lease),
                3 => CustodyAction::BindStaging(lease.check),
                5 => CustodyAction::ClaimPreparation(lease),
                6 => CustodyAction::RenewPreparation(lease),
                _ => return Err("unknown fixture kind".into()),
            }
        }
    };
    let mut mutation = identity()?;
    if short_expiry {
        mutation.expires_at_ms = mutation.issued_at_ms + 1_000;
    }
    let command = PreparedCustody::prepare(&f.client(), &f.target, action, mutation).await?;
    let original = command.evidence().clone();
    let registered = command.register(&f.client(), identity()?).await?;
    let committed = if execute_original {
        Some(registered.recover(&f.client()).await?)
    } else {
        None
    };
    Ok((original, committed))
}
async fn restore(
    f: &Fixture,
    client: CellClient,
    kind: u8,
    limits: StagingLimits,
) -> Result<(StagingCoordinator, StagingTicket)> {
    let service = StagingCoordinator::new(f.target.clone(), limits, f.authority())?;
    let ready = ReadyStaging::restore(client, f.target.clone(), [230 + kind; 16]).await?;
    let ticket = service.submit(ready).map_err(|(error, _)| error)?;
    Ok((service, ticket))
}
async fn settle(ticket: &StagingTicket) -> Result<StagingState> {
    Ok(timeout(Duration::from_secs(10), ticket.wait()).await?)
}
async fn expired(evidence: &PendingMutation) -> Result {
    let until = evidence.identity().expires_at_ms;
    let now = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis())?;
    if now <= until {
        tokio::time::sleep(Duration::from_millis((until - now + 1) as u64)).await;
    }
    Ok(())
}

#[tokio::test]
async fn cold_staging_reconstructs_all_seven_heads_without_replacing_originals_or_clocks() -> Result
{
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        for kind in 0..7 {
            let f = Fixture::new(format).await?;
            let (evidence, expected) = head(&f, kind, true).await?;
            let expected = expected.ok_or("original not executed")?;
            // A changed restart profile must not hide original knowledge or
            // rewrite its command duration. New renewals use the new profile.
            let limits = StagingLimits {
                lease_ms: 10_000,
                renew_before_ms: 5_000,
                ..StagingLimits::default()
            };
            let (service, ticket) = restore(&f, f.client(), kind, limits).await?;
            let state = settle(&ticket).await?;
            if kind < 3 {
                assert!(
                    matches!(state, StagingState::Active(_)),
                    "kind {kind}: {state:?}"
                );
            } else {
                assert!(
                    matches!(state, StagingState::Bound(_)),
                    "kind {kind}: {state:?}"
                );
                assert_eq!(
                    ticket.bound_session()?.lease.token,
                    granted_token(&expected.output)?
                );
            }
            assert_eq!(ticket.restored_evidence(), Some(&evidence));
            assert_eq!(
                *ticket.restored_outcome().ok_or("original reply lost")?,
                expected
            );
            let saved = RegisteredCustody::load_latest(&f.client(), &f.target, [230 + kind; 16])
                .await?
                .ok_or("head missing")?;
            assert_eq!(saved.evidence(), &evidence);
            assert!(service.close_and_drain().await.is_empty());
            assert!(ticket.spawn(|_| async { Ok(()) }).is_err());
            assert_eq!(
                *ticket.restored_outcome().ok_or("closed history lost")?,
                expected
            );
            assert_eq!(service.stats().admitted, 0);
            f.runtime.shutdown().await?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn cold_staging_keeps_all_original_receipts_after_sdk_expiry_and_actual_owner_restore()
-> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        for kind in 0..7 {
            let f = Fixture::new(format).await?;
            let (evidence, expected) = head(&f, kind, true).await?;
            let expected = expected.ok_or("original not executed")?;
            let old = granted_token(&expected.output)?;
            let (runtime, handle, client) =
                super::super::durable_recovery::restore_owner(&f, &check(old)).await?;
            assert_ne!(handle.owner_fence(), old.owner);
            expired(&evidence).await?;
            assert!(matches!(
                client.resolve(&evidence).await?,
                Resolution::Expired
            ));
            let (service, ticket) =
                restore(&f, client.clone(), kind, StagingLimits::default()).await?;
            assert!(matches!(settle(&ticket).await?, StagingState::Fenced(_)));
            assert_eq!(ticket.restored_evidence(), Some(&evidence));
            assert_eq!(
                *ticket.restored_outcome().ok_or("old-owner history lost")?,
                expected
            );
            assert!(ticket.bound_session().is_err());
            assert!(ticket.spawn(|_| async { Ok(()) }).is_err());
            assert!(service.close_and_drain().await.is_empty());
            let saved = RegisteredCustody::load_latest(&client, &f.target, [230 + kind; 16])
                .await?
                .ok_or("old head missing")?;
            assert_eq!(saved.evidence(), &evidence);
            assert_eq!(saved.recover(&client).await?, expected);
            runtime.shutdown().await?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn cold_staging_absent_originals_execute_or_fence_under_actual_new_owner_without_new_identity()
-> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        for kind in 0..7 {
            let f = Fixture::new(format).await?;
            let (evidence, expected) = head(&f, kind, false).await?;
            assert!(expected.is_none());
            assert!(matches!(
                f.client().resolve(&evidence).await?,
                Resolution::Absent
            ));
            let (runtime, handle, client) =
                super::super::durable_recovery::restore_owner_fence(&f, f.handle.owner_fence())
                    .await?;
            let (service, ticket) =
                restore(&f, client.clone(), kind, StagingLimits::default()).await?;
            let state = settle(&ticket).await?;
            assert_eq!(ticket.restored_evidence(), Some(&evidence));
            let saved = RegisteredCustody::load_latest(&client, &f.target, [230 + kind; 16])
                .await?
                .ok_or("registered original lost")?;
            assert_eq!(saved.evidence(), &evidence);
            if matches!(kind, 2 | 3 | 6) {
                assert!(
                    matches!(state, StagingState::Fenced(_)),
                    "old-token kind {kind}: {state:?}"
                );
                let outcome = ticket.restored_outcome().ok_or("stale denial lost")?;
                assert!(
                    matches!(
                        &outcome.output,
                        CustodyReply::Preparation(PreparationReply::Denied(
                            PreparationDenial::Stale
                        )) | CustodyReply::Staging(StagingReply::Denied(PreparationDenial::Stale))
                    ),
                    "kind {kind}: {outcome:?}"
                );
                assert!(saved.settled());
                assert!(
                    matches!(saved.recover(&client).await, Err(InvocationError::Rejected(value)) if *value == *outcome)
                );
                assert!(matches!(
                    client.resolve(&evidence).await?,
                    Resolution::Committed(_)
                ));
            } else {
                let outcome = ticket
                    .restored_outcome()
                    .ok_or("absent original reply lost")?;
                assert_eq!(granted_token(&outcome.output)?.owner, handle.owner_fence());
                assert_eq!(saved.recover(&client).await?, *outcome);
                assert!(
                    matches!(state, StagingState::Active(_) | StagingState::Bound(_)),
                    "kind {kind}: {state:?}"
                );
            }
            assert!(service.close_and_drain().await.is_empty());
            runtime.shutdown().await?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn cold_staging_denials_remain_original_after_authority_is_repaired() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        for kind in [0, 4] {
            let f = Fixture::new(format).await?;
            let action = if kind == 0 {
                CustodyAction::BeginStaging(f.begin([230 + kind; 16]))
            } else {
                CustodyAction::BeginPreparation(f.begin([230 + kind; 16]))
            };
            let prepared =
                PreparedCustody::prepare(&f.client(), &f.target, action, identity()?).await?;
            let evidence = prepared.evidence().clone();
            let registered = prepared.register(&f.client(), identity()?).await?;
            super::super::publishing::edit(&f, "UPDATE repository_identity SET owner='other'")
                .await?;
            let expected = match registered.recover(&f.client()).await {
                Err(InvocationError::Rejected(value)) => *value,
                value => return Err(format!("expected original denial: {value:?}").into()),
            };
            super::super::publishing::edit(&f, "UPDATE repository_identity SET owner='owner'")
                .await?;
            let (service, ticket) = restore(&f, f.client(), kind, StagingLimits::default()).await?;
            assert!(matches!(settle(&ticket).await?, StagingState::Fenced(_)));
            assert_eq!(ticket.restored_evidence(), Some(&evidence));
            assert_eq!(
                *ticket.restored_outcome().ok_or("original denial missing")?,
                expected
            );
            assert!(ticket.spawn(|_| async { Ok(()) }).is_err());
            assert!(ticket.bound_session().is_err());
            assert_eq!(f.counts().await?, (0, 0));
            assert!(service.close_and_drain().await.is_empty());
            f.runtime.shutdown().await?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn cold_staging_reply_loss_and_query_failures_retain_originals_through_closed_recovery()
-> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        for kind in [0, 3, 6] {
            for fault in 0..4 {
                let f = Fixture::new(format).await?;
                let (evidence, _) = head(&f, kind, false).await?;
                let ready =
                    ReadyStaging::restore(f.client(), f.target.clone(), [230 + kind; 16]).await?;
                let service = StagingCoordinator::new(
                    f.target.clone(),
                    StagingLimits::default(),
                    f.authority(),
                )?;
                if fault == 0 {
                    // The ready capability has the authentic original bytes,
                    // but a failed phase read still cannot authorize execution.
                    super::super::publishing::edit(
                        &f,
                        "ALTER TABLE catalog_custody_commands RENAME TO custody_query_fault",
                    )
                    .await?;
                } else {
                    service.fault_for_test(fault);
                }
                let ticket = service.submit(ready).map_err(|(error, _)| error)?;
                assert!(matches!(
                    terminal(&ticket).await?,
                    StagingState::Uncertain(_)
                ));
                assert_eq!(ticket.restored_evidence(), Some(&evidence));
                assert!(ticket.restored_outcome().is_none());
                assert_eq!(
                    service.stats().command_bytes,
                    super::super::super::custody::RESERVATION
                );
                drop(ticket);
                let ticket = service
                    .pending([230 + kind; 16])
                    .ok_or("dropped observer lost original")?;
                assert_eq!(service.close_and_drain().await.len(), 1);
                if fault == 0 {
                    assert!(matches!(
                        f.client().resolve(&evidence).await?,
                        Resolution::Absent
                    ));
                    super::super::publishing::edit(
                        &f,
                        "ALTER TABLE custody_query_fault RENAME TO catalog_custody_commands",
                    )
                    .await?;
                }
                service.recover(&ticket)?;
                assert!(matches!(
                    terminal(&ticket).await?,
                    StagingState::Stopped | StagingState::Bound(_) | StagingState::Fenced(_)
                ));
                let original = ticket
                    .restored_outcome()
                    .ok_or("closed recovery lost original reply")?;
                let saved =
                    RegisteredCustody::load_latest(&f.client(), &f.target, [230 + kind; 16])
                        .await?
                        .ok_or("original disappeared")?;
                assert_eq!(saved.evidence(), &evidence);
                assert_eq!(saved.recover(&f.client()).await?, *original);
                assert!(ticket.bound_session().is_err());
                assert!(ticket.spawn(|_| async { Ok(()) }).is_err());
                assert!(service.close_and_drain().await.is_empty());
                assert_eq!(service.stats().command_bytes, 0);
                f.runtime.shutdown().await?;
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn cold_bound_owner_loss_cancels_workers_before_releasing_resource_credit() -> Result {
    use std::sync::atomic::{AtomicBool, Ordering};
    struct Resource {
        service: StagingCoordinator,
        dropped: Arc<AtomicBool>,
        wrong_order: Arc<AtomicBool>,
    }
    impl Drop for Resource {
        fn drop(&mut self) {
            if self.service.stats().workers != 1 {
                self.wrong_order.store(true, Ordering::Release);
            }
            self.dropped.store(true, Ordering::Release);
        }
    }
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        for kind in [3, 4, 6] {
            let f = Fixture::new(format).await?;
            let (evidence, expected) = head(&f, kind, true).await?;
            let expected = expected.ok_or("original missing")?;
            let (service, ticket) = restore(&f, f.client(), kind, StagingLimits::default()).await?;
            assert!(matches!(settle(&ticket).await?, StagingState::Bound(_)));
            let session = ticket.bound_session()?;
            let dropped = Arc::new(AtomicBool::new(false));
            let wrong_order = Arc::new(AtomicBool::new(false));
            let resource = Resource {
                service: service.clone(),
                dropped: dropped.clone(),
                wrong_order: wrong_order.clone(),
            };
            let (entered, running) = oneshot::channel();
            let worker = ticket.spawn_bound(move |_, _context| async move {
                let _resource = resource;
                let _ = entered.send(());
                std::future::pending::<std::result::Result<(), StagingError>>().await
            })?;
            timeout(Duration::from_secs(10), running).await??;
            let (runtime, handle, _) =
                super::super::durable_recovery::restore_owner(&f, &session.check).await?;
            assert_ne!(handle.owner_fence(), session.lease.token.owner);
            assert!(session.check_owner().await.is_err());
            // A clone observing the fence after it fired must also wake.
            timeout(Duration::from_secs(2), session.clone().wait_fenced()).await?;
            // No manual renewal, clock advance, coordinator stop or job-state
            // mutation: the session's permanent shared fence must wake work.
            assert!(
                timeout(Duration::from_secs(2), worker.wait())
                    .await?
                    .is_err()
            );
            assert!(session.live_lease().is_err());
            assert!(dropped.load(Ordering::Acquire));
            assert!(!wrong_order.load(Ordering::Acquire));
            assert_eq!(service.stats().workers, 0);
            assert!(ticket.spawn_bound(|_, _context| async { Ok(()) }).is_err());
            assert_eq!(ticket.restored_evidence(), Some(&evidence));
            assert_eq!(
                *ticket.restored_outcome().ok_or("historical outcome lost")?,
                expected
            );
            assert!(service.close_and_drain().await.is_empty());
            runtime.shutdown().await?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn cold_staging_unsettled_expired_originals_keep_exact_evidence_and_never_execute() -> Result
{
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        for kind in 0..7 {
            let f = Fixture::new(format).await?;
            let (evidence, expected) = head_expiring(&f, kind, false, true).await?;
            assert!(expected.is_none());
            let (runtime, _, client) =
                super::super::durable_recovery::restore_owner_fence(&f, f.handle.owner_fence())
                    .await?;
            expired(&evidence).await?;
            assert!(matches!(
                client.resolve(&evidence).await?,
                Resolution::Expired
            ));
            let (service, ticket) =
                restore(&f, client.clone(), kind, StagingLimits::default()).await?;
            assert!(matches!(
                terminal(&ticket).await?,
                StagingState::Uncertain(_)
            ));
            assert_eq!(ticket.restored_evidence(), Some(&evidence));
            assert!(ticket.restored_outcome().is_none());
            assert!(ticket.spawn(|_| async { Ok(()) }).is_err());
            assert!(ticket.spawn_bound(|_, _context| async { Ok(()) }).is_err());
            assert_eq!(
                service.stats().command_bytes,
                super::super::super::custody::RESERVATION
            );
            assert_eq!(service.close_and_drain().await.len(), 1);
            service.recover(&ticket)?;
            assert!(matches!(
                terminal(&ticket).await?,
                StagingState::Uncertain(_)
            ));
            let saved = RegisteredCustody::load_latest(&client, &f.target, [230 + kind; 16])
                .await?
                .ok_or("expired head lost")?;
            assert_eq!(saved.evidence(), &evidence);
            assert!(!saved.settled());
            assert!(
                matches!(saved.recover(&client).await, Err(InvocationError::Pending(value)) if *value == evidence)
            );
            assert!(matches!(
                client.resolve(&evidence).await?,
                Resolution::Expired
            ));
            assert_eq!(service.close_and_drain().await.len(), 1);
            runtime.shutdown().await?;
        }
    }
    Ok(())
}
