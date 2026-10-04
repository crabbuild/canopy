use super::*;

async fn session(f: &Fixture, operation: [u8; 16]) -> Result<Arc<PreparationSession>> {
    let started = registered_preparation(f, operation).await?;
    let lease = lease(started.output)?;
    Ok(Arc::new(
        PreparationSession::open(
            f.client(),
            f.target.clone(),
            check(lease.token),
            Some(started.receipt),
        )
        .await?,
    ))
}
fn request_for(s: &PreparationSession) -> LeaseRequest {
    LeaseRequest {
        check: s.check.clone(),
        lease_ms: DEFAULT_LEASE_MS,
    }
}
async fn ready(
    f: &Fixture,
    s: &Arc<PreparationSession>,
    kind: PreparationCommandKind,
    mutation: MutationIdentity,
) -> Result<ReadyPreparation> {
    Ok(match kind {
        PreparationCommandKind::Claim => {
            ReadyPreparation::claim(f.client(), f.target.clone(), request_for(s), mutation).await?
        }
        PreparationCommandKind::Renew => s.ready_renew(mutation, DEFAULT_LEASE_MS).await?,
    })
}
fn changed(state: PublicationState) -> Result<PreparationCommandOutcome> {
    match state {
        PublicationState::Finished(Ok(PublicationOutcome::Preparation(value))) => Ok(value),
        other => Err(format!("preparation outcome: {other:?}").into()),
    }
}
async fn replay(
    f: &Fixture,
    s: &PreparationSession,
    kind: PreparationCommandKind,
    mutation: MutationIdentity,
) -> Result<cellule_runtime::Committed<PreparationReply>> {
    let saved = RegisteredCustody::load_latest(&f.client(), &f.target, s.lease.token.operation)
        .await?
        .ok_or("registered command missing")?;
    assert_eq!(saved.evidence().identity(), mutation);
    let result = saved.recover_preparation(&f.client()).await?;
    let PreparationReply::Granted(lease) = &result.output else {
        return Err("unexpected replay denial".into());
    };
    assert_eq!(
        lease.token == s.lease.token,
        kind == PreparationCommandKind::Renew
    );
    Ok(result)
}
#[tokio::test]
async fn bound_lease_commands_keep_exact_identity_and_original_floor_through_closed_uncertain_recovery()
-> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        for kind in [PreparationCommandKind::Claim, PreparationCommandKind::Renew] {
            for fault in [1, 2, 3, 4, 5, 6] {
                let f = Fixture::new(format).await?;
                let s = session(&f, [196; 16]).await?;
                f.install_empty_root(1).await?;
                let coordinator =
                    PublicationCoordinator::new(f.target.clone(), PublicationLimits::default())?;
                let mutation = identity()?;
                coordinator.fault_for_test(fault);
                let ticket = coordinator
                    .submit(ready(&f, &s, kind, mutation).await?)
                    .await?;
                let PublicationState::Uncertain(error) =
                    timeout(Duration::from_secs(10), ticket.wait()).await?
                else {
                    return Err("lease uncertainty".into());
                };
                let (evidence, registrar) = match &*error {
                    PublicationError::Preparation(cellule_runtime::InvocationError::Pending(
                        evidence,
                    )) => ((**evidence).clone(), None),
                    PublicationError::Custody { evidence, source } => {
                        let CustodyError::Registration(source) = &**source else {
                            return Err("wrong registrar error".into());
                        };
                        let InvocationError::Pending(registrar) = &**source else {
                            return Err("registrar identity lost".into());
                        };
                        ((**evidence).clone(), Some((**registrar).clone()))
                    }
                    _ => return Err("exact lease evidence".into()),
                };
                let original = match f.client().resolve(&evidence).await? {
                    cellule_runtime::Resolution::Absent => None,
                    cellule_runtime::Resolution::Committed(value) => Some(value.commit_sequence()),
                    other => return Err(format!("unexpected {other:?}").into()),
                };
                assert_eq!(original.is_some(), matches!(fault, 2 | 3));
                assert_eq!(
                    coordinator.reservations_for_test().await,
                    (1, super::super::super::custody::RESERVATION, 1)
                );
                drop(ticket);
                let retained = coordinator
                    .pending([196; 16])
                    .await
                    .ok_or("lease retained")?;
                assert_eq!(coordinator.close_and_drain().await.len(), 1);
                coordinator.recover(&retained).await?;
                let outcome = changed(timeout(Duration::from_secs(10), retained.wait()).await?)?;
                assert_eq!(outcome.kind, kind);
                if let Some(registrar) = registrar {
                    assert!(matches!(
                        f.client().resolve(&registrar).await?,
                        cellule_runtime::Resolution::Committed(_)
                    ));
                }
                assert_eq!(outcome.committed, replay(&f, &s, kind, mutation).await?);
                if let Some(sequence) = original {
                    assert_eq!(outcome.committed.receipt.commit_sequence, sequence);
                }
                let fresh = outcome.session.map_err(|e| e.to_string())?;
                let (lease, _) = fresh.live_lease()?;
                assert_eq!(lease.format, format);
                match kind {
                    PreparationCommandKind::Claim => {
                        assert_ne!(lease.token, s.lease.token);
                        assert_eq!(lease.base.generation, 1);
                    }
                    PreparationCommandKind::Renew => {
                        assert_eq!(lease.token, s.lease.token);
                        assert_eq!(lease.base.generation, 0);
                        assert!(Arc::ptr_eq(&fresh, &s));
                    }
                }
                let old = s.lease.token;
                let old_floor = f.handle.query(0, 32, move |conn| {
                    Ok(conn.query_row("SELECT generation FROM catalog_leases WHERE incarnation=?1 AND admission_sequence=?2", rusqlite::params![old.owner.incarnation.as_bytes().as_slice(), old.attempt as i64], |row| row.get::<_, i64>(0))?.to_be_bytes().to_vec())
                }).await?;
                assert_eq!(old_floor.as_slice(), 0i64.to_be_bytes());
                assert!(retained.response().await.is_err());
                assert_eq!(coordinator.reservations_for_test().await, (0, 0, 0));
                assert!(coordinator.close_and_drain().await.is_empty());
                f.runtime.shutdown().await?;
            }
        }
    }
    Ok(())
}
#[tokio::test]
async fn bound_lease_ready_admission_preserves_command_and_canceled_observer_session_ownership()
-> Result {
    let f = Fixture::new(ObjectFormat::Sha256).await?;
    let s = session(&f, [197; 16]).await?;
    let weak = Arc::downgrade(&s);
    let mutation = identity()?;
    let prepared = s.ready_renew(mutation, DEFAULT_LEASE_MS).await?;
    let foreign = PublicationCoordinator::new(
        crate::repository_target(
            f.target.tenant(),
            f.target.application(),
            uuid::Uuid::new_v4().into_bytes(),
        )?,
        PublicationLimits::default(),
    )?;
    let rejected = foreign
        .submit(prepared)
        .await
        .err()
        .ok_or("foreign admitted")?;
    assert_eq!(rejected.reason, PublicationScheduleError::Foreign);
    let coordinator = PublicationCoordinator::new(f.target.clone(), PublicationLimits::default())?;
    let (release, entered) = coordinator.pause_for_test().await;
    let ticket = coordinator.submit(rejected.ready).await?;
    timeout(Duration::from_secs(5), entered).await??;
    let duplicate = coordinator
        .submit(s.ready_renew(identity()?, DEFAULT_LEASE_MS).await?)
        .await
        .err()
        .ok_or("duplicate admitted")?;
    assert_eq!(duplicate.reason, PublicationScheduleError::Duplicate);
    drop(duplicate);
    drop(s);
    drop(ticket);
    assert!(weak.upgrade().is_some());
    let pending = coordinator
        .pending([197; 16])
        .await
        .ok_or("running lease retained")?;
    release.send(()).map_err(|_| "worker gone")?;
    let outcome = changed(timeout(Duration::from_secs(10), pending.wait()).await?)?;
    let fresh = outcome.session.map_err(|e| e.to_string())?;
    assert_eq!(
        outcome.committed,
        replay(&f, &fresh, PreparationCommandKind::Renew, mutation).await?
    );
    assert!(coordinator.close_and_drain().await.is_empty());
    let prepared = fresh.ready_renew(identity()?, DEFAULT_LEASE_MS).await?;
    let refused = coordinator
        .submit(prepared)
        .await
        .err()
        .ok_or("closed admitted")?;
    assert_eq!(refused.reason, PublicationScheduleError::Closed);
    let other = PublicationCoordinator::new(f.target.clone(), PublicationLimits::default())?;
    let retried = other.submit(refused.ready).await?;
    changed(timeout(Duration::from_secs(10), retried.wait()).await?)?;
    assert!(other.close_and_drain().await.is_empty());
    assert!(foreign.close_and_drain().await.is_empty());
    f.runtime.shutdown().await?;
    Ok(())
}
#[tokio::test]
async fn bound_lease_committed_recovery_keeps_receipt_when_fresh_custody_is_revoked_expired_or_superseded()
-> Result {
    for kind in [PreparationCommandKind::Claim, PreparationCommandKind::Renew] {
        for mode in [0, 1, 2] {
            let f = Fixture::new(ObjectFormat::Sha256).await?;
            let s = session(&f, [198; 16]).await?;
            let coordinator =
                PublicationCoordinator::new(f.target.clone(), PublicationLimits::default())?;
            let mutation = identity()?;
            coordinator.fault_for_test(2);
            let ticket = coordinator
                .submit(ready(&f, &s, kind, mutation).await?)
                .await?;
            assert!(matches!(
                timeout(Duration::from_secs(10), ticket.wait()).await?,
                PublicationState::Uncertain(_)
            ));
            let original = replay(&f, &s, kind, mutation).await?;
            let current = lease(original.output.clone())?;
            match mode {
                0 => edit(&f, "UPDATE repository_identity SET owner='other' WHERE singleton=1").await?,
                1 => edit(&f, "UPDATE catalog_operations SET expires_at_ms=0; UPDATE catalog_leases SET expires_at_ms=0").await?,
                _ => { f.client().command::<ClaimPreparation>(&f.target, identity()?, LeaseRequest { check: check(current.token), lease_ms: DEFAULT_LEASE_MS }).await?; }
            }
            coordinator.recover(&ticket).await?;
            let outcome = changed(timeout(Duration::from_secs(10), ticket.wait()).await?)?;
            assert_eq!(outcome.committed, original);
            assert!(outcome.session.is_err());
            if kind == PreparationCommandKind::Renew {
                assert!(s.live_lease().is_err());
            }
            assert_eq!(outcome.committed, replay(&f, &s, kind, mutation).await?);
            assert!(coordinator.close_and_drain().await.is_empty());
            f.runtime.shutdown().await?;
        }
    }
    Ok(())
}
#[tokio::test]
async fn bound_lease_absent_recovery_rechecks_authority_and_claim_can_recover_expired_source()
-> Result {
    for kind in [PreparationCommandKind::Claim, PreparationCommandKind::Renew] {
        for mode in [0, 1, 2] {
            let f = Fixture::new(ObjectFormat::Sha1).await?;
            let s = session(&f, [199; 16]).await?;
            let coordinator =
                PublicationCoordinator::new(f.target.clone(), PublicationLimits::default())?;
            coordinator.fault_for_test(1);
            let ticket = coordinator
                .submit(ready(&f, &s, kind, identity()?).await?)
                .await?;
            assert!(matches!(
                timeout(Duration::from_secs(10), ticket.wait()).await?,
                PublicationState::Uncertain(_)
            ));
            let denial = match mode {
                0 => {
                    edit(
                        &f,
                        "UPDATE repository_identity SET owner='other' WHERE singleton=1",
                    )
                    .await?;
                    PreparationDenial::Unauthorized
                }
                1 => {
                    edit(&f, "UPDATE catalog_operations SET expires_at_ms=0; UPDATE catalog_leases SET expires_at_ms=0").await?;
                    PreparationDenial::Expired
                }
                _ => {
                    f.client()
                        .command::<ClaimPreparation>(&f.target, identity()?, request_for(&s))
                        .await?;
                    PreparationDenial::Stale
                }
            };
            coordinator.recover(&ticket).await?;
            let state = timeout(Duration::from_secs(10), ticket.wait()).await?;
            if kind == PreparationCommandKind::Claim && mode == 1 {
                let result = changed(state)?;
                let fresh = result.session.map_err(|e| e.to_string())?;
                assert_ne!(fresh.live_lease()?.0.token, s.lease.token);
            } else {
                let PublicationState::Finished(Err(error)) = state else {
                    return Err("absent lease accepted".into());
                };
                assert!(
                    matches!(&*error, PublicationError::Preparation(cellule_runtime::InvocationError::Rejected(value)) if value.output == PreparationReply::Denied(denial))
                );
                if kind == PreparationCommandKind::Renew {
                    assert!(s.live_lease().is_err());
                }
            }
            assert!(coordinator.close_and_drain().await.is_empty());
            f.runtime.shutdown().await?;
        }
    }
    Ok(())
}
#[tokio::test]
async fn bound_lease_ready_rejects_invalid_context_size_duration_and_never_revives_fenced_session()
-> Result {
    let f = Fixture::new(ObjectFormat::Sha256).await?;
    let s = session(&f, [200; 16]).await?;
    for duration in [0, MAX_LEASE_MS + 1] {
        assert!(s.ready_renew(identity()?, duration).await.is_err());
    }
    let mut wrong = request_for(&s);
    wrong.check.token.repository = uuid::Uuid::new_v4().into_bytes();
    assert!(
        ReadyPreparation::claim(f.client(), f.target.clone(), wrong, identity()?)
            .await
            .is_err()
    );
    let mut huge = request_for(&s);
    huge.check.actor = "x".repeat(8192);
    assert!(
        ReadyPreparation::claim(f.client(), f.target.clone(), huge, identity()?)
            .await
            .is_err()
    );
    let coordinator = PublicationCoordinator::new(f.target.clone(), PublicationLimits::default())?;
    coordinator.fault_for_test(2);
    let ticket = coordinator
        .submit(s.ready_renew(identity()?, DEFAULT_LEASE_MS).await?)
        .await?;
    assert!(matches!(
        timeout(Duration::from_secs(10), ticket.wait()).await?,
        PublicationState::Uncertain(_)
    ));
    s.fence();
    coordinator.recover(&ticket).await?;
    let result = changed(timeout(Duration::from_secs(10), ticket.wait()).await?)?;
    assert!(result.session.is_err());
    assert!(s.ready_renew(identity()?, DEFAULT_LEASE_MS).await.is_err());
    assert!(s.live_lease().is_err());
    assert!(coordinator.close_and_drain().await.is_empty());
    f.runtime.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn bound_lease_claim_after_actual_owner_restore_uses_new_fence_and_preserves_original_pin()
-> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let f = Fixture::new(format).await?;
        let original = session(&f, [201; 16]).await?;
        let old = original.lease.token;
        f.handle.drain().await?;
        f.runtime.shutdown().await?;
        let owner_session = SessionId::from_bytes([202; 16]);
        let runtime = CellRuntime::new(SqlWorkerPool::new(1, 4)?, 64 << 20, owner_session)?;
        let authority = CellAuthority::new(f.layout.clone());
        let idle = authority.load(f.target.cell_id()).await?.ok_or("idle")?;
        let provision = CellCatalog::new(f.layout.clone(), f.target.tenant())
            .lookup(f.target.cell_id())
            .await?
            .ok_or("provision")?;
        let handle = runtime
            .acquire_idle_restored(
                provision,
                f.replica.clone(),
                authority,
                idle,
                f.root.path().join("bound-restored.sqlite"),
                Owner {
                    session: owner_session,
                    endpoint: "https://bound-restored.invalid".into(),
                },
            )
            .await?;
        let client = CellClient::local(Arc::clone(&f.registry), handle.clone());
        let coordinator =
            PublicationCoordinator::new(f.target.clone(), PublicationLimits::default())?;
        let mutation = identity()?;
        coordinator.fault_for_test(2);
        let ticket = coordinator
            .submit(
                ReadyPreparation::claim(
                    client.clone(),
                    f.target.clone(),
                    request_for(&original),
                    mutation,
                )
                .await?,
            )
            .await?;
        assert!(matches!(
            timeout(Duration::from_secs(10), ticket.wait()).await?,
            PublicationState::Uncertain(_)
        ));
        drop(ticket);
        let retained = coordinator
            .pending(old.operation)
            .await
            .ok_or("restored Claim retained")?;
        coordinator.recover(&retained).await?;
        let outcome = changed(timeout(Duration::from_secs(10), retained.wait()).await?)?;
        let original_command = RegisteredCustody::load_latest(&client, &f.target, old.operation)
            .await?
            .ok_or("claim journal missing")?;
        assert_eq!(original_command.evidence().identity(), mutation);
        let replay = original_command.recover_preparation(&client).await?;
        assert_eq!(outcome.committed, replay);
        let current = outcome.session.map_err(|e| e.to_string())?;
        let next = current.live_lease()?.0;
        assert_ne!(next.token.owner, old.owner);
        assert_ne!(next.token.artifact_operation, old.artifact_operation);
        assert_eq!(next.base, original.lease.base);
        let old_pin = handle.query(0, 32, move |conn| {
            Ok(conn.query_row("SELECT generation FROM catalog_leases WHERE incarnation=?1 AND admission_sequence=?2", rusqlite::params![old.owner.incarnation.as_bytes().as_slice(), old.attempt as i64], |row| row.get::<_, i64>(0))?.to_be_bytes().to_vec())
        }).await?;
        assert_eq!(old_pin.as_slice(), 0i64.to_be_bytes());
        let renewed = coordinator
            .submit(current.ready_renew(identity()?, DEFAULT_LEASE_MS).await?)
            .await?;
        let renewed = changed(timeout(Duration::from_secs(10), renewed.wait()).await?)?;
        assert_eq!(renewed.kind, PreparationCommandKind::Renew);
        assert!(renewed.session.is_ok());
        assert!(coordinator.close_and_drain().await.is_empty());
        runtime.shutdown().await?;
    }
    Ok(())
}
