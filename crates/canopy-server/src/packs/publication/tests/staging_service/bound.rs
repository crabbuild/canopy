use super::*;
use crate::packs::catalog::{CatalogFileLimits, CatalogFiles};
use crate::packs::closure::{BaseResolver, ClosureError};
use canopy_object_storage::artifact::ArtifactStore;
use cellule_ltx::DiskBudget;

async fn wait_for(
    ticket: &StagingTicket,
    predicate: impl Fn(&StagingState) -> bool,
) -> Result<StagingState> {
    timeout(Duration::from_secs(10), async {
        loop {
            let state = ticket.state();
            if predicate(&state) {
                return Ok(state);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?
}
async fn bind(
    f: &Fixture,
    c: &StagingCoordinator,
    op: [u8; 16],
    actor: &str,
) -> Result<StagingTicket> {
    let ticket = submit(f, c, op, actor).await?;
    active(&ticket).await?;
    ticket.seal()?;
    assert!(matches!(terminal(&ticket).await?, StagingState::Bound(_)));
    Ok(ticket)
}
async fn claim(
    f: &Fixture,
    c: &StagingCoordinator,
    token: PreparationToken,
    mutation: MutationIdentity,
) -> Result<StagingTicket> {
    let ready = ReadyStaging::claim_bound(
        f.client(),
        f.target.clone(),
        LeaseRequest {
            check: check(token),
            lease_ms: DEFAULT_LEASE_MS,
        },
        mutation,
    )
    .await?;
    Ok(c.submit(ready).map_err(|(e, _)| e)?)
}
async fn new_token(f: &Fixture, op: [u8; 16]) -> Result<PreparationToken> {
    Ok(lease(
        f.client()
            .command::<BeginPreparation>(&f.target, identity()?, f.begin(op))
            .await?
            .output,
    )?
    .token)
}
#[tokio::test]
async fn bound_service_automatic_renewal_keeps_canceled_worker_and_result_owned_through_close()
-> Result {
    let f = Fixture::new(ObjectFormat::Sha256).await?;
    let c = StagingCoordinator::new(
        f.target.clone(),
        StagingLimits {
            renew_before_ms: DEFAULT_LEASE_MS - 1000,
            ..StagingLimits::default()
        },
    )?;
    let ticket = bind(&f, &c, [203; 16], "owner").await?;
    let original = ticket.bound_result().ok_or("binding receipt")?;
    let session = ticket.bound_session()?;
    let weak = Arc::downgrade(&session);
    let (release, wait) = oneshot::channel();
    let (entered, running) = oneshot::channel();
    let worker = ticket.spawn_bound(move |session| async move {
        session.live_lease()?;
        let _ = entered.send(());
        wait.await.map_err(|_| StagingError::Worker)?;
        session.live_lease()?;
        Ok(session)
    })?;
    let id = worker.id();
    timeout(Duration::from_secs(10), running).await??;
    drop(worker);
    drop(session);
    drop(ticket);
    let retained = c.pending([203; 16]).ok_or("bound job lost")?;
    timeout(Duration::from_secs(10), async {
        while retained.bound_renewal().is_none() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    let renewed = lease(retained.bound_renewal().ok_or("renewal outcome")?.output)?;
    assert!(renewed.expires_at_ms > original.lease.expires_at_ms);
    assert_eq!(renewed.base, original.lease.base);
    assert_eq!(renewed.token, original.lease.token);
    assert_eq!(
        retained.bound_result().ok_or("original result")?.receipt,
        original.receipt
    );
    assert!(weak.upgrade().is_some());
    assert_eq!(c.stats().workers, 1);
    assert_eq!(c.stats().admitted, 1);
    let closing = c.clone();
    let close = tokio::spawn(async move { closing.close_and_drain().await });
    timeout(Duration::from_secs(10), async {
        while !c.stats().closed {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    assert!(!close.is_finished());
    assert!(retained.spawn_bound(|_| async { Ok(()) }).is_err());
    release.send(()).map_err(|_| "worker lost")?;
    let result = retained
        .pending_task::<Arc<PreparationSession>>(id)
        .ok_or("bound result lost")?;
    let transferred = result.wait().await.map_err(|e| e.to_string())?;
    assert!(timeout(Duration::from_secs(10), close).await??.is_empty());
    assert!(transferred.live_lease().is_err());
    assert!(retained.bound_session().is_err());
    assert_eq!(c.stats().workers, 0);
    assert_eq!(c.stats().admitted, 0);
    assert_eq!(
        retained
            .bound_result()
            .ok_or("stopped binding receipt")?
            .receipt,
        original.receipt
    );
    f.runtime.shutdown().await?;
    Ok(())
}
#[tokio::test]
async fn bound_service_renewal_retains_exact_absent_lost_and_panicked_commands_through_closed_recovery()
-> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        for fault in [1, 2, 3] {
            let f = Fixture::new(format).await?;
            let c = StagingCoordinator::new(f.target.clone(), StagingLimits::default())?;
            let ticket = bind(&f, &c, [204; 16], "owner").await?;
            let original = ticket.bound_result().ok_or("binding")?;
            let shared = ticket.bound_session()?;
            f.install_empty_root(1).await?;
            c.fault_for_test(fault);
            ticket.renew_for_test();
            let StagingState::Uncertain(error) = wait_for(&ticket, |s| {
                matches!(s, StagingState::Uncertain(_) | StagingState::Fenced(_))
            })
            .await?
            else {
                return Err("bound renewal uncertainty".into());
            };
            let StagingError::BoundRenew(error) = &*error else {
                return Err("renewal evidence kind".into());
            };
            let InvocationError::Pending(evidence) = &**error else {
                return Err("renewal evidence".into());
            };
            let evidence = (**evidence).clone();
            let sequence = match f.client().resolve(&evidence).await? {
                cellule_runtime::Resolution::Absent => None,
                cellule_runtime::Resolution::Committed(value) => Some(value.commit_sequence()),
                other => return Err(format!("unexpected {other:?}").into()),
            };
            assert_eq!(sequence.is_some(), fault != 1);
            assert_eq!(c.stats().command_bytes, 8192);
            drop(ticket);
            let retained = c.pending([204; 16]).ok_or("renewal lost")?;
            assert_eq!(c.close_and_drain().await.len(), 1);
            c.recover(&retained)?;
            wait_for(&retained, |s| {
                matches!(s, StagingState::Bound(_) | StagingState::Fenced(_))
            })
            .await?;
            assert!(c.close_and_drain().await.is_empty());
            let renewed = retained.bound_renewal().ok_or("known renewal")?;
            let granted = lease(renewed.output)?;
            assert_eq!(granted.base, original.lease.base);
            assert_eq!(granted.token, original.lease.token);
            if let Some(sequence) = sequence {
                assert_eq!(renewed.receipt.commit_sequence, sequence);
            }
            let cellule_runtime::Resolution::Committed(resolved) =
                f.client().resolve(&evidence).await?
            else {
                return Err("resolved renewal".into());
            };
            assert_eq!(renewed.receipt.commit_sequence, resolved.commit_sequence());
            assert_eq!(
                retained.bound_result().ok_or("original binding")?.receipt,
                original.receipt
            );
            assert!(shared.live_lease().is_err());
            assert_eq!(c.stats().admitted, 0);
            f.runtime.shutdown().await?;
        }
    }
    Ok(())
}
#[tokio::test]
async fn bound_service_claim_retains_exact_identity_and_new_namespace_through_closed_recovery()
-> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        for fault in [1, 2, 3] {
            let f = Fixture::new(format).await?;
            let old = new_token(&f, [205; 16]).await?;
            let c = StagingCoordinator::new(f.target.clone(), StagingLimits::default())?;
            let mutation = identity()?;
            c.fault_for_test(fault);
            let ticket = claim(&f, &c, old, mutation).await?;
            let StagingState::Uncertain(error) = terminal(&ticket).await? else {
                return Err("bound Claim uncertainty".into());
            };
            let StagingError::BoundClaim(error) = &*error else {
                return Err("bound Claim evidence".into());
            };
            let InvocationError::Pending(evidence) = &**error else {
                return Err("Claim evidence".into());
            };
            let evidence = (**evidence).clone();
            drop(ticket);
            let retained = c.pending([205; 16]).ok_or("Claim lost")?;
            assert_eq!(c.close_and_drain().await.len(), 1);
            c.recover(&retained)?;
            let StagingState::Bound(bound) = terminal(&retained).await? else {
                return Err("Claim recovery".into());
            };
            assert_ne!(bound.lease.token, old);
            assert_ne!(bound.lease.token.artifact_operation, old.artifact_operation);
            let replay = f
                .client()
                .command::<ClaimPreparation>(
                    &f.target,
                    mutation,
                    LeaseRequest {
                        check: check(old),
                        lease_ms: DEFAULT_LEASE_MS,
                    },
                )
                .await?;
            assert_eq!(bound.receipt, replay.receipt);
            assert_eq!(bound.lease, lease(replay.output)?);
            let cellule_runtime::Resolution::Committed(value) =
                f.client().resolve(&evidence).await?
            else {
                return Err("Claim exact resolve".into());
            };
            assert_eq!(value.commit_sequence(), bound.receipt.commit_sequence);
            assert!(c.close_and_drain().await.is_empty());
            assert!(retained.bound_session().is_err());
            assert_eq!(f.counts().await?, (1, 2));
            f.runtime.shutdown().await?;
        }
    }
    Ok(())
}
#[tokio::test]
async fn bound_service_known_renewal_receipts_survive_revocation_expiry_and_superseding_claim()
-> Result {
    for committed in [false, true] {
        for mode in [0, 1, 2] {
            let f = Fixture::new(ObjectFormat::Sha256).await?;
            let c = StagingCoordinator::new(f.target.clone(), StagingLimits::default())?;
            let ticket = bind(&f, &c, [206; 16], "owner").await?;
            let session = ticket.bound_session()?;
            let binding = ticket.bound_result().ok_or("binding")?;
            c.fault_for_test(if committed { 2 } else { 1 });
            ticket.renew_for_test();
            let StagingState::Uncertain(error) = wait_for(&ticket, |s| {
                matches!(s, StagingState::Uncertain(_) | StagingState::Fenced(_))
            })
            .await?
            else {
                return Err("renew uncertain".into());
            };
            let StagingError::BoundRenew(error) = &*error else {
                return Err("renew error".into());
            };
            let InvocationError::Pending(evidence) = &**error else {
                return Err("evidence".into());
            };
            let evidence = (**evidence).clone();
            let denial = match mode {
                0 => {
                    super::super::publishing::edit(
                        &f,
                        "UPDATE repository_identity SET owner='other' WHERE singleton=1",
                    )
                    .await?;
                    PreparationDenial::Unauthorized
                }
                1 => {
                    super::super::publishing::edit(&f, "UPDATE catalog_operations SET expires_at_ms=0; UPDATE catalog_leases SET expires_at_ms=0").await?;
                    PreparationDenial::Expired
                }
                _ => {
                    f.client()
                        .command::<ClaimPreparation>(
                            &f.target,
                            identity()?,
                            LeaseRequest {
                                check: session.check.clone(),
                                lease_ms: DEFAULT_LEASE_MS,
                            },
                        )
                        .await?;
                    PreparationDenial::Stale
                }
            };
            c.recover(&ticket)?;
            let StagingState::Fenced(error) =
                wait_for(&ticket, |s| matches!(s, StagingState::Fenced(_))).await?
            else {
                return Err("fresh custody not fenced".into());
            };
            if committed {
                let renewal = ticket.bound_renewal().ok_or("known outcome lost")?;
                assert!(matches!(renewal.output, PreparationReply::Granted(_)));
                let cellule_runtime::Resolution::Committed(value) =
                    f.client().resolve(&evidence).await?
                else {
                    return Err("original committed receipt".into());
                };
                assert_eq!(renewal.receipt.commit_sequence, value.commit_sequence());
            } else {
                assert!(ticket.bound_renewal().is_none());
                assert!(
                    matches!(&*error, StagingError::BoundRenew(value) if matches!(&**value, InvocationError::Rejected(result) if result.output == PreparationReply::Denied(denial)))
                );
            }
            assert!(session.live_lease().is_err());
            assert!(ticket.bound_session().is_err());
            assert_eq!(
                ticket.bound_result().ok_or("binding lost")?.receipt,
                binding.receipt
            );
            assert!(c.close_and_drain().await.is_empty());
            f.runtime.shutdown().await?;
        }
    }
    Ok(())
}
#[tokio::test]
async fn bound_service_phase_handoff_and_residence_cap_fence_existing_bases_and_inflight_workers()
-> Result {
    let f = Fixture::new(ObjectFormat::Sha256).await?;
    let native =
        crate::packs::catalog::tests::prepared_for_repository(f.format, f.repository).await?;
    f.install_catalog(1, native.stored).await?;
    let c = StagingCoordinator::new(f.target.clone(), StagingLimits::default())?;
    let ticket = submit(&f, &c, [207; 16], "owner").await?;
    active(&ticket).await?;
    let work = ticket.spawn(|ctx| async { Ok(ctx) })?;
    let old = work.wait().await.map_err(|e| e.to_string())?;
    ticket.seal()?;
    assert!(matches!(terminal(&ticket).await?, StagingState::Bound(_)));
    assert!(old.ensure_live().is_err());
    assert!(ticket.spawn(|_| async { Ok(()) }).is_err());
    let session = ticket.bound_session()?;
    let store = native.store.clone();
    let root = tempfile::TempDir::new()?;
    let base = ticket
        .open_base(
            native.indexes.clone(),
            Arc::new(CatalogFiles::new(
                root.path(),
                DiskBudget::new(64 << 20),
                store,
                f.format,
                CatalogFileLimits::default(),
            )?),
        )
        .await?;
    let base_context = base.context().base.ok_or("native base")?;
    let oid = *native
        .fixture
        .objects
        .keys()
        .next()
        .ok_or("native object")?;
    assert!(base.resolve(base_context, &[oid]).await?.objects[0].is_some());
    // Fixture a past ceiling without fencing: the base reader must reject even
    // before the supervisor gets a chance to apply its permanent shared fence.
    let mut limited = base.select_current().await?;
    limited.session.ceiling = Some(tokio::time::Instant::now());
    assert!(!session.fenced.load(std::sync::atomic::Ordering::Acquire));
    assert!(matches!(
        limited.resolve(base_context, &[oid]).await,
        Err(ClosureError::LeaseExpired)
    ));
    assert!(!session.fenced.load(std::sync::atomic::Ordering::Acquire));
    let (entered, running) = oneshot::channel();
    let worker = ticket.spawn_bound(move |_| async move {
        let _ = entered.send(());
        std::future::pending::<std::result::Result<(), StagingError>>().await
    })?;
    timeout(Duration::from_secs(10), running).await??;
    tokio::time::pause();
    tokio::time::advance(Duration::from_millis(DEFAULT_LEASE_MS + 1)).await;
    tokio::time::resume();
    wait_for(&ticket, |s| matches!(s, StagingState::Fenced(_))).await?;
    assert!(session.live_lease().is_err());
    assert!(base.live_lease().is_err());
    assert!(matches!(
        base.resolve(base_context, &[oid]).await,
        Err(ClosureError::LeaseExpired)
    ));
    assert!(worker.wait().await.is_err());
    assert!(c.close_and_drain().await.is_empty());
    assert_eq!(c.stats().workers, 0);
    f.runtime.shutdown().await?;
    Ok(())
}
#[tokio::test]
async fn bound_service_worker_caps_results_and_failure_reuse_staging_admission() -> Result {
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
    let c = StagingCoordinator::new(
        f.target.clone(),
        StagingLimits {
            workers: 2,
            workers_per_actor: 1,
            ..StagingLimits::default()
        },
    )?;
    let a = bind(&f, &c, [208; 16], "owner").await?;
    let b = bind(&f, &c, [209; 16], "owner").await?;
    let session = a.bound_session()?;
    let dropped = Arc::new(AtomicBool::new(false));
    let wrong = Arc::new(AtomicBool::new(false));
    let owned = Owned {
        c: c.clone(),
        dropped: dropped.clone(),
        wrong: wrong.clone(),
    };
    let (done, completed) = oneshot::channel();
    let work = a.spawn_bound(move |_| async move {
        let _ = done.send(());
        Ok(owned)
    })?;
    timeout(Duration::from_secs(10), completed).await??;
    assert_eq!(c.stats().workers, 1);
    assert!(b.spawn_bound(|_| async { Ok(()) }).is_err());
    super::super::publishing::edit(
        &f,
        "UPDATE repository_identity SET owner='other' WHERE singleton=1",
    )
    .await?;
    a.renew_for_test();
    wait_for(&a, |s| matches!(s, StagingState::Fenced(_))).await?;
    assert!(work.wait().await.is_err());
    assert!(dropped.load(Ordering::Acquire));
    assert!(!wrong.load(Ordering::Acquire));
    assert!(session.live_lease().is_err());
    assert_eq!(c.stats().workers, 0);
    assert!(c.close_and_drain().await.is_empty());
    for invalid in [0, MAX_LEASE_MS + 1] {
        assert!(
            StagingCoordinator::new(
                f.target.clone(),
                StagingLimits {
                    bound_lifetime_ms: invalid,
                    ..StagingLimits::default()
                }
            )
            .is_err()
        );
    }
    f.runtime.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn bound_service_checkpoint_shares_renewal_order_exact_recovery_and_original_receipt_custody()
-> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        for fault in [1, 2, 3] {
            let f = Fixture::new(format).await?;
            let (source, first) = super::super::inputs::active(&f, [210; 16]).await?;
            let store = Arc::new(ArtifactStore::new(Arc::new(InMemory::new()), f.repository));
            let prior = super::super::inputs::seal(&f, &first, store.clone(), 300).await?;
            first
                .register_inputs(prior.clone(), identity()?)
                .map_err(|(e, _)| e)?
                .wait()
                .await
                .map_err(|e| e.to_string())?;
            first.seal()?;
            let StagingState::Bound(old) = terminal(&first).await? else {
                return Err("source bind".into());
            };
            assert!(source.close_and_drain().await.is_empty());
            let c = StagingCoordinator::new(f.target.clone(), StagingLimits::default())?;
            let ticket = claim(&f, &c, old.lease.token, identity()?).await?;
            assert!(matches!(terminal(&ticket).await?, StagingState::Bound(_)));
            let session = ticket.bound_session()?;
            let parent = prior.clone();
            let provider = store.clone();
            let worker = ticket.spawn_bound(move |s| async move {
                s.adopt_native_inputs(provider, &parent)
                    .await
                    .map_err(|e| StagingError::Input(Box::new(e)))
            })?;
            let adopted = worker.wait().await.map_err(|e| e.to_string())?;
            assert_eq!(adopted.root()?, prior.root()?);
            // Resolve a due renewal before the admitted checkpoint; its original
            // output remains separate from the checkpoint's new exact command.
            ticket.renew_for_test();
            timeout(Duration::from_secs(10), async {
                while ticket.bound_renewal().is_none() {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await?;
            let renewal = ticket.bound_renewal().ok_or("ordered renewal")?;
            let mutation = identity()?;
            c.fault_for_test(fault);
            let observer = ticket
                .register_inputs(adopted.clone(), mutation)
                .map_err(|(e, _)| e)?;
            drop(observer);
            let StagingState::Uncertain(error) = wait_for(&ticket, |s| {
                matches!(s, StagingState::Uncertain(_) | StagingState::Fenced(_))
            })
            .await?
            else {
                return Err("bound checkpoint uncertainty".into());
            };
            let StagingError::Checkpoint(error) = &*error else {
                return Err("bound checkpoint evidence".into());
            };
            let InvocationError::Pending(evidence) = &**error else {
                return Err("checkpoint evidence".into());
            };
            let evidence = (**evidence).clone();
            assert_eq!(c.stats().command_bytes, 12 << 10);
            if fault == 2 {
                super::super::publishing::edit(
                    &f,
                    "UPDATE repository_identity SET owner='other' WHERE singleton=1",
                )
                .await?;
            }
            assert_eq!(c.close_and_drain().await.len(), 1);
            c.recover(&ticket)?;
            let recorded = ticket
                .pending_inputs()
                .ok_or("checkpoint observer lost")?
                .wait()
                .await
                .map_err(|e| e.to_string())?;
            let original = f
                .client()
                .command::<RegisterStagedInputs>(&f.target, mutation, adopted.clone())
                .await?;
            assert_eq!(recorded, original.receipt);
            assert!(recorded.commit_sequence > renewal.receipt.commit_sequence);
            let cellule_runtime::Resolution::Committed(value) =
                f.client().resolve(&evidence).await?
            else {
                return Err("checkpoint resolution".into());
            };
            assert_eq!(recorded.commit_sequence, value.commit_sequence());
            assert!(c.close_and_drain().await.is_empty());
            assert!(session.live_lease().is_err());
            assert_eq!(
                ticket.bound_renewal().ok_or("renewal overwritten")?.receipt,
                renewal.receipt
            );
            if fault != 2 {
                let current = f
                    .client()
                    .query::<CheckStagedInputs>(&f.target, Some(recorded), session.check.clone())
                    .await?
                    .output
                    .ok_or("destination checkpoint")?;
                assert_eq!(current.root()?, prior.root()?);
            } else {
                assert!(matches!(ticket.state(), StagingState::Fenced(_)));
            }
            f.runtime.shutdown().await?;
        }
    }
    Ok(())
}
#[tokio::test]
async fn bound_service_restored_owner_claim_retains_old_pin_and_owns_new_session_workers() -> Result
{
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let f = Fixture::new(format).await?;
        let old = new_token(&f, [211; 16]).await?;
        f.handle.drain().await?;
        f.runtime.shutdown().await?;
        let owner_session = SessionId::from_bytes([212; 16]);
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
                f.root.path().join("bound-lifecycle-restored.sqlite"),
                Owner {
                    session: owner_session,
                    endpoint: "https://bound-lifecycle-restored.invalid".into(),
                },
            )
            .await?;
        let client = CellClient::local(f.registry.clone(), handle.clone());
        let c = StagingCoordinator::new(f.target.clone(), StagingLimits::default())?;
        let mutation = identity()?;
        c.fault_for_test(2);
        let ready = ReadyStaging::claim_bound(
            client.clone(),
            f.target.clone(),
            LeaseRequest {
                check: check(old),
                lease_ms: DEFAULT_LEASE_MS,
            },
            mutation,
        )
        .await?;
        let ticket = c.submit(ready).map_err(|(e, _)| e)?;
        assert!(matches!(
            terminal(&ticket).await?,
            StagingState::Uncertain(_)
        ));
        c.recover(&ticket)?;
        let StagingState::Bound(bound) = terminal(&ticket).await? else {
            return Err("restored bound Claim".into());
        };
        assert_ne!(bound.lease.token.owner, old.owner);
        assert_ne!(bound.lease.token.artifact_operation, old.artifact_operation);
        let replay = client
            .command::<ClaimPreparation>(
                &f.target,
                mutation,
                LeaseRequest {
                    check: check(old),
                    lease_ms: DEFAULT_LEASE_MS,
                },
            )
            .await?;
        assert_eq!(bound.receipt, replay.receipt);
        let old_pin = handle.query(0, 32, move |conn| { Ok(conn.query_row("SELECT generation FROM catalog_leases WHERE incarnation=?1 AND admission_sequence=?2", rusqlite::params![old.owner.incarnation.as_bytes().as_slice(), old.attempt as i64], |row| row.get::<_, i64>(0))?.to_be_bytes().to_vec()) }).await?;
        assert_eq!(old_pin.as_slice(), 0i64.to_be_bytes());
        let worker =
            ticket.spawn_bound(|session| async move { Ok(session.live_lease()?.0.token) })?;
        assert_eq!(
            worker.wait().await.map_err(|e| e.to_string())?,
            bound.lease.token
        );
        ticket.renew_for_test();
        timeout(Duration::from_secs(10), async {
            while ticket.bound_renewal().is_none() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await?;
        assert_eq!(
            lease(ticket.bound_renewal().ok_or("restored renewal")?.output)?.token,
            bound.lease.token
        );
        assert!(c.close_and_drain().await.is_empty());
        runtime.shutdown().await?;
    }
    Ok(())
}
