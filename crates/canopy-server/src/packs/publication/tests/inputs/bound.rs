use super::*;
use crate::packs::catalog::{CatalogFileLimits, CatalogFiles, CatalogIndexes};
use crate::packs::metadata::tests::limits;
use crate::packs::verification::{PhysicalVerifier, physical::tests::physical_limits};
use cellule_ltx::DiskBudget;

struct Bound {
    fixture: Fixture,
    store: Arc<ArtifactStore>,
    prior: NativeInputCertificate,
    proof: NativeInputCertificate,
    session: Arc<PreparationSession>,
    coordinator: PublicationCoordinator,
}
impl Bound {
    async fn new(format: ObjectFormat, operation: [u8; 16], real: bool) -> Result<Self> {
        let fixture = Fixture::new(format).await?;
        let (staging, ticket) = active(&fixture, operation).await?;
        let store = Arc::new(ArtifactStore::new(
            Arc::new(InMemory::new()),
            fixture.repository,
        ));
        let prior = if real {
            let provider = store.clone();
            let work = ticket.spawn(move |context| async move {
                capture_real(context, provider)
                    .await
                    .map_err(StagingError::Input)
            })?;
            work.wait().await.map_err(|e| e.to_string())?
        } else {
            seal(&fixture, &ticket, store.clone(), 300).await?
        };
        ticket
            .register_inputs(prior.clone(), identity()?)
            .map_err(|(e, _)| e)?
            .wait()
            .await
            .map_err(|e| e.to_string())?;
        ticket.seal()?;
        let StagingState::Bound(bound) =
            timeout(Duration::from_secs(10), ticket.wait_terminal()).await?
        else {
            return Err("bound source".into());
        };
        assert!(staging.close_and_drain().await.is_empty());
        let claimed = fixture
            .client()
            .command::<ClaimPreparation>(
                &fixture.target,
                identity()?,
                LeaseRequest {
                    check: LeaseCheck {
                        token: bound.lease.token,
                        actor: "owner".into(),
                    },
                    lease_ms: DEFAULT_LEASE_MS,
                },
            )
            .await?;
        let next = lease(claimed.output)?;
        let session = Arc::new(
            PreparationSession::open(
                fixture.client(),
                fixture.target.clone(),
                LeaseCheck {
                    token: next.token,
                    actor: "owner".into(),
                },
                Some(claimed.receipt),
            )
            .await?,
        );
        let proof = session.adopt_native_inputs(store.clone(), &prior).await?;
        assert_eq!(proof.root()?, prior.root()?);
        let coordinator =
            PublicationCoordinator::new(fixture.target.clone(), PublicationLimits::default())?;
        Ok(Self {
            fixture,
            store,
            prior,
            proof,
            session,
            coordinator,
        })
    }
    async fn submit(&self, mutation: MutationIdentity) -> Result<PublicationTicket> {
        Ok(self
            .coordinator
            .submit(
                self.session
                    .ready_inputs(mutation, self.proof.clone())
                    .await?,
            )
            .await?)
    }
}
fn registered(state: PublicationState) -> Result<RegisteredNativeInputs> {
    match state {
        PublicationState::Finished(Ok(PublicationOutcome::Inputs(value))) => Ok(value),
        state => Err(format!("bound checkpoint outcome: {state:?}").into()),
    }
}
#[tokio::test]
async fn bound_checkpoint_keeps_exact_command_through_absence_lost_reply_panic_and_closed_recovery()
-> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        for fault in [1, 2, 3] {
            let bound = Bound::new(format, [185; 16], false).await?;
            let mutation = identity()?;
            bound.coordinator.fault_for_test(fault);
            let ticket = bound.submit(mutation).await?;
            let PublicationState::Uncertain(error) =
                timeout(Duration::from_secs(10), ticket.wait()).await?
            else {
                return Err("bound registration uncertainty".into());
            };
            let PublicationError::Inputs(InvocationError::Pending(evidence)) = &*error else {
                return Err("bound exact evidence".into());
            };
            let evidence = (**evidence).clone();
            drop(ticket);
            let pending = bound
                .coordinator
                .pending([185; 16])
                .await
                .ok_or("retained bound command")?;
            let stats = bound.coordinator.stats().await;
            assert_eq!(
                (
                    stats.admitted,
                    stats.command_bytes,
                    stats.foreground,
                    stats.maintenance
                ),
                (1, 8192, 1, 0)
            );
            let closed = bound.coordinator.close_and_drain().await;
            assert_eq!(closed.len(), 1);
            bound.coordinator.recover(&pending).await?;
            let result = registered(timeout(Duration::from_secs(10), pending.wait()).await?)?;
            assert!(result.custody.is_ok());
            assert!(matches!(
                result.registration.output,
                StagingReply::Granted(_)
            ));
            let replay = bound
                .fixture
                .client()
                .command::<RegisterStagedInputs>(
                    &bound.fixture.target,
                    mutation,
                    bound.proof.clone(),
                )
                .await?;
            assert_eq!(result.registration, replay);
            let cellule_runtime::Resolution::Committed(outcome) =
                bound.fixture.client().resolve(&evidence).await?
            else {
                return Err("bound original resolution".into());
            };
            assert_eq!(replay.receipt.commit_sequence, outcome.commit_sequence());
            assert_eq!(
                check(
                    &bound.fixture.client(),
                    &bound.fixture.target,
                    bound.proof.token()?
                )
                .await?,
                Some(bound.proof.clone())
            );
            assert!(pending.response().await.is_err());
            assert!(bound.session.live_lease().is_ok());
            assert_eq!(bound.coordinator.stats().await.command_bytes, 0);
            assert!(bound.coordinator.close_and_drain().await.is_empty());
            bound.fixture.runtime.shutdown().await?;
        }
    }
    Ok(())
}
#[tokio::test]
async fn bound_checkpoint_canceled_observer_and_foreign_duplicate_closed_admission_keep_original_ready()
-> Result {
    let Bound {
        fixture,
        store,
        prior,
        proof,
        session,
        coordinator,
    } = Bound::new(ObjectFormat::Sha256, [186; 16], false).await?;
    assert!(matches!(
        session.ready_inputs(identity()?, prior).await,
        Err(NativeInputReadyError::Codec(_))
    ));
    let mutation = identity()?;
    let ready = session.ready_inputs(mutation, proof.clone()).await?;
    let other = Fixture::new(ObjectFormat::Sha256).await?;
    let foreign = PublicationCoordinator::new(other.target.clone(), PublicationLimits::default())?;
    let refused = foreign
        .submit(ready)
        .await
        .err()
        .ok_or("expected refused admission")?;
    assert_eq!(refused.reason, PublicationScheduleError::Foreign);
    assert!(matches!(refused.ready, ReadyPublication::Inputs(_)));
    assert!(
        check(&fixture.client(), &fixture.target, proof.token()?)
            .await?
            .is_none()
    );
    let (release, entered) = coordinator.pause_for_test().await;
    let ticket = coordinator.submit(refused.ready).await?;
    timeout(Duration::from_secs(5), entered).await??;
    let extra = session.ready_inputs(identity()?, proof.clone()).await?;
    let refused = coordinator
        .submit(extra)
        .await
        .err()
        .ok_or("expected refused admission")?;
    assert_eq!(refused.reason, PublicationScheduleError::Duplicate);
    let weak = Arc::downgrade(&session);
    drop(refused);
    drop(session);
    drop(ticket);
    assert!(weak.upgrade().is_some());
    let pending = coordinator
        .pending([186; 16])
        .await
        .ok_or("canceled bound observer")?;
    assert_eq!(coordinator.reservations_for_test().await, (1, 8192, 1));
    release.send(()).map_err(|_| "bound worker stopped")?;
    let result = registered(timeout(Duration::from_secs(10), pending.wait()).await?)?;
    assert!(result.custody.is_ok());
    assert!(weak.upgrade().is_none());
    assert_eq!(
        result.registration,
        fixture
            .client()
            .command::<RegisterStagedInputs>(&fixture.target, mutation, proof.clone())
            .await?
    );
    assert!(coordinator.close_and_drain().await.is_empty());
    let fresh = Arc::new(
        PreparationSession::open(
            fixture.client(),
            fixture.target.clone(),
            LeaseCheck {
                token: proof.token()?,
                actor: "owner".into(),
            },
            Some(result.registration.receipt),
        )
        .await?,
    );
    let ready = fresh.ready_inputs(identity()?, proof).await?;
    assert_eq!(
        coordinator
            .submit(ready)
            .await
            .err()
            .ok_or("expected refused admission")?
            .reason,
        PublicationScheduleError::Closed
    );
    assert!(foreign.close_and_drain().await.is_empty());
    drop(store);
    fixture.runtime.shutdown().await?;
    other.runtime.shutdown().await?;
    Ok(())
}
#[tokio::test]
async fn bound_checkpoint_committed_recovery_preserves_original_receipt_and_fences_revoked_expired_or_claimed_session()
-> Result {
    for mode in [0, 1, 2] {
        let bound = Bound::new(ObjectFormat::Sha256, [187; 16], false).await?;
        let mutation = identity()?;
        bound.coordinator.fault_for_test(2);
        let ticket = bound.submit(mutation).await?;
        assert!(matches!(
            timeout(Duration::from_secs(10), ticket.wait()).await?,
            PublicationState::Uncertain(_)
        ));
        match mode {
            0 => mutate(&bound.fixture.handle, "UPDATE repository_identity SET owner='other' WHERE singleton=1".into()).await?,
            1 => mutate(&bound.fixture.handle, "UPDATE catalog_operations SET expires_at_ms=0; UPDATE catalog_leases SET expires_at_ms=0".into()).await?,
            _ => { bound.fixture.client().command::<ClaimPreparation>(&bound.fixture.target, identity()?, LeaseRequest { check: bound.session.check.clone(), lease_ms: DEFAULT_LEASE_MS }).await?; }
        }
        bound.coordinator.recover(&ticket).await?;
        let result = registered(timeout(Duration::from_secs(10), ticket.wait()).await?)?;
        assert!(result.custody.is_err());
        assert!(bound.session.live_lease().is_err());
        let replay = bound
            .fixture
            .client()
            .command::<RegisterStagedInputs>(&bound.fixture.target, mutation, bound.proof.clone())
            .await?;
        assert_eq!(result.registration, replay);
        assert!(matches!(
            session_ready(&bound).await,
            Err(NativeInputReadyError::Base(_))
        ));
        assert!(bound.coordinator.close_and_drain().await.is_empty());
        bound.fixture.runtime.shutdown().await?;
    }
    Ok(())
}
async fn session_ready(
    bound: &Bound,
) -> std::result::Result<ReadyNativeInputs, NativeInputReadyError> {
    bound
        .session
        .ready_inputs(
            crate::server::mutation_identity().unwrap(),
            bound.proof.clone(),
        )
        .await
}
#[tokio::test]
async fn bound_checkpoint_absent_recovery_denies_revocation_expiry_and_claim_without_attaching_inventory()
-> Result {
    for mode in [0, 1, 2] {
        let bound = Bound::new(ObjectFormat::Sha1, [188; 16], false).await?;
        bound.coordinator.fault_for_test(1);
        let ticket = bound.submit(identity()?).await?;
        assert!(matches!(
            timeout(Duration::from_secs(10), ticket.wait()).await?,
            PublicationState::Uncertain(_)
        ));
        let reason = match mode {
            0 => {
                mutate(
                    &bound.fixture.handle,
                    "UPDATE repository_identity SET owner='other' WHERE singleton=1".into(),
                )
                .await?;
                PreparationDenial::Unauthorized
            }
            1 => {
                mutate(&bound.fixture.handle, "UPDATE catalog_operations SET expires_at_ms=0; UPDATE catalog_leases SET expires_at_ms=0".into()).await?;
                PreparationDenial::Expired
            }
            _ => {
                bound
                    .fixture
                    .client()
                    .command::<ClaimPreparation>(
                        &bound.fixture.target,
                        identity()?,
                        LeaseRequest {
                            check: bound.session.check.clone(),
                            lease_ms: DEFAULT_LEASE_MS,
                        },
                    )
                    .await?;
                PreparationDenial::Stale
            }
        };
        bound.coordinator.recover(&ticket).await?;
        let PublicationState::Finished(Err(error)) =
            timeout(Duration::from_secs(10), ticket.wait()).await?
        else {
            return Err("absent bound checkpoint accepted".into());
        };
        assert!(
            matches!(&*error, PublicationError::Inputs(InvocationError::Rejected(value)) if value.output == StagingReply::Denied(reason))
        );
        assert!(bound.session.live_lease().is_err());
        let token = bound.proof.token()?;
        let count = bound.fixture.handle.query(0,32,move |conn| { Ok(conn.query_row("SELECT count(*) FROM catalog_leases WHERE incarnation=?1 AND admission_sequence=?2 AND input_checkpoint IS NOT NULL",rusqlite::params![token.owner.incarnation.as_bytes().as_slice(),token.attempt as i64],|row|row.get::<_,i64>(0))?.to_be_bytes().to_vec()) }).await?;
        assert_eq!(count.as_slice(), 0i64.to_be_bytes());
        assert!(bound.coordinator.close_and_drain().await.is_empty());
        bound.fixture.runtime.shutdown().await?;
    }
    Ok(())
}
#[tokio::test]
async fn bound_checkpoint_real_retained_pair_publishes_after_source_pin_expiry_in_both_formats()
-> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let bound = Bound::new(format, [189; 16], true).await?;
        let ticket = bound.submit(identity()?).await?;
        let registered = registered(timeout(Duration::from_secs(10), ticket.wait()).await?)?;
        assert!(registered.custody.is_ok());
        assert!(bound.coordinator.close_and_drain().await.is_empty());
        let old = bound.prior.token()?;
        mutate(&bound.fixture.handle, format!("UPDATE catalog_leases SET expires_at_ms=0 WHERE incarnation=x'{}' AND admission_sequence={}",hex::encode(old.owner.incarnation.as_bytes()),old.attempt)).await?;
        let input_index = NativeInputIndex::new(bound.store.clone(), format);
        let mut cursor = input_index.cursor(bound.proof.root()?, None)?;
        let native = cursor.next().await?.ok_or("retained native input")?;
        assert!(cursor.next().await?.is_none());
        let root = tempfile::TempDir::new()?;
        let budget = DiskBudget::new(64 << 20);
        let resources = crate::native_resources::NativeResources::default();
        let mut physical = PhysicalVerifier::download(
            root.path(),
            budget.clone(),
            &bound.store,
            native,
            physical_limits(),
            resources.scope(crate::native_resources::NativeClass::Foreground),
        )
        .await?;
        let segment = physical.inspect_next_shard(native.object_count).await?;
        let tip = segment
            .headers_after(None)?
            .into_iter()
            .find(|h| h.object.kind == crate::ObjectKind::Commit)
            .ok_or("retained tip")?
            .object
            .oid;
        let witness = physical.finish().await?;
        let indexes = Arc::new(CatalogIndexes::new(bound.store.clone(), format));
        let files = Arc::new(CatalogFiles::new(
            root.path(),
            budget.clone(),
            bound.store,
            format,
            CatalogFileLimits::default(),
        )?);
        let base = Arc::new(
            PreparationBaseResolver::open(
                bound.fixture.client(),
                bound.fixture.target.clone(),
                bound.session.check.clone(),
                indexes,
                files,
                Some(registered.registration.receipt),
            )
            .await?,
        );
        let mut preparation =
            CatalogPreparation::new(root.path(), budget.clone(), base, limits()).await?;
        preparation.begin_retained_pack(witness).await?;
        preparation.add_segment(segment).await?;
        preparation.finish_pack().await?;
        let prepared = preparation.finish().await?;
        let publication = prepared
            .ref_proof(
                super::super::publishing::plan(vec![super::super::publishing::update(
                    "refs/heads/main",
                    None,
                    Some(tip),
                )]),
                root.path(),
                budget.clone(),
                limits(),
            )
            .await?;
        let result = bound
            .fixture
            .client()
            .command::<PublishCatalogRefs>(&bound.fixture.target, identity()?, publication)
            .await?;
        assert!(matches!(result.output, PublicationReply::Published(_)));
        bound.fixture.runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn bound_checkpoint_shares_push_actor_quotas_and_exact_mixed_byte_admission() -> Result {
    use super::super::coordinator::{empty_in_store, finished, refused, request};
    use super::super::prepare::cleaned;
    let mut bound = Bound::new(ObjectFormat::Sha256, [190; 16], false).await?;
    assert!(bound.coordinator.close_and_drain().await.is_empty());
    bound.coordinator = PublicationCoordinator::new(
        bound.fixture.target.clone(),
        PublicationLimits {
            operations: 6,
            per_actor: 2,
            command_bytes: (24 << 20) + (16 << 10),
            in_flight: 1,
            maintenance_operations: 1,
            maintenance_in_flight: 1,
            foreground_burst: 3,
        },
    )?;
    mutate(&bound.fixture.handle, "INSERT INTO repository_members VALUES('writer','write'); INSERT INTO repository_members VALUES('third','write')".into()).await?;
    let (release, entered) = bound.coordinator.pause_for_test().await;
    let input = bound.submit(identity()?).await?;
    timeout(Duration::from_secs(5), entered).await??;
    let mut pushes = Vec::new();
    for (operation, actor) in [
        (191, "owner"),
        (192, "owner"),
        (193, "writer"),
        (194, "writer"),
        (195, "third"),
    ] {
        let (prepared, root, budget) =
            empty_in_store(&bound.fixture, [operation; 16], actor, bound.store.clone()).await?;
        let ready = prepared
            .ready_push(
                identity()?,
                request(refused()),
                root.path(),
                budget.clone(),
                limits(),
            )
            .await?;
        pushes.push((prepared, root, budget, Some(ReadyPublication::from(ready))));
    }
    let owner = bound
        .coordinator
        .submit(pushes[0].3.take().ok_or("owner ready")?)
        .await?;
    let refused_owner = bound
        .coordinator
        .submit(pushes[1].3.take().ok_or("quota ready")?)
        .await
        .err()
        .ok_or("owner quota bypassed")?;
    assert_eq!(refused_owner.reason, PublicationScheduleError::Capacity);
    pushes[1].3 = Some(refused_owner.ready);
    let writer_a = bound
        .coordinator
        .submit(pushes[2].3.take().ok_or("writer ready")?)
        .await?;
    let writer_b = bound
        .coordinator
        .submit(pushes[3].3.take().ok_or("writer ready")?)
        .await?;
    let bytes = bound
        .coordinator
        .submit(pushes[4].3.take().ok_or("third ready")?)
        .await
        .err()
        .ok_or("mixed byte quota bypassed")?;
    assert_eq!(bytes.reason, PublicationScheduleError::Capacity);
    pushes[4].3 = Some(bytes.ready);
    // One 8 KiB checkpoint plus three 8 MiB push commands. Another account
    // still has an operation slot, but no byte credit for its full command.
    assert_eq!(
        bound.coordinator.reservations_for_test().await,
        (4, (24 << 20) + 8192, 2)
    );
    release
        .send(())
        .map_err(|_| "mixed dispatch worker stopped")?;
    assert!(
        registered(timeout(Duration::from_secs(10), input.wait()).await?)?
            .custody
            .is_ok()
    );
    for ticket in [owner, writer_a, writer_b] {
        finished(timeout(Duration::from_secs(10), ticket.wait()).await?)?;
        assert_eq!(ticket.response().await?, refused());
    }
    assert_eq!(bound.coordinator.reservations_for_test().await, (0, 0, 0));
    let replay = bound
        .coordinator
        .submit(pushes[1].3.take().ok_or("retained owner ready")?)
        .await?;
    finished(timeout(Duration::from_secs(10), replay.wait()).await?)?;
    assert!(bound.coordinator.close_and_drain().await.is_empty());
    for (prepared, root, budget, ready) in pushes {
        drop(ready);
        drop(prepared);
        cleaned(root.path(), &budget).await?;
    }
    bound.fixture.runtime.shutdown().await?;
    Ok(())
}
