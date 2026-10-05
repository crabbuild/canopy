//! Accepted ownership survives actual command, lease and observer boundaries.
use super::*;
use cellule_runtime::Resolution;
mod restarts;

fn queue(f: &Fixture) -> Result<PublicationCoordinator> {
    Ok(PublicationCoordinator::new(
        f.target.clone(),
        PublicationLimits::default(),
        f.publication_budget.clone(),
    )?)
}
fn input(f: &Fixture, reader: u8, actor: &str, lease_ms: u64) -> BeginRequest {
    let mut input = f.begin([reader; 16]);
    input.actor = actor.into();
    input.lease_ms = lease_ms;
    input
}
fn shared_context(
    f: &Fixture,
    store: Arc<ArtifactStore>,
    root: &tempfile::TempDir,
    budget: ServingReadBudget,
) -> Result<ServingContext> {
    Ok(ServingContext::new(
        f.client(),
        f.target.clone(),
        f.authority(),
        Arc::new(CatalogIndexes::new(store.clone(), f.format)),
        Arc::new(CatalogFiles::new(
            root.path(),
            DiskBudget::new(64 << 20),
            store,
            f.format,
            CatalogFileLimits::default(),
        )?),
        budget,
        "owner".into(),
    )?)
}
async fn ready(owner: &ServingOwner) -> Result<ServingOwnerStats> {
    Ok(timeout(Duration::from_secs(8), async {
        loop {
            let stats = owner.stats();
            if stats.phase == ServingOwnerPhase::Ready {
                break stats;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .map_err(|error| format!("serving owner readiness: {error}; {:?}", owner.stats()))?)
}
async fn zero_pins(f: &Fixture) -> Result {
    timeout(Duration::from_secs(8), async {
        while pin_count(f).await.unwrap() != 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    Ok(())
}
async fn command(ticket: &PublicationTicket) -> Result<Committed<ServingReply>> {
    match timeout(Duration::from_secs(8), ticket.wait()).await? {
        PublicationState::Finished(Ok(PublicationOutcome::ServingCommand(value))) => Ok(value),
        state => Err(format!("serving command {state:?}").into()),
    }
}
async fn original(
    f: &Fixture,
    reader: u8,
    actor: &str,
    lease_ms: u64,
) -> Result<ReadyServingCommand> {
    Ok(ReadyServingCommand::acquire(
        f.client(),
        f.target.clone(),
        input(f, reader, actor, lease_ms),
        identity()?,
        f.authority(),
    )
    .await?)
}

#[tokio::test]
async fn accepted_local_handoff_retains_expired_revoked_grant_without_granting_read() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let f = Fixture::new(format).await?;
        let store = Arc::new(ArtifactStore::new(Arc::new(InMemory::new()), f.repository));
        let fact = initialize(&f, store.clone()).await?;
        edit(
            &f,
            "INSERT INTO repository_members(account,role) VALUES('viewer','read')",
        )
        .await?;
        let q = queue(&f)?;
        let request = original(&f, 212, "viewer", 1_000).await?;
        let grant = granted(
            command(&q.submit(request.dispatch_copy()).await?)
                .await?
                .output,
        )?;
        edit(&f, "DELETE FROM repository_members WHERE account='viewer'").await?;
        tokio::time::sleep(Duration::from_millis(1_100)).await;
        let root = tempfile::TempDir::new()?;
        let tasks = TaskTracker::new();
        let budget = ServingReadBudget::new(4, tasks.clone())?;
        budget.close();
        let pin = request
            .retain_acquisition(shared_context(&f, store.clone(), &root, budget)?)
            .await?;
        assert_eq!(pin.token(), grant.token);
        assert_eq!(pin.fact(), fact);
        assert!(matches!(
            pin.headers(Some("viewer".into()), &[missing(&f)?]).await,
            Err(ServingReadError::Inactive)
        ));
        assert_eq!(pin_count(&f).await?, 1);
        assert_eq!(
            release(&f, &pin).await?.output,
            ServingReleaseReply::Released
        );
        assert!(
            request
                .retain_acquisition(context(&f, store, &root, tasks.clone())?)
                .await
                .is_err()
        );
        assert!(q.close_and_drain().await.is_empty());
        tasks.close();
        tasks.wait().await;
        f.runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn missing_denied_and_restored_originals_cannot_mint_physical_handoff() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let f = Fixture::new(format).await?;
        let store = Arc::new(ArtifactStore::new(Arc::new(InMemory::new()), f.repository));
        initialize(&f, store.clone()).await?;
        let q = queue(&f)?;
        let root = tempfile::TempDir::new()?;
        let tasks = TaskTracker::new();
        let pending = original(&f, 214, "owner", DEFAULT_LEASE_MS).await?;
        assert!(
            pending
                .retain_acquisition(context(&f, store.clone(), &root, tasks.clone())?)
                .await
                .is_err()
        );
        assert_eq!(pin_count(&f).await?, 0);
        q.fault_for_test(1);
        let ticket = q.submit(pending.dispatch_copy()).await?;
        assert!(matches!(
            ticket.wait().await,
            PublicationState::Uncertain(_)
        ));
        assert!(
            pending
                .retain_acquisition(context(&f, store.clone(), &root, tasks.clone())?)
                .await
                .is_err()
        );
        assert_eq!(pin_count(&f).await?, 0);
        ticket.recover().await?;
        let accepted = command(&ticket).await?;
        let restored =
            ReadyServingCommand::restore(f.client(), f.target.clone(), [214; 16], f.authority())
                .await?;
        assert_eq!(restored.evidence(), pending.evidence());
        assert!(matches!(
            restored
                .retain_acquisition(context(&f, store.clone(), &root, tasks.clone())?)
                .await,
            Err(ServingReadError::Context)
        ));
        let pin = pending
            .retain_acquisition(context(&f, store.clone(), &root, tasks.clone())?)
            .await?;
        assert_eq!(pin.token(), granted(accepted.output)?.token);
        assert!(matches!(
            pending
                .retain_acquisition(context(&f, store.clone(), &root, tasks.clone())?)
                .await,
            Err(ServingReadError::AlreadyOwned)
        ));
        let denied = original(&f, 215, "other", DEFAULT_LEASE_MS).await?;
        assert!(matches!(
            q.submit(denied.dispatch_copy()).await?.wait().await,
            PublicationState::Finished(Err(_))
        ));
        assert!(
            denied
                .retain_acquisition(context(&f, store, &root, tasks.clone())?)
                .await
                .is_err()
        );
        assert_eq!(pin_count(&f).await?, 1);
        release(&f, &pin).await?;
        assert!(q.close_and_drain().await.is_empty());
        tasks.close();
        tasks.wait().await;
        f.runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn handoff_uses_original_acquisition_ordinal_after_a_later_renewal() -> Result {
    let f = Fixture::new(ObjectFormat::Sha256).await?;
    let store = Arc::new(ArtifactStore::new(Arc::new(InMemory::new()), f.repository));
    let fact = initialize(&f, store.clone()).await?;
    let q = queue(&f)?;
    let root = tempfile::TempDir::new()?;
    let tasks = TaskTracker::new();
    let original = original(&f, 216, "owner", DEFAULT_LEASE_MS).await?;
    let accepted = command(&q.submit(original.dispatch_copy()).await?).await?;
    let pin = original
        .retain_acquisition(context(&f, store.clone(), &root, tasks.clone())?)
        .await?;
    let renewal = pin
        .ready_renew(
            "owner".into(),
            f.begin([216; 16]).request_digest,
            identity()?,
            DEFAULT_LEASE_MS,
        )
        .await?;
    let renewed = command(&q.submit(renewal.dispatch_copy()).await?).await?;
    assert!(renewed.receipt.commit_sequence > accepted.receipt.commit_sequence);
    assert!(matches!(
        renewal
            .retain_acquisition(context(&f, store.clone(), &root, tasks.clone())?)
            .await,
        Err(ServingReadError::Context)
    ));
    drop(renewal);
    drop(pin);
    let retained = original
        .retain_acquisition(context(&f, store, &root, tasks.clone())?)
        .await?;
    assert_eq!(
        retained.token().admission_sequence,
        accepted.receipt.commit_sequence
    );
    assert_eq!(retained.fact(), fact);
    release(&f, &retained).await?;
    assert!(q.close_and_drain().await.is_empty());
    tasks.close();
    tasks.wait().await;
    f.runtime.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn automatic_owner_recovers_all_six_fault_modes_after_snapshot_observer_loss() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        for fault in 1..=6 {
            let f = Fixture::new(format).await?;
            let store = Arc::new(ArtifactStore::new(Arc::new(InMemory::new()), f.repository));
            initialize(&f, store.clone()).await?;
            let q = queue(&f)?;
            let root = tempfile::TempDir::new()?;
            let tasks = TaskTracker::new();
            let (dispatch, entered) = q.pause_for_test().await;
            q.fault_for_test(fault);
            let owner = ServingOwner::start(
                context(&f, store, &root, tasks.clone())?,
                q.clone(),
                input(&f, 217, "owner", DEFAULT_LEASE_MS),
                identity()?,
            )
            .await?;
            let observed = owner.clone();
            let observer =
                tokio::spawn(async move { observed.snapshot(Some("owner".into())).await });
            timeout(Duration::from_secs(8), entered).await??;
            observer.abort();
            assert!(
                observer
                    .await
                    .err()
                    .ok_or("observer completed")?
                    .is_cancelled()
            );
            dispatch.send(()).map_err(|_| "dispatcher lost")?;
            let stats = ready(&owner).await?;
            assert_eq!(stats.token.ok_or("token")?.reader, [217; 16]);
            let saved = RegisteredCustody::load_for(
                &f.client(),
                &f.target,
                CustodyPurpose::Serving,
                [217; 16],
            )
            .await?
            .ok_or("original missing")?;
            assert_eq!(
                saved
                    .recover_serving(&f.client())
                    .await?
                    .receipt
                    .commit_sequence,
                stats.token.unwrap().admission_sequence
            );
            let snapshot = owner.snapshot(Some("owner".into())).await?;
            assert_eq!(snapshot.headers(&[missing(&f)?]).await?, vec![None]);
            drop(snapshot);
            assert_eq!(
                timeout(Duration::from_secs(8), owner.close_and_drain())
                    .await?
                    .phase,
                ServingOwnerPhase::Released
            );
            assert_eq!(pin_count(&f).await?, 0);
            assert_eq!(q.stats().await.command_bytes, 0);
            assert!(q.close_and_drain().await.is_empty());
            tasks.close();
            tasks.wait().await;
            f.runtime.shutdown().await?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn closed_owner_keeps_borrowed_generation_renewing_until_last_snapshot_clone_drops() -> Result
{
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let f = Fixture::new(format).await?;
        let store = Arc::new(ArtifactStore::new(Arc::new(InMemory::new()), f.repository));
        let fact = initialize(&f, store.clone()).await?;
        let q = queue(&f)?;
        let root = tempfile::TempDir::new()?;
        let tasks = TaskTracker::new();
        let owner = ServingOwner::start(
            context(&f, store, &root, tasks.clone())?,
            q.clone(),
            input(&f, 218, "owner", RENEWAL_LEASE_MS),
            identity()?,
        )
        .await?;
        ready(&owner).await?;
        let snapshot = owner.snapshot(Some("owner".into())).await?;
        let clone = snapshot.clone();
        let token = owner.stats().token.ok_or("token")?;
        owner.close();
        assert!(matches!(
            owner.snapshot(Some("owner".into())).await,
            Err(ServingReadError::Inactive)
        ));
        tokio::time::sleep(Duration::from_millis(RENEWAL_LEASE_MS * 3 / 2)).await;
        timeout(Duration::from_secs(8), async {
            while owner.stats().renewals < 2 {
                assert_eq!(
                    owner.stats().phase,
                    ServingOwnerPhase::Ready,
                    "{:?}",
                    owner.stats()
                );
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .map_err(|error| {
            format!(
                "{format:?}: borrowed-snapshot renewals: {error}; {:?}",
                owner.stats()
            )
        })?;
        assert!(owner.stats().renewals >= 2, "{:?}", owner.stats());
        assert_eq!(owner.stats().token, Some(token));
        assert_eq!(snapshot.fact(), fact);
        assert_eq!(snapshot.headers(&[missing(&f)?]).await?, vec![None]);
        drop(snapshot);
        assert_eq!(pin_count(&f).await?, 1);
        drop(clone);
        assert_eq!(
            timeout(Duration::from_secs(8), owner.close_and_drain())
                .await
                .map_err(|error| format!(
                    "{format:?}: last snapshot drain: {error}; {:?}",
                    owner.stats()
                ))?
                .phase,
            ServingOwnerPhase::Released
        );
        assert_eq!(pin_count(&f).await?, 0);
        assert!(q.close_and_drain().await.is_empty());
        tasks.close();
        tasks.wait().await;
        f.runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn last_handle_drop_drains_after_borrow_and_joins_uncertain_release() -> Result {
    let f = Fixture::new(ObjectFormat::Sha256).await?;
    let store = Arc::new(ArtifactStore::new(Arc::new(InMemory::new()), f.repository));
    initialize(&f, store.clone()).await?;
    let q = queue(&f)?;
    let root = tempfile::TempDir::new()?;
    let tasks = TaskTracker::new();
    let owner = ServingOwner::start(
        context(&f, store, &root, tasks.clone())?,
        q.clone(),
        input(&f, 219, "owner", DEFAULT_LEASE_MS),
        identity()?,
    )
    .await?;
    ready(&owner).await?;
    let snapshot = owner.snapshot(Some("owner".into())).await?;
    let drained = owner.drain_observer();
    drop(owner);
    assert_eq!(pin_count(&f).await?, 1);
    assert_eq!(snapshot.headers(&[missing(&f)?]).await?, vec![None]);
    q.fault_for_test(3);
    drop(snapshot);
    assert_eq!(
        timeout(Duration::from_secs(8), drained.wait()).await?.phase,
        ServingOwnerPhase::Released
    );
    zero_pins(&f).await?;
    assert!(q.close_and_drain().await.is_empty());
    tasks.close();
    tasks.wait().await;
    f.runtime.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn denied_release_stays_owned_until_current_admin_can_authentically_release() -> Result {
    let f = Fixture::new(ObjectFormat::Sha256).await?;
    let store = Arc::new(ArtifactStore::new(Arc::new(InMemory::new()), f.repository));
    initialize(&f, store.clone()).await?;
    let q = queue(&f)?;
    let root = tempfile::TempDir::new()?;
    let tasks = TaskTracker::new();
    let owner = ServingOwner::start(
        context(&f, store, &root, tasks.clone())?,
        q.clone(),
        input(&f, 220, "owner", DEFAULT_LEASE_MS),
        identity()?,
    )
    .await?;
    ready(&owner).await?;
    let (dispatch, entered) = q.pause_for_test().await;
    owner.close();
    timeout(Duration::from_secs(8), entered).await??;
    edit(&f, "UPDATE repository_identity SET owner='other'").await?;
    dispatch.send(()).map_err(|_| "dispatcher lost")?;
    timeout(Duration::from_secs(8), async {
        while owner.stats().last_error.is_none() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    assert_eq!(pin_count(&f).await?, 1);
    let closing = owner.clone();
    let observer = tokio::spawn(async move { closing.close_and_drain().await });
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!observer.is_finished());
    observer.abort();
    assert!(observer.await.err().ok_or("closed early")?.is_cancelled());
    edit(&f, "UPDATE repository_identity SET owner='owner'").await?;
    assert_eq!(
        timeout(Duration::from_secs(8), owner.close_and_drain())
            .await?
            .phase,
        ServingOwnerPhase::Released
    );
    assert_eq!(pin_count(&f).await?, 0);
    assert!(q.close_and_drain().await.is_empty());
    tasks.close();
    tasks.wait().await;
    f.runtime.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn closed_read_budget_refuses_new_work_but_preserves_existing_owner_cleanup() -> Result {
    let f = Fixture::new(ObjectFormat::Sha1).await?;
    let store = Arc::new(ArtifactStore::new(Arc::new(InMemory::new()), f.repository));
    initialize(&f, store.clone()).await?;
    let q = queue(&f)?;
    let root = tempfile::TempDir::new()?;
    let tasks = TaskTracker::new();
    let budget = ServingReadBudget::new(4, tasks.clone())?;
    let context = shared_context(&f, store, &root, budget.clone())?;
    let owner = ServingOwner::start(
        context.clone(),
        q.clone(),
        input(&f, 221, "owner", 1_000),
        identity()?,
    )
    .await?;
    ready(&owner).await?;
    let snapshot = owner.snapshot(Some("owner".into())).await?;
    budget.close();
    owner.close();
    assert!(
        ServingOwner::start(
            context,
            q.clone(),
            input(&f, 222, "owner", DEFAULT_LEASE_MS),
            identity()?
        )
        .await
        .is_err()
    );
    assert!(matches!(
        snapshot.headers(&[missing(&f)?]).await,
        Err(ServingReadError::Inactive)
    ));
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(pin_count(&f).await?, 1);
    drop(snapshot);
    assert_eq!(
        timeout(Duration::from_secs(8), owner.close_and_drain())
            .await?
            .phase,
        ServingOwnerPhase::Released
    );
    assert!(q.close_and_drain().await.is_empty());
    tasks.close();
    tasks.wait().await;
    f.runtime.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn shared_owner_and_snapshot_budgets_preserve_account_shares_and_physical_read_slots()
-> Result {
    let f = Fixture::new(ObjectFormat::Sha256).await?;
    let store = Arc::new(ArtifactStore::new(Arc::new(InMemory::new()), f.repository));
    initialize(&f, store.clone()).await?;
    edit(
        &f,
        "INSERT INTO repository_members(account,role) VALUES('viewer','read')",
    )
    .await?;
    let q = queue(&f)?;
    let root = tempfile::TempDir::new()?;
    let tasks = TaskTracker::new();
    let context = shared_context(&f, store, &root, ServingReadBudget::new(4, tasks.clone())?)?;
    let mut owners = Vec::new();
    for reader in 223..=224 {
        owners.push(
            ServingOwner::start(
                context.clone(),
                q.clone(),
                input(&f, reader, "owner", DEFAULT_LEASE_MS),
                identity()?,
            )
            .await?,
        );
    }
    assert!(
        ServingOwner::start(
            context.clone(),
            q.clone(),
            input(&f, 225, "owner", DEFAULT_LEASE_MS),
            identity()?
        )
        .await
        .is_err()
    );
    owners.push(
        ServingOwner::start(
            context,
            q.clone(),
            input(&f, 226, "viewer", DEFAULT_LEASE_MS),
            identity()?,
        )
        .await?,
    );
    for owner in &owners {
        ready(owner).await?;
    }
    assert_eq!(pin_count(&f).await?, 3);
    let first = owners[0].snapshot(Some("owner".into())).await?;
    let second = owners[0].snapshot(Some("owner".into())).await?;
    assert!(owners[0].snapshot(Some("owner".into())).await.is_err());
    let other = owners[0].snapshot(Some("viewer".into())).await?;
    assert_eq!(first.headers(&[missing(&f)?]).await?, vec![None]);
    assert_eq!(other.headers(&[missing(&f)?]).await?, vec![None]);
    drop(first);
    drop(second);
    drop(other);
    for owner in owners {
        assert_eq!(
            timeout(Duration::from_secs(8), owner.close_and_drain())
                .await?
                .phase,
            ServingOwnerPhase::Released
        );
    }
    assert_eq!(pin_count(&f).await?, 0);
    assert!(q.close_and_drain().await.is_empty());
    tasks.close();
    tasks.wait().await;
    f.runtime.shutdown().await?;
    Ok(())
}
