use super::*;

#[tokio::test]
async fn known_registration_denial_closes_without_a_pin_or_namespace_and_returns_owner_admission()
-> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let f = Fixture::new(format).await?;
        let store = Arc::new(ArtifactStore::new(Arc::new(InMemory::new()), f.repository));
        initialize(&f, store.clone()).await?;
        let q = queue(&f)?;
        let root = tempfile::TempDir::new()?;
        let tasks = TaskTracker::new();
        let context = shared_context(&f, store, &root, ServingReadBudget::new(2, tasks.clone())?)?;
        let before = f.counts().await?;
        for reader in 229..=231 {
            let owner = ServingOwner::start(
                context.clone(),
                q.clone(),
                input(&f, reader, "other", DEFAULT_LEASE_MS),
                identity()?,
            )
            .await?;
            let result = timeout(Duration::from_secs(8), owner.drain_observer().wait()).await?;
            assert_eq!(result.phase, ServingOwnerPhase::Denied);
            assert!(result.token.is_none());
            assert_eq!(pin_count(&f).await?, 0);
            assert_eq!(f.counts().await?, before);
            assert!(owner.snapshot(Some("other".into())).await.is_err());
        }
        assert!(q.close_and_drain().await.is_empty());
        tasks.close();
        tasks.wait().await;
        f.runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn lost_acquisition_ack_then_read_revocation_still_hands_off_and_authentically_drains()
-> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let f = Fixture::new(format).await?;
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
        let (dispatch, entered) = q.pause_for_test().await;
        q.fault_for_test(2);
        let owner = ServingOwner::start(
            context(&f, store, &root, tasks.clone())?,
            q.clone(),
            input(&f, 232, "viewer", DEFAULT_LEASE_MS),
            identity()?,
        )
        .await?;
        let (recover, recovering) = owner.pause_recovery_for_test().await;
        timeout(Duration::from_secs(8), entered).await??;
        dispatch.send(()).map_err(|_| "dispatch disappeared")?;
        timeout(Duration::from_secs(8), recovering).await??;
        let ticket = q
            .pending_serving_command([232; 16])
            .await
            .ok_or("owned uncertainty disappeared")?;
        assert!(matches!(ticket.state(), PublicationState::Uncertain(_)));
        assert_eq!(pin_count(&f).await?, 1);
        edit(&f, "DELETE FROM repository_members WHERE account='viewer'").await?;
        recover.send(()).map_err(|_| "recovery owner disappeared")?;
        let result = timeout(Duration::from_secs(8), owner.drain_observer().wait()).await?;
        assert_eq!(result.phase, ServingOwnerPhase::Released);
        assert!(result.token.is_some());
        assert!(matches!(
            ticket.state(),
            PublicationState::Finished(Ok(PublicationOutcome::ServingCommand(_)))
        ));
        assert_eq!(pin_count(&f).await?, 0);
        assert!(owner.snapshot(Some("viewer".into())).await.is_err());
        let saved =
            RegisteredCustody::load_for(&f.client(), &f.target, CustodyPurpose::Serving, [232; 16])
                .await?
                .ok_or("recorded grant lost")?;
        assert!(matches!(
            saved.recover_serving(&f.client()).await?.output,
            ServingReply::Granted(_)
        ));
        assert!(q.close_and_drain().await.is_empty());
        tasks.close();
        tasks.wait().await;
        f.runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn producer_restarts_preserve_factory_identity_held_ticket_capture_renewal_and_release()
-> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        for point in 1..=5 {
            let f = Fixture::new(format).await?;
            let store = Arc::new(ArtifactStore::new(Arc::new(InMemory::new()), f.repository));
            initialize(&f, store.clone()).await?;
            let q = queue(&f)?;
            let root = tempfile::TempDir::new()?;
            let tasks = TaskTracker::new();
            let first = identity()?;
            let owner = ServingOwner::start(
                context(&f, store, &root, tasks.clone())?,
                q.clone(),
                input(&f, 227, "owner", 2_000),
                first,
            )
            .await?;
            // The current-thread runtime cannot run the spawned producer until
            // this caller next yields, after its failure point is configured.
            owner.fault_for_test(point);
            let stats = ready(&owner).await?;
            let token = stats.token.ok_or("token")?;
            if point == 4 {
                timeout(Duration::from_secs(8), async {
                    while owner.stats().renewals == 0 {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                })
                .await?;
            }
            if point <= 3 {
                let saved = RegisteredCustody::load_for(
                    &f.client(),
                    &f.target,
                    CustodyPurpose::Serving,
                    [227; 16],
                )
                .await?
                .ok_or("original")?;
                assert_eq!(saved.evidence().identity().request_id, first.request_id);
                assert!(matches!(
                    f.client().resolve(saved.evidence()).await?,
                    Resolution::Committed(_)
                ));
                assert_eq!(
                    token.admission_sequence,
                    saved
                        .recover_serving(&f.client())
                        .await?
                        .receipt
                        .commit_sequence
                );
            }
            let final_state = timeout(Duration::from_secs(8), owner.close_and_drain()).await?;
            assert_eq!(final_state.phase, ServingOwnerPhase::Released);
            assert!(final_state.retries >= 1, "point {point}: {final_state:?}");
            assert_eq!(final_state.token, Some(token));
            assert_eq!(pin_count(&f).await?, 0);
            assert!(q.close_and_drain().await.is_empty());
            tasks.close();
            tasks.wait().await;
            f.runtime.shutdown().await?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn owner_drain_keeps_cell_root_and_workspace_until_detached_provider_work_finishes() -> Result
{
    use std::sync::atomic::Ordering;
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let f = Fixture::new(format).await?;
        let provider = Arc::new(super::super::blocked::Gate::new());
        let store = Arc::new(ArtifactStore::new(provider.clone(), f.repository));
        initialize(&f, store.clone()).await?;
        let q = queue(&f)?;
        let root = tempfile::TempDir::new()?;
        let tasks = TaskTracker::new();
        let owner = ServingOwner::start(
            context(&f, store, &root, tasks.clone())?,
            q.clone(),
            input(&f, 228, "owner", DEFAULT_LEASE_MS),
            identity()?,
        )
        .await?;
        ready(&owner).await?;
        let snapshot = owner.snapshot(Some("owner".into())).await?;
        provider.armed.store(true, Ordering::Release);
        let oid = missing(&f)?;
        let observed = tokio::spawn(async move { snapshot.headers(&[oid]).await });
        timeout(Duration::from_secs(8), provider.entered.acquire())
            .await??
            .forget();
        observed.abort();
        assert!(
            observed
                .await
                .err()
                .ok_or("provider completed")?
                .is_cancelled()
        );
        let closing = owner.clone();
        let drain = tokio::spawn(async move { closing.close_and_drain().await });
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(!drain.is_finished());
        assert!(!tasks.is_empty());
        assert_eq!(pin_count(&f).await?, 1);
        assert!(root.path().exists());
        assert!(matches!(
            owner.snapshot(Some("owner".into())).await,
            Err(ServingReadError::Inactive)
        ));
        provider.proceed.add_permits(1);
        assert_eq!(
            timeout(Duration::from_secs(8), drain).await??.phase,
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
