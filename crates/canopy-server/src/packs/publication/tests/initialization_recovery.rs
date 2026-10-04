//! Original initialization identity survives real restore and SDK expiry.
use super::*;
use super::{
    initialization::empty,
    prepare::cleaned,
    publishing::{edit, state},
};
use crate::packs::{
    catalog::{CatalogFileLimits, CatalogFiles, CatalogIndexes},
    metadata::tests::limits,
};
use canopy_object_storage::artifact::ArtifactStore;
use cellule_ltx::DiskBudget;
use cellule_runtime::Resolution;
use object_store::{ObjectStore, ObjectStoreExt};
use tokio::time::{Duration, timeout};

#[tokio::test]
async fn lost_initialization_registration_is_discovered_after_fresh_disk_owner_restore() -> Result {
    let f = Fixture::new(ObjectFormat::Sha256).await?;
    let store = Arc::new(ArtifactStore::new(Arc::new(InMemory::new()), f.repository));
    let operation = [233; 16];
    let (prepared, root, budget) = empty(&f, operation, store.clone()).await?;
    let command = f
        .client()
        .prepare_command::<InitializeCatalogRefs>(
            &f.target,
            identity()?,
            prepared.empty_ref_initialization().await?,
        )
        .await?;
    let original = command.evidence().clone();
    let check = check(prepared.token());
    let lost = super::super::recovery::persist(
        &prepared.base.session,
        &command,
        super::super::recovery::Kind::Initialization,
        &store,
        identity()?,
        2,
    )
    .await;
    assert!(
        matches!(lost, Err(RootRecoveryError::Registration(ref error)) if matches!(&**error, InvocationError::Pending(_)))
    );
    assert!(matches!(
        f.client().resolve(&original).await?,
        Resolution::Absent
    ));
    let saved = RegisteredRootRecovery::load_initialization(
        &f.client(),
        &f.target,
        &store,
        &f.begin(operation),
    )
    .await?
    .ok_or("winning initialization registration absent")?;
    assert_eq!(saved.evidence(), &original);
    let rival = f
        .client()
        .prepare_command::<InitializeCatalogRefs>(
            &f.target,
            identity()?,
            prepared.empty_ref_initialization().await?,
        )
        .await?;
    super::mandatory_registration::not_started(&f, &rival).await?;
    let rival_registration = super::super::recovery::persist(
        &prepared.base.session,
        &rival,
        super::super::recovery::Kind::Initialization,
        &store,
        identity()?,
        0,
    )
    .await;
    assert!(
        matches!(rival_registration, Err(RootRecoveryError::Registration(ref error)) if matches!(&**error, InvocationError::Rejected(value) if value.output==RootRecoveryReply::Denied(PreparationDenial::Conflict)))
    );
    let mut wrong = f.begin(operation);
    wrong.request_digest[0] ^= 1;
    assert!(
        RegisteredRootRecovery::load_initialization(&f.client(), &f.target, &store, &wrong)
            .await?
            .is_none()
    );
    drop(rival);
    drop(command);
    drop(saved);
    drop(prepared);
    cleaned(root.path(), &budget).await?;
    let (runtime, handle, client) = super::durable_recovery::restore_owner(&f, &check).await?;
    assert!(!f.root.path().join("a.sqlite").exists());
    let saved = RegisteredRootRecovery::load_initialization(
        &client,
        &f.target,
        &store,
        &f.begin(operation),
    )
    .await?
    .ok_or("restored registration absent")?;
    assert_eq!(saved.evidence(), &original);
    assert!(matches!(
        client.resolve(&original).await?,
        Resolution::Absent
    ));
    let before = state(&handle).await?;
    let denied = match saved
        .recover_initialization(&client, &store, &f.authority())
        .await
    {
        Err(PublicationError::Initialization(InvocationError::Rejected(value))) => value,
        other => {
            return Err(format!("cold original must settle its stale owner: {other:?}").into());
        }
    };
    assert_eq!(
        denied.output,
        InitializationReply::Denied(PreparationDenial::Stale)
    );
    assert_eq!(state(&handle).await?, before);
    assert!(matches!(saved.recover_initialization(&client,
&store,
&f.authority(),).await, Err(PublicationError::Initialization(InvocationError::Rejected(ref value))) if value.receipt==denied.receipt));
    // Only a definitive original denial permits a new owner to claim. It gets
    // its own namespace; the original pin and result remain unchanged.
    let started = client
        .command::<ClaimPreparation>(
            &f.target,
            identity()?,
            LeaseRequest {
                check: check.clone(),
                lease_ms: DEFAULT_LEASE_MS,
            },
        )
        .await?;
    let current = lease(started.output)?;
    assert_eq!(current.token.owner, handle.owner_fence());
    assert_ne!(
        current.token.artifact_operation,
        check.token.artifact_operation
    );
    assert_eq!(current.base.generation, 0);
    let scratch = tempfile::TempDir::new()?;
    let disk = DiskBudget::new(64 << 20);
    let indexes = Arc::new(CatalogIndexes::new(store.clone(), f.format));
    let files = Arc::new(CatalogFiles::new(
        scratch.path(),
        disk.clone(),
        store.clone(),
        f.format,
        CatalogFileLimits::default(),
    )?);
    let base = Arc::new(
        PreparationBaseResolver::open(
            client.clone(),
            f.target.clone(),
            super::check(current.token),
            indexes,
            files,
            Some(started.receipt),
            f.authority(),
        )
        .await?,
    );
    let prepared = Arc::new(
        CatalogPreparation::new(scratch.path(), disk.clone(), base, limits())
            .await?
            .finish()
            .await?,
    );
    let ready = prepared.ready_initialization(identity()?).await?;
    let registered = ready.persist_recovery(&store, identity()?).await?;
    let result = ready.complete(&registered, &store).await?;
    assert!(
        matches!(result.output, InitializationReply::Initialized(ref fact) if fact.generation==1)
    );
    assert!(matches!(saved.recover_initialization(&client,
&store,
&f.authority(),).await, Err(PublicationError::Initialization(InvocationError::Rejected(ref value))) if value.receipt==denied.receipt));
    handle
        .query(0, 128, |db| {
            assert_eq!(
                db.query_row(
                    "SELECT artifact_sequence FROM repository_identity",
                    [],
                    |r| r.get::<_, u64>(0)
                )?,
                2
            );
            assert_eq!(
                db.query_row("SELECT count(*) FROM catalog_initialization", [], |r| r
                    .get::<_, u64>(0))?,
                1
            );
            assert_eq!(
                db.query_row(
                    "SELECT count(*) FROM catalog_leases WHERE recovery_phase IS NOT NULL",
                    [],
                    |r| r.get::<_, u64>(0)
                )?,
                2
            );
            Ok(Vec::new())
        })
        .await?;
    drop(prepared);
    cleaned(scratch.path(), &disk).await?;
    runtime.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn original_initialization_receipt_survives_lost_ack_expiry_body_loss_and_owner_restore()
-> Result {
    let f = Fixture::new(ObjectFormat::Sha1).await?;
    let provider: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let store = Arc::new(ArtifactStore::new(provider.clone(), f.repository));
    let (prepared, root, budget) = empty(&f, [234; 16], store.clone()).await?;
    let prepared = Arc::new(prepared);
    let mut mutation = identity()?;
    mutation.expires_at_ms = mutation.issued_at_ms + 8_000;
    let ready = prepared.ready_initialization(mutation).await?;
    let registered = ready.persist_recovery(&store, identity()?).await?;
    let original = registered.evidence().clone();
    let check = check(registered.token());
    let bound = ready.bind_recovery(registered.clone(), &store)?;
    let queue = PublicationCoordinator::new(f.target.clone(), PublicationLimits::default())?;
    queue.fault_for_test(2);
    let observer = queue
        .submit(bound)
        .await
        .map_err(|failure| format!("initialization admission: {:?}", failure.reason))?;
    assert!(
        matches!(timeout(Duration::from_secs(10), observer.wait()).await?, PublicationState::Uncertain(ref error) if matches!(&**error, PublicationError::Initialization(InvocationError::Pending(evidence)) if **evidence==original))
    );
    assert_eq!(queue.stats().await.command_bytes, 20 << 10);
    assert_eq!(queue.close_and_drain().await.len(), 1);
    observer.recover().await?;
    let expected = match timeout(Duration::from_secs(10), observer.wait()).await? {
        PublicationState::Finished(Ok(PublicationOutcome::Initialization(value))) => value,
        other => return Err(format!("original initialization recovery: {other:?}").into()),
    };
    assert!(matches!(
        expected.output,
        InitializationReply::Initialized(_)
    ));
    assert_eq!(queue.stats().await.command_bytes, 0);
    assert_eq!(queue.stats().await.admitted, 0);
    assert!(queue.close_and_drain().await.is_empty());
    drop(observer);
    drop(queue);
    drop(prepared);
    cleaned(root.path(), &budget).await?;
    // Delete the exact saved body manifest and revoke current rights. Original
    // phase knowledge must win before both body I/O and fresh permission checks.
    for (key, descriptor) in registered.command_bodies_for_test() {
        let path = store.path(key, descriptor.digest)?;
        provider.head(&path).await?;
        provider.delete(&path).await?;
        assert!(matches!(
            provider.head(&path).await,
            Err(object_store::Error::NotFound { .. })
        ));
    }
    edit(&f, "UPDATE repository_identity SET owner='replacement'").await?;
    drop(registered);
    let (runtime, handle, client) = super::durable_recovery::restore_owner(&f, &check).await?;
    let now = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis())?;
    if now <= mutation.expires_at_ms {
        tokio::time::sleep(Duration::from_millis(u64::try_from(
            mutation.expires_at_ms - now + 1,
        )?))
        .await;
    }
    assert!(matches!(
        client.resolve(&original).await?,
        Resolution::Expired
    ));
    let loaded = RegisteredRootRecovery::load(&client, &f.target, &store, &check)
        .await?
        .ok_or("original initialization pin absent")?;
    assert_eq!(loaded.evidence(), &original);
    let before = state(&handle).await?;
    let result = loaded
        .recover_initialization(&client, &store, &f.authority())
        .await?;
    assert_eq!(
        (result.output, result.receipt),
        (expected.output.clone(), expected.receipt)
    );
    let queue = PublicationCoordinator::new(f.target.clone(), PublicationLimits::default())?;
    let observer = queue
        .submit(loaded.ready(client, (*store).clone(), f.authority())?)
        .await
        .map_err(|failure| format!("cold initialization admission: {:?}", failure.reason))?;
    assert!(
        matches!(timeout(Duration::from_secs(10), observer.wait()).await?, PublicationState::Finished(Ok(PublicationOutcome::Initialization(ref value))) if value.receipt==expected.receipt && value.output==expected.output)
    );
    assert!(queue.close_and_drain().await.is_empty());
    assert_eq!(state(&handle).await?, before);
    runtime.shutdown().await?;
    Ok(())
}
