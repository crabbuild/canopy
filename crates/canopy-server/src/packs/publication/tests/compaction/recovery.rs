use super::*;

#[tokio::test]
async fn compaction_final_validation_rejects_changed_floor_base_and_expired_attempts() -> Result {
    let fixture = Fixture::new(ObjectFormat::Sha256).await?;
    let inventory = seed(&fixture, 2).await?;
    let prepared = prepare_compaction(&fixture, &inventory, 180, &[0, 1]).await?;
    let certificate = prepared.compact.certificate().await?;
    let mut data = certificate.data()?;
    data.retention_floor = 0;
    data.retention_certificate = None;
    reject(
        &fixture,
        CatalogCertificate::seal(&data, &[16; 32])?,
        PreparationDenial::Conflict,
    )
    .await?;
    data = certificate.data()?;
    data.base.certificate = Some([99; 32]);
    reject(
        &fixture,
        CatalogCertificate::seal(&data, &[16; 32])?,
        PreparationDenial::Conflict,
    )
    .await?;
    edit(&fixture,"UPDATE catalog_leases SET expires_at_ms=0 WHERE artifact_operation=(SELECT artifact_operation FROM catalog_operations ORDER BY id DESC LIMIT 1); UPDATE catalog_operations SET expires_at_ms=0 WHERE id=(SELECT id FROM catalog_operations ORDER BY id DESC LIMIT 1);").await?;
    reject(&fixture, certificate, PreparationDenial::Expired).await?;
    assert_eq!(outcomes(&fixture.handle).await?, 0);
    fixture.runtime.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn compaction_ack_survives_owner_restore_and_old_fences_cannot_write() -> Result {
    let fixture = Fixture::new(ObjectFormat::Sha1).await?;
    let inventory = seed(&fixture, 3).await?;
    let first = prepare_compaction(&fixture, &inventory, 180, &[0, 1]).await?;
    let second = prepare_compaction(&fixture, &inventory, 181, &[1, 2]).await?;
    let certificate = first.compact.certificate().await?;
    let pending = second.compact.certificate().await?;
    let mutation = identity()?;
    let committed = fixture
        .client()
        .command::<PublishCatalogCompaction>(&fixture.target, mutation, certificate.clone())
        .await?;
    let before = state(&fixture.handle).await?;
    fixture.handle.drain().await?;
    fixture.runtime.shutdown().await?;
    let session = SessionId::from_bytes([182; 16]);
    let runtime = CellRuntime::new(SqlWorkerPool::new(1, 4)?, 64 << 20, session)?;
    let authority = CellAuthority::new(fixture.layout.clone());
    let idle = authority
        .load(fixture.target.cell_id())
        .await?
        .ok_or("idle")?;
    let provision = CellCatalog::new(fixture.layout.clone(), fixture.target.tenant())
        .lookup(fixture.target.cell_id())
        .await?
        .ok_or("provision")?;
    let handle = runtime
        .acquire_idle_restored(
            provision,
            fixture.replica.clone(),
            authority,
            idle,
            fixture.root.path().join("compaction-restored.sqlite"),
            Owner {
                session,
                endpoint: "https://compaction-owner-b.invalid".into(),
            },
        )
        .await?;
    assert!(handle.owner_fence().epoch > first.compact.token().owner.epoch);
    let client = CellClient::local(Arc::clone(&fixture.registry), handle.clone());
    assert_eq!(state(&handle).await?, before);
    let replay = client
        .command::<PublishCatalogCompaction>(&fixture.target, mutation, certificate.clone())
        .await?;
    assert_eq!(replay.receipt, committed.receipt);
    assert_eq!(replay.output, committed.output);
    assert_eq!(
        client
            .command::<PublishCatalogCompaction>(&fixture.target, identity()?, certificate)
            .await?
            .output,
        committed.output
    );
    assert_eq!(
        client
            .query::<CheckCompletedCompaction>(&fixture.target, None, fixture.begin([180; 16]))
            .await?
            .output,
        Some(committed.output)
    );
    let stale = client
        .command::<PublishCatalogCompaction>(&fixture.target, identity()?, pending)
        .await;
    assert!(
        matches!(stale,Err(InvocationError::Rejected(ref value)) if value.output==CompactionReply::Denied(PreparationDenial::Stale)),
        "{stale:?}"
    );
    assert_eq!(state(&handle).await?, before);
    assert_eq!(outcomes(&handle).await?, 1);
    runtime.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn invalid_selection_and_admission_failure_produce_no_compaction_workspace_or_proof() -> Result
{
    let fixture = Fixture::new(ObjectFormat::Sha256).await?;
    let inventory = seed(&fixture, 2).await?;
    let (base, _, _) = opened(&fixture, [180; 16], Arc::clone(&inventory.store)).await?;
    let root = tempfile::TempDir::new()?;
    for selected in [&[0][..], &[0, 0][..], &[0, 99][..]] {
        let budget = DiskBudget::new(128 << 20);
        assert!(
            PreparedCompaction::prepare(
                root.path(),
                budget.clone(),
                Arc::clone(&base),
                selected,
                CompactionLimits::default()
            )
            .await
            .is_err()
        );
        cleaned(root.path(), &budget).await?;
    }
    for limits in [
        CompactionLimits {
            input_runs: 1,
            spool: limits(),
            ..CompactionLimits::default()
        },
        CompactionLimits {
            input_bytes: 1,
            spool: limits(),
            ..CompactionLimits::default()
        },
    ] {
        let budget = DiskBudget::new(128 << 20);
        assert!(matches!(
            PreparedCompaction::prepare(
                root.path(),
                budget.clone(),
                Arc::clone(&base),
                &[0, 1],
                limits
            )
            .await,
            Err(CatalogPreparationError::Metadata(
                crate::packs::metadata::MetadataError::Limit
            ))
        ));
        cleaned(root.path(), &budget).await?;
    }
    let budget = DiskBudget::new(1);
    assert!(
        PreparedCompaction::prepare(
            root.path(),
            budget.clone(),
            Arc::clone(&base),
            &[0, 1],
            CompactionLimits::default()
        )
        .await
        .is_err()
    );
    cleaned(root.path(), &budget).await?;
    edit(&fixture,"UPDATE repository_identity SET owner='other'; INSERT INTO repository_members VALUES('owner','write');").await?;
    let budget = DiskBudget::new(128 << 20);
    assert!(
        PreparedCompaction::prepare(
            root.path(),
            budget.clone(),
            base,
            &[0, 1],
            CompactionLimits::default()
        )
        .await
        .is_err()
    );
    cleaned(root.path(), &budget).await?;
    assert_eq!(outcomes(&fixture.handle).await?, 0);
    fixture.runtime.shutdown().await?;
    Ok(())
}
