use super::*;

fn staging(reply: StagingReply) -> Result<StagingLease> {
    match reply {
        StagingReply::Granted(lease) => Ok(*lease),
        StagingReply::Denied(reason) => Err(format!("denied: {reason:?}").into()),
    }
}
fn denied(
    result: std::result::Result<
        cellule_runtime::Committed<StagingReply>,
        InvocationError<StagingReply>,
    >,
    reason: PreparationDenial,
) {
    assert!(
        matches!(result, Err(InvocationError::Rejected(ref value)) if value.output == StagingReply::Denied(reason))
    );
}
async fn start(fixture: &Fixture, operation: [u8; 16]) -> Result<StagingLease> {
    staging(
        fixture
            .client()
            .command::<BeginStaging>(&fixture.target, identity()?, fixture.begin(operation))
            .await?
            .output,
    )
}

#[test]
fn staged_pin_normalized_binding_has_no_nullable_foreign_key_escape() -> Result {
    let mut connection = rusqlite::Connection::open_in_memory()?;
    connection.execute_batch("PRAGMA foreign_keys=ON")?;
    connection.execute_batch(SCHEMA)?;
    connection.execute(
        "INSERT INTO catalog_generations(generation,catalog,certificate) VALUES(1,x'01',zeroblob(32))",
        [],
    )?;
    connection.execute("INSERT INTO catalog_leases(incarnation,admission_sequence,operation,owner_epoch,artifact_operation,generation,expires_at_ms) VALUES(zeroblob(16),1,zeroblob(16),x'0000000000000001',?1,NULL,100)", [artifact_number(1).as_slice()])?;
    for (logical, epoch, sequence, namespace, floor, expires) in [
        ([1u8; 16], 1u64, 1, artifact_number(1), None, 100),
        ([0u8; 16], 2u64, 1, artifact_number(1), None, 100),
        ([0u8; 16], 1u64, 2, artifact_number(1), None, 100),
        ([0u8; 16], 1u64, 1, artifact_number(2), None, 100),
        ([0u8; 16], 1u64, 1, artifact_number(1), Some(0), 100),
        ([0u8; 16], 1u64, 1, artifact_number(1), None, 101),
    ] {
        let tx = connection.transaction()?;
        tx.execute("INSERT INTO catalog_operations(id,actor,request_digest,incarnation,owner_epoch,admission_sequence,artifact_operation,generation,expires_at_ms) VALUES(?1,'owner',zeroblob(32),zeroblob(16),?2,?3,?4,?5,?6)", rusqlite::params![logical.as_slice(),epoch.to_be_bytes().as_slice(),sequence,namespace.as_slice(),floor,expires])?;
        assert!(tx.commit().is_err());
    }
    connection.execute("INSERT INTO catalog_operations(id,actor,request_digest,incarnation,owner_epoch,admission_sequence,artifact_operation,generation,expires_at_ms) VALUES(zeroblob(16),'owner',zeroblob(32),zeroblob(16),x'0000000000000001',1,?1,NULL,100)", [artifact_number(1).as_slice()])?;
    assert!(
        connection
            .execute(
                "UPDATE catalog_leases SET attestation=x'01',attestation_digest=zeroblob(32)",
                []
            )
            .is_err()
    );
    assert!(
        connection
            .execute(
                "UPDATE catalog_operations SET attestation=x'01',attestation_digest=zeroblob(32)",
                []
            )
            .is_err()
    );
    let tx = connection.transaction()?;
    tx.execute("UPDATE catalog_leases SET generation=1", [])?;
    assert!(tx.commit().is_err()); // both sides must bind in one transaction
    assert_eq!(
        connection.query_row("SELECT binding_generation FROM catalog_leases", [], |r| r
            .get::<_, i64>(
            0
        ))?,
        -1
    );
    let tx = connection.transaction()?;
    tx.execute("UPDATE catalog_leases SET generation=1", [])?;
    tx.execute("UPDATE catalog_operations SET generation=1", [])?;
    tx.commit()?;
    for sql in [
        "UPDATE catalog_leases SET generation=NULL",
        "UPDATE catalog_leases SET generation=0",
        "UPDATE catalog_leases SET artifact_operation=randomblob(16)",
    ] {
        assert!(connection.execute(sql, []).is_err());
    }
    assert!(
        connection
            .execute("DELETE FROM catalog_generations WHERE generation=1", [])
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn staging_exact_replay_late_bind_and_phase_separation_preserve_one_namespace() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let fixture = Fixture::new(format).await?;
        let client = fixture.client();
        let mutation = identity()?;
        let input = fixture.begin([201; 16]);
        let first = client
            .command::<BeginStaging>(&fixture.target, mutation, input.clone())
            .await?;
        let staged = staging(first.output.clone())?;
        let replay = client
            .command::<BeginStaging>(&fixture.target, mutation, input.clone())
            .await?;
        assert_eq!(replay.receipt, first.receipt);
        assert_eq!(replay.output, first.output);
        assert_eq!(
            staging(
                client
                    .command::<BeginStaging>(&fixture.target, identity()?, input.clone())
                    .await?
                    .output
            )?
            .token,
            staged.token
        );
        assert_eq!(fixture.counts().await?, (1, 1));
        assert!(
            client
                .query::<CheckPreparation>(&fixture.target, None, check(staged.token))
                .await?
                .output
                .is_none()
        );
        assert!(
            client
                .query::<CheckPreparationFrontier>(&fixture.target, None, check(staged.token))
                .await?
                .output
                .is_none()
        );
        for reply in [
            client
                .command::<BeginPreparation>(&fixture.target, identity()?, input.clone())
                .await,
            client
                .command::<RenewPreparation>(&fixture.target, identity()?, request(staged.token))
                .await,
            client
                .command::<ClaimPreparation>(&fixture.target, identity()?, request(staged.token))
                .await,
        ] {
            rejected(reply, PreparationDenial::Conflict);
        }
        let renewed = staging(
            client
                .command::<RenewStaging>(&fixture.target, identity()?, request(staged.token))
                .await?
                .output,
        )?;
        assert_eq!(renewed.token, staged.token);
        assert!(renewed.expires_at_ms >= staged.expires_at_ms);
        fixture.install_empty_root(1).await?;
        fixture.install_empty_root(2).await?;
        assert_eq!(super::frontier::reap(&fixture).await?, 1);
        assert_eq!(
            super::frontier::facts(&fixture).await?,
            super::frontier::expected(&[0, 2])
        );
        let bind_identity = identity()?;
        let bound = client
            .command::<BindStaging>(&fixture.target, bind_identity, check(staged.token))
            .await?;
        let active = lease(bound.output.clone())?;
        assert_eq!(active.token, staged.token);
        assert_eq!(active.base.generation, 2);
        assert_eq!(active.expires_at_ms, renewed.expires_at_ms);
        fixture.install_empty_root(3).await?;
        let replay = client
            .command::<BindStaging>(&fixture.target, bind_identity, check(staged.token))
            .await?;
        assert_eq!(replay.receipt, bound.receipt);
        assert_eq!(replay.output, bound.output);
        let again = lease(
            client
                .command::<BindStaging>(&fixture.target, identity()?, check(staged.token))
                .await?
                .output,
        )?;
        assert_eq!(again.base, active.base);
        assert_eq!(again.expires_at_ms, active.expires_at_ms);
        assert!(
            client
                .query::<CheckStaging>(&fixture.target, None, check(staged.token))
                .await?
                .output
                .is_none()
        );
        for reply in [
            client
                .command::<BeginStaging>(&fixture.target, identity()?, input)
                .await,
            client
                .command::<RenewStaging>(&fixture.target, identity()?, request(staged.token))
                .await,
            client
                .command::<ClaimStaging>(&fixture.target, identity()?, request(staged.token))
                .await,
        ] {
            denied(reply, PreparationDenial::Conflict);
        }
        assert_eq!(super::frontier::reap(&fixture).await?, 0);
        assert_eq!(fixture.counts().await?, (1, 1));
        fixture.runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn staged_input_does_not_accumulate_generation_history_beyond_fact_capacity() -> Result {
    let fixture = Fixture::new(ObjectFormat::Sha256).await?;
    let staged = start(&fixture, [202; 16]).await?;
    let catalog = fixture.install_empty_root(1).await?;
    let mut e = BoundedEncoder::new(256)?;
    catalog.encode(&mut e)?;
    let bytes = e.finish();
    // Trusted catalog injection exercises retention, not publication throughput.
    // Use bounded batches; the production reaper runs between every batch.
    for first in (2..=10_241).step_by(512) {
        let bytes = bytes.clone();
        fixture
            .handle
            .execute(
                identity()?,
                Digest::from_bytes([202; 32]),
                sql::now(0)?,
                1,
                0,
                move |tx| {
                    let mut insert =
                        tx.prepare("INSERT INTO catalog_generations(generation,catalog,certificate) VALUES(?1,?2,?3)")?;
                    for n in first..first + 512 {
                        insert.execute(rusqlite::params![n, bytes, [42u8; 32].as_slice()])?;
                    }
                    drop(insert);
                    tx.execute("UPDATE catalog_state SET generation=?1", [first + 511])?;
                    Ok(cellule_runtime::cell::executor::HandlerOutcome::Success(
                        Vec::new(),
                    ))
                },
            )
            .await?;
        assert_eq!(super::frontier::reap(&fixture).await?, 512);
        assert_eq!(
            super::frontier::facts(&fixture).await?,
            super::frontier::expected(&[0, first as u64 + 511])
        );
        let renewed = staging(
            fixture
                .client()
                .command::<RenewStaging>(&fixture.target, identity()?, request(staged.token))
                .await?
                .output,
        )?;
        assert_eq!(renewed.token, staged.token);
    }
    let bound = lease(
        fixture
            .client()
            .command::<BindStaging>(&fixture.target, identity()?, check(staged.token))
            .await?
            .output,
    )?;
    assert_eq!(bound.base.generation, 10_241);
    assert_eq!(fixture.counts().await?, (1, 1));
    fixture.runtime.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn staging_claim_abort_and_stale_owner_do_not_reuse_or_drop_old_input_custody() -> Result {
    let fixture = Fixture::new(ObjectFormat::Sha256).await?;
    let old = start(&fixture, [203; 16]).await?;
    let client = fixture.client();
    let mut wrong = check(old.token);
    wrong.token.owner.epoch += 1;
    rejected(
        client
            .command::<BindStaging>(&fixture.target, identity()?, wrong.clone())
            .await,
        PreparationDenial::Stale,
    );
    denied(
        client
            .command::<RenewStaging>(
                &fixture.target,
                identity()?,
                LeaseRequest {
                    check: wrong,
                    lease_ms: DEFAULT_LEASE_MS,
                },
            )
            .await,
        PreparationDenial::Stale,
    );
    let next = staging(
        client
            .command::<ClaimStaging>(&fixture.target, identity()?, request(old.token))
            .await?
            .output,
    )?;
    assert_ne!(next.token.artifact_operation, old.token.artifact_operation);
    assert_eq!(next.token.artifact_operation, artifact_number(2));
    assert_eq!(fixture.counts().await?, (1, 2));
    rejected(
        client
            .command::<BindStaging>(&fixture.target, identity()?, check(old.token))
            .await,
        PreparationDenial::Stale,
    );
    assert!(
        client
            .query::<CheckStaging>(&fixture.target, None, check(old.token))
            .await?
            .output
            .is_none()
    );
    client
        .command::<AbortPreparation>(&fixture.target, identity()?, check(next.token))
        .await?;
    assert_eq!(fixture.counts().await?, (0, 2));
    assert_eq!(super::frontier::reap(&fixture).await?, 0);
    let recreated = start(&fixture, [203; 16]).await?;
    assert_eq!(recreated.token.artifact_operation, artifact_number(3));
    fixture.runtime.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn physical_inputs_verified_before_binding_feed_the_existing_catalog_proof() -> Result {
    use crate::packs::{
        catalog::{CatalogFileLimits, CatalogFiles, CatalogIndexes},
        metadata::tests::limits,
        verification::physical::tests::prepared_for_store,
    };
    use canopy_object_storage::artifact::ArtifactStore;
    use cellule_ltx::DiskBudget;
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let fixture = Fixture::new(format).await?;
        let staged = start(&fixture, [204; 16]).await?;
        let provider: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let store = Arc::new(ArtifactStore::new(
            Arc::clone(&provider),
            fixture.repository,
        ));
        let native = prepared_for_store(
            format,
            32,
            staged.token.artifact_operation,
            provider,
            Arc::clone(&store),
        )
        .await?;
        let indexes = Arc::new(CatalogIndexes::new(Arc::clone(&store), format));
        let files = Arc::new(CatalogFiles::new(
            fixture.root.path(),
            DiskBudget::new(64 << 20),
            store,
            format,
            CatalogFileLimits::default(),
        )?);
        assert!(matches!(
            PreparationBaseResolver::open(
                fixture.client(),
                fixture.target.clone(),
                check(staged.token),
                Arc::clone(&indexes),
                Arc::clone(&files),
                None
            )
            .await,
            Err(PreparationBaseError::Inactive)
        ));
        let root = tempfile::TempDir::new()?;
        let budget = DiskBudget::new(256 << 20);
        let (witness, segments) =
            super::prepare::physical(&native, root.path(), budget.clone()).await?;
        assert_eq!(fixture.counts().await?, (1, 1));
        let bound = fixture
            .client()
            .command::<BindStaging>(&fixture.target, identity()?, check(staged.token))
            .await?;
        let active = lease(bound.output)?;
        assert_eq!(active.token, staged.token);
        let base = Arc::new(
            PreparationBaseResolver::open(
                fixture.client(),
                fixture.target.clone(),
                check(active.token),
                indexes,
                files,
                Some(bound.receipt),
            )
            .await?,
        );
        let mut assembler =
            CatalogPreparation::new(root.path(), budget.clone(), base, limits()).await?;
        assembler.begin_pack(witness)?;
        for segment in segments {
            assembler.add_segment(segment).await?;
        }
        assembler.finish_pack().await?;
        let proof = assembler.finish().await?;
        assert_eq!(proof.token(), staged.token);
        assert_eq!(proof.object_count(), native.fixture.objects.len() as u64);
        let certificate = proof.certificate().await?;
        assert_eq!(certificate.data()?.retention_floor, active.base.generation);
        assert!(matches!(
            proof.attest(identity()?).await?.output,
            AttestationOutcome::Registered(_)
        ));
        drop(proof);
        super::prepare::cleaned(root.path(), &budget).await?;
        fixture.runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn staging_takeover_restores_exact_outcomes_and_requires_a_new_creating_attempt() -> Result {
    let fixture = Fixture::new(ObjectFormat::Sha1).await?;
    let mutation = identity()?;
    let input = fixture.begin([205; 16]);
    let first = fixture
        .client()
        .command::<BeginStaging>(&fixture.target, mutation, input.clone())
        .await?;
    let old = staging(first.output.clone())?;
    fixture.handle.drain().await?;
    fixture.runtime.shutdown().await?;
    let session = SessionId::from_bytes([205; 16]);
    let runtime = CellRuntime::new(SqlWorkerPool::new(1, 4)?, 64 << 20, session)?;
    let authority = CellAuthority::new(fixture.layout.clone());
    let idle = authority
        .load(fixture.target.cell_id())
        .await?
        .ok_or("idle")?;
    let proof = CellCatalog::new(fixture.layout.clone(), fixture.target.tenant())
        .lookup(fixture.target.cell_id())
        .await?
        .ok_or("proof")?;
    let handle = runtime
        .acquire_idle_restored(
            proof,
            fixture.replica.clone(),
            authority,
            idle,
            fixture.root.path().join("staging-restored.sqlite"),
            Owner {
                session,
                endpoint: "https://staging-successor.invalid".into(),
            },
        )
        .await?;
    let client = CellClient::local(Arc::clone(&fixture.registry), handle.clone());
    let replay = client
        .command::<BeginStaging>(&fixture.target, mutation, input)
        .await?;
    assert_eq!(replay.receipt, first.receipt);
    assert_eq!(replay.output, first.output);
    denied(
        client
            .command::<RenewStaging>(&fixture.target, identity()?, request(old.token))
            .await,
        PreparationDenial::Stale,
    );
    rejected(
        client
            .command::<BindStaging>(&fixture.target, identity()?, check(old.token))
            .await,
        PreparationDenial::Stale,
    );
    let claim_identity = identity()?;
    let claimed = client
        .command::<ClaimStaging>(&fixture.target, claim_identity, request(old.token))
        .await?;
    let next = staging(claimed.output.clone())?;
    assert_eq!(next.token.owner, handle.owner_fence());
    assert_eq!(next.token.artifact_operation, artifact_number(2));
    assert!(next.token.attempt > old.token.attempt);
    let replay = client
        .command::<ClaimStaging>(&fixture.target, claim_identity, request(old.token))
        .await?;
    assert_eq!(replay.receipt, claimed.receipt);
    assert_eq!(replay.output, claimed.output);
    assert_eq!(counts(&handle).await?, (1, 2));
    assert!(
        client
            .query::<CheckStaging>(&fixture.target, None, check(old.token))
            .await?
            .output
            .is_none()
    );
    let active = lease(
        client
            .command::<BindStaging>(&fixture.target, identity()?, check(next.token))
            .await?
            .output,
    )?;
    assert_eq!(active.token, next.token);
    runtime.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn staging_checks_current_access_exact_identity_and_expiry_before_late_binding() -> Result {
    let fixture = Fixture::new(ObjectFormat::Sha256).await?;
    fixture
        .handle
        .execute(
            identity()?,
            Digest::from_bytes([206; 32]),
            sql::now(0)?,
            1,
            0,
            |tx| {
                tx.execute(
                    "INSERT INTO repository_members(account,role) VALUES('writer','write')",
                    [],
                )?;
                Ok(cellule_runtime::cell::executor::HandlerOutcome::Success(
                    Vec::new(),
                ))
            },
        )
        .await?;
    let client = fixture.client();
    let mut input = fixture.begin([206; 16]);
    input.actor = "writer".into();
    let staged = staging(
        client
            .command::<BeginStaging>(&fixture.target, identity()?, input.clone())
            .await?
            .output,
    )?;
    let valid = LeaseCheck {
        token: staged.token,
        actor: "writer".into(),
    };
    let mut wrong_checks = Vec::new();
    for field in 0..4 {
        let mut wrong = valid.clone();
        match field {
            0 => wrong.token.request_digest[0] ^= 1,
            1 => wrong.token.artifact_operation = artifact_number(2),
            2 => wrong.token.attempt += 1,
            _ => wrong.actor = "owner".into(),
        }
        wrong_checks.push(wrong);
    }
    for wrong in wrong_checks {
        assert!(
            client
                .query::<CheckStaging>(&fixture.target, None, wrong.clone())
                .await?
                .output
                .is_none()
        );
        rejected(
            client
                .command::<BindStaging>(&fixture.target, identity()?, wrong)
                .await,
            PreparationDenial::Stale,
        );
    }
    fixture
        .handle
        .execute(
            identity()?,
            Digest::from_bytes([207; 32]),
            sql::now(0)?,
            1,
            0,
            |tx| {
                tx.execute(
                    "UPDATE repository_members SET role='read' WHERE account='writer'",
                    [],
                )?;
                Ok(cellule_runtime::cell::executor::HandlerOutcome::Success(
                    Vec::new(),
                ))
            },
        )
        .await?;
    assert!(
        client
            .query::<CheckStaging>(&fixture.target, None, valid.clone())
            .await?
            .output
            .is_none()
    );
    rejected(
        client
            .command::<BindStaging>(&fixture.target, identity()?, valid.clone())
            .await,
        PreparationDenial::Unauthorized,
    );
    denied(
        client
            .command::<RenewStaging>(
                &fixture.target,
                identity()?,
                LeaseRequest {
                    check: valid.clone(),
                    lease_ms: DEFAULT_LEASE_MS,
                },
            )
            .await,
        PreparationDenial::Unauthorized,
    );
    fixture
        .handle
        .execute(
            identity()?,
            Digest::from_bytes([208; 32]),
            sql::now(0)?,
            1,
            0,
            |tx| {
                tx.execute(
                    "UPDATE repository_members SET role='write' WHERE account='writer'",
                    [],
                )?;
                tx.execute("UPDATE catalog_operations SET expires_at_ms=0", [])?;
                tx.execute("UPDATE catalog_leases SET expires_at_ms=0", [])?;
                Ok(cellule_runtime::cell::executor::HandlerOutcome::Success(
                    Vec::new(),
                ))
            },
        )
        .await?;
    denied(
        client
            .command::<BeginStaging>(&fixture.target, identity()?, input)
            .await,
        PreparationDenial::Expired,
    );
    denied(
        client
            .command::<RenewStaging>(
                &fixture.target,
                identity()?,
                LeaseRequest {
                    check: valid.clone(),
                    lease_ms: DEFAULT_LEASE_MS,
                },
            )
            .await,
        PreparationDenial::Expired,
    );
    rejected(
        client
            .command::<BindStaging>(&fixture.target, identity()?, valid.clone())
            .await,
        PreparationDenial::Expired,
    );
    assert!(
        client
            .query::<CheckStaging>(&fixture.target, None, valid)
            .await?
            .output
            .is_none()
    );
    assert_eq!(super::frontier::reap(&fixture).await?, 2);
    fixture.runtime.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn staging_shares_operation_and_pin_quotas_and_binds_without_allocating_another_pin() -> Result
{
    let fixture = Fixture::new(ObjectFormat::Sha256).await?;
    let staged = start(&fixture, [209; 16]).await?;
    let owner = fixture.handle.owner_fence();
    for first in (1..MAX_OPERATIONS).step_by(REAP_ROWS as usize) {
        let last = (first + REAP_ROWS).min(MAX_OPERATIONS);
        fixture.handle.execute(identity()?,Digest::from_bytes([209;32]),sql::now(0)?,1,0,move |tx| {
            let mut pin = tx.prepare("INSERT INTO catalog_leases(incarnation,admission_sequence,operation,owner_epoch,artifact_operation,generation,expires_at_ms) VALUES(?1,?2,?3,?4,?5,?6,0)")?;
            let mut operation = tx.prepare("INSERT INTO catalog_operations(id,actor,request_digest,incarnation,owner_epoch,admission_sequence,artifact_operation,generation,expires_at_ms) VALUES(?1,'owner',zeroblob(32),?2,?3,?4,?5,?6,0)")?;
            for n in first..last {
                let mut id = [0u8;16]; id[..8].copy_from_slice(&n.to_be_bytes());
                let seq = 1_000_000+n as i64;
                let floor = if n%2==0 { Some(0) } else { None }; // both phases consume the same quotas
                pin.execute(rusqlite::params![owner.incarnation.as_bytes().as_slice(),seq,id.as_slice(),owner.epoch.to_be_bytes().as_slice(),artifact_number(seq as u64).as_slice(),floor])?;
                operation.execute(rusqlite::params![id.as_slice(),owner.incarnation.as_bytes().as_slice(),owner.epoch.to_be_bytes().as_slice(),seq,artifact_number(seq as u64).as_slice(),floor])?;
            }
            Ok(cellule_runtime::cell::executor::HandlerOutcome::Success(Vec::new()))
        }).await?;
    }
    let client = fixture.client();
    denied(
        client
            .command::<BeginStaging>(&fixture.target, identity()?, fixture.begin([210; 16]))
            .await,
        PreparationDenial::Capacity,
    );
    rejected(
        client
            .command::<BeginPreparation>(&fixture.target, identity()?, fixture.begin([211; 16]))
            .await,
        PreparationDenial::Capacity,
    );
    fixture
        .handle
        .execute(
            identity()?,
            Digest::from_bytes([210; 32]),
            sql::now(0)?,
            1,
            0,
            move |tx| {
                tx.execute(
                    "DELETE FROM catalog_operations WHERE id!=?1",
                    [staged.token.operation.as_slice()],
                )?;
                Ok(cellule_runtime::cell::executor::HandlerOutcome::Success(
                    Vec::new(),
                ))
            },
        )
        .await?;
    for first in (MAX_OPERATIONS..MAX_GENERATION_LEASES).step_by(REAP_ROWS as usize) {
        let last = (first + REAP_ROWS).min(MAX_GENERATION_LEASES);
        fixture.handle.execute(identity()?,Digest::from_bytes([211;32]),sql::now(0)?,1,0,move |tx| {
            let mut pin = tx.prepare("INSERT INTO catalog_leases(incarnation,admission_sequence,operation,owner_epoch,artifact_operation,generation,expires_at_ms) VALUES(?1,?2,?3,?4,?5,NULL,0)")?;
            for n in first..last {
                let mut id = [0u8;16]; id[..8].copy_from_slice(&n.to_be_bytes());
                let seq = 1_000_000+n as i64;
                pin.execute(rusqlite::params![owner.incarnation.as_bytes().as_slice(),seq,id.as_slice(),owner.epoch.to_be_bytes().as_slice(),artifact_number(seq as u64).as_slice()])?;
            }
            Ok(cellule_runtime::cell::executor::HandlerOutcome::Success(Vec::new()))
        }).await?;
    }
    assert_eq!(fixture.counts().await?, (1, MAX_GENERATION_LEASES));
    denied(
        client
            .command::<BeginStaging>(&fixture.target, identity()?, fixture.begin([210; 16]))
            .await,
        PreparationDenial::Capacity,
    );
    denied(
        client
            .command::<ClaimStaging>(&fixture.target, identity()?, request(staged.token))
            .await,
        PreparationDenial::Capacity,
    );
    let bound = lease(
        client
            .command::<BindStaging>(&fixture.target, identity()?, check(staged.token))
            .await?
            .output,
    )?;
    assert_eq!(bound.token, staged.token);
    assert_eq!(fixture.counts().await?, (1, MAX_GENERATION_LEASES));
    assert_eq!(super::frontier::reap(&fixture).await?, REAP_ROWS);
    fixture.runtime.shutdown().await?;
    Ok(())
}

#[test]
fn staging_codecs_are_bounded_and_reject_truncated_or_invalid_leases() -> Result {
    let original = StagingLease {
        token: PreparationToken {
            repository: uuid::Uuid::new_v4().into_bytes(),
            operation: [212; 16],
            artifact_operation: artifact_number(1),
            request_digest: [212; 32],
            owner: OwnerFence {
                incarnation: IncarnationId::from_bytes([212; 16]),
                epoch: u64::MAX,
            },
            attempt: 1,
        },
        format: ObjectFormat::Sha256,
        observed_at_ms: 10,
        expires_at_ms: 100,
    };
    let reply = StagingReply::Granted(Box::new(original));
    let mut e = BoundedEncoder::new(256)?;
    reply.encode(&mut e)?;
    let bytes = e.finish();
    let mut d = BoundedDecoder::new(&bytes, 256)?;
    assert_eq!(StagingReply::decode(&mut d)?, reply);
    d.finish()?;
    for length in 0..bytes.len() {
        let mut d = BoundedDecoder::new(&bytes[..length], 256)?;
        assert!(StagingReply::decode(&mut d).is_err());
    }
    for invalid in [
        StagingLease {
            observed_at_ms: -1,
            ..original
        },
        StagingLease {
            expires_at_ms: 10,
            ..original
        },
    ] {
        assert!(invalid.encode(&mut BoundedEncoder::new(256)?).is_err());
    }
    for reason in [
        PreparationDenial::Unauthorized,
        PreparationDenial::Conflict,
        PreparationDenial::Stale,
        PreparationDenial::Expired,
        PreparationDenial::Capacity,
        PreparationDenial::Missing,
    ] {
        let reply = StagingReply::Denied(reason);
        let mut e = BoundedEncoder::new(1)?;
        reply.encode(&mut e)?;
        let bytes = e.finish();
        let mut d = BoundedDecoder::new(&bytes, 1)?;
        assert_eq!(StagingReply::decode(&mut d)?, reply);
        d.finish()?;
    }
    Ok(())
}
