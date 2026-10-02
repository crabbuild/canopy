use super::*;
use crate::packs::sources::{NativeInputIndex, NativePackDescriptor};
use canopy_object_storage::artifact::{ArtifactDescriptor, ArtifactStore};
use tokio::time::{Duration, timeout};

async fn capture_real(
    context: StagingContext,
    store: Arc<ArtifactStore>,
) -> std::result::Result<NativeInputCertificate, Box<dyn std::error::Error + Send + Sync>> {
    let source = crate::packs::metadata::tests::fixture(context.format(), 4)
        .await
        .map_err(|e| std::io::Error::other(e.to_string()))?;
    let root = tempfile::TempDir::new()?;
    let disk = cellule_ltx::DiskBudget::new(16 << 20);
    let backend = crate::git_http::GitHttpBackend::initialize(
        root.path().into(),
        disk,
        "refs/heads/main",
        context.format(),
        crate::native_resources::NativeResources::default()
            .scope(crate::native_resources::NativeClass::Foreground),
    )
    .await?;
    for entry in std::fs::read_dir(source.root.path().join("objects/pack"))? {
        let entry = entry?;
        std::fs::copy(
            entry.path(),
            backend
                .git_dir()
                .join("objects/pack")
                .join(entry.file_name()),
        )?;
    }
    let inputs = backend
        .stage_native_packs(
            &context,
            &store,
            crate::packs::verification::physical::tests::physical_limits(),
        )
        .await?;
    Ok(context.seal_native_inputs(store, inputs).await?)
}

async fn active(
    fixture: &Fixture,
    operation: [u8; 16],
) -> Result<(StagingCoordinator, StagingTicket)> {
    let coordinator = StagingCoordinator::new(fixture.target.clone(), StagingLimits::default())?;
    let ready = ReadyStaging::new(
        fixture.client(),
        fixture.target.clone(),
        fixture.begin(operation),
        identity()?,
    )
    .await?;
    let ticket = coordinator.submit(ready).map_err(|(error, _)| error)?;
    assert!(matches!(
        timeout(Duration::from_secs(10), ticket.wait()).await?,
        StagingState::Active(_)
    ));
    Ok((coordinator, ticket))
}
fn records(
    repository: [u8; 16],
    operation: [u8; 16],
    format: ObjectFormat,
    count: u32,
) -> impl Iterator<Item = NativePackDescriptor> + Send {
    (1..=count).map(move |n| {
        let digest = *blake3::hash(&n.to_be_bytes()).as_bytes();
        let git_checksum = crate::ObjectId::try_from(&digest[..format.bytes()]).unwrap();
        let artifact = |size| ArtifactDescriptor {
            size,
            digest,
            manifest_digest: [18; 32],
        };
        NativePackDescriptor {
            repository,
            operation,
            format,
            git_checksum,
            object_count: 1,
            pack: artifact(100),
            index: artifact(8 + 1024 + (format.bytes() as u64 + 8) + 2 * format.bytes() as u64),
        }
    })
}
async fn seal(
    fixture: &Fixture,
    ticket: &StagingTicket,
    store: Arc<ArtifactStore>,
    count: u32,
) -> Result<NativeInputCertificate> {
    let repository = fixture.repository;
    let format = fixture.format;
    let task = ticket.spawn(move |context| async move {
        let token = context.token()?;
        context
            .seal_native_inputs(
                store,
                records(repository, token.artifact_operation, format, count),
            )
            .await
            .map_err(|e| StagingError::Input(Box::new(e)))
    })?;
    task.wait()
        .await
        .map_err(|e| format!("input factory: {e:?}").into())
}
async fn check(
    client: &CellClient,
    target: &CellTarget,
    token: PreparationToken,
) -> Result<Option<NativeInputCertificate>> {
    Ok(client
        .query::<CheckStagedInputs>(
            target,
            None,
            LeaseCheck {
                token,
                actor: "owner".into(),
            },
        )
        .await?
        .output)
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
async fn mutate(handle: &CellHandle, statement: String) -> Result {
    let digest = Digest::from_bytes(*blake3::hash(statement.as_bytes()).as_bytes());
    handle
        .execute(
            identity()?,
            digest,
            sql::now(0)?,
            statement.len(),
            0,
            move |tx| {
                tx.execute_batch(&statement)?;
                Ok(cellule_runtime::cell::executor::HandlerOutcome::Success(
                    Vec::new(),
                ))
            },
        )
        .await?;
    Ok(())
}

#[tokio::test]
async fn staged_inputs_reuse_bounded_index_and_checkpoint_is_immutable() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let fixture = Fixture::new(format).await?;
        let (coordinator, ticket) = active(&fixture, [170; 16]).await?;
        let store = Arc::new(ArtifactStore::new(
            Arc::new(InMemory::new()),
            fixture.repository,
        ));
        let proof = seal(&fixture, &ticket, store.clone(), 300).await?;
        let root = proof.root()?.ok_or("root")?;
        assert_eq!((root.record_count, root.object_count), (300, 300));
        assert!(root.height > 0 && root.artifact.size <= 64 << 10);
        let mut e = BoundedEncoder::new(CERTIFICATE_BYTES)?;
        proof.encode(&mut e)?;
        assert!(e.finish().len() < 1024);
        let before = fixture.counts().await?;
        let mutation = identity()?;
        let first = fixture
            .client()
            .command::<RegisterStagedInputs>(&fixture.target, mutation, proof.clone())
            .await?;
        let replay = fixture
            .client()
            .command::<RegisterStagedInputs>(&fixture.target, mutation, proof.clone())
            .await?;
        assert_eq!(first.receipt, replay.receipt);
        assert_eq!(
            check(&fixture.client(), &fixture.target, proof.token()?).await?,
            Some(proof.clone())
        );
        assert_eq!(fixture.counts().await?, before);
        let index = NativeInputIndex::new(store, format);
        let mut cursor = index.cursor(Some(root), None)?;
        let mut count = 0;
        while let Some(native) = cursor.next().await? {
            assert_eq!(native.operation, proof.token()?.artifact_operation);
            count += 1;
        }
        assert_eq!(count, 300);
        let different = seal(
            &fixture,
            &ticket,
            Arc::new(ArtifactStore::new(
                Arc::new(InMemory::new()),
                fixture.repository,
            )),
            1,
        )
        .await?;
        denied(
            fixture
                .client()
                .command::<RegisterStagedInputs>(&fixture.target, identity()?, different)
                .await,
            PreparationDenial::Conflict,
        );
        let sql = cellule_runtime::primitives::sql::SqlCell::<RepositoryModule>::new(
            fixture.client(),
            fixture.target.clone(),
        )?;
        let result=sql.query(Some(first.receipt),sql::statement("SELECT input_checkpoint,input_checkpoint_digest,generation FROM catalog_leases",vec![])).await?;
        assert!(matches!(sql::rows(&result.output)?[0][2], SqlValue::Null));
        ticket.stop();
        assert!(coordinator.close_and_drain().await.is_empty());
        fixture.runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn input_checkpoint_rejects_tampering_scope_revocation_and_stale_attempt() -> Result {
    let fixture = Fixture::new(ObjectFormat::Sha256).await?;
    let (coordinator, ticket) = active(&fixture, [171; 16]).await?;
    let store = Arc::new(ArtifactStore::new(
        Arc::new(InMemory::new()),
        fixture.repository,
    ));
    let proof = seal(&fixture, &ticket, store, 1).await?;
    let mut e = BoundedEncoder::new(CERTIFICATE_BYTES)?;
    proof.encode(&mut e)?;
    let mut bytes = e.finish();
    let last = bytes.len() - 1;
    bytes[last] ^= 1;
    let mut d = BoundedDecoder::new(&bytes, CERTIFICATE_BYTES)?;
    let tampered = NativeInputCertificate::decode(&mut d)?;
    d.finish()?;
    denied(
        fixture
            .client()
            .command::<RegisterStagedInputs>(&fixture.target, identity()?, tampered)
            .await,
        PreparationDenial::Conflict,
    );
    assert!(
        check(&fixture.client(), &fixture.target, proof.token()?)
            .await?
            .is_none()
    );
    let foreign = Fixture::new(ObjectFormat::Sha256).await?;
    denied(
        foreign
            .client()
            .command::<RegisterStagedInputs>(&foreign.target, identity()?, proof.clone())
            .await,
        PreparationDenial::Unauthorized,
    );
    foreign.runtime.shutdown().await?;
    mutate(
        &fixture.handle,
        "UPDATE repository_identity SET owner='another'".into(),
    )
    .await?;
    denied(
        fixture
            .client()
            .command::<RegisterStagedInputs>(&fixture.target, identity()?, proof.clone())
            .await,
        PreparationDenial::Unauthorized,
    );
    assert!(
        check(&fixture.client(), &fixture.target, proof.token()?)
            .await?
            .is_none()
    );
    mutate(
        &fixture.handle,
        "UPDATE repository_identity SET owner='owner'".into(),
    )
    .await?;
    // Claim gets a new admitted identity even on this same owner. Its previous
    // independent pin survives, but an old proof cannot populate the new pin.
    let claimed = fixture
        .client()
        .command::<ClaimStaging>(
            &fixture.target,
            identity()?,
            LeaseRequest {
                check: LeaseCheck {
                    token: proof.token()?,
                    actor: "owner".into(),
                },
                lease_ms: DEFAULT_LEASE_MS,
            },
        )
        .await?;
    assert!(matches!(claimed.output, StagingReply::Granted(_)));
    denied(
        fixture
            .client()
            .command::<RegisterStagedInputs>(&fixture.target, identity()?, proof.clone())
            .await,
        PreparationDenial::Stale,
    );
    ticket.stop();
    assert!(coordinator.close_and_drain().await.is_empty());
    fixture.runtime.shutdown().await?;
    Ok(())
}

#[test]
fn input_checkpoint_sql_pairs_and_immutability_reject_partial_or_replaced_facts() -> Result {
    let connection = rusqlite::Connection::open_in_memory()?;
    connection.execute_batch(SCHEMA)?;
    connection.execute("INSERT INTO catalog_leases(incarnation,admission_sequence,operation,owner_epoch,artifact_operation,generation,expires_at_ms) VALUES(zeroblob(16),1,zeroblob(16),x'0000000000000001',x'43414e4f505930310000000000000001',NULL,100)", [])?;
    for sql in [
        "UPDATE catalog_leases SET input_checkpoint=x'01'",
        "UPDATE catalog_leases SET input_checkpoint_digest=zeroblob(32)",
    ] {
        assert!(connection.execute(sql, []).is_err());
    }
    connection.execute(
        "UPDATE catalog_leases SET input_checkpoint=x'01',input_checkpoint_digest=zeroblob(32)",
        [],
    )?;
    for sql in [
        "UPDATE catalog_leases SET input_checkpoint=NULL,input_checkpoint_digest=NULL",
        "UPDATE catalog_leases SET input_checkpoint=x'02'",
        "UPDATE catalog_leases SET input_checkpoint_digest=randomblob(32)",
    ] {
        assert!(connection.execute(sql, []).is_err());
    }
    connection.execute(
        "UPDATE catalog_leases SET input_checkpoint=x'01',input_checkpoint_digest=zeroblob(32)",
        [],
    )?;
    Ok(())
}

#[tokio::test]
async fn source_pin_expiry_after_reconstruction_refuses_final_adoption() -> Result {
    let fixture = Fixture::new(ObjectFormat::Sha1).await?;
    let (old_coordinator, old_ticket) = active(&fixture, [174; 16]).await?;
    let store = Arc::new(ArtifactStore::new(
        Arc::new(InMemory::new()),
        fixture.repository,
    ));
    let old = seal(&fixture, &old_ticket, store.clone(), 1).await?;
    fixture
        .client()
        .command::<RegisterStagedInputs>(&fixture.target, identity()?, old.clone())
        .await?;
    old_ticket.stop();
    assert!(old_coordinator.close_and_drain().await.is_empty());
    let coordinator = StagingCoordinator::new(fixture.target.clone(), StagingLimits::default())?;
    let ready = ReadyStaging::claim(
        fixture.client(),
        fixture.target.clone(),
        LeaseRequest {
            check: LeaseCheck {
                token: old.token()?,
                actor: "owner".into(),
            },
            lease_ms: DEFAULT_LEASE_MS,
        },
        identity()?,
    )
    .await?;
    let ticket = coordinator.submit(ready).map_err(|(e, _)| e)?;
    assert!(matches!(
        timeout(Duration::from_secs(10), ticket.wait()).await?,
        StagingState::Active(_)
    ));
    let previous = old.clone();
    let task = ticket.spawn(move |context| async move {
        context
            .adopt_native_inputs(store, &previous)
            .await
            .map_err(|e| StagingError::Input(Box::new(e)))
    })?;
    let adopted = task
        .wait()
        .await
        .map_err(|e| format!("reconstruct: {e:?}"))?;
    mutate(
        &fixture.handle,
        format!(
            "UPDATE catalog_leases SET expires_at_ms=0 WHERE admission_sequence={}",
            old.token()?.attempt
        ),
    )
    .await?;
    assert!(
        check(&fixture.client(), &fixture.target, old.token()?)
            .await?
            .is_none()
    );
    denied(
        fixture
            .client()
            .command::<RegisterStagedInputs>(&fixture.target, identity()?, adopted.clone())
            .await,
        PreparationDenial::Expired,
    );
    assert!(
        check(&fixture.client(), &fixture.target, adopted.token()?)
            .await?
            .is_none()
    );
    ticket.stop();
    assert!(coordinator.close_and_drain().await.is_empty());
    fixture.runtime.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn bound_preparation_claim_adopts_exact_input_root_without_copying_nodes() -> Result {
    let fixture = Fixture::new(ObjectFormat::Sha256).await?;
    let (coordinator, ticket) = active(&fixture, [175; 16]).await?;
    let store = Arc::new(ArtifactStore::new(
        Arc::new(InMemory::new()),
        fixture.repository,
    ));
    let prior = seal(&fixture, &ticket, store.clone(), 300).await?;
    fixture
        .client()
        .command::<RegisterStagedInputs>(&fixture.target, identity()?, prior.clone())
        .await?;
    ticket.seal()?;
    let StagingState::Bound(bound) =
        timeout(Duration::from_secs(10), ticket.wait_terminal()).await?
    else {
        return Err("bind".into());
    };
    assert!(coordinator.close_and_drain().await.is_empty());
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
    let session = PreparationSession::open(
        fixture.client(),
        fixture.target.clone(),
        LeaseCheck {
            token: next.token,
            actor: "owner".into(),
        },
        Some(claimed.receipt),
    )
    .await?;
    let adopted = session.adopt_native_inputs(store, &prior).await?;
    assert_eq!(adopted.root()?, prior.root()?);
    assert_eq!(adopted.token()?, next.token);
    assert_ne!(
        adopted.token()?.artifact_operation,
        prior.token()?.artifact_operation
    );
    fixture
        .client()
        .command::<RegisterStagedInputs>(&fixture.target, identity()?, adopted.clone())
        .await?;
    assert_eq!(
        check(&fixture.client(), &fixture.target, next.token).await?,
        Some(adopted)
    );
    fixture.runtime.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn claimed_staging_retains_exact_dispatch_after_absence_lost_ack_and_panic() -> Result {
    for fault in [1, 2, 3] {
        let fixture = Fixture::new(ObjectFormat::Sha256).await?;
        let (old_coordinator, old_ticket) = active(&fixture, [176; 16]).await?;
        let StagingState::Active(old) = old_ticket.state() else {
            return Err("old active".into());
        };
        old_ticket.stop();
        assert!(old_coordinator.close_and_drain().await.is_empty());
        let coordinator =
            StagingCoordinator::new(fixture.target.clone(), StagingLimits::default())?;
        coordinator.fault_for_test(fault);
        let ready = ReadyStaging::claim(
            fixture.client(),
            fixture.target.clone(),
            LeaseRequest {
                check: LeaseCheck {
                    token: old.token,
                    actor: "owner".into(),
                },
                lease_ms: DEFAULT_LEASE_MS,
            },
            identity()?,
        )
        .await?;
        let ticket = coordinator.submit(ready).map_err(|(e, _)| e)?;
        assert!(matches!(
            timeout(Duration::from_secs(10), ticket.wait()).await?,
            StagingState::Uncertain(_)
        ));
        let evidence = match ticket.state() {
            StagingState::Uncertain(error) => match &*error {
                StagingError::Claim(error) => match &**error {
                    InvocationError::Pending(pending) => (**pending).clone(),
                    _ => return Err("claim evidence".into()),
                },
                _ => return Err("claim type".into()),
            },
            _ => return Err("uncertain".into()),
        };
        drop(ticket);
        let retained = coordinator
            .pending(old.token.operation)
            .ok_or("retained claim")?;
        assert_eq!(coordinator.stats().command_bytes, 8192);
        coordinator.recover(&retained)?;
        let StagingState::Active(next) = timeout(Duration::from_secs(10), retained.wait()).await?
        else {
            return Err("resolved claim".into());
        };
        assert_ne!(next.token, old.token);
        assert_eq!(fixture.counts().await?, (1, 2));
        assert!(matches!(
            fixture.client().resolve(&evidence).await?,
            cellule_runtime::Resolution::Committed(_)
        ));
        retained.stop();
        assert!(coordinator.close_and_drain().await.is_empty());
        fixture.runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn restored_owner_claims_and_adopts_only_a_retained_exact_input_checkpoint() -> Result {
    let fixture = Fixture::new(ObjectFormat::Sha256).await?;
    let (coordinator, ticket) = active(&fixture, [172; 16]).await?;
    let store = Arc::new(ArtifactStore::new(
        Arc::new(InMemory::new()),
        fixture.repository,
    ));
    let provider = store.clone();
    let task = ticket.spawn(move |context| async move {
        capture_real(context, provider)
            .await
            .map_err(StagingError::Input)
    })?;
    let proof = task
        .wait()
        .await
        .map_err(|e| format!("native capture: {e:?}"))?;
    let mutation = identity()?;
    let first = fixture
        .client()
        .command::<RegisterStagedInputs>(&fixture.target, mutation, proof.clone())
        .await?;
    ticket.stop();
    assert!(coordinator.close_and_drain().await.is_empty());
    fixture.handle.drain().await?;
    fixture.runtime.shutdown().await?;
    let session = SessionId::from_bytes([173; 16]);
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
            fixture.root.path().join("inputs-b.sqlite"),
            Owner {
                session,
                endpoint: "https://inputs-b.invalid".into(),
            },
        )
        .await?;
    let client = CellClient::local(Arc::clone(&fixture.registry), handle.clone());
    let replay = client
        .command::<RegisterStagedInputs>(&fixture.target, mutation, proof.clone())
        .await?;
    assert_eq!(replay.receipt, first.receipt);
    denied(
        client
            .command::<RegisterStagedInputs>(&fixture.target, identity()?, proof.clone())
            .await,
        PreparationDenial::Stale,
    );
    assert_eq!(
        check(&client, &fixture.target, proof.token()?).await?,
        Some(proof.clone())
    );
    let coordinator = StagingCoordinator::new(fixture.target.clone(), StagingLimits::default())?;
    let ready = ReadyStaging::claim(
        client.clone(),
        fixture.target.clone(),
        LeaseRequest {
            check: LeaseCheck {
                token: proof.token()?,
                actor: "owner".into(),
            },
            lease_ms: DEFAULT_LEASE_MS,
        },
        identity()?,
    )
    .await?;
    let ticket = coordinator.submit(ready).map_err(|(e, _)| e)?;
    let StagingState::Active(new) = timeout(Duration::from_secs(10), ticket.wait()).await? else {
        return Err("claim".into());
    };
    assert_ne!(new.token.owner, proof.token()?.owner);
    assert_ne!(
        new.token.artifact_operation,
        proof.token()?.artifact_operation
    );
    let old = proof.clone();
    let provider = store.clone();
    let task = ticket.spawn(move |context| async move {
        context
            .adopt_native_inputs(provider, &old)
            .await
            .map_err(|e| StagingError::Input(Box::new(e)))
    })?;
    let adopted = task.wait().await.map_err(|e| format!("adopt: {e:?}"))?;
    assert_eq!(adopted.token()?, new.token);
    assert_eq!(adopted.root()?.ok_or("new root")?.record_count, 1);
    assert_eq!(adopted.root()?, proof.root()?);
    client
        .command::<RegisterStagedInputs>(&fixture.target, identity()?, adopted.clone())
        .await?;
    // The committed destination root retains its native incarnations after the
    // source pin expires. Re-registration must not require the parent again.
    mutate(
        &handle,
        format!(
            "UPDATE catalog_leases SET expires_at_ms=0 WHERE admission_sequence={}",
            proof.token()?.attempt
        ),
    )
    .await?;
    assert!(
        check(&client, &fixture.target, proof.token()?)
            .await?
            .is_none()
    );
    client
        .command::<RegisterStagedInputs>(&fixture.target, identity()?, adopted.clone())
        .await?;
    let index = NativeInputIndex::new(store.clone(), fixture.format);
    let mut old = index.cursor(proof.root()?, None)?;
    let mut new_cursor = index.cursor(adopted.root()?, None)?;
    while let Some(native) = old.next().await? {
        assert_eq!(new_cursor.next().await?, Some(native));
        let scratch = tempfile::TempDir::new()?;
        let budget = cellule_ltx::DiskBudget::new(64 << 20);
        let resources = crate::native_resources::NativeResources::default();
        let mut verifier = crate::packs::verification::PhysicalVerifier::download(
            scratch.path(),
            budget,
            &store,
            native,
            crate::packs::verification::physical::tests::physical_limits(),
            resources.scope(crate::native_resources::NativeClass::Foreground),
        )
        .await?;
        verifier.inspect_next_shard(native.object_count).await?;
        verifier.finish().await?;
    }
    assert!(new_cursor.next().await?.is_none());
    assert_eq!(
        check(&client, &fixture.target, new.token).await?,
        Some(adopted)
    );
    ticket.stop();
    assert!(coordinator.close_and_drain().await.is_empty());
    runtime.shutdown().await?;
    Ok(())
}
