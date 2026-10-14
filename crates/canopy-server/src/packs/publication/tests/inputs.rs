use super::*;
mod bound;
mod custody;
mod requests;
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

pub(super) async fn active(
    fixture: &Fixture,
    operation: [u8; 16],
) -> Result<(StagingCoordinator, StagingTicket)> {
    let coordinator = StagingCoordinator::new(
        fixture.target.clone(),
        StagingLimits::default(),
        fixture.authority(),
    )?;
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
pub(super) async fn seal(
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

#[test]
fn input_checkpoint_sql_append_requires_exact_predecessor_unbound_phase_and_revision_capacity()
-> Result {
    let connection = rusqlite::Connection::open_in_memory()?;
    connection.execute_batch(SCHEMA)?;
    connection.execute("INSERT INTO catalog_leases(incarnation,admission_sequence,operation,owner_epoch,artifact_operation,generation,expires_at_ms) VALUES(zeroblob(16),1,zeroblob(16),x'0000000000000001',x'43414e4f505930310000000000000001',NULL,100)", [])?;
    connection.execute(
        "UPDATE catalog_leases SET input_checkpoint=x'01',input_checkpoint_digest=zeroblob(32)",
        [],
    )?;
    for invalid in [
        "UPDATE catalog_leases SET input_checkpoint=x'02',input_checkpoint_digest=randomblob(32),input_checkpoint_revision=1,input_checkpoint_previous_digest=randomblob(32)",
        "UPDATE catalog_leases SET input_checkpoint=x'02',input_checkpoint_digest=randomblob(32),input_checkpoint_revision=2,input_checkpoint_previous_digest=input_checkpoint_digest",
        "UPDATE catalog_leases SET input_checkpoint=x'02',input_checkpoint_revision=1,input_checkpoint_previous_digest=input_checkpoint_digest",
        "UPDATE catalog_leases SET generation=0,input_checkpoint=x'02',input_checkpoint_digest=randomblob(32),input_checkpoint_revision=1,input_checkpoint_previous_digest=input_checkpoint_digest",
    ] {
        assert!(connection.execute(invalid, []).is_err(), "{invalid}");
    }
    connection.execute_batch("SAVEPOINT bound; UPDATE catalog_leases SET generation=0;")?;
    assert!(connection.execute("UPDATE catalog_leases SET input_checkpoint=x'02',input_checkpoint_digest=randomblob(32),input_checkpoint_revision=1,input_checkpoint_previous_digest=input_checkpoint_digest", []).is_err());
    connection.execute_batch("ROLLBACK TO bound; RELEASE bound;")?;
    for revision in 1i64..=256 {
        let prior: Vec<u8> = connection.query_row(
            "SELECT input_checkpoint_digest FROM catalog_leases",
            [],
            |row| row.get(0),
        )?;
        let bytes = revision.to_be_bytes();
        let digest = *blake3::hash(&bytes).as_bytes();
        assert_eq!(connection.execute("UPDATE catalog_leases SET input_checkpoint=?1,input_checkpoint_digest=?2,input_checkpoint_previous_digest=?3,input_checkpoint_revision=?4 WHERE input_checkpoint_digest=?3",rusqlite::params![bytes.as_slice(),digest.as_slice(),prior,revision])?, 1);
        assert_eq!(connection.execute("UPDATE catalog_leases SET input_checkpoint=input_checkpoint,input_checkpoint_digest=input_checkpoint_digest,input_checkpoint_previous_digest=input_checkpoint_previous_digest,input_checkpoint_revision=input_checkpoint_revision", [])?, 1);
    }
    assert!(connection.execute("UPDATE catalog_leases SET input_checkpoint=x'03',input_checkpoint_digest=randomblob(32),input_checkpoint_revision=257,input_checkpoint_previous_digest=input_checkpoint_digest", []).is_err());
    connection.execute("UPDATE catalog_leases SET generation=0", [])?;
    assert!(connection.execute("UPDATE catalog_leases SET input_checkpoint=x'04',input_checkpoint_digest=randomblob(32),input_checkpoint_revision=257,input_checkpoint_previous_digest=input_checkpoint_digest", []).is_err());
    assert_eq!(
        connection.query_row(
            "SELECT input_checkpoint_revision FROM catalog_leases",
            [],
            |row| row.get::<_, i64>(0)
        )?,
        256
    );
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
    let coordinator = StagingCoordinator::new(
        fixture.target.clone(),
        StagingLimits::default(),
        fixture.authority(),
    )?;
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
    let session = Arc::new(
        PreparationSession::open(
            fixture.client(),
            fixture.target.clone(),
            LeaseCheck {
                token: next.token,
                actor: "owner".into(),
            },
            Some(claimed.receipt),
            fixture.authority(),
        )
        .await?,
    );
    let adopted = session.adopt_native_inputs(store, &prior).await?;
    assert_eq!(adopted.root()?, prior.root()?);
    assert_eq!(adopted.token()?, next.token);
    assert_ne!(
        adopted.token()?.artifact_operation,
        prior.token()?.artifact_operation
    );
    let publisher = PublicationCoordinator::new(
        fixture.target.clone(),
        PublicationLimits::default(),
        fixture.publication_budget.clone(),
    )?;
    let ready = session.ready_inputs(identity()?, adopted.clone()).await?;
    let registered = publisher.submit(ready).await?;
    let PublicationState::Finished(Ok(PublicationOutcome::Inputs(result))) =
        timeout(Duration::from_secs(10), registered.wait()).await?
    else {
        return Err("bound registration".into());
    };
    assert!(result.custody.is_ok());
    assert!(publisher.close_and_drain().await.is_empty());
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
        let coordinator = StagingCoordinator::new(
            fixture.target.clone(),
            StagingLimits::default(),
            fixture.authority(),
        )?;
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
        assert_eq!(
            coordinator.stats().command_bytes,
            super::super::custody::RESERVATION
        );
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
    let coordinator = StagingCoordinator::new(
        fixture.target.clone(),
        StagingLimits::default(),
        fixture.authority(),
    )?;
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
    let registration_identity = identity()?;
    let registered = client
        .command::<RegisterStagedInputs>(&fixture.target, registration_identity, adopted.clone())
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
    assert_eq!(
        check(&client, &fixture.target, new.token).await?,
        Some(adopted.clone())
    );
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
        let segment = verifier.inspect_next_shard(native.object_count).await?;
        let witness = verifier.finish().await?;
        let tip = segment
            .headers_after(None)?
            .into_iter()
            .find(|h| h.object.kind == crate::ObjectKind::Commit)
            .ok_or("retained commit")?
            .object
            .oid;
        ticket.seal()?;
        assert!(matches!(
            timeout(Duration::from_secs(10), ticket.wait_terminal()).await?,
            StagingState::Bound(_)
        ));
        let indexes = Arc::new(crate::packs::catalog::CatalogIndexes::new(
            store.clone(),
            fixture.format,
        ));
        let files = Arc::new(crate::packs::catalog::CatalogFiles::new(
            scratch.path(),
            cellule_ltx::DiskBudget::new(64 << 20),
            store.clone(),
            fixture.format,
            crate::packs::catalog::CatalogFileLimits::default(),
        )?);
        let base = Arc::new(ticket.open_base(indexes, files).await?);
        let mut builder = CatalogPreparation::new(
            scratch.path(),
            cellule_ltx::DiskBudget::new(64 << 20),
            base,
            crate::packs::metadata::tests::limits(),
        )
        .await?;
        builder.begin_retained_pack(witness).await?;
        builder.add_segment(segment).await?;
        builder.finish_pack().await?;
        let prepared = builder.finish().await?;
        let publication = prepared
            .ref_proof(
                super::publishing::plan(vec![super::publishing::update(
                    "refs/heads/main",
                    None,
                    Some(tip),
                )]),
                scratch.path(),
                cellule_ltx::DiskBudget::new(64 << 20),
                crate::packs::metadata::tests::limits(),
            )
            .await?;
        let committed = client
            .command::<PublishCatalogRefs>(&fixture.target, identity()?, publication)
            .await?;
        assert!(matches!(committed.output, PublicationReply::Published(_)));
    }
    assert!(new_cursor.next().await?.is_none());
    // Completion retires the active operation, while its independent pin and
    // the published source roots keep their immutable custody facts.
    assert!(check(&client, &fixture.target, new.token).await?.is_none());
    let token = new.token;
    let pinned = handle.query(0,32,move |connection| {
        Ok(connection.query_row("SELECT input_checkpoint_digest FROM catalog_leases WHERE incarnation=?1 AND admission_sequence=?2",rusqlite::params![token.owner.incarnation.as_bytes().as_slice(),token.attempt as i64],|row|row.get::<_,Vec<u8>>(0))?)
    }).await?;
    let mut e = BoundedEncoder::new(CERTIFICATE_BYTES)?;
    adopted.encode(&mut e)?;
    assert_eq!(pinned, blake3::hash(&e.finish()).as_bytes());
    let replay = client
        .command::<RegisterStagedInputs>(&fixture.target, registration_identity, adopted)
        .await?;
    assert_eq!(replay.receipt, registered.receipt);
    ticket.stop();
    assert!(coordinator.close_and_drain().await.is_empty());
    runtime.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn service_checkpoint_retains_exact_identity_after_cancellation_absence_lost_reply_and_panic()
-> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        for fault in [1, 2, 3] {
            let fixture = Fixture::new(format).await?;
            let (coordinator, ticket) = active(&fixture, [177; 16]).await?;
            let store = Arc::new(ArtifactStore::new(
                Arc::new(InMemory::new()),
                fixture.repository,
            ));
            let proof = seal(&fixture, &ticket, store, 1).await?;
            let mutation = identity()?;
            coordinator.fault_for_test(fault);
            let observer = ticket
                .register_inputs(proof.clone(), mutation)
                .map_err(|(e, _)| e)?;
            drop(observer);
            let StagingState::Uncertain(error) =
                timeout(Duration::from_secs(10), ticket.wait_terminal()).await?
            else {
                return Err("checkpoint uncertainty".into());
            };
            let StagingError::Checkpoint(error) = &*error else {
                return Err("checkpoint command".into());
            };
            let InvocationError::Pending(evidence) = &**error else {
                return Err("checkpoint evidence".into());
            };
            let evidence = (**evidence).clone();
            let retained = ticket.pending_inputs().ok_or("checkpoint observer")?;
            assert!(
                matches!(retained.wait().await, Err(e) if matches!(&*e, StagingError::Checkpoint(_)))
            );
            assert_eq!(
                coordinator.stats().command_bytes,
                super::super::custody::RESERVATION + 4096
            );
            // Closing retains unknown registrations and their charged command.
            let pending = timeout(Duration::from_secs(10), coordinator.close_and_drain()).await?;
            assert_eq!(pending.len(), 1);
            coordinator.recover(&pending[0])?;
            let receipt = timeout(Duration::from_secs(10), retained.wait())
                .await?
                .map_err(|e| format!("registration recovery: {e:?}"))?;
            assert!(matches!(
                timeout(Duration::from_secs(10), pending[0].wait_terminal()).await?,
                StagingState::Stopped
            ));
            let replay = fixture
                .client()
                .command::<RegisterStagedInputs>(&fixture.target, mutation, proof.clone())
                .await?;
            assert_eq!(receipt, replay.receipt);
            let cellule_runtime::Resolution::Committed(outcome) =
                fixture.client().resolve(&evidence).await?
            else {
                return Err("original checkpoint outcome".into());
            };
            assert_eq!(receipt.commit_sequence, outcome.commit_sequence());
            assert_eq!(
                check(&fixture.client(), &fixture.target, proof.token()?).await?,
                Some(proof)
            );
            assert_eq!(fixture.counts().await?, (1, 1));
            assert_eq!(coordinator.stats().admitted, 0);
            assert_eq!(coordinator.stats().command_bytes, 0);
            assert!(coordinator.close_and_drain().await.is_empty());
            fixture.runtime.shutdown().await?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn service_checkpoint_seal_orders_registration_before_binding_and_keeps_original_receipt()
-> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let fixture = Fixture::new(format).await?;
        let (coordinator, ticket) = active(&fixture, [178; 16]).await?;
        let store = Arc::new(ArtifactStore::new(
            Arc::new(InMemory::new()),
            fixture.repository,
        ));
        let proof = seal(&fixture, &ticket, store, 1).await?;
        let mutation = identity()?;
        let observer = ticket
            .register_inputs(proof.clone(), mutation)
            .map_err(|(e, _)| e)?;
        ticket.seal()?;
        let StagingState::Bound(bound) =
            timeout(Duration::from_secs(10), ticket.wait_terminal()).await?
        else {
            return Err("checkpoint then bind".into());
        };
        let receipt = observer
            .wait()
            .await
            .map_err(|e| format!("checkpoint: {e:?}"))?;
        assert!(receipt.commit_sequence < bound.receipt.commit_sequence);
        assert_eq!(
            receipt,
            ticket
                .pending_inputs()
                .ok_or("retained receipt")?
                .wait()
                .await
                .map_err(|e| e.to_string())?
        );
        assert_eq!(
            check(&fixture.client(), &fixture.target, proof.token()?).await?,
            Some(proof.clone())
        );
        let replay = fixture
            .client()
            .command::<RegisterStagedInputs>(&fixture.target, mutation, proof.clone())
            .await?;
        assert_eq!(receipt, replay.receipt);
        denied(
            fixture
                .client()
                .command::<RegisterStagedInputs>(&fixture.target, identity()?, proof)
                .await,
            PreparationDenial::Conflict,
        );
        assert!(coordinator.close_and_drain().await.is_empty());
        fixture.runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn service_checkpoint_rejects_foreign_attempt_duplicate_and_closed_admission_without_execution()
-> Result {
    let fixture = Fixture::new(ObjectFormat::Sha256).await?;
    let (coordinator, ticket) = active(&fixture, [179; 16]).await?;
    let store = Arc::new(ArtifactStore::new(
        Arc::new(InMemory::new()),
        fixture.repository,
    ));
    let proof = seal(&fixture, &ticket, store.clone(), 1).await?;
    let ready = ReadyStaging::new(
        fixture.client(),
        fixture.target.clone(),
        fixture.begin([180; 16]),
        identity()?,
    )
    .await?;
    let other = coordinator.submit(ready).map_err(|(e, _)| e)?;
    assert!(matches!(
        timeout(Duration::from_secs(10), other.wait()).await?,
        StagingState::Active(_)
    ));
    let foreign_attempt = other.register_inputs(proof.clone(), identity()?);
    assert!(matches!(foreign_attempt, Err((StagingError::Context, _))));
    assert!(other.pending_inputs().is_none());
    let another = Fixture::new(ObjectFormat::Sha256).await?;
    let (another_coordinator, another_ticket) = active(&another, [181; 16]).await?;
    let foreign = seal(
        &another,
        &another_ticket,
        Arc::new(ArtifactStore::new(
            Arc::new(InMemory::new()),
            another.repository,
        )),
        1,
    )
    .await?;
    assert!(matches!(
        ticket.register_inputs(foreign, identity()?),
        Err((StagingError::Context, _))
    ));
    assert!(ticket.pending_inputs().is_none());
    let mutation = identity()?;
    let registration = ticket
        .register_inputs(proof.clone(), mutation)
        .map_err(|(e, _)| e)?;
    let receipt = timeout(Duration::from_secs(10), registration.wait())
        .await?
        .map_err(|e| e.to_string())?;
    // Wait for the fresh live probe before checking duplicate admission.
    assert!(matches!(
        timeout(Duration::from_secs(10), ticket.wait()).await?,
        StagingState::Active(_)
    ));
    let refused_identity = identity()?;
    assert!(matches!(
        ticket.register_inputs(proof.clone(), refused_identity),
        Err((StagingError::Duplicate, _))
    ));
    let unused = fixture
        .client()
        .prepare_command::<RegisterStagedInputs>(&fixture.target, refused_identity, proof.clone())
        .await?;
    assert!(matches!(
        fixture.client().resolve(unused.evidence()).await?,
        cellule_runtime::Resolution::Absent
    ));
    other.stop();
    ticket.stop();
    assert!(matches!(
        ticket.register_inputs(proof, identity()?),
        Err((StagingError::Inactive, _))
    ));
    assert_eq!(
        registration.wait().await.map_err(|e| e.to_string())?,
        receipt
    );
    assert!(coordinator.close_and_drain().await.is_empty());
    another_ticket.stop();
    assert!(another_coordinator.close_and_drain().await.is_empty());
    another.runtime.shutdown().await?;
    fixture.runtime.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn service_checkpoint_recovery_preserves_commit_but_refuses_fresh_authority_after_revocation()
-> Result {
    for fault in [1, 2] {
        let fixture = Fixture::new(ObjectFormat::Sha256).await?;
        let (coordinator, ticket) = active(&fixture, [182; 16]).await?;
        let proof = seal(
            &fixture,
            &ticket,
            Arc::new(ArtifactStore::new(
                Arc::new(InMemory::new()),
                fixture.repository,
            )),
            1,
        )
        .await?;
        let mutation = identity()?;
        coordinator.fault_for_test(fault);
        let registration = ticket
            .register_inputs(proof.clone(), mutation)
            .map_err(|(e, _)| e)?;
        assert!(matches!(
            timeout(Duration::from_secs(10), ticket.wait_terminal()).await?,
            StagingState::Uncertain(_)
        ));
        mutate(
            &fixture.handle,
            "UPDATE repository_identity SET owner='other' WHERE singleton=1".into(),
        )
        .await?;
        coordinator.recover(&ticket)?;
        let StagingState::Fenced(_) =
            timeout(Duration::from_secs(10), ticket.wait_terminal()).await?
        else {
            return Err("revoked checkpoint fence".into());
        };
        let result = registration.wait().await;
        if fault == 2 {
            let receipt = result.map_err(|e| e.to_string())?;
            let replay = fixture
                .client()
                .command::<RegisterStagedInputs>(&fixture.target, mutation, proof.clone())
                .await?;
            assert_eq!(receipt, replay.receipt);
        } else {
            assert!(
                matches!(result, Err(e) if matches!(&*e, StagingError::Checkpoint(error) if matches!(&**error, InvocationError::Rejected(value) if value.output == StagingReply::Denied(PreparationDenial::Unauthorized))))
            );
        }
        assert!(
            check(&fixture.client(), &fixture.target, proof.token()?)
                .await?
                .is_none()
        );
        assert!(coordinator.close_and_drain().await.is_empty());
        fixture.runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn service_checkpoint_absent_recovery_rejects_authoritative_expiry_without_attaching_inventory()
-> Result {
    let fixture = Fixture::new(ObjectFormat::Sha256).await?;
    let (coordinator, ticket) = active(&fixture, [183; 16]).await?;
    let proof = seal(
        &fixture,
        &ticket,
        Arc::new(ArtifactStore::new(
            Arc::new(InMemory::new()),
            fixture.repository,
        )),
        1,
    )
    .await?;
    coordinator.fault_for_test(1);
    let registration = ticket
        .register_inputs(proof.clone(), identity()?)
        .map_err(|(e, _)| e)?;
    assert!(matches!(
        timeout(Duration::from_secs(10), ticket.wait_terminal()).await?,
        StagingState::Uncertain(_)
    ));
    mutate(
        &fixture.handle,
        "UPDATE catalog_operations SET expires_at_ms=0; UPDATE catalog_leases SET expires_at_ms=0"
            .into(),
    )
    .await?;
    coordinator.recover(&ticket)?;
    assert!(matches!(
        timeout(Duration::from_secs(10), ticket.wait_terminal()).await?,
        StagingState::Fenced(_)
    ));
    assert!(
        matches!(registration.wait().await, Err(e) if matches!(&*e, StagingError::Checkpoint(error) if matches!(&**error, InvocationError::Rejected(value) if value.output == StagingReply::Denied(PreparationDenial::Expired))))
    );
    let sql = cellule_runtime::primitives::sql::SqlCell::<RepositoryModule>::new(
        fixture.client(),
        fixture.target.clone(),
    )?;
    let result = sql
        .query(
            None,
            sql::statement(
                "SELECT input_checkpoint,input_checkpoint_digest FROM catalog_leases",
                vec![],
            ),
        )
        .await?;
    assert!(matches!(
        sql::rows(&result.output)?[0].as_slice(),
        [SqlValue::Null, SqlValue::Null]
    ));
    assert!(
        check(&fixture.client(), &fixture.target, proof.token()?)
            .await?
            .is_none()
    );
    assert!(coordinator.close_and_drain().await.is_empty());
    fixture.runtime.shutdown().await?;
    Ok(())
}
