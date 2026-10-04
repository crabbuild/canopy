use super::*;
use super::{
    prepare::{cleaned, opened},
    publishing::{edit, plan, state, update},
    reconcile::graph,
};
use crate::packs::{
    catalog::CatalogSnapshot, metadata::tests::limits, ref_state::RefStateSnapshotRoot,
};
use canopy_object_storage::artifact::ArtifactStore;
use cellule_ltx::DiskBudget;

pub(super) async fn empty(
    fixture: &Fixture,
    operation: [u8; 16],
    store: Arc<ArtifactStore>,
) -> Result<(PreparedCatalog, tempfile::TempDir, DiskBudget)> {
    let (base, _, _) = opened(fixture, operation, store).await?;
    let root = tempfile::TempDir::new()?;
    let budget = DiskBudget::new(64 << 20);
    let prepared = CatalogPreparation::new(root.path(), budget.clone(), base, limits())
        .await?
        .finish()
        .await?;
    Ok((prepared, root, budget))
}
fn initialized(reply: InitializationReply) -> Result<GenerationFact> {
    match reply {
        InitializationReply::Initialized(fact) => Ok(*fact),
        InitializationReply::Denied(why) => Err(format!("initialization denied {why:?}").into()),
    }
}
pub(super) async fn registered(
    fixture: &Fixture,
    prepared: &PreparedCatalog,
    input: InitialRefProof,
    mutation: MutationIdentity,
) -> Result<(
    cellule_runtime::PreparedCommand<InitializeCatalogRefs>,
    RegisteredRootRecovery,
)> {
    let command = fixture
        .client()
        .prepare_command::<InitializeCatalogRefs>(&fixture.target, mutation, input)
        .await?;
    let record = super::super::recovery::persist(
        &prepared.base.session,
        &command,
        super::super::recovery::Kind::Initialization,
        &prepared.base.indexes().store(),
        identity()?,
        0,
    )
    .await?;
    Ok((command, record))
}
async fn reject(
    fixture: &Fixture,
    command: cellule_runtime::PreparedCommand<InitializeCatalogRefs>,
    registered: &RegisteredRootRecovery,
    store: &ArtifactStore,
    reason: PreparationDenial,
) -> Result {
    let before = state(&fixture.handle).await?;
    let evidence = command.evidence().clone();
    let result = command.execute().await?;
    assert_eq!(result.output, InitializationReply::Denied(reason));
    assert_eq!(state(&fixture.handle).await?, before);
    assert!(matches!(
        fixture.client().resolve(&evidence).await?,
        cellule_runtime::Resolution::Committed(
            cellule_runtime::cell::executor::StoredOutcome::Success { .. }
        )
    ));
    let recovered = registered
        .recover_initialization(&fixture.client(), store, &fixture.authority())
        .await;
    assert!(
        matches!(recovered,Err(PublicationError::Initialization(InvocationError::Rejected(ref value))) if value.receipt==result.receipt && value.output==result.output)
    );
    Ok(())
}

#[tokio::test]
async fn unregistered_initialization_keeps_sdk_and_catalog_absent() -> Result {
    let fixture = Fixture::new(ObjectFormat::Sha1).await?;
    let store = Arc::new(ArtifactStore::new(
        Arc::new(InMemory::new()),
        fixture.repository,
    ));
    let (prepared, root, budget) = empty(&fixture, [229; 16], store).await?;
    let command = fixture
        .client()
        .prepare_command::<InitializeCatalogRefs>(
            &fixture.target,
            identity()?,
            prepared.empty_ref_initialization().await?,
        )
        .await?;
    let evidence = command.evidence().clone();
    let before = state(&fixture.handle).await?;
    let result = command.execute().await;
    assert!(
        matches!(result, Err(InvocationError::NotStarted(_))),
        "{result:?}"
    );
    assert_eq!(state(&fixture.handle).await?, before);
    assert!(matches!(
        fixture.client().resolve(&evidence).await?,
        cellule_runtime::Resolution::Absent
    ));
    drop(prepared);
    cleaned(root.path(), &budget).await?;
    fixture.runtime.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn cold_initialization_records_original_expiry_and_revocation_denials() -> Result {
    for (sql, reason) in [
        (
            "UPDATE catalog_leases SET expires_at_ms=0; UPDATE catalog_operations SET expires_at_ms=0",
            PreparationDenial::Expired,
        ),
        (
            "UPDATE repository_identity SET owner='replacement'",
            PreparationDenial::Unauthorized,
        ),
    ] {
        let fixture = Fixture::new(ObjectFormat::Sha256).await?;
        let store = Arc::new(ArtifactStore::new(
            Arc::new(InMemory::new()),
            fixture.repository,
        ));
        let (prepared, root, budget) = empty(&fixture, [231; 16], store.clone()).await?;
        let proof = prepared.empty_ref_initialization().await?;
        let (command, registered) = registered(&fixture, &prepared, proof, identity()?).await?;
        let evidence = command.evidence().clone();
        drop(command);
        drop(prepared);
        cleaned(root.path(), &budget).await?;
        edit(&fixture, sql).await?;
        let before = state(&fixture.handle).await?;
        let result = registered
            .recover_initialization(&fixture.client(), &store, &fixture.authority())
            .await;
        assert!(
            matches!(result,Err(PublicationError::Initialization(InvocationError::Rejected(ref value))) if value.output==InitializationReply::Denied(reason)),
            "{result:?}"
        );
        assert_eq!(state(&fixture.handle).await?, before);
        assert!(matches!(
            fixture.client().resolve(&evidence).await?,
            cellule_runtime::Resolution::Committed(
                cellule_runtime::cell::executor::StoredOutcome::Success { .. }
            )
        ));
        fixture.runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn fresh_initialization_commits_joint_empty_roots_and_enables_first_ref_preparation() -> Result
{
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let fixture = Fixture::new(format).await?;
        let provider: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let store = Arc::new(ArtifactStore::new(provider.clone(), fixture.repository));
        let (prepared, root, budget) = empty(&fixture, [220; 16], store.clone()).await?;
        fn send<T: Send>(value: T) -> T {
            value
        }
        let proof = send(prepared.empty_ref_initialization()).await?;
        let retry = prepared.empty_ref_initialization().await?;
        assert_eq!(retry, proof);
        let mut e = BoundedEncoder::new(INITIALIZATION_BYTES)?;
        proof.encode(&mut e)?;
        let bytes = e.finish();
        assert!(bytes.len() <= INITIALIZATION_BYTES as usize);
        let mut d = BoundedDecoder::new(&bytes, INITIALIZATION_BYTES)?;
        assert_eq!(InitialRefProof::decode(&mut d)?, proof);
        d.finish()?;
        for cut in 0..bytes.len() {
            let mut d = BoundedDecoder::new(&bytes[..cut], INITIALIZATION_BYTES)?;
            assert!(InitialRefProof::decode(&mut d).is_err());
        }
        assert!(
            fixture
                .client()
                .query::<CheckInitializedCatalog>(&fixture.target, None, fixture.begin([220; 16]))
                .await?
                .output
                .is_none()
        );
        let mutation = identity()?;
        let (command, registered) =
            registered(&fixture, &prepared, proof.clone(), mutation).await?;
        let committed = command.clone().execute().await?;
        let fact = initialized(committed.output.clone())?;
        let mut e = BoundedEncoder::new(512)?;
        committed.output.encode(&mut e)?;
        let reply_bytes = e.finish();
        let mut d = BoundedDecoder::new(&reply_bytes, 512)?;
        assert_eq!(InitializationReply::decode(&mut d)?, committed.output);
        d.finish()?;
        for invalid in [
            GenerationFact {
                generation: 2,
                ..fact
            },
            GenerationFact { refs: None, ..fact },
        ] {
            assert!(
                InitializationReply::Initialized(Box::new(invalid))
                    .encode(&mut BoundedEncoder::new(512)?)
                    .is_err()
            );
            let mut e = BoundedEncoder::new(512)?;
            e.write_u8(0)?;
            invalid.encode(&mut e)?;
            let bytes = e.finish();
            assert!(InitializationReply::decode(&mut BoundedDecoder::new(&bytes, 512)?).is_err());
        }
        assert_eq!(
            (fact.generation, fact.catalog, fact.refs),
            (1, Some(prepared.catalog()), Some(proof.refs))
        );
        assert_eq!(
            fact.certificate,
            Some(*blake3::hash(&proof.certificate.bytes()?).as_bytes())
        );
        let snapshot = proof.refs.read(&store).await?;
        assert_eq!(
            (snapshot.generation, snapshot.root, snapshot.default_branch),
            (0, None, "refs/heads/main".into())
        );
        let catalog = CatalogSnapshot::download(&store, prepared.catalog()).await?;
        assert!(catalog.sources.is_none());
        assert_eq!(
            fixture
                .client()
                .command::<InitializeCatalogRefs>(&fixture.target, mutation, proof.clone())
                .await?
                .receipt,
            committed.receipt
        );
        assert!(matches!(
            fixture
                .client()
                .command::<InitializeCatalogRefs>(&fixture.target, identity()?, proof.clone())
                .await,
            Err(InvocationError::NotStarted(_))
        ));
        let recovered = registered
            .recover_initialization(&fixture.client(), &store, &fixture.authority())
            .await?;
        assert_eq!(
            (recovered.output, recovered.receipt),
            (committed.output.clone(), committed.receipt)
        );
        assert_eq!(
            fixture
                .client()
                .query::<CheckInitializedCatalog>(&fixture.target, None, fixture.begin([220; 16]))
                .await?
                .output,
            Some(fact)
        );
        for mode in 0..4 {
            let mut request = fixture.begin([220; 16]);
            if mode == 0 {
                request.actor = "other".into();
            } else if mode == 1 {
                request.request_digest = [91; 32];
            } else if mode == 2 {
                request.operation = [221; 16];
            } else {
                request.repository[15] ^= 1;
            }
            assert!(
                fixture
                    .client()
                    .query::<CheckInitializedCatalog>(&fixture.target, None, request)
                    .await?
                    .output
                    .is_none()
            );
        }
        rejected(
            fixture
                .client()
                .command::<BeginPreparation>(&fixture.target, identity()?, fixture.begin([220; 16]))
                .await,
            PreparationDenial::Conflict,
        );
        let next = graph(&fixture, provider, store.clone(), [222; 16], 4).await?;
        assert_eq!(next.prepared.base().refs, Some(proof.refs));
        let planned = plan(vec![update("refs/heads/main", None, Some(next.initial))]);
        let refs = next.prepared.prepare_ref_snapshot(&planned).await?;
        assert_eq!(refs.snapshot().read(&store).await?.generation, 1);
        assert!(matches!(
            next.prepared.empty_ref_initialization().await,
            Err(InitializationPreparationError::Ineligible)
        ));
        for sql in [
            "UPDATE catalog_initialization SET actor='other'",
            "DELETE FROM catalog_initialization",
            "INSERT OR REPLACE INTO catalog_initialization SELECT * FROM catalog_initialization",
        ] {
            let before = state(&fixture.handle).await?;
            assert!(edit(&fixture, sql).await.is_err());
            assert_eq!(state(&fixture.handle).await?, before);
        }
        drop(next.prepared);
        cleaned(next.root.path(), &next.budget).await?;
        drop(prepared);
        cleaned(root.path(), &budget).await?;
        fixture.runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn initialization_refuses_history_head_changes_revocation_expiry_and_forged_authority()
-> Result {
    for (sql, reason) in [
        (
            "INSERT INTO refs VALUES('refs/heads/deleted',NULL,1)",
            PreparationDenial::Conflict,
        ),
        (
            "UPDATE ref_generation SET generation=1",
            PreparationDenial::Conflict,
        ),
        (
            "UPDATE ref_generation SET default_branch='refs/heads/other'",
            PreparationDenial::Conflict,
        ),
        (
            "UPDATE repository_identity SET owner='replacement'",
            PreparationDenial::Unauthorized,
        ),
        (
            "UPDATE catalog_leases SET expires_at_ms=0; UPDATE catalog_operations SET expires_at_ms=0",
            PreparationDenial::Expired,
        ),
    ] {
        let fixture = Fixture::new(ObjectFormat::Sha1).await?;
        let store = Arc::new(ArtifactStore::new(
            Arc::new(InMemory::new()),
            fixture.repository,
        ));
        let (prepared, root, budget) = empty(&fixture, [223; 16], store.clone()).await?;
        let proof = prepared.empty_ref_initialization().await?;
        let (command, registered) = registered(&fixture, &prepared, proof, identity()?).await?;
        edit(&fixture, sql).await?;
        reject(&fixture, command, &registered, &store, reason).await?;
        drop(prepared);
        cleaned(root.path(), &budget).await?;
        fixture.runtime.shutdown().await?;
    }
    for mode in 0..3 {
        let fixture = Fixture::new(ObjectFormat::Sha256).await?;
        let store = Arc::new(ArtifactStore::new(
            Arc::new(InMemory::new()),
            fixture.repository,
        ));
        let (prepared, root, budget) = empty(&fixture, [224; 16], store.clone()).await?;
        let proof = prepared.empty_ref_initialization().await?;
        let mut forged = proof.clone();
        let mut data = proof.certificate.data()?;
        if mode == 0 {
            data.token.owner.epoch += 1;
        }
        if mode == 2 {
            data.tenant = [96; 16];
        }
        forged.certificate =
            CatalogCertificate::seal(&data, &if mode == 1 { [17; 32] } else { [16; 32] })?;
        let (command, registered) = registered(&fixture, &prepared, forged, identity()?).await?;
        if mode == 0 {
            let before = state(&fixture.handle).await?;
            let evidence = command.evidence().clone();
            assert!(matches!(
                command.execute().await,
                Err(InvocationError::NotStarted(_))
            ));
            assert_eq!(state(&fixture.handle).await?, before);
            assert!(matches!(
                fixture.client().resolve(&evidence).await?,
                cellule_runtime::Resolution::Absent
            ));
        } else {
            reject(
                &fixture,
                command,
                &registered,
                &store,
                PreparationDenial::Unauthorized,
            )
            .await?;
        }
        let mut e = BoundedEncoder::new(128)?;
        proof.refs.encode(&mut e)?;
        let mut bytes = e.finish();
        let end = bytes.len() - 1;
        bytes[end] ^= 1;
        let mut d = BoundedDecoder::new(&bytes, 128)?;
        let refs = RefStateSnapshotRoot::decode(&mut d)?;
        d.finish()?;
        let raw = InitialRefProof {
            refs,
            certificate: proof.certificate,
        };
        assert!(
            raw.encode(&mut BoundedEncoder::new(INITIALIZATION_BYTES)?)
                .is_err()
        );
        drop(prepared);
        cleaned(root.path(), &budget).await?;
        fixture.runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn initialization_late_failure_rolls_back_roots_checkpoint_and_outcome_and_races_commit_once()
-> Result {
    let fixture = Fixture::new(ObjectFormat::Sha256).await?;
    let store = Arc::new(ArtifactStore::new(
        Arc::new(InMemory::new()),
        fixture.repository,
    ));
    let (first, root_a, budget_a) = empty(&fixture, [225; 16], store.clone()).await?;
    let (second, root_b, budget_b) = empty(&fixture, [226; 16], store).await?;
    let a = first.empty_ref_initialization().await?;
    let b = second.empty_ref_initialization().await?;
    let (command_a, registered_a) = registered(&fixture, &first, a, identity()?).await?;
    let (command_b, registered_b) = registered(&fixture, &second, b, identity()?).await?;
    edit(&fixture,"CREATE TRIGGER fail_initialization BEFORE INSERT ON catalog_initialization BEGIN SELECT RAISE(ABORT,'late initialization fault'); END;").await?;
    let before = state(&fixture.handle).await?;
    let failed = command_a.clone().execute().await;
    assert!(
        matches!(failed,Err(InvocationError::NotStarted(Error::Sqlite(rusqlite::Error::SqliteFailure(_,Some(ref message))))) if message == "late initialization fault"),
        "{failed:?}"
    );
    assert_eq!(state(&fixture.handle).await?, before);
    fixture
        .handle
        .query(0, 128, |db| {
            assert_eq!(
                db.query_row(
                    "SELECT count(*) FROM catalog_leases WHERE recovery_phase IS NOT NULL",
                    [],
                    |row| row.get::<_, u64>(0)
                )?,
                0
            );
            Ok(Vec::new())
        })
        .await?;
    assert!(matches!(
        fixture.client().resolve(command_a.evidence()).await?,
        cellule_runtime::Resolution::Absent
    ));
    assert!(
        fixture
            .client()
            .query::<CheckInitializedCatalog>(&fixture.target, None, fixture.begin([225; 16]))
            .await?
            .output
            .is_none()
    );
    edit(&fixture, "DROP TRIGGER fail_initialization").await?;
    let client = fixture.client();
    let (result_a, result_b) = tokio::join!(command_a.execute(), command_b.execute());
    let result_a = result_a?;
    let result_b = result_b?;
    let (committed, losing, winner, loser, registered_loser) =
        match (&result_a.output, &result_b.output) {
            (
                InitializationReply::Initialized(_),
                InitializationReply::Denied(PreparationDenial::Conflict),
            ) => (result_a, result_b, [225; 16], [226; 16], registered_b),
            (
                InitializationReply::Denied(PreparationDenial::Conflict),
                InitializationReply::Initialized(_),
            ) => (result_b, result_a, [226; 16], [225; 16], registered_a),
            other => {
                return Err(
                    format!("initialization must have exactly one winner: {other:?}").into(),
                );
            }
        };
    let fact = initialized(committed.output)?;
    assert_eq!(fact.generation, 1);
    assert_eq!(
        client
            .query::<CheckInitializedCatalog>(&fixture.target, None, fixture.begin(winner))
            .await?
            .output,
        Some(fact)
    );
    assert!(
        client
            .query::<CheckInitializedCatalog>(&fixture.target, None, fixture.begin(loser))
            .await?
            .output
            .is_none()
    );
    let recovered = registered_loser
        .recover_initialization(&client, &first.base.indexes().store(), &fixture.authority())
        .await;
    assert!(
        matches!(recovered,Err(PublicationError::Initialization(InvocationError::Rejected(ref value))) if value.receipt==losing.receipt && value.output==losing.output)
    );
    drop(first);
    drop(second);
    cleaned(root_a.path(), &budget_a).await?;
    cleaned(root_b.path(), &budget_b).await?;
    fixture.runtime.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn initialization_exact_outcome_survives_owner_restore_and_pending_old_attempts_cannot_write()
-> Result {
    let fixture = Fixture::new(ObjectFormat::Sha1).await?;
    let store = Arc::new(ArtifactStore::new(
        Arc::new(InMemory::new()),
        fixture.repository,
    ));
    let (first, root_a, budget_a) = empty(&fixture, [227; 16], store.clone()).await?;
    let (second, root_b, budget_b) = empty(&fixture, [228; 16], store).await?;
    let a = first.empty_ref_initialization().await?;
    let b = second.empty_ref_initialization().await?;
    let mutation = identity()?;
    let (command_a, registered_a) = registered(&fixture, &first, a.clone(), mutation).await?;
    let (command_b, registered_b) = registered(&fixture, &second, b.clone(), identity()?).await?;
    let snapshot_b = command_b.snapshot();
    let body_b = command_b.input_bytes().to_vec();
    let committed = command_a.execute().await?;
    fixture.handle.drain().await?;
    fixture.runtime.shutdown().await?;
    let session = SessionId::from_bytes([229; 16]);
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
            fixture.root.path().join("initialization-restored.sqlite"),
            Owner {
                session,
                endpoint: "https://initialization-restored.invalid".into(),
            },
        )
        .await?;
    assert!(handle.owner_fence().epoch > first.token().owner.epoch);
    let client = CellClient::local(Arc::clone(&fixture.registry), handle.clone());
    let exact = client
        .command::<InitializeCatalogRefs>(&fixture.target, mutation, a.clone())
        .await?;
    assert_eq!(
        (exact.output, exact.receipt),
        (committed.output.clone(), committed.receipt)
    );
    assert!(matches!(
        client
            .command::<InitializeCatalogRefs>(&fixture.target, identity()?, a)
            .await,
        Err(InvocationError::NotStarted(_))
    ));
    let recovered = registered_a
        .recover_initialization(&client, &first.base.indexes().store(), &fixture.authority())
        .await?;
    assert_eq!(
        (recovered.output, recovered.receipt),
        (committed.output.clone(), committed.receipt)
    );
    let before = state(&handle).await?;
    // Cold recovery must settle the absent original under the new owner. It
    // cannot depend on opening the old owner's now-invalid live capability.
    let denied = registered_b
        .recover_initialization(
            &client,
            &second.base.indexes().store(),
            &fixture.authority(),
        )
        .await;
    assert!(
        matches!(denied,Err(PublicationError::Initialization(InvocationError::Rejected(ref value))) if value.output==InitializationReply::Denied(PreparationDenial::Stale)),
        "{denied:?}"
    );
    let stale = client
        .restore_command::<InitializeCatalogRefs>(snapshot_b, body_b)?
        .execute()
        .await?;
    assert_eq!(
        stale.output,
        InitializationReply::Denied(PreparationDenial::Stale)
    );
    assert_eq!(state(&handle).await?, before);
    assert!(
        matches!(denied,Err(PublicationError::Initialization(InvocationError::Rejected(ref value))) if value.receipt==stale.receipt && value.output==stale.output)
    );
    assert_eq!(
        client
            .query::<CheckInitializedCatalog>(&fixture.target, None, fixture.begin([227; 16]))
            .await?
            .output,
        Some(initialized(committed.output)?)
    );
    drop(first);
    drop(second);
    cleaned(root_a.path(), &budget_a).await?;
    cleaned(root_b.path(), &budget_b).await?;
    runtime.shutdown().await?;
    Ok(())
}
