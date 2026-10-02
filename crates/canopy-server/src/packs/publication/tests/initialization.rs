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

async fn empty(
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
async fn reject(fixture: &Fixture, input: InitialRefProof, reason: PreparationDenial) -> Result {
    let before = state(&fixture.handle).await?;
    let result = fixture
        .client()
        .command::<InitializeCatalogRefs>(&fixture.target, identity()?, input)
        .await;
    assert!(
        matches!(result,Err(InvocationError::Rejected(ref value)) if value.output==InitializationReply::Denied(reason)),
        "{result:?}"
    );
    assert_eq!(state(&fixture.handle).await?, before);
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
        let committed = fixture
            .client()
            .command::<InitializeCatalogRefs>(&fixture.target, mutation, proof.clone())
            .await?;
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
        assert_eq!(
            fixture
                .client()
                .command::<InitializeCatalogRefs>(&fixture.target, identity()?, proof.clone())
                .await?
                .output,
            committed.output
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
        let (prepared, root, budget) = empty(&fixture, [223; 16], store).await?;
        let proof = prepared.empty_ref_initialization().await?;
        edit(&fixture, sql).await?;
        reject(&fixture, proof, reason).await?;
        drop(prepared);
        cleaned(root.path(), &budget).await?;
        fixture.runtime.shutdown().await?;
    }
    let fixture = Fixture::new(ObjectFormat::Sha256).await?;
    let store = Arc::new(ArtifactStore::new(
        Arc::new(InMemory::new()),
        fixture.repository,
    ));
    let (prepared, root, budget) = empty(&fixture, [224; 16], store).await?;
    let proof = prepared.empty_ref_initialization().await?;
    let mut stale = proof.clone();
    let mut data = stale.certificate.data()?;
    data.token.owner.epoch += 1;
    stale.certificate = CatalogCertificate::seal(&data, &[16; 32])?;
    reject(&fixture, stale, PreparationDenial::Stale).await?;
    let mut forged = proof.clone();
    forged.certificate = CatalogCertificate::seal(&proof.certificate.data()?, &[17; 32])?;
    reject(&fixture, forged, PreparationDenial::Unauthorized).await?;
    let mut wrong = proof.clone();
    let mut data = wrong.certificate.data()?;
    data.tenant = [96; 16];
    wrong.certificate = CatalogCertificate::seal(&data, &[16; 32])?;
    reject(&fixture, wrong, PreparationDenial::Unauthorized).await?;
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
    edit(&fixture,"CREATE TRIGGER fail_initialization BEFORE INSERT ON catalog_initialization BEGIN SELECT RAISE(ABORT,'late initialization fault'); END;").await?;
    let before = state(&fixture.handle).await?;
    assert!(
        fixture
            .client()
            .command::<InitializeCatalogRefs>(&fixture.target, identity()?, a.clone())
            .await
            .is_err()
    );
    assert_eq!(state(&fixture.handle).await?, before);
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
    let (result_a, result_b) = tokio::join!(
        client.command::<InitializeCatalogRefs>(&fixture.target, identity()?, a.clone()),
        client.command::<InitializeCatalogRefs>(&fixture.target, identity()?, b.clone()),
    );
    let (committed, losing, winner, loser) = match (result_a, result_b) {
        (Ok(committed), Err(InvocationError::Rejected(rejected))) => {
            assert_eq!(
                rejected.output,
                InitializationReply::Denied(PreparationDenial::Conflict)
            );
            (committed, b, [225; 16], [226; 16])
        }
        (Err(InvocationError::Rejected(rejected)), Ok(committed)) => {
            assert_eq!(
                rejected.output,
                InitializationReply::Denied(PreparationDenial::Conflict)
            );
            (committed, a, [226; 16], [225; 16])
        }
        other => {
            return Err(format!("initialization must have exactly one winner: {other:?}").into());
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
    reject(&fixture, losing, PreparationDenial::Conflict).await?;
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
    let committed = fixture
        .client()
        .command::<InitializeCatalogRefs>(&fixture.target, mutation, a.clone())
        .await?;
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
    assert_eq!(
        client
            .command::<InitializeCatalogRefs>(&fixture.target, identity()?, a)
            .await?
            .output,
        committed.output
    );
    let before = state(&handle).await?;
    let stale = client
        .command::<InitializeCatalogRefs>(&fixture.target, identity()?, b)
        .await;
    assert!(
        matches!(stale,Err(InvocationError::Rejected(ref value)) if value.output==InitializationReply::Denied(PreparationDenial::Stale))
    );
    assert_eq!(state(&handle).await?, before);
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
