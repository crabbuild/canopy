use super::*;
use super::{
    prepare::{cleaned, opened},
    publishing::{edit, plan, state, update},
    reconcile::graph,
};
use crate::packs::{
    catalog::{CatalogFiles, CatalogIndexes, CatalogReader, CatalogSnapshot},
    directory::snapshot::DirectorySnapshot,
    metadata::tests::limits,
};
use canopy_object_storage::artifact::ArtifactStore;
use cellule_ltx::DiskBudget;

mod recovery;

struct Inventory {
    provider: Arc<dyn object_store::ObjectStore>,
    store: Arc<ArtifactStore>,
}
async fn seed(fixture: &Fixture, roots: usize) -> Result<Inventory> {
    let provider: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let store = Arc::new(ArtifactStore::new(
        Arc::clone(&provider),
        fixture.repository,
    ));
    for n in 0..roots {
        push(
            fixture,
            &Inventory {
                provider: Arc::clone(&provider),
                store: Arc::clone(&store),
            },
            n as u8 + 20,
            4 + n % 3,
        )
        .await?;
    }
    Ok(Inventory { provider, store })
}
async fn push(fixture: &Fixture, inventory: &Inventory, operation: u8, blobs: usize) -> Result {
    let graph = graph(
        fixture,
        Arc::clone(&inventory.provider),
        Arc::clone(&inventory.store),
        [operation; 16],
        blobs,
    )
    .await?;
    let proof = Box::pin(graph.prepared.ref_proof(
        plan(vec![update(
            &format!("refs/heads/b-{operation}"),
            None,
            Some(graph.initial),
        )]),
        graph.root.path(),
        graph.budget.clone(),
        limits(),
    ))
    .await?;
    let result = fixture
        .client()
        .command::<PublishCatalogRefs>(&fixture.target, identity()?, proof)
        .await?;
    assert!(matches!(result.output, PublicationReply::Published(_)));
    Ok(())
}
struct Prepared {
    compact: PreparedCompaction,
    root: tempfile::TempDir,
    budget: DiskBudget,
    files: Arc<CatalogFiles>,
    indexes: Arc<CatalogIndexes>,
}
async fn prepare_compaction(
    fixture: &Fixture,
    inventory: &Inventory,
    operation: u8,
    selected: &[usize],
) -> Result<Prepared> {
    let (base, files, indexes) =
        opened(fixture, [operation; 16], Arc::clone(&inventory.store)).await?;
    let root = tempfile::TempDir::new()?;
    let budget = DiskBudget::new(128 << 20);
    let compact = PreparedCompaction::prepare(
        root.path(),
        budget.clone(),
        base,
        selected,
        CompactionLimits {
            spool: limits(),
            output: crate::packs::metadata::MetadataLimits {
                max_file_bytes: 16 << 10,
                cache_kib: 16,
            },
            ..CompactionLimits::default()
        },
    )
    .await?;
    Ok(Prepared {
        compact,
        root,
        budget,
        files,
        indexes,
    })
}
async fn refs(handle: &CellHandle) -> Result<Vec<u8>> {
    Ok(handle
        .query(0, 64 << 10, |connection| {
            let mut statement =
                connection.prepare("SELECT name,oid,version FROM refs ORDER BY name")?;
            let rows = statement
                .query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, Option<Vec<u8>>>(1)?,
                        row.get::<_, i64>(2)?,
                    ))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            let generation: u64 =
                connection.query_row("SELECT generation FROM ref_generation", [], |row| {
                    row.get(0)
                })?;
            serde_json::to_vec(&(rows, generation)).map_err(|_| Error::Command("fixture ref state"))
        })
        .await?)
}
async fn outcomes(handle: &CellHandle) -> Result<u64> {
    let bytes = handle
        .query(0, 64, |connection| {
            let count: u64 =
                connection.query_row("SELECT count(*) FROM catalog_compactions", [], |row| {
                    row.get(0)
                })?;
            Ok(count.to_be_bytes().to_vec())
        })
        .await?;
    Ok(u64::from_be_bytes(
        bytes.try_into().map_err(|_| "invalid outcome count")?,
    ))
}
async fn reject(
    fixture: &Fixture,
    certificate: CatalogCertificate,
    reason: PreparationDenial,
) -> Result {
    let before = state(&fixture.handle).await?;
    let count = outcomes(&fixture.handle).await?;
    let result = fixture
        .client()
        .command::<PublishCatalogCompaction>(&fixture.target, identity()?, certificate)
        .await;
    assert!(
        matches!(result, Err(InvocationError::Rejected(ref value)) if value.output == CompactionReply::Denied(reason)),
        "{result:?}"
    );
    assert_eq!(state(&fixture.handle).await?, before);
    assert_eq!(outcomes(&fixture.handle).await?, count);
    Ok(())
}

#[tokio::test]
async fn full_ingress_compaction_preserves_native_inventory_refs_and_exact_replay() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let fixture = Fixture::new(format).await?;
        let inventory = seed(&fixture, 32).await?;
        let prepared =
            prepare_compaction(&fixture, &inventory, 180, &(0..32).collect::<Vec<_>>()).await?;
        let original_catalog = prepared.compact.base().catalog.ok_or("base")?;
        let original = CatalogSnapshot::download(&inventory.store, original_catalog).await?;
        let directory = DirectorySnapshot::download(&inventory.store, original.directory).await?;
        assert_eq!(directory.level_zero.len(), 32);
        let mut ids = std::collections::BTreeSet::new();
        for root in &directory.level_zero {
            let mut cursor = prepared.indexes.ranges().cursor(Some(*root), None)?;
            while let Some(stored) = cursor.next().await? {
                use crate::packs::directory::snapshot::RunLoader;
                let run = prepared.files.load(stored).await?;
                let mut after = None;
                loop {
                    let entries = run.entries_after(after)?;
                    if entries.is_empty() {
                        break;
                    }
                    after = entries.last().map(|entry| entry.header.object.oid);
                    ids.extend(entries.iter().map(|entry| entry.header.object.oid));
                }
            }
        }
        assert_eq!(prepared.compact.object_count(), ids.len() as u64);
        let old_reader =
            CatalogReader::open(Arc::clone(&prepared.indexes), original_catalog).await?;
        let new_reader =
            CatalogReader::open(Arc::clone(&prepared.indexes), prepared.compact.catalog()).await?;
        for oid in &ids {
            assert_eq!(
                old_reader
                    .lookup(*oid, &*prepared.files, &*prepared.files)
                    .await?
                    .ok_or("old")?
                    .entry,
                new_reader
                    .lookup(*oid, &*prepared.files, &*prepared.files)
                    .await?
                    .ok_or("new")?
                    .entry
            );
        }
        assert!(
            fixture
                .client()
                .query::<CheckCompletedCompaction>(&fixture.target, None, fixture.begin([180; 16]))
                .await?
                .output
                .is_none()
        );
        let before = refs(&fixture.handle).await?;
        let certificate = prepared.compact.certificate().await?;
        assert!(certificate.bytes()?.len() <= CERTIFICATE_BYTES as usize);
        let mutation = identity()?;
        let committed = fixture
            .client()
            .command::<PublishCatalogCompaction>(&fixture.target, mutation, certificate.clone())
            .await?;
        assert!(
            matches!(committed.output,CompactionReply::Published(value) if value.generation==33)
        );
        assert_eq!(refs(&fixture.handle).await?, before);
        let current =
            CatalogSnapshot::download(&inventory.store, prepared.compact.catalog()).await?;
        assert_eq!(current.sources, original.sources);
        assert_eq!(
            DirectorySnapshot::download(&inventory.store, current.directory)
                .await?
                .level_zero
                .len(),
            1
        );
        // Old pinned roots remain usable after publication; compaction deletes no bytes.
        for oid in ids {
            assert!(
                old_reader
                    .lookup(oid, &*prepared.files, &*prepared.files)
                    .await?
                    .is_some()
            );
        }
        let replay = fixture
            .client()
            .command::<PublishCatalogCompaction>(&fixture.target, mutation, certificate.clone())
            .await?;
        assert_eq!(replay.receipt, committed.receipt);
        assert_eq!(replay.output, committed.output);
        assert_eq!(
            fixture
                .client()
                .command::<PublishCatalogCompaction>(&fixture.target, identity()?, certificate)
                .await?
                .output,
            committed.output
        );
        assert_eq!(
            fixture
                .client()
                .query::<CheckCompletedCompaction>(&fixture.target, None, fixture.begin([180; 16]))
                .await?
                .output,
            Some(committed.output)
        );
        let counts = fixture.counts().await?;
        rejected(
            fixture
                .client()
                .command::<BeginPreparation>(&fixture.target, identity()?, fixture.begin([180; 16]))
                .await,
            PreparationDenial::Conflict,
        );
        assert_eq!(fixture.counts().await?, counts);
        assert_eq!(outcomes(&fixture.handle).await?, 1);
        drop(prepared.compact);
        cleaned(prepared.root.path(), &prepared.budget).await?;
        fixture.runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn compaction_reconciles_new_ingress_but_cannot_resurrect_replaced_roots() -> Result {
    let fixture = Fixture::new(ObjectFormat::Sha256).await?;
    let inventory = seed(&fixture, 2).await?;
    let prepared = prepare_compaction(&fixture, &inventory, 180, &[1, 0]).await?;
    let competitor = prepare_compaction(&fixture, &inventory, 181, &[0, 1]).await?;
    let certificate = prepared.compact.certificate().await?;
    fixture
        .client()
        .command::<RegisterCatalogAttestation>(&fixture.target, identity()?, certificate.clone())
        .await?;
    push(&fixture, &inventory, 99, 8).await?;
    reject(&fixture, certificate, PreparationDenial::Conflict).await?;
    let selected = prepared.compact.reconcile().await?;
    assert_eq!(selected.token(), prepared.compact.token());
    assert_eq!(
        selected.inventory_digest(),
        prepared.compact.inventory_digest()
    );
    let before = refs(&fixture.handle).await?;
    let result = fixture
        .client()
        .command::<PublishCatalogCompaction>(
            &fixture.target,
            identity()?,
            selected.certificate().await?,
        )
        .await?;
    assert!(matches!(result.output,CompactionReply::Published(value) if value.generation==4));
    assert_eq!(refs(&fixture.handle).await?, before);
    let snapshot = CatalogSnapshot::download(&inventory.store, selected.catalog()).await?;
    assert_eq!(
        DirectorySnapshot::download(&inventory.store, snapshot.directory)
            .await?
            .level_zero
            .len(),
        2
    );
    assert!(matches!(
        competitor.compact.reconcile().await,
        Err(CatalogPreparationError::Catalog(
            crate::packs::directory::index::IndexError::Stale
        ))
    ));
    assert_eq!(outcomes(&fixture.handle).await?, 1);
    fixture.runtime.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn maintenance_certificate_purpose_authority_tampering_and_late_rollback_are_enforced()
-> Result {
    let fixture = Fixture::new(ObjectFormat::Sha1).await?;
    let inventory = seed(&fixture, 3).await?;
    let prepared = prepare_compaction(&fixture, &inventory, 180, &[0, 1]).await?;
    let certificate = prepared.compact.certificate().await?;
    let mut data = certificate.data()?;
    data.compaction = false;
    reject(
        &fixture,
        CatalogCertificate::seal(&data, &[16; 32])?,
        PreparationDenial::Unauthorized,
    )
    .await?;
    data.compaction = true;
    data.refs_digest = Some([1; 32]);
    assert!(CatalogCertificate::seal(&data, &[16; 32]).is_err());
    data.refs_digest = None;
    data.object_count += 1;
    let mut tampered = certificate.clone();
    let mut e = BoundedEncoder::new(960)?;
    data.encode(&mut e)?;
    tampered.body = e.finish();
    reject(&fixture, tampered, PreparationDenial::Unauthorized).await?;
    edit(&fixture,"UPDATE repository_identity SET owner='other'; INSERT INTO repository_members VALUES('owner','write');").await?;
    reject(
        &fixture,
        certificate.clone(),
        PreparationDenial::Unauthorized,
    )
    .await?;
    assert!(prepared.compact.certificate().await.is_err());
    assert_eq!(
        fixture
            .client()
            .query::<CheckCompletedCompaction>(&fixture.target, None, fixture.begin([180; 16]))
            .await?
            .output,
        Some(CompactionReply::Denied(PreparationDenial::Unauthorized))
    );
    edit(&fixture,"UPDATE repository_identity SET owner='owner'; DELETE FROM repository_members WHERE account='owner'; CREATE TRIGGER forced_compaction_failure BEFORE INSERT ON catalog_compactions BEGIN SELECT RAISE(ABORT,'forced late compaction failure'); END;").await?;
    let before = state(&fixture.handle).await?;
    assert!(
        fixture
            .client()
            .command::<PublishCatalogCompaction>(&fixture.target, identity()?, certificate.clone())
            .await
            .is_err()
    );
    assert_eq!(state(&fixture.handle).await?, before);
    assert_eq!(outcomes(&fixture.handle).await?, 0);
    edit(&fixture, "DROP TRIGGER forced_compaction_failure;").await?;
    fixture
        .client()
        .command::<PublishCatalogCompaction>(&fixture.target, identity()?, certificate)
        .await?;
    assert!(
        edit(&fixture, "UPDATE catalog_compactions SET actor='other';")
            .await
            .is_err()
    );
    assert!(
        edit(
            &fixture,
            "INSERT OR REPLACE INTO catalog_compactions SELECT * FROM catalog_compactions;"
        )
        .await
        .is_err()
    );
    assert_eq!(outcomes(&fixture.handle).await?, 1);
    fixture.runtime.shutdown().await?;
    Ok(())
}
