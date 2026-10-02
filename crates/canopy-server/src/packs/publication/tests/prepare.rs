use super::*;
use crate::packs::{
    catalog::{CatalogFileLimits, CatalogFiles, CatalogIndexes, CatalogReader, CatalogSnapshot},
    closure::ClosureError,
    directory::snapshot::DirectorySnapshot,
    metadata::{MetadataSegment, tests::limits},
    verification::{
        PhysicalPackWitness, PhysicalVerifier,
        physical::tests::{
            Prepared,
            independence::{git_input, upload_pair_for_operation},
            physical_limits, prepared_for_context, prepared_for_store,
        },
    },
};
use canopy_object_storage::artifact::ArtifactStore;
use cellule_ltx::DiskBudget;
use std::{path::Path, time::Duration};
type NativeAttempt = (
    Prepared,
    Arc<PreparationBaseResolver>,
    Arc<CatalogFiles>,
    Arc<CatalogIndexes>,
);
pub(super) async fn opened_native(
    fixture: &Fixture,
    operation: [u8; 16],
    blobs: usize,
) -> Result<NativeAttempt> {
    let provider: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let store = Arc::new(ArtifactStore::new(provider.clone(), fixture.repository));
    let (base, files, indexes) = opened(fixture, operation, Arc::clone(&store)).await?;
    let native = prepared_for_store(
        fixture.format,
        blobs,
        base.context().operation,
        provider,
        store,
    )
    .await?;
    Ok((native, base, files, indexes))
}

pub(super) async fn opened(
    fixture: &Fixture,
    operation: [u8; 16],
    store: Arc<ArtifactStore>,
) -> Result<(
    Arc<PreparationBaseResolver>,
    Arc<CatalogFiles>,
    Arc<CatalogIndexes>,
)> {
    let started = fixture
        .client()
        .command::<BeginPreparation>(&fixture.target, identity()?, fixture.begin(operation))
        .await?;
    let token = lease(started.output)?.token;
    let indexes = Arc::new(CatalogIndexes::new(Arc::clone(&store), fixture.format));
    let files = Arc::new(CatalogFiles::new(
        fixture.root.path(),
        DiskBudget::new(64 << 20),
        store,
        fixture.format,
        CatalogFileLimits::default(),
    )?);
    let base = Arc::new(
        PreparationBaseResolver::open(
            fixture.client(),
            fixture.target.clone(),
            check(token),
            Arc::clone(&indexes),
            Arc::clone(&files),
            Some(started.receipt),
        )
        .await?,
    );
    Ok((base, files, indexes))
}
pub(super) async fn physical(
    prepared: &Prepared,
    root: &Path,
    budget: DiskBudget,
) -> Result<(PhysicalPackWitness, Vec<Arc<MetadataSegment>>)> {
    let mut physical = PhysicalVerifier::download(
        root,
        budget,
        &prepared.store,
        prepared.descriptor,
        physical_limits(),
        crate::native_resources::NativeResources::default()
            .scope(crate::native_resources::NativeClass::Foreground),
    )
    .await?;
    let halfway = prepared.descriptor.object_count / 2;
    let mut segments = Vec::new();
    for count in [1, halfway, prepared.descriptor.object_count - halfway - 1] {
        segments.push(physical.inspect_next_shard(count).await?);
    }
    Ok((physical.finish().await?, segments))
}
pub(super) async fn cleaned(root: &Path, budget: &DiskBudget) -> Result {
    tokio::time::timeout(Duration::from_secs(5), async {
        while budget.used() != 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    assert_eq!(std::fs::read_dir(root)?.count(), 0);
    Ok(())
}

#[tokio::test]
async fn complete_physical_partitions_build_exact_catalogs_and_reuse_the_certified_base() -> Result
{
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let fixture = Fixture::new(format).await?;
        let (prepared, base, files, indexes) = opened_native(&fixture, [2; 16], 1600).await?;
        let token = base.context_token();
        let root = tempfile::TempDir::new()?;
        let budget = DiskBudget::new(256 << 20);
        let run_limits = crate::packs::metadata::MetadataLimits {
            max_file_bytes: 16 << 10,
            cache_kib: 16,
        };
        let mut assembler = CatalogPreparation::new_with_run_limits(
            root.path(),
            budget.clone(),
            base,
            limits(),
            run_limits,
        )
        .await?;
        let mut expected_segments = Vec::new();
        // Repeated exact inputs must not add source leaves or duplicate objects.
        for _ in 0..2 {
            let (witness, segments) = physical(&prepared, root.path(), budget.clone()).await?;
            assembler.begin_pack(witness)?;
            expected_segments = segments.iter().map(|s| s.descriptor()).collect();
            for segment in segments {
                assembler.add_segment(segment).await?;
            }
            assembler.finish_pack().await?;
        }
        let proof = assembler.finish().await?;
        assert_eq!(proof.token(), token);
        assert_eq!(proof.base().generation, 0);
        assert_eq!(proof.object_count(), prepared.fixture.objects.len() as u64);
        assert_eq!(proof.input_count(), 1);
        assert_eq!(
            proof.edge_count(),
            prepared
                .fixture
                .objects
                .values()
                .map(|(_, edges)| edges
                    .iter()
                    .map(|e| (e.child, e.expected_kind))
                    .collect::<std::collections::BTreeSet<_>>()
                    .len() as u64)
                .sum::<u64>()
        );
        proof.ensure_live()?;
        let stored = proof.catalog();
        let snapshot = CatalogSnapshot::download(&prepared.store, stored).await?;
        let directory = DirectorySnapshot::download(&prepared.store, snapshot.directory).await?;
        assert_eq!(directory.level_zero.len(), 1);
        let run_root = directory.level_zero[0];
        assert!(run_root.record_count > crate::packs::directory::snapshot::LEVEL_ZERO_ROOTS as u64);
        assert_eq!(run_root.object_count, proof.object_count());
        let mut run_cursor = indexes.ranges().cursor(Some(run_root), None)?;
        let mut run_count = 0;
        while let Some(run) = run_cursor.next().await? {
            assert!(run.run.size <= run_limits.max_file_bytes);
            run_count += 1;
        }
        assert_eq!(run_count, run_root.record_count);
        let sources = indexes.sources();
        let mut cursor = sources.cursor(snapshot.sources, None)?;
        let mut found = Vec::new();
        while let Some(record) = cursor.next().await? {
            assert_eq!(record.native(), prepared.descriptor);
            found.push(record.metadata.segment);
        }
        found.sort_by_key(|segment| segment.identity.first_ordinal);
        assert_eq!(found, expected_segments);
        let reader = CatalogReader::open(Arc::clone(&indexes), stored).await?;
        for ids in prepared
            .fixture
            .objects
            .keys()
            .copied()
            .collect::<Vec<_>>()
            .chunks(512)
        {
            let headers = reader.headers(ids, &*files, &*files).await?;
            for (oid, header) in ids.iter().zip(headers) {
                assert_eq!(
                    header.ok_or("header")?.object,
                    prepared.fixture.objects[oid].0
                );
            }
        }
        drop(proof);
        cleaned(root.path(), &budget).await?;
        // Fixture-only publication supplies a trusted base for the next stage;
        // production certificate issuance/final publication is still separate.
        fixture.install_catalog(1, stored).await?;
        let (base, _, _) = opened(&fixture, [47; 16], Arc::clone(&prepared.store)).await?;
        let next = CatalogPreparation::new(root.path(), budget.clone(), base, limits())
            .await?
            .finish()
            .await?;
        assert_eq!(next.object_count(), 0);
        assert_eq!(next.input_count(), 0);
        assert_eq!(next.base().catalog, Some(stored));
        let unchanged = CatalogSnapshot::download(&prepared.store, next.catalog()).await?;
        assert_eq!(unchanged.sources, snapshot.sources);
        assert_eq!(
            DirectorySnapshot::download(&prepared.store, unchanged.directory).await?,
            DirectorySnapshot::download(&prepared.store, snapshot.directory).await?
        );
        drop(next);
        cleaned(root.path(), &budget).await?;
        let commit = prepared
            .fixture
            .objects
            .values()
            .find(|(object, _)| object.kind == crate::ObjectKind::Commit)
            .ok_or("commit")?
            .0;
        let pack = git_input(
            prepared.fixture.root.path(),
            &["pack-objects", "--stdout", "--no-reuse-delta"],
            format!("{}\n", hex::encode(commit.oid)).as_bytes(),
        )
        .await?;
        let (base, files, indexes) =
            opened(&fixture, [50; 16], Arc::clone(&prepared.store)).await?;
        let descriptor =
            upload_pair_for_operation(&prepared, commit.oid, &pack, base.context().operation)
                .await?;
        let mut physical = PhysicalVerifier::download(
            root.path(),
            budget.clone(),
            &prepared.store,
            descriptor,
            physical_limits(),
            crate::native_resources::NativeResources::default()
                .scope(crate::native_resources::NativeClass::Foreground),
        )
        .await?;
        let segment = physical.inspect_next_shard(1).await?;
        let mut assembler =
            CatalogPreparation::new(root.path(), budget.clone(), base, limits()).await?;
        assembler.begin_pack(physical.finish().await?)?;
        assembler.add_segment(segment).await?;
        assembler.finish_pack().await?;
        let extension = assembler.finish().await?;
        assert_eq!(extension.base().catalog, Some(stored));
        assert_eq!(extension.object_count(), 1);
        let extended = CatalogSnapshot::download(&prepared.store, extension.catalog()).await?;
        let old_directory =
            DirectorySnapshot::download(&prepared.store, snapshot.directory).await?;
        let new_directory =
            DirectorySnapshot::download(&prepared.store, extended.directory).await?;
        assert_eq!(
            &new_directory.level_zero[..old_directory.level_zero.len()],
            old_directory.level_zero.as_slice()
        );
        assert_eq!(
            new_directory.level_zero.len(),
            old_directory.level_zero.len() + 1
        );
        let reader = CatalogReader::open(indexes, extension.catalog()).await?;
        assert_eq!(
            reader.headers(&[commit.oid], &*files, &*files).await?[0]
                .ok_or("commit header")?
                .object,
            commit
        );
        drop(extension);
        cleaned(root.path(), &budget).await?;
        fixture.runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn incomplete_or_out_of_order_inputs_poison_catalog_preparation() -> Result {
    let fixture = Fixture::new(ObjectFormat::Sha256).await?;
    let (prepared, base, _, _) = opened_native(&fixture, [2; 16], 4).await?;
    let root = tempfile::TempDir::new()?;
    let budget = DiskBudget::new(256 << 20);
    for order in [false, true] {
        let (witness, segments) = physical(&prepared, root.path(), budget.clone()).await?;
        let mut assembler =
            CatalogPreparation::new(root.path(), budget.clone(), Arc::clone(&base), limits())
                .await?;
        assembler.begin_pack(witness)?;
        if order {
            assert!(
                assembler
                    .add_segment(Arc::clone(&segments[1]))
                    .await
                    .is_err()
            );
        } else {
            assembler.add_segment(Arc::clone(&segments[0])).await?;
            assert!(assembler.finish_pack().await.is_err());
        }
        assert!(assembler.finish().await.is_err());
        drop(segments);
        cleaned(root.path(), &budget).await?;
    }
    // Successful physical verification in a different operation grants no
    // authority to inject that source into this catalog attempt.
    let wrong = prepared_for_context(fixture.format, 4, fixture.repository, [3; 16]).await?;
    let (witness, segments) = physical(&wrong, root.path(), budget.clone()).await?;
    let mut assembler =
        CatalogPreparation::new(root.path(), budget.clone(), base, limits()).await?;
    assert!(assembler.begin_pack(witness).is_err());
    assert!(assembler.finish().await.is_err());
    drop(segments);
    cleaned(root.path(), &budget).await?;
    fixture.runtime.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn physical_validity_cannot_publish_missing_graph_dependencies() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let fixture = Fixture::new(format).await?;
        let (prepared, base, _, _) = opened_native(&fixture, [2; 16], 4).await?;
        let commit = prepared
            .fixture
            .objects
            .values()
            .find(|(o, _)| o.kind == crate::ObjectKind::Commit)
            .ok_or("commit")?
            .0;
        let pack = git_input(
            prepared.fixture.root.path(),
            &["pack-objects", "--stdout", "--no-reuse-delta"],
            format!("{}\n", hex::encode(commit.oid)).as_bytes(),
        )
        .await?;
        let descriptor =
            upload_pair_for_operation(&prepared, commit.oid, &pack, base.context().operation)
                .await?;
        let root = tempfile::TempDir::new()?;
        let budget = DiskBudget::new(256 << 20);
        let mut physical = PhysicalVerifier::download(
            root.path(),
            budget.clone(),
            &prepared.store,
            descriptor,
            physical_limits(),
            crate::native_resources::NativeResources::default()
                .scope(crate::native_resources::NativeClass::Foreground),
        )
        .await?;
        let segment = physical.inspect_next_shard(1).await?;
        let mut assembler =
            CatalogPreparation::new(root.path(), budget.clone(), base, limits()).await?;
        assembler.begin_pack(physical.finish().await?)?;
        assembler.add_segment(segment).await?;
        assembler.finish_pack().await?;
        assert!(matches!(
            assembler.finish().await,
            Err(CatalogPreparationError::Closure(ClosureError::Missing(_)))
        ));
        cleaned(root.path(), &budget).await?;
        fixture.runtime.shutdown().await?;
    }
    Ok(())
}

#[test]
fn canceled_catalog_finish_keeps_the_private_workspace_until_queued_work_drains() -> Result {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()?;
    runtime.block_on(async {
        let fixture = Fixture::new(ObjectFormat::Sha256).await?;
        let store = Arc::new(ArtifactStore::new(
            Arc::new(InMemory::new()),
            fixture.repository,
        ));
        let (base, _, _) = opened(&fixture, [48; 16], store).await?;
        let root = tempfile::TempDir::new()?;
        let budget = DiskBudget::new(128 << 20);
        let assembler =
            CatalogPreparation::new(root.path(), budget.clone(), base, limits()).await?;
        let workspace = std::fs::read_dir(root.path())?
            .next()
            .ok_or("workspace")??
            .path();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let started = Arc::new(tokio::sync::Notify::new());
        let notify = Arc::clone(&started);
        let blocked = tokio::task::spawn_blocking(move || {
            notify.notify_one();
            release_rx.recv().unwrap();
        });
        started.notified().await;
        let mut finish = Box::pin(assembler.finish());
        assert!(
            tokio::time::timeout(Duration::from_millis(25), &mut finish)
                .await
                .is_err()
        );
        drop(finish);
        assert!(workspace.exists());
        assert_eq!(
            budget.used(),
            crate::packs::metadata::growth::INITIAL_BYTES * 3
        );
        release_tx.send(())?;
        blocked.await?;
        cleaned(root.path(), &budget).await?;
        fixture.runtime.shutdown().await?;
        Ok::<_, Box<dyn std::error::Error>>(())
    })
}

#[tokio::test]
async fn catalog_admission_failure_leaves_no_private_workspace_or_proof() -> Result {
    let fixture = Fixture::new(ObjectFormat::Sha1).await?;
    let store = Arc::new(ArtifactStore::new(
        Arc::new(InMemory::new()),
        fixture.repository,
    ));
    let (base, _, _) = opened(&fixture, [49; 16], store).await?;
    let root = tempfile::TempDir::new()?;
    let budget = DiskBudget::new(1);
    assert!(
        CatalogPreparation::new(root.path(), budget.clone(), base, limits())
            .await
            .is_err()
    );
    cleaned(root.path(), &budget).await?;
    fixture.runtime.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn a_different_artifact_backend_cannot_supply_publication_input_proofs() -> Result {
    let fixture = Fixture::new(ObjectFormat::Sha256).await?;
    let destination = Arc::new(ArtifactStore::new(
        Arc::new(InMemory::new()),
        fixture.repository,
    ));
    let (base, _, _) = opened(&fixture, [2; 16], destination).await?;
    let source = prepared_for_context(
        fixture.format,
        4,
        fixture.repository,
        base.context().operation,
    )
    .await?;
    let root = tempfile::TempDir::new()?;
    let budget = DiskBudget::new(256 << 20);
    let (witness, segments) = physical(&source, root.path(), budget.clone()).await?;
    // A clone of the exact service capability retains the physical binding.
    witness.verify_store(&source.store.clone())?;
    let mut assembler =
        CatalogPreparation::new(root.path(), budget.clone(), base, limits()).await?;
    assert!(matches!(
        assembler.begin_pack(witness),
        Err(CatalogPreparationError::Physical(_))
    ));
    assert!(assembler.finish().await.is_err());
    drop(segments);
    cleaned(root.path(), &budget).await?;
    fixture.runtime.shutdown().await?;
    Ok(())
}
