use super::super::{
    directory::{DirectoryBuilder, DirectoryRun},
    metadata::{
        MetadataError, MetadataSegment, StoredSegment,
        tests::{Fixture, builder, fill, fixture, limits},
    },
    sources::{SourceRecord, tests::source},
};
use super::*;
use cellule_ltx::DiskBudget;
use object_store::{ObjectStore, ObjectStoreExt, memory::InMemory};
use std::path::Path;

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;
pub(in crate::packs) struct Prepared {
    pub(in crate::packs) fixture: Fixture,
    pub(in crate::packs) store: Arc<ArtifactStore>,
    provider: Arc<dyn ObjectStore>,
    snapshot: CatalogSnapshot,
    pub(in crate::packs) stored: StoredCatalog,
    pub(in crate::packs) indexes: Arc<CatalogIndexes>,
}
async fn prepared(format: ObjectFormat) -> Result<Prepared> {
    prepared_for_repository(format, [1; 16]).await
}
pub(in crate::packs) async fn prepared_for_repository(
    format: ObjectFormat,
    repository: [u8; 16],
) -> Result<Prepared> {
    let mut fixture = fixture(format, 4).await?;
    fixture.identity.repository = repository;
    let provider: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let store = Arc::new(ArtifactStore::new(Arc::clone(&provider), repository));
    let mut writer = builder(&fixture, DiskBudget::new(128 << 20), fixture.identity)?;
    fill(
        &mut writer,
        &fixture.objects.values().cloned().collect::<Vec<_>>(),
    )?;
    let segment = Arc::new(writer.seal(&fixture.index)?);
    let metadata = Arc::clone(&segment).upload(&store).await?;
    let index_path = std::fs::read_dir(fixture.root.path().join("objects/pack"))?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .find(|path| path.extension().is_some_and(|ext| ext == "idx"))
        .ok_or("index")?;
    let mut artifacts = Vec::new();
    for (kind, path) in [
        (ArtifactKind::Pack, index_path.with_extension("pack")),
        (ArtifactKind::Index, index_path),
    ] {
        let bytes = std::fs::read(path)?;
        artifacts.push(
            store
                .put(
                    ArtifactKey {
                        operation: fixture.identity.operation,
                        binding_digest: fixture.identity.pack_digest,
                        kind,
                    },
                    bytes.len() as u64,
                    *blake3::hash(&bytes).as_bytes(),
                    &mut bytes.as_slice(),
                )
                .await?,
        );
    }
    let source = SourceRecord {
        metadata,
        pack: artifacts[0],
        index: artifacts[1],
        pack_object_count: fixture.index.len(),
    };
    let sources = SourceIndex::new(Arc::clone(&store), format)
        .insert(None, [6; 16], source)
        .await?;
    let mut directory = DirectoryBuilder::new(
        fixture.root.path(),
        DiskBudget::new(128 << 20),
        repository,
        [5; 16],
        format,
        limits(),
    )?;
    directory.add_segment(&segment)?;
    let run = Arc::new(directory.seal()?).upload(&store).await?;
    let indexes = Arc::new(CatalogIndexes::new(Arc::clone(&store), format));
    let root = indexes.ranges().insert(None, [5; 16], run).await?;
    let mut directory = DirectorySnapshot::empty(repository, format);
    directory.append(indexes.ranges(), root).await?;
    let snapshot = CatalogSnapshot {
        directory: directory.upload(&store, [7; 16]).await?,
        sources: Some(sources),
    };
    let stored = snapshot.upload(&store, [8; 16]).await?;
    Ok(Prepared {
        fixture,
        store,
        provider,
        snapshot,
        stored,
        indexes,
    })
}
struct Loader<'a> {
    root: &'a Path,
    store: &'a ArtifactStore,
    budget: DiskBudget,
}
impl RunLoader for Loader<'_> {
    async fn load(
        &self,
        run: super::super::directory::StoredRun,
    ) -> std::result::Result<Arc<DirectoryRun>, MetadataError> {
        DirectoryRun::download(self.root, self.budget.clone(), self.store, run, limits()).await
    }
}
impl SourceLoader for Loader<'_> {
    async fn load(
        &self,
        segment: StoredSegment,
    ) -> std::result::Result<Arc<MetadataSegment>, MetadataError> {
        MetadataSegment::download(
            self.root,
            self.budget.clone(),
            self.store,
            segment,
            limits(),
        )
        .await
    }
}

#[tokio::test]
async fn native_catalog_roundtrip_and_old_roots_remain_independently_readable() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let prepared = prepared(format).await?;
        assert!(prepared.stored.artifact.size <= u64::from(CATALOG_BYTES));
        assert_eq!(
            CatalogSnapshot::download(&prepared.store, prepared.stored).await?,
            prepared.snapshot
        );
        let reader = CatalogReader::open(Arc::clone(&prepared.indexes), prepared.stored).await?;
        assert_eq!(reader.stored(), prepared.stored);
        let before = prepared.indexes.stats();
        let next = prepared.snapshot.upload(&prepared.store, [13; 16]).await?;
        let next = CatalogReader::open(Arc::clone(&prepared.indexes), next).await?;
        let after = prepared.indexes.stats();
        assert_eq!(after.0.loaded_nodes, before.0.loaded_nodes);
        assert_eq!(after.1.loaded_nodes, before.1.loaded_nodes);
        assert!(after.1.cache_hits > before.1.cache_hits);
        assert_ne!(next.stored(), reader.stored());
        let budget = DiskBudget::new(128 << 20);
        let loader = Loader {
            root: prepared.fixture.root.path(),
            store: &prepared.store,
            budget: budget.clone(),
        };
        for (oid, (expected, _)) in &prepared.fixture.objects {
            let resolved = reader
                .lookup(*oid, &loader, &loader)
                .await?
                .ok_or("resolved")?;
            assert_eq!(resolved.entry.header.object, *expected);
            assert_eq!(
                resolved.source.metadata.header(*oid)?,
                Some(resolved.entry.header)
            );
            assert_eq!(budget.used(), resolved.source.record.metadata.artifact.size);
            drop(resolved);
            assert_eq!(budget.used(), 0);
        }
        let missing = if format == ObjectFormat::Sha1 {
            ObjectId::Sha1([255; 20])
        } else {
            ObjectId::Sha256([255; 32])
        };
        assert!(reader.lookup(missing, &loader, &loader).await?.is_none());
        let directory = DirectorySnapshot::empty([1; 16], format)
            .upload(&prepared.store, [9; 16])
            .await?;
        let empty = CatalogSnapshot {
            directory,
            sources: None,
        }
        .upload(&prepared.store, [10; 16])
        .await?;
        let empty = CatalogReader::open(Arc::clone(&prepared.indexes), empty).await?;
        let oid = *prepared.fixture.objects.keys().next().ok_or("oid")?;
        assert!(empty.lookup(oid, &loader, &loader).await?.is_none());
        assert!(reader.lookup(oid, &loader, &loader).await?.is_some());
    }
    Ok(())
}

#[test]
fn catalog_codec_rejects_truncation_trailing_wrong_domains_and_oversized_descriptors() -> Result {
    let format = ObjectFormat::Sha256;
    let snapshot = CatalogSnapshot {
        directory: StoredSnapshot {
            repository: [1; 16],
            operation: [2; 16],
            format,
            artifact: ArtifactDescriptor {
                size: 128,
                digest: [3; 32],
                manifest_digest: [4; 32],
            },
        },
        sources: None,
    };
    let bytes = snapshot.encode([5; 16])?;
    assert_eq!(CatalogSnapshot::decode(&bytes)?, (snapshot, [5; 16]));
    for n in 0..bytes.len() {
        assert!(CatalogSnapshot::decode(&bytes[..n]).is_err());
    }
    let mut trailing = bytes.clone();
    trailing.push(0);
    assert!(CatalogSnapshot::decode(&trailing).is_err());
    let mut domain = bytes.clone();
    domain[4] ^= 1;
    assert!(CatalogSnapshot::decode(&domain).is_err());
    assert!(CatalogSnapshot::decode(&vec![0; CATALOG_BYTES as usize + 1]).is_err());
    let mut oversized = snapshot;
    oversized.directory.artifact.size = 65537;
    assert!(oversized.encode([5; 16]).is_err());
    let stored = StoredCatalog {
        repository: [1; 16],
        operation: [5; 16],
        format,
        artifact: ArtifactDescriptor {
            size: u64::from(CATALOG_BYTES) + 1,
            digest: [3; 32],
            manifest_digest: [4; 32],
        },
    };
    assert!(stored.validate().is_err());
    Ok(())
}

#[test]
fn persisted_catalog_contract_matches_independent_big_endian_golden_vectors() -> Result {
    use crate::packs::directory::SegmentKey;
    for (format, golden) in [
        (
            ObjectFormat::Sha1,
            include_str!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../../docs/design/catalog-root-v1-sha1.hex"
            )),
        ),
        (
            ObjectFormat::Sha256,
            include_str!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../../docs/design/catalog-root-v1-sha256.hex"
            )),
        ),
    ] {
        // Codec fixtures only; these descriptors do not name populated files.
        let snapshot = CatalogSnapshot {
            directory: StoredSnapshot {
                repository: [1; 16],
                operation: [2; 16],
                format,
                artifact: ArtifactDescriptor {
                    size: 128,
                    digest: [3; 32],
                    manifest_digest: [4; 32],
                },
            },
            sources: Some(SourceRoot {
                operation: [12; 16],
                height: 1,
                artifact: ArtifactDescriptor {
                    size: 4096,
                    digest: [10; 32],
                    manifest_digest: [11; 32],
                },
                first_key: SegmentKey {
                    operation: [6; 16],
                    digest: [7; 32],
                },
                last_key: SegmentKey {
                    operation: [8; 16],
                    digest: [9; 32],
                },
                record_count: 11,
                object_count: 64,
            }),
        };
        let bytes = hex::decode(golden.trim())?;
        assert_eq!(snapshot.encode([5; 16])?, bytes);
        assert_eq!(CatalogSnapshot::decode(&bytes)?, (snapshot, [5; 16]));
    }
    Ok(())
}

#[tokio::test]
async fn missing_or_wrong_source_roots_cannot_resolve_a_directory_entry() -> Result {
    let prepared = prepared(ObjectFormat::Sha256).await?;
    let mut missing = prepared.snapshot;
    missing.sources = None;
    let stored = missing.upload(&prepared.store, [9; 16]).await?;
    assert!(
        CatalogReader::open(Arc::clone(&prepared.indexes), stored)
            .await
            .is_err()
    );
    let index = SourceIndex::new(Arc::clone(&prepared.store), ObjectFormat::Sha256);
    let unrelated = index
        .insert(None, [10; 16], source(1, ObjectFormat::Sha256))
        .await?;
    let mut dangling = prepared.snapshot;
    dangling.sources = Some(unrelated);
    let stored = dangling.upload(&prepared.store, [11; 16]).await?;
    let reader = CatalogReader::open(Arc::clone(&prepared.indexes), stored).await?;
    let loader = Loader {
        root: prepared.fixture.root.path(),
        store: &prepared.store,
        budget: DiskBudget::new(128 << 20),
    };
    let oid = *prepared.fixture.objects.keys().next().ok_or("oid")?;
    assert!(reader.lookup(oid, &loader, &loader).await.is_err());
    assert!(reader.headers(&[oid, oid], &loader, &loader).await.is_err());
    assert_eq!(loader.budget.used(), 0);
    // A valid authenticated directory root is not a source-tree node.
    let mut wrong = prepared.snapshot;
    let root = wrong.sources.as_mut().ok_or("source root")?;
    root.artifact = wrong.directory.artifact;
    root.operation = wrong.directory.operation;
    let stored = wrong.upload(&prepared.store, [12; 16]).await?;
    assert!(
        CatalogReader::open(Arc::clone(&prepared.indexes), stored)
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn catalog_context_summary_and_artifact_corruption_fail_before_reads() -> Result {
    let prepared = prepared(ObjectFormat::Sha256).await?;
    let mut wrong = prepared.stored;
    wrong.format = ObjectFormat::Sha1;
    assert!(
        CatalogReader::open(Arc::clone(&prepared.indexes), wrong)
            .await
            .is_err()
    );
    let mut wrong = prepared.snapshot;
    wrong.directory.format = ObjectFormat::Sha1;
    let stored = wrong.upload(&prepared.store, [9; 16]).await?;
    assert!(
        CatalogReader::open(Arc::clone(&prepared.indexes), stored)
            .await
            .is_err()
    );
    let mut wrong = prepared.snapshot;
    wrong.directory.repository = [2; 16];
    assert!(wrong.upload(&prepared.store, [10; 16]).await.is_err());
    let mut wrong = prepared.snapshot;
    wrong.sources.as_mut().ok_or("source root")?.object_count += 1;
    let stored = wrong.upload(&prepared.store, [11; 16]).await?;
    assert!(
        CatalogReader::open(Arc::clone(&prepared.indexes), stored)
            .await
            .is_err()
    );
    let path = prepared
        .store
        .path(prepared.stored.key(), prepared.stored.artifact.digest)?;
    prepared
        .provider
        .put(
            &canopy_object_storage::external::part(&path, 0),
            bytes::Bytes::from(vec![0; prepared.stored.artifact.size as usize]).into(),
        )
        .await?;
    assert!(
        CatalogReader::open(Arc::clone(&prepared.indexes), prepared.stored)
            .await
            .is_err()
    );
    Ok(())
}

mod files;
