use super::super::metadata::{
    MetadataSegment,
    tests::{builder, fill, fixture},
};
use super::*;
mod changes;
use canopy_object_storage::artifact::ArtifactStore;
use canopy_object_storage::external::MAX_ARTIFACT_BYTES;
use cellule_ltx::DiskBudget;
use object_store::{ObjectStore, ObjectStoreExt, memory::InMemory};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

fn oid(n: u64, format: ObjectFormat) -> ObjectId {
    let mut bytes = vec![0; format.bytes()];
    bytes[..8].copy_from_slice(&n.to_be_bytes());
    bytes.try_into().unwrap()
}
// Descriptor-tree fixtures do not claim native verification or publication.
pub(in crate::packs) fn source(n: u64, format: ObjectFormat) -> SourceRecord {
    let mut operation = [0; 16];
    operation[..8].copy_from_slice(&n.to_be_bytes());
    let descriptor = |size, byte| ArtifactDescriptor {
        size,
        digest: [byte; 32],
        manifest_digest: [byte + 1; 32],
    };
    let metadata = descriptor(16 << 10, 3);
    let pack = descriptor(128, 5);
    SourceRecord {
        metadata: StoredSegment {
            segment: SegmentDescriptor {
                identity: SegmentIdentity {
                    repository: [1; 16],
                    operation,
                    format,
                    pack_digest: pack.digest,
                    git_checksum: oid(1, format),
                    first_ordinal: 0,
                    object_count: 2,
                },
                edge_count: 0,
                inventory_digest: [9; 32],
                first_oid: oid(1, format),
                last_oid: oid(2, format),
                size: metadata.size,
                digest: metadata.digest,
            },
            artifact: metadata,
        },
        pack,
        index: descriptor(
            8 + 256 * 4 + 2 * (format.bytes() as u64 + 8) + 2 * format.bytes() as u64,
            7,
        ),
        pack_object_count: 2,
    }
}
fn store() -> (Arc<ArtifactStore>, Arc<dyn ObjectStore>) {
    let provider: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    (
        Arc::new(ArtifactStore::new(Arc::clone(&provider), [1; 16])),
        provider,
    )
}

#[tokio::test]
async fn source_catalog_split_cold_lookup_seek_and_retained_roots_for_both_formats() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let (store, _) = store();
        let index = SourceIndex::new(store, format);
        let mut root = None;
        for n in (0..SOURCE_FANOUT + 17).rev() {
            root = Some(
                index
                    .insert(root, [5; 16], source(n as u64, format))
                    .await?,
            );
        }
        let old = root.ok_or("root")?;
        assert_eq!(old.height, 1);
        assert_eq!(old.record_count, (SOURCE_FANOUT + 17) as u64);
        assert_eq!(old.object_count, old.record_count * 2);
        assert_eq!(index.insert(root, [6; 16], source(0, format)).await?, old);
        index.clear_cache()?;
        let stats = index.stats();
        assert_eq!(
            index.find(root, source(100, format).key()).await?,
            Some(source(100, format))
        );
        assert_eq!(
            index.stats().loaded_nodes - stats.loaded_nodes,
            u64::from(old.height) + 1
        );
        let mut cursor = index.cursor(root, Some(source(99, format).key()))?;
        for n in 100..SOURCE_FANOUT + 17 {
            assert_eq!(cursor.next().await?, Some(source(n as u64, format)));
        }
        assert!(cursor.next().await?.is_none());
        let mut changed = source(100, format);
        changed.index.manifest_digest[0] ^= 1;
        assert!(matches!(
            index.insert(root, [6; 16], changed).await,
            Err(IndexError::RangeOverlap)
        ));
        assert!(matches!(
            index.remove(root, [6; 16], changed).await,
            Err(IndexError::Stale)
        ));
        for n in 0..SOURCE_FANOUT + 17 {
            root = index
                .remove(root, [7; 16], source(n as u64, format))
                .await?;
        }
        assert!(root.is_none());
        assert_eq!(
            index.find(Some(old), source(100, format).key()).await?,
            Some(source(100, format))
        );
    }
    Ok(())
}

#[tokio::test]
async fn catalog_binding_context_summary_and_authenticated_corruption_fail_closed() -> Result {
    let format = ObjectFormat::Sha256;
    let (store, provider) = store();
    let index = SourceIndex::new(Arc::clone(&store), format);
    let original = source(1, format);
    let root = index.insert(None, [5; 16], original).await?;
    let mut forged = root;
    forged.record_count += 1;
    assert!(index.find(Some(forged), original.key()).await.is_err());
    let mut forged = root;
    forged.first_key.operation[0] ^= 1;
    assert!(index.find(Some(forged), original.key()).await.is_err());
    let wrong_format = SourceIndex::new(Arc::clone(&store), ObjectFormat::Sha1);
    assert!(wrong_format.find(Some(root), original.key()).await.is_err());
    let wrong_repo = SourceIndex::new(
        Arc::new(ArtifactStore::new(Arc::clone(&provider), [2; 16])),
        format,
    );
    assert!(wrong_repo.find(Some(root), original.key()).await.is_err());
    let key = ArtifactKey {
        operation: root.operation,
        binding_digest: root.artifact.digest,
        kind: ArtifactKind::CatalogNode,
    };
    let path = store.path(key, root.artifact.digest)?;
    provider
        .put(
            &canopy_object_storage::external::part(&path, 0),
            bytes::Bytes::from(vec![0; root.artifact.size as usize]).into(),
        )
        .await?;
    index.clear_cache()?;
    assert!(index.find(Some(root), original.key()).await.is_err());
    let mut cursor = index.cursor(Some(root), None)?;
    assert!(cursor.next().await.is_err());
    assert!(matches!(cursor.next().await, Err(IndexError::Integrity)));
    Ok(())
}

#[test]
fn source_descriptor_rejects_mismatched_bindings_bounds_and_ordinal_overflow() {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let original = source(1, format);
        assert!(original.validate([1; 16], format).is_ok());
        let mut invalid = Vec::new();
        let mut bad = original;
        bad.metadata.segment.identity.pack_digest[0] ^= 1;
        invalid.push(bad);
        let mut bad = original;
        bad.metadata.artifact.digest[0] ^= 1;
        invalid.push(bad);
        let mut bad = original;
        bad.metadata.artifact.size += 1;
        invalid.push(bad);
        let mut bad = original;
        bad.metadata.segment.identity.first_ordinal = u32::MAX;
        invalid.push(bad);
        let mut bad = original;
        bad.pack_object_count = 1;
        invalid.push(bad);
        let mut bad = original;
        bad.metadata.segment.identity.object_count = 0;
        invalid.push(bad);
        let mut bad = original;
        bad.metadata.segment.last_oid = bad.metadata.segment.first_oid;
        invalid.push(bad);
        let mut bad = original;
        bad.index.size -= 1;
        invalid.push(bad);
        let mut bad = original;
        bad.index.size += 1;
        invalid.push(bad);
        let mut bad = original;
        bad.pack.size = 1;
        invalid.push(bad);
        let mut bad = original;
        bad.pack.size = MAX_ARTIFACT_BYTES + 1;
        invalid.push(bad);
        let mut bad = original;
        bad.metadata.segment.edge_count = u64::MAX;
        invalid.push(bad);
        for bad in invalid {
            assert!(bad.validate([1; 16], format).is_err(), "accepted {bad:?}");
        }
        assert!(original.validate([2; 16], format).is_err());
    }
}

fn native_paths(root: &Path) -> Result<(PathBuf, PathBuf)> {
    let index = std::fs::read_dir(root.join("objects/pack"))?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .find(|p| p.extension().is_some_and(|ext| ext == "idx"))
        .ok_or("index")?;
    Ok((index.with_extension("pack"), index))
}
async fn upload(
    store: &ArtifactStore,
    key: ArtifactKey,
    path: &Path,
) -> Result<ArtifactDescriptor> {
    let bytes = std::fs::read(path)?;
    Ok(store
        .put(
            key,
            bytes.len() as u64,
            *blake3::hash(&bytes).as_bytes(),
            &mut bytes.as_slice(),
        )
        .await?)
}

struct DownloadLoader<'a> {
    store: &'a ArtifactStore,
    root: &'a Path,
    budget: DiskBudget,
}
impl SourceLoader for DownloadLoader<'_> {
    async fn load(
        &self,
        stored: StoredSegment,
    ) -> std::result::Result<Arc<MetadataSegment>, super::super::metadata::MetadataError> {
        MetadataSegment::download(
            self.root,
            self.budget.clone(),
            self.store,
            stored,
            super::super::metadata::tests::limits(),
        )
        .await
    }
}

#[tokio::test]
async fn native_artifacts_and_exact_shard_partition_bindings_for_both_formats() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let fixture = fixture(format, 16).await?;
        let (store, _) = store();
        let (pack_path, index_path) = native_paths(fixture.root.path())?;
        let key = |kind| ArtifactKey {
            operation: fixture.identity.operation,
            binding_digest: fixture.identity.pack_digest,
            kind,
        };
        let pack = upload(&store, key(ArtifactKind::Pack), &pack_path).await?;
        let index_artifact = upload(&store, key(ArtifactKind::Index), &index_path).await?;
        let mut sources = Vec::new();
        let mut local_segments: Vec<Arc<MetadataSegment>> = Vec::new();
        let halfway = fixture.index.len() / 2;
        for (first, count) in [(0, halfway), (halfway, fixture.index.len() - halfway)] {
            let identity = SegmentIdentity {
                first_ordinal: first,
                object_count: count,
                ..fixture.identity
            };
            let mut writer = builder(&fixture, DiskBudget::new(128 << 20), identity)?;
            let objects = fixture
                .index
                .ids_from(first)?
                .take(count as usize)
                .map(|oid| Ok(fixture.objects.get(&oid?).ok_or("object")?.clone()))
                .collect::<Result<Vec<_>>>()?;
            fill(&mut writer, &objects)?;
            let segment = Arc::new(writer.seal(&fixture.index)?);
            let metadata = Arc::clone(&segment).upload(&store).await?;
            sources.push(SourceRecord {
                metadata,
                pack,
                index: index_artifact,
                pack_object_count: fixture.index.len(),
            });
            local_segments.push(segment);
        }
        let checked = sources[0].verify_native_files(&pack_path, &index_path)?;
        checked.verify_source(sources[1])?;
        assert_eq!(checked.index().len(), fixture.index.len());
        for (source, segment) in sources.iter().zip(&local_segments) {
            let header = segment
                .header(source.metadata.segment.first_oid)?
                .ok_or("header")?;
            let entry = super::super::directory::DirectoryEntry {
                header,
                source: source.key(),
                location_version: 1,
            };
            source.verify_directory_entry(segment, entry)?;
            let mut bad = entry;
            bad.source.operation[0] ^= 1;
            assert!(source.verify_directory_entry(segment, bad).is_err());
            let mut bad = entry;
            bad.header.object.digest[0] ^= 1;
            assert!(matches!(
                source.verify_directory_entry(segment, bad),
                Err(IndexError::Metadata(
                    super::super::metadata::MetadataError::IdentityConflict
                ))
            ));
            let mut bad = entry;
            bad.location_version = 0;
            assert!(source.verify_directory_entry(segment, bad).is_err());
            assert!(
                source
                    .verify_directory_entry(
                        &local_segments[if source == &sources[0] { 1 } else { 0 }],
                        entry
                    )
                    .is_err()
            );
        }
        let mut coverage = PackCoverage::new(sources[0])?;
        for source in &sources {
            coverage.add(*source)?;
        }
        coverage.finish()?;
        let mut missing = PackCoverage::new(sources[0])?;
        missing.add(sources[0])?;
        assert!(missing.finish().is_err());
        let mut reversed = PackCoverage::new(sources[0])?;
        assert!(reversed.add(sources[1]).is_err());
        assert!(reversed.add(sources[0]).is_err());
        assert!(reversed.finish().is_err());
        let mut duplicate = PackCoverage::new(sources[0])?;
        duplicate.add(sources[0])?;
        assert!(duplicate.add(sources[0]).is_err());
        assert!(duplicate.finish().is_err());
        let mut altered = sources[1];
        altered.index.manifest_digest[0] ^= 1;
        assert!(checked.verify_source(altered).is_err());
        let mut coverage = PackCoverage::new(sources[0])?;
        coverage.add(sources[0])?;
        assert!(coverage.add(altered).is_err());
        assert!(coverage.finish().is_err());
        let mut endpoint = sources[0];
        endpoint.metadata.segment.first_oid = oid(1, format);
        assert!(checked.verify_source(endpoint).is_err());
        let catalog = SourceIndex::new(Arc::clone(&store), format);
        let root = catalog.insert(None, [5; 16], sources[0]).await?;
        let root = catalog.insert(Some(root), [5; 16], sources[1]).await?;
        catalog.clear_cache()?;
        for source in &sources {
            assert_eq!(catalog.find(Some(root), source.key()).await?, Some(*source));
        }
        let budget = DiskBudget::new(128 << 20);
        let loader = DownloadLoader {
            store: &store,
            root: fixture.root.path(),
            budget: budget.clone(),
        };
        let mut directory = super::super::directory::DirectoryBuilder::new(
            fixture.root.path(),
            DiskBudget::new(128 << 20),
            [1; 16],
            [8; 16],
            format,
            super::super::metadata::tests::limits(),
        )?;
        for segment in &local_segments {
            directory.add_segment(segment)?;
        }
        let directory = directory.seal()?;
        for source in &sources {
            let entry = directory
                .find(source.metadata.segment.first_oid)?
                .ok_or("entry")?;
            let resolved = catalog.resolve(Some(root), entry, &loader).await?;
            assert_eq!(resolved.record, *source);
            assert_eq!(
                resolved.metadata.header(entry.header.object.oid)?,
                Some(entry.header)
            );
            assert_eq!(budget.used(), source.metadata.artifact.size);
            drop(resolved);
            assert_eq!(budget.used(), 0);
            let mut conflict = entry;
            conflict.header.edge_digest[0] ^= 1;
            assert!(
                catalog
                    .resolve(Some(root), conflict, &loader)
                    .await
                    .is_err()
            );
            assert_eq!(budget.used(), 0);
            assert!(catalog.resolve(None, entry, &loader).await.is_err());
        }
        let mut bytes = std::fs::read(&pack_path)?;
        bytes[12] ^= 1;
        let bad_pack = fixture.root.path().join("corrupted.pack");
        std::fs::write(&bad_pack, &bytes)?;
        assert!(
            sources[0]
                .verify_native_files(&bad_pack, &index_path)
                .is_err()
        );
        let mut bytes = std::fs::read(&index_path)?;
        bytes[12] ^= 1;
        let bad_index = fixture.root.path().join("corrupted.idx");
        std::fs::write(&bad_index, &bytes)?;
        assert!(
            sources[0]
                .verify_native_files(&pack_path, &bad_index)
                .is_err()
        );
        // Rewrite only the declared BLAKE3: the native checksum must still fail.
        let mut forged = sources[0];
        forged.pack.digest = *blake3::hash(&std::fs::read(&bad_pack)?).as_bytes();
        forged.metadata.segment.identity.pack_digest = forged.pack.digest;
        assert!(forged.verify_native_files(&bad_pack, &index_path).is_err());
    }
    Ok(())
}
