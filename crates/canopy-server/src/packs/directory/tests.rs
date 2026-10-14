use super::*;
use crate::packs::metadata::tests::{Fixture, builder, fill, fixture, limits};
use object_store::{ObjectStore, ObjectStoreExt, memory::InMemory};

mod compaction_inventory;
mod coverage;
mod partition;
mod partition_lifetime;

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;
fn segment(fixture: &Fixture, budget: DiskBudget, operation: [u8; 16]) -> Result<MetadataSegment> {
    let mut identity = fixture.identity;
    identity.operation = operation;
    let mut writer = builder(fixture, budget, identity)?;
    fill(
        &mut writer,
        &fixture.objects.values().cloned().collect::<Vec<_>>(),
    )?;
    Ok(writer.seal(&fixture.index)?)
}
fn directory(fixture: &Fixture, budget: DiskBudget) -> Result<DirectoryBuilder> {
    Ok(DirectoryBuilder::new(
        fixture.root.path(),
        budget,
        fixture.identity.repository,
        [3; 16],
        fixture.identity.format,
        limits(),
    )?)
}

#[tokio::test]
async fn canonical_runs_deduplicate_native_shards_and_choose_stable_sources() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let fixture = fixture(format, 40).await?;
        let budget = DiskBudget::new(128 << 20);
        let first = segment(&fixture, budget.clone(), [1; 16])?;
        let second = segment(&fixture, budget.clone(), [2; 16])?;
        let mut a = directory(&fixture, budget.clone())?;
        a.add_segment(&second)?;
        a.add_segment(&first)?;
        a.add_segment(&first)?;
        let a = a.seal()?;
        let mut b = directory(&fixture, budget.clone())?;
        b.add_segment(&first)?;
        b.add_segment(&second)?;
        let b = b.seal()?;
        assert_eq!(a.descriptor().object_count, fixture.objects.len() as u64);
        assert_eq!(
            a.descriptor().inventory_digest,
            b.descriptor().inventory_digest
        );
        for oid in fixture.objects.keys() {
            let entry = a.find(*oid)?.ok_or("entry")?;
            assert_eq!(entry.header, first.header(*oid)?.ok_or("header")?);
            assert_eq!(
                entry.source,
                SegmentKey {
                    operation: [1; 16],
                    digest: first.descriptor().digest
                }
            );
            assert_eq!(Some(entry), b.find(*oid)?);
        }
        assert!(a.find(format.zero())?.is_none());
        assert!(
            a.find(if format == ObjectFormat::Sha1 {
                ObjectFormat::Sha256.zero()
            } else {
                ObjectFormat::Sha1.zero()
            })?
            .is_none()
        );
        let mut compact = directory(&fixture, budget.clone())?;
        compact.add_run(&a)?;
        compact.add_run(&b)?;
        let compact = compact.seal()?;
        assert_eq!(
            compact.descriptor().inventory_digest,
            a.descriptor().inventory_digest
        );
        assert_eq!(compact.entries_after(None)?, a.entries_after(None)?);
        drop((first, second, a, b, compact));
        assert_eq!(budget.used(), 0);
    }
    Ok(())
}

#[tokio::test]
async fn conflicting_body_or_graph_identity_rolls_back_a_complete_batch() -> Result {
    let fixture = fixture(ObjectFormat::Sha256, 4).await?;
    let budget = DiskBudget::new(128 << 20);
    let source = segment(&fixture, budget.clone(), [2; 16])?;
    for graph in [false, true] {
        let mut writer = directory(&fixture, budget.clone())?;
        writer.add_segment(&source)?;
        let oid = *fixture.objects.keys().next().ok_or("oid")?;
        let original = DirectoryEntry {
            location_version: 1,
            header: source.header(oid)?.ok_or("header")?,
            source: SegmentKey {
                operation: [2; 16],
                digest: source.descriptor().digest,
            },
        };
        let mut extra = original;
        extra.header.object.oid = ObjectId::Sha256([91; 32]);
        assert!(!fixture.objects.contains_key(&extra.header.object.oid));
        let mut conflict = original;
        if graph {
            conflict.header.edge_digest[0] ^= 1;
        } else {
            conflict.header.object.digest[0] ^= 1;
        }
        assert!(matches!(
            writer.put_entries(&[extra, conflict]),
            Err(MetadataError::IdentityConflict)
        ));
        let sealed = writer.seal()?;
        assert_eq!(
            sealed.descriptor().object_count,
            fixture.objects.len() as u64
        );
        assert!(sealed.find(extra.header.object.oid)?.is_none());
        assert_eq!(sealed.find(oid)?, Some(original));
    }
    drop(source);
    assert_eq!(budget.used(), 0);
    Ok(())
}

#[tokio::test]
async fn source_copy_failure_poisoning_prevents_partial_run_publication() -> Result {
    let fixture = fixture(ObjectFormat::Sha1, 4).await?;
    let budget = DiskBudget::new(128 << 20);
    let source = segment(&fixture, budget.clone(), [1; 16])?;
    let mut bad = builder(&fixture, budget.clone(), fixture.identity)?;
    let mut objects = fixture.objects.values().cloned().collect::<Vec<_>>();
    objects[0].0.digest[0] ^= 1; // Model a conflicting trusted verifier result.
    fill(&mut bad, &objects)?;
    let bad = bad.seal(&fixture.index)?;
    let mut writer = directory(&fixture, budget.clone())?;
    writer.add_segment(&source)?;
    assert!(matches!(
        writer.add_segment(&bad),
        Err(MetadataError::IdentityConflict)
    ));
    assert!(matches!(
        writer.add_segment(&source),
        Err(MetadataError::Integrity)
    ));
    assert!(matches!(writer.seal(), Err(MetadataError::Integrity)));
    drop((source, bad));
    assert_eq!(budget.used(), 0);
    Ok(())
}

#[tokio::test]
async fn large_run_pages_and_compaction_stay_within_disk_admission() -> Result {
    let fixture = fixture(ObjectFormat::Sha256, 1600).await?;
    let budget = DiskBudget::new(128 << 20);
    let source = segment(&fixture, budget.clone(), [2; 16])?;
    let mut writer = directory(&fixture, budget.clone())?;
    writer.add_segment(&source)?;
    let run = writer.seal()?;
    assert_eq!(
        budget.used(),
        source.descriptor().size + run.descriptor().size
    );
    let mut after = None;
    let mut count = 0;
    loop {
        let page = run.entries_after(after)?;
        if page.is_empty() {
            break;
        }
        assert!(page.len() <= PAGE_OBJECTS);
        for entry in &page {
            assert!(after.is_none_or(|oid| oid < entry.header.object.oid));
            after = Some(entry.header.object.oid);
        }
        count += page.len() as u64;
    }
    assert_eq!(count, run.descriptor().object_count);
    let mut compact = directory(&fixture, budget.clone())?;
    compact.add_run(&run)?;
    let compact = compact.seal()?;
    assert_eq!(
        compact.descriptor().inventory_digest,
        run.descriptor().inventory_digest
    );
    assert!(matches!(
        DirectoryBuilder::new(
            fixture.root.path(),
            DiskBudget::new(3 * metadata::growth::INITIAL_BYTES - 1),
            fixture.identity.repository,
            [4; 16],
            fixture.identity.format,
            limits()
        ),
        Err(MetadataError::Budget(_))
    ));
    drop((source, run, compact));
    assert_eq!(budget.used(), 0);
    Ok(())
}

#[tokio::test]
async fn directory_artifacts_roundtrip_and_reject_corruption_and_wrong_repository() -> Result {
    let fixture = fixture(ObjectFormat::Sha256, 4).await?;
    let budget = DiskBudget::new(128 << 20);
    let source = segment(&fixture, budget.clone(), [2; 16])?;
    let mut writer = directory(&fixture, budget.clone())?;
    writer.add_segment(&source)?;
    let run = Arc::new(writer.seal()?);
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let artifacts = ArtifactStore::new(Arc::clone(&store), fixture.identity.repository);
    let stored = Arc::clone(&run).upload(&artifacts).await?;
    let downloaded = DirectoryRun::download(
        fixture.root.path(),
        budget.clone(),
        &artifacts,
        stored,
        limits(),
    )
    .await?;
    assert_eq!(downloaded.descriptor(), run.descriptor());
    assert_eq!(downloaded.entries_after(None)?, run.entries_after(None)?);
    drop(downloaded);
    let charged = budget.used();
    let other = ArtifactStore::new(Arc::clone(&store), [9; 16]);
    assert!(matches!(
        DirectoryRun::download(
            fixture.root.path(),
            budget.clone(),
            &other,
            stored,
            limits()
        )
        .await,
        Err(MetadataError::Integrity)
    ));
    assert!(matches!(
        Arc::clone(&run).upload(&other).await,
        Err(MetadataError::Integrity)
    ));
    let mut wrong = stored;
    wrong.artifact.size += 1;
    assert!(matches!(
        DirectoryRun::download(
            fixture.root.path(),
            budget.clone(),
            &artifacts,
            wrong,
            limits()
        )
        .await,
        Err(MetadataError::Integrity)
    ));
    let path = artifacts.path(stored.run.key(), stored.artifact.digest)?;
    assert!(path.as_ref().contains("/git-catalogs/"));
    assert!(path.as_ref().contains("/directory/"));
    store
        .put(
            &canopy_object_storage::external::part(&path, 0),
            bytes::Bytes::from(vec![0; stored.artifact.size as usize]).into(),
        )
        .await?;
    assert!(
        DirectoryRun::download(
            fixture.root.path(),
            budget.clone(),
            &artifacts,
            stored,
            limits()
        )
        .await
        .is_err()
    );
    assert_eq!(budget.used(), charged);
    drop((source, run));
    assert_eq!(budget.used(), 0);
    Ok(())
}

#[tokio::test]
async fn empty_directory_and_mismatched_format_or_repository_cannot_seal() -> Result {
    let fixture = fixture(ObjectFormat::Sha1, 4).await?;
    let budget = DiskBudget::new(128 << 20);
    assert!(matches!(
        directory(&fixture, budget.clone())?.seal(),
        Err(MetadataError::Integrity)
    ));
    let source = segment(&fixture, budget.clone(), [2; 16])?;
    for (repository, format) in [
        ([9; 16], ObjectFormat::Sha1),
        (fixture.identity.repository, ObjectFormat::Sha256),
    ] {
        let mut writer = DirectoryBuilder::new(
            fixture.root.path(),
            budget.clone(),
            repository,
            [3; 16],
            format,
            limits(),
        )?;
        assert!(matches!(
            writer.add_segment(&source),
            Err(MetadataError::Integrity)
        ));
        assert!(matches!(writer.seal(), Err(MetadataError::Integrity)));
    }
    drop(source);
    assert_eq!(budget.used(), 0);
    Ok(())
}

#[tokio::test]
async fn relocation_cas_is_atomic_and_old_versions_cannot_restore_old_sources() -> Result {
    let fixture = fixture(ObjectFormat::Sha256, 4).await?;
    let budget = DiskBudget::new(128 << 20);
    let old_source = segment(&fixture, budget.clone(), [1; 16])?;
    let replacement = segment(&fixture, budget.clone(), [9; 16])?;
    let mut writer = directory(&fixture, budget.clone())?;
    writer.add_segment(&old_source)?;
    let old = writer.seal()?;
    let expected = old.entries_after(None)?;
    let mut writer = directory(&fixture, budget.clone())?;
    writer.add_run(&old)?;
    let mut stale = expected.clone();
    stale.last_mut().ok_or("entry")?.source.digest[0] ^= 1;
    assert!(matches!(
        writer.relocate(&replacement, &stale),
        Err(MetadataError::PlacementConflict)
    ));
    writer.relocate(&replacement, &expected)?;
    assert!(matches!(
        writer.relocate(&replacement, &expected),
        Err(MetadataError::PlacementConflict)
    ));
    writer.add_segment(&old_source)?;
    let updated = writer.seal()?;
    assert_eq!(
        updated.descriptor().inventory_digest,
        old.descriptor().inventory_digest
    );
    for entry in updated.entries_after(None)? {
        assert_eq!(entry.location_version, 2);
        assert_eq!(
            entry.source,
            SegmentKey {
                operation: [9; 16],
                digest: replacement.descriptor().digest
            }
        );
    }
    let mut merged = directory(&fixture, budget.clone())?;
    merged.add_run(&updated)?;
    merged.add_run(&old)?;
    let merged = merged.seal()?;
    assert_eq!(merged.entries_after(None)?, updated.entries_after(None)?);
    drop((merged, updated, old, replacement, old_source));
    assert_eq!(budget.used(), 0);
    Ok(())
}

struct Loaded(Vec<Arc<DirectoryRun>>);
impl snapshot::RunLoader for Loaded {
    async fn load(
        &self,
        stored: StoredRun,
    ) -> std::result::Result<Arc<DirectoryRun>, MetadataError> {
        self.0
            .iter()
            .find(|run| run.descriptor() == stored.run)
            .cloned()
            .ok_or(MetadataError::Integrity)
    }
}

#[tokio::test]
async fn snapshot_bounds_selection_and_roundtrips_authenticated_root_bytes() -> Result {
    use snapshot::{DirectorySnapshot, LEVEL_ZERO_ROOTS, MAX_LEVELS, MAX_SELECTED_RUNS};
    let fixture = fixture(ObjectFormat::Sha256, 4).await?;
    let budget = DiskBudget::new(128 << 20);
    let source = segment(&fixture, budget.clone(), [1; 16])?;
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let artifacts = Arc::new(ArtifactStore::new(
        Arc::clone(&store),
        fixture.identity.repository,
    ));
    let index = index::RangeIndex::new(Arc::clone(&artifacts), fixture.identity.format);
    let mut snapshot =
        DirectorySnapshot::empty(fixture.identity.repository, fixture.identity.format);
    let mut loaded = Loaded(Vec::new());
    let mut last = None;
    for n in 0..=MAX_SELECTED_RUNS {
        let mut writer = DirectoryBuilder::new(
            fixture.root.path(),
            budget.clone(),
            fixture.identity.repository,
            [n as u8 + 10; 16],
            fixture.identity.format,
            limits(),
        )?;
        writer.add_segment(&source)?;
        let run = Arc::new(writer.seal()?);
        let stored = Arc::clone(&run).upload(&artifacts).await?;
        loaded.0.push(run);
        let root = index.insert(None, [n as u8 + 70; 16], stored).await?;
        if n < LEVEL_ZERO_ROOTS {
            snapshot.append(&index, root).await?;
        } else if n < MAX_SELECTED_RUNS {
            snapshot.levels.push(Some(root));
        } else {
            assert!(matches!(
                snapshot.append(&index, root).await,
                Err(index::IndexError::Limit)
            ));
        }
        last = Some(root);
    }
    assert_eq!(snapshot.levels.len(), MAX_LEVELS);
    let oid = *fixture.objects.keys().next().ok_or("oid")?;
    assert_eq!(
        snapshot.selected_runs(&index, oid).await?.len(),
        MAX_SELECTED_RUNS
    );
    assert_eq!(
        snapshot
            .lookup(&index, &loaded, oid)
            .await?
            .ok_or("entry")?
            .header,
        source.header(oid)?.ok_or("header")?
    );
    let ids: Vec<_> = fixture.objects.keys().rev().copied().collect();
    let batch = snapshot.lookup_batch(&index, &loaded, &ids).await?;
    for (oid, actual) in ids.iter().zip(batch) {
        assert_eq!(
            actual.ok_or("batch entry")?.header,
            source.header(*oid)?.ok_or("header")?
        );
    }
    let stored = snapshot.upload(&artifacts, [90; 16]).await?;
    let restored = DirectorySnapshot::download(&artifacts, stored).await?;
    assert_eq!(restored, snapshot);
    let bytes = snapshot.encode([90; 16])?;
    let mut old_layout = bytes.clone();
    let domain = b"canopy.directory-root.v3\0";
    let at = old_layout
        .windows(domain.len())
        .position(|bytes| bytes == domain)
        .ok_or("domain")?;
    for version in *b"12" {
        old_layout[at + domain.len() - 2] = version;
        assert!(matches!(
            DirectorySnapshot::decode(&old_layout),
            Err(index::IndexError::Integrity)
        ));
    }
    for length in [0, 1, bytes.len() - 1] {
        assert!(DirectorySnapshot::decode(&bytes[..length]).is_err());
    }
    let mut trailing = bytes;
    trailing.push(0);
    assert!(DirectorySnapshot::decode(&trailing).is_err());
    let mut too_many = snapshot.clone();
    too_many.levels.push(None);
    assert!(matches!(too_many.validate(), Err(index::IndexError::Limit)));
    too_many = snapshot.clone();
    too_many.level_zero.push(last.ok_or("last")?);
    assert!(matches!(too_many.validate(), Err(index::IndexError::Limit)));
    let other = ArtifactStore::new(Arc::clone(&store), [99; 16]);
    assert!(DirectorySnapshot::download(&other, stored).await.is_err());
    let mut wrong = stored;
    wrong.format = ObjectFormat::Sha1;
    assert!(matches!(
        DirectorySnapshot::download(&artifacts, wrong).await,
        Err(index::IndexError::Integrity)
    ));
    drop((snapshot, restored, loaded, source));
    assert_eq!(budget.used(), 0);
    Ok(())
}

#[tokio::test]
async fn newer_placement_does_not_hide_conflicting_canonical_headers_in_other_levels() -> Result {
    use snapshot::DirectorySnapshot;
    let fixture = fixture(ObjectFormat::Sha1, 4).await?;
    let budget = DiskBudget::new(128 << 20);
    let original = segment(&fixture, budget.clone(), [1; 16])?;
    let replacement = segment(&fixture, budget.clone(), [9; 16])?;
    let mut writer = directory(&fixture, budget.clone())?;
    writer.add_segment(&original)?;
    let first = writer.seal()?;
    let mut writer = directory(&fixture, budget.clone())?;
    writer.add_run(&first)?;
    writer.relocate(&replacement, &first.entries_after(None)?)?;
    let newer = Arc::new(writer.seal()?);
    let mut bad = builder(&fixture, budget.clone(), fixture.identity)?;
    let mut objects = fixture.objects.values().cloned().collect::<Vec<_>>();
    objects[0].0.digest[0] ^= 1;
    fill(&mut bad, &objects)?;
    let bad = bad.seal(&fixture.index)?;
    let mut writer = directory(&fixture, budget.clone())?;
    writer.add_segment(&bad)?;
    let bad_run = Arc::new(writer.seal()?);
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let artifacts = Arc::new(ArtifactStore::new(store, fixture.identity.repository));
    let index = index::RangeIndex::new(Arc::clone(&artifacts), fixture.identity.format);
    let new_stored = Arc::clone(&newer).upload(&artifacts).await?;
    let bad_stored = Arc::clone(&bad_run).upload(&artifacts).await?;
    let mut snapshot =
        DirectorySnapshot::empty(fixture.identity.repository, fixture.identity.format);
    let new_root = index.insert(None, [72; 16], new_stored).await?;
    snapshot.append(&index, new_root).await?;
    snapshot
        .levels
        .push(Some(index.insert(None, [70; 16], bad_stored).await?));
    let loaded = Loaded(vec![newer, bad_run]);
    let oid = objects[0].0.oid;
    assert!(matches!(
        snapshot.lookup(&index, &loaded, oid).await,
        Err(index::IndexError::Metadata(MetadataError::IdentityConflict))
    ));
    assert!(matches!(
        snapshot.lookup_batch(&index, &loaded, &[oid, oid]).await,
        Err(index::IndexError::Metadata(MetadataError::IdentityConflict))
    ));
    let mut legitimate =
        DirectorySnapshot::empty(fixture.identity.repository, fixture.identity.format);
    legitimate.append(&index, new_root).await?;
    let old = Arc::new(first);
    let old_stored = Arc::clone(&old).upload(&artifacts).await?;
    legitimate
        .levels
        .push(Some(index.insert(None, [71; 16], old_stored).await?));
    let loaded = Loaded(vec![Arc::clone(&loaded.0[0]), old]);
    let chosen = legitimate
        .lookup(&index, &loaded, oid)
        .await?
        .ok_or("entry")?;
    assert_eq!(chosen.location_version, 2);
    assert_eq!(chosen.source.operation, [9; 16]);
    Ok(())
}

// Deliberately inconsistent authenticated directory fixture. Keep arbitrary
// entry insertion inside directory tests rather than exposing it to services.
pub(in crate::packs) fn inconsistent_run(
    root: &Path,
    repository: [u8; 16],
    operation: [u8; 16],
    format: ObjectFormat,
    entries: &[DirectoryEntry],
) -> std::result::Result<DirectoryRun, MetadataError> {
    let mut writer = DirectoryBuilder::new(
        root,
        DiskBudget::new(128 << 20),
        repository,
        operation,
        format,
        limits(),
    )?;
    writer.put_entries(entries)?;
    writer.seal()
}
