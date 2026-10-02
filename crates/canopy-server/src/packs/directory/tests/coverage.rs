use super::*;
use crate::packs::catalog::{CatalogFileLimits, CatalogFiles};
use crate::packs::directory::{
    index::IndexError,
    snapshot::{DirectorySnapshot, RunLoader},
};

#[tokio::test]
async fn split_coverage_preserves_exact_inventory_and_rejects_forged_parent_or_parts() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let fixture = fixture(format, 40).await?;
        let budget = DiskBudget::new(128 << 20);
        let source = segment(&fixture, budget.clone(), [1; 16])?;
        let mut builder = directory(&fixture, budget.clone())?;
        builder.add_segment(&source)?;
        let run = builder.seal()?;
        let ids = fixture.objects.keys().copied().collect::<Vec<_>>();
        let (left, right, edges) = run.split_coverage(run.descriptor().coverage(), ids[15])?;
        let left = left.ok_or("left")?;
        let right = right.ok_or("right")?;
        assert_eq!(
            left.object_count + right.object_count,
            run.descriptor().object_count
        );
        assert_eq!(left.first_oid, ids[0]);
        assert_eq!(left.last_oid, ids[15]);
        assert_eq!(right.first_oid, ids[16]);
        assert_eq!(right.last_oid, *ids.last().ok_or("last")?);
        assert_eq!(edges, run.verify_coverage(left)?);
        run.verify_coverage(right)?;
        let mut merged = directory(&fixture, budget.clone())?;
        merged.add_coverage(&run, right)?;
        merged.add_coverage(&run, left)?;
        assert_eq!(
            merged.seal()?.descriptor().inventory_digest,
            run.descriptor().inventory_digest
        );
        let zero: ObjectId = vec![0; format.bytes()].try_into()?;
        assert_eq!(
            run.split_coverage(run.descriptor().coverage(), zero)?.0,
            None
        );
        assert_eq!(
            run.split_coverage(run.descriptor().coverage(), zero)?.1,
            Some(run.descriptor().coverage())
        );
        assert_eq!(run.split_coverage(left, left.last_oid)?.1, None);
        for fault in 0..3 {
            let mut bad = left;
            match fault {
                0 => bad.inventory_digest[0] ^= 1,
                1 => bad.object_count += 1,
                _ => bad.first_oid = ids[1],
            }
            assert!(run.split_coverage(bad, ids[5]).is_err());
            let mut poisoned = directory(&fixture, budget.clone())?;
            assert!(poisoned.add_coverage(&run, bad).is_err());
            assert!(poisoned.seal().is_err());
        }
        let mut fake_full = run.descriptor().coverage();
        fake_full.object_count -= 1;
        assert!(fake_full.validate(run.descriptor()).is_err());
        drop(run);
        drop(source);
        assert_eq!(budget.used(), 0);
    }
    Ok(())
}

#[tokio::test]
async fn logical_ranges_share_one_authenticated_file_and_bound_batch_lookup_and_path_updates()
-> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let fixture = fixture(format, 40).await?;
        let budget = DiskBudget::new(128 << 20);
        let source = segment(&fixture, budget.clone(), [1; 16])?;
        let mut builder = directory(&fixture, budget.clone())?;
        builder.add_segment(&source)?;
        let run = Arc::new(builder.seal()?);
        let ids = fixture.objects.keys().copied().collect::<Vec<_>>();
        let (left, right, _) = run.split_coverage(run.descriptor().coverage(), ids[15])?;
        let provider: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let store = Arc::new(ArtifactStore::new(provider, fixture.identity.repository));
        let full = run.upload(&store).await?;
        let left = StoredRun {
            coverage: left.ok_or("left")?,
            ..full
        };
        let right = StoredRun {
            coverage: right.ok_or("right")?,
            ..full
        };
        let cache_budget = DiskBudget::new(128 << 20);
        let files = CatalogFiles::new(
            fixture.root.path(),
            cache_budget.clone(),
            Arc::clone(&store),
            format,
            CatalogFileLimits::default(),
        )?;
        let original = files.load(full).await?;
        let a = files.load(left).await?;
        let b = files.load(right).await?;
        assert!(Arc::ptr_eq(&original, &a) && Arc::ptr_eq(&a, &b));
        assert_eq!(files.stats()?.downloaded_files, 1);
        let index = index::RangeIndex::new(store, format);
        let first = index.insert(None, [50; 16], left).await?;
        let both = index.insert(Some(first), [50; 16], right).await?;
        assert_eq!(both.record_count, 2);
        assert_eq!(both.object_count, full.coverage.object_count);
        assert_eq!(index.find(Some(both), ids[0]).await?, Some(left));
        assert_eq!(index.find(Some(both), ids[20]).await?, Some(right));
        assert!(matches!(
            index.remove(Some(both), [51; 16], full).await,
            Err(IndexError::Stale)
        ));
        assert!(matches!(
            index.insert(Some(both), [51; 16], full).await,
            Err(IndexError::RangeOverlap)
        ));
        let mut snapshot = DirectorySnapshot::empty(fixture.identity.repository, format);
        snapshot.append(&index, both).await?;
        let entries = snapshot.lookup_batch(&index, &files, &ids).await?;
        for (oid, entry) in ids.iter().zip(entries) {
            assert_eq!(
                entry.ok_or("entry")?.header,
                source.header(*oid)?.ok_or("header")?
            );
        }
        let mut narrow = DirectorySnapshot::empty(fixture.identity.repository, format);
        narrow.append(&index, first).await?;
        assert!(narrow.lookup(&index, &files, ids[20]).await?.is_none());
        let remaining = index
            .remove(Some(both), [51; 16], left)
            .await?
            .ok_or("remaining")?;
        assert_eq!(remaining.object_count, right.coverage.object_count);
        assert!(index.find(Some(remaining), ids[0]).await?.is_none());
        assert_eq!(index.find(Some(both), ids[0]).await?, Some(left));
        let mut forged = right;
        forged.artifact.manifest_digest[0] ^= 1;
        assert!(files.load(forged).await.is_err());
        drop(original);
        drop(a);
        drop(b);
        drop(files);
        assert_eq!(cache_budget.used(), 0);
        drop(source);
        assert_eq!(budget.used(), 0);
    }
    Ok(())
}
