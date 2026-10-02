use super::*;
use crate::{
    ObjectId,
    packs::directory::{
        StoredRun,
        index::{IndexError, NodeRef},
        snapshot::{MAX_LEVELS, RunLoader},
    },
};

pub(super) fn job_limits() -> CompactionLimits {
    CompactionLimits {
        spool: limits(),
        output: crate::packs::metadata::MetadataLimits {
            max_file_bytes: 16 << 10,
            cache_kib: 16,
        },
        ..CompactionLimits::default()
    }
}
async fn prepare_range(
    fixture: &Fixture,
    inventory: &Inventory,
    operation: u8,
    source: CompactionSource,
    after: Option<ObjectId>,
) -> Result<Prepared> {
    prepare_range_with_limits(fixture, inventory, operation, source, after, job_limits()).await
}
async fn prepare_range_with_limits(
    fixture: &Fixture,
    inventory: &Inventory,
    operation: u8,
    source: CompactionSource,
    after: Option<ObjectId>,
    limits: CompactionLimits,
) -> Result<Prepared> {
    let (base, files, indexes) =
        opened(fixture, [operation; 16], Arc::clone(&inventory.store)).await?;
    let root = tempfile::TempDir::new()?;
    let budget = DiskBudget::new(128 << 20);
    let compact =
        PreparedCompaction::prepare_range(root.path(), budget.clone(), base, source, after, limits)
            .await?
            .ok_or("empty job")?;
    Ok(Prepared {
        compact,
        root,
        budget,
        files,
        indexes,
    })
}
async fn directory(
    inventory: &Inventory,
    catalog: crate::packs::catalog::StoredCatalog,
) -> Result<DirectorySnapshot> {
    let catalog = CatalogSnapshot::download(&inventory.store, catalog).await?;
    Ok(DirectorySnapshot::download(&inventory.store, catalog.directory).await?)
}
async fn runs(indexes: &CatalogIndexes, root: Option<NodeRef>) -> Result<Vec<StoredRun>> {
    let mut cursor = indexes.ranges().cursor(root, None)?;
    let mut result = Vec::new();
    while let Some(run) = cursor.next().await? {
        result.push(run);
    }
    Ok(result)
}
async fn publish(fixture: &Fixture, compact: &PreparedCompaction) -> Result {
    let result = fixture
        .client()
        .command::<PublishCatalogCompaction>(
            &fixture.target,
            identity()?,
            compact.certificate().await?,
        )
        .await?;
    assert!(matches!(result.output, CompactionReply::Published(_)));
    Ok(())
}
pub(super) async fn entries(
    prepared: &Prepared,
    catalog: crate::packs::catalog::StoredCatalog,
) -> Result<std::collections::BTreeMap<ObjectId, crate::packs::directory::DirectoryEntry>> {
    let reader = CatalogReader::open(Arc::clone(&prepared.indexes), catalog).await?;
    let directory = reader.directory();
    let mut ids = std::collections::BTreeSet::new();
    for root in directory
        .level_zero
        .iter()
        .copied()
        .map(Some)
        .chain(directory.levels)
    {
        for stored in runs(&prepared.indexes, root).await? {
            let run = prepared.files.load(stored).await?;
            let mut after = None;
            loop {
                let page = run.entries_after(after)?;
                if page.is_empty() {
                    break;
                }
                after = page.last().map(|entry| entry.header.object.oid);
                ids.extend(page.iter().map(|entry| entry.header.object.oid));
            }
        }
    }
    let mut result = std::collections::BTreeMap::new();
    let ids = ids.into_iter().collect::<Vec<_>>();
    for page in ids.chunks(512) {
        let entries = reader
            .directory()
            .lookup_batch(prepared.indexes.ranges(), &*prepared.files, page)
            .await?;
        let headers = reader
            .headers(page, &*prepared.files, &*prepared.files)
            .await?;
        for ((oid, entry), header) in page.iter().zip(entries).zip(headers) {
            let entry = entry.ok_or("object")?;
            assert_eq!(header, Some(entry.header));
            result.insert(*oid, entry);
        }
    }
    Ok(result)
}

#[tokio::test]
async fn native_ingress_and_adjacent_level_promotion_preserve_inventory_and_refs() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let fixture = Fixture::new(format).await?;
        let inventory = seed(&fixture, 2).await?;
        let before = refs(&fixture.handle).await?;
        let first = prepare_range(
            &fixture,
            &inventory,
            180,
            CompactionSource::Ingress(0),
            None,
        )
        .await?;
        assert_eq!(first.compact.input_count(), 1);
        let old_catalog = first.compact.base().catalog.ok_or("base")?;
        let old_directory = directory(&inventory, old_catalog).await?;
        let original = runs(&first.indexes, Some(old_directory.level_zero[0])).await?;
        let promoted = directory(&inventory, first.compact.catalog()).await?;
        assert_eq!(promoted.level_zero.len(), 1);
        assert_eq!(runs(&first.indexes, promoted.levels[0]).await?, original);
        assert_eq!(first.budget.used(), 0);
        assert_eq!(std::fs::read_dir(first.root.path())?.count(), 0);
        assert_eq!(
            entries(&first, old_catalog).await?,
            entries(&first, first.compact.catalog()).await?
        );
        publish(&fixture, &first.compact).await?;

        let second = prepare_range(
            &fixture,
            &inventory,
            181,
            CompactionSource::Ingress(0),
            None,
        )
        .await?;
        assert_eq!(second.compact.input_count(), 2);
        let original_inventory =
            entries(&second, second.compact.base().catalog.ok_or("base")?).await?;
        assert_eq!(
            entries(&second, second.compact.catalog()).await?,
            original_inventory
        );
        assert!(
            directory(&inventory, second.compact.catalog())
                .await?
                .level_zero
                .is_empty()
        );
        publish(&fixture, &second.compact).await?;
        let third =
            prepare_range(&fixture, &inventory, 182, CompactionSource::Level(0), None).await?;
        let old = directory(&inventory, third.compact.base().catalog.ok_or("base")?).await?;
        let new = directory(&inventory, third.compact.catalog()).await?;
        assert!(new.levels[0].is_none());
        assert_eq!(
            runs(&third.indexes, old.levels[0]).await?,
            runs(&third.indexes, new.levels[1]).await?
        );
        assert_eq!(
            entries(&third, third.compact.catalog()).await?,
            original_inventory
        );
        publish(&fixture, &third.compact).await?;
        assert_eq!(refs(&fixture.handle).await?, before);
        assert_eq!(outcomes(&fixture.handle).await?, 3);
        let (base, _, _) = opened(&fixture, [183; 16], Arc::clone(&inventory.store)).await?;
        assert!(
            PreparedCompaction::prepare_range(
                third.root.path(),
                third.budget.clone(),
                base,
                CompactionSource::Level(0),
                None,
                job_limits()
            )
            .await?
            .is_none()
        );
        fixture.runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn native_partial_ranges_reuse_untouched_files_and_reconcile_disjoint_level_updates() -> Result
{
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let fixture = Fixture::new(format).await?;
        let provider: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let inventory = Inventory {
            provider: Arc::clone(&provider),
            store: Arc::new(ArtifactStore::new(provider, fixture.repository)),
        };
        // Every native artifact and reader uses this exact store capability.
        let graph = super::super::reconcile::graph_with_run_limits(
            &fixture,
            Arc::clone(&inventory.provider),
            Arc::clone(&inventory.store),
            [20; 16],
            260,
            job_limits().output,
        )
        .await?;
        let proof = graph
            .prepared
            .ref_proof(
                plan(vec![update("refs/heads/native", None, Some(graph.initial))]),
                graph.root.path(),
                graph.budget.clone(),
                limits(),
            )
            .await?;
        fixture
            .client()
            .command::<PublishCatalogRefs>(&fixture.target, identity()?, proof)
            .await?;
        let before = refs(&fixture.handle).await?;
        let first = prepare_range(
            &fixture,
            &inventory,
            180,
            CompactionSource::Ingress(0),
            None,
        )
        .await?;
        let original_catalog = first.compact.base().catalog.ok_or("base")?;
        let original_directory = directory(&inventory, original_catalog).await?;
        let original = runs(&first.indexes, Some(original_directory.level_zero[0])).await?;
        assert!(original.len() > 3);
        let canonical = entries(&first, original_catalog).await?;
        let competing = prepare_range(
            &fixture,
            &inventory,
            181,
            CompactionSource::Ingress(0),
            Some(original[0].run.last_oid),
        )
        .await?;
        publish(&fixture, &first.compact).await?;
        assert!(matches!(
            competing.compact.reconcile().await,
            Err(CatalogPreparationError::Catalog(IndexError::Stale))
        ));
        let partial = directory(&inventory, first.compact.catalog()).await?;
        assert_eq!(
            runs(&first.indexes, Some(partial.level_zero[0])).await?,
            original[1..]
        );
        assert_eq!(
            runs(&first.indexes, partial.levels[0]).await?,
            original[..1]
        );
        assert_eq!(entries(&first, first.compact.catalog()).await?, canonical);
        let second = prepare_range(
            &fixture,
            &inventory,
            182,
            CompactionSource::Ingress(0),
            None,
        )
        .await?;
        publish(&fixture, &second.compact).await?;
        let left =
            prepare_range(&fixture, &inventory, 183, CompactionSource::Level(0), None).await?;
        let right = prepare_range(
            &fixture,
            &inventory,
            184,
            CompactionSource::Level(0),
            Some(original[0].run.last_oid),
        )
        .await?;
        let certificate = right.compact.certificate().await?;
        fixture
            .client()
            .command::<RegisterCatalogAttestation>(&fixture.target, identity()?, certificate)
            .await?;
        publish(&fixture, &left.compact).await?;
        let rebound = right.compact.reconcile().await?;
        assert_eq!(rebound.inventory_digest(), right.compact.inventory_digest());
        assert_eq!(rebound.token(), right.compact.token());
        publish(&fixture, &rebound).await?;
        let final_directory = directory(&inventory, rebound.catalog()).await?;
        assert!(final_directory.levels[0].is_none());
        assert_eq!(
            runs(&right.indexes, final_directory.levels[1]).await?,
            original[..2]
        );
        assert_eq!(
            runs(&right.indexes, Some(final_directory.level_zero[0])).await?,
            original[2..]
        );
        assert_eq!(entries(&right, rebound.catalog()).await?, canonical);
        assert_eq!(entries(&right, original_catalog).await?, canonical);
        assert_eq!(refs(&fixture.handle).await?, before);
        for prepared in [first, competing, second, left, right] {
            let Prepared { root, budget, .. } = prepared;
            cleaned(root.path(), &budget).await?;
        }
        fixture.runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn range_reconciliation_rejects_new_changed_or_missing_overlapping_target_inputs() -> Result {
    let fixture = Fixture::new(ObjectFormat::Sha256).await?;
    let inventory = seed(&fixture, 2).await?;
    let first = prepare_range(
        &fixture,
        &inventory,
        180,
        CompactionSource::Ingress(0),
        None,
    )
    .await?;
    let second = prepare_range(
        &fixture,
        &inventory,
        181,
        CompactionSource::Ingress(1),
        None,
    )
    .await?;
    publish(&fixture, &first.compact).await?;
    assert!(matches!(
        second.compact.reconcile().await,
        Err(CatalogPreparationError::Catalog(IndexError::Stale))
    ));
    let overlap = prepare_range(
        &fixture,
        &inventory,
        182,
        CompactionSource::Ingress(0),
        None,
    )
    .await?;
    assert_eq!(overlap.compact.input_count(), 2);
    push(&fixture, &inventory, 99, 6).await?;
    let changed = prepare_range(
        &fixture,
        &inventory,
        184,
        CompactionSource::Ingress(1),
        None,
    )
    .await?;
    publish(&fixture, &changed.compact).await?;
    assert!(matches!(
        overlap.compact.reconcile().await,
        Err(CatalogPreparationError::Catalog(IndexError::Stale))
    ));
    let overlap = prepare_range(
        &fixture,
        &inventory,
        185,
        CompactionSource::Ingress(0),
        None,
    )
    .await?;
    let promote =
        prepare_range(&fixture, &inventory, 183, CompactionSource::Level(0), None).await?;
    publish(&fixture, &promote.compact).await?;
    assert!(matches!(
        overlap.compact.reconcile().await,
        Err(CatalogPreparationError::Catalog(IndexError::Stale))
    ));
    assert_eq!(outcomes(&fixture.handle).await?, 3);
    fixture.runtime.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn range_job_rejects_limits_and_invalid_positions_without_scratch_or_outcomes() -> Result {
    let fixture = Fixture::new(ObjectFormat::Sha1).await?;
    let inventory = seed(&fixture, 2).await?;
    let first = prepare_range(
        &fixture,
        &inventory,
        180,
        CompactionSource::Ingress(0),
        None,
    )
    .await?;
    publish(&fixture, &first.compact).await?;
    let (base, _, _) = opened(&fixture, [181; 16], Arc::clone(&inventory.store)).await?;
    let root = tempfile::TempDir::new()?;
    let current = base.catalog_parts().0;
    let source = first
        .indexes
        .ranges()
        .cursor(Some(current.level_zero[0]), None)?
        .next()
        .await?
        .ok_or("source")?;
    let target = first
        .indexes
        .ranges()
        .cursor(current.levels[0], None)?
        .next()
        .await?
        .ok_or("target")?;
    for limits in [
        CompactionLimits {
            input_bytes: source.run.size + target.run.size - 1,
            ..job_limits()
        },
        CompactionLimits {
            input_runs: 1,
            ..job_limits()
        },
        CompactionLimits {
            input_bytes: 1,
            ..job_limits()
        },
    ] {
        let budget = DiskBudget::new(128 << 20);
        let result = PreparedCompaction::prepare_range(
            root.path(),
            budget.clone(),
            Arc::clone(&base),
            CompactionSource::Ingress(0),
            None,
            limits,
        )
        .await;
        if source.coverage.first_oid < target.coverage.first_oid
            && limits.input_bytes >= source.run.size
        {
            // The budget can still admit the nonoverlapping prefix before the
            // first target. Verify that this progress preserves the exact suffix.
            let prepared = result?.ok_or("prefix")?;
            assert_eq!(prepared.input_count(), 1);
            let next = directory(&inventory, prepared.catalog()).await?;
            let remainder = runs(&first.indexes, Some(next.level_zero[0])).await?;
            assert_eq!(remainder[0].run, source.run);
            assert_eq!(remainder[0].artifact, source.artifact);
            assert!(remainder[0].coverage.object_count < source.coverage.object_count);
            assert!(
                runs(&first.indexes, next.levels[0])
                    .await?
                    .contains(&target)
            );
            prepared.certificate().await?;
        } else {
            assert!(result.is_err());
        }
        cleaned(root.path(), &budget).await?;
    }
    for source in [
        CompactionSource::Ingress(usize::MAX),
        CompactionSource::Level(usize::MAX),
        CompactionSource::Level(MAX_LEVELS - 1),
    ] {
        assert!(
            PreparedCompaction::prepare_range(
                root.path(),
                DiskBudget::new(1),
                Arc::clone(&base),
                source,
                None,
                job_limits()
            )
            .await
            .is_err()
        );
    }
    let wrong: ObjectId = vec![1; 32].try_into()?;
    assert!(
        PreparedCompaction::prepare_range(
            root.path(),
            DiskBudget::new(1),
            Arc::clone(&base),
            CompactionSource::Ingress(0),
            Some(wrong),
            job_limits()
        )
        .await
        .is_err()
    );
    let after = base.catalog_parts().0.level_zero[0].last_key;
    assert!(
        PreparedCompaction::prepare_range(
            root.path(),
            DiskBudget::new(1),
            base,
            CompactionSource::Ingress(0),
            Some(after),
            job_limits()
        )
        .await?
        .is_none()
    );
    assert_eq!(outcomes(&fixture.handle).await?, 1);
    fixture.runtime.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn native_range_jobs_partition_large_inputs_and_merge_multiple_target_files() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let fixture = Fixture::new(format).await?;
        let inventory = seed(&fixture, 0).await?;
        push(&fixture, &inventory, 20, 260).await?;
        let first = prepare_range(
            &fixture,
            &inventory,
            180,
            CompactionSource::Ingress(0),
            None,
        )
        .await?;
        let original = first.compact.base().catalog.ok_or("base")?;
        let old = directory(&inventory, original).await?;
        let old_runs = runs(&first.indexes, Some(old.level_zero[0])).await?;
        assert_eq!(old_runs.len(), 1);
        assert!(old_runs[0].run.size > job_limits().output.max_file_bytes);
        let new = directory(&inventory, first.compact.catalog()).await?;
        let targets = runs(&first.indexes, new.levels[0]).await?;
        assert!(targets.len() > 3);
        assert!(
            targets
                .iter()
                .all(|run| run.run.size <= job_limits().output.max_file_bytes)
        );
        assert_eq!(
            entries(&first, original).await?,
            entries(&first, first.compact.catalog()).await?
        );
        publish(&fixture, &first.compact).await?;
        push(&fixture, &inventory, 21, 261).await?;
        let second = prepare_range(
            &fixture,
            &inventory,
            181,
            CompactionSource::Ingress(0),
            None,
        )
        .await?;
        assert!(second.compact.input_count() > 2);
        let original = second.compact.base().catalog.ok_or("base")?;
        assert_eq!(
            entries(&second, original).await?,
            entries(&second, second.compact.catalog()).await?
        );
        let before = refs(&fixture.handle).await?;
        publish(&fixture, &second.compact).await?;
        assert_eq!(refs(&fixture.handle).await?, before);
        assert_eq!(
            CatalogSnapshot::download(&inventory.store, original)
                .await?
                .sources,
            CatalogSnapshot::download(&inventory.store, second.compact.catalog())
                .await?
                .sources
        );
        assert!(
            directory(&inventory, second.compact.catalog())
                .await?
                .level_zero
                .is_empty()
        );
        assert_eq!(outcomes(&fixture.handle).await?, 2);
        for prepared in [first, second] {
            let Prepared { root, budget, .. } = prepared;
            cleaned(root.path(), &budget).await?;
        }
        fixture.runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn bounded_windows_finish_native_ingress_without_rewriting_suffix_files_or_losing_objects()
-> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let fixture = Fixture::new(format).await?;
        let inventory = seed(&fixture, 0).await?;
        push(&fixture, &inventory, 20, 260).await?;
        let first = prepare_range(
            &fixture,
            &inventory,
            180,
            CompactionSource::Ingress(0),
            None,
        )
        .await?;
        let target = directory(&inventory, first.compact.catalog()).await?;
        let targets = runs(&first.indexes, target.levels[0]).await?;
        assert!(targets.len() > 3);
        publish(&fixture, &first.compact).await?;
        push(&fixture, &inventory, 21, 261).await?;
        let limits = CompactionLimits {
            input_runs: 2,
            ..job_limits()
        };
        let competing = prepare_range_with_limits(
            &fixture,
            &inventory,
            181,
            CompactionSource::Ingress(0),
            None,
            limits,
        )
        .await?;
        let original = competing.compact.base().catalog.ok_or("base")?;
        let original_directory = directory(&inventory, original).await?;
        let source = runs(&competing.indexes, Some(original_directory.level_zero[0])).await?[0];
        let canonical = entries(&competing, original).await?;
        let before = refs(&fixture.handle).await?;
        let mut previous = source.coverage.object_count;
        let mut jobs = 0;
        loop {
            jobs += 1;
            assert!(jobs <= targets.len() + 2, "no bounded progress");
            let prepared = prepare_range_with_limits(
                &fixture,
                &inventory,
                182 + jobs as u8,
                CompactionSource::Ingress(0),
                None,
                limits,
            )
            .await?;
            assert!(prepared.compact.input_count() <= 2);
            let next = directory(&inventory, prepared.compact.catalog()).await?;
            if let Some(root) = next.level_zero.first() {
                let remainder = runs(&prepared.indexes, Some(*root)).await?;
                assert_eq!(remainder.len(), 1);
                assert_eq!(remainder[0].run, source.run);
                assert_eq!(remainder[0].artifact, source.artifact);
                assert!(remainder[0].coverage.object_count < previous);
                assert_eq!(remainder[0].coverage.last_oid, source.coverage.last_oid);
                previous = remainder[0].coverage.object_count;
            }
            assert_eq!(
                entries(&prepared, prepared.compact.catalog()).await?,
                canonical
            );
            publish(&fixture, &prepared.compact).await?;
            assert_eq!(refs(&fixture.handle).await?, before);
            if jobs == 1 {
                assert!(matches!(
                    competing.compact.reconcile().await,
                    Err(CatalogPreparationError::Catalog(IndexError::Stale))
                ));
            }
            let done = next.level_zero.is_empty();
            let Prepared { root, budget, .. } = prepared;
            cleaned(root.path(), &budget).await?;
            if done {
                break;
            }
        }
        assert!(jobs > 1);
        assert_eq!(outcomes(&fixture.handle).await?, jobs as u64 + 1);
        assert_eq!(entries(&competing, original).await?, canonical);
        fixture.runtime.shutdown().await?;
    }
    Ok(())
}
