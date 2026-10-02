use super::*;

#[tokio::test]
async fn geometric_planner_drains_native_ingress_and_level_debt_without_changing_refs() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let fixture = Fixture::new(format).await?;
        let inventory = seed(&fixture, 4).await?;
        let original_refs = refs(&fixture.handle).await?;
        let policy = CompactionPolicy {
            base_objects: 2,
            level_ratio: 2,
            ingress_high_water: 2,
            urgent_burst: 2,
        };
        let mut planner = CompactionPlanner::new(policy)?;
        let mut expected = None;
        let mut first_catalog = None;
        let mut jobs = 0;
        let mut done = false;
        for turn in 0..50 {
            let (base, files, indexes) =
                opened(&fixture, [180 + turn; 16], Arc::clone(&inventory.store)).await?;
            let before = base.generation_fact().catalog.ok_or("catalog")?;
            let current = base.catalog_parts().0;
            let root = tempfile::TempDir::new()?;
            let budget = DiskBudget::new(128 << 20);
            let prepared = planner
                .prepare_next(root.path(), budget.clone(), base, range::job_limits())
                .await?;
            if let Some(compact) = prepared {
                let prepared = Prepared {
                    compact,
                    root,
                    budget,
                    files,
                    indexes,
                };
                let prior = range::entries(&prepared, before).await?;
                if expected.is_none() {
                    expected = Some(prior.clone());
                    first_catalog = Some(before);
                }
                assert_eq!(Some(&prior), expected.as_ref());
                let after = range::entries(&prepared, prepared.compact.catalog()).await?;
                assert_eq!(after, prior);
                let proof = prepared.compact.certificate().await?;
                let result = fixture
                    .client()
                    .command::<PublishCatalogCompaction>(&fixture.target, identity()?, proof)
                    .await?;
                assert!(matches!(result.output, CompactionReply::Published(_)));
                assert_eq!(refs(&fixture.handle).await?, original_refs);
                assert_eq!(
                    Some(range::entries(&prepared, first_catalog.ok_or("old")?).await?),
                    expected
                );
                jobs += 1;
                drop(prepared.compact);
                cleaned(prepared.root.path(), &prepared.budget).await?;
            } else {
                let pressure = policy.pressure(&current)?;
                assert_eq!(pressure.ingress_roots, 0);
                assert!(
                    pressure
                        .level_objects
                        .iter()
                        .zip(pressure.level_targets)
                        .all(|(objects, target)| *objects <= target)
                );
                assert!(current.levels.iter().skip(1).any(Option::is_some));
                cleaned(root.path(), &budget).await?;
                done = true;
                break;
            }
        }
        assert!(done, "geometric debt must drain in the bounded fixture");
        assert!(jobs > 4, "exercise both ingress and higher-level jobs");
        assert_eq!(outcomes(&fixture.handle).await?, jobs);
        fixture.runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn planner_admission_failure_retries_the_same_ingress_without_publishing() -> Result {
    let fixture = Fixture::new(ObjectFormat::Sha1).await?;
    let inventory = seed(&fixture, 2).await?;
    let (base, _, _) = opened(&fixture, [180; 16], Arc::clone(&inventory.store)).await?;
    let original = state(&fixture.handle).await?;
    let original_catalog = base.generation_fact().catalog;
    let retained = base.catalog_parts().0.level_zero[1];
    let mut planner = CompactionPlanner::new(CompactionPolicy::default())?;
    let root = tempfile::TempDir::new()?;
    let budget = DiskBudget::new(128 << 20);
    assert!(
        planner
            .prepare_next(
                root.path(),
                budget.clone(),
                Arc::clone(&base),
                CompactionLimits {
                    input_bytes: 1,
                    ..range::job_limits()
                }
            )
            .await
            .is_err()
    );
    assert_eq!(state(&fixture.handle).await?, original);
    cleaned(root.path(), &budget).await?;
    let retry = planner
        .prepare_next(root.path(), budget.clone(), base, range::job_limits())
        .await?
        .ok_or("retry")?;
    assert_eq!(retry.base().catalog, original_catalog);
    let catalog = CatalogSnapshot::download(&inventory.store, retry.catalog()).await?;
    let directory = DirectorySnapshot::download(&inventory.store, catalog.directory).await?;
    assert_eq!(directory.level_zero, vec![retained]);
    assert_eq!(outcomes(&fixture.handle).await?, 0);
    drop(retry);
    cleaned(root.path(), &budget).await?;
    fixture.runtime.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn empty_planner_rechecks_current_admin_access_before_reporting_no_work() -> Result {
    let fixture = Fixture::new(ObjectFormat::Sha1).await?;
    let inventory = seed(&fixture, 0).await?;
    let (base, files, _) = opened(&fixture, [180; 16], Arc::clone(&inventory.store)).await?;
    let mut planner = CompactionPlanner::new(CompactionPolicy::default())?;
    let root = tempfile::TempDir::new()?;
    let budget = DiskBudget::new(128 << 20);
    assert!(
        planner
            .prepare_next(
                root.path(),
                budget.clone(),
                Arc::clone(&base),
                range::job_limits()
            )
            .await?
            .is_none()
    );
    edit(&fixture, "UPDATE repository_identity SET owner='other'; INSERT INTO repository_members VALUES('owner','write');").await?;
    let original = state(&fixture.handle).await?;
    assert!(
        planner
            .prepare_next(root.path(), budget.clone(), base, range::job_limits())
            .await
            .is_err()
    );
    assert_eq!(state(&fixture.handle).await?, original);
    assert_eq!(files.stats()?.downloaded_files, 0);
    assert_eq!(outcomes(&fixture.handle).await?, 0);
    cleaned(root.path(), &budget).await?;
    fixture.runtime.shutdown().await?;
    Ok(())
}
