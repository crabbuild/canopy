use super::*;

fn run_limits() -> MetadataLimits {
    MetadataLimits {
        max_file_bytes: 16 << 10,
        cache_kib: 16,
    }
}

#[tokio::test]
async fn streaming_partition_preserves_sources_versions_and_one_candidate_per_root() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let fixture = fixture(format, 1600).await?;
        let budget = DiskBudget::new(128 << 20);
        let source = segment(&fixture, budget.clone(), [1; 16])?;
        let mut writer = directory(&fixture, budget.clone())?;
        writer.add_segment(&source)?;
        let original = writer.seal()?;
        let replacement = segment(&fixture, budget.clone(), [9; 16])?;
        let mut writer = directory(&fixture, budget.clone())?;
        writer.add_run(&original)?;
        writer.relocate(&replacement, &original.entries_after(None)?)?;
        let input = Arc::new(writer.seal()?);
        drop((original, replacement));
        assert_eq!(input.entries_after(None)?[0].location_version, 2);
        let input_size = input.descriptor().size;
        let mut stream =
            DirectoryPartitioner::new(Arc::clone(&input), budget.clone(), run_limits())?;
        let artifacts = Arc::new(ArtifactStore::new(
            Arc::new(InMemory::new()),
            fixture.identity.repository,
        ));
        let index = index::RangeIndex::new(Arc::clone(&artifacts), format);
        let mut root = None;
        let mut loaded = Loaded(Vec::new());
        let mut count = 0_u64;
        let mut previous = None;
        let mut bytes = 0;
        while let Some(run) = stream.next_run()? {
            let descriptor = run.descriptor();
            assert!(descriptor.size <= run_limits().max_file_bytes);
            assert!(previous.is_none_or(|last| last < descriptor.first_oid));
            previous = Some(descriptor.last_oid);
            let mut after = None;
            loop {
                let entries = run.entries_after(after)?;
                if entries.is_empty() {
                    break;
                }
                for entry in &entries {
                    assert_eq!(Some(*entry), input.find(entry.header.object.oid)?);
                    count += 1;
                }
                after = entries.last().map(|entry| entry.header.object.oid);
            }
            let stored = Arc::clone(&run).upload(&artifacts).await?;
            root = Some(index.insert(root, [80; 16], stored).await?);
            bytes += descriptor.size;
            loaded.0.push(run);
            // Completed files shrink to their exact charge; no abandoned output
            // reservation or rollback journal accumulates between calls.
            assert_eq!(budget.used(), source.descriptor().size + input_size + bytes);
        }
        assert!(stream.next_run()?.is_none());
        assert_eq!(count, input.descriptor().object_count);
        let root = root.ok_or("root")?;
        assert!(root.record_count > snapshot::LEVEL_ZERO_ROOTS as u64);
        assert_eq!(root.object_count, count);
        let mut snapshot = snapshot::DirectorySnapshot::empty(fixture.identity.repository, format);
        snapshot.append(&index, root).await?;
        snapshot.append(&index, root).await?;
        assert_eq!(snapshot.level_zero.len(), 1);
        for ids in fixture
            .objects
            .keys()
            .copied()
            .collect::<Vec<_>>()
            .chunks(PAGE_OBJECTS)
        {
            for oid in ids {
                assert_eq!(snapshot.selected_runs(&index, *oid).await?.len(), 1);
            }
            let results = snapshot.lookup_batch(&index, &loaded, ids).await?;
            for (oid, result) in ids.iter().zip(results) {
                assert_eq!(result, input.find(*oid)?);
            }
        }
        drop((stream, input, source, loaded));
        assert_eq!(budget.used(), 0);
    }
    Ok(())
}

#[tokio::test]
async fn partition_failure_poisons_stream_and_small_runs_reuse_the_original_file() -> Result {
    let fixture = fixture(ObjectFormat::Sha256, 80).await?;
    let budget = DiskBudget::new(128 << 20);
    let source = segment(&fixture, budget.clone(), [1; 16])?;
    let mut writer = directory(&fixture, budget.clone())?;
    writer.add_segment(&source)?;
    let input = Arc::new(writer.seal()?);
    assert!(input.descriptor().size > run_limits().max_file_bytes);
    let output_budget = DiskBudget::new(1);
    let mut stream =
        DirectoryPartitioner::new(Arc::clone(&input), output_budget.clone(), run_limits())?;
    assert!(matches!(stream.next_run(), Err(MetadataError::Budget(_))));
    assert!(matches!(stream.next_run(), Err(MetadataError::Integrity)));
    assert_eq!(output_budget.used(), 0);
    let mut small = DirectoryPartitioner::new(Arc::clone(&input), output_budget, limits())?;
    assert!(Arc::ptr_eq(&input, &small.next_run()?.ok_or("small run")?));
    assert!(small.next_run()?.is_none());
    drop((stream, small, input, source));
    assert_eq!(budget.used(), 0);
    Ok(())
}

#[tokio::test]
async fn partition_exhaustion_rejects_an_incomplete_input_inventory() -> Result {
    let fixture = fixture(ObjectFormat::Sha1, 80).await?;
    let budget = DiskBudget::new(128 << 20);
    let source = segment(&fixture, budget.clone(), [1; 16])?;
    for mismatch in 0..3 {
        let mut writer = directory(&fixture, budget.clone())?;
        writer.add_segment(&source)?;
        let mut input = writer.seal()?;
        // Trusted memory fault injection: authenticated physical descriptors do
        // not by themselves prove complete canonical output coverage.
        match mismatch {
            0 => input.descriptor.object_count += 1,
            1 => input.descriptor.inventory_digest[0] ^= 1,
            _ => input.descriptor.last_oid = ObjectId::Sha1([255; 20]),
        }
        let mut stream = DirectoryPartitioner::new(Arc::new(input), budget.clone(), run_limits())?;
        loop {
            match stream.next_run() {
                Ok(Some(run)) => drop(run),
                Ok(None) => return Err("incomplete inventory accepted".into()),
                Err(MetadataError::Integrity) => break,
                Err(error) => return Err(error.into()),
            }
        }
        assert!(matches!(stream.next_run(), Err(MetadataError::Integrity)));
    }
    drop(source);
    assert_eq!(budget.used(), 0);
    Ok(())
}

#[test]
fn canceled_partition_observer_keeps_workspace_and_disk_charge_until_worker_drains() -> Result {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()?;
    runtime.block_on(async {
        let fixture = fixture(ObjectFormat::Sha256, 80).await?;
        let budget = DiskBudget::new(128 << 20);
        let source = segment(&fixture, budget.clone(), [1; 16])?;
        let root = tempfile::TempDir::new()?;
        let workspace = Arc::new(tempfile::TempDir::new_in(root.path())?);
        let path = workspace.path().to_owned();
        let mut writer = DirectoryBuilder::new(
            workspace.path(),
            budget.clone(),
            fixture.identity.repository,
            [80; 16],
            fixture.identity.format,
            limits(),
        )?;
        writer.retain_workspace(workspace);
        writer.add_segment(&source)?;
        let input = Arc::new(writer.seal()?);
        let input_size = input.descriptor().size;
        drop(source);
        let mut stream = DirectoryPartitioner::new(input, budget.clone(), run_limits())?;
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let started = Arc::new(tokio::sync::Notify::new());
        let notify = Arc::clone(&started);
        let blocked = tokio::task::spawn_blocking(move || {
            notify.notify_one();
            release_rx.recv().unwrap();
        });
        started.notified().await;
        let queued = Arc::new(tokio::sync::Notify::new());
        let notify = Arc::clone(&queued);
        let observer = tokio::spawn(async move {
            let task = tokio::task::spawn_blocking(move || stream.next_run());
            notify.notify_one();
            task.await
        });
        queued.notified().await;
        observer.abort();
        assert!(matches!(observer.await, Err(error) if error.is_cancelled()));
        assert!(path.exists());
        assert_eq!(budget.used(), input_size);
        release_tx.send(())?;
        blocked.await?;
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while budget.used() != 0 {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await?;
        assert!(!path.exists());
        assert_eq!(std::fs::read_dir(root.path())?.count(), 0);
        Ok::<_, Box<dyn std::error::Error>>(())
    })
}
