use super::*;
use crate::packs::directory::StoredRun;
use std::future::Future;

fn file_limits(open_files: u32, cached_files: usize) -> CatalogFileLimits {
    CatalogFileLimits {
        open_files,
        cached_files,
        metadata: limits(),
    }
}
fn files(
    prepared: &Prepared,
    budget: DiskBudget,
    limits: CatalogFileLimits,
) -> Result<CatalogFiles> {
    Ok(CatalogFiles::new(
        prepared.fixture.root.path(),
        budget,
        Arc::clone(&prepared.store),
        prepared.stored.format,
        limits,
    )?)
}
async fn run(prepared: &Prepared) -> Result<StoredRun> {
    let snapshot =
        DirectorySnapshot::download(&prepared.store, prepared.snapshot.directory).await?;
    Ok(*snapshot.level_zero.first().ok_or("run")?)
}
async fn source(prepared: &Prepared) -> Result<SourceRecord> {
    let run = run(prepared).await?;
    let loader = Loader {
        root: prepared.fixture.root.path(),
        store: &prepared.store,
        budget: DiskBudget::new(128 << 20),
    };
    let run = RunLoader::load(&loader, run).await?;
    let entry = run.entries_after(None)?.into_iter().next().ok_or("entry")?;
    Ok(
        SourceIndex::new(Arc::clone(&prepared.store), prepared.stored.format)
            .find(prepared.snapshot.sources, entry.source)
            .await?
            .ok_or("source")?,
    )
}

#[tokio::test]
async fn worker_files_coalesce_native_lookups_and_reuse_cache_across_roots() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let prepared = prepared(format).await?;
        let budget = DiskBudget::new(128 << 20);
        let files = Arc::new(files(&prepared, budget.clone(), file_limits(4, 2))?);
        let reader =
            Arc::new(CatalogReader::open(Arc::clone(&prepared.indexes), prepared.stored).await?);
        let oid = *prepared.fixture.objects.keys().next().ok_or("oid")?;
        let mut jobs = Vec::new();
        for _ in 0..16 {
            let files = Arc::clone(&files);
            let reader = Arc::clone(&reader);
            jobs.push(tokio::spawn(async move {
                reader.lookup(oid, &*files, &*files).await
            }));
        }
        let mut resolved = Vec::new();
        for job in jobs {
            resolved.push(job.await??.ok_or("resolved")?);
        }
        for item in &resolved {
            assert_eq!(item.entry.header.object, prepared.fixture.objects[&oid].0);
            assert!(Arc::ptr_eq(
                &item.source.metadata,
                &resolved[0].source.metadata
            ));
        }
        let stats = files.stats()?;
        assert_eq!(stats.open_files, 2);
        assert_eq!(stats.cached_files, 2);
        assert_eq!(stats.downloaded_files, 2);
        assert!(stats.cache_hits >= 30);
        let next = prepared.snapshot.upload(&prepared.store, [71; 16]).await?;
        let next = CatalogReader::open(Arc::clone(&prepared.indexes), next).await?;
        let retained = next.lookup(oid, &*files, &*files).await?.ok_or("next")?;
        assert_eq!(files.stats()?.downloaded_files, 2);
        drop(resolved);
        let path = retained.source.metadata.path().to_owned();
        let parent = path.parent().ok_or("parent")?.to_owned();
        let size = retained.source.record.metadata.artifact.size;
        drop(files);
        assert_eq!(budget.used(), size);
        assert!(path.exists() && parent.exists());
        assert_eq!(
            retained.source.metadata.header(oid)?,
            Some(retained.entry.header)
        );
        drop(retained);
        assert_eq!(budget.used(), 0);
        assert!(!path.exists() && !parent.exists());
    }
    Ok(())
}

#[tokio::test]
async fn borrowed_files_hold_slots_and_eviction_compares_exact_descriptors() -> Result {
    let prepared = prepared(ObjectFormat::Sha256).await?;
    let budget = DiskBudget::new(128 << 20);
    let files = files(&prepared, budget.clone(), file_limits(1, 1))?;
    let stored_run = run(&prepared).await?;
    let stored_metadata = source(&prepared).await?.metadata;
    let borrowed = RunLoader::load(&files, stored_run).await?;
    let path = borrowed.path().to_owned();
    assert!(matches!(
        SourceLoader::load(&files, stored_metadata).await,
        Err(MetadataError::Limit)
    ));
    assert_eq!(files.stats()?.open_files, 1);
    assert_eq!(budget.used(), stored_run.artifact.size);
    drop(borrowed);
    let metadata = SourceLoader::load(&files, stored_metadata).await?;
    assert!(!path.exists());
    assert_eq!(budget.used(), stored_metadata.artifact.size);
    let mut forged = stored_metadata;
    forged.segment.inventory_digest[0] ^= 1;
    assert!(matches!(
        SourceLoader::load(&files, forged).await,
        Err(MetadataError::Integrity)
    ));
    let mut foreign = stored_run;
    foreign.run.repository[0] ^= 1;
    assert!(matches!(
        RunLoader::load(&files, foreign).await,
        Err(MetadataError::Integrity)
    ));
    assert_eq!(files.stats()?.downloaded_files, 2);
    assert!(Arc::ptr_eq(
        &metadata,
        &SourceLoader::load(&files, stored_metadata).await?
    ));
    drop(metadata);
    drop(files);
    assert_eq!(budget.used(), 0);
    Ok(())
}

#[tokio::test]
async fn failed_downloads_release_slots_and_cannot_enter_cache() -> Result {
    let prepared = prepared(ObjectFormat::Sha1).await?;
    let stored = run(&prepared).await?;
    let small = DiskBudget::new(stored.artifact.size - 1);
    let loader = files(&prepared, small.clone(), file_limits(1, 1))?;
    assert!(matches!(
        RunLoader::load(&loader, stored).await,
        Err(MetadataError::Budget(_))
    ));
    assert_eq!(small.used(), 0);
    assert_eq!(loader.stats()?.open_files, 0);
    assert_eq!(loader.stats()?.cached_files, 0);
    let path = prepared.store.path(
        ArtifactKey {
            operation: stored.run.operation,
            binding_digest: stored.run.digest,
            kind: ArtifactKind::DirectoryRun,
        },
        stored.artifact.digest,
    )?;
    prepared
        .provider
        .put(
            &canopy_object_storage::external::part(&path, 0),
            bytes::Bytes::from(vec![0; stored.artifact.size as usize]).into(),
        )
        .await?;
    let budget = DiskBudget::new(128 << 20);
    let loader = files(&prepared, budget.clone(), file_limits(1, 1))?;
    for _ in 0..2 {
        assert!(RunLoader::load(&loader, stored).await.is_err());
        assert_eq!(budget.used(), 0);
        assert_eq!(loader.stats()?.open_files, 0);
        assert_eq!(loader.stats()?.cached_files, 0);
        assert_eq!(loader.stats()?.downloaded_files, 0);
    }
    Ok(())
}

#[test]
fn canceled_queued_download_keeps_its_open_slot_and_disk_admission() -> Result {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .max_blocking_threads(1)
        .enable_all()
        .build()?;
    let prepared = runtime.block_on(prepared(ObjectFormat::Sha256))?;
    let stored = runtime.block_on(run(&prepared))?;
    let budget = DiskBudget::new(128 << 20);
    let files = files(&prepared, budget.clone(), file_limits(1, 0))?;
    let (ready_tx, ready_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let blocker = runtime.spawn_blocking(move || {
        ready_tx.send(()).unwrap();
        release_rx.recv().unwrap();
    });
    ready_rx.recv_timeout(std::time::Duration::from_secs(5))?;
    let mut future = Box::pin(RunLoader::load(&files, stored));
    let pending = {
        let _entered = runtime.enter();
        let mut context = std::task::Context::from_waker(std::task::Waker::noop());
        matches!(future.as_mut().poll(&mut context), std::task::Poll::Pending)
    };
    drop(future);
    let stats = files.stats()?;
    let charged = budget.used();
    release_tx.send(())?;
    runtime.block_on(async {
        blocker.await?;
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while files.stats().unwrap().open_files != 0 || budget.used() != 0 {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await?;
        Ok::<_, Box<dyn std::error::Error>>(())
    })?;
    assert!(pending);
    assert_eq!(stats.open_files, 1);
    assert_eq!(stats.cached_files, 0);
    assert_eq!(charged, stored.artifact.size);
    assert_eq!(files.stats()?.downloaded_files, 0);
    Ok(())
}

#[test]
fn canceled_queued_sql_lookup_retains_file_and_private_directory_until_worker_exit() -> Result {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .max_blocking_threads(1)
        .enable_all()
        .build()?;
    let prepared = runtime.block_on(prepared(ObjectFormat::Sha1))?;
    let stored = runtime.block_on(run(&prepared))?;
    let snapshot = runtime.block_on(DirectorySnapshot::download(
        &prepared.store,
        prepared.snapshot.directory,
    ))?;
    let budget = DiskBudget::new(128 << 20);
    let files = files(&prepared, budget.clone(), file_limits(1, 1))?;
    let borrowed = runtime.block_on(RunLoader::load(&files, stored))?;
    let weak = Arc::downgrade(&borrowed);
    let path = borrowed.path().to_owned();
    let parent = path.parent().ok_or("parent")?.to_owned();
    drop(borrowed);
    let ranges = RangeIndex::new(Arc::clone(&prepared.store), ObjectFormat::Sha1);
    let oid = *prepared.fixture.objects.keys().next().ok_or("oid")?;
    let (ready_tx, ready_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let blocker = runtime.spawn_blocking(move || {
        ready_tx.send(()).unwrap();
        release_rx.recv().unwrap();
    });
    ready_rx.recv_timeout(std::time::Duration::from_secs(5))?;
    let mut future = Box::pin(snapshot.lookup(&ranges, &files, oid));
    let pending = {
        let _entered = runtime.enter();
        let mut context = std::task::Context::from_waker(std::task::Waker::noop());
        matches!(future.as_mut().poll(&mut context), std::task::Poll::Pending)
    };
    drop(future);
    drop(files);
    let charged = budget.used();
    let pinned = weak.upgrade().is_some() && path.exists() && parent.exists();
    release_tx.send(())?;
    runtime.block_on(async {
        blocker.await?;
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while budget.used() != 0 || path.exists() || parent.exists() || weak.upgrade().is_some()
            {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await?;
        Ok::<_, Box<dyn std::error::Error>>(())
    })?;
    assert!(pending && pinned);
    assert_eq!(charged, stored.artifact.size);
    assert!(weak.upgrade().is_none());
    assert!(!path.exists() && !parent.exists());
    Ok(())
}

struct CountingFiles {
    files: CatalogFiles,
    runs: std::sync::atomic::AtomicUsize,
    metadata: std::sync::atomic::AtomicUsize,
}
impl RunLoader for CountingFiles {
    async fn load(
        &self,
        stored: StoredRun,
    ) -> std::result::Result<Arc<DirectoryRun>, MetadataError> {
        self.runs.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        RunLoader::load(&self.files, stored).await
    }
}
impl SourceLoader for CountingFiles {
    async fn load(
        &self,
        stored: StoredSegment,
    ) -> std::result::Result<Arc<MetadataSegment>, MetadataError> {
        self.metadata
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        SourceLoader::load(&self.files, stored).await
    }
}
#[tokio::test]
async fn canonical_header_batches_group_files_and_preserve_request_order() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let prepared = prepared(format).await?;
        let budget = DiskBudget::new(128 << 20);
        // A single slot suffices: header batches release each group before
        // opening the next file, rather than pinning a file per returned OID.
        let files = CountingFiles {
            files: files(&prepared, budget.clone(), file_limits(1, 1))?,
            runs: std::sync::atomic::AtomicUsize::new(0),
            metadata: std::sync::atomic::AtomicUsize::new(0),
        };
        let reader = CatalogReader::open(Arc::clone(&prepared.indexes), prepared.stored).await?;
        let native: Vec<_> = prepared.fixture.objects.keys().rev().copied().collect();
        let ids: Vec<_> = (0..512)
            .map(|n| {
                if n % 17 == 0 {
                    format.zero()
                } else if n % 23 == 0 {
                    if format == ObjectFormat::Sha1 {
                        ObjectFormat::Sha256.zero()
                    } else {
                        ObjectFormat::Sha1.zero()
                    }
                } else {
                    native[n % native.len()]
                }
            })
            .collect();
        let headers = reader.headers(&ids, &files, &files).await?;
        assert_eq!(headers.len(), ids.len());
        for (oid, actual) in ids.iter().zip(&headers) {
            if let Some((expected, _)) = prepared.fixture.objects.get(oid) {
                assert_eq!(actual.ok_or("header")?.object, *expected);
            } else {
                assert!(actual.is_none());
            }
        }
        assert_eq!(files.runs.load(std::sync::atomic::Ordering::Relaxed), 1);
        assert_eq!(files.metadata.load(std::sync::atomic::Ordering::Relaxed), 1);
        assert_eq!(files.files.stats()?.open_files, 1);
        assert_eq!(files.files.stats()?.downloaded_files, 2);
        assert!(reader.headers(&[], &files, &files).await?.is_empty());
        assert!(matches!(
            reader.headers(&vec![native[0]; 513], &files, &files).await,
            Err(IndexError::Limit)
        ));
        let stored = source(&prepared).await?.metadata;
        let metadata = SourceLoader::load(&files.files, stored).await?;
        assert!(matches!(
            metadata.headers(&vec![native[0]; 513]),
            Err(MetadataError::Limit)
        ));
        drop(metadata);
        let stored = run(&prepared).await?;
        let run = RunLoader::load(&files.files, stored).await?;
        assert!(matches!(
            run.find_batch(&vec![native[0]; 513]),
            Err(MetadataError::Limit)
        ));
        drop(run);
        drop(files);
        assert_eq!(budget.used(), 0);
    }
    Ok(())
}

#[tokio::test]
async fn authenticated_directory_bytes_cannot_hide_a_late_source_header_conflict() -> Result {
    let prepared = prepared(ObjectFormat::Sha256).await?;
    let loader = Loader {
        root: prepared.fixture.root.path(),
        store: &prepared.store,
        budget: DiskBudget::new(128 << 20),
    };
    let original = RunLoader::load(&loader, run(&prepared).await?).await?;
    let mut entries = original.entries_after(None)?;
    entries.last_mut().ok_or("entry")?.header.object.digest[0] ^= 1;
    let bad = Arc::new(crate::packs::directory::tests::inconsistent_run(
        prepared.fixture.root.path(),
        prepared.stored.repository,
        [75; 16],
        prepared.stored.format,
        &entries,
    )?)
    .upload(&prepared.store)
    .await?;
    let mut snapshot = DirectorySnapshot::empty(prepared.stored.repository, prepared.stored.format);
    snapshot.append(bad)?;
    let catalog = CatalogSnapshot {
        directory: snapshot.upload(&prepared.store, [76; 16]).await?,
        sources: prepared.snapshot.sources,
    }
    .upload(&prepared.store, [77; 16])
    .await?;
    let reader = CatalogReader::open(Arc::clone(&prepared.indexes), catalog).await?;
    let budget = DiskBudget::new(128 << 20);
    let files = files(&prepared, budget.clone(), file_limits(1, 1))?;
    let ids: Vec<_> = entries
        .iter()
        .map(|entry| entry.header.object.oid)
        .collect();
    assert!(matches!(
        reader.headers(&ids, &files, &files).await,
        Err(IndexError::Metadata(MetadataError::IdentityConflict))
    ));
    drop(files);
    assert_eq!(budget.used(), 0);
    Ok(())
}
