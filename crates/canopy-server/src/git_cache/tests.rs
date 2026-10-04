use super::*;

async fn wait_for_cleanup(budget: &DiskBudget) -> Result<(), Box<dyn std::error::Error>> {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while budget.used() != 0 {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await?;
    Ok(())
}

#[tokio::test]
async fn overlapping_indexes_are_not_unique_coverage_and_registration_is_idempotent()
-> Result<(), Box<dyn std::error::Error>> {
    for format in [crate::ObjectFormat::Sha1, crate::ObjectFormat::Sha256] {
        let root = tempfile::TempDir::new()?;
        let budget = DiskBudget::new(512 << 20);
        let mut packs = Vec::new();
        let mut verified = HashSet::new();
        for unique in [b"first".as_slice(), b"second".as_slice()] {
            let source = GitCache::create(
                root.path().into(),
                budget.clone(),
                "refs/heads/main",
                format,
                crate::native_resources::NativeResources::default()
                    .scope(crate::native_resources::NativeClass::Foreground),
            )
            .await?;
            for body in [b"overlapping object".as_slice(), unique] {
                let oid = object_id(format, ObjectKind::Blob, body);
                verified.insert(oid);
                source
                    .store_object(oid, ObjectKind::Blob, body.to_vec())
                    .await?;
            }
            packs.push(source.repacked(root.path().into(), budget.clone()).await?);
        }
        let target = GitCache::create(
            root.path().into(),
            budget.clone(),
            "refs/heads/main",
            format,
            crate::native_resources::NativeResources::default()
                .scope(crate::native_resources::NativeClass::Foreground),
        )
        .await?;
        for pack in &packs {
            assert_eq!(
                target
                    .retain_verified_packs(Arc::clone(pack), verified.clone())
                    .await?,
                2
            );
        }
        assert_eq!(verified.len(), 3);
        assert_eq!(target.indexed_entries(), 4);
        assert_eq!(
            target
                .retain_verified_packs(Arc::clone(&packs[0]), verified.clone())
                .await?,
            2
        );
        assert_eq!(target.indexed_entries(), 4);
        drop(packs);
        // Registered handles belong to copied destination indexes, not to the
        // disposable source cache whose files have now been removed.
        assert!(
            target
                .missing_objects(verified.into_iter().collect())
                .await?
                .is_empty()
        );
        drop(target);
        wait_for_cleanup(&budget).await?;
        assert_eq!(budget.used(), 0);
    }
    Ok(())
}

#[tokio::test]
async fn repacking_rotates_a_complete_cache_without_invalidating_active_readers()
-> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::TempDir::new()?;
    let budget = DiskBudget::new(128 << 20);
    let cache = GitCache::create(
        root.path().into(),
        budget.clone(),
        "refs/heads/main",
        crate::ObjectFormat::Sha1,
        crate::native_resources::NativeResources::default()
            .scope(crate::native_resources::NativeClass::Foreground),
    )
    .await?;
    let mut ids = Vec::new();
    for n in 0..64 {
        let body = format!("{n}\n{}", "shared historical contents\n".repeat(400));
        let oid = object_id(crate::ObjectFormat::Sha1, ObjectKind::Blob, body.as_bytes());
        cache
            .store_object(oid, ObjectKind::Blob, body.into_bytes())
            .await?;
        ids.push(oid);
    }
    let reader = Arc::clone(&cache);
    let packed = cache.repacked(root.path().into(), budget.clone()).await?;
    assert!(packed.missing_objects(ids.clone()).await?.is_empty());
    assert!(!packed.object_path(ids[0]).exists());
    assert_eq!(packed.indexed_entries(), 64);
    let reused = GitCache::create(
        root.path().into(),
        budget.clone(),
        "refs/heads/main",
        crate::ObjectFormat::Sha1,
        crate::native_resources::NativeResources::default()
            .scope(crate::native_resources::NativeClass::Foreground),
    )
    .await?;
    assert_eq!(
        reused
            .retain_verified_packs(Arc::clone(&packed), ids[..63].iter().copied().collect())
            .await?,
        0
    );
    assert_eq!(
        reused
            .retain_verified_packs(Arc::clone(&packed), ids.iter().copied().collect())
            .await?,
        64
    );
    assert!(reused.missing_objects(ids.clone()).await?.is_empty());
    drop(reused);
    for source in [&reader, &packed] {
        let output = crate::native_git::command(&source.git_dir())?
            .args(["cat-file", "blob", &hex::encode(ids[0])])
            .output()
            .await?;
        assert!(output.status.success());
        assert!(output.stdout.starts_with(b"0\nshared historical contents"));
    }
    drop(cache);
    drop(packed);
    assert!(reader.object_path(ids[0]).is_file());
    drop(reader);
    wait_for_cleanup(&budget).await?;
    assert_eq!(budget.used(), 0);
    Ok(())
}

#[tokio::test]
async fn concurrent_hydration_publishes_each_object_once() -> Result<(), Box<dyn std::error::Error>>
{
    let root = tempfile::TempDir::new()?;
    let budget = DiskBudget::new(1 << 20);
    let cache = GitCache::create(
        root.path().into(),
        budget.clone(),
        "refs/heads/main",
        crate::ObjectFormat::Sha1,
        crate::native_resources::NativeResources::default()
            .scope(crate::native_resources::NativeClass::Foreground),
    )
    .await?;
    assert!(cache.object_writes.get().is_none());
    let body = b"shared by concurrent fetches".to_vec();
    let oid = object_id(crate::ObjectFormat::Sha1, ObjectKind::Blob, &body);
    let mut workers = tokio::task::JoinSet::new();
    for _ in 0..32 {
        let cache = Arc::clone(&cache);
        let body = body.clone();
        workers.spawn(async move { cache.store_object(oid, ObjectKind::Blob, body).await });
    }
    while let Some(result) = workers.join_next().await {
        result??;
    }
    assert!(cache.object_writes.get().is_some());
    assert!(cache.missing_objects(vec![oid]).await?.is_empty());
    assert_eq!(budget.used(), tree_bytes(cache.root())?);
    let output = tokio::process::Command::new("git")
        .arg("--git-dir")
        .arg(cache.git_dir())
        .args(["cat-file", "blob", &hex::encode(oid)])
        .output()
        .await?;
    assert!(output.status.success());
    assert_eq!(output.stdout, body);
    Ok(())
}

#[tokio::test]
async fn hydrated_files_remain_charged_until_the_last_reader_releases_them()
-> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::TempDir::new()?;
    let budget = DiskBudget::new(1 << 20);
    let cache = GitCache::create(
        root.path().into(),
        budget.clone(),
        "refs/heads/stable/next",
        crate::ObjectFormat::Sha1,
        crate::native_resources::NativeResources::default()
            .scope(crate::native_resources::NativeClass::Foreground),
    )
    .await?;
    assert_eq!(
        fs::read(cache.git_dir().join("HEAD"))?,
        b"ref: refs/heads/stable/next\n"
    );
    let body = b"cache bytes are disposable, object identity is not\n";
    let oid = object_id(crate::ObjectFormat::Sha1, ObjectKind::Blob, body);
    cache
        .store_object(oid, ObjectKind::Blob, body.to_vec())
        .await?;
    cache
        .store_refs(&BTreeMap::from([(
            "refs/tags/blob".into(),
            RefExpectation {
                oid: Some(oid),
                version: 1,
            },
        )]))
        .await?;
    assert_eq!(budget.used(), tree_bytes(cache.root())?);
    let output = tokio::process::Command::new("git")
        .arg("--git-dir")
        .arg(cache.git_dir())
        .args(["cat-file", "blob", "refs/tags/blob"])
        .output()
        .await?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(output.stdout, body);
    let path = cache.root().to_path_buf();
    let reader = Arc::clone(&cache);
    drop(cache);
    assert_eq!(budget.used(), tree_bytes(&path)?);
    drop(reader);
    assert!(!path.exists());
    assert_eq!(budget.used(), 0);
    Ok(())
}

#[tokio::test]
async fn hydration_stops_before_writing_unadmitted_bytes() -> Result<(), Box<dyn std::error::Error>>
{
    let root = tempfile::TempDir::new()?;
    let budget = DiskBudget::new(1024);
    let cache = GitCache::create(
        root.path().into(),
        budget.clone(),
        "refs/heads/main",
        crate::ObjectFormat::Sha1,
        crate::native_resources::NativeResources::default()
            .scope(crate::native_resources::NativeClass::Foreground),
    )
    .await?;
    let initial = budget.used();
    let occupied = budget.try_reserve(budget.capacity() - initial - 1)?;
    let body = b"cannot fit in one byte".to_vec();
    let result = cache
        .store_object(
            object_id(crate::ObjectFormat::Sha1, ObjectKind::Blob, &body),
            ObjectKind::Blob,
            body.clone(),
        )
        .await;
    assert!(result.is_err_and(|error| error.is_admission()));
    let oid = object_id(crate::ObjectFormat::Sha1, ObjectKind::Blob, &body);
    assert_eq!(cache.missing_objects(vec![oid]).await?, vec![oid]);
    assert_eq!(tree_bytes(cache.root())?, initial);
    drop(cache);
    assert_eq!(budget.used(), occupied.bytes());
    drop(occupied);
    assert_eq!(budget.used(), 0);
    Ok(())
}

#[tokio::test]
async fn snapshots_share_verified_bytes_and_keep_native_writes_private()
-> Result<(), Box<dyn std::error::Error>> {
    use std::process::Stdio;
    use tokio::io::AsyncWriteExt;
    let root = tempfile::Builder::new()
        .prefix("cache path with spaces ")
        .tempdir()?;
    let budget = DiskBudget::new(1 << 20);
    let objects = GitCache::create(
        root.path().into(),
        budget.clone(),
        "refs/heads/main",
        crate::ObjectFormat::Sha1,
        crate::native_resources::NativeResources::default()
            .scope(crate::native_resources::NativeClass::Foreground),
    )
    .await?;
    let body = b"verified durable object";
    let oid = object_id(crate::ObjectFormat::Sha1, ObjectKind::Blob, body);
    objects
        .store_object(oid, ObjectKind::Blob, body.to_vec())
        .await?;
    let objects_path = objects.root().to_path_buf();
    let mut snapshots = Vec::new();
    for name in ["refs/tags/old", "refs/tags/new"] {
        let cache = GitCache::create_with_objects(
            root.path().into(),
            budget.clone(),
            "refs/heads/main",
            crate::ObjectFormat::Sha1,
            Some(Arc::clone(&objects)),
            crate::native_resources::NativeResources::default()
                .scope(crate::native_resources::NativeClass::Foreground),
        )
        .await?;
        assert!(cache.object_writes.get().is_none());
        cache
            .store_refs(&BTreeMap::from([(
                name.into(),
                RefExpectation {
                    oid: Some(oid),
                    version: 1,
                },
            )]))
            .await?;
        let output = crate::native_git::command(&cache.git_dir())?
            .args(["cat-file", "blob", name])
            .output()
            .await?;
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(output.stdout, body);
        assert!(!cache.object_path(oid).exists());
        snapshots.push(cache);
    }
    let pending = b"private native output";
    let mut child = crate::native_git::command(&snapshots[0].git_dir())?
        .args(["hash-object", "-w", "--stdin"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()?;
    child
        .stdin
        .take()
        .ok_or("missing stdin")?
        .write_all(pending)
        .await?;
    assert!(child.wait_with_output().await?.status.success());
    let pending_oid = object_id(crate::ObjectFormat::Sha1, ObjectKind::Blob, pending);
    assert_eq!(
        objects.missing_objects(vec![oid, pending_oid]).await?,
        vec![pending_oid]
    );
    assert!(snapshots[0].object_path(pending_oid).is_file());
    snapshots[0].reconcile().await?;
    drop(objects);
    assert!(objects_path.exists());
    drop(snapshots);
    assert!(!objects_path.exists());
    assert_eq!(budget.used(), 0);
    Ok(())
}

#[tokio::test]
async fn a_fenced_generation_retains_its_borrowed_objects() -> Result<(), Box<dyn std::error::Error>>
{
    let root = tempfile::TempDir::new()?;
    let budget = DiskBudget::new(1 << 20);
    let objects = GitCache::create(
        root.path().into(),
        budget.clone(),
        "refs/heads/main",
        crate::ObjectFormat::Sha1,
        crate::native_resources::NativeResources::default()
            .scope(crate::native_resources::NativeClass::Foreground),
    )
    .await?;
    let generation = GitCache::create_with_objects(
        root.path().into(),
        budget.clone(),
        "refs/heads/main",
        crate::ObjectFormat::Sha1,
        Some(Arc::clone(&objects)),
        crate::native_resources::NativeResources::default()
            .scope(crate::native_resources::NativeClass::Foreground),
    )
    .await?;
    let fence =
        crate::native_git::lock_file(&generation.git_dir().join(crate::native_git::WORKER_LOCK))?;
    fence.try_lock_shared()?;
    let object_path = objects.root().to_path_buf();
    let generation_path = generation.root().to_path_buf();
    let charged = budget.used();
    drop(objects);
    drop(generation);
    assert!(generation_path.exists());
    assert!(object_path.exists());
    assert_eq!(budget.used(), charged);
    drop(fence);
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while budget.used() != 0 {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await?;
    assert!(!generation_path.exists());
    assert!(!object_path.exists());
    Ok(())
}

#[tokio::test]
async fn native_writes_require_admission_before_reconciliation_succeeds()
-> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::TempDir::new()?;
    let budget = DiskBudget::new(1024);
    let cache = GitCache::create(
        root.path().into(),
        budget.clone(),
        "refs/heads/main",
        crate::ObjectFormat::Sha1,
        crate::native_resources::NativeResources::default()
            .scope(crate::native_resources::NativeClass::Foreground),
    )
    .await?;
    let initial = budget.used();
    let occupied = budget.try_reserve(budget.capacity() - initial)?;
    fs::write(cache.git_dir().join("native-pack"), [0; 256])?;
    assert!(
        cache
            .reconcile()
            .await
            .is_err_and(|error| error.is_admission())
    );
    drop(occupied);
    cache.reconcile().await?;
    assert_eq!(budget.used(), initial + 256);
    drop(cache);
    assert_eq!(budget.used(), 0);
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn failed_cleanup_does_not_release_disk_admission() -> Result<(), Box<dyn std::error::Error>>
{
    use std::os::unix::fs::PermissionsExt;
    let root = tempfile::TempDir::new()?;
    let budget = DiskBudget::new(1024);
    let cache = GitCache::create(
        root.path().into(),
        budget.clone(),
        "refs/heads/main",
        crate::ObjectFormat::Sha1,
        crate::native_resources::NativeResources::default()
            .scope(crate::native_resources::NativeClass::Foreground),
    )
    .await?;
    let charged = budget.used();
    let git_dir = cache.git_dir();
    fs::set_permissions(&git_dir, fs::Permissions::from_mode(0o500))?;
    drop(cache);
    let retained = budget.used();
    let remaining = git_dir.exists();
    if remaining {
        fs::set_permissions(&git_dir, fs::Permissions::from_mode(0o700))?;
    }
    assert!(remaining, "requires an unprivileged test user");
    assert_eq!(retained, charged);
    Ok(())
}

#[tokio::test]
async fn streaming_blob_hydration_preserves_bytes_and_disk_admission()
-> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::TempDir::new()?;
    let budget = DiskBudget::new(1 << 20);
    let cache = GitCache::create(
        root.path().into(),
        budget.clone(),
        "refs/heads/main",
        crate::ObjectFormat::Sha1,
        crate::native_resources::NativeResources::default()
            .scope(crate::native_resources::NativeClass::Foreground),
    )
    .await?;
    let blobs =
        crate::blob::LargeBlobStore::new(Arc::new(object_store::memory::InMemory::new()), [7; 16]);
    let body = vec![17; 9 * 1024 * 1024];
    let reference = blobs
        .put(
            object_id(crate::ObjectFormat::Sha1, ObjectKind::Blob, &body),
            body.len() as u64,
            &mut body.as_slice(),
        )
        .await?;
    cache.store_blob(blobs.read(&reference).await?).await?;
    let output = tokio::process::Command::new("git")
        .arg("--git-dir")
        .arg(cache.git_dir())
        .args(["cat-file", "blob", &hex::encode(reference.oid)])
        .output()
        .await?;
    assert!(output.status.success());
    assert_eq!(output.stdout, body);
    assert_eq!(budget.used(), tree_bytes(cache.root())?);
    drop(cache);
    assert_eq!(budget.used(), 0);

    let cache = GitCache::create(
        root.path().into(),
        budget.clone(),
        "refs/heads/main",
        crate::ObjectFormat::Sha1,
        crate::native_resources::NativeResources::default()
            .scope(crate::native_resources::NativeClass::Foreground),
    )
    .await?;
    let occupied = budget.try_reserve(budget.capacity() - budget.used())?;
    assert!(
        cache
            .store_blob(blobs.read(&reference).await?)
            .await
            .is_err_and(|error| error.is_admission())
    );
    drop(cache);
    assert_eq!(budget.used(), occupied.bytes());
    Ok(())
}

#[tokio::test]
async fn corrupt_stream_never_installs_a_reusable_object() -> Result<(), Box<dyn std::error::Error>>
{
    use object_store::ObjectStoreExt;
    let root = tempfile::TempDir::new()?;
    let budget = DiskBudget::new(1 << 20);
    let cache = GitCache::create(
        root.path().into(),
        budget.clone(),
        "refs/heads/main",
        crate::ObjectFormat::Sha1,
        crate::native_resources::NativeResources::default()
            .scope(crate::native_resources::NativeClass::Foreground),
    )
    .await?;
    let store = Arc::new(object_store::memory::InMemory::new());
    let blobs = crate::blob::LargeBlobStore::new(store.clone(), [9; 16]);
    let mut body = vec![23; 9 * 1024 * 1024];
    let reference = blobs
        .put(
            object_id(crate::ObjectFormat::Sha1, ObjectKind::Blob, &body),
            body.len() as u64,
            &mut body.as_slice(),
        )
        .await?;
    // The first range can be written before the final range detects corruption.
    // Only the test-owned blob key is changed, then restored before retry.
    let path = crate::external::part(&crate::blob::blob_path([9; 16], &reference.sha256), 1);
    let metadata = store.head(&path).await?;
    let last = body.last_mut().ok_or("empty fixture")?;
    *last ^= 1;
    store
        .put(&metadata.location, body[8 * 1024 * 1024..].to_vec().into())
        .await?;
    assert!(
        cache
            .store_blob(blobs.read(&reference).await?)
            .await
            .is_err()
    );
    assert_eq!(
        cache.missing_objects(vec![reference.oid]).await?,
        vec![reference.oid]
    );
    *body.last_mut().ok_or("empty fixture")? ^= 1;
    store
        .put(&metadata.location, body[8 * 1024 * 1024..].to_vec().into())
        .await?;
    cache.store_blob(blobs.read(&reference).await?).await?;
    assert!(cache.missing_objects(vec![reference.oid]).await?.is_empty());
    drop(cache);
    assert_eq!(budget.used(), 0);
    Ok(())
}

#[tokio::test]
async fn incomplete_pack_extracts_verified_large_blobs_without_admitting_foreign_members()
-> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::TempDir::new()?;
    let budget = DiskBudget::new(256 << 20);
    let source = GitCache::create(
        root.path().into(),
        budget.clone(),
        "refs/heads/main",
        crate::ObjectFormat::Sha1,
        crate::native_resources::NativeResources::default()
            .scope(crate::native_resources::NativeClass::Foreground),
    )
    .await?;
    let body = vec![b'x'; 2 << 20];
    let oid = object_id(crate::ObjectFormat::Sha1, ObjectKind::Blob, &body);
    let digest = *blake3::hash(&body).as_bytes();
    source
        .store_object(oid, ObjectKind::Blob, body.clone())
        .await?;
    let foreign = object_id(crate::ObjectFormat::Sha1, ObjectKind::Blob, b"foreign");
    source
        .store_object(foreign, ObjectKind::Blob, b"foreign".to_vec())
        .await?;
    let packed = source.repacked(root.path().into(), budget.clone()).await?;
    let (hash, pack, index, ids) = packed.pack_sources().await?.pop().unwrap();
    assert_eq!(
        ids.iter().copied().collect::<HashSet<_>>(),
        [oid, foreign].into_iter().collect()
    );
    let store = Arc::new(object_store::memory::InMemory::new());
    let reader = crate::pack_store::PackReader::new(
        store,
        [4; 16],
        root.path().into(),
        budget.clone(),
        crate::ObjectFormat::Sha1,
        source.native.clone(),
    );
    let record = crate::pack_store::PackRecord {
        hash,
        pack: reader.upload(pack).await?,
        index: reader.upload(index).await?,
        approved: false,
    };
    let target = GitCache::create(
        root.path().into(),
        budget.clone(),
        "refs/heads/main",
        crate::ObjectFormat::Sha1,
        crate::native_resources::NativeResources::default()
            .scope(crate::native_resources::NativeClass::Foreground),
    )
    .await?;
    target
        .store_native_blob(
            reader
                .native_reader(record.clone(), oid, body.len() as u64, digest)
                .await?,
        )
        .await?;
    assert!(target.missing_objects(vec![oid]).await?.is_empty());
    assert_eq!(target.missing_objects(vec![foreign]).await?, vec![foreign]);
    let output = crate::native_git::command(&target.git_dir())?
        .args(["cat-file", "blob", &hex::encode(oid)])
        .output()
        .await?;
    assert!(output.status.success());
    assert_eq!(output.stdout, body);
    assert!(
        reader
            .cache()
            .await?
            .missing_objects(vec![oid, foreign])
            .await?
            .len()
            == 2
    );
    let invalid = GitCache::create(
        root.path().into(),
        budget.clone(),
        "refs/heads/main",
        crate::ObjectFormat::Sha1,
        crate::native_resources::NativeResources::default()
            .scope(crate::native_resources::NativeClass::Foreground),
    )
    .await?;
    assert!(
        invalid
            .store_native_blob(reader.native_reader(record, oid, 2 << 20, [0; 32]).await?)
            .await
            .is_err()
    );
    assert_eq!(invalid.missing_objects(vec![oid]).await?, vec![oid]);
    Ok(())
}
