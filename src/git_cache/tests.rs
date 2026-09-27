use super::*;

#[tokio::test]
async fn hydrated_files_remain_charged_until_the_last_reader_releases_them()
-> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::TempDir::new()?;
    let budget = DiskBudget::new(1 << 20);
    let cache =
        GitCache::create(root.path().into(), budget.clone(), "refs/heads/stable/next").await?;
    assert_eq!(
        fs::read(cache.git_dir().join("HEAD"))?,
        b"ref: refs/heads/stable/next\n"
    );
    let body = b"cache bytes are disposable, object identity is not\n";
    let oid = object_id(ObjectKind::Blob, body);
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
    let cache = GitCache::create(root.path().into(), budget.clone(), "refs/heads/main").await?;
    let initial = budget.used();
    let occupied = budget.try_reserve(budget.capacity() - initial - 1)?;
    let body = b"cannot fit in one byte".to_vec();
    let result = cache
        .store_object(
            object_id(ObjectKind::Blob, &body),
            ObjectKind::Blob,
            body.clone(),
        )
        .await;
    assert!(result.is_err_and(|error| error.is_admission()));
    let oid = object_id(ObjectKind::Blob, &body);
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
    let objects = GitCache::create(root.path().into(), budget.clone(), "refs/heads/main").await?;
    let body = b"verified durable object";
    let oid = object_id(ObjectKind::Blob, body);
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
            Some(Arc::clone(&objects)),
        )
        .await?;
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
    let pending_oid = object_id(ObjectKind::Blob, pending);
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
    let objects = GitCache::create(root.path().into(), budget.clone(), "refs/heads/main").await?;
    let generation = GitCache::create_with_objects(
        root.path().into(),
        budget.clone(),
        "refs/heads/main",
        Some(Arc::clone(&objects)),
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
    Ok(())
}

#[tokio::test]
async fn native_writes_require_admission_before_reconciliation_succeeds()
-> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::TempDir::new()?;
    let budget = DiskBudget::new(1024);
    let cache = GitCache::create(root.path().into(), budget.clone(), "refs/heads/main").await?;
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
    let cache = GitCache::create(root.path().into(), budget.clone(), "refs/heads/main").await?;
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
    let cache = GitCache::create(root.path().into(), budget.clone(), "refs/heads/main").await?;
    let blobs = crate::large_blob::LargeBlobStore::new(
        Arc::new(object_store::memory::InMemory::new()),
        [7; 16],
    );
    let body = vec![17; 9 * 1024 * 1024];
    let reference = blobs
        .put(
            object_id(ObjectKind::Blob, &body),
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

    let cache = GitCache::create(root.path().into(), budget.clone(), "refs/heads/main").await?;
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
    let cache = GitCache::create(root.path().into(), budget.clone(), "refs/heads/main").await?;
    let store = Arc::new(object_store::memory::InMemory::new());
    let blobs = crate::large_blob::LargeBlobStore::new(store.clone(), [9; 16]);
    let mut body = vec![23; 9 * 1024 * 1024];
    let reference = blobs
        .put(
            object_id(ObjectKind::Blob, &body),
            body.len() as u64,
            &mut body.as_slice(),
        )
        .await?;
    // The first range can be written before the final range detects corruption.
    // Only the test-owned blob key is changed, then restored before retry.
    let path = crate::large_blob::blob_path([9; 16], &reference.sha256);
    let metadata = store.head(&path).await?;
    let last = body.last_mut().ok_or("empty fixture")?;
    *last ^= 1;
    store.put(&metadata.location, body.clone().into()).await?;
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
    store.put(&metadata.location, body.into()).await?;
    cache.store_blob(blobs.read(&reference).await?).await?;
    assert!(cache.missing_objects(vec![reference.oid]).await?.is_empty());
    drop(cache);
    assert_eq!(budget.used(), 0);
    Ok(())
}
