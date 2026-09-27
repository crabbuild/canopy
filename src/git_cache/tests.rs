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
        .store_object(object_id(ObjectKind::Blob, &body), ObjectKind::Blob, body)
        .await;
    assert!(result.is_err_and(|error| error.is_admission()));
    assert_eq!(tree_bytes(cache.root())?, initial);
    drop(cache);
    assert_eq!(budget.used(), occupied.bytes());
    drop(occupied);
    assert_eq!(budget.used(), 0);
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
