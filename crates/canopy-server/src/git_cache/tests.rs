use super::*;

#[tokio::test]
async fn streamed_native_extraction_publishes_only_a_verified_complete_object()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::{git_objects::GitObjects, packs::metadata::CanonicalObject};
    for format in [crate::ObjectFormat::Sha1, crate::ObjectFormat::Sha256] {
        let root = tempfile::TempDir::new()?;
        let budget = DiskBudget::new(32 << 20);
        let native = crate::native_resources::NativeResources::default()
            .scope(crate::native_resources::NativeClass::Foreground);
        let source = GitCache::create(
            root.path().into(),
            budget.clone(),
            "refs/heads/main",
            format,
            native.clone(),
        )
        .await?;
        let target = GitCache::create(
            root.path().into(),
            budget.clone(),
            "refs/heads/main",
            format,
            native.clone(),
        )
        .await?;
        let mut body = vec![0; 2 << 20];
        blake3::Hasher::new()
            .update(b"streamed native extraction")
            .finalize_xof()
            .fill(&mut body);
        let expected = CanonicalObject {
            oid: object_id(format, ObjectKind::Blob, &body),
            kind: ObjectKind::Blob,
            size: body.len() as u64,
            digest: *blake3::hash(&body).as_bytes(),
        };
        source
            .store_object(expected.oid, expected.kind, body.clone())
            .await?;
        let foreign_body = b"an unrelated object must remain private";
        let foreign = object_id(format, ObjectKind::Blob, foreign_body);
        source
            .store_object(foreign, ObjectKind::Blob, foreign_body.to_vec())
            .await?;
        let mut wrong = expected;
        wrong.digest[0] ^= 1;
        assert!(
            target
                .copy_native_owned(source.git_dir(), wrong, source.clone())
                .await
                .is_err()
        );
        assert!(!target.object_present(expected.oid)?);
        target
            .copy_native_owned(source.git_dir(), expected, source.clone())
            .await?;
        assert!(target.object_present(expected.oid)?);
        assert!(!target.object_present(foreign)?);
        let mut objects = GitObjects::batch_owned(&target.git_dir(), &native, target.clone())?;
        assert_eq!(objects.read_verified(expected, body.len()).await?, body);
        objects.finish().await?;
        drop(source);
        drop(target);
        wait_for_cleanup(&budget).await?;
    }
    Ok(())
}

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
