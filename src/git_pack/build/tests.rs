use super::*;
use crate::{ObjectFormat, ObjectKind, object_id};
use cellule_ltx::DiskBudget;
use object_store::memory::InMemory;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

async fn fixture(
    format: ObjectFormat,
) -> TestResult<(tempfile::TempDir, Arc<GitCache>, PackStore, DiskBudget)> {
    let local = tempfile::TempDir::new()?;
    let budget = DiskBudget::new(8 * 1024 * 1024);
    let cache = GitCache::create(
        local.path().into(),
        budget.clone(),
        "refs/heads/main",
        format,
    )
    .await?;
    let store = PackStore::new(
        Arc::new(InMemory::new()),
        uuid::Uuid::new_v4().into_bytes(),
        local.path().into(),
        budget.clone(),
        1024 * 1024,
        1024 * 1024,
    )?;
    Ok((local, cache, store, budget))
}

#[tokio::test]
async fn builder_preserves_native_delta_compression_and_exact_objects() -> TestResult {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let (_local, cache, store, budget) = fixture(format).await?;
        let mut expected = Vec::new();
        for revision in 0..12 {
            let mut body = b"native immutable packs retain repeated content\n".repeat(2048);
            body[revision * 12] = revision as u8;
            let oid = object_id(format, ObjectKind::Blob, &body);
            cache
                .store_object(oid, ObjectKind::Blob, body.clone())
                .await?;
            expected.push((oid, body));
        }
        let source = store
            .build(
                Arc::clone(&cache),
                expected.iter().map(|(oid, _)| *oid).collect(),
                Vec::new(),
                false,
            )
            .await?;
        let native = source.data()?;
        assert_eq!(native.object_ids().count(), expected.len());
        assert!(native.identity().pack_bytes < 16 * 1024);
        let mut delta_count = 0;
        for (oid, body) in expected {
            assert!(native.contains(oid)?);
            delta_count += u64::from(native.header(oid)?.ok_or("missing header")?.delta_depth > 0);
            assert_eq!(
                native.object(oid, 1024 * 1024)?,
                Some((ObjectKind::Blob, body))
            );
        }
        assert!(delta_count > 0);
        let before = budget.used();
        let bytes = native.identity().pack_bytes + native.identity().index_bytes;
        assert!(before > bytes);
        let identity = store.publish(Arc::clone(&source)).await?;
        let read = store.load(&identity).await?;
        assert_eq!(read.data()?.identity(), &identity);
        assert_eq!(budget.used(), before + bytes);
        drop(read);
        drop(source);
        drop(store);
        drop(cache);
        assert_eq!(budget.used(), 0);
    }
    Ok(())
}

#[tokio::test]
async fn incremental_history_pack_never_needs_an_excluded_delta_base() -> TestResult {
    let (source, _, _) = crate::git_pack::tests::fixture(ObjectFormat::Sha1).await?;
    let (_local, cache, store, _budget) = fixture(ObjectFormat::Sha1).await?;
    // Install the source pack as native Git's input, not loose copies of bodies.
    let output = crate::native_git::command(&cache.git_dir())?
        .args(["-c", "protocol.file.allow=always", "--git-dir"])
        .arg(cache.git_dir())
        .args(["fetch", "--no-tags"])
        .arg(source.path())
        .arg("HEAD")
        .output()
        .await?;
    // Protocol policy is an explicit test fixture choice before the command.
    if !output.status.success() {
        return Err(String::from_utf8_lossy(&output.stderr).into_owned().into());
    }
    cache.reconcile().await?;
    let tip = crate::git_pack::tests::git(source.path(), &["rev-parse", "HEAD"]).await?;
    let old = crate::git_pack::tests::git(source.path(), &["rev-parse", "HEAD~1"]).await?;
    let tip = ObjectId::from_hex(tip.strip_suffix(b"\n").ok_or("tip newline")?)?;
    let old = ObjectId::from_hex(old.strip_suffix(b"\n").ok_or("old newline")?)?;
    let pack = store.build(cache, vec![tip], vec![old], true).await?;
    let ids: Vec<_> = pack.data()?.object_ids().collect::<Result<_, _>>()?;
    assert_eq!(ids.len(), 3);
    for oid in ids {
        let (kind, body) = pack
            .data()?
            .object(oid, 1024 * 1024)?
            .ok_or("missing object")?;
        assert_eq!(object_id(ObjectFormat::Sha1, kind, &body), oid);
    }
    Ok(())
}
