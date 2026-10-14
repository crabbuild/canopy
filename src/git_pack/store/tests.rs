use super::*;
use object_store::memory::InMemory;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

async fn prepared(format: ObjectFormat, budget: &DiskBudget) -> TestResult<Arc<OwnedPack>> {
    let (_source, path, identity) = crate::git_pack::tests::fixture(format).await?;
    let directory = tempfile::TempDir::new()?;
    let reservation = budget.try_reserve(identity.pack_bytes + identity.index_bytes)?;
    std::fs::copy(&path, directory.path().join("objects.pack"))?;
    std::fs::copy(
        path.with_extension("idx"),
        directory.path().join("objects.idx"),
    )?;
    Ok(Arc::new(OwnedPack::open(
        directory,
        identity,
        reservation,
        1024 * 1024,
        1024 * 1024,
    )?))
}

#[tokio::test]
async fn immutable_pairs_round_trip_once_and_share_disk_admission() -> TestResult {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let provider = Arc::new(InMemory::new());
        let budget = DiskBudget::new(1024 * 1024);
        let source = prepared(format, &budget).await?;
        let local = tempfile::TempDir::new()?;
        let store = PackStore::new(
            provider.clone(),
            uuid::Uuid::new_v4().into_bytes(),
            local.path().into(),
            budget.clone(),
            1024 * 1024,
            1024 * 1024,
        )?;
        let identity = store.publish(Arc::clone(&source)).await?;
        assert_eq!(&identity, source.data()?.identity());
        assert_eq!(store.publish(Arc::clone(&source)).await?, identity);
        let (first, second) = tokio::try_join!(store.load(&identity), store.load(&identity))?;
        assert!(Arc::ptr_eq(&first, &second));
        assert_eq!(
            budget.used(),
            2 * (identity.pack_bytes + identity.index_bytes)
        );
        for oid in source.data()?.object_ids() {
            let oid = oid?;
            assert_eq!(
                first.data()?.object(oid, 1024 * 1024)?,
                source.data()?.object(oid, 1024 * 1024)?
            );
        }
        drop(first);
        drop(second);
        drop(store);
        drop(source);
        assert_eq!(budget.used(), 0);
        assert_eq!(std::fs::read_dir(local.path())?.count(), 0);
        let mut remote = provider.list(None);
        let mut count = 0;
        while let Some(object) = poll_fn(|cx| remote.as_mut().poll_next(cx)).await {
            let object = object?;
            assert!(!object.location.as_ref().contains("staging"));
            count += 1;
        }
        assert_eq!(count, 2);
    }
    Ok(())
}

#[tokio::test]
async fn corruption_cannot_be_overwritten_or_installed_in_a_fresh_cache() -> TestResult {
    let provider = Arc::new(InMemory::new());
    let budget = DiskBudget::new(1024 * 1024);
    let source = prepared(ObjectFormat::Sha1, &budget).await?;
    let local = tempfile::TempDir::new()?;
    let store = PackStore::new(
        provider.clone(),
        uuid::Uuid::new_v4().into_bytes(),
        local.path().into(),
        budget.clone(),
        1024 * 1024,
        1024 * 1024,
    )?;
    let identity = store.publish(Arc::clone(&source)).await?;
    let path = store.pack_path(&identity);
    let mut bytes = std::fs::read(source.directory()?.join("objects.pack"))?;
    bytes[12] ^= 1;
    provider
        .put(&path, Bytes::from(bytes.clone()).into())
        .await?;
    assert!(matches!(
        store.publish(Arc::clone(&source)).await,
        Err(PackError::Integrity)
    ));
    assert!(matches!(
        store.load(&identity).await,
        Err(PackError::Integrity)
    ));
    assert_eq!(
        provider.get(&path).await?.bytes().await?.as_ref(),
        bytes.as_slice()
    );
    assert_eq!(budget.used(), identity.pack_bytes + identity.index_bytes);
    assert_eq!(std::fs::read_dir(local.path())?.count(), 0);
    Ok(())
}

#[tokio::test]
async fn a_cancelled_publisher_retains_source_files_until_remote_work_settles() -> TestResult {
    use object_store::throttle::{ThrottleConfig, ThrottledStore};
    let provider = Arc::new(ThrottledStore::new(
        InMemory::new(),
        ThrottleConfig {
            wait_get_per_call: std::time::Duration::from_millis(150),
            ..ThrottleConfig::default()
        },
    ));
    let budget = DiskBudget::new(1024 * 1024);
    let source = prepared(ObjectFormat::Sha1, &budget).await?;
    let local = tempfile::TempDir::new()?;
    let store = PackStore::new(
        provider.clone(),
        uuid::Uuid::new_v4().into_bytes(),
        local.path().into(),
        budget.clone(),
        1024 * 1024,
        1024 * 1024,
    )?;
    let identity = source.data()?.identity().clone();
    let work = store.clone();
    let owned = Arc::clone(&source);
    let caller = tokio::spawn(async move { work.publish(owned).await });
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        loop {
            let prefix =
                ObjectPath::from(format!("repos/{}/git-packs", hex::encode(store.repository)));
            let mut objects = provider.list(Some(&prefix));
            if let Some(object) = poll_fn(|cx| objects.as_mut().poll_next(cx)).await {
                object?;
                break Ok::<_, object_store::Error>(());
            }
            tokio::task::yield_now().await;
        }
    })
    .await??;
    caller.abort();
    let _ = caller.await;
    drop(source);
    assert!(
        budget.used() > 0,
        "the detached publisher still owns its source files"
    );
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        while budget.used() != 0 {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await?;
    assert!(store.load(&identity).await.is_ok());
    Ok(())
}

#[tokio::test]
#[ignore = "requires an isolated S3-compatible provider namespace and explicit credentials"]
async fn native_pair_round_trips_through_rustfs() -> TestResult {
    use object_store::prefix::PrefixStore;
    let bucket = std::env::var("CANOPY_TEST_S3_BUCKET")?;
    let prefix = std::env::var("CANOPY_TEST_S3_PREFIX")?;
    let url = url::Url::parse(&format!("s3://{bucket}/{prefix}"))?;
    let options = std::env::vars()
        .flat_map(|(key, value)| {
            [
                (key.clone(), value.clone()),
                (key.to_ascii_lowercase(), value),
            ]
        })
        .chain(std::iter::once((
            "aws_copy_if_not_exists".into(),
            object_store::aws::S3CopyIfNotExists::Multipart.to_string(),
        )));
    let (provider, root) = object_store::parse_url_opts(&url, options)?;
    let provider: Arc<dyn ObjectStore> = Arc::from(provider);
    let provider: Arc<dyn ObjectStore> = Arc::new(PrefixStore::new(provider, root));
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let budget = DiskBudget::new(1024 * 1024);
        let source = prepared(format, &budget).await?;
        let local = tempfile::TempDir::new()?;
        let store = PackStore::new(
            provider.clone(),
            uuid::Uuid::new_v4().into_bytes(),
            local.path().into(),
            budget.clone(),
            1024 * 1024,
            1024 * 1024,
        )?;
        let identity = store.publish(Arc::clone(&source)).await?;
        assert_eq!(store.publish(Arc::clone(&source)).await?, identity);
        let restored = store.load(&identity).await?;
        for oid in source.data()?.object_ids() {
            let oid = oid?;
            assert_eq!(
                restored.data()?.object(oid, 1024 * 1024)?,
                source.data()?.object(oid, 1024 * 1024)?
            );
        }
    }
    Ok(())
}
