use super::*;
use async_trait::async_trait;
use futures_util::stream::BoxStream;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::Semaphore;

#[derive(Debug)]
pub(super) struct Gate {
    store: InMemory,
    pub(super) armed: AtomicBool,
    pub(super) entered: Semaphore,
    pub(super) proceed: Semaphore,
}
impl Gate {
    pub(super) fn new() -> Self {
        Self {
            store: InMemory::new(),
            armed: AtomicBool::new(false),
            entered: Semaphore::new(0),
            proceed: Semaphore::new(0),
        }
    }
}
impl std::fmt::Display for Gate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "blocked serving provider")
    }
}
#[async_trait]
impl ObjectStore for Gate {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.store.put_opts(location, payload, opts).await
    }
    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.store.put_multipart_opts(location, opts).await
    }
    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        if self.armed.swap(false, Ordering::AcqRel) {
            self.entered.add_permits(1);
            self.proceed
                .acquire()
                .await
                .expect("provider gate")
                .forget();
        }
        self.store.get_opts(location, options).await
    }
    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.store.delete_stream(locations)
    }
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.store.list(prefix)
    }
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.store.list_with_delimiter(prefix).await
    }
    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.store.copy_opts(from, to, options).await
    }
}

#[tokio::test]
async fn canceled_observer_cannot_release_pin_while_real_provider_worker_is_suspended() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let f = Fixture::new(format).await?;
        let provider = Arc::new(Gate::new());
        let store = Arc::new(ArtifactStore::new(provider.clone(), f.repository));
        initialize(&f, store.clone()).await?;
        let (lease, _, _) = acquire(&f, Some("owner"), 247, DEFAULT_LEASE_MS).await?;
        let root = tempfile::TempDir::new()?;
        let tasks = TaskTracker::new();
        let pin = ServingPin::open(
            context(&f, store.clone(), &root, tasks.clone())?,
            lease.token,
            Some("owner".into()),
        )
        .await?;
        provider.armed.store(true, Ordering::Release);
        let worker = pin.clone();
        let oid = missing(&f)?;
        let observer =
            tokio::spawn(async move { worker.headers(Some("owner".into()), &[oid]).await });
        timeout(Duration::from_secs(5), provider.entered.acquire())
            .await??
            .forget();
        observer.abort();
        assert!(observer.await.unwrap_err().is_cancelled());
        assert!(!tasks.is_empty());
        // Even a separately constructed context/budget cannot mint a second
        // drain counter while the detached provider worker owns this pin.
        assert!(matches!(
            ServingPin::open(
                context(&f, store.clone(), &root, tasks.clone())?,
                lease.token,
                Some("owner".into())
            )
            .await,
            Err(ServingReadError::AlreadyOwned)
        ));

        assert!(
            timeout(Duration::from_millis(50), pin.ready_release(identity()?))
                .await
                .is_err()
        );
        assert_eq!(pin_count(&f).await?, 1);
        assert!(matches!(
            pin.headers(Some("owner".into()), &[oid]).await,
            Err(ServingReadError::Inactive)
        ));
        provider.proceed.add_permits(1);
        tasks.close();
        timeout(Duration::from_secs(5), tasks.wait()).await?;
        assert_eq!(
            release(&f, &pin).await?.output,
            ServingReleaseReply::Released
        );
        assert_eq!(pin_count(&f).await?, 0);
        f.runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn revocation_during_provider_io_discards_result_without_abandoning_retention() -> Result {
    let f = Fixture::new(ObjectFormat::Sha256).await?;
    let provider = Arc::new(Gate::new());
    let store = Arc::new(ArtifactStore::new(provider.clone(), f.repository));
    initialize(&f, store.clone()).await?;
    edit(
        &f,
        "INSERT INTO repository_members(account,role) VALUES('viewer','read')",
    )
    .await?;
    let (lease, _, _) = acquire(&f, Some("viewer"), 248, DEFAULT_LEASE_MS).await?;
    let root = tempfile::TempDir::new()?;
    let tasks = TaskTracker::new();
    let pin = ServingPin::open(
        context(&f, store.clone(), &root, tasks.clone())?,
        lease.token,
        Some("viewer".into()),
    )
    .await?;
    provider.armed.store(true, Ordering::Release);
    let worker = pin.clone();
    let oid = missing(&f)?;
    let observer = tokio::spawn(async move { worker.headers(Some("viewer".into()), &[oid]).await });
    timeout(Duration::from_secs(5), provider.entered.acquire())
        .await??
        .forget();
    edit(&f, "DELETE FROM repository_members WHERE account='viewer'").await?;
    assert!(
        timeout(Duration::from_millis(50), pin.close_and_drain())
            .await
            .is_err()
    );
    assert_eq!(pin_count(&f).await?, 1);
    provider.proceed.add_permits(1);
    assert!(matches!(
        timeout(Duration::from_secs(5), observer).await??,
        Err(ServingReadError::Inactive)
    ));
    assert_eq!(
        release(&f, &pin).await?.output,
        ServingReleaseReply::Released
    );
    tasks.close();
    tasks.wait().await;
    f.runtime.shutdown().await?;
    Ok(())
}
