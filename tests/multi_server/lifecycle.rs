use super::*;
use cellule_runtime::{Control, ControlState};
use futures_core::Stream;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use std::{
    fmt,
    fs::File,
    pin::Pin,
    sync::{
        Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::{sync::Notify, time::timeout};

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;
type StoreStream<T> = Pin<Box<dyn Stream<Item = object_store::Result<T>> + Send + 'static>>;

#[derive(Debug, Default)]
struct PausedStore {
    inner: InMemory,
    phase: Mutex<Option<ControlState>>,
    entered: Notify,
    proceed: Notify,
    deny: AtomicBool,
}

impl PausedStore {
    fn arm(&self, state: ControlState) {
        *self.phase.lock().unwrap() = Some(state);
    }

    async fn wait(&self) -> Result {
        timeout(Duration::from_secs(5), self.entered.notified()).await?;
        Ok(())
    }
}

impl fmt::Display for PausedStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("paused-lifecycle-store")
    }
}

#[async_trait::async_trait]
impl ObjectStore for PausedStore {
    async fn put_opts(
        &self,
        path: &StorePath,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        let pause = {
            let mut phase = self.phase.lock().unwrap();
            let state = Control::decode(
                &payload
                    .iter()
                    .flat_map(|bytes| bytes.iter().copied())
                    .collect::<Vec<_>>(),
            )
            .ok()
            .map(|control| control.state);
            if phase.is_some() && *phase == state {
                phase.take();
                true
            } else {
                false
            }
        };
        if pause {
            self.entered.notify_one();
            self.proceed.notified().await;
            if self.deny.swap(false, Ordering::SeqCst) {
                return Err(object_store::Error::PermissionDenied {
                    path: path.to_string(),
                    source: Box::new(std::io::Error::new(
                        std::io::ErrorKind::PermissionDenied,
                        "injected lifecycle denial",
                    )),
                });
            }
        }
        self.inner.put_opts(path, payload, options).await
    }
    async fn put_multipart_opts(
        &self,
        path: &StorePath,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(path, options).await
    }
    async fn get_opts(
        &self,
        path: &StorePath,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.inner.get_opts(path, options).await
    }
    fn delete_stream(&self, paths: StoreStream<StorePath>) -> StoreStream<StorePath> {
        self.inner.delete_stream(paths)
    }
    fn list(&self, prefix: Option<&StorePath>) -> StoreStream<ObjectMeta> {
        self.inner.list(prefix)
    }
    async fn list_with_delimiter(
        &self,
        prefix: Option<&StorePath>,
    ) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(
        &self,
        from: &StorePath,
        to: &StorePath,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

fn workspace_lock(data: &Path) -> Result<File> {
    Ok(File::options()
        .read(true)
        .write(true)
        .open(data.join(".canopy-owner.lock"))?)
}

async fn wait_for_cleanup(data: &Path) -> Result {
    timeout(Duration::from_secs(10), async {
        loop {
            let file = workspace_lock(data)?;
            match file.try_lock() {
                Ok(()) => return Ok::<_, Box<dyn std::error::Error>>(()),
                Err(std::fs::TryLockError::WouldBlock) => {
                    tokio::time::sleep(Duration::from_millis(10)).await
                }
                Err(error) => return Err(error.into()),
            }
        }
    })
    .await??;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn cancelled_startup_keeps_workspace_until_publication_and_cleanup_settle() -> Result {
    let files = tempfile::TempDir::new()?;
    let data = files.path().join("node");
    let store = Arc::new(PausedStore::default());
    store.arm(ControlState::Serving);
    let address = available_address().await?;
    let start = tokio::spawn(CanopyServer::start(
        config(address, data.clone()),
        store.clone(),
    ));
    store.wait().await?;
    start.abort();
    assert!(start.await.is_err_and(|error| error.is_cancelled()));
    assert!(matches!(
        workspace_lock(&data)?.try_lock(),
        Err(std::fs::TryLockError::WouldBlock)
    ));
    store.proceed.notify_one();
    wait_for_cleanup(&data).await?;
    let server = CanopyServer::start(config(address, data), store).await?;
    create_repository(address, "after-cancellation").await?;
    server.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn dropped_handle_and_cancelled_shutdown_drain_before_releasing_workspace() -> Result {
    for cancel_shutdown in [false, true] {
        let files = tempfile::TempDir::new()?;
        let data = files.path().join("node");
        let store = Arc::new(PausedStore::default());
        let address = available_address().await?;
        let server = CanopyServer::start(config(address, data.clone()), store.clone()).await?;
        create_repository(address, "persistent").await?;
        let client = reqwest::Client::new();
        let endpoint = format!("http://{address}/api/repositories/persistent");
        let before: serde_json::Value = client
            .get(&endpoint)
            .bearer_auth("local-test-token")
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        store.arm(ControlState::Idle);
        if cancel_shutdown {
            let shutdown = tokio::spawn(server.shutdown());
            store.wait().await?;
            shutdown.abort();
            assert!(shutdown.await.is_err_and(|error| error.is_cancelled()));
        } else {
            drop(server);
            store.wait().await?;
        }
        assert!(matches!(
            workspace_lock(&data)?.try_lock(),
            Err(std::fs::TryLockError::WouldBlock)
        ));
        store.proceed.notify_one();
        wait_for_cleanup(&data).await?;
        let restored = CanopyServer::start(config(address, data), store).await?;
        let after: serde_json::Value = client
            .get(&endpoint)
            .bearer_auth("local-test-token")
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        assert_eq!(after["repository_id"], before["repository_id"]);
        restored.shutdown().await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn failed_drain_retains_workspace_exclusion_until_process_restart() -> Result {
    let files = tempfile::TempDir::new()?;
    let data = files.path().join("node");
    let store = Arc::new(PausedStore::default());
    let server = CanopyServer::start(
        config(available_address().await?, data.clone()),
        store.clone(),
    )
    .await?;
    store.arm(ControlState::Idle);
    store.deny.store(true, Ordering::SeqCst);
    let shutdown = tokio::spawn(server.shutdown());
    store.wait().await?;
    store.proceed.notify_one();
    assert!(timeout(Duration::from_secs(10), shutdown).await??.is_err());
    assert!(matches!(
        workspace_lock(&data)?.try_lock(),
        Err(std::fs::TryLockError::WouldBlock)
    ));
    Ok(())
}

#[test]
fn runtime_destruction_cannot_release_an_unconfirmed_sql_workspace() -> Result {
    for during_startup in [false, true] {
        let files = tempfile::TempDir::new()?;
        let data = files.path().join("node");
        let store = Arc::new(PausedStore::default());
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()?;
        let address = runtime.block_on(available_address())?;
        let mut server = None;
        let mut startup = None;
        if during_startup {
            store.arm(ControlState::Serving);
            startup = Some(runtime.spawn(CanopyServer::start(
                config(address, data.clone()),
                store.clone(),
            )));
            runtime.block_on(store.wait())?;
        } else {
            server = Some(runtime.block_on(CanopyServer::start(
                config(address, data.clone()),
                store.clone(),
            ))?);
        }
        runtime.shutdown_timeout(Duration::from_secs(1));
        drop((server, startup));
        assert!(
            matches!(
                workspace_lock(&data)?.try_lock(),
                Err(std::fs::TryLockError::WouldBlock)
            ),
            "workspace was released without a confirmed drain; during_startup={during_startup}"
        );
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()?;
        let restarted = runtime.block_on(CanopyServer::start(config(address, data), store));
        assert!(
            matches!(restarted, Err(canopy_server::server::ServerError::Io(error)) if error.kind() == std::io::ErrorKind::WouldBlock)
        );
        runtime.shutdown_timeout(Duration::from_secs(1));
    }
    Ok(())
}
