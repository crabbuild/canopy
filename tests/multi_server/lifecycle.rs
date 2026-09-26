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

#[tokio::test(flavor = "multi_thread")]
async fn maintenance_waits_for_confirmed_cell_release() -> Result {
    use canopy_server::{CanopyApplication, build_descriptor, deployment::Deployment};
    use cellule_app::CellApplication;
    use cellule_runtime::{ApplicationIdentity, RequestId};
    let files = tempfile::TempDir::new()?;
    let store = Arc::new(PausedStore::default());
    let address = available_address().await?;
    let settings = config(address, files.path().join("node"));
    let application = CanopyApplication::compile(build_descriptor(
        include_bytes!("../../Cargo.lock"),
        env!("CARGO_PKG_VERSION"),
    ))?;
    let deployment = Deployment::new(
        cellule_store::Store::new(store.clone()),
        settings.store_prefix.clone(),
        ApplicationIdentity::new(settings.tenant, settings.application),
        settings.fleet,
        settings.image,
        application.registry(),
    )?;
    let server = CanopyServer::start(settings, store.clone()).await?;
    create_repository(address, "held").await?;
    store.arm(ControlState::Idle);
    let operation = RequestId::from_bytes(uuid::Uuid::new_v4().into_bytes());
    deployment.begin_maintenance(operation).await?;
    store.wait().await?;
    let now = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis(),
    )?;
    let status = deployment.status(now).await?;
    assert!(!status.drained);
    assert_eq!(status.advertised_sessions, 1);
    assert!(status.unsettled_cells > 0);
    assert!(deployment.end_maintenance(operation, now).await.is_err());
    store.proceed.notify_one();
    server.shutdown().await?;
    assert!(deployment.status(now).await?.drained);
    deployment.end_maintenance(operation, now).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn cancelled_maintenance_recovery_retains_enrollment_until_cell_cleanup() -> Result {
    use canopy_server::{
        CanopyApplication, RepositoryModule, build_descriptor,
        deployment::{Deployment, RecoveryConfig},
        repository_target,
    };
    use cellule_app::CellApplication;
    use cellule_runtime::{
        ApplicationIdentity, CatalogEntry, CatalogRole, CellCatalog, CellModule, CellStorageLayout,
        RequestId,
    };
    let files = tempfile::TempDir::new()?;
    let store = Arc::new(PausedStore::default());
    let settings = config(available_address().await?, files.path().join("original"));
    let application = CanopyApplication::compile(build_descriptor(
        include_bytes!("../../Cargo.lock"),
        env!("CARGO_PKG_VERSION"),
    ))?;
    let deployment = Deployment::new(
        cellule_store::Store::new(store.clone()),
        settings.store_prefix.clone(),
        ApplicationIdentity::new(settings.tenant, settings.application),
        settings.fleet,
        settings.image,
        application.registry(),
    )?;
    let layout = CellStorageLayout::new(
        cellule_store::Store::new(store.clone()),
        settings.store_prefix.clone(),
        *settings.application.as_bytes(),
    );
    let catalog = CellCatalog::new(layout, settings.tenant);
    let target = repository_target(
        settings.tenant,
        settings.application,
        uuid::Uuid::new_v4().into_bytes(),
    )?;
    let worker_data = files.path().join("recovery");
    let recovery = RecoveryConfig {
        node: settings.node,
        signing_key: settings.signing_key.clone(),
        endpoint: settings.peer_endpoint.clone(),
        data_dir: worker_data.clone(),
        local_disk_limit_bytes: settings.local_disk_limit_bytes,
    };
    let server = CanopyServer::start(settings, store.clone()).await?;
    server.shutdown().await?;
    catalog
        .provision(CatalogEntry::new(
            &target,
            CatalogRole::Sql,
            application
                .registry()
                .module_code(RepositoryModule::NAME)
                .ok_or("missing module")?,
            1,
        )?)
        .await?;
    let operation = RequestId::from_bytes(uuid::Uuid::new_v4().into_bytes());
    deployment.begin_maintenance(operation).await?;
    store.arm(ControlState::Recovering);
    let worker = deployment.clone();
    let pending =
        tokio::spawn(async move { worker.recover_maintenance(operation, recovery).await });
    store.wait().await?;
    pending.abort();
    assert!(pending.await.is_err_and(|error| error.is_cancelled()));
    assert!(matches!(
        workspace_lock(&worker_data)?.try_lock(),
        Err(std::fs::TryLockError::WouldBlock)
    ));
    let now = || -> Result<i64> {
        Ok(i64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_millis(),
        )?)
    };
    let status = deployment.status(now()?).await?;
    assert_eq!(status.advertised_sessions, 1);
    assert!(!status.drained);
    assert!(deployment.end_maintenance(operation, now()?).await.is_err());
    store.proceed.notify_one();
    wait_for_cleanup(&worker_data).await?;
    assert!(deployment.status(now()?).await?.drained);
    deployment.end_maintenance(operation, now()?).await?;
    Ok(())
}
