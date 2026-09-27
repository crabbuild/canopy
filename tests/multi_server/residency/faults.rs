use std::{fmt, pin::Pin, sync::Mutex};

use crab_cell_runtime::control::Control;
use futures_core::Stream;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use tokio::sync::Notify;

use super::*;

mod admission;
mod git_discovery;

#[derive(Clone, Copy, Debug)]
enum ReleaseFault {
    Pause,
    LostReply,
    Denied,
}

#[derive(Debug, Default)]
struct ReleaseStore {
    inner: InMemory,
    fault: Mutex<Option<(StorePath, ReleaseFault)>>,
    paused_read: Mutex<Option<StorePath>>,
    entered: Notify,
    proceed: Notify,
}

impl ReleaseStore {
    fn arm(&self, path: StorePath, fault: ReleaseFault) {
        *self.fault.lock().unwrap() = Some((path, fault));
    }

    async fn wait(&self) -> Result {
        timeout(Duration::from_secs(5), self.entered.notified()).await?;
        Ok(())
    }
}

impl fmt::Display for ReleaseStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("release-fault-store")
    }
}

type StoreStream<T> = Pin<Box<dyn Stream<Item = object_store::Result<T>> + Send + 'static>>;

#[async_trait::async_trait]
impl ObjectStore for ReleaseStore {
    async fn put_opts(
        &self,
        path: &StorePath,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        let fault = {
            let mut armed = self.fault.lock().unwrap();
            if armed.as_ref().is_some_and(|(target, _)| target == path)
                && Control::decode(
                    &payload
                        .iter()
                        .flat_map(|bytes| bytes.iter().copied())
                        .collect::<Vec<_>>(),
                )
                .is_ok_and(|control| control.state == ControlState::Idle)
            {
                armed.take().map(|(_, fault)| fault)
            } else {
                None
            }
        };
        let Some(fault) = fault else {
            return self.inner.put_opts(path, payload, options).await;
        };
        self.entered.notify_one();
        self.proceed.notified().await;
        if matches!(fault, ReleaseFault::Denied) {
            return Err(object_store::Error::PermissionDenied {
                path: path.to_string(),
                source: Box::new(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    "injected release denial",
                )),
            });
        }
        let result = self.inner.put_opts(path, payload, options).await?;
        if matches!(fault, ReleaseFault::LostReply) {
            return Err(object_store::Error::Generic {
                store: "release-fault-store",
                source: Box::new(std::io::Error::new(
                    std::io::ErrorKind::ConnectionReset,
                    "injected lost release reply",
                )),
            });
        }
        Ok(result)
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
        let pause = {
            let mut armed = self.paused_read.lock().unwrap();
            if armed.as_ref() == Some(path) {
                armed.take();
                true
            } else {
                false
            }
        };
        if pause {
            self.entered.notify_one();
            self.proceed.notified().await;
        }
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

struct Fixture {
    workspace: tempfile::TempDir,
    store: Arc<ReleaseStore>,
    server: CanopyServer,
    client: reqwest::Client,
    address: std::net::SocketAddr,
    layout: CellStorageLayout,
    target: crab_cell_runtime::CellTarget,
    repository_dir: std::path::PathBuf,
    oid: Vec<u8>,
}

impl Fixture {
    async fn new() -> Result<Self> {
        let workspace = tempfile::TempDir::new()?;
        let store = Arc::new(ReleaseStore::default());
        let address = available_address().await?;
        let settings = config(address, workspace.path().join("server"));
        let tenant = settings.tenant;
        let application = settings.application;
        let layout = CellStorageLayout::new(
            Store::new(store.clone()),
            settings.store_prefix.clone(),
            *application.as_bytes(),
        );
        let server = CanopyServer::start(settings, store.clone()).await?;
        let client = reqwest::Client::new();
        let (url, id) = create(&client, address, "original").await?;
        let source = workspace.path().join("source");
        run_git(None, &["init", "-b", "main", path_str(&source)?]).await?;
        run_git(Some(&source), &["config", "user.name", "Canopy Test"]).await?;
        run_git(
            Some(&source),
            &["config", "user.email", "canopy@example.invalid"],
        )
        .await?;
        tokio::fs::write(
            source.join("README.md"),
            b"retained through failed eviction\n",
        )
        .await?;
        run_git(Some(&source), &["add", "."]).await?;
        run_git(Some(&source), &["commit", "-m", "Original"]).await?;
        run_git(
            Some(&source),
            &[
                "-c",
                "http.extraHeader=Authorization: Bearer local-test-token",
                "push",
                &url,
                "HEAD:refs/heads/main",
            ],
        )
        .await?;
        let oid = run_git(Some(&source), &["rev-parse", "HEAD"]).await?;
        create(&client, address, "second").await?;
        create(&client, address, "third").await?;
        let local = workspace.path().join("server/runtime-v1");
        Ok(Self {
            workspace,
            store,
            server,
            client,
            address,
            layout,
            target: repository_target(tenant, application, id)?,
            repository_dir: local.join(hex::encode(id)),
            oid,
        })
    }

    async fn interrupt_release(
        &self,
        fault: ReleaseFault,
    ) -> Result<tokio::task::JoinHandle<reqwest::Result<reqwest::Response>>> {
        self.store.arm(
            self.layout.control_path(self.target.cell_id().as_bytes()),
            fault,
        );
        let request = self
            .client
            .post(format!("http://{}/api/repositories", self.address))
            .bearer_auth("local-test-token")
            .json(&serde_json::json!({"name":"fourth"}));
        let request = tokio::spawn(async move { request.send().await });
        self.store.wait().await?;
        assert!(self.repository_dir.join("repository.sqlite").exists());
        let control = CellAuthority::new(self.layout.clone())
            .load(self.target.cell_id())
            .await?
            .ok_or("authority missing")?;
        assert_eq!(control.value().state, ControlState::Serving);
        Ok(request)
    }

    async fn clone_original(&self, address: std::net::SocketAddr) -> Result {
        let clone = self.workspace.path().join("restored");
        run_git(
            None,
            &[
                "-c",
                "http.extraHeader=Authorization: Bearer local-test-token",
                "clone",
                &format!("http://{address}/canopy/original.git"),
                path_str(&clone)?,
            ],
        )
        .await?;
        assert_eq!(
            run_git(Some(&clone), &["rev-parse", "HEAD"]).await?,
            self.oid
        );
        assert_eq!(
            tokio::fs::read(clone.join("README.md")).await?,
            b"retained through failed eviction\n"
        );
        run_git(Some(&clone), &["fsck", "--full"]).await?;
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn lost_release_reply_is_resolved_before_local_cleanup() -> Result {
    let fixture = Fixture::new().await?;
    let pending = fixture.interrupt_release(ReleaseFault::LostReply).await?;
    fixture.store.proceed.notify_one();
    pending.await??.error_for_status()?;
    assert!(!fixture.repository_dir.exists());
    fixture.clone_original(fixture.address).await?;
    fixture.server.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn disconnected_admission_finishes_release_and_allows_a_later_restore() -> Result {
    let fixture = Fixture::new().await?;
    let pending = fixture.interrupt_release(ReleaseFault::Pause).await?;
    pending.abort();
    assert!(pending.await.unwrap_err().is_cancelled());
    fixture.store.proceed.notify_one();
    create(&fixture.client, fixture.address, "fourth").await?;
    fixture.clone_original(fixture.address).await?;
    fixture.server.shutdown().await?;
    Ok(())
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread")]
async fn failed_local_cleanup_retries_before_restoring_the_released_repository() -> Result {
    use std::os::unix::fs::PermissionsExt;
    let fixture = Fixture::new().await?;
    let pending = fixture.interrupt_release(ReleaseFault::Pause).await?;
    let original_permissions = std::fs::metadata(&fixture.repository_dir)?.permissions();
    std::fs::set_permissions(
        &fixture.repository_dir,
        std::fs::Permissions::from_mode(0o500),
    )?;
    fixture.store.proceed.notify_one();
    let response = pending.await?;
    std::fs::set_permissions(&fixture.repository_dir, original_permissions)?;
    assert_eq!(response?.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
    assert!(fixture.repository_dir.join("repository.sqlite").exists());
    let control = CellAuthority::new(fixture.layout.clone())
        .load(fixture.target.cell_id())
        .await?
        .ok_or("authority missing")?;
    assert_eq!(control.value().state, ControlState::Idle);
    fixture.clone_original(fixture.address).await?;
    fixture.server.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn denied_release_retains_local_state_and_recovers_after_node_restart() -> Result {
    let fixture = Fixture::new().await?;
    let pending = fixture.interrupt_release(ReleaseFault::Denied).await?;
    fixture.store.proceed.notify_one();
    assert_eq!(
        pending.await??.status(),
        reqwest::StatusCode::SERVICE_UNAVAILABLE
    );
    assert!(fixture.repository_dir.join("repository.sqlite").exists());
    let response = fixture
        .client
        .post(format!("http://{}/api/repositories", fixture.address))
        .bearer_auth("local-test-token")
        .json(&serde_json::json!({"name":"original"}))
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
    for (name, status) in [
        ("original", reqwest::StatusCode::SERVICE_UNAVAILABLE),
        ("second", reqwest::StatusCode::OK),
    ] {
        let response = fixture
            .client
            .get(format!(
                "http://{}/canopy/{name}.git/info/refs?service=git-upload-pack",
                fixture.address
            ))
            .bearer_auth("local-test-token")
            .send()
            .await?;
        assert_eq!(response.status(), status);
        response.bytes().await?;
    }
    // Only a new node/session recovers an owner whose terminal release failed.
    let address = available_address().await?;
    let settings = config(address, fixture.workspace.path().join("restarted"));
    fixture.server.shutdown().await?;
    let server = CanopyServer::start(settings, fixture.store.clone()).await?;
    let clone = fixture.workspace.path().join("restarted-clone");
    run_git(
        None,
        &[
            "-c",
            "http.extraHeader=Authorization: Bearer local-test-token",
            "clone",
            &format!("http://{address}/canopy/original.git"),
            path_str(&clone)?,
        ],
    )
    .await?;
    assert_eq!(
        run_git(Some(&clone), &["rev-parse", "HEAD"]).await?,
        fixture.oid
    );
    assert_eq!(
        tokio::fs::read(clone.join("README.md")).await?,
        b"retained through failed eviction\n"
    );
    run_git(Some(&clone), &["fsck", "--full"]).await?;
    server.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn paused_release_keeps_other_warm_repositories_available() -> Result {
    let fixture = Fixture::new().await?;
    let pending = fixture.interrupt_release(ReleaseFault::Pause).await?;
    let request = fixture
        .client
        .get(format!(
            "http://{}/api/repositories/original",
            fixture.address
        ))
        .bearer_auth("local-test-token");
    let mut returning = tokio::spawn(async move { request.send().await });
    let held = timeout(Duration::from_millis(100), &mut returning).await;
    let warm = fixture.read_warm_repository().await;
    fixture.store.proceed.notify_one();
    pending.await??.error_for_status()?;
    assert!(
        held.is_err(),
        "a releasing repository must wait for a fresh serving route"
    );
    returning.await??.error_for_status()?;
    warm?;
    fixture.clone_original(fixture.address).await?;
    fixture.server.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn paused_cold_activation_keeps_other_warm_repositories_available() -> Result {
    let fixture = Fixture::new().await?;
    create(&fixture.client, fixture.address, "fourth").await?;
    assert!(!fixture.repository_dir.exists());
    *fixture.store.paused_read.lock().unwrap() = Some(
        fixture
            .layout
            .control_path(fixture.target.cell_id().as_bytes()),
    );
    let mut requests = tokio::task::JoinSet::new();
    for _ in 0..12 {
        let request = fixture
            .client
            .get(format!(
                "http://{}/api/repositories/original",
                fixture.address
            ))
            .bearer_auth("local-test-token");
        requests.spawn(async move {
            request
                .send()
                .await?
                .error_for_status()?
                .json::<serde_json::Value>()
                .await
        });
    }
    fixture.store.wait().await?;
    let warm = fixture.read_warm_repository().await;
    fixture.store.proceed.notify_one();
    while let Some(result) = requests.join_next().await {
        assert_eq!(result??["name"], "original");
    }
    warm?;
    fixture.clone_original(fixture.address).await?;
    fixture.server.shutdown().await?;
    Ok(())
}

impl Fixture {
    async fn read_warm_repository(&self) -> Result {
        timeout(Duration::from_secs(2), async {
            let response = self
                .client
                .get(format!("http://{}/api/repositories/third", self.address))
                .bearer_auth("local-test-token")
                .send()
                .await?
                .error_for_status()?
                .json::<serde_json::Value>()
                .await?;
            assert_eq!(response["name"], "third");
            let response = self
                .client
                .get(format!(
                    "http://{}/canopy/third.git/info/refs?service=git-upload-pack",
                    self.address
                ))
                .bearer_auth("local-test-token")
                .header("Git-Protocol", "version=2")
                .send()
                .await?
                .error_for_status()?
                .bytes()
                .await?;
            assert!(response.starts_with(b"000eversion 2\n"));
            Ok::<_, Box<dyn std::error::Error>>(())
        })
        .await??;
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn paused_cold_repository_does_not_serialize_other_cold_activations() -> Result {
    let fixture = Fixture::new().await?;
    create(&fixture.client, fixture.address, "fourth").await?;
    assert!(!fixture.repository_dir.exists());
    *fixture.store.paused_read.lock().unwrap() = Some(
        fixture
            .layout
            .control_path(fixture.target.cell_id().as_bytes()),
    );
    let request = fixture
        .client
        .get(format!(
            "http://{}/api/repositories/original",
            fixture.address
        ))
        .bearer_auth("local-test-token");
    let pending = tokio::spawn(async move { request.send().await });
    fixture.store.wait().await?;
    // Both requests need cold admission in a full node. Pausing one authority
    // lookup must not prevent another repository from releasing a different slot.
    let independent = timeout(
        Duration::from_secs(2),
        create(&fixture.client, fixture.address, "independent"),
    )
    .await;
    fixture.store.proceed.notify_one();
    pending.await??.error_for_status()?;
    independent??;
    fixture.clone_original(fixture.address).await?;
    fixture.server.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn cancelled_cold_activation_retains_its_reserved_slot() -> Result {
    let fixture = Fixture::new().await?;
    create(&fixture.client, fixture.address, "fourth").await?;
    assert!(!fixture.repository_dir.exists());
    let mut uploads = Vec::new();
    let oid = hex::encode(Sha256::digest(b"x"));
    for name in ["third", "fourth"] {
        let mut stream = TcpStream::connect(fixture.address).await?;
        stream.write_all(format!(
            "PUT /canopy/{name}.git/info/lfs/objects/{oid} HTTP/1.1\r\nHost: {}\r\nAuthorization: Bearer local-test-token\r\nContent-Length: 1\r\nExpect: 100-continue\r\nConnection: close\r\n\r\n", fixture.address
        ).as_bytes()).await?;
        let mut response = vec![0; b"HTTP/1.1 100 Continue\r\n\r\n".len()];
        timeout(Duration::from_secs(5), stream.read_exact(&mut response)).await??;
        assert_eq!(response, b"HTTP/1.1 100 Continue\r\n\r\n");
        uploads.push(stream);
    }
    *fixture.store.paused_read.lock().unwrap() = Some(
        fixture
            .layout
            .control_path(fixture.target.cell_id().as_bytes()),
    );
    let request = fixture
        .client
        .get(format!(
            "http://{}/api/repositories/original",
            fixture.address
        ))
        .bearer_auth("local-test-token");
    let pending = tokio::spawn(async move { request.send().await });
    fixture.store.wait().await?;
    pending.abort();
    assert!(pending.await.unwrap_err().is_cancelled());
    // Two slots are pinned by uploads. The third has left its previous resident
    // and belongs to the paused restore, even after its HTTP client disconnects.
    let denied = timeout(
        Duration::from_secs(2),
        fixture
            .client
            .get(format!(
                "http://{}/api/repositories/second",
                fixture.address
            ))
            .bearer_auth("local-test-token")
            .send(),
    )
    .await;
    fixture.store.proceed.notify_one();
    assert_eq!(denied??.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
    fixture.clone_original(fixture.address).await?;
    drop(uploads);
    fixture.server.shutdown().await?;
    Ok(())
}
