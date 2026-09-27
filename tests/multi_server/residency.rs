use super::*;
use canopy_server::repository_target;
use cellule_runtime::{CellAuthority, CellStorageLayout, ControlState};
use cellule_store::Store;
use sha2::{Digest as _, Sha256};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    time::{Duration, timeout},
};

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

#[path = "residency/faults.rs"]
mod faults;

#[tokio::test(flavor = "multi_thread")]
async fn repositories_beyond_resident_capacity_restore_git_and_lfs_on_the_same_node() -> Result {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let workspace = tempfile::TempDir::new()?;
    let address = available_address().await?;
    let settings = config(address, workspace.path().join("server"));
    let tenant = settings.tenant;
    let application = settings.application;
    let layout = CellStorageLayout::new(
        Store::new(Arc::clone(&store)),
        settings.store_prefix.clone(),
        *application.as_bytes(),
    );
    let authority = CellAuthority::new(layout);
    let server = CanopyServer::start(settings, store).await?;
    let local_root = workspace.path().join("server/runtime-v1");
    let local = workspace.path().join("source");
    run_git(None, &["init", "-b", "main", path_str(&local)?]).await?;
    run_git(Some(&local), &["config", "user.name", "Canopy Test"]).await?;
    run_git(
        Some(&local),
        &["config", "user.email", "canopy@example.invalid"],
    )
    .await?;
    run_git(Some(&local), &["lfs", "install", "--local"]).await?;
    run_git(Some(&local), &["lfs", "track", "*.lfs"]).await?;
    let lfs_body = vec![0x6b; 1_300_000];
    tokio::fs::write(local.join("asset.lfs"), &lfs_body).await?;
    let client = reqwest::Client::new();
    let mut repositories = Vec::new();
    for index in 0..6 {
        let name = format!("repository-{index}");
        let (url, id) = create(&client, address, &name).await?;
        let body = format!("repository {index}\n");
        tokio::fs::write(local.join("README.md"), &body).await?;
        run_git(Some(&local), &["add", "."]).await?;
        run_git(Some(&local), &["commit", "-m", &name]).await?;
        run_git(
            Some(&local),
            &[
                "-c",
                "http.extraHeader=Authorization: Bearer local-test-token",
                "push",
                &url,
                "HEAD:refs/heads/main",
            ],
        )
        .await?;
        let oid = run_git(Some(&local), &["rev-parse", "HEAD"]).await?;
        repositories.push((url, id, body, oid));
        let mut serving = 0;
        for (_, id, _, _) in &repositories {
            let target = repository_target(tenant, application, *id)?;
            let control = authority
                .load(target.cell_id())
                .await?
                .ok_or("Cell authority missing")?;
            assert!(control.value().root.is_some());
            if control.value().state == ControlState::Serving {
                serving += 1;
            } else {
                assert_eq!(control.value().state, ControlState::Idle);
                assert!(control.value().owner.is_none());
                assert!(!local_root.join(hex::encode(id)).exists());
            }
        }
        assert_eq!(serving, repositories.len().min(3));
    }
    // Reading in creation order forces another eviction and exact-root restore
    // for each repository while the same node lease remains alive.
    for (index, (url, id, body, oid)) in repositories.iter().enumerate() {
        let target = repository_target(tenant, application, *id)?;
        let before = authority
            .load(target.cell_id())
            .await?
            .ok_or("Cell missing")?;
        let root = before.value().root.clone();
        let clone = workspace.path().join(format!("clone-{index}"));
        run_git(
            None,
            &[
                "-c",
                "http.extraHeader=Authorization: Bearer local-test-token",
                "clone",
                url,
                path_str(&clone)?,
            ],
        )
        .await?;
        assert_eq!(
            tokio::fs::read(clone.join("README.md")).await?,
            body.as_bytes()
        );
        run_git(Some(&clone), &["lfs", "install", "--local"]).await?;
        run_git(
            Some(&clone),
            &[
                "-c",
                "http.extraHeader=Authorization: Bearer local-test-token",
                "lfs",
                "pull",
            ],
        )
        .await?;
        assert!(tokio::fs::read(clone.join("asset.lfs")).await? == lfs_body);
        assert_eq!(&run_git(Some(&clone), &["rev-parse", "HEAD"]).await?, oid);
        run_git(Some(&clone), &["fsck", "--full"]).await?;
        let after = authority
            .load(target.cell_id())
            .await?
            .ok_or("Cell missing")?;
        assert_eq!(
            after.value().root,
            root,
            "read-only restoration published a new root"
        );
    }
    server.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn active_uploads_block_eviction_until_a_request_disconnects() -> Result {
    let workspace = tempfile::TempDir::new()?;
    let address = available_address().await?;
    let server = CanopyServer::start(
        config(address, workspace.path().join("server")),
        Arc::new(InMemory::new()),
    )
    .await?;
    let client = reqwest::Client::new();
    let body = b"pinned LFS upload";
    let oid = hex::encode(Sha256::digest(body));
    let mut uploads = Vec::new();
    for index in 0..3 {
        let name = format!("pinned-{index}");
        create(&client, address, &name).await?;
        let mut stream = TcpStream::connect(address).await?;
        let headers = format!(
            "PUT /canopy/{name}.git/info/lfs/objects/{oid} HTTP/1.1\r\nHost: {address}\r\nAuthorization: Bearer local-test-token\r\nContent-Length: {}\r\nExpect: 100-continue\r\nConnection: close\r\n\r\n",
            body.len()
        );
        stream.write_all(headers.as_bytes()).await?;
        let mut response = vec![0; b"HTTP/1.1 100 Continue\r\n\r\n".len()];
        timeout(Duration::from_secs(5), stream.read_exact(&mut response)).await??;
        assert_eq!(response, b"HTTP/1.1 100 Continue\r\n\r\n");
        uploads.push(stream);
    }
    let creation = || {
        client
            .post(format!("http://{address}/api/repositories"))
            .bearer_auth("local-test-token")
            .json(&serde_json::json!({"name":"fourth"}))
            .send()
    };
    assert_eq!(
        creation().await?.status(),
        reqwest::StatusCode::SERVICE_UNAVAILABLE
    );
    drop(uploads.remove(0));
    timeout(Duration::from_secs(5), async {
        loop {
            let response = creation().await?;
            if response.status().is_success() {
                break Ok::<_, reqwest::Error>(());
            }
            assert_eq!(response.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await??;
    for (index, mut stream) in uploads.into_iter().enumerate() {
        stream.write_all(body).await?;
        let mut response = Vec::new();
        timeout(Duration::from_secs(5), stream.read_to_end(&mut response)).await??;
        assert!(
            response.starts_with(b"HTTP/1.1 200 OK\r\n"),
            "{}",
            String::from_utf8_lossy(&response)
        );
        let fetched = client
            .get(format!(
                "http://{address}/canopy/pinned-{}.git/info/lfs/objects/{oid}",
                index + 1
            ))
            .bearer_auth("local-test-token")
            .send()
            .await?
            .error_for_status()?
            .bytes()
            .await?;
        assert_eq!(fetched.as_ref(), body);
    }
    server.shutdown().await?;
    Ok(())
}

async fn create(
    client: &reqwest::Client,
    address: std::net::SocketAddr,
    name: &str,
) -> Result<(String, [u8; 16])> {
    let response: serde_json::Value = client
        .post(format!("http://{address}/api/repositories"))
        .bearer_auth("local-test-token")
        .json(&serde_json::json!({"name":name}))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    Ok((
        response["clone_url"]
            .as_str()
            .ok_or("clone URL missing")?
            .into(),
        uuid::Uuid::parse_str(
            response["repository_id"]
                .as_str()
                .ok_or("repository UUID missing")?,
        )?
        .into_bytes(),
    ))
}

#[tokio::test(flavor = "multi_thread")]
async fn configured_residency_admits_a_larger_working_set_and_stays_bounded() -> Result {
    let workspace = tempfile::TempDir::new()?;
    let address = available_address().await?;
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut settings = config(address, workspace.path().join("server"));
    settings.max_active_repositories = 100;
    let tenant = settings.tenant;
    let application = settings.application;
    let authority = CellAuthority::new(CellStorageLayout::new(
        Store::new(Arc::clone(&store)),
        settings.store_prefix.clone(),
        *application.as_bytes(),
    ));
    let server = CanopyServer::start(settings, store).await?;
    let client = reqwest::Client::new();
    let mut targets = Vec::new();
    for index in 0..100 {
        let (_, id) = create(&client, address, &format!("working-set-{index}")).await?;
        targets.push(repository_target(tenant, application, id)?);
    }
    for target in &targets {
        let control = authority
            .load(target.cell_id())
            .await?
            .ok_or("Cell missing")?;
        assert_eq!(control.value().state, ControlState::Serving);
    }
    let (_, id) = create(&client, address, "overflow").await?;
    targets.push(repository_target(tenant, application, id)?);
    let mut serving = 0;
    for target in &targets {
        let control = authority
            .load(target.cell_id())
            .await?
            .ok_or("Cell missing")?;
        serving += usize::from(control.value().state == ControlState::Serving);
    }
    assert_eq!(serving, 100);
    client
        .get(format!("http://{address}/api/repositories/working-set-0"))
        .bearer_auth("local-test-token")
        .send()
        .await?
        .error_for_status()?;
    server.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn invalid_residency_limits_fail_before_creating_local_state() -> Result {
    let workspace = tempfile::TempDir::new()?;
    for limit in [0, 10_000, usize::MAX] {
        let path = workspace.path().join(format!("limit-{limit}"));
        let mut settings = config(available_address().await?, path.clone());
        settings.max_active_repositories = limit;
        let result = CanopyServer::start(settings, Arc::new(InMemory::new())).await;
        assert!(matches!(
            result,
            Err(canopy_server::server::ServerError::Http(_))
        ));
        assert!(!path.exists());
    }
    Ok(())
}
