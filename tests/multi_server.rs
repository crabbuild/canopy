use std::{path::Path, sync::Arc};

use canopy_server::server::{CanopyServer, ServerConfig};
use cellule_runtime::{ApplicationId, Digest, NodeId, TenantId};
use ed25519_dalek::SigningKey;
use object_store::{ObjectStore, memory::InMemory, path::Path as StorePath};
use tokio::{net::TcpListener, process::Command};

#[tokio::test(flavor = "multi_thread")]
async fn leased_server_recovers_two_repositories_with_git_and_lfs()
-> Result<(), Box<dyn std::error::Error>> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let workspace = tempfile::TempDir::new()?;
    let first_address = available_address().await?;
    let first = CanopyServer::start(
        config(first_address, workspace.path().join("first")),
        Arc::clone(&store),
    )
    .await?;
    assert_eq!(first.local_addr(), first_address);
    let first_url = create_repository(first_address, "example").await?;
    assert_eq!(
        create_repository(first_address, "example").await?,
        first_url
    );
    let client = reqwest::Client::new();
    let advertisement = client
        .get(format!("{first_url}/info/refs?service=git-upload-pack"))
        .bearer_auth("local-test-token")
        .header("Git-Protocol", "version=2")
        .send()
        .await?;
    assert_eq!(advertisement.status(), reqwest::StatusCode::OK);
    assert!(advertisement.bytes().await?.starts_with(b"000eversion 2\n"));
    let other_url = create_repository(first_address, "other").await?;
    assert_ne!(first_url, other_url);
    let listing_url = format!("http://{first_address}/api/repositories");
    assert_eq!(
        client.get(&listing_url).send().await?.status(),
        reqwest::StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        client
            .post(&listing_url)
            .bearer_auth("local-test-token")
            .json(&serde_json::json!({"name": "../bad"}))
            .send()
            .await?
            .status(),
        reqwest::StatusCode::UNPROCESSABLE_ENTITY
    );
    let listing: serde_json::Value = client
        .get(&listing_url)
        .bearer_auth("local-test-token")
        .send()
        .await?
        .json()
        .await?;
    assert_eq!(
        listing["repositories"]
            .as_array()
            .ok_or("list missing")?
            .len(),
        2
    );

    let local = workspace.path().join("local");
    run_git(None, &["init", "-b", "main", path_str(&local)?]).await?;
    run_git(Some(&local), &["config", "user.name", "Canopy Test"]).await?;
    run_git(
        Some(&local),
        &["config", "user.email", "canopy@example.invalid"],
    )
    .await?;
    run_git(Some(&local), &["lfs", "install", "--local"]).await?;
    run_git(Some(&local), &["lfs", "track", "*.lfs"]).await?;
    tokio::fs::write(local.join("README.md"), b"restored by a leased node\n").await?;
    let lfs_body = vec![0x79; 1_300_000];
    tokio::fs::write(local.join("asset.lfs"), &lfs_body).await?;
    run_git(
        Some(&local),
        &["add", ".gitattributes", "README.md", "asset.lfs"],
    )
    .await?;
    run_git(Some(&local), &["commit", "-m", "Initial commit"]).await?;
    run_git(
        Some(&local),
        &[
            "-c",
            "http.extraHeader=Authorization: Bearer local-test-token",
            "push",
            &first_url,
            "HEAD:refs/heads/main",
        ],
    )
    .await?;
    let original = run_git(Some(&local), &["rev-parse", "HEAD"]).await?;
    let other = workspace.path().join("other");
    run_git(None, &["init", "-b", "main", path_str(&other)?]).await?;
    run_git(Some(&other), &["config", "user.name", "Canopy Test"]).await?;
    run_git(
        Some(&other),
        &["config", "user.email", "canopy@example.invalid"],
    )
    .await?;
    tokio::fs::write(other.join("README.md"), b"another Repository Cell\n").await?;
    run_git(Some(&other), &["add", "README.md"]).await?;
    run_git(Some(&other), &["commit", "-m", "Other repository"]).await?;
    run_git(
        Some(&other),
        &[
            "-c",
            "http.extraHeader=Authorization: Bearer local-test-token",
            "push",
            &other_url,
            "HEAD:refs/heads/main",
        ],
    )
    .await?;
    let other_oid = run_git(Some(&other), &["rev-parse", "HEAD"]).await?;
    first.shutdown().await?;

    let second_address = available_address().await?;
    let second_url = format!("http://{second_address}/canopy/example.git");
    let second = CanopyServer::start(
        config(second_address, workspace.path().join("second")),
        store,
    )
    .await?;
    let clone = workspace.path().join("clone");
    run_git(
        None,
        &[
            "-c",
            "http.extraHeader=Authorization: Bearer local-test-token",
            "clone",
            &second_url,
            path_str(&clone)?,
        ],
    )
    .await?;
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
    assert_eq!(
        tokio::fs::read(clone.join("README.md")).await?,
        b"restored by a leased node\n"
    );
    assert!(tokio::fs::read(clone.join("asset.lfs")).await? == lfs_body);
    assert_eq!(
        run_git(Some(&clone), &["rev-parse", "HEAD"]).await?,
        original
    );
    let other_clone = workspace.path().join("other-clone");
    let other_second_url = format!("http://{second_address}/canopy/other.git");
    run_git(
        None,
        &[
            "-c",
            "http.extraHeader=Authorization: Bearer local-test-token",
            "clone",
            &other_second_url,
            path_str(&other_clone)?,
        ],
    )
    .await?;
    assert_eq!(
        tokio::fs::read(other_clone.join("README.md")).await?,
        b"another Repository Cell\n"
    );
    assert_eq!(
        run_git(Some(&other_clone), &["rev-parse", "HEAD"]).await?,
        other_oid
    );
    second.shutdown().await?;
    Ok(())
}

async fn create_repository(
    address: std::net::SocketAddr,
    name: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    let response = reqwest::Client::new()
        .post(format!("http://{address}/api/repositories"))
        .bearer_auth("local-test-token")
        .json(&serde_json::json!({"name": name}))
        .send()
        .await?;
    if !response.status().is_success() {
        return Err(format!(
            "repository creation failed: {} {}",
            response.status(),
            response.text().await?
        )
        .into());
    }
    let body: serde_json::Value = response.json().await?;
    Ok(body["clone_url"]
        .as_str()
        .ok_or("clone URL missing")?
        .into())
}

fn config(address: std::net::SocketAddr, data_dir: std::path::PathBuf) -> ServerConfig {
    ServerConfig {
        tenant: TenantId::from_bytes([51; 16]),
        application: ApplicationId::from_bytes([52; 16]),
        node: NodeId::from_bytes([54; 16]),
        fleet: Digest::from_bytes([55; 32]),
        image: Digest::from_bytes([56; 32]),
        signing_key: SigningKey::from_bytes(&[57; 32]),
        owner: "canopy".into(),
        token: "local-test-token".into(),
        public_url: format!("http://{address}"),
        peer_endpoint: "https://canopy.test".into(),
        listen: address,
        data_dir,
        store_prefix: StorePath::from("single-server-test"),
        local_disk_limit_bytes: 1 << 30,
    }
}

async fn available_address() -> Result<std::net::SocketAddr, std::io::Error> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    listener.local_addr()
}

fn path_str(path: &Path) -> Result<&str, &'static str> {
    path.to_str().ok_or("path is not UTF-8")
}

async fn run_git(cwd: Option<&Path>, args: &[&str]) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let mut command = Command::new("git");
    command.arg("-c").arg("credential.helper=");
    command.env("GIT_TERMINAL_PROMPT", "0");
    if let Some(cwd) = cwd {
        command.current_dir(cwd);
    }
    let output = command.args(args).output().await?;
    if !output.status.success() {
        return Err(format!(
            "git {} failed: {}",
            args.join(" "),
            String::from_utf8_lossy(&output.stderr)
        )
        .into());
    }
    Ok(output.stdout)
}
