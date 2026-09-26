use std::{path::Path, sync::Arc};

use canopy_server::server::{SingleRepositoryConfig, SingleRepositoryServer};
use cellule_runtime::{ApplicationId, Digest, NodeId, TenantId};
use ed25519_dalek::SigningKey;
use object_store::{ObjectStore, memory::InMemory, path::Path as StorePath};
use tokio::{net::TcpListener, process::Command};

#[tokio::test(flavor = "multi_thread")]
async fn leased_server_restarts_from_storage_and_serves_git_and_lfs()
-> Result<(), Box<dyn std::error::Error>> {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let workspace = tempfile::TempDir::new()?;
    let first_address = available_address().await?;
    let first_url = format!("http://{first_address}/repo.git");
    let first = SingleRepositoryServer::start(
        config(first_address, workspace.path().join("first")),
        Arc::clone(&store),
    )
    .await?;
    assert_eq!(first.local_addr(), first_address);

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
    first.shutdown().await?;

    let second_address = available_address().await?;
    let second_url = format!("http://{second_address}/repo.git");
    let second = SingleRepositoryServer::start(
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
    second.shutdown().await?;
    Ok(())
}

fn config(address: std::net::SocketAddr, data_dir: std::path::PathBuf) -> SingleRepositoryConfig {
    let mut repository = [53; 16];
    repository[6] = 0x73;
    repository[8] = 0x83;
    SingleRepositoryConfig {
        tenant: TenantId::from_bytes([51; 16]),
        application: ApplicationId::from_bytes([52; 16]),
        repository,
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
