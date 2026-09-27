#[path = "multi_server/account_admin.rs"]
mod account_admin;
#[path = "multi_server/account_audit.rs"]
mod account_audit;
#[path = "multi_server/accounts.rs"]
mod accounts;
#[path = "multi_server/backup.rs"]
mod backup;
#[path = "multi_server/browse.rs"]
mod browse;
#[path = "multi_server/bulk_refs.rs"]
mod bulk_refs;
#[path = "multi_server/candidates.rs"]
mod candidates;
#[path = "multi_server/comparison.rs"]
mod comparison;
#[path = "multi_server/compatibility.rs"]
mod compatibility;
#[path = "multi_server/merge.rs"]
mod merge;
#[path = "multi_server/partial_clone.rs"]
mod partial_clone;
#[path = "support/paused_blobs.rs"]
mod paused_blobs;
#[path = "multi_server/pulls.rs"]
mod pulls;
#[path = "multi_server/rebase.rs"]
mod rebase;
#[path = "multi_server/sha256.rs"]
mod sha256;
#[path = "multi_server/size.rs"]
mod size;
#[path = "multi_server/ssh.rs"]
mod ssh;
#[path = "multi_server/ssh_keys.rs"]
mod ssh_keys;
use std::{path::Path, sync::Arc};

use canopy_server::server::{CanopyServer, ServerConfig};
use crab_cell_runtime::{ApplicationId, Digest, TenantId, identity::NodeId};
use ed25519_dalek::SigningKey;
use object_store::{ObjectStore, memory::InMemory, path::Path as StorePath};
use tokio::{net::TcpListener, process::Command};

#[path = "multi_server/branch_rules.rs"]
mod branch_rules;
#[path = "multi_server/checks.rs"]
mod checks;
#[path = "multi_server/collaborators.rs"]
mod collaborators;
#[path = "multi_server/default_branch.rs"]
mod default_branch;
#[path = "multi_server/deployment.rs"]
mod deployment;
#[path = "multi_server/discovery.rs"]
mod discovery;
#[path = "multi_server/issues.rs"]
mod issues;
#[path = "multi_server/large_objects.rs"]
mod large_objects;
#[path = "multi_server/lfs_locks.rs"]
mod lfs_locks;
#[path = "multi_server/lifecycle.rs"]
mod lifecycle;
#[path = "multi_server/peers.rs"]
mod peers;
#[path = "multi_server/residency.rs"]
mod residency;
#[path = "multi_server/tokens.rs"]
mod tokens;
#[path = "multi_server/transfers.rs"]
mod transfers;
#[path = "multi_server/visibility.rs"]
mod visibility;
#[path = "multi_server/workspace.rs"]
mod workspace;

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
    let anonymous = client.get(&listing_url).send().await?;
    assert_eq!(anonymous.status(), reqwest::StatusCode::OK);
    assert_eq!(
        anonymous.json::<serde_json::Value>().await?["repositories"],
        serde_json::json!([])
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
    let reader_token = format!("cnp_{}", "ab".repeat(32));
    let account_url = format!("http://{first_address}/api/accounts");
    assert_eq!(
        client
            .post(&account_url)
            .bearer_auth("local-test-token")
            .json(&serde_json::json!({"name": "reader", "token": reader_token, "scope": "read"}))
            .send()
            .await?
            .status(),
        reqwest::StatusCode::OK
    );
    let reader_listing: serde_json::Value = client
        .get(&listing_url)
        .bearer_auth(&reader_token)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(reader_listing["repositories"], serde_json::json!([]));
    assert_eq!(
        client
            .post(&listing_url)
            .bearer_auth(&reader_token)
            .json(&serde_json::json!({"name": "reader-repo"}))
            .send()
            .await?
            .status(),
        reqwest::StatusCode::FORBIDDEN
    );
    let first_id = listing["repositories"]
        .as_array()
        .ok_or("list missing")?
        .iter()
        .find(|entry| entry["name"] == "example")
        .and_then(|entry| entry["repository_id"].as_str())
        .ok_or("repository UUID missing")?;

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
    let reader_refs = format!("{first_url}/info/refs?service=git-upload-pack");
    assert_eq!(
        client
            .get(&reader_refs)
            .bearer_auth(&reader_token)
            .send()
            .await?
            .status(),
        reqwest::StatusCode::NOT_FOUND
    );
    let collaborator_url = format!("{listing_url}/example/collaborators/reader");
    assert_eq!(
        client
            .put(&collaborator_url)
            .bearer_auth("local-test-token")
            .json(&serde_json::json!({"role": "read"}))
            .send()
            .await?
            .status(),
        reqwest::StatusCode::OK
    );
    assert_eq!(
        client
            .get(&reader_refs)
            .bearer_auth(&reader_token)
            .send()
            .await?
            .status(),
        reqwest::StatusCode::OK
    );
    assert_eq!(
        client
            .get(&reader_refs)
            .basic_auth("reader", Some(&reader_token))
            .send()
            .await?
            .status(),
        reqwest::StatusCode::OK
    );
    assert_eq!(
        client
            .get(&reader_refs)
            .basic_auth("canopy", Some(&reader_token))
            .send()
            .await?
            .status(),
        reqwest::StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        client
            .get(format!("{first_url}/info/refs?service=git-receive-pack"))
            .bearer_auth(&reader_token)
            .send()
            .await?
            .status(),
        reqwest::StatusCode::FORBIDDEN
    );
    assert_eq!(
        client
            .put(&collaborator_url)
            .bearer_auth("local-test-token")
            .json(&serde_json::json!({"role": "write"}))
            .send()
            .await?
            .status(),
        reqwest::StatusCode::OK
    );
    assert_eq!(
        client
            .get(format!("{first_url}/info/refs?service=git-receive-pack"))
            .bearer_auth(&reader_token)
            .send()
            .await?
            .status(),
        reqwest::StatusCode::FORBIDDEN
    );
    assert_eq!(
        client
            .put(&collaborator_url)
            .bearer_auth("local-test-token")
            .json(&serde_json::json!({"role": "read"}))
            .send()
            .await?
            .status(),
        reqwest::StatusCode::OK
    );
    assert_eq!(
        client
            .get(format!("{other_url}/info/refs?service=git-upload-pack"))
            .bearer_auth(&reader_token)
            .send()
            .await?
            .status(),
        reqwest::StatusCode::NOT_FOUND
    );
    let reader_clone = workspace.path().join("reader-clone");
    let reader_header = format!("http.extraHeader=Authorization: Bearer {reader_token}");
    run_git(
        None,
        &[
            "-c",
            &reader_header,
            "clone",
            &first_url,
            path_str(&reader_clone)?,
        ],
    )
    .await?;
    assert_eq!(
        run_git(Some(&reader_clone), &["rev-parse", "HEAD"]).await?,
        original
    );
    let writer_token = format!("cnp_{}", "cd".repeat(32));
    client
        .post(&account_url)
        .bearer_auth("local-test-token")
        .json(&serde_json::json!({"name":"writer", "token":writer_token, "scope":"write"}))
        .send()
        .await?
        .error_for_status()?;
    let writer_url = format!("{listing_url}/example/collaborators/writer");
    client
        .put(&writer_url)
        .bearer_auth("local-test-token")
        .json(&serde_json::json!({"role":"write"}))
        .send()
        .await?
        .error_for_status()?;
    let owner_id = uuid::Uuid::new_v4().to_string();
    let writer_id = uuid::Uuid::new_v4().to_string();
    let request = |token: &str, id: &str| {
        client
            .post(format!("{first_url}/git-receive-pack"))
            .bearer_auth(token)
            .header("Idempotency-Key", id)
            .header("Content-Type", "text/plain")
            .body(Vec::new())
    };
    assert_eq!(
        request("local-test-token", &owner_id)
            .send()
            .await?
            .status(),
        reqwest::StatusCode::UNSUPPORTED_MEDIA_TYPE
    );
    assert_eq!(
        request(&writer_token, &owner_id).send().await?.status(),
        reqwest::StatusCode::CONFLICT
    );
    assert_eq!(
        request(&writer_token, &writer_id).send().await?.status(),
        reqwest::StatusCode::UNSUPPORTED_MEDIA_TYPE
    );
    assert_eq!(
        request("local-test-token", "invalid")
            .send()
            .await?
            .status(),
        reqwest::StatusCode::BAD_REQUEST
    );
    client
        .delete(&writer_url)
        .bearer_auth("local-test-token")
        .send()
        .await?
        .error_for_status()?;
    assert_eq!(
        request(&writer_token, &writer_id).send().await?.status(),
        reqwest::StatusCode::NOT_FOUND
    );
    let rename_url = format!("{listing_url}/example");
    assert_eq!(
        client
            .patch(&rename_url)
            .json(&serde_json::json!({"name": "renamed", "repository_id": first_id}))
            .send()
            .await?
            .status(),
        reqwest::StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        client
            .patch(&rename_url)
            .bearer_auth("local-test-token")
            .json(&serde_json::json!({"name": "other", "repository_id": first_id}))
            .send()
            .await?
            .status(),
        reqwest::StatusCode::CONFLICT
    );
    let renamed: serde_json::Value = client
        .patch(&rename_url)
        .bearer_auth("local-test-token")
        .json(&serde_json::json!({"name": "renamed", "repository_id": first_id}))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(renamed["repository_id"], first_id);
    let renamed_url = renamed["clone_url"].as_str().ok_or("clone URL missing")?;
    assert_eq!(
        client
            .patch(&rename_url)
            .bearer_auth("local-test-token")
            .json(&serde_json::json!({"name": "renamed", "repository_id": first_id}))
            .send()
            .await?
            .status(),
        reqwest::StatusCode::OK
    );
    assert_eq!(
        client
            .get(format!("{first_url}/info/refs?service=git-upload-pack"))
            .bearer_auth("local-test-token")
            .send()
            .await?
            .status(),
        reqwest::StatusCode::NOT_FOUND
    );
    let live_clone = workspace.path().join("renamed-live-clone");
    run_git(
        None,
        &[
            "-c",
            "http.extraHeader=Authorization: Bearer local-test-token",
            "clone",
            renamed_url,
            path_str(&live_clone)?,
        ],
    )
    .await?;
    assert_eq!(
        run_git(Some(&live_clone), &["rev-parse", "HEAD"]).await?,
        original
    );
    first.shutdown().await?;

    let second_address = available_address().await?;
    let second_url = format!("http://{second_address}/canopy/renamed.git");
    let second = CanopyServer::start(
        config(second_address, workspace.path().join("second")),
        store,
    )
    .await?;
    assert_eq!(
        client
            .get(format!("{second_url}/info/refs?service=git-upload-pack"))
            .bearer_auth(&reader_token)
            .send()
            .await?
            .status(),
        reqwest::StatusCode::OK
    );
    let reader_listing: serde_json::Value = client
        .get(format!("http://{second_address}/api/repositories"))
        .bearer_auth(&reader_token)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(
        reader_listing["repositories"]
            .as_array()
            .ok_or("missing reader listing")?
            .len(),
        1
    );
    assert_eq!(reader_listing["repositories"][0]["name"], "renamed");
    assert_eq!(reader_listing["repositories"][0]["repository_id"], first_id);
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
    assert_eq!(
        client
            .delete(format!(
                "http://{second_address}/api/repositories/renamed/collaborators/reader"
            ))
            .bearer_auth("local-test-token")
            .send()
            .await?
            .status(),
        reqwest::StatusCode::NO_CONTENT
    );
    assert_eq!(
        client
            .get(format!("{second_url}/info/refs?service=git-upload-pack"))
            .bearer_auth(&reader_token)
            .send()
            .await?
            .status(),
        reqwest::StatusCode::NOT_FOUND
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
        peer_ca_pem: None,
        listen: address,
        ssh: None,
        data_dir,
        store_prefix: StorePath::from("single-server-test"),
        local_disk_limit_bytes: 1 << 30,
        max_active_repositories: 3,
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
