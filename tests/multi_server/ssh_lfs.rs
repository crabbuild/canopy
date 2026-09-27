use super::*;
use reqwest::{Client, StatusCode};
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};

async fn grant(
    session: &russh::client::Handle<PinnedHost>,
    repository: &str,
    operation: &str,
) -> Result<Value> {
    let command = format!("git-lfs-authenticate '{repository}' {operation}");
    let (code, body, error) = exec(session, &command).await?;
    assert_eq!(code, 0, "{}", String::from_utf8_lossy(&error));
    Ok(serde_json::from_slice(&body)?)
}

fn authorization(grant: &Value) -> Result<&str> {
    grant["header"]["Authorization"]
        .as_str()
        .ok_or("LFS authorization missing".into())
}

async fn status(request: reqwest::RequestBuilder, expected: StatusCode) -> Result {
    let response = request.send().await?;
    assert_eq!(response.status(), expected, "{}", response.text().await?);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn stock_lfs_uses_ssh_identity_for_push_pull_and_locks_after_restore() -> Result {
    let workspace = tempfile::TempDir::new()?;
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let host = ssh_key::PrivateKey::new(
        ssh_key::private::Ed25519Keypair::from_seed(&[22; 32]).into(),
        "test",
    )?;
    let address = available_address().await?;
    let server = CanopyServer::start(
        server_config(address, workspace.path().join("first"), &host)?,
        store.clone(),
    )
    .await?;
    create_repository(address, "ssh-lfs").await?;
    let key = key(workspace.path(), "writer").await?;
    register(address, "canopy", &key, "write").await?;
    let ssh_address = server.ssh_addr().ok_or("SSH listener missing")?;
    let known = workspace.path().join("known_hosts");
    known_host(&known, ssh_address, &host).await?;
    let ssh = transport(&key, &known)?;
    let url = format!("ssh://git@{ssh_address}/canopy/ssh-lfs.git");
    let source = workspace.path().join("source");
    git(None, &ssh, &["init", "-b", "main", path_str(&source)?]).await?;
    for (name, value) in [
        ("user.name", "Test"),
        ("user.email", "test@example.invalid"),
        ("commit.gpgsign", "false"),
        ("lfs.locksverify", "true"),
    ] {
        git(Some(&source), &ssh, &["config", name, value]).await?;
    }
    git(Some(&source), &ssh, &["remote", "add", "origin", &url]).await?;
    git(Some(&source), &ssh, &["lfs", "install", "--local"]).await?;
    git(
        Some(&source),
        &ssh,
        &["lfs", "track", "--lockable", "*.lfs"],
    )
    .await?;
    let bytes = vec![0x79; 9 * 1024 * 1024];
    tokio::fs::write(source.join("asset.lfs"), &bytes).await?;
    git(Some(&source), &ssh, &["add", "."]).await?;
    git(
        Some(&source),
        &ssh,
        &["commit", "-m", "LFS via SSH credentials"],
    )
    .await?;
    git(Some(&source), &ssh, &["lfs", "lock", "asset.lfs"]).await?;
    git(Some(&source), &ssh, &["push", "-u", "origin", "main"]).await?;
    let session = connect(ssh_address, &host, &key, "git", true).await?;
    let saved = grant(&session, "canopy/ssh-lfs.git", "download").await?;
    session
        .disconnect(russh::Disconnect::ByApplication, "", "")
        .await?;
    server.shutdown().await?;

    let address = available_address().await?;
    let restored = CanopyServer::start(
        server_config(address, workspace.path().join("restored"), &host)?,
        store,
    )
    .await?;
    let ssh_address = restored.ssh_addr().ok_or("SSH listener missing")?;
    known_host(&known, ssh_address, &host).await?;
    let url = format!("ssh://git@{ssh_address}/canopy/ssh-lfs.git");
    // Grants are in the Directory Cell, so another node can authenticate the
    // exact credential after disk loss without retaining the original SSH session.
    status(
        Client::new()
            .get(format!(
                "http://{address}/canopy/ssh-lfs.git/info/lfs/locks"
            ))
            .header("Authorization", authorization(&saved)?),
        StatusCode::OK,
    )
    .await?;
    let clone = workspace.path().join("clone");
    git(None, &ssh, &["clone", &url, path_str(&clone)?]).await?;
    git(Some(&clone), &ssh, &["lfs", "install", "--local"]).await?;
    git(Some(&clone), &ssh, &["lfs", "pull"]).await?;
    assert_eq!(tokio::fs::read(clone.join("asset.lfs")).await?, bytes);
    let oid = hex::encode(Sha256::digest(&bytes));
    let object = clone
        .join(".git/lfs/objects")
        .join(&oid[..2])
        .join(&oid[2..4])
        .join(&oid);
    tokio::fs::remove_file(&object).await?;
    let partial = clone
        .join(".git/lfs/incomplete")
        .join(format!("{oid}.part"));
    tokio::fs::create_dir_all(partial.parent().ok_or("missing LFS directory")?).await?;
    tokio::fs::write(&partial, &bytes[..8 * 1024 * 1024 + 11]).await?;
    let resumed = git_command(Some(&clone), &ssh, &["lfs", "fetch", "origin", "main"])
        .env("GIT_TRACE", "1")
        .output()
        .await?;
    assert!(
        resumed.status.success(),
        "{}",
        String::from_utf8_lossy(&resumed.stderr)
    );
    assert!(String::from_utf8_lossy(&resumed.stderr).contains("HTTP: 206"));
    assert_eq!(tokio::fs::read(&object).await?, bytes);
    let locks = git(Some(&clone), &ssh, &["lfs", "locks", "--json"]).await?;
    assert!(String::from_utf8(locks)?.contains("asset.lfs"));
    git(Some(&clone), &ssh, &["lfs", "unlock", "asset.lfs"]).await?;
    git(Some(&clone), &ssh, &["fsck", "--strict", "--full"]).await?;
    restored.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn ssh_lfs_grants_fence_repository_operation_revocation_and_expiry() -> Result {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN)
        .try_init();
    let workspace = tempfile::TempDir::new()?;
    let host = ssh_key::PrivateKey::new(
        ssh_key::private::Ed25519Keypair::from_seed(&[23; 32]).into(),
        "test",
    )?;
    let address = available_address().await?;
    let server = CanopyServer::start(
        server_config(address, workspace.path().join("server"), &host)?,
        Arc::new(InMemory::new()),
    )
    .await?;
    create_repository(address, "grants").await?;
    create_repository(address, "other").await?;
    let key = key(workspace.path(), "reader").await?;
    register(address, "canopy", &key, "read").await?;
    let ssh_address = server.ssh_addr().ok_or("SSH listener missing")?;
    let reader = connect(ssh_address, &host, &key, "git", true).await?;
    let download = grant(&reader, "/canopy/grants.git", "download").await?;
    let expires = download["expires_in"].as_u64().ok_or("expiry missing")?;
    assert_eq!(expires, 300);
    let expiry = tokio::time::Instant::now() + Duration::from_secs(expires + 1);
    let auth = authorization(&download)?;
    let base = format!("http://{address}");
    let endpoint = format!("{base}/canopy/grants.git/info/lfs");
    assert_eq!(download["href"], endpoint);
    let client = Client::new();
    status(
        client
            .get(format!("{endpoint}/locks"))
            .header("Authorization", auth),
        StatusCode::OK,
    )
    .await?;
    for path in [
        "/api/session",
        "/canopy/grants.git/info/refs?service=git-upload-pack",
        "/canopy/other.git/info/lfs/locks",
    ] {
        status(
            client
                .get(format!("{base}{path}"))
                .header("Authorization", auth),
            StatusCode::UNAUTHORIZED,
        )
        .await?;
    }
    status(
        client
            .post(format!("{endpoint}/objects/batch"))
            .header("Authorization", auth)
            .json(&json!({"operation":"upload","objects":[]})),
        StatusCode::FORBIDDEN,
    )
    .await?;
    status(
        client
            .put(format!("{endpoint}/objects/{}", "01".repeat(32)))
            .header("Authorization", auth)
            .body("forbidden"),
        StatusCode::UNAUTHORIZED,
    )
    .await?;
    let (code, _, _) = exec(&reader, "git-lfs-authenticate canopy/grants.git upload").await?;
    assert_eq!(code, 1);

    client
        .post(format!("{base}/api/accounts"))
        .bearer_auth(AUTH)
        .json(&json!({"name":"member","token":format!("cnp_{}", "64".repeat(32)),"scope":"write"}))
        .send()
        .await?
        .error_for_status()?;
    let member_key = super::key(workspace.path(), "member").await?;
    let id = register(address, "member", &member_key, "write").await?;
    let member = connect(ssh_address, &host, &member_key, "git", true).await?;
    assert_eq!(
        exec(&member, "git-lfs-authenticate canopy/grants.git upload")
            .await?
            .0,
        1
    );
    let membership = format!("{base}/api/repositories/grants/collaborators/member");
    client
        .put(&membership)
        .bearer_auth(AUTH)
        .json(&json!({"role":"write"}))
        .send()
        .await?
        .error_for_status()?;
    let upload = grant(&member, "canopy/grants.git", "upload").await?;
    let upload_auth = authorization(&upload)?;
    let batch = || {
        client
            .post(format!("{endpoint}/objects/batch"))
            .header("Authorization", upload_auth)
            .json(&json!({"operation":"upload","objects":[]}))
    };
    status(batch(), StatusCode::OK).await?;
    status(
        client
            .post(format!("{endpoint}/objects/batch"))
            .header("Authorization", upload_auth)
            .json(&json!({"operation":"download","objects":[]})),
        StatusCode::FORBIDDEN,
    )
    .await?;
    status(
        client
            .get(format!("{endpoint}/objects/{}", "01".repeat(32)))
            .header("Authorization", upload_auth),
        StatusCode::UNAUTHORIZED,
    )
    .await?;
    client
        .put(&membership)
        .bearer_auth(AUTH)
        .json(&json!({"role":"read"}))
        .send()
        .await?
        .error_for_status()?;
    status(batch(), StatusCode::FORBIDDEN).await?;
    client
        .put(&membership)
        .bearer_auth(AUTH)
        .json(&json!({"role":"write"}))
        .send()
        .await?
        .error_for_status()?;
    status(batch(), StatusCode::OK).await?;
    client
        .delete(format!("{base}/api/accounts/member/ssh-keys/{id}"))
        .bearer_auth(AUTH)
        .send()
        .await?
        .error_for_status()?;
    status(batch(), StatusCode::UNAUTHORIZED).await?;
    assert_eq!(
        exec(&member, "git-lfs-authenticate canopy/grants.git upload")
            .await?
            .0,
        1
    );
    member
        .disconnect(russh::Disconnect::ByApplication, "", "")
        .await?;

    let disabled_key = super::key(workspace.path(), "disabled").await?;
    register(address, "member", &disabled_key, "write").await?;
    let disabled = connect(ssh_address, &host, &disabled_key, "git", true).await?;
    let revoked = grant(&disabled, "canopy/grants.git", "download").await?;
    client
        .post(format!("{base}/api/accounts/member/disable"))
        .bearer_auth(AUTH)
        .send()
        .await?
        .error_for_status()?;
    status(
        client
            .get(format!("{endpoint}/locks"))
            .header("Authorization", authorization(&revoked)?),
        StatusCode::UNAUTHORIZED,
    )
    .await?;
    disabled
        .disconnect(russh::Disconnect::ByApplication, "", "")
        .await?;
    reader
        .disconnect(russh::Disconnect::ByApplication, "", "")
        .await?;

    let repository: Value = client
        .get(format!("{base}/api/repositories/grants"))
        .bearer_auth(AUTH)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    client
        .patch(format!("{base}/api/repositories/grants"))
        .bearer_auth(AUTH)
        .json(&json!({"repository_id":repository["repository_id"],"name":"renamed"}))
        .send()
        .await?
        .error_for_status()?;
    create_repository(address, "grants").await?;
    status(
        client
            .get(format!("{endpoint}/locks"))
            .header("Authorization", auth),
        StatusCode::UNAUTHORIZED,
    )
    .await?;
    let endpoint = format!("{base}/canopy/renamed.git/info/lfs");
    status(
        client
            .get(format!("{endpoint}/locks"))
            .header("Authorization", auth),
        StatusCode::OK,
    )
    .await?;

    // Use the real owner clock and production lifetime: no alternate test TTL
    // or direct SQLite edit can conceal an expiry fence missing from HTTP auth.
    tokio::time::sleep_until(expiry).await;
    status(
        client
            .get(format!("{endpoint}/locks"))
            .header("Authorization", auth),
        StatusCode::UNAUTHORIZED,
    )
    .await?;
    let reader = connect(ssh_address, &host, &key, "git", true).await?;
    let fresh = grant(&reader, "canopy/renamed.git", "download").await?;
    status(
        client
            .get(format!("{endpoint}/locks"))
            .header("Authorization", authorization(&fresh)?),
        StatusCode::OK,
    )
    .await?;
    reader
        .disconnect(russh::Disconnect::ByApplication, "", "")
        .await?;
    server.shutdown().await?;
    Ok(())
}
