use super::*;
use reqwest::{Client, StatusCode};
use serde_json::{Value, json};

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;
const OWNER: &str = "local-test-token";
const AUTH: &str = "http.extraHeader=Authorization: Bearer local-test-token";

async fn response(request: reqwest::RequestBuilder, expected: StatusCode) -> Result<Value> {
    let response = request.send().await?;
    let status = response.status();
    let bytes = response.bytes().await?;
    assert_eq!(status, expected, "{}", String::from_utf8_lossy(&bytes));
    if bytes.is_empty() {
        return Ok(Value::Null);
    }
    Ok(serde_json::from_slice(&bytes)?)
}

async fn collaborator(client: &Client, address: std::net::SocketAddr) -> Result<String> {
    let token = format!("cnp_{}", "dc".repeat(32));
    response(
        client
            .post(format!("http://{address}/api/accounts"))
            .bearer_auth(OWNER)
            .json(&json!({"name":"writer","token":token,"scope":"write"})),
        StatusCode::OK,
    )
    .await?;
    response(
        client
            .put(format!(
                "http://{address}/api/repositories/locks/collaborators/writer"
            ))
            .bearer_auth(OWNER)
            .json(&json!({"role":"write"})),
        StatusCode::OK,
    )
    .await?;
    Ok(token)
}

#[tokio::test(flavor = "multi_thread")]
async fn lock_api_is_exclusive_paginated_authorized_and_durable() -> Result {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let workspace = tempfile::TempDir::new()?;
    let address = available_address().await?;
    let server = CanopyServer::start(
        config(address, workspace.path().join("first")),
        Arc::clone(&store),
    )
    .await?;
    let url = create_repository(address, "locks").await?;
    let other = create_repository(address, "other-locks").await?;
    let locks = format!("{url}/info/lfs/locks");
    let client = Client::new();
    let writer = collaborator(&client, address).await?;
    let limited = format!("cnp_{}", "de".repeat(32));
    response(
        client
            .post(format!("http://{address}/api/accounts/canopy/tokens"))
            .bearer_auth(OWNER)
            .json(&json!({"id":uuid::Uuid::new_v4().to_string(),"token":limited,"scope":"read"})),
        StatusCode::NO_CONTENT,
    )
    .await?;
    let first = client
        .post(&locks)
        .bearer_auth(OWNER)
        .json(&json!({"path":"資料/asset.lfs","ref":{"name":"refs/heads/main"}}));
    let second = client
        .post(&locks)
        .bearer_auth(&writer)
        .json(&json!({"path":"資料/asset.lfs","ref":{"name":"refs/heads/other"}}));
    let (first, second) = tokio::try_join!(first.send(), second.send())?;
    let (accepted, rejected) = if first.status() == StatusCode::CREATED {
        (first, second)
    } else {
        (second, first)
    };
    assert_eq!(accepted.status(), StatusCode::CREATED);
    assert_eq!(rejected.status(), StatusCode::CONFLICT);
    assert_eq!(
        accepted.headers()["content-type"],
        "application/vnd.git-lfs+json"
    );
    let lock = accepted.json::<Value>().await?["lock"].clone();
    assert_eq!(rejected.json::<Value>().await?["lock"], lock);
    let timestamp = lock["locked_at"].as_str().ok_or("timestamp missing")?;
    assert_eq!(timestamp.len(), 20);
    assert!(timestamp.ends_with('Z'));
    let id = lock["id"].as_str().ok_or("id missing")?;
    let owning_token = if lock["owner"]["name"] == "canopy" {
        OWNER
    } else {
        &writer
    };
    let foreign_token = if owning_token == OWNER {
        &writer
    } else {
        OWNER
    };
    response(
        client
            .post(format!("{locks}/{id}/unlock"))
            .bearer_auth(foreign_token)
            .json(&json!({})),
        StatusCode::FORBIDDEN,
    )
    .await?;
    let independent = response(
        client
            .post(format!("{other}/info/lfs/locks"))
            .bearer_auth(OWNER)
            .json(&json!({"path":"資料/asset.lfs"})),
        StatusCode::CREATED,
    )
    .await?;
    assert_ne!(independent["lock"]["id"], lock["id"]);
    let own = response(
        client
            .post(&locks)
            .bearer_auth(OWNER)
            .json(&json!({"path":"second.lfs"})),
        StatusCode::CREATED,
    )
    .await?["lock"]
        .clone();
    let theirs = response(
        client
            .post(&locks)
            .bearer_auth(&writer)
            .json(&json!({"path":"third.lfs"})),
        StatusCode::CREATED,
    )
    .await?["lock"]
        .clone();
    let first_page = response(
        client
            .get(&locks)
            .bearer_auth(&limited)
            .query(&[("limit", "1")]),
        StatusCode::OK,
    )
    .await?;
    assert_eq!(first_page["locks"], json!([lock]));
    let cursor = first_page["next_cursor"].as_str().ok_or("cursor missing")?;
    let verified = response(
        client
            .post(format!("{locks}/verify"))
            .bearer_auth(OWNER)
            .json(&json!({"cursor":cursor,"limit":2})),
        StatusCode::OK,
    )
    .await?;
    assert_eq!(verified["ours"], json!([own]));
    assert_eq!(verified["theirs"], json!([theirs]));
    assert!(verified["next_cursor"].is_null());
    let filtered = response(
        client
            .get(&locks)
            .bearer_auth(OWNER)
            .query(&[("path", "資料/asset.lfs"), ("id", id)]),
        StatusCode::OK,
    )
    .await?;
    assert_eq!(filtered["locks"], json!([lock]));
    for suffix in ["", "/verify", &format!("/{id}/unlock")] {
        response(
            client
                .post(format!("{locks}{suffix}"))
                .bearer_auth(&limited)
                .json(&json!({"path":"denied.lfs","force":true})),
            StatusCode::FORBIDDEN,
        )
        .await?;
    }
    for path in [
        "",
        "/absolute",
        "a/../b",
        "a//b",
        "./a",
        "nul\0path",
        &"x".repeat(4097),
    ] {
        response(
            client
                .post(&locks)
                .bearer_auth(OWNER)
                .json(&json!({"path":path})),
            StatusCode::UNPROCESSABLE_ENTITY,
        )
        .await?;
    }
    response(
        client
            .get(&locks)
            .bearer_auth(OWNER)
            .query(&[("cursor", "-1")]),
        StatusCode::UNPROCESSABLE_ENTITY,
    )
    .await?;
    // A role downgrade preserves the lock but removes mutation/verification authority.
    let member = format!("http://{address}/api/repositories/locks/collaborators/writer");
    response(
        client
            .put(&member)
            .bearer_auth(OWNER)
            .json(&json!({"role":"read"})),
        StatusCode::OK,
    )
    .await?;
    let third_id = theirs["id"].as_str().ok_or("id missing")?;
    for suffix in ["", "/verify", &format!("/{third_id}/unlock")] {
        response(
            client
                .post(format!("{locks}{suffix}"))
                .bearer_auth(&writer)
                .json(&json!({"path":"denied.lfs","force":true})),
            StatusCode::FORBIDDEN,
        )
        .await?;
    }
    let before = response(client.get(&locks).bearer_auth(OWNER), StatusCode::OK).await?;
    server.shutdown().await?;
    let address = available_address().await?;
    let restored =
        CanopyServer::start(config(address, workspace.path().join("restored")), store).await?;
    let locks = format!("http://{address}/canopy/locks.git/info/lfs/locks");
    assert_eq!(
        response(client.get(&locks).bearer_auth(OWNER), StatusCode::OK).await?,
        before
    );
    // Force is permitted to any current writer, including deleting a former writer's lock.
    let unlocked = response(
        client
            .post(format!("{locks}/{third_id}/unlock"))
            .bearer_auth(OWNER)
            .json(&json!({"force":true})),
        StatusCode::OK,
    )
    .await?;
    assert_eq!(unlocked["lock"], theirs);
    response(
        client
            .post(format!("{locks}/{third_id}/unlock"))
            .bearer_auth(OWNER)
            .json(&json!({"force":true})),
        StatusCode::NOT_FOUND,
    )
    .await?;
    let visibility = format!("http://{address}/api/repositories/locks/visibility");
    let current = response(client.get(&visibility).bearer_auth(OWNER), StatusCode::OK).await?;
    response(client.put(&visibility).bearer_auth(OWNER).json(&json!({"repository_id":current["repository_id"],"expected_generation":current["generation"],"visibility":"public"})), StatusCode::OK).await?;
    let public = response(client.get(&locks), StatusCode::OK).await?;
    assert_eq!(public["locks"], json!([lock, own]));
    // Anonymous readers can inspect public locks but cannot verify or mutate them.
    let denied = client
        .post(format!("{locks}/verify"))
        .json(&json!({}))
        .send()
        .await?;
    assert_eq!(denied.status(), StatusCode::UNAUTHORIZED);
    restored.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn stock_lfs_locks_block_conflicting_pushes_and_unlock_allows_retry() -> Result {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let workspace = tempfile::TempDir::new()?;
    let address = available_address().await?;
    let server =
        CanopyServer::start(config(address, workspace.path().join("server")), store).await?;
    let url = create_repository(address, "locks").await?;
    let client = Client::new();
    let token = collaborator(&client, address).await?;
    let auth = format!("http.extraHeader=Authorization: Bearer {token}");
    let source = workspace.path().join("source");
    run_git(None, &["init", "-b", "main", path_str(&source)?]).await?;
    run_git(Some(&source), &["config", "user.name", "Canopy Test"]).await?;
    run_git(
        Some(&source),
        &["config", "user.email", "test@example.invalid"],
    )
    .await?;
    run_git(Some(&source), &["config", "commit.gpgsign", "false"]).await?;
    run_git(Some(&source), &["remote", "add", "origin", &url]).await?;
    run_git(Some(&source), &["lfs", "install", "--local"]).await?;
    run_git(Some(&source), &["lfs", "track", "--lockable", "*.lfs"]).await?;
    tokio::fs::write(source.join("asset.lfs"), b"original\n").await?;
    run_git(Some(&source), &["add", "."]).await?;
    run_git(Some(&source), &["commit", "-m", "Initial"]).await?;
    run_git(Some(&source), &["-c", AUTH, "push", "-u", "origin", "main"]).await?;
    let old = run_git(Some(&source), &["rev-parse", "HEAD"]).await?;
    let created = run_git(
        Some(&source),
        &["-c", AUTH, "lfs", "lock", "--json", "asset.lfs"],
    )
    .await?;
    let created: Value = serde_json::from_slice(&created)?;
    let lock_id = created[0]["id"].as_str().ok_or("stock lock ID missing")?;
    assert_eq!(created[0]["path"], "asset.lfs");
    let listed = run_git(Some(&source), &["-c", &auth, "lfs", "locks", "--json"]).await?;
    assert!(String::from_utf8(listed)?.contains("asset.lfs"));
    // Verification is client-enforced. Stock pre-push must fail before publishing refs.
    run_git(Some(&source), &["config", "lfs.locksverify", "true"]).await?;
    tokio::fs::write(source.join("asset.lfs"), b"changed\n").await?;
    run_git(Some(&source), &["add", "asset.lfs"]).await?;
    run_git(Some(&source), &["commit", "-m", "Change locked asset"]).await?;
    let push = tokio::process::Command::new("git")
        .current_dir(&source)
        .args(["-c", &auth, "push", "origin", "main"])
        .env("GIT_TERMINAL_PROMPT", "0")
        .output()
        .await?;
    assert!(!push.status.success(), "conflicting LFS push succeeded");
    let message = format!(
        "{}{}",
        String::from_utf8_lossy(&push.stdout),
        String::from_utf8_lossy(&push.stderr)
    );
    assert!(
        message.contains("asset.lfs") && message.contains("canopy"),
        "{message}"
    );
    let remote = run_git(
        Some(&source),
        &["-c", AUTH, "ls-remote", "origin", "refs/heads/main"],
    )
    .await?;
    assert!(remote.starts_with(String::from_utf8(old)?.trim().as_bytes()));
    let unlocked = run_git(
        Some(&source),
        &[
            "-c", &auth, "lfs", "unlock", "--force", "--json", "--id", lock_id,
        ],
    )
    .await?;
    let unlocked: Value = serde_json::from_slice(&unlocked)?;
    assert_eq!(unlocked, json!([{"id":lock_id,"unlocked":true}]));
    run_git(Some(&source), &["-c", &auth, "push", "origin", "main"]).await?;
    let clone = workspace.path().join("clone");
    run_git(None, &["-c", AUTH, "clone", &url, path_str(&clone)?]).await?;
    run_git(Some(&clone), &["lfs", "install", "--local"]).await?;
    run_git(Some(&clone), &["-c", AUTH, "lfs", "pull"]).await?;
    assert_eq!(
        tokio::fs::read(clone.join("asset.lfs")).await?,
        b"changed\n"
    );
    run_git(Some(&clone), &["fsck", "--strict", "--full"]).await?;
    run_git(Some(&clone), &["-c", AUTH, "lfs", "lock", "asset.lfs"]).await?;
    run_git(Some(&clone), &["-c", AUTH, "lfs", "unlock", "asset.lfs"]).await?;
    server.shutdown().await?;
    Ok(())
}
