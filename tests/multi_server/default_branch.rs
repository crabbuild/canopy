use super::*;
use reqwest::StatusCode;
use serde_json::{Value, json};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
const AUTH: &str = "http.extraHeader=Authorization: Bearer local-test-token";

async fn read(client: &reqwest::Client, api: &str) -> Result<Value> {
    Ok(client
        .get(api)
        .bearer_auth("local-test-token")
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?)
}

fn update(head: &Value, reference: &str) -> Value {
    json!({"repository_id": head["repository_id"], "reference": reference, "expected_generation": head["generation"]})
}

async fn put(
    client: &reqwest::Client,
    api: &str,
    input: &Value,
    expected: StatusCode,
) -> Result<()> {
    let response = client
        .put(api)
        .bearer_auth("local-test-token")
        .json(input)
        .send()
        .await?;
    assert_eq!(response.status(), expected, "{}", response.text().await?);
    Ok(())
}

async fn clone_branch(
    url: &str,
    path: &Path,
    protocol: &str,
    branch: &str,
    content: Option<&[u8]>,
) -> Result<()> {
    run_git(
        None,
        &[
            "-c",
            AUTH,
            "-c",
            &format!("protocol.version={protocol}"),
            "clone",
            url,
            path_str(path)?,
        ],
    )
    .await?;
    assert_eq!(
        run_git(Some(path), &["symbolic-ref", "HEAD"]).await?,
        format!("refs/heads/{branch}\n").as_bytes()
    );
    if let Some(content) = content {
        assert_eq!(tokio::fs::read(path.join("README.md")).await?, content);
        run_git(Some(path), &["fsck", "--strict", "--full"]).await?;
    }
    Ok(())
}

async fn discovery(url: &str, protocol: &str, branch: &str) -> Result<()> {
    let refs = run_git(
        None,
        &[
            "-c",
            AUTH,
            "-c",
            &format!("protocol.version={protocol}"),
            "ls-remote",
            "--symref",
            url,
            "HEAD",
        ],
    )
    .await?;
    assert!(
        refs.starts_with(format!("ref: refs/heads/{branch}\tHEAD\n").as_bytes()),
        "{}",
        String::from_utf8_lossy(&refs)
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn default_branch_controls_discovery_and_clone_after_fresh_owner_restore() -> Result<()> {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let workspace = tempfile::TempDir::new()?;
    let address = available_address().await?;
    let server = CanopyServer::start(
        config(address, workspace.path().join("first")),
        Arc::clone(&store),
    )
    .await?;
    let url = create_repository(address, "branches").await?;
    let api = format!("http://{address}/api/repositories/branches/default-branch");
    let client = reqwest::Client::new();
    assert_eq!(
        client.get(&api).send().await?.status(),
        StatusCode::UNAUTHORIZED
    );
    let initial = read(&client, &api).await?;
    assert_eq!(initial["reference"], "refs/heads/main");
    put(
        &client,
        &api,
        &update(&initial, "refs/heads/trunk"),
        StatusCode::OK,
    )
    .await?;
    clone_branch(&url, &workspace.path().join("unborn"), "2", "trunk", None).await?;
    // Protocol v2 explicitly carries the unborn target even before any refs exist.
    let unborn = b"0014command=ls-refs\n0001000csymrefs\n000bunborn\n0000";
    let response = client
        .post(format!("{url}/git-upload-pack"))
        .bearer_auth("local-test-token")
        .header("Git-Protocol", "version=2")
        .header("Content-Type", "application/x-git-upload-pack-request")
        .body(unborn.to_vec())
        .send()
        .await?
        .error_for_status()?
        .bytes()
        .await?;
    assert!(
        String::from_utf8_lossy(&response).contains("unborn HEAD symref-target:refs/heads/trunk")
    );
    let selected = read(&client, &api).await?;
    put(
        &client,
        &api,
        &update(&selected, "refs/heads/main"),
        StatusCode::OK,
    )
    .await?;
    for reference in [
        "HEAD",
        "refs/tags/main",
        "refs/heads/a\nref: refs/heads/b",
        "refs/heads/a.lock",
    ] {
        put(
            &client,
            &api,
            &update(&selected, reference),
            StatusCode::UNPROCESSABLE_ENTITY,
        )
        .await?;
    }
    let mut wrong_id = update(&selected, "refs/heads/main");
    wrong_id["repository_id"] = json!(uuid::Uuid::new_v4().to_string());
    put(&client, &api, &wrong_id, StatusCode::CONFLICT).await?;

    let local = workspace.path().join("source");
    run_git(None, &["init", "-b", "main", path_str(&local)?]).await?;
    run_git(Some(&local), &["config", "user.name", "Canopy Test"]).await?;
    run_git(
        Some(&local),
        &["config", "user.email", "canopy@example.invalid"],
    )
    .await?;
    tokio::fs::write(local.join("README.md"), b"main branch\n").await?;
    run_git(Some(&local), &["add", "README.md"]).await?;
    run_git(Some(&local), &["commit", "-m", "Main"]).await?;
    run_git(Some(&local), &["checkout", "-b", "trunk"]).await?;
    tokio::fs::write(local.join("README.md"), b"trunk branch\n").await?;
    run_git(Some(&local), &["commit", "-am", "Trunk"]).await?;
    run_git(Some(&local), &["-c", AUTH, "push", &url, "main", "trunk"]).await?;
    for protocol in ["0", "2"] {
        discovery(&url, protocol, "main").await?;
    }
    let before = read(&client, &api).await?;
    put(
        &client,
        &api,
        &update(&before, "refs/heads/absent"),
        StatusCode::CONFLICT,
    )
    .await?;
    put(
        &client,
        &api,
        &update(&before, "refs/heads/trunk"),
        StatusCode::OK,
    )
    .await?;
    put(
        &client,
        &api,
        &update(&before, "refs/heads/main"),
        StatusCode::CONFLICT,
    )
    .await?;
    for protocol in ["0", "2"] {
        discovery(&url, protocol, "trunk").await?;
        clone_branch(
            &url,
            &workspace.path().join(format!("live-{protocol}")),
            protocol,
            "trunk",
            Some(b"trunk branch\n"),
        )
        .await?;
    }
    // Admin token scope alone grants no repository ownership. Read access still
    // permits discovering HEAD, and revocation hides the metadata again.
    let token = format!("cnp_{}", "ad".repeat(32));
    client
        .post(format!("http://{address}/api/accounts"))
        .bearer_auth("local-test-token")
        .json(&json!({"name": "collaborator", "token": token, "scope": "admin"}))
        .send()
        .await?
        .error_for_status()?;
    assert_eq!(
        client.get(&api).bearer_auth(&token).send().await?.status(),
        StatusCode::NOT_FOUND
    );
    let member = format!("http://{address}/api/repositories/branches/collaborators/collaborator");
    client
        .put(&member)
        .bearer_auth("local-test-token")
        .json(&json!({"role": "write"}))
        .send()
        .await?
        .error_for_status()?;
    assert_eq!(
        client.get(&api).bearer_auth(&token).send().await?.status(),
        StatusCode::OK
    );
    let selected = read(&client, &api).await?;
    assert_eq!(
        client
            .put(&api)
            .bearer_auth(&token)
            .json(&update(&selected, "refs/heads/main"))
            .send()
            .await?
            .status(),
        StatusCode::FORBIDDEN
    );
    client
        .delete(&member)
        .bearer_auth("local-test-token")
        .send()
        .await?
        .error_for_status()?;
    assert_eq!(
        client.get(&api).bearer_auth(&token).send().await?.status(),
        StatusCode::NOT_FOUND
    );

    // Deleting the selected branch preserves its symbolic target. Recreating
    // that branch restores clone behavior without another administrative write.
    run_git(
        Some(&local),
        &["-c", AUTH, "push", &url, ":refs/heads/trunk"],
    )
    .await?;
    assert_eq!(read(&client, &api).await?["reference"], "refs/heads/trunk");
    let deleted = read(&client, &api).await?;
    put(
        &client,
        &api,
        &update(&deleted, "refs/heads/trunk"),
        StatusCode::CONFLICT,
    )
    .await?;
    run_git(Some(&local), &["-c", AUTH, "push", &url, "trunk"]).await?;
    let final_state = read(&client, &api).await?;
    server.shutdown().await?;
    let address = available_address().await?;
    let restored =
        CanopyServer::start(config(address, workspace.path().join("restored")), store).await?;
    let url = format!("http://{address}/canopy/branches.git");
    let api = format!("http://{address}/api/repositories/branches/default-branch");
    assert_eq!(read(&client, &api).await?, final_state);
    for protocol in ["0", "2"] {
        discovery(&url, protocol, "trunk").await?;
        clone_branch(
            &url,
            &workspace.path().join(format!("restored-{protocol}")),
            protocol,
            "trunk",
            Some(b"trunk branch\n"),
        )
        .await?;
    }
    restored.shutdown().await?;
    Ok(())
}
