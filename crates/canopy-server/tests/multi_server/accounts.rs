use super::*;
use reqwest::{Client, StatusCode};
use serde_json::json;

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;
const OWNER: &str = "local-test-token";

async fn status(request: reqwest::RequestBuilder, expected: StatusCode) -> Result {
    let response = request.send().await?;
    assert_eq!(response.status(), expected, "{}", response.text().await?);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn disabled_account_loses_all_credentials_and_stays_disabled_after_restore() -> Result {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let workspace = tempfile::TempDir::new()?;
    let address = available_address().await?;
    let server = CanopyServer::start(
        config(address, workspace.path().join("first")),
        Arc::clone(&store),
    )
    .await?;
    let client = Client::new();
    let base = format!("http://{address}");
    let repo = create_repository(address, "accounts").await?;
    let account_token = format!("cnp_{}", hex::encode([71; 32]));
    let second_token = format!("cnp_{}", hex::encode([72; 32]));
    let delayed_token = format!("cnp_{}", hex::encode([73; 32]));
    let account = json!({"name":"member", "token":account_token, "scope":"admin"});
    status(
        client
            .post(format!("{base}/api/accounts"))
            .bearer_auth(OWNER)
            .json(&account),
        StatusCode::OK,
    )
    .await?;
    status(
        client
            .put(format!(
                "{base}/api/repositories/accounts/collaborators/member"
            ))
            .bearer_auth(OWNER)
            .json(&json!({"role":"read"})),
        StatusCode::OK,
    )
    .await?;
    status(
        client
            .post(format!("{base}/api/accounts/member/tokens"))
            .bearer_auth(&account_token)
            .json(&json!({"id":uuid::Uuid::new_v4().to_string(), "token":second_token, "scope":"read"})),
        StatusCode::NO_CONTENT,
    )
    .await?;

    let disable = format!("{base}/api/accounts/member/disable");
    status(client.post(&disable), StatusCode::UNAUTHORIZED).await?;
    for token in [&account_token, &second_token] {
        status(
            client.post(&disable).bearer_auth(token),
            StatusCode::FORBIDDEN,
        )
        .await?;
    }
    status(
        client
            .post(format!("{base}/api/accounts/canopy/disable"))
            .bearer_auth(OWNER),
        StatusCode::CONFLICT,
    )
    .await?;
    status(
        client
            .post(format!("{base}/api/accounts/missing/disable"))
            .bearer_auth(OWNER),
        StatusCode::NOT_FOUND,
    )
    .await?;

    let source = workspace.path().join("source");
    run_git(None, &["init", "-b", "main", path_str(&source)?]).await?;
    run_git(Some(&source), &["config", "user.name", "Canopy Test"]).await?;
    run_git(
        Some(&source),
        &["config", "user.email", "canopy@example.invalid"],
    )
    .await?;
    std::fs::write(
        source.join("README.md"),
        b"Preserved after account disable\n",
    )?;
    run_git(Some(&source), &["add", "."]).await?;
    run_git(Some(&source), &["commit", "-m", "Account fixture"]).await?;
    run_git(
        Some(&source),
        &[
            "-c",
            "http.extraHeader=Authorization: Bearer local-test-token",
            "push",
            &repo,
            "main",
        ],
    )
    .await?;
    let header = format!("http.extraHeader=Authorization: Bearer {second_token}");
    run_git(
        None,
        &[
            "-c",
            &header,
            "clone",
            &repo,
            path_str(&workspace.path().join("before"))?,
        ],
    )
    .await?;

    let details: serde_json::Value = client
        .get(format!("{base}/api/repositories/accounts"))
        .bearer_auth(&account_token)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let created: serde_json::Value = client.post(format!("{base}/api/repositories/accounts/issues"))
        .bearer_auth(&account_token)
        .json(&json!({"repository_id": details["repository_id"], "id": uuid::Uuid::new_v4().to_string(), "title": "Keep attribution", "body": "Written before disablement"}))
        .send().await?.error_for_status()?.json().await?;
    let number = created["number"].as_i64().ok_or("missing issue number")?;
    let issue: serde_json::Value = client
        .get(format!("{base}/api/repositories/accounts/issues/{number}"))
        .bearer_auth(&account_token)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;

    // Body admission proves the member authenticated before the disable. The
    // Directory transaction must still deny issuance after account state changes.
    let body = serde_json::to_vec(
        &json!({"id":uuid::Uuid::new_v4().to_string(), "token":delayed_token, "scope":"admin"}),
    )?;
    let upload = tokens::paused_upload(
        address,
        "/api/accounts/member/tokens",
        &account_token,
        body.len(),
    )
    .await?;
    for _ in 0..2 {
        status(
            client.post(&disable).bearer_auth(OWNER),
            StatusCode::NO_CONTENT,
        )
        .await?;
    }
    tokens::finish_upload(upload, &body, 404).await?;
    denied(
        &client,
        &base,
        &[&account_token, &second_token, &delayed_token],
    )
    .await?;
    status(
        client
            .post(format!("{base}/api/accounts"))
            .bearer_auth(OWNER)
            .json(&account),
        StatusCode::CONFLICT,
    )
    .await?;
    status(
        client
            .put(format!(
                "{base}/api/repositories/accounts/collaborators/member"
            ))
            .bearer_auth(OWNER)
            .json(&json!({"role":"write"})),
        StatusCode::NOT_FOUND,
    )
    .await?;
    server.shutdown().await?;

    let address = available_address().await?;
    let restored =
        CanopyServer::start(config(address, workspace.path().join("restored")), store).await?;
    let base = format!("http://{address}");
    let recovered_issue: serde_json::Value = client
        .get(format!("{base}/api/repositories/accounts/issues/{number}"))
        .bearer_auth(OWNER)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(recovered_issue, issue);
    denied(
        &client,
        &base,
        &[&account_token, &second_token, &delayed_token],
    )
    .await?;
    status(
        client
            .post(format!("{base}/api/accounts/member/disable"))
            .bearer_auth(OWNER),
        StatusCode::NO_CONTENT,
    )
    .await?;
    status(
        client
            .post(format!("{base}/api/accounts"))
            .bearer_auth(OWNER)
            .json(&account),
        StatusCode::CONFLICT,
    )
    .await?;
    let clone = workspace.path().join("owner-restored");
    let repo = format!("{base}/canopy/accounts.git");
    run_git(
        None,
        &[
            "-c",
            "http.extraHeader=Authorization: Bearer local-test-token",
            "clone",
            &repo,
            path_str(&clone)?,
        ],
    )
    .await?;
    assert_eq!(
        std::fs::read(clone.join("README.md"))?,
        b"Preserved after account disable\n"
    );
    run_git(Some(&clone), &["fsck", "--strict", "--full"]).await?;
    restored.shutdown().await?;
    Ok(())
}

async fn denied(client: &Client, base: &str, tokens: &[&str]) -> Result {
    for token in tokens {
        for path in [
            "/api/repositories",
            "/api/accounts/member/tokens",
            "/canopy/accounts.git/info/refs?service=git-upload-pack",
            "/canopy/accounts.git/info/refs?service=git-receive-pack",
        ] {
            status(
                client.get(format!("{base}{path}")).bearer_auth(token),
                StatusCode::UNAUTHORIZED,
            )
            .await?;
        }
        for path in [
            "git-upload-pack",
            "git-receive-pack",
            "info/lfs/objects/batch",
        ] {
            status(
                client
                    .post(format!("{base}/canopy/accounts.git/{path}"))
                    .bearer_auth(token)
                    .json(&json!({"operation":"download", "objects":[]})),
                StatusCode::UNAUTHORIZED,
            )
            .await?;
        }
        let oid = "0".repeat(64);
        let object = format!("{base}/canopy/accounts.git/info/lfs/objects/{oid}");
        status(
            client.get(&object).bearer_auth(token),
            StatusCode::UNAUTHORIZED,
        )
        .await?;
        status(
            client.put(&object).bearer_auth(token).body("denied"),
            StatusCode::UNAUTHORIZED,
        )
        .await?;
    }
    Ok(())
}
