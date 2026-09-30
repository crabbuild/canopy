use super::tokens::{finish_upload, paused_upload};
use super::*;
use reqwest::{Client, StatusCode};
use serde_json::{Value, json};

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;
const OWNER: &str = "local-test-token";

async fn status(request: reqwest::RequestBuilder, expected: StatusCode) -> Result {
    let response = request.send().await?;
    assert_eq!(response.status(), expected, "{}", response.text().await?);
    Ok(())
}
async fn value(request: reqwest::RequestBuilder) -> Result<Value> {
    Ok(request.send().await?.error_for_status()?.json().await?)
}

#[tokio::test(flavor = "multi_thread")]
async fn commit_checks_bind_reporters_versions_and_reruns_across_recovery() -> Result {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let workspace = tempfile::TempDir::new()?;
    let address = available_address().await?;
    let server = CanopyServer::start(
        config(address, workspace.path().join("first")),
        Arc::clone(&store),
    )
    .await?;
    let git_url = create_repository(address, "team").await?;
    create_repository(address, "other").await?;
    let client = Client::new();
    let base = format!("http://{address}");
    let repo = format!("{base}/api/repositories/team");
    let repository = value(client.get(&repo).bearer_auth(OWNER)).await?["repository_id"].clone();
    let contexts = format!("{repo}/check-contexts");
    let context_api = format!("{contexts}/unit-tests");
    let mut tokens = Vec::new();
    for (index, (account, scope, role)) in [
        ("ci", "write", Some("read")),
        ("developer", "write", Some("write")),
        ("viewer", "read", Some("read")),
        ("outsider", "admin", None),
    ]
    .into_iter()
    .enumerate()
    {
        let token = format!("cnp_{index:064x}");
        status(
            client
                .post(format!("{base}/api/accounts"))
                .bearer_auth(OWNER)
                .json(&json!({"name":account, "scope":scope, "token":token})),
            StatusCode::OK,
        )
        .await?;
        if let Some(role) = role {
            status(
                client
                    .put(format!("{repo}/collaborators/{account}"))
                    .bearer_auth(OWNER)
                    .json(&json!({"role":role})),
                StatusCode::OK,
            )
            .await?;
        }
        tokens.push(token);
    }
    let [ci, developer, viewer, outsider] = tokens.as_slice() else {
        return Err("missing credentials".into());
    };
    let local = workspace.path().join("local");
    run_git(None, &["init", "-b", "main", path_str(&local)?]).await?;
    run_git(Some(&local), &["config", "user.name", "Canopy Test"]).await?;
    run_git(
        Some(&local),
        &["config", "user.email", "test@example.invalid"],
    )
    .await?;
    run_git(Some(&local), &["commit", "--allow-empty", "-m", "First"]).await?;
    let oid = String::from_utf8(run_git(Some(&local), &["rev-parse", "HEAD"]).await?)?
        .trim()
        .to_owned();
    run_git(Some(&local), &["commit", "--allow-empty", "-m", "Second"]).await?;
    let other_oid = String::from_utf8(run_git(Some(&local), &["rev-parse", "HEAD"]).await?)?
        .trim()
        .to_owned();
    let tree_oid = String::from_utf8(run_git(Some(&local), &["rev-parse", "HEAD^{tree}"]).await?)?
        .trim()
        .to_owned();
    run_git(
        Some(&local),
        &[
            "-c",
            "http.extraHeader=Authorization: Bearer local-test-token",
            "push",
            &git_url,
            "HEAD:refs/heads/main",
        ],
    )
    .await?;
    let commit_api = format!("{repo}/commits/{oid}/checks");
    let mut policy =
        json!({"repository_id":repository, "expected_version":0, "reporter":"ci", "enabled":true});
    status(client.get(&contexts), StatusCode::UNAUTHORIZED).await?;
    status(
        client.get(&contexts).bearer_auth(outsider),
        StatusCode::NOT_FOUND,
    )
    .await?;
    status(
        client
            .put(&context_api)
            .bearer_auth(developer)
            .json(&policy),
        StatusCode::FORBIDDEN,
    )
    .await?;
    let mut invalid = policy.clone();
    invalid["reporter"] = json!("outsider");
    status(
        client.put(&context_api).bearer_auth(OWNER).json(&invalid),
        StatusCode::NOT_FOUND,
    )
    .await?;
    status(
        client.put(&context_api).bearer_auth(OWNER).json(&policy),
        StatusCode::NO_CONTENT,
    )
    .await?;
    status(
        client.put(&context_api).bearer_auth(OWNER).json(&policy),
        StatusCode::CONFLICT,
    )
    .await?;
    let pending = value(client.get(&commit_api).bearer_auth(viewer)).await?;
    assert_eq!(pending["checks"][0]["context"]["reporter"], "ci");
    assert!(pending["checks"][0]["run"].is_null());
    let first_id = uuid::Uuid::new_v4().to_string();
    let first = json!({"repository_id":repository, "id":first_id, "context":"unit-tests", "context_version":1});
    status(
        client.post(&commit_api).bearer_auth(developer).json(&first),
        StatusCode::FORBIDDEN,
    )
    .await?;
    status(
        client.post(&commit_api).bearer_auth(OWNER).json(&first),
        StatusCode::FORBIDDEN,
    )
    .await?;
    status(
        client.post(&commit_api).bearer_auth(viewer).json(&first),
        StatusCode::FORBIDDEN,
    )
    .await?;
    let (start, retry) = tokio::join!(
        value(client.post(&commit_api).bearer_auth(ci).json(&first)),
        value(client.post(&commit_api).bearer_auth(ci).json(&first))
    );
    assert_eq!(start?, json!({"id":first_id}));
    assert_eq!(retry?, json!({"id":first_id}));
    status(
        client
            .post(format!("{repo}/commits/{other_oid}/checks"))
            .bearer_auth(ci)
            .json(&first),
        StatusCode::CONFLICT,
    )
    .await?;
    let first_api = format!("{repo}/checks/{first_id}");
    let mut report = json!({"repository_id":repository, "expected_version":1, "state":"in_progress", "summary":"Running"});
    status(
        client.put(&first_api).bearer_auth(developer).json(&report),
        StatusCode::FORBIDDEN,
    )
    .await?;
    status(
        client.put(&first_api).bearer_auth(ci).json(&report),
        StatusCode::NO_CONTENT,
    )
    .await?;
    let second_id = uuid::Uuid::new_v4().to_string();
    let mut second = first.clone();
    second["id"] = json!(second_id);
    assert_eq!(
        value(client.post(&commit_api).bearer_auth(ci).json(&second)).await?,
        json!({"id":second_id})
    );
    report["expected_version"] = json!(2);
    report["state"] = json!("success");
    status(
        client.put(&first_api).bearer_auth(ci).json(&report),
        StatusCode::NO_CONTENT,
    )
    .await?;
    assert_eq!(
        value(client.post(&commit_api).bearer_auth(ci).json(&first)).await?,
        json!({"id":first_id})
    );
    let newest = value(client.get(&commit_api).bearer_auth(viewer)).await?;
    assert_eq!(newest["checks"][0]["run"]["id"], second_id);
    assert_eq!(newest["checks"][0]["run"]["state"], "queued");
    assert_eq!(
        value(client.get(&first_api).bearer_auth(viewer)).await?["check"]["state"],
        "success"
    );
    report["expected_version"] = json!(3);
    report["state"] = json!("failure");
    status(
        client.put(&first_api).bearer_auth(ci).json(&report),
        StatusCode::CONFLICT,
    )
    .await?;
    let second_api = format!("{repo}/checks/{second_id}");
    report["expected_version"] = json!(1);
    let mut competing = report.clone();
    competing["state"] = json!("success");
    let (a, b) = tokio::join!(
        client.put(&second_api).bearer_auth(ci).json(&report).send(),
        client
            .put(&second_api)
            .bearer_auth(ci)
            .json(&competing)
            .send()
    );
    let mut codes = [a?.status().as_u16(), b?.status().as_u16()];
    codes.sort();
    assert_eq!(codes, [204, 409]);
    policy["expected_version"] = json!(1);
    policy["reporter"] = json!("developer");
    status(
        client.put(&context_api).bearer_auth(OWNER).json(&policy),
        StatusCode::NO_CONTENT,
    )
    .await?;
    assert!(
        value(client.get(&commit_api).bearer_auth(viewer)).await?["checks"][0]["run"].is_null()
    );
    status(
        client.post(&commit_api).bearer_auth(ci).json(&first),
        StatusCode::FORBIDDEN,
    )
    .await?;
    status(
        client.post(&commit_api).bearer_auth(developer).json(&first),
        StatusCode::CONFLICT,
    )
    .await?;
    let third_id = uuid::Uuid::new_v4().to_string();
    let third = json!({"repository_id":repository, "id":third_id, "context":"unit-tests", "context_version":2});
    assert_eq!(
        value(client.post(&commit_api).bearer_auth(developer).json(&third)).await?,
        json!({"id":third_id})
    );
    let third_api = format!("{repo}/checks/{third_id}");
    let mut invalid = report.clone();
    invalid["summary"] = json!("x".repeat(4097));
    status(
        client.put(&third_api).bearer_auth(developer).json(&invalid),
        StatusCode::UNPROCESSABLE_ENTITY,
    )
    .await?;
    invalid["summary"] = json!("fine");
    invalid["state"] = json!("queued");
    status(
        client.put(&third_api).bearer_auth(developer).json(&invalid),
        StatusCode::UNPROCESSABLE_ENTITY,
    )
    .await?;
    report["summary"] = json!("x".repeat(4096));
    report["state"] = json!("success");
    status(
        client.put(&third_api).bearer_auth(developer).json(&report),
        StatusCode::NO_CONTENT,
    )
    .await?;
    for target in [&tree_oid, &"0".repeat(40)] {
        status(
            client
                .get(format!("{repo}/commits/{target}/checks"))
                .bearer_auth(OWNER),
            StatusCode::NOT_FOUND,
        )
        .await?;
        status(
            client
                .post(format!("{repo}/commits/{target}/checks"))
                .bearer_auth(developer)
                .json(&third),
            StatusCode::NOT_FOUND,
        )
        .await?;
    }
    status(
        client
            .get(format!("{repo}/commits/invalid/checks"))
            .bearer_auth(OWNER),
        StatusCode::UNPROCESSABLE_ENTITY,
    )
    .await?;
    status(
        client
            .get(format!(
                "{base}/api/repositories/other/commits/{oid}/checks"
            ))
            .bearer_auth(OWNER),
        StatusCode::NOT_FOUND,
    )
    .await?;
    let delayed_id = uuid::Uuid::new_v4().to_string();
    let mut delayed = third.clone();
    delayed["id"] = json!(delayed_id);
    let body = serde_json::to_vec(&delayed)?;
    let upload = paused_upload(
        address,
        &format!("/api/repositories/team/commits/{oid}/checks"),
        developer,
        body.len(),
    )
    .await?;
    policy["expected_version"] = json!(2);
    policy["enabled"] = json!(false);
    status(
        client.put(&context_api).bearer_auth(OWNER).json(&policy),
        StatusCode::NO_CONTENT,
    )
    .await?;
    finish_upload(upload, &body, 409).await?;
    status(
        client
            .get(format!("{repo}/checks/{delayed_id}"))
            .bearer_auth(OWNER),
        StatusCode::NOT_FOUND,
    )
    .await?;
    assert_eq!(
        value(client.get(&commit_api).bearer_auth(viewer)).await?["checks"],
        json!([])
    );
    assert_eq!(
        value(client.get(&contexts).bearer_auth(viewer)).await?["contexts"][0]["enabled"],
        false
    );
    policy["expected_version"] = json!(3);
    policy["enabled"] = json!(true);
    policy["reporter"] = json!("ci");
    status(
        client.put(&context_api).bearer_auth(OWNER).json(&policy),
        StatusCode::NO_CONTENT,
    )
    .await?;
    let final_id = uuid::Uuid::new_v4().to_string();
    let final_start = json!({"repository_id":repository, "id":final_id, "context":"unit-tests", "context_version":4});
    assert_eq!(
        value(client.post(&commit_api).bearer_auth(ci).json(&final_start)).await?,
        json!({"id":final_id})
    );
    let final_api = format!("{repo}/checks/{final_id}");
    status(
        client
            .delete(format!("{repo}/collaborators/ci"))
            .bearer_auth(OWNER),
        StatusCode::NO_CONTENT,
    )
    .await?;
    status(
        client.put(&final_api).bearer_auth(ci).json(&report),
        StatusCode::NOT_FOUND,
    )
    .await?;
    status(
        client.post(&commit_api).bearer_auth(ci).json(&final_start),
        StatusCode::NOT_FOUND,
    )
    .await?;
    status(
        client.get(&commit_api).bearer_auth(ci),
        StatusCode::NOT_FOUND,
    )
    .await?;
    status(
        client
            .put(format!("{repo}/collaborators/ci"))
            .bearer_auth(OWNER)
            .json(&json!({"role":"read"})),
        StatusCode::OK,
    )
    .await?;
    report["summary"] = json!("All checks passed");
    status(
        client.put(&final_api).bearer_auth(ci).json(&report),
        StatusCode::NO_CONTENT,
    )
    .await?;
    for number in 0..32 {
        status(client.put(format!("{contexts}/check-{number:02}")).bearer_auth(OWNER).json(&json!({"repository_id":repository, "expected_version":0, "reporter":"ci", "enabled":true})), StatusCode::NO_CONTENT).await?;
    }
    for (api, key) in [(&contexts, "contexts"), (&commit_api, "checks")] {
        let page = value(client.get(api).bearer_auth(viewer)).await?;
        assert_eq!(page[key].as_array().ok_or("missing page")?.len(), 32);
        assert_eq!(page["next_after"], "check-31");
        let end = value(
            client
                .get(api)
                .query(&[("after", "check-31")])
                .bearer_auth(viewer),
        )
        .await?;
        assert_eq!(end[key].as_array().ok_or("missing end")?.len(), 1);
        assert!(end["next_after"].is_null());
        status(
            client.get(api).query(&[("after", "")]).bearer_auth(viewer),
            StatusCode::UNPROCESSABLE_ENTITY,
        )
        .await?;
    }
    let before = value(
        client
            .get(&commit_api)
            .query(&[("after", "check-31")])
            .bearer_auth(viewer),
    )
    .await?;
    let historical = value(client.get(&first_api).bearer_auth(viewer)).await?;
    status(
        client
            .patch(&repo)
            .bearer_auth(OWNER)
            .json(&json!({"name":"renamed", "repository_id":repository})),
        StatusCode::OK,
    )
    .await?;
    server.shutdown().await?;
    let restored_address = available_address().await?;
    let restored = CanopyServer::start(
        config(restored_address, workspace.path().join("restored")),
        Arc::clone(&store),
    )
    .await?;
    let restored_repo = format!("http://{restored_address}/api/repositories/renamed");
    let restored_commit = format!("{restored_repo}/commits/{oid}/checks");
    assert_eq!(
        value(
            client
                .get(&restored_commit)
                .query(&[("after", "check-31")])
                .bearer_auth(viewer)
        )
        .await?,
        before
    );
    assert_eq!(
        value(
            client
                .get(format!("{restored_repo}/checks/{first_id}"))
                .bearer_auth(viewer)
        )
        .await?,
        historical
    );
    assert_eq!(
        value(
            client
                .post(&restored_commit)
                .bearer_auth(ci)
                .json(&final_start)
        )
        .await?,
        json!({"id":final_id})
    );
    assert_eq!(
        value(
            client
                .get(&restored_commit)
                .query(&[("after", "check-31")])
                .bearer_auth(viewer)
        )
        .await?,
        before
    );
    let mut next = final_start.clone();
    next["id"] = json!(uuid::Uuid::new_v4().to_string());
    let created = value(client.post(&restored_commit).bearer_auth(ci).json(&next)).await?;
    assert_eq!(created["id"], next["id"]);
    let current = value(
        client
            .get(&restored_commit)
            .query(&[("after", "check-31")])
            .bearer_auth(OWNER),
    )
    .await?;
    assert_eq!(current["checks"][0]["run"]["id"], next["id"]);
    assert_eq!(current["checks"][0]["run"]["state"], "queued");
    restored.shutdown().await?;
    Ok(())
}
