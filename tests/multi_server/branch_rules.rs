use super::*;
use reqwest::{Client, StatusCode};
use serde_json::{Value, json};

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;
const OWNER: &str = "local-test-token";
const AUTH: &str = "http.extraHeader=Authorization: Bearer local-test-token";

async fn status(request: reqwest::RequestBuilder, expected: StatusCode) -> Result {
    let response = request.send().await?;
    assert_eq!(response.status(), expected, "{}", response.text().await?);
    Ok(())
}
async fn value(request: reqwest::RequestBuilder) -> Result<Value> {
    Ok(request.send().await?.error_for_status()?.json().await?)
}
async fn push(local: &Path, url: &str, refs: &[&str], accepted: bool) -> Result<String> {
    let result = Command::new("git")
        .current_dir(local)
        .env("GIT_TERMINAL_PROMPT", "0")
        .args(["-c", "credential.helper=", "-c", AUTH, "push", url])
        .args(refs)
        .output()
        .await?;
    let message = String::from_utf8_lossy(&result.stderr).into_owned();
    assert_eq!(result.status.success(), accepted, "{refs:?}: {message}");
    Ok(message)
}
async fn refs(url: &str) -> Result<String> {
    Ok(String::from_utf8(
        run_git(None, &["-c", AUTH, "ls-remote", "--refs", url]).await?,
    )?)
}
async fn start(
    client: &Client,
    repo: &str,
    repository: &Value,
    oid: &str,
    version: i64,
) -> Result<String> {
    let id = uuid::Uuid::new_v4().to_string();
    status(client.post(format!("{repo}/commits/{oid}/checks")).bearer_auth(OWNER)
        .json(&json!({"repository_id":repository, "id":id, "context":"unit", "context_version":version})), StatusCode::OK).await?;
    Ok(id)
}
async fn report(client: &Client, repo: &str, repository: &Value, id: &str, state: &str) -> Result {
    status(client.put(format!("{repo}/checks/{id}")).bearer_auth(OWNER)
        .json(&json!({"repository_id":repository,"expected_version":1,"state":state,"summary":"Branch test"})), StatusCode::NO_CONTENT).await
}

#[tokio::test(flavor = "multi_thread")]
async fn protected_pushes_preserve_native_reports_and_policy_across_recovery() -> Result {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let workspace = tempfile::TempDir::new()?;
    let address = available_address().await?;
    let server = CanopyServer::start(
        config(address, workspace.path().join("first")),
        Arc::clone(&store),
    )
    .await?;
    let url = create_repository(address, "team").await?;
    let repo = format!("http://{address}/api/repositories/team");
    let api = format!("{repo}/branch-rules");
    let client = Client::new();
    let repository = value(client.get(&repo).bearer_auth(OWNER)).await?["repository_id"].clone();
    let local = workspace.path().join("local");
    run_git(None, &["init", "-b", "main", path_str(&local)?]).await?;
    run_git(Some(&local), &["config", "user.name", "Canopy Test"]).await?;
    run_git(
        Some(&local),
        &["config", "user.email", "test@example.invalid"],
    )
    .await?;
    run_git(Some(&local), &["commit", "--allow-empty", "-m", "First"]).await?;
    let original = String::from_utf8(run_git(Some(&local), &["rev-parse", "HEAD"]).await?)?
        .trim()
        .to_owned();
    push(&local, &url, &["HEAD:refs/heads/main"], true).await?;
    run_git(Some(&local), &["commit", "--allow-empty", "-m", "Second"]).await?;
    let next = String::from_utf8(run_git(Some(&local), &["rev-parse", "HEAD"]).await?)?
        .trim()
        .to_owned();
    let mut rule = json!({"repository_id":repository,"rule":{"reference":"refs/heads/main", "expected_version":0,"enabled":true,"deny_deletions":true,"fast_forward_only":true,"required_checks":["unit"]}});
    status(client.get(&api), StatusCode::UNAUTHORIZED).await?;
    status(
        client.put(&api).bearer_auth(OWNER).json(&rule),
        StatusCode::CONFLICT,
    )
    .await?;
    let context =
        json!({"repository_id":repository,"expected_version":0,"enabled":true,"reporter":"canopy"});
    let context_api = format!("{repo}/check-contexts/unit");
    status(
        client.put(&context_api).bearer_auth(OWNER).json(&context),
        StatusCode::NO_CONTENT,
    )
    .await?;
    status(
        client.put(&api).bearer_auth(OWNER).json(&rule),
        StatusCode::NO_CONTENT,
    )
    .await?;
    status(
        client.put(&api).bearer_auth(OWNER).json(&rule),
        StatusCode::CONFLICT,
    )
    .await?;
    for (field, bad) in [
        ("reference", json!("refs/tags/v1")),
        ("required_checks", json!(["unit", "unit"])),
        ("expected_version", json!(-1)),
    ] {
        let mut invalid = rule.clone();
        invalid["rule"][field] = bad;
        status(
            client.put(&api).bearer_auth(OWNER).json(&invalid),
            StatusCode::UNPROCESSABLE_ENTITY,
        )
        .await?;
    }
    let reader = format!("cnp_{}", "34".repeat(32));
    status(
        client
            .post(format!("http://{address}/api/accounts"))
            .bearer_auth(OWNER)
            .json(&json!({"name":"reader", "token":reader,"scope":"write"})),
        StatusCode::OK,
    )
    .await?;
    status(
        client
            .put(format!("{repo}/collaborators/reader"))
            .bearer_auth(OWNER)
            .json(&json!({"role":"read"})),
        StatusCode::OK,
    )
    .await?;
    status(
        client.put(&api).bearer_auth(&reader).json(&rule),
        StatusCode::FORBIDDEN,
    )
    .await?;
    assert_eq!(
        value(client.get(&api).bearer_auth(&reader)).await?["rules"][0]["version"],
        1
    );

    // Missing checks reject one ref while an ordinary sibling push persists the commit.
    let result = push(
        &local,
        &url,
        &["HEAD:refs/heads/main", "HEAD:refs/heads/feature"],
        false,
    )
    .await?;
    assert!(result.contains("hook declined"), "{result}");
    let remote = refs(&url).await?;
    assert!(remote.contains(&format!("{original}\trefs/heads/main")));
    assert!(remote.contains(&format!("{next}\trefs/heads/feature")));
    push(
        &local,
        &url,
        &["--atomic", "HEAD:refs/heads/main", "HEAD:refs/heads/atomic"],
        false,
    )
    .await?;
    assert!(!refs(&url).await?.contains("refs/heads/atomic"));
    let old = start(&client, &repo, &repository, &next, 1).await?;
    let newest = start(&client, &repo, &repository, &next, 1).await?;
    report(&client, &repo, &repository, &old, "success").await?;
    push(&local, &url, &["HEAD:refs/heads/main"], false).await?;
    report(&client, &repo, &repository, &newest, "failure").await?;
    push(&local, &url, &["HEAD:refs/heads/main"], false).await?;
    let passing = start(&client, &repo, &repository, &next, 1).await?;
    report(&client, &repo, &repository, &passing, "success").await?;
    // A changed context invalidates an otherwise successful attempt.
    let mut changed = context.clone();
    changed["expected_version"] = json!(1);
    status(
        client.put(&context_api).bearer_auth(OWNER).json(&changed),
        StatusCode::NO_CONTENT,
    )
    .await?;
    push(&local, &url, &["HEAD:refs/heads/main"], false).await?;
    let passing = start(&client, &repo, &repository, &next, 2).await?;
    report(&client, &repo, &repository, &passing, "success").await?;
    push(&local, &url, &["HEAD:refs/heads/main"], true).await?;
    let old_pass = start(&client, &repo, &repository, &original, 2).await?;
    report(&client, &repo, &repository, &old_pass, "success").await?;
    let rollback = format!("+{original}:refs/heads/main");
    let rejected = push(&local, &url, &[&rollback], false).await?;
    assert!(rejected.contains("fast-forward"), "{rejected}");
    push(&local, &url, &[":refs/heads/main"], false).await?;

    // Ref names valid in Git may contain shell metacharacters; hook cases quote them.
    let quoted = "refs/heads/a'b;$()";
    let mut unusual = rule.clone();
    unusual["rule"]["reference"] = json!(quoted);
    status(
        client.put(&api).bearer_auth(OWNER).json(&unusual),
        StatusCode::NO_CONTENT,
    )
    .await?;
    push(&local, &url, &[&format!("HEAD:{quoted}")], true).await?;
    push(&local, &url, &[&format!(":{quoted}")], false).await?;
    unusual["rule"]["expected_version"] = json!(1);
    unusual["rule"]["deny_deletions"] = json!(false);
    unusual["rule"]["fast_forward_only"] = json!(false);
    status(
        client.put(&api).bearer_auth(OWNER).json(&unusual),
        StatusCode::NO_CONTENT,
    )
    .await?;
    push(&local, &url, &[&format!("+{original}:{quoted}")], true).await?;
    start(&client, &repo, &repository, &original, 2).await?;
    // Deletion has its own policy; it needs no passing check for a new tip.
    push(&local, &url, &[&format!(":{quoted}")], true).await?;
    let before = value(client.get(&api).bearer_auth(OWNER)).await?;
    status(
        client
            .patch(&repo)
            .bearer_auth(OWNER)
            .json(&json!({"name":"renamed", "repository_id":repository})),
        StatusCode::OK,
    )
    .await?;
    server.shutdown().await?;
    let address = available_address().await?;
    let restored = CanopyServer::start(
        config(address, workspace.path().join("restored")),
        Arc::clone(&store),
    )
    .await?;
    let repo = format!("http://{address}/api/repositories/renamed");
    let api = format!("{repo}/branch-rules");
    let url = format!("http://{address}/canopy/renamed.git");
    assert_eq!(value(client.get(&api).bearer_auth(OWNER)).await?, before);
    assert!(
        refs(&url)
            .await?
            .contains(&format!("{next}\trefs/heads/main"))
    );
    push(&local, &url, &[&rollback], false).await?;
    push(&local, &url, &[":refs/heads/main"], false).await?;
    rule["rule"]["expected_version"] = json!(1);
    rule["rule"]["enabled"] = json!(false);
    status(
        client.put(&api).bearer_auth(OWNER).json(&rule),
        StatusCode::NO_CONTENT,
    )
    .await?;
    push(&local, &url, &[&rollback], true).await?;
    rule["rule"]["expected_version"] = json!(2);
    rule["rule"]["enabled"] = json!(true);
    status(
        client.put(&api).bearer_auth(OWNER).json(&rule),
        StatusCode::NO_CONTENT,
    )
    .await?;
    // Restoration preserves the ancestry proof needed to promote the checked tip.
    push(&local, &url, &["HEAD:refs/heads/main"], true).await?;
    push(&local, &url, &[":refs/heads/main"], false).await?;
    restored.shutdown().await?;
    Ok(())
}
