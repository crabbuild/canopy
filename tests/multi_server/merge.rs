use super::tokens::{finish_upload, paused_upload};
use super::*;
use reqwest::{Client, StatusCode};
use serde_json::{Value, json};

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;
const OWNER: &str = "local-test-token";
const AUTH: &str = "http.extraHeader=Authorization: Bearer local-test-token";
async fn value(request: reqwest::RequestBuilder) -> Result<Value> {
    let response = request.send().await?;
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "{}",
        response.text().await?
    );
    Ok(response.json().await?)
}
async fn status(request: reqwest::RequestBuilder, expected: StatusCode) -> Result {
    let response = request.send().await?;
    assert_eq!(response.status(), expected, "{}", response.text().await?);
    Ok(())
}
async fn oid(local: &Path, rev: &str) -> Result<String> {
    Ok(
        String::from_utf8(run_git(Some(local), &["rev-parse", rev]).await?)?
            .trim()
            .into(),
    )
}
async fn push(local: &Path, url: &str, refs: &[&str], accepted: bool) -> Result {
    let result = Command::new("git")
        .current_dir(local)
        .args(["-c", "credential.helper=", "-c", AUTH, "push", url])
        .args(refs)
        .output()
        .await?;
    assert_eq!(
        result.status.success(),
        accepted,
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    Ok(())
}
async fn current(client: &Client, api: &str) -> Result<Value> {
    Ok(value(client.get(api).bearer_auth(OWNER)).await?["pull"].clone())
}
fn revision(p: &Value) -> Value {
    json!({"pull_version":p["version"],"source_oid":p["source"]["oid"],"source_version":p["source"]["version"],"base_oid":p["base"]["oid"],"base_version":p["base"]["version"]})
}
async fn intent(client: &Client, api: &str, repository: &Value) -> Result<Value> {
    Ok(
        json!({"repository_id":repository,"id":uuid::Uuid::new_v4().to_string(),"revision":revision(&current(client,api).await?),"strategy":"fast_forward"}),
    )
}
async fn review(
    client: &Client,
    api: &str,
    repository: &Value,
    token: &str,
    kind: &str,
) -> Result<Value> {
    let input = json!({"repository_id":repository,"id":uuid::Uuid::new_v4().to_string(),"revision":revision(&current(client,api).await?),"kind":kind,"body":"Review"});
    value(
        client
            .post(format!("{api}/reviews"))
            .bearer_auth(token)
            .json(&input),
    )
    .await?;
    Ok(input)
}
async fn policy(client: &Client, api: &str) -> Result<Value> {
    Ok(value(
        client
            .get(format!("{api}/review-policy"))
            .bearer_auth(OWNER),
    )
    .await?["policy"]
        .clone())
}
async fn new_pull(
    client: &Client,
    repo: &str,
    repository: &Value,
    source_ref: &str,
    source: &str,
    base: &str,
) -> Result<String> {
    let created=value(client.post(format!("{repo}/pulls")).bearer_auth(OWNER).json(&json!({"repository_id":repository,"id":uuid::Uuid::new_v4().to_string(),"title":"Merge this","body":"Description","draft":false,"source_ref":source_ref,"source_oid":source,"base_ref":"refs/heads/main","base_oid":base}))).await?;
    Ok(format!("{repo}/pulls/{}", created["number"]))
}
async fn init(local: &Path) -> Result {
    run_git(None, &["init", "-b", "main", path_str(local)?]).await?;
    run_git(Some(local), &["config", "user.name", "Merge Test"]).await?;
    run_git(
        Some(local),
        &["config", "user.email", "test@example.invalid"],
    )
    .await?;
    run_git(Some(local), &["commit", "--allow-empty", "-m", "Base"]).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn reviewed_merge_is_atomic_replayable_and_visible_to_git_after_recovery() -> Result {
    let workspace = tempfile::TempDir::new()?;
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let address = available_address().await?;
    let server = CanopyServer::start(
        config(address, workspace.path().join("node")),
        Arc::clone(&store),
    )
    .await?;
    let url = create_repository(address, "merge").await?;
    let repo = format!("http://{address}/api/repositories/merge");
    let client = Client::new();
    let repository = value(client.get(&repo).bearer_auth(OWNER)).await?["repository_id"].clone();
    let local = workspace.path().join("local");
    init(&local).await?;
    let base = oid(&local, "HEAD").await?;
    push(&local, &url, &["HEAD:refs/heads/main"], true).await?;
    tokio::fs::write(local.join("reviewed.txt"), b"durable reviewed change\n").await?;
    run_git(Some(&local), &["add", "."]).await?;
    run_git(Some(&local), &["commit", "-m", "Feature"]).await?;
    let source = oid(&local, "HEAD").await?;
    push(&local, &url, &["HEAD:refs/heads/feature"], true).await?;
    let api = new_pull(
        &client,
        &repo,
        &repository,
        "refs/heads/feature",
        &source,
        &base,
    )
    .await?;
    let merge_api = format!("{api}/merge");
    let alice = format!("cnp_{}", "71".repeat(32));
    let bob = format!("cnp_{}", "72".repeat(32));
    let viewer = format!("cnp_{}", "73".repeat(32));
    for (name, token, scope, role) in [
        ("alice", &alice, "write", "write"),
        ("bob", &bob, "write", "write"),
        ("viewer", &viewer, "read", "read"),
    ] {
        value(
            client
                .post(format!("http://{address}/api/accounts"))
                .bearer_auth(OWNER)
                .json(&json!({"name":name,"token":token,"scope":scope})),
        )
        .await?;
        value(
            client
                .put(format!("{repo}/collaborators/{name}"))
                .bearer_auth(OWNER)
                .json(&json!({"role":role})),
        )
        .await?;
    }
    let initial = policy(&client, &api).await?;
    assert_eq!(initial["required_approvals"], 0);
    assert_eq!(initial["reviews_satisfied"], true);
    let mut rule = json!({"repository_id":repository,"rule":{"reference":"refs/heads/main","expected_version":0,"enabled":true,"deny_deletions":false,"fast_forward_only":false,"required_checks":[],"require_pull_request":true,"required_approvals":2}});
    for (field, bad) in [
        ("require_pull_request", json!(false)),
        ("required_approvals", json!(17)),
    ] {
        let mut invalid = rule.clone();
        invalid["rule"][field] = bad;
        status(
            client
                .put(format!("{repo}/branch-rules"))
                .bearer_auth(OWNER)
                .json(&invalid),
            StatusCode::UNPROCESSABLE_ENTITY,
        )
        .await?;
    }
    status(
        client
            .put(format!("{repo}/branch-rules"))
            .bearer_auth(OWNER)
            .json(&rule),
        StatusCode::NO_CONTENT,
    )
    .await?;
    let input = intent(&client, &api, &repository).await?;
    status(
        client.post(&merge_api).json(&input),
        StatusCode::UNAUTHORIZED,
    )
    .await?;
    status(
        client.post(&merge_api).bearer_auth(&viewer).json(&input),
        StatusCode::FORBIDDEN,
    )
    .await?;
    status(
        client.post(&merge_api).bearer_auth(OWNER).json(&input),
        StatusCode::CONFLICT,
    )
    .await?;
    // Owner pushes and deletion cannot bypass required-PR rules, even with
    // deny_deletions and fast_forward_only explicitly disabled.
    push(
        &local,
        &url,
        &["HEAD:refs/heads/main", "HEAD:refs/heads/mixed"],
        false,
    )
    .await?;
    let refs = String::from_utf8(run_git(None, &["-c", AUTH, "ls-remote", "--refs", &url]).await?)?;
    assert!(refs.contains(&format!("{source}\trefs/heads/mixed")));
    assert!(refs.contains(&format!("{base}\trefs/heads/main")));
    push(
        &local,
        &url,
        &["--atomic", "HEAD:refs/heads/main", "HEAD:refs/heads/atomic"],
        false,
    )
    .await?;
    push(&local, &url, &[":refs/heads/main"], false).await?;
    let old = review(&client, &api, &repository, &alice, "approve").await?;
    review(&client, &api, &repository, &bob, "approve").await?;
    assert_eq!(policy(&client, &api).await?["approvals"], 2);
    review(&client, &api, &repository, &alice, "request_changes").await?;
    review(&client, &api, &repository, &alice, "comment").await?;
    value(
        client
            .post(format!("{api}/reviews"))
            .bearer_auth(&alice)
            .json(&old),
    )
    .await?;
    assert_eq!(policy(&client, &api).await?["approvals"], 1);
    assert_eq!(policy(&client, &api).await?["changes_requested"], true);
    status(
        client.post(&merge_api).bearer_auth(OWNER).json(&input),
        StatusCode::CONFLICT,
    )
    .await?;
    review(&client, &api, &repository, &alice, "approve").await?;
    status(
        client
            .delete(format!("{repo}/collaborators/bob"))
            .bearer_auth(OWNER),
        StatusCode::NO_CONTENT,
    )
    .await?;
    value(
        client
            .put(format!("{repo}/collaborators/bob"))
            .bearer_auth(OWNER)
            .json(&json!({"role":"write"})),
    )
    .await?;
    assert_eq!(policy(&client, &api).await?["approvals"], 1);
    review(&client, &api, &repository, &bob, "approve").await?;
    // The final command rechecks current policy and review heads after upload.
    let body = serde_json::to_vec(&input)?;
    let paused = paused_upload(
        address,
        "/api/repositories/merge/pulls/1/merge",
        OWNER,
        body.len(),
    )
    .await?;
    review(&client, &api, &repository, &bob, "request_changes").await?;
    finish_upload(paused, &body, 409).await?;
    assert_eq!(current(&client, &api).await?["base"]["oid"], base);
    review(&client, &api, &repository, &bob, "approve").await?;
    // Required checks still gate the same publication path.
    status(client.put(format!("{repo}/check-contexts/unit")).bearer_auth(OWNER).json(&json!({"repository_id":repository,"expected_version":0,"enabled":true,"reporter":"canopy"})),StatusCode::NO_CONTENT).await?;
    rule["rule"]["expected_version"] = json!(1);
    rule["rule"]["required_checks"] = json!(["unit"]);
    status(
        client
            .put(format!("{repo}/branch-rules"))
            .bearer_auth(OWNER)
            .json(&rule),
        StatusCode::NO_CONTENT,
    )
    .await?;
    status(
        client.post(&merge_api).bearer_auth(OWNER).json(&input),
        StatusCode::CONFLICT,
    )
    .await?;
    let check = uuid::Uuid::new_v4().to_string();
    value(client.post(format!("{repo}/commits/{source}/checks")).bearer_auth(OWNER).json(&json!({"repository_id":repository,"id":check,"context":"unit","context_version":1}))).await?;
    status(client.put(format!("{repo}/checks/{check}")).bearer_auth(OWNER).json(&json!({"repository_id":repository,"expected_version":1,"state":"success","summary":"Passed"})),StatusCode::NO_CONTENT).await?;
    // Reviews cannot turn a direct Git push into a merge endpoint.
    push(&local, &url, &["HEAD:refs/heads/main"], false).await?;
    let (a, b) = tokio::join!(
        value(client.post(&merge_api).bearer_auth(OWNER).json(&input)),
        value(client.post(&merge_api).bearer_auth(OWNER).json(&input))
    );
    let result = a?;
    assert_eq!(result, b?);
    assert_eq!(result["merge"]["oid"], source);
    let merged = current(&client, &api).await?;
    assert_eq!(merged["state"], "merged");
    assert_eq!(merged["version"], 2);
    assert_eq!(merged["base"]["version"], 2);
    assert_eq!(merged["base"]["oid"], source);
    assert_eq!(merged["merge"], result["merge"]);
    let edit = json!({"repository_id":repository,"expected_version":2,"title":"Changed","body":"","state":"open","draft":false});
    status(
        client.put(&api).bearer_auth(OWNER).json(&edit),
        StatusCode::CONFLICT,
    )
    .await?;
    let mut other = input.clone();
    other["id"] = json!(uuid::Uuid::new_v4().to_string());
    status(
        client.post(&merge_api).bearer_auth(OWNER).json(&other),
        StatusCode::CONFLICT,
    )
    .await?;
    other = input.clone();
    other["revision"]["base_version"] = json!(2);
    status(
        client.post(&merge_api).bearer_auth(OWNER).json(&other),
        StatusCode::CONFLICT,
    )
    .await?;
    status(
        client.post(&merge_api).bearer_auth(&alice).json(&input),
        StatusCode::CONFLICT,
    )
    .await?;
    // Deleting the source after publication does not remove the durable result.
    push(&local, &url, &[":refs/heads/feature"], true).await?;
    let final_pull = current(&client, &api).await?;
    assert_eq!(final_pull["merge"], result["merge"]);
    value(
        client
            .patch(&repo)
            .bearer_auth(OWNER)
            .json(&json!({"name":"renamed","repository_id":repository})),
    )
    .await?;
    server.shutdown().await?;
    let address = available_address().await?;
    let restored =
        CanopyServer::start(config(address, workspace.path().join("restored")), store).await?;
    let api = format!("http://{address}/api/repositories/renamed/pulls/1");
    assert_eq!(current(&client, &api).await?, final_pull);
    assert_eq!(
        value(
            client
                .post(format!("{api}/merge"))
                .bearer_auth(OWNER)
                .json(&input)
        )
        .await?,
        result
    );
    let clone = workspace.path().join("clone");
    run_git(
        None,
        &[
            "-c",
            AUTH,
            "clone",
            &format!("http://{address}/canopy/renamed.git"),
            path_str(&clone)?,
        ],
    )
    .await?;
    assert_eq!(oid(&clone, "HEAD").await?, source);
    assert_eq!(
        tokio::fs::read(clone.join("reviewed.txt")).await?,
        b"durable reviewed change\n"
    );
    run_git(Some(&clone), &["fsck", "--strict"]).await?;
    restored.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn merge_rechecks_revisions_authority_and_competing_publications() -> Result {
    let workspace = tempfile::TempDir::new()?;
    let address = available_address().await?;
    let server = CanopyServer::start(
        config(address, workspace.path().join("node")),
        Arc::new(InMemory::new()),
    )
    .await?;
    let url = create_repository(address, "races").await?;
    let repo = format!("http://{address}/api/repositories/races");
    let client = Client::new();
    let repository = value(client.get(&repo).bearer_auth(OWNER)).await?["repository_id"].clone();
    let local = workspace.path().join("local");
    init(&local).await?;
    let base = oid(&local, "HEAD").await?;
    push(&local, &url, &["HEAD:refs/heads/main"], true).await?;
    let tree = oid(&local, "HEAD^{tree}").await?;
    let mut sources = Vec::new();
    for (name, parent) in [
        ("left", Some(&base)),
        ("right", Some(&base)),
        ("unrelated", None),
    ] {
        let mut args = vec!["commit-tree", &tree, "-m", name];
        if let Some(parent) = parent {
            args.extend(["-p", parent]);
        }
        let source = String::from_utf8(run_git(Some(&local), &args).await?)?
            .trim()
            .to_owned();
        push(
            &local,
            &url,
            &[&format!("{source}:refs/heads/{name}")],
            true,
        )
        .await?;
        let api = new_pull(
            &client,
            &repo,
            &repository,
            &format!("refs/heads/{name}"),
            &source,
            &base,
        )
        .await?;
        sources.push((api, source));
    }
    let unrelated = intent(&client, &sources[2].0, &repository).await?;
    let response = client
        .post(format!("{}/merge", sources[2].0))
        .bearer_auth(OWNER)
        .json(&unrelated)
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::CONFLICT);
    assert!(response.text().await?.contains("descend"));
    let api = &sources[0].0;
    let merge_api = format!("{api}/merge");
    let old = intent(&client, api, &repository).await?;
    push(
        &local,
        &url,
        &[&format!("+{}:refs/heads/left", sources[1].1)],
        true,
    )
    .await?;
    push(
        &local,
        &url,
        &[&format!("+{}:refs/heads/left", sources[0].1)],
        true,
    )
    .await?;
    status(
        client.post(&merge_api).bearer_auth(OWNER).json(&old),
        StatusCode::CONFLICT,
    )
    .await?;
    let input = intent(&client, api, &repository).await?;
    let edit = json!({"repository_id":repository,"expected_version":1,"title":"Draft","body":"","state":"open","draft":true});
    status(
        client.put(api).bearer_auth(OWNER).json(&edit),
        StatusCode::NO_CONTENT,
    )
    .await?;
    status(
        client.post(&merge_api).bearer_auth(OWNER).json(&input),
        StatusCode::CONFLICT,
    )
    .await?;
    let draft = intent(&client, api, &repository).await?;
    status(
        client.post(&merge_api).bearer_auth(OWNER).json(&draft),
        StatusCode::CONFLICT,
    )
    .await?;
    let mut edit = edit;
    edit["expected_version"] = json!(2);
    edit["draft"] = json!(false);
    status(
        client.put(api).bearer_auth(OWNER).json(&edit),
        StatusCode::NO_CONTENT,
    )
    .await?;
    let writer = format!("cnp_{}", "79".repeat(32));
    value(
        client
            .post(format!("http://{address}/api/accounts"))
            .bearer_auth(OWNER)
            .json(&json!({"name":"merger","token":writer,"scope":"write"})),
    )
    .await?;
    value(
        client
            .put(format!("{repo}/collaborators/merger"))
            .bearer_auth(OWNER)
            .json(&json!({"role":"write"})),
    )
    .await?;
    let input = intent(&client, api, &repository).await?;
    let bytes = serde_json::to_vec(&input)?;
    let upload = paused_upload(
        address,
        "/api/repositories/races/pulls/1/merge",
        &writer,
        bytes.len(),
    )
    .await?;
    status(
        client
            .delete(format!("{repo}/collaborators/merger"))
            .bearer_auth(OWNER),
        StatusCode::NO_CONTENT,
    )
    .await?;
    finish_upload(upload, &bytes, 404).await?;
    assert_eq!(current(&client, api).await?["base"]["oid"], base);
    let left = intent(&client, &sources[0].0, &repository).await?;
    let right = intent(&client, &sources[1].0, &repository).await?;
    let a = client
        .post(format!("{}/merge", sources[0].0))
        .bearer_auth(OWNER)
        .json(&left);
    let b = client
        .post(format!("{}/merge", sources[1].0))
        .bearer_auth(OWNER)
        .json(&right);
    let (a, b) = tokio::join!(a.send(), b.send());
    let a = a?;
    let b = b?;
    assert!(matches!(
        (a.status(), b.status()),
        (StatusCode::OK, StatusCode::CONFLICT) | (StatusCode::CONFLICT, StatusCode::OK)
    ));
    let pulls = [
        current(&client, &sources[0].0).await?,
        current(&client, &sources[1].0).await?,
    ];
    assert_eq!(pulls.iter().filter(|p| p["state"] == "merged").count(), 1);
    assert_eq!(pulls[0]["base"]["version"], 2);
    assert_eq!(pulls[1]["base"]["version"], 2);
    let winner = pulls.iter().find(|p| p["state"] == "merged").unwrap();
    let native = String::from_utf8(
        run_git(None, &["-c", AUTH, "ls-remote", &url, "refs/heads/main"]).await?,
    )?;
    assert_eq!(
        native.trim(),
        format!(
            "{}\trefs/heads/main",
            winner["merge"]["oid"].as_str().unwrap()
        )
    );
    server.shutdown().await?;
    Ok(())
}
