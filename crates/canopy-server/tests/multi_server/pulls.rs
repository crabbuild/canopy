use super::tokens::{finish_upload, paused_upload};
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
async fn push(local: &Path, url: &str, reference: &str) -> Result {
    run_git(Some(local), &["-c", AUTH, "push", url, reference]).await?;
    Ok(())
}
async fn oid(local: &Path, revision: &str) -> Result<String> {
    Ok(
        String::from_utf8(run_git(Some(local), &["rev-parse", revision]).await?)?
            .trim()
            .into(),
    )
}
fn revision(pull: &Value) -> Value {
    json!({"pull_version":pull["version"],"source_oid":pull["source"]["oid"],"source_version":pull["source"]["version"],"base_oid":pull["base"]["oid"],"base_version":pull["base"]["version"]})
}
async fn current(client: &Client, pull_api: &str) -> Result<Value> {
    Ok(value(client.get(pull_api).bearer_auth(OWNER)).await?["pull"].clone())
}
async fn review(
    client: &Client,
    api: &str,
    token: &str,
    repository: &Value,
    kind: &str,
) -> Result<Value> {
    let payload = json!({"repository_id":repository,"id":uuid::Uuid::new_v4().to_string(),"revision":revision(&current(client,api).await?),"kind":kind,"body":"Reviewed"});
    value(
        client
            .post(format!("{api}/reviews"))
            .bearer_auth(token)
            .json(&payload),
    )
    .await?;
    Ok(payload)
}
async fn applicable(client: &Client, api: &str) -> Result<Vec<String>> {
    Ok(
        value(client.get(format!("{api}/reviews")).bearer_auth(OWNER)).await?["reviews"]
            .as_array()
            .ok_or("missing reviews")?
            .iter()
            .filter(|review| review["applicable"] == true)
            .map(|review| review["id"].as_str().unwrap().into())
            .collect(),
    )
}
#[tokio::test(flavor = "multi_thread")]
async fn pull_reviews_follow_exact_revisions_and_membership_across_recovery() -> Result {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let workspace = tempfile::TempDir::new()?;
    let address = available_address().await?;
    let server = CanopyServer::start(
        config(address, workspace.path().join("first")),
        Arc::clone(&store),
    )
    .await?;
    let url = create_repository(address, "team").await?;
    create_repository(address, "other").await?;
    let base = format!("http://{address}");
    let repo = format!("{base}/api/repositories/team");
    let api = format!("{repo}/pulls");
    let client = Client::new();
    let repository = value(client.get(&repo).bearer_auth(OWNER)).await?["repository_id"].clone();
    let mut tokens = Vec::new();
    for (index, (account, scope, role)) in [
        ("author", "write", "read"),
        ("reviewer", "write", "write"),
        ("viewer", "read", "read"),
    ]
    .into_iter()
    .enumerate()
    {
        let token = format!("cnp_{:064x}", index + 7);
        status(
            client
                .post(format!("{base}/api/accounts"))
                .bearer_auth(OWNER)
                .json(&json!({"name":account,"token":token,"scope":scope})),
            StatusCode::OK,
        )
        .await?;
        status(
            client
                .put(format!("{repo}/collaborators/{account}"))
                .bearer_auth(OWNER)
                .json(&json!({"role":role})),
            StatusCode::OK,
        )
        .await?;
        tokens.push(token);
    }
    let [author, reviewer, viewer] = tokens.as_slice() else {
        return Err("missing credentials".into());
    };
    let local = workspace.path().join("local");
    run_git(None, &["init", "-b", "main", path_str(&local)?]).await?;
    run_git(Some(&local), &["config", "user.name", "Test"]).await?;
    run_git(
        Some(&local),
        &["config", "user.email", "test@example.invalid"],
    )
    .await?;
    run_git(Some(&local), &["commit", "--allow-empty", "-m", "Base"]).await?;
    let original = oid(&local, "HEAD").await?;
    push(&local, &url, "HEAD:refs/heads/main").await?;
    tokio::fs::write(local.join("change.txt"), b"Change for review\n").await?;
    run_git(Some(&local), &["add", "change.txt"]).await?;
    run_git(Some(&local), &["commit", "-m", "Feature"]).await?;
    let first = oid(&local, "HEAD").await?;
    push(&local, &url, "HEAD:refs/heads/feature").await?;
    let creation = json!({"repository_id":repository,"id":uuid::Uuid::new_v4().to_string(),"title":"Review this change","body":"Description","draft":false,"source_ref":"refs/heads/feature","source_oid":first,"base_ref":"refs/heads/main","base_oid":original});
    status(client.get(&api), StatusCode::UNAUTHORIZED).await?;
    status(
        client.post(&api).bearer_auth(viewer).json(&creation),
        StatusCode::FORBIDDEN,
    )
    .await?;
    let (a, b) = tokio::join!(
        value(client.post(&api).bearer_auth(author).json(&creation)),
        value(client.post(&api).bearer_auth(author).json(&creation))
    );
    assert_eq!(a?, json!({"number":1}));
    assert_eq!(b?, json!({"number":1}));
    let pull_api = format!("{api}/1");
    let review_api = format!("{pull_api}/reviews");
    for (field, bad, code) in [
        ("title", json!("Different"), StatusCode::CONFLICT),
        (
            "source_ref",
            json!("refs/tags/v1"),
            StatusCode::UNPROCESSABLE_ENTITY,
        ),
        (
            "body",
            json!("x".repeat(16385)),
            StatusCode::UNPROCESSABLE_ENTITY,
        ),
        (
            "repository_id",
            json!(uuid::Uuid::new_v4().to_string()),
            StatusCode::CONFLICT,
        ),
    ] {
        let mut invalid = creation.clone();
        invalid[field] = bad;
        status(client.post(&api).bearer_auth(author).json(&invalid), code).await?;
    }
    let mut missing = creation.clone();
    missing["id"] = json!(uuid::Uuid::new_v4().to_string());
    missing["source_oid"] = json!("00".repeat(20));
    status(
        client.post(&api).bearer_auth(author).json(&missing),
        StatusCode::CONFLICT,
    )
    .await?;
    let mut attempt = json!({"repository_id":repository,"id":uuid::Uuid::new_v4().to_string(),"revision":revision(&current(&client,&pull_api).await?),"kind":"approve","body":""});
    status(
        client.post(&review_api).bearer_auth(author).json(&attempt),
        StatusCode::FORBIDDEN,
    )
    .await?;
    status(
        client.post(&review_api).bearer_auth(viewer).json(&attempt),
        StatusCode::FORBIDDEN,
    )
    .await?;
    let (a, b) = tokio::join!(
        value(
            client
                .post(&review_api)
                .bearer_auth(reviewer)
                .json(&attempt)
        ),
        value(
            client
                .post(&review_api)
                .bearer_auth(reviewer)
                .json(&attempt)
        )
    );
    assert_eq!(a?, b?);
    assert_eq!(
        applicable(&client, &pull_api).await?,
        vec![attempt["id"].as_str().unwrap()]
    );
    let objection = review(&client, &pull_api, reviewer, &repository, "request_changes").await?;
    review(&client, &pull_api, reviewer, &repository, "comment").await?;
    value(
        client
            .post(&review_api)
            .bearer_auth(reviewer)
            .json(&attempt),
    )
    .await?;
    assert_eq!(
        applicable(&client, &pull_api).await?,
        vec![objection["id"].as_str().unwrap()]
    );
    attempt["body"] = json!("Changed");
    status(
        client
            .post(&review_api)
            .bearer_auth(reviewer)
            .json(&attempt),
        StatusCode::CONFLICT,
    )
    .await?;
    let approved = review(&client, &pull_api, reviewer, &repository, "approve").await?;
    // Source movement and its ABA return must both invalidate an existing review.
    run_git(
        Some(&local),
        &["commit", "--allow-empty", "-m", "Follow-up"],
    )
    .await?;
    let second = oid(&local, "HEAD").await?;
    push(&local, &url, "HEAD:refs/heads/feature").await?;
    assert!(applicable(&client, &pull_api).await?.is_empty());
    push(&local, &url, &format!("+{first}:refs/heads/feature")).await?;
    assert!(applicable(&client, &pull_api).await?.is_empty());
    value(
        client
            .post(&review_api)
            .bearer_auth(reviewer)
            .json(&approved),
    )
    .await?;
    let mut stale = approved.clone();
    stale["id"] = json!(uuid::Uuid::new_v4().to_string());
    status(
        client.post(&review_api).bearer_auth(reviewer).json(&stale),
        StatusCode::CONFLICT,
    )
    .await?;
    push(&local, &url, "HEAD:refs/heads/feature").await?;
    review(&client, &pull_api, reviewer, &repository, "approve").await?;
    push(&local, &url, &format!("{first}:refs/heads/main")).await?;
    assert!(applicable(&client, &pull_api).await?.is_empty());
    let mut edit = json!({"repository_id":repository,"expected_version":1,"title":"Edited","body":"Updated description","state":"closed","draft":true});
    status(
        client.put(&pull_api).bearer_auth(author).json(&edit),
        StatusCode::NO_CONTENT,
    )
    .await?;
    status(
        client.put(&pull_api).bearer_auth(author).json(&edit),
        StatusCode::CONFLICT,
    )
    .await?;
    let mut blocked = stale.clone();
    blocked["revision"] = revision(&current(&client, &pull_api).await?);
    status(
        client
            .post(&review_api)
            .bearer_auth(reviewer)
            .json(&blocked),
        StatusCode::CONFLICT,
    )
    .await?;
    edit["expected_version"] = json!(2);
    edit["state"] = json!("open");
    status(
        client.put(&pull_api).bearer_auth(author).json(&edit),
        StatusCode::NO_CONTENT,
    )
    .await?;
    blocked["revision"] = revision(&current(&client, &pull_api).await?);
    status(
        client
            .post(&review_api)
            .bearer_auth(reviewer)
            .json(&blocked),
        StatusCode::CONFLICT,
    )
    .await?;
    review(&client, &pull_api, author, &repository, "comment").await?;
    edit["expected_version"] = json!(3);
    edit["draft"] = json!(false);
    status(
        client.put(&pull_api).bearer_auth(author).json(&edit),
        StatusCode::NO_CONTENT,
    )
    .await?;
    let old_grant = review(&client, &pull_api, reviewer, &repository, "approve").await?;
    let membership = format!("{repo}/collaborators/reviewer");
    status(
        client.delete(&membership).bearer_auth(OWNER),
        StatusCode::NO_CONTENT,
    )
    .await?;
    assert!(applicable(&client, &pull_api).await?.is_empty());
    status(
        client
            .put(&membership)
            .bearer_auth(OWNER)
            .json(&json!({"role":"write"})),
        StatusCode::OK,
    )
    .await?;
    value(
        client
            .post(&review_api)
            .bearer_auth(reviewer)
            .json(&old_grant),
    )
    .await?;
    assert!(applicable(&client, &pull_api).await?.is_empty());
    let good = review(&client, &pull_api, reviewer, &repository, "approve").await?;
    status(
        client
            .put(&membership)
            .bearer_auth(OWNER)
            .json(&json!({"role":"write"})),
        StatusCode::OK,
    )
    .await?;
    assert_eq!(
        applicable(&client, &pull_api).await?,
        vec![good["id"].as_str().unwrap()]
    );
    status(
        client
            .put(&membership)
            .bearer_auth(OWNER)
            .json(&json!({"role":"read"})),
        StatusCode::OK,
    )
    .await?;
    let mut foreign_edit = edit.clone();
    foreign_edit["expected_version"] = json!(4);
    status(
        client
            .put(&pull_api)
            .bearer_auth(reviewer)
            .json(&foreign_edit),
        StatusCode::FORBIDDEN,
    )
    .await?;
    status(
        client
            .put(&membership)
            .bearer_auth(OWNER)
            .json(&json!({"role":"write"})),
        StatusCode::OK,
    )
    .await?;
    assert!(applicable(&client, &pull_api).await?.is_empty());
    let delayed = json!({"repository_id":repository,"id":uuid::Uuid::new_v4().to_string(),"revision":revision(&current(&client,&pull_api).await?),"kind":"approve","body":"Delayed"});
    let body = serde_json::to_vec(&delayed)?;
    let upload = paused_upload(
        address,
        "/api/repositories/team/pulls/1/reviews",
        reviewer,
        body.len(),
    )
    .await?;
    status(
        client.delete(&membership).bearer_auth(OWNER),
        StatusCode::NO_CONTENT,
    )
    .await?;
    finish_upload(upload, &body, 404).await?;
    status(
        client
            .put(&membership)
            .bearer_auth(OWNER)
            .json(&json!({"role":"write"})),
        StatusCode::OK,
    )
    .await?;
    push(&local, &url, ":refs/heads/feature").await?;
    assert!(current(&client, &pull_api).await?["source"]["oid"].is_null());
    status(
        client
            .post(&review_api)
            .bearer_auth(reviewer)
            .json(&delayed),
        StatusCode::CONFLICT,
    )
    .await?;
    push(&local, &url, "HEAD:refs/heads/feature").await?;
    let final_review = review(&client, &pull_api, reviewer, &repository, "approve").await?;
    assert_eq!(current(&client, &pull_api).await?["source"]["oid"], second);
    assert_eq!(
        value(client.post(&api).bearer_auth(author).json(&creation)).await?,
        json!({"number":1})
    );
    let edited = current(&client, &pull_api).await?;
    assert_eq!(edited["version"], 4);
    assert_eq!(edited["title"], "Edited");
    for number in 0..32 {
        let mut another = creation.clone();
        another["id"] = json!(uuid::Uuid::new_v4().to_string());
        another["title"] = json!(format!("Pull {number}"));
        another["source_oid"] = json!(second);
        another["base_oid"] = json!(first);
        another["body"] = json!("x".repeat(16384));
        value(client.post(&api).bearer_auth(author).json(&another)).await?;
    }
    let page = value(client.get(&api).bearer_auth(viewer)).await?;
    assert_eq!(page["pulls"].as_array().unwrap().len(), 32);
    assert_eq!(page["next_after"], 32);
    assert!(page["pulls"][0].get("body").is_none());
    let end = value(client.get(&api).query(&[("after", 32)]).bearer_auth(viewer)).await?;
    assert_eq!(end["pulls"].as_array().unwrap().len(), 1);
    assert!(end["next_after"].is_null());
    // Review pages stay bounded even when every body reaches the byte limit.
    for _ in 0..17 {
        let mut comment = final_review.clone();
        comment["id"] = json!(uuid::Uuid::new_v4().to_string());
        comment["kind"] = json!("comment");
        comment["body"] = json!("x".repeat(16384));
        value(client.post(&review_api).bearer_auth(author).json(&comment)).await?;
    }
    let reviews = value(client.get(&review_api).bearer_auth(viewer)).await?;
    assert_eq!(reviews["reviews"].as_array().unwrap().len(), 16);
    let review_tail = value(
        client
            .get(&review_api)
            .query(&[("after", reviews["next_after"].as_i64().unwrap())])
            .bearer_auth(viewer),
    )
    .await?;
    assert!(!review_tail["reviews"].as_array().unwrap().is_empty());
    status(
        client
            .get(format!("{base}/api/repositories/other/pulls/1"))
            .bearer_auth(OWNER),
        StatusCode::NOT_FOUND,
    )
    .await?;
    status(
        client
            .patch(&repo)
            .bearer_auth(OWNER)
            .json(&json!({"repository_id":repository,"name":"renamed"})),
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
    let api = format!("http://{address}/api/repositories/renamed/pulls");
    let pull_api = format!("{api}/1");
    let review_api = format!("{pull_api}/reviews");
    assert_eq!(current(&client, &pull_api).await?, edited);
    assert_eq!(
        value(client.get(&review_api).bearer_auth(viewer)).await?,
        reviews
    );
    value(
        client
            .post(&review_api)
            .bearer_auth(reviewer)
            .json(&final_review),
    )
    .await?;
    assert_eq!(
        value(client.get(&review_api).bearer_auth(viewer)).await?,
        reviews
    );
    assert_eq!(
        applicable(&client, &pull_api).await?,
        vec![final_review["id"].as_str().unwrap()]
    );
    // New decisions continue the same ordering after owner recovery.
    review(&client, &pull_api, reviewer, &repository, "request_changes").await?;
    assert!(applicable(&client, &pull_api).await?.is_empty());
    let mut cursor = 0;
    let mut selected = Vec::new();
    loop {
        let page = value(
            client
                .get(&review_api)
                .query(&[("after", cursor)])
                .bearer_auth(OWNER),
        )
        .await?;
        selected.extend(
            page["reviews"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|r| r["applicable"] == true)
                .cloned(),
        );
        let Some(next) = page["next_after"].as_i64() else {
            break;
        };
        cursor = next;
    }
    assert_eq!(selected.len(), 1);
    assert_eq!(selected[0]["kind"], "request_changes");
    restored.shutdown().await?;
    Ok(())
}
