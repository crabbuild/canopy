use super::*;

#[tokio::test(flavor = "multi_thread")]
async fn historical_comparisons_survive_branch_deletion_and_recheck_current_access() -> Result {
    let workspace = tempfile::TempDir::new()?;
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let address = available_address().await?;
    let server = CanopyServer::start(
        config(address, workspace.path().join("first")),
        Arc::clone(&store),
    )
    .await?;
    let url = create_repository(address, "history").await?;
    let repo = format!("http://{address}/api/repositories/history");
    let api = format!("{repo}/pulls/1");
    let compare = format!("{api}/comparison");
    let client = Client::new();
    let local = workspace.path().join("local");
    init(&local).await?;
    tokio::fs::write(local.join("edited"), b"before\n").await?;
    run_git(Some(&local), &["add", "."]).await?;
    run_git(Some(&local), &["commit", "-m", "Base"]).await?;
    let base = oid(&local, "HEAD").await?;
    push(&local, &url, "HEAD:refs/heads/main").await?;
    tokio::fs::write(local.join("edited"), b"reviewed\n").await?;
    run_git(Some(&local), &["commit", "-am", "Reviewed change"]).await?;
    let source = oid(&local, "HEAD").await?;
    push(&local, &url, "HEAD:refs/heads/feature").await?;
    open(&client, &repo, &source, &base).await?;
    let live = request(&client, &repo).await?;
    let original = value(client.post(&compare).bearer_auth(OWNER).json(&live)).await?;
    let original_patch = patch(&client, &compare, &live, b"edited").await?;
    let review = value(client.post(format!("{api}/reviews")).bearer_auth(OWNER).json(&json!({
        "repository_id":live["repository_id"],"id":uuid::Uuid::new_v4().to_string(),
        "revision":live["target"]["revision"],"kind":"comment","body":"Read this exact version"
    }))).await?;
    let historical = json!({"repository_id":live["repository_id"],"target":{"kind":"review","number":review["number"]},"query":{"kind":"files"}});
    tokio::fs::write(local.join("edited"), b"published later\n").await?;
    run_git(Some(&local), &["commit", "-am", "Later change"]).await?;
    let later = oid(&local, "HEAD").await?;
    push(&local, &url, "HEAD:refs/heads/feature").await?;
    status(
        client.post(&compare).bearer_auth(OWNER).json(&live),
        StatusCode::CONFLICT,
    )
    .await?;
    assert_eq!(
        value(client.post(&compare).bearer_auth(OWNER).json(&historical)).await?,
        original
    );
    let reviewed_file = preview(&client, &compare, &historical, b"edited", "after").await?;
    assert_eq!(
        URL_SAFE_NO_PAD.decode(reviewed_file["content_base64"].as_str().unwrap())?,
        b"reviewed\n"
    );
    open(&client, &repo, &later, &base).await?;
    status(
        client
            .post(format!("{repo}/pulls/2/comparison"))
            .bearer_auth(OWNER)
            .json(&historical),
        StatusCode::NOT_FOUND,
    )
    .await?;
    let merged = json!({"repository_id":live["repository_id"],"target":{"kind":"merged"},"query":{"kind":"files"}});
    status(
        client.post(&compare).bearer_auth(OWNER).json(&merged),
        StatusCode::NOT_FOUND,
    )
    .await?;
    let current = request(&client, &repo).await?;
    let published = value(client.post(&compare).bearer_auth(OWNER).json(&current)).await?;
    let published_patch = patch(&client, &compare, &current, b"edited").await?;
    assert_ne!(original_patch["hunks"], published_patch["hunks"]);
    let merge_input = json!({"repository_id":live["repository_id"],"id":uuid::Uuid::new_v4().to_string(),"revision":current["target"]["revision"],"strategy":"fast_forward"});
    let result = value(
        client
            .post(format!("{api}/merge"))
            .bearer_auth(OWNER)
            .json(&merge_input),
    )
    .await?;
    assert_eq!(result["merge"]["revision"], current["target"]["revision"]);
    assert_eq!(
        value(client.get(&api).bearer_auth(OWNER)).await?["pull"]["merge"],
        result["merge"]
    );
    push(&local, &url, ":refs/heads/feature").await?;
    push(&local, &url, ":refs/heads/main").await?;
    assert_eq!(
        value(client.post(&compare).bearer_auth(OWNER).json(&merged)).await?,
        published
    );
    assert_eq!(
        value(client.post(&compare).bearer_auth(OWNER).json(&historical)).await?,
        original
    );
    let merged_file = preview(&client, &compare, &merged, b"edited", "after").await?;
    assert_eq!(
        URL_SAFE_NO_PAD.decode(merged_file["content_base64"].as_str().unwrap())?,
        b"published later\n"
    );
    let reader = format!("cnp_{}", "b8".repeat(32));
    value(
        client
            .post(format!("http://{address}/api/accounts"))
            .bearer_auth(OWNER)
            .json(&json!({"name":"reader","token":reader,"scope":"read"})),
    )
    .await?;
    let access = format!("{repo}/collaborators/reader");
    value(
        client
            .put(&access)
            .bearer_auth(OWNER)
            .json(&json!({"role":"read"})),
    )
    .await?;
    assert_eq!(
        value(client.post(&compare).bearer_auth(&reader).json(&historical)).await?,
        original
    );
    let mut merged_patch = merged.clone();
    merged_patch["query"] = json!({"kind":"patch","path_base64":URL_SAFE_NO_PAD.encode(b"edited")});
    assert_eq!(
        value(
            client
                .post(&compare)
                .bearer_auth(&reader)
                .json(&merged_patch)
        )
        .await?["patch"],
        published_patch
    );
    let body = serde_json::to_vec(&merged_patch)?;
    let paused = paused_upload(
        address,
        "/api/repositories/history/pulls/1/comparison",
        &reader,
        body.len(),
    )
    .await?;
    status(
        client.delete(&access).bearer_auth(OWNER),
        StatusCode::NO_CONTENT,
    )
    .await?;
    finish_upload(paused, &body, 404).await?;
    status(
        client.post(&compare).bearer_auth(&reader).json(&historical),
        StatusCode::NOT_FOUND,
    )
    .await?;
    for target in [
        json!({"kind":"review","number":0}),
        json!({"kind":"merged","revision":current["target"]["revision"]}),
        json!({"kind":"review","number":1,"revision":current["target"]["revision"]}),
    ] {
        let mut invalid = merged.clone();
        invalid["target"] = target;
        status(
            client.post(&compare).bearer_auth(OWNER).json(&invalid),
            StatusCode::UNPROCESSABLE_ENTITY,
        )
        .await?;
    }
    server.shutdown().await?;
    let address = available_address().await?;
    let restored =
        CanopyServer::start(config(address, workspace.path().join("fresh")), store).await?;
    let api = format!("http://{address}/api/repositories/history/pulls/1");
    let compare = format!("{api}/comparison");
    for (input, expected) in [(&historical, &original), (&merged, &published)] {
        assert_eq!(
            value(client.post(&compare).bearer_auth(OWNER).json(input)).await?,
            *expected
        );
    }
    assert_eq!(
        patch(&client, &compare, &historical, b"edited").await?,
        original_patch
    );
    assert_eq!(
        patch(&client, &compare, &merged, b"edited").await?,
        published_patch
    );
    assert_eq!(
        preview(&client, &compare, &historical, b"edited", "after").await?,
        reviewed_file
    );
    assert_eq!(
        preview(&client, &compare, &merged, b"edited", "after").await?,
        merged_file
    );
    assert_eq!(
        value(
            client
                .post(format!("{api}/merge"))
                .bearer_auth(OWNER)
                .json(&merge_input)
        )
        .await?,
        result
    );
    status(
        client.post(&compare).bearer_auth(&reader).json(&merged),
        StatusCode::NOT_FOUND,
    )
    .await?;
    restored.shutdown().await?;
    Ok(())
}
