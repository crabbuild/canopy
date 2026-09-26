use super::*;

#[tokio::test(flavor = "multi_thread")]
async fn line_threads_bind_verified_hunks_and_survive_ref_loss_with_authorized_retries() -> Result {
    let workspace = tempfile::TempDir::new()?;
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let address = available_address().await?;
    let server = CanopyServer::start(
        config(address, workspace.path().join("first")),
        Arc::clone(&store),
    )
    .await?;
    let url = create_repository(address, "threads").await?;
    let repo = format!("http://{address}/api/repositories/threads");
    let api = format!("{repo}/pulls/1");
    let threads = format!("{api}/threads");
    let client = Client::new();
    let mut tokens = Vec::new();
    for (i, (account, scope)) in [
        ("commenter", "write"),
        ("other", "write"),
        ("viewer", "read"),
    ]
    .into_iter()
    .enumerate()
    {
        let token = format!("cnp_{:064x}", i + 51);
        value(
            client
                .post(format!("http://{address}/api/accounts"))
                .bearer_auth(OWNER)
                .json(&json!({"name":account,"token":token,"scope":scope})),
        )
        .await?;
        value(
            client
                .put(format!("{repo}/collaborators/{account}"))
                .bearer_auth(OWNER)
                .json(&json!({"role":"read"})),
        )
        .await?;
        tokens.push(token);
    }
    let (author, other, viewer) = (&tokens[0], &tokens[1], &tokens[2]);
    let local = workspace.path().join("local");
    init(&local).await?;
    let original: String = (0..30).map(|n| format!("line {n}\n")).collect();
    tokio::fs::write(local.join("file"), &original).await?;
    tokio::fs::write(local.join("deleted"), b"gone\n").await?;
    run_git(Some(&local), &["add", "."]).await?;
    run_git(Some(&local), &["commit", "-m", "Before"]).await?;
    let base = oid(&local, "HEAD").await?;
    push(&local, &url, "HEAD:refs/heads/main").await?;
    tokio::fs::write(
        local.join("file"),
        original.replace("line 10\n", "changed\n"),
    )
    .await?;
    tokio::fs::remove_file(local.join("deleted")).await?;
    tokio::fs::write(local.join("binary"), b"\0\xff").await?;
    run_git(Some(&local), &["add", "-A"]).await?;
    run_git(Some(&local), &["commit", "-m", "After"]).await?;
    let source = oid(&local, "HEAD").await?;
    push(&local, &url, "HEAD:refs/heads/feature").await?;
    open(&client, &repo, &source, &base).await?;
    let current = request(&client, &repo).await?;
    let input = json!({"repository_id":current["repository_id"],"id":uuid::Uuid::new_v4().to_string(),"target":current["target"],"path_base64":URL_SAFE_NO_PAD.encode("file"),"side":"after","line":11,"body":"Discuss <script>literal</script>"});
    status(
        client.post(&threads).bearer_auth(viewer).json(&input),
        StatusCode::FORBIDDEN,
    )
    .await?;
    for (key, val) in [
        ("line", json!(0)),
        ("line", json!(1)),
        ("line", json!(20001)),
        ("side", json!("invalid")),
        ("path_base64", json!(URL_SAFE_NO_PAD.encode("../file"))),
        ("path_base64", json!(URL_SAFE_NO_PAD.encode("binary"))),
        ("body", json!("  ")),
    ] {
        let mut bad = input.clone();
        bad[key] = val;
        status(
            client.post(&threads).bearer_auth(author).json(&bad),
            StatusCode::UNPROCESSABLE_ENTITY,
        )
        .await?;
    }
    assert_eq!(
        value(client.get(&threads).bearer_auth(OWNER)).await?["threads"],
        json!([])
    );
    let (a, b) = tokio::join!(
        value(client.post(&threads).bearer_auth(author).json(&input)),
        value(client.post(&threads).bearer_auth(author).json(&input))
    );
    assert_eq!(a?, b?);
    let thread_api = format!("{threads}/1");
    let first = value(client.get(&thread_api).bearer_auth(viewer)).await?;
    assert_eq!(
        first["thread"]["anchor"]["revision"],
        current["target"]["revision"]
    );
    assert_eq!(
        first["thread"]["anchor"]["blob_oid"],
        oid(&local, "HEAD:file").await?
    );
    let review = value(client.post(format!("{api}/reviews")).bearer_auth(OWNER).json(&json!({"repository_id":current["repository_id"],"id":uuid::Uuid::new_v4().to_string(),"revision":current["target"]["revision"],"kind":"comment","body":"Review snapshot"}))).await?;
    let mut removed = input.clone();
    removed["id"] = json!(uuid::Uuid::new_v4().to_string());
    removed["side"] = json!("before");
    removed["line"] = json!(1);
    removed["path_base64"] = json!(URL_SAFE_NO_PAD.encode("deleted"));
    assert_eq!(
        value(client.post(&threads).bearer_auth(author).json(&removed)).await?["number"],
        2
    );
    let resolve =
        json!({"repository_id":current["repository_id"],"expected_version":1,"resolved":true});
    status(
        client.put(&thread_api).bearer_auth(other).json(&resolve),
        StatusCode::FORBIDDEN,
    )
    .await?;
    status(
        client.put(&thread_api).bearer_auth(author).json(&resolve),
        StatusCode::NO_CONTENT,
    )
    .await?;
    status(
        client.put(&thread_api).bearer_auth(author).json(&resolve),
        StatusCode::CONFLICT,
    )
    .await?;
    let reopen =
        json!({"repository_id":current["repository_id"],"expected_version":2,"resolved":false});
    status(
        client.put(&thread_api).bearer_auth(OWNER).json(&reopen),
        StatusCode::NO_CONTENT,
    )
    .await?;
    let reply_api = format!("{thread_api}/comments");
    let reply = json!({"repository_id":current["repository_id"],"id":uuid::Uuid::new_v4().to_string(),"body":"A reply"});
    let replied = value(client.post(&reply_api).bearer_auth(other).json(&reply)).await?;
    assert_eq!(
        value(client.post(&reply_api).bearer_auth(other).json(&reply)).await?,
        replied
    );
    let mut altered = reply.clone();
    altered["body"] = json!("Changed identity");
    status(
        client.post(&reply_api).bearer_auth(other).json(&altered),
        StatusCode::CONFLICT,
    )
    .await?;
    for _ in 0..16 {
        let mut next = reply.clone();
        next["id"] = json!(uuid::Uuid::new_v4().to_string());
        value(client.post(&reply_api).bearer_auth(other).json(&next)).await?;
    }
    let page = value(client.get(&reply_api).bearer_auth(viewer)).await?;
    assert_eq!(page["comments"].as_array().unwrap().len(), 16);
    let last = value(
        client
            .get(format!("{reply_api}?after={}", page["next_after"]))
            .bearer_auth(viewer),
    )
    .await?;
    assert_eq!(last["comments"].as_array().unwrap().len(), 1);
    // A different revision cannot retarget existing discussions or their retries.
    tokio::fs::write(local.join("file"), b"follow-up\n").await?;
    run_git(Some(&local), &["commit", "-am", "Later"]).await?;
    push(&local, &url, "HEAD:refs/heads/feature").await?;
    assert_eq!(
        value(client.post(&threads).bearer_auth(author).json(&input)).await?["number"],
        1
    );
    let mut stale = input.clone();
    stale["id"] = json!(uuid::Uuid::new_v4().to_string());
    status(
        client.post(&threads).bearer_auth(author).json(&stale),
        StatusCode::CONFLICT,
    )
    .await?;
    let now = request(&client, &repo).await?;
    value(client.post(format!("{api}/merge")).bearer_auth(OWNER).json(&json!({"repository_id":current["repository_id"],"id":uuid::Uuid::new_v4().to_string(),"revision":now["target"]["revision"],"strategy":"fast_forward"}))).await?;
    push(&local, &url, ":refs/heads/feature").await?;
    push(&local, &url, ":refs/heads/main").await?;
    for target in [
        json!({"kind":"review","number":review["number"]}),
        json!({"kind":"merged"}),
    ] {
        let mut historical = input.clone();
        historical["id"] = json!(uuid::Uuid::new_v4().to_string());
        historical["target"] = target;
        if historical["target"]["kind"] == "merged" {
            historical["line"] = json!(1);
        }
        value(client.post(&threads).bearer_auth(author).json(&historical)).await?;
    }
    for _ in 0..13 {
        let mut next = input.clone();
        next["id"] = json!(uuid::Uuid::new_v4().to_string());
        next["target"] = json!({"kind":"review","number":review["number"]});
        value(client.post(&threads).bearer_auth(author).json(&next)).await?;
    }
    let thread_page = value(client.get(&threads).bearer_auth(viewer)).await?;
    assert_eq!(thread_page["threads"].as_array().unwrap().len(), 16);
    let remaining = value(
        client
            .get(format!("{threads}?after={}", thread_page["next_after"]))
            .bearer_auth(viewer),
    )
    .await?;
    assert_eq!(remaining["threads"].as_array().unwrap().len(), 1);
    let anchored = json!({"repository_id":current["repository_id"],"target":{"kind":"thread","number":1},"query":{"kind":"patch","path_base64":URL_SAFE_NO_PAD.encode("file")}});
    let patch = value(
        client
            .post(format!("{api}/comparison"))
            .bearer_auth(viewer)
            .json(&anchored),
    )
    .await?;
    assert_eq!(patch["patch"]["revision"], current["target"]["revision"]);
    status(
        client
            .get(format!("{repo}/pulls/2/threads/1"))
            .bearer_auth(OWNER),
        StatusCode::NOT_FOUND,
    )
    .await?;
    status(
        client
            .post(format!("{repo}/pulls/2/comparison"))
            .bearer_auth(OWNER)
            .json(&anchored),
        StatusCode::NOT_FOUND,
    )
    .await?;
    let body = serde_json::to_vec(&reply)?;
    let paused = paused_upload(
        address,
        "/api/repositories/threads/pulls/1/threads/1/comments",
        other,
        body.len(),
    )
    .await?;
    status(
        client
            .delete(format!("{repo}/collaborators/other"))
            .bearer_auth(OWNER),
        StatusCode::NO_CONTENT,
    )
    .await?;
    finish_upload(paused, &body, 404).await?;
    status(
        client
            .delete(format!("{repo}/collaborators/commenter"))
            .bearer_auth(OWNER),
        StatusCode::NO_CONTENT,
    )
    .await?;
    status(
        client.post(&threads).bearer_auth(author).json(&input),
        StatusCode::NOT_FOUND,
    )
    .await?;
    let records = value(client.get(&threads).bearer_auth(OWNER)).await?;
    let thread = value(client.get(&thread_api).bearer_auth(OWNER)).await?;
    assert_eq!(thread["thread"]["version"], 3);
    server.shutdown().await?;
    let address = available_address().await?;
    let restored =
        CanopyServer::start(config(address, workspace.path().join("fresh")), store).await?;
    let api = format!("http://{address}/api/repositories/threads/pulls/1");
    assert_eq!(
        value(client.get(format!("{api}/threads")).bearer_auth(OWNER)).await?,
        records
    );
    assert_eq!(
        value(client.get(format!("{api}/threads/1")).bearer_auth(OWNER)).await?,
        thread
    );
    assert_eq!(
        value(
            client
                .get(format!("{api}/threads/1/comments"))
                .bearer_auth(OWNER)
        )
        .await?,
        page
    );
    assert_eq!(
        value(
            client
                .post(format!("{api}/comparison"))
                .bearer_auth(viewer)
                .json(&anchored)
        )
        .await?,
        patch
    );
    status(
        client
            .post(format!("{api}/threads"))
            .bearer_auth(author)
            .json(&input),
        StatusCode::NOT_FOUND,
    )
    .await?;
    restored.shutdown().await?;
    Ok(())
}
