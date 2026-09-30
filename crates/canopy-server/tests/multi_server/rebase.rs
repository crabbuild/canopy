use super::candidates::{commit, file, git_input};
use super::merge::{
    AUTH, OWNER, Result, current, init, new_pull, oid, push, revision, status, value,
};
use super::*;
use reqwest::{Client, StatusCode};
use serde_json::{Value, json};

#[tokio::test(flavor = "multi_thread")]
async fn rebase_conflicts_limits_and_stale_publication_keep_branches_unchanged() -> Result {
    let workspace = tempfile::TempDir::new()?;
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let address = available_address().await?;
    let server = CanopyServer::start(
        config(address, workspace.path().join("node")),
        Arc::clone(&store),
    )
    .await?;
    let url = create_repository(address, "rebase").await?;
    let repo = format!("http://{address}/api/repositories/rebase");
    let client = Client::new();
    let repository = value(client.get(&repo).bearer_auth(OWNER)).await?["repository_id"].clone();
    let local = workspace.path().join("local");
    init(&local).await?;
    file(&local, "conflict", b"common\n", "Common").await?;
    let common = oid(&local, "HEAD").await?;
    let tree = oid(&local, "HEAD^{tree}").await?;
    file(&local, "conflict", b"base\n", "Base changed").await?;
    let base = oid(&local, "HEAD").await?;
    run_git(Some(&local), &["checkout", "-b", "source", &common]).await?;
    file(&local, "conflict", b"other\n", "Intermediate conflict").await?;
    file(&local, "conflict", b"common\n", "Undo conflicting change").await?;
    let conflict = oid(&local, "HEAD").await?;
    let merged = commit(&local, &tree, &[&common, &base], "Source merge").await?;
    let oversized = commit(&local, &tree, &[&common], &"m".repeat(65536)).await?;
    let mut long = common.clone();
    for index in 0..129 {
        long = commit(&local, &tree, &[&long], &format!("Change {index}")).await?;
    }
    let unrelated = commit(&local, &tree, &[], "Unrelated").await?;
    let source_body = format!(
        "tree {tree}\nparent {common}\nauthor Original <author@example.invalid> 1600000000 -0700\ncommitter Previous <previous@example.invalid> 1600000001 +0100\nencoding ISO-8859-1\ngpgsig -----BEGIN PGP SIGNATURE-----\n old signature\n -----END PGP SIGNATURE-----\nx-custom opaque\n continued\n\nOriginal message without newline: "
    );
    let mut raw = source_body.into_bytes();
    raw.push(0xe9);
    let signed = String::from_utf8(
        git_input(
            &local,
            &["hash-object", "-t", "commit", "-w", "--stdin"],
            &raw,
        )
        .await?,
    )?
    .trim()
    .to_owned();
    push(&local, &url, &[&format!("{base}:refs/heads/main")], true).await?;
    let mut saved = Vec::new();
    for (name, source, result) in [
        (
            "conflict",
            &conflict,
            json!({"state":"conflicted","paths_base64":["Y29uZmxpY3Q"]}),
        ),
        (
            "merged",
            &merged,
            json!({"state":"rebase_unavailable","reason":"merge_history"}),
        ),
        (
            "large",
            &oversized,
            json!({"state":"rebase_unavailable","reason":"limit"}),
        ),
        (
            "long",
            &long,
            json!({"state":"rebase_unavailable","reason":"limit"}),
        ),
        (
            "contained",
            &common,
            json!({"state":"rebase_unavailable","reason":"no_commits"}),
        ),
        ("unrelated", &unrelated, json!({"state":"unrelated"})),
    ] {
        let reference = format!("refs/heads/{name}");
        push(&local, &url, &[&format!("{source}:{reference}")], true).await?;
        let api = new_pull(&client, &repo, &repository, &reference, source, &base).await?;
        let initial = current(&client, &api).await?;
        let request = json!({"repository_id":repository,"id":uuid::Uuid::new_v4().to_string(),"revision":revision(&initial),"strategy":"rebase","message":""});
        let endpoint = format!("{api}/merge-candidates");
        let candidate = value(client.post(&endpoint).bearer_auth(OWNER).json(&request)).await?;
        assert_eq!(candidate["candidate"]["result"], result, "{name}");
        assert_eq!(candidate["fetch_ref"], Value::Null);
        let publish = json!({"repository_id":repository,"id":uuid::Uuid::new_v4().to_string(),"revision":revision(&initial),"strategy":"rebase","candidate_id":request["id"]});
        status(
            client
                .post(format!("{api}/merge"))
                .bearer_auth(OWNER)
                .json(&publish),
            StatusCode::CONFLICT,
        )
        .await?;
        assert_eq!(current(&client, &api).await?, initial);
        saved.push((initial["number"].clone(), request, candidate));
    }
    // A clean final branch diff cannot hide a conflict in an earlier replay.
    run_git(
        Some(&local),
        &["merge-tree", "--write-tree", &base, &conflict],
    )
    .await?;
    push(
        &local,
        &url,
        &[&format!("{signed}:refs/heads/signed")],
        true,
    )
    .await?;
    let api = new_pull(
        &client,
        &repo,
        &repository,
        "refs/heads/signed",
        &signed,
        &base,
    )
    .await?;
    let initial = current(&client, &api).await?;
    let request = json!({"repository_id":repository,"id":uuid::Uuid::new_v4().to_string(),"revision":revision(&initial),"strategy":"rebase","message":""});
    let endpoint = format!("{api}/merge-candidates");
    let mut invalid = request.clone();
    invalid["message"] = json!("Do not replace original messages");
    status(
        client.post(&endpoint).bearer_auth(OWNER).json(&invalid),
        StatusCode::UNPROCESSABLE_ENTITY,
    )
    .await?;
    let ready = value(client.post(&endpoint).bearer_auth(OWNER).json(&request)).await?;
    assert_eq!(ready["candidate"]["result"]["state"], "ready");
    let tip = ready["candidate"]["result"]["oid"].as_str().ok_or("tip")?;
    run_git(
        Some(&local),
        &[
            "-c",
            AUTH,
            "fetch",
            &url,
            ready["fetch_ref"].as_str().ok_or("ref")?,
        ],
    )
    .await?;
    let bytes = run_git(Some(&local), &["cat-file", "commit", tip]).await?;
    let mut expected = format!("tree {}\nparent {base}\nauthor Original <author@example.invalid> 1600000000 -0700\ncommitter canopy <canopy@users.canopy.invalid> {} +0000\nencoding ISO-8859-1\nx-custom opaque\n continued\n\nOriginal message without newline: ", oid(&local,"main^{tree}").await?, ready["candidate"]["created_at_ms"].as_i64().ok_or("time")? / 1000).into_bytes();
    expected.push(0xe9);
    assert_eq!(bytes, expected);
    let publication = json!({"repository_id":repository,"id":uuid::Uuid::new_v4().to_string(),"revision":revision(&initial),"strategy":"rebase","candidate_id":request["id"]});
    let moved = commit(&local, &tree, &[&signed], "Source moves").await?;
    push(&local, &url, &[&format!("{moved}:refs/heads/signed")], true).await?;
    push(
        &local,
        &url,
        &[&format!("+{signed}:refs/heads/signed")],
        true,
    )
    .await?;
    status(
        client
            .post(format!("{api}/merge"))
            .bearer_auth(OWNER)
            .json(&publication),
        StatusCode::CONFLICT,
    )
    .await?;
    assert_eq!(current(&client, &api).await?["base"]["oid"], base);
    saved.push((initial["number"].clone(), request, ready));
    server.shutdown().await?;
    let address = available_address().await?;
    let server =
        CanopyServer::start(config(address, workspace.path().join("restored")), store).await?;
    for (number, request, candidate) in saved {
        let endpoint =
            format!("http://{address}/api/repositories/rebase/pulls/{number}/merge-candidates");
        assert_eq!(
            value(client.post(&endpoint).bearer_auth(OWNER).json(&request)).await?,
            candidate
        );
    }
    server.shutdown().await?;
    Ok(())
}
