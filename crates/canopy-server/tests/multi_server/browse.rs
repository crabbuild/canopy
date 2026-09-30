use super::merge::{OWNER, Result, init, oid, push, status, value};
use super::*;
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use reqwest::{Client, StatusCode};
use serde_json::{Value, json};

async fn browse(client: &Client, api: &str, repository: &Value, query: Value) -> Result<Value> {
    Ok(value(
        client
            .post(api)
            .bearer_auth(OWNER)
            .json(&json!({"repository_id":repository,"query":query})),
    )
    .await?["view"]
        .clone())
}
#[tokio::test(flavor = "multi_thread")]
async fn browser_reads_exact_git_snapshots_with_pages_modes_and_recovery() -> Result {
    let workspace = tempfile::TempDir::new()?;
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let address = available_address().await?;
    let server = CanopyServer::start(
        config(address, workspace.path().join("node")),
        Arc::clone(&store),
    )
    .await?;
    let url = create_repository(address, "browse").await?;
    let repo = format!("http://{address}/api/repositories/browse");
    let api = format!("{repo}/browse");
    let client = Client::new();
    let repository = value(client.get(&repo).bearer_auth(OWNER)).await?["repository_id"].clone();
    let empty = browse(&client, &api, &repository, json!({"kind":"resolve"})).await?;
    assert_eq!(empty["resolved"]["oid"], Value::Null);
    let local = workspace.path().join("local");
    init(&local).await?;
    for n in 0..38 {
        tokio::fs::write(
            local.join(format!("file-{n:02}.txt")),
            format!("file {n}\n"),
        )
        .await?;
    }
    tokio::fs::create_dir(local.join("src")).await?;
    tokio::fs::write(
        local.join("src/code.html"),
        b"<script>window.pwned = true</script>\n",
    )
    .await?;
    tokio::fs::write(local.join("binary"), b"\0\xff\x01").await?;
    tokio::fs::write(local.join("large"), vec![b'x'; 1024 * 1024]).await?;
    run_git(Some(&local), &["add", "."]).await?;
    run_git(Some(&local), &["update-index", "--chmod=+x", "file-00.txt"]).await?;
    let target = oid(&local, ":file-01.txt").await?;
    let parent = oid(&local, "HEAD").await?;
    for entry in [
        format!("120000,{target},link"),
        format!("160000,{parent},submodule"),
    ] {
        run_git(
            Some(&local),
            &["update-index", "--add", "--cacheinfo", &entry],
        )
        .await?;
    }
    use tokio::io::AsyncWriteExt;
    let mut child = Command::new("git")
        .current_dir(&local)
        .args(["update-index", "-z", "--index-info"])
        .stdin(std::process::Stdio::piped())
        .spawn()?;
    let mut record = format!("100644 {target}\traw-").into_bytes();
    record.extend_from_slice(b"\xff\n\0");
    child
        .stdin
        .take()
        .ok_or("index stdin")?
        .write_all(&record)
        .await?;
    assert!(child.wait().await?.success());
    run_git(
        Some(&local),
        &["commit", "-m", "Tree <script> & Unicode 树"],
    )
    .await?;
    for n in 0..34 {
        run_git(
            Some(&local),
            &["commit", "--allow-empty", "-m", &format!("History {n}")],
        )
        .await?;
    }
    let first_parent = oid(&local, "HEAD").await?;
    let tree = oid(&local, "HEAD^{tree}").await?;
    let merge = String::from_utf8(
        run_git(
            Some(&local),
            &[
                "commit-tree",
                &tree,
                "-p",
                &first_parent,
                "-p",
                &parent,
                "-m",
                "Ordered merge parents",
            ],
        )
        .await?,
    )?;
    run_git(Some(&local), &["update-ref", "HEAD", merge.trim()]).await?;
    let commit = oid(&local, "HEAD").await?;
    run_git(Some(&local), &["tag", "-a", "v1", "-m", "Annotated tag"]).await?;
    push(
        &local,
        &url,
        &["HEAD:refs/heads/main", "refs/tags/v1"],
        true,
    )
    .await?;
    let resolved = browse(&client, &api, &repository, json!({"kind":"resolve"})).await?;
    assert_eq!(resolved["resolved"]["oid"], commit);
    let tree_request = json!({"kind":"tree","commit":commit,"path_base64":""});
    let first = browse(&client, &api, &repository, tree_request.clone()).await?;
    assert_eq!(
        first["tree"]["commit"]["parents"],
        json!([first_parent, parent])
    );
    assert_eq!(first["tree"]["entries"].as_array().unwrap().len(), 32);
    let mut next = tree_request.clone();
    next["after"] = first["tree"]["next_after"].clone();
    let second = browse(&client, &api, &repository, next).await?;
    assert_eq!(second["tree"]["next_after"], Value::Null);
    let entries: Vec<_> = first["tree"]["entries"]
        .as_array()
        .unwrap()
        .iter()
        .chain(second["tree"]["entries"].as_array().unwrap())
        .collect();
    assert_eq!(entries.len(), 44);
    assert_eq!(
        entries.iter().find(|e| e["name"] == "file-00.txt").unwrap()["mode"],
        "100755"
    );
    assert_eq!(
        entries.iter().find(|e| e["name"] == "link").unwrap()["kind"],
        "symlink"
    );
    assert_eq!(
        entries.iter().find(|e| e["name"] == "submodule").unwrap()["kind"],
        "gitlink"
    );
    let raw = entries
        .iter()
        .find(|e| e["name"] == Value::Null)
        .ok_or("raw filename")?;
    assert_eq!(
        URL_SAFE_NO_PAD.decode(raw["name_base64"].as_str().unwrap())?,
        b"raw-\xff\n"
    );
    let folder = browse(
        &client,
        &api,
        &repository,
        json!({"kind":"tree","commit":commit,"path_base64":URL_SAFE_NO_PAD.encode(b"src")}),
    )
    .await?;
    assert_eq!(folder["tree"]["entries"][0]["name"], "code.html");
    for (name, expected) in [
        (
            "src/code.html",
            b"<script>window.pwned = true</script>\n".as_slice(),
        ),
        ("binary", b"\0\xff\x01"),
        ("link", b"file 1\n"),
    ] {
        let file = browse(
            &client,
            &api,
            &repository,
            json!({"kind":"file","commit":commit,"path_base64":URL_SAFE_NO_PAD.encode(name)}),
        )
        .await?;
        assert_eq!(
            URL_SAFE_NO_PAD.decode(file["file"]["content_base64"].as_str().unwrap())?,
            expected
        );
        assert_eq!(
            file["file"]["oid"],
            oid(&local, &format!("HEAD:{name}")).await?
        );
    }
    let large = browse(
        &client,
        &api,
        &repository,
        json!({"kind":"file","commit":commit,"path_base64":URL_SAFE_NO_PAD.encode(b"large")}),
    )
    .await?;
    assert_eq!(large["file"]["content_status"], "too_large");
    assert_eq!(large["file"]["size"], 1024 * 1024);
    let gitlink = browse(
        &client,
        &api,
        &repository,
        json!({"kind":"file","commit":commit,"path_base64":URL_SAFE_NO_PAD.encode(b"submodule")}),
    )
    .await?;
    assert_eq!(gitlink["file"]["content_status"], "gitlink");
    let tag = browse(
        &client,
        &api,
        &repository,
        json!({"kind":"resolve","reference":"refs/tags/v1"}),
    )
    .await?;
    let tagged = browse(
        &client,
        &api,
        &repository,
        json!({"kind":"tree","commit":tag["resolved"]["oid"],"path_base64":""}),
    )
    .await?;
    assert_eq!(tagged, first);
    let history = browse(
        &client,
        &api,
        &repository,
        json!({"kind":"history","commit":commit}),
    )
    .await?;
    let continuation = browse(
        &client,
        &api,
        &repository,
        json!({"kind":"history","commit":history["history"]["next_commit"]}),
    )
    .await?;
    let actual: Vec<_> = history["history"]["commits"]
        .as_array()
        .unwrap()
        .iter()
        .chain(continuation["history"]["commits"].as_array().unwrap())
        .map(|c| c["oid"].as_str().unwrap())
        .collect();
    let native =
        String::from_utf8(run_git(Some(&local), &["log", "--first-parent", "--format=%H"]).await?)?;
    assert_eq!(actual, native.lines().collect::<Vec<_>>());
    let refs = browse(&client, &api, &repository, json!({"kind":"refs"})).await?;
    run_git(Some(&local), &["commit", "--allow-empty", "-m", "Moved"]).await?;
    push(&local, &url, &["HEAD:refs/heads/main"], true).await?;
    status(client.post(&api).bearer_auth(OWNER).json(&json!({"repository_id":repository,"query":{"kind":"refs","after":"refs/heads/main","generation":refs["refs"]["generation"]}})),StatusCode::CONFLICT).await?;
    assert_eq!(
        browse(&client, &api, &repository, tree_request.clone()).await?,
        first
    );
    let reader = format!("cnp_{}", "c3".repeat(32));
    value(
        client
            .post(format!("http://{address}/api/accounts"))
            .bearer_auth(OWNER)
            .json(&json!({"name":"viewer","token":reader,"scope":"read"})),
    )
    .await?;
    value(
        client
            .put(format!("{repo}/collaborators/viewer"))
            .bearer_auth(OWNER)
            .json(&json!({"role":"read"})),
    )
    .await?;
    let body = serde_json::to_vec(&json!({"repository_id":repository,"query":tree_request}))?;
    let paused = super::tokens::paused_upload(
        address,
        "/api/repositories/browse/browse",
        &reader,
        body.len(),
    )
    .await?;
    status(
        client
            .delete(format!("{repo}/collaborators/viewer"))
            .bearer_auth(OWNER),
        StatusCode::NO_CONTENT,
    )
    .await?;
    super::tokens::finish_upload(paused, &body, 404).await?;
    for query in [
        json!({"kind":"tree","commit":commit,"path_base64":URL_SAFE_NO_PAD.encode(b"../src")}),
        json!({"kind":"file","commit":commit,"path_base64":""}),
        json!({"kind":"refs","after":"refs/heads/main"}),
    ] {
        status(
            client
                .post(&api)
                .bearer_auth(OWNER)
                .json(&json!({"repository_id":repository,"query":query})),
            StatusCode::UNPROCESSABLE_ENTITY,
        )
        .await?;
    }
    let web = client.get(format!("http://{address}/")).send().await?;
    assert_eq!(web.status(), StatusCode::OK);
    assert!(
        web.headers()["content-security-policy"]
            .to_str()?
            .contains("frame-ancestors 'none'")
    );
    let response = client
        .post(&api)
        .bearer_auth(OWNER)
        .json(&json!({"repository_id":repository,"query":tree_request}))
        .send()
        .await?;
    assert_eq!(response.headers()["cache-control"], "no-store");
    server.shutdown().await?;
    let address = available_address().await?;
    let restored =
        CanopyServer::start(config(address, workspace.path().join("restored")), store).await?;
    let api = format!("http://{address}/api/repositories/browse/browse");
    assert_eq!(
        browse(&client, &api, &repository, tree_request).await?,
        first
    );
    assert_eq!(
        browse(
            &client,
            &api,
            &repository,
            json!({"kind":"history","commit":commit})
        )
        .await?,
        history
    );
    restored.shutdown().await?;
    Ok(())
}
