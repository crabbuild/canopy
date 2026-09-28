use super::tokens::{finish_upload, paused_upload};
use super::*;
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use reqwest::{Client, StatusCode};
use serde_json::{Value, json};
use std::collections::BTreeMap;

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
async fn oid(local: &Path, revision: &str) -> Result<String> {
    Ok(
        String::from_utf8(run_git(Some(local), &["rev-parse", revision]).await?)?
            .trim()
            .into(),
    )
}
async fn init(local: &Path) -> Result {
    run_git(None, &["init", "-b", "main", path_str(local)?]).await?;
    run_git(Some(local), &["config", "user.name", "Comparison Test"]).await?;
    run_git(
        Some(local),
        &["config", "user.email", "test@example.invalid"],
    )
    .await?;
    Ok(())
}
async fn push(local: &Path, url: &str, reference: &str) -> Result {
    run_git(Some(local), &["-c", AUTH, "push", url, reference]).await?;
    Ok(())
}
async fn request(client: &Client, repo: &str) -> Result<Value> {
    let pull = value(client.get(format!("{repo}/pulls/1")).bearer_auth(OWNER)).await?;
    let p = &pull["pull"];
    Ok(
        json!({"repository_id":pull["repository_id"],"target":{"kind":"current","revision":{
        "pull_version":p["version"],"source_oid":p["source"]["oid"],"source_version":p["source"]["version"],
        "base_oid":p["base"]["oid"],"base_version":p["base"]["version"]}},"query":{"kind":"files"}}),
    )
}
async fn open(client: &Client, repo: &str, source: &str, base: &str) -> Result {
    let repository = value(client.get(repo).bearer_auth(OWNER)).await?["repository_id"].clone();
    value(client.post(format!("{repo}/pulls")).bearer_auth(OWNER).json(&json!({
        "repository_id":repository,"id":uuid::Uuid::new_v4().to_string(),"title":"Compare","body":"","draft":false,
        "source_ref":"refs/heads/feature","source_oid":source,"base_ref":"refs/heads/main","base_oid":base
    }))).await?;
    Ok(())
}
async fn files(
    client: &Client,
    api: &str,
    token: &str,
    input: &Value,
    merge_base: &str,
) -> Result<BTreeMap<Vec<u8>, Value>> {
    let mut input = input.clone();
    let mut files = BTreeMap::new();
    let mut previous: Option<Vec<u8>> = None;
    loop {
        let page = value(client.post(api).bearer_auth(token).json(&input)).await?;
        assert_eq!(page["comparison"]["merge_base"], merge_base);
        assert_eq!(page["comparison"]["revision"], input["target"]["revision"]);
        let rows = page["comparison"]["files"].as_array().unwrap();
        assert!(rows.len() <= 32);
        for file in rows {
            let path = URL_SAFE_NO_PAD.decode(file["path_base64"].as_str().unwrap())?;
            assert!(previous.as_ref().is_none_or(|p| p < &path));
            assert_eq!(file["path"], json!(String::from_utf8(path.clone()).ok()));
            previous = Some(path.clone());
            assert!(
                files
                    .insert(path, json!({"before":file["before"],"after":file["after"]}))
                    .is_none()
            );
        }
        let next = &page["comparison"]["next_after"];
        if next.is_null() {
            break;
        }
        assert_eq!(rows.len(), 32);
        assert_eq!(next, &rows.last().unwrap()["path_base64"]);
        input["query"]["after"] = next.clone();
    }
    Ok(files)
}
async fn native_changes(
    local: &Path,
    before: &str,
    after: &str,
) -> Result<BTreeMap<Vec<u8>, Value>> {
    let raw = run_git(
        Some(local),
        &[
            "diff-tree",
            "--no-commit-id",
            "--raw",
            "-r",
            "-z",
            "--no-renames",
            before,
            after,
        ],
    )
    .await?;
    let fields: Vec<_> = raw.split(|b| *b == 0).filter(|v| !v.is_empty()).collect();
    let mut files = BTreeMap::new();
    for pair in fields.as_chunks::<2>().0 {
        let header: Vec<_> = std::str::from_utf8(pair[0])?
            .trim_start_matches(':')
            .split(' ')
            .collect();
        let entry = |mode: &str, oid: &str| {
            if mode == "000000" {
                Value::Null
            } else {
                json!({"mode":mode,"oid":oid})
            }
        };
        files.insert(
            pair[1].to_vec(),
            json!({"before":entry(header[0],header[2]),"after":entry(header[1],header[3])}),
        );
    }
    Ok(files)
}
async fn preview(
    client: &Client,
    api: &str,
    input: &Value,
    path: &[u8],
    side: &str,
) -> Result<Value> {
    let mut input = input.clone();
    input["query"] = json!({"kind":"file","path_base64":URL_SAFE_NO_PAD.encode(path),"side":side});
    Ok(value(client.post(api).bearer_auth(OWNER).json(&input)).await?["file"].clone())
}

async fn patch(client: &Client, api: &str, input: &Value, path: &[u8]) -> Result<Value> {
    let mut input = input.clone();
    input["query"] = json!({"kind":"patch","path_base64":URL_SAFE_NO_PAD.encode(path)});
    Ok(value(client.post(api).bearer_auth(OWNER).json(&input)).await?["patch"].clone())
}

#[tokio::test(flavor = "multi_thread")]
async fn comparison_matches_git_and_preserves_exact_views_across_recovery() -> Result {
    let workspace = tempfile::TempDir::new()?;
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let address = available_address().await?;
    let server = CanopyServer::start(
        config(address, workspace.path().join("first")),
        Arc::clone(&store),
    )
    .await?;
    let url = create_repository(address, "compare").await?;
    let repo = format!("http://{address}/api/repositories/compare");
    let api = format!("{repo}/pulls/1/comparison");
    let client = Client::new();
    let local = workspace.path().join("local");
    init(&local).await?;
    for name in ["edited", "deleted", "renamed", "mode", "file-to-dir"] {
        tokio::fs::write(local.join(name), b"before\n").await?;
    }
    tokio::fs::create_dir(local.join("dir-to-file")).await?;
    tokio::fs::write(local.join("dir-to-file/child"), b"before\n").await?;
    run_git(Some(&local), &["add", "."]).await?;
    run_git(Some(&local), &["commit", "-m", "Common"]).await?;
    let common = oid(&local, "HEAD").await?;
    tokio::fs::write(local.join("base-only"), b"base change\n").await?;
    run_git(Some(&local), &["add", "."]).await?;
    run_git(Some(&local), &["commit", "-m", "Base advanced"]).await?;
    let base = oid(&local, "HEAD").await?;
    push(&local, &url, "HEAD:refs/heads/main").await?;
    run_git(Some(&local), &["checkout", "-b", "feature", &common]).await?;
    tokio::fs::write(local.join("edited"), b"after\n").await?;
    tokio::fs::remove_file(local.join("deleted")).await?;
    tokio::fs::rename(local.join("renamed"), local.join("new-name")).await?;
    tokio::fs::remove_file(local.join("file-to-dir")).await?;
    tokio::fs::create_dir(local.join("file-to-dir")).await?;
    tokio::fs::write(local.join("file-to-dir/child"), b"child\n").await?;
    tokio::fs::remove_dir_all(local.join("dir-to-file")).await?;
    tokio::fs::write(local.join("dir-to-file"), b"file\n").await?;
    tokio::fs::write(local.join("binary"), b"\0\xff\x01").await?;
    tokio::fs::write(local.join("large"), vec![0x61; 1024 * 1024]).await?;
    for n in 0..34 {
        tokio::fs::write(local.join(format!("page-{n:02}")), b"page\n").await?;
    }
    run_git(Some(&local), &["add", "-A"]).await?;
    run_git(Some(&local), &["update-index", "--chmod=+x", "mode"]).await?;
    let target = oid(&local, ":new-name").await?;
    run_git(
        Some(&local),
        &[
            "update-index",
            "--add",
            "--cacheinfo",
            &format!("120000,{target},link"),
        ],
    )
    .await?;
    run_git(
        Some(&local),
        &[
            "update-index",
            "--add",
            "--cacheinfo",
            &format!("160000,{common},submodule"),
        ],
    )
    .await?;
    // Index plumbing preserves arbitrary Git names even on filesystems that
    // reject non-UTF-8 directory entries.
    use tokio::io::AsyncWriteExt;
    let mut child = Command::new("git")
        .current_dir(&local)
        .args(["update-index", "-z", "--index-info"])
        .stdin(std::process::Stdio::piped())
        .spawn()?;
    let mut record = format!("100644 {target}\traw-").into_bytes();
    record.extend_from_slice(b"\xff\0");
    child
        .stdin
        .take()
        .ok_or("index stdin missing")?
        .write_all(&record)
        .await?;
    assert!(child.wait().await?.success());
    run_git(Some(&local), &["commit", "-m", "Source changes"]).await?;
    let source = oid(&local, "HEAD").await?;
    push(&local, &url, "HEAD:refs/heads/feature").await?;
    open(&client, &repo, &source, &base).await?;
    let input = request(&client, &repo).await?;
    let native_base = run_git(Some(&local), &["merge-base", "--all", &base, &source]).await?;
    assert_eq!(String::from_utf8(native_base)?.trim(), common);
    let expected = native_changes(&local, &common, &source).await?;
    assert!(!expected.contains_key(b"base-only".as_slice()));
    assert!(expected.len() > 32);
    assert_eq!(
        files(&client, &api, OWNER, &input, &common).await?,
        expected
    );
    for (path, entries) in &expected {
        let result = patch(&client, &api, &input, path).await?;
        assert_eq!(result["before"], entries["before"]);
        assert_eq!(result["after"], entries["after"]);
        assert_eq!(result["path"], json!(String::from_utf8(path.clone()).ok()));
        let state = match path.as_slice() {
            b"binary" => "binary",
            b"large" => "too_large",
            b"submodule" => "gitlink",
            _ => "text",
        };
        assert_eq!(result["status"], state);
        if state != "text" || path == b"mode" {
            assert_eq!(result["hunks"], json!([]));
        }
    }
    for (path, side, bytes) in [
        (b"edited".as_slice(), "before", b"before\n".as_slice()),
        (b"edited", "after", b"after\n"),
        (b"binary", "after", b"\0\xff\x01"),
        (b"link", "after", b"before\n"),
        (b"deleted", "before", b"before\n"),
    ] {
        let file = preview(&client, &api, &input, path, side).await?;
        assert_eq!(file["content_status"], "included");
        assert_eq!(
            URL_SAFE_NO_PAD.decode(file["content_base64"].as_str().unwrap())?,
            bytes
        );
    }
    for (path, state) in [
        (b"large".as_slice(), "too_large"),
        (b"submodule", "gitlink"),
    ] {
        let file = preview(&client, &api, &input, path, "after").await?;
        assert_eq!(file["content_status"], state);
        assert!(file["content_base64"].is_null());
    }
    for (path, code) in [
        ("deleted", StatusCode::NOT_FOUND),
        ("link/child", StatusCode::NOT_FOUND),
        ("../edited", StatusCode::UNPROCESSABLE_ENTITY),
        ("dir-to-file/", StatusCode::UNPROCESSABLE_ENTITY),
    ] {
        let mut bad = input.clone();
        bad["query"] =
            json!({"kind":"file","path_base64":URL_SAFE_NO_PAD.encode(path),"side":"after"});
        status(client.post(&api).bearer_auth(OWNER).json(&bad), code).await?;
    }
    let mut bad = input.clone();
    bad["query"]["after"] = json!("not+base64");
    status(
        client.post(&api).bearer_auth(OWNER).json(&bad),
        StatusCode::UNPROCESSABLE_ENTITY,
    )
    .await?;
    bad = input.clone();
    bad["repository_id"] = json!(uuid::Uuid::new_v4().to_string());
    status(
        client.post(&api).bearer_auth(OWNER).json(&bad),
        StatusCode::CONFLICT,
    )
    .await?;
    status(client.post(&api).json(&input), StatusCode::UNAUTHORIZED).await?;
    let reader = format!("cnp_{}", "39".repeat(32));
    value(
        client
            .post(format!("http://{address}/api/accounts"))
            .bearer_auth(OWNER)
            .json(&json!({"name":"reader","token":reader,"scope":"read"})),
    )
    .await?;
    let collaborator = format!("{repo}/collaborators/reader");
    status(
        client.post(&api).bearer_auth(&reader).json(&input),
        StatusCode::NOT_FOUND,
    )
    .await?;
    value(
        client
            .put(&collaborator)
            .bearer_auth(OWNER)
            .json(&json!({"role":"read"})),
    )
    .await?;
    assert_eq!(
        files(&client, &api, &reader, &input, &common).await?,
        expected
    );
    let body = serde_json::to_vec(&input)?;
    let paused = paused_upload(
        address,
        "/api/repositories/compare/pulls/1/comparison",
        &reader,
        body.len(),
    )
    .await?;
    status(
        client.delete(&collaborator).bearer_auth(OWNER),
        StatusCode::NO_CONTENT,
    )
    .await?;
    finish_upload(paused, &body, 404).await?;
    // Returning to the same source OID must not resurrect the old view.
    run_git(Some(&local), &["commit", "--allow-empty", "-m", "Movement"]).await?;
    push(&local, &url, "HEAD:refs/heads/feature").await?;
    status(
        client.post(&api).bearer_auth(OWNER).json(&input),
        StatusCode::CONFLICT,
    )
    .await?;
    push(&local, &url, &format!("+{source}:refs/heads/feature")).await?;
    status(
        client.post(&api).bearer_auth(OWNER).json(&input),
        StatusCode::CONFLICT,
    )
    .await?;
    let current = request(&client, &repo).await?;
    let snapshot = value(client.post(&api).bearer_auth(OWNER).json(&current)).await?;
    let file_snapshot = preview(&client, &api, &current, b"edited", "after").await?;
    value(
        client
            .patch(&repo)
            .bearer_auth(OWNER)
            .json(&json!({"name":"renamed","repository_id":current["repository_id"]})),
    )
    .await?;
    server.shutdown().await?;
    let address = available_address().await?;
    let restored =
        CanopyServer::start(config(address, workspace.path().join("restored")), store).await?;
    let api = format!("http://{address}/api/repositories/renamed/pulls/1/comparison");
    assert_eq!(
        value(client.post(&api).bearer_auth(OWNER).json(&current)).await?,
        snapshot
    );
    assert_eq!(
        preview(&client, &api, &current, b"edited", "after").await?,
        file_snapshot
    );
    assert_eq!(
        files(&client, &api, OWNER, &current, &common).await?,
        expected
    );
    restored.shutdown().await?;
    Ok(())
}

async fn commit(local: &Path, tree: &str, parents: &[String], message: &str) -> Result<String> {
    let mut args = vec!["commit-tree", tree, "-m", message];
    for parent in parents {
        args.extend(["-p", parent]);
    }
    Ok(String::from_utf8(run_git(Some(local), &args).await?)?
        .trim()
        .into())
}

#[tokio::test(flavor = "multi_thread")]
async fn comparison_merge_bases_match_git_for_wide_unrelated_and_crisscross_histories() -> Result {
    let workspace = tempfile::TempDir::new()?;
    let address = available_address().await?;
    let server = CanopyServer::start(
        config(address, workspace.path().join("node")),
        Arc::new(InMemory::new()),
    )
    .await?;
    let url = create_repository(address, "graph").await?;
    let repo = format!("http://{address}/api/repositories/graph");
    let api = format!("{repo}/pulls/1/comparison");
    let client = Client::new();
    let local = workspace.path().join("local");
    init(&local).await?;
    run_git(Some(&local), &["commit", "--allow-empty", "-m", "Root"]).await?;
    let tree = oid(&local, "HEAD^{tree}").await?;
    let mut parents = Vec::new();
    for n in 0..600 {
        parents.push(commit(&local, &tree, &[], &format!("Root {n}")).await?);
    }
    let source = commit(&local, &tree, &parents, "Wide merge").await?;
    // Choose the last lexicographic parent so a truncated first SQL page fails.
    let base = parents.iter().max().unwrap();
    push(&local, &url, &format!("{base}:refs/heads/main")).await?;
    push(&local, &url, &format!("{source}:refs/heads/feature")).await?;
    open(&client, &repo, &source, base).await?;
    let native = run_git(Some(&local), &["merge-base", "--all", base, &source]).await?;
    assert_eq!(String::from_utf8(native)?.trim(), base);
    let input = request(&client, &repo).await?;
    assert!(files(&client, &api, OWNER, &input, base).await?.is_empty());
    let left = commit(&local, &tree, &parents[..2], "Left").await?;
    let right = commit(
        &local,
        &tree,
        &[parents[1].clone(), parents[0].clone()],
        "Right",
    )
    .await?;
    push(&local, &url, &format!("+{left}:refs/heads/main")).await?;
    push(&local, &url, &format!("+{right}:refs/heads/feature")).await?;
    let native = run_git(Some(&local), &["merge-base", "--all", &left, &right]).await?;
    assert_eq!(String::from_utf8(native)?.lines().count(), 2);
    let input = request(&client, &repo).await?;
    let ambiguous = client
        .post(&api)
        .bearer_auth(OWNER)
        .json(&input)
        .send()
        .await?;
    assert_eq!(ambiguous.status(), StatusCode::CONFLICT);
    assert!(ambiguous.text().await?.contains("unique merge base"));
    let unrelated = commit(&local, &tree, &[], "Unrelated").await?;
    push(&local, &url, &format!("+{unrelated}:refs/heads/feature")).await?;
    let native = Command::new("git")
        .current_dir(&local)
        .args(["merge-base", "--all", &left, &unrelated])
        .output()
        .await?;
    assert_eq!(native.status.code(), Some(1));
    assert!(native.stdout.is_empty());
    let input = request(&client, &repo).await?;
    let response = client
        .post(&api)
        .bearer_auth(OWNER)
        .json(&input)
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::CONFLICT);
    assert!(response.text().await?.contains("unrelated histories"));
    server.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn comparison_rejects_oversized_change_sets_without_partial_results() -> Result {
    use tokio::io::AsyncWriteExt;
    let workspace = tempfile::TempDir::new()?;
    let address = available_address().await?;
    let server = CanopyServer::start(
        config(address, workspace.path().join("node")),
        Arc::new(InMemory::new()),
    )
    .await?;
    let url = create_repository(address, "limits").await?;
    let repo = format!("http://{address}/api/repositories/limits");
    let api = format!("{repo}/pulls/1/comparison");
    let client = Client::new();
    let local = workspace.path().join("local");
    init(&local).await?;
    tokio::fs::write(local.join("seed"), b"content\n").await?;
    run_git(Some(&local), &["add", "seed"]).await?;
    run_git(Some(&local), &["commit", "-m", "Root"]).await?;
    let base = oid(&local, "HEAD").await?;
    let blob = oid(&local, "HEAD:seed").await?;
    push(&local, &url, "HEAD:refs/heads/main").await?;
    let mut body = Vec::new();
    for n in 0..10001 {
        body.extend_from_slice(format!("100644 blob {blob}\tfile-{n:05}\0").as_bytes());
    }
    let mut child = Command::new("git")
        .current_dir(&local)
        .args(["mktree", "-z"])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .spawn()?;
    child
        .stdin
        .take()
        .ok_or("tree stdin missing")?
        .write_all(&body)
        .await?;
    let output = child.wait_with_output().await?;
    assert!(output.status.success());
    let tree = String::from_utf8(output.stdout)?.trim().to_owned();
    let source = commit(
        &local,
        &tree,
        std::slice::from_ref(&base),
        "Many changed paths",
    )
    .await?;
    push(&local, &url, &format!("{source}:refs/heads/feature")).await?;
    open(&client, &repo, &source, &base).await?;
    let input = request(&client, &repo).await?;
    status(
        client.post(&api).bearer_auth(OWNER).json(&input),
        StatusCode::PAYLOAD_TOO_LARGE,
    )
    .await?;
    // A specific file stays readable without materializing the changed-file set.
    let file = preview(&client, &api, &input, b"file-10000", "after").await?;
    assert_eq!(
        URL_SAFE_NO_PAD.decode(file["content_base64"].as_str().unwrap())?,
        b"content\n"
    );
    let too_large = vec![b' '; 32 * 1024 + 1];
    status(
        client.post(&api).bearer_auth(OWNER).body(too_large),
        StatusCode::PAYLOAD_TOO_LARGE,
    )
    .await?;
    server.shutdown().await?;
    Ok(())
}

#[path = "comparison/history.rs"]
mod history;

#[path = "comparison/patches.rs"]
mod patches;

#[path = "comparison/threads.rs"]
mod threads;
