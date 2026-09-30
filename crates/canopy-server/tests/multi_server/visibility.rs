use super::*;
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use reqwest::{Client, Method, StatusCode};
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;
const OWNER: &str = "local-test-token";
const AUTH: &str = "http.extraHeader=Authorization: Bearer local-test-token";

async fn value(request: reqwest::RequestBuilder) -> Result<Value> {
    let response = request.send().await?;
    let status = response.status();
    let text = response.text().await?;
    assert!(status.is_success(), "{status}: {text}");
    Ok(serde_json::from_str(&text)?)
}
async fn status(request: reqwest::RequestBuilder, expected: StatusCode) -> Result {
    let response = request.send().await?;
    assert_eq!(response.status(), expected, "{}", response.text().await?);
    Ok(())
}
async fn visibility(client: &Client, repo: &str, setting: &str) -> Result<Value> {
    let current = value(client.get(format!("{repo}/visibility")).bearer_auth(OWNER)).await?;
    let input = json!({"repository_id":current["repository_id"],"expected_generation":current["generation"],"visibility":setting});
    let output = value(
        client
            .put(format!("{repo}/visibility"))
            .bearer_auth(OWNER)
            .json(&input),
    )
    .await?;
    assert_eq!(output["visibility"], setting);
    status(
        client
            .put(format!("{repo}/visibility"))
            .bearer_auth(OWNER)
            .json(&input),
        StatusCode::CONFLICT,
    )
    .await?;
    Ok(output)
}
async fn clone_public(url: &str, path: &Path, body: &[u8]) -> Result {
    run_git(
        None,
        &["-c", "http.extraHeader=", "clone", url, path_str(path)?],
    )
    .await?;
    run_git(Some(path), &["lfs", "install", "--local"]).await?;
    run_git(Some(path), &["-c", "http.extraHeader=", "lfs", "pull"]).await?;
    assert_eq!(std::fs::read(path.join("README.md"))?, b"Public main\n");
    assert!(std::fs::read(path.join("asset.lfs"))? == body);
    run_git(Some(path), &["fsck", "--strict", "--full"]).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn public_reads_and_private_revocation_survive_cell_recovery() -> Result {
    let files = tempfile::TempDir::new()?;
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let a = available_address().await?;
    let first =
        CanopyServer::start(config(a, files.path().join("first")), Arc::clone(&store)).await?;
    let url = create_repository(a, "visible").await?;
    create_repository(a, "secret").await?;
    let base = format!("http://{a}");
    let repo = format!("{base}/api/repositories/visible");
    let client = Client::new();
    let id = value(client.get(&repo).bearer_auth(OWNER)).await?["repository_id"].clone();
    let outsider = format!("cnp_{}", hex::encode([88; 32]));
    value(
        client
            .post(format!("{base}/api/accounts"))
            .bearer_auth(OWNER)
            .json(&json!({"name":"outsider","token":outsider,"scope":"write"})),
    )
    .await?;
    assert_eq!(
        value(client.get(format!("{base}/api/repositories"))).await?["repositories"],
        json!([])
    );
    status(
        client.get(format!("{url}/info/refs?service=git-upload-pack")),
        StatusCode::UNAUTHORIZED,
    )
    .await?;
    let source = files.path().join("source");
    run_git(None, &["init", "-b", "main", path_str(&source)?]).await?;
    run_git(Some(&source), &["config", "user.name", "Canopy Test"]).await?;
    run_git(
        Some(&source),
        &["config", "user.email", "canopy@example.invalid"],
    )
    .await?;
    run_git(Some(&source), &["lfs", "install", "--local"]).await?;
    run_git(Some(&source), &["lfs", "track", "*.lfs"]).await?;
    let body = b"Public LFS\n".repeat(10_000);
    std::fs::write(source.join("asset.lfs"), &body)?;
    std::fs::write(source.join("README.md"), b"Public main\n")?;
    run_git(Some(&source), &["add", "."]).await?;
    run_git(Some(&source), &["commit", "-m", "Main"]).await?;
    run_git(Some(&source), &["-c", AUTH, "push", &url, "main"]).await?;
    let main = String::from_utf8(run_git(Some(&source), &["rev-parse", "HEAD"]).await?)?
        .trim()
        .to_owned();
    std::fs::write(source.join("README.md"), b"Public feature\n")?;
    run_git(Some(&source), &["commit", "-am", "Feature"]).await?;
    run_git(
        Some(&source),
        &["-c", AUTH, "push", &url, "HEAD:refs/heads/feature"],
    )
    .await?;
    let feature = String::from_utf8(run_git(Some(&source), &["rev-parse", "HEAD"]).await?)?
        .trim()
        .to_owned();
    value(client.post(format!("{repo}/issues")).bearer_auth(OWNER).json(&json!({"repository_id":id,"id":uuid::Uuid::new_v4().to_string(),"title":"Visible issue","body":"Issue contents"}))).await?;
    value(client.post(format!("{repo}/issues/1/comments")).bearer_auth(OWNER).json(&json!({"repository_id":id,"id":uuid::Uuid::new_v4().to_string(),"body":"Issue reply"}))).await?;
    value(client.post(format!("{repo}/pulls")).bearer_auth(OWNER).json(&json!({"repository_id":id,"id":uuid::Uuid::new_v4().to_string(),"title":"Visible pull","body":"Pull contents","draft":false,"source_ref":"refs/heads/feature","source_oid":feature,"base_ref":"refs/heads/main","base_oid":main}))).await?;
    let pull =
        value(client.get(format!("{repo}/pulls/1")).bearer_auth(OWNER)).await?["pull"].clone();
    let revision = json!({"pull_version":pull["version"],"source_oid":pull["source"]["oid"],"source_version":pull["source"]["version"],"base_oid":pull["base"]["oid"],"base_version":pull["base"]["version"]});
    let target = json!({"kind":"current","revision":revision});
    let path = URL_SAFE_NO_PAD.encode("README.md");
    value(client.post(format!("{repo}/pulls/1/reviews")).bearer_auth(OWNER).json(&json!({"repository_id":id,"id":uuid::Uuid::new_v4().to_string(),"revision":revision,"kind":"comment","body":"Visible review"}))).await?;
    value(client.post(format!("{repo}/pulls/1/threads")).bearer_auth(OWNER).json(&json!({"repository_id":id,"id":uuid::Uuid::new_v4().to_string(),"target":target,"path_base64":path,"side":"after","line":1,"body":"Visible thread"}))).await?;
    value(client.post(format!("{repo}/pulls/1/threads/1/comments")).bearer_auth(OWNER).json(&json!({"repository_id":id,"id":uuid::Uuid::new_v4().to_string(),"body":"Thread reply"}))).await?;
    status(client.put(format!("{repo}/check-contexts/test")).bearer_auth(OWNER).json(&json!({"repository_id":id,"expected_version":0,"reporter":"canopy","enabled":true})), StatusCode::NO_CONTENT).await?;
    let check = uuid::Uuid::new_v4().to_string();
    value(
        client
            .post(format!("{repo}/commits/{main}/checks"))
            .bearer_auth(OWNER)
            .json(&json!({"repository_id":id,"id":check,"context":"test","context_version":1})),
    )
    .await?;
    let read_paths = [
        "issues".into(),
        "issues/1".into(),
        "issues/1/comments".into(),
        "pulls".into(),
        "pulls/1".into(),
        "pulls/1/reviews".into(),
        "pulls/1/threads".into(),
        "pulls/1/threads/1".into(),
        "pulls/1/threads/1/comments".into(),
        "pulls/1/review-policy".into(),
        "branch-rules".into(),
        "check-contexts".into(),
        format!("commits/{main}/checks"),
        format!("checks/{check}"),
        "default-branch".into(),
        "visibility".into(),
    ];
    for path in &read_paths {
        status(
            client.get(format!("{repo}/{path}")),
            StatusCode::UNAUTHORIZED,
        )
        .await?;
    }
    status(
        client
            .put(format!("{repo}/visibility"))
            .bearer_auth(&outsider)
            .json(&json!({})),
        StatusCode::FORBIDDEN,
    )
    .await?;
    visibility(&client, &repo, "public").await?;
    for generation in [-1, i64::MAX] {
        status(client.put(format!("{repo}/visibility")).bearer_auth(OWNER)
            .json(&json!({"repository_id":id,"expected_generation":generation,"visibility":"public"})), StatusCode::UNPROCESSABLE_ENTITY).await?;
    }
    let owned = value(
        client
            .get(format!("{base}/api/repositories"))
            .bearer_auth(OWNER),
    )
    .await?;
    assert_eq!(
        owned["repositories"]
            .as_array()
            .ok_or("missing owned list")?
            .len(),
        2
    );
    value(client.post(format!("{repo}/pulls/1/reviews")).bearer_auth(&outsider).json(&json!({"repository_id":id,"id":uuid::Uuid::new_v4().to_string(),"revision":revision,"kind":"comment","body":"Public reader comment"}))).await?;
    status(client.post(format!("{repo}/pulls/1/reviews")).bearer_auth(&outsider).json(&json!({"repository_id":id,"id":uuid::Uuid::new_v4().to_string(),"revision":revision,"kind":"approve","body":"Not a collaborator"})), StatusCode::FORBIDDEN).await?;
    let listing = value(client.get(format!("{base}/api/repositories"))).await?;
    assert_eq!(
        listing["repositories"]
            .as_array()
            .ok_or("missing list")?
            .len(),
        1
    );
    assert_eq!(listing["repositories"][0]["name"], "visible");
    let detail = value(client.get(&repo)).await?;
    assert!(detail["viewer"].is_null());
    assert_eq!(detail["role"], "read");
    for path in &read_paths {
        status(client.get(format!("{repo}/{path}")), StatusCode::OK).await?;
    }
    assert_eq!(
        value(client.get(format!("{repo}/issues/1"))).await?["issue"]["body"],
        "Issue contents"
    );
    let browse =
        json!({"repository_id":id,"query":{"kind":"file","commit":main,"path_base64":path}});
    status(
        client.post(format!("{repo}/browse")).json(&browse),
        StatusCode::OK,
    )
    .await?;
    let comparison =
        json!({"repository_id":id,"target":target,"query":{"kind":"patch","path_base64":path}});
    status(
        client
            .post(format!("{repo}/pulls/1/comparison"))
            .json(&comparison),
        StatusCode::OK,
    )
    .await?;
    clone_public(&url, &files.path().join("public-clone"), &body).await?;
    for suffix in ["info/refs?service=git-receive-pack", "git-receive-pack"] {
        let method = if suffix.starts_with("info/") {
            Method::GET
        } else {
            Method::POST
        };
        status(
            client.request(method, format!("{url}/{suffix}")),
            StatusCode::UNAUTHORIZED,
        )
        .await?;
    }
    for suffix in [
        "issues",
        "issues/1/comments",
        "pulls",
        "pulls/1/reviews",
        "pulls/1/threads",
        "pulls/1/merge",
        "pulls/1/merge-candidates",
    ] {
        status(
            client.post(format!("{repo}/{suffix}")).json(&json!({})),
            StatusCode::UNAUTHORIZED,
        )
        .await?;
    }
    status(
        client.get(format!("{repo}/collaborators")),
        StatusCode::UNAUTHORIZED,
    )
    .await?;
    let lfs_oid = hex::encode(Sha256::digest(&body));
    status(
        client
            .post(format!("{url}/info/lfs/objects/batch"))
            .json(&json!({"operation":"upload","objects":[{"oid":lfs_oid,"size":body.len()}]})),
        StatusCode::UNAUTHORIZED,
    )
    .await?;
    status(
        client
            .put(format!("{url}/info/lfs/objects/{lfs_oid}"))
            .body(body.clone()),
        StatusCode::UNAUTHORIZED,
    )
    .await?;
    for address in [
        &repo,
        &format!("{url}/info/refs?service=git-upload-pack"),
        &format!("{url}/info/lfs/objects/{lfs_oid}"),
    ] {
        status(
            client.get(address).bearer_auth("invalid-token"),
            StatusCode::UNAUTHORIZED,
        )
        .await?;
    }
    status(
        client
            .get(format!("{url}/info/refs?service=git-receive-pack"))
            .bearer_auth(&outsider),
        StatusCode::FORBIDDEN,
    )
    .await?;
    // A stale public discovery hint must never keep a newly private repository visible.
    visibility(&client, &repo, "private").await?;
    assert_eq!(
        value(client.get(format!("{base}/api/repositories"))).await?["repositories"],
        json!([])
    );
    assert_eq!(
        value(
            client
                .get(format!("{base}/api/repositories"))
                .bearer_auth(&outsider)
        )
        .await?["repositories"],
        json!([])
    );
    for path in &read_paths {
        status(
            client.get(format!("{repo}/{path}")),
            StatusCode::UNAUTHORIZED,
        )
        .await?;
    }
    status(
        client.post(format!("{repo}/browse")).json(&browse),
        StatusCode::UNAUTHORIZED,
    )
    .await?;
    status(
        client.get(format!("{url}/info/lfs/objects/{lfs_oid}")),
        StatusCode::UNAUTHORIZED,
    )
    .await?;
    visibility(&client, &repo, "public").await?;
    first.shutdown().await?;
    let b = available_address().await?;
    let second =
        CanopyServer::start(config(b, files.path().join("second")), Arc::clone(&store)).await?;
    let recovered = format!("http://{b}/api/repositories/visible");
    for path in &read_paths {
        status(client.get(format!("{recovered}/{path}")), StatusCode::OK).await?;
    }
    clone_public(
        &format!("http://{b}/canopy/visible.git"),
        &files.path().join("recovered-clone"),
        &body,
    )
    .await?;
    visibility(&client, &recovered, "private").await?;
    second.shutdown().await?;
    let c = available_address().await?;
    let third = CanopyServer::start(config(c, files.path().join("third")), store).await?;
    assert_eq!(
        value(client.get(format!("http://{c}/api/repositories"))).await?["repositories"],
        json!([])
    );
    status(
        client.get(format!(
            "http://{c}/canopy/visible.git/info/refs?service=git-upload-pack"
        )),
        StatusCode::UNAUTHORIZED,
    )
    .await?;
    third.shutdown().await?;
    Ok(())
}
