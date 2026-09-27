use super::*;

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

#[tokio::test(flavor = "multi_thread")]
async fn sha256_repository_push_clone_fetch_and_restore() -> Result {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let workspace = tempfile::TempDir::new()?;
    let first_address = available_address().await?;
    let first = CanopyServer::start(
        config(first_address, workspace.path().join("first")),
        Arc::clone(&store),
    )
    .await?;
    let response: serde_json::Value = reqwest::Client::new()
        .post(format!("http://{first_address}/api/repositories"))
        .bearer_auth("local-test-token")
        .json(&serde_json::json!({"name": "sha256", "object_format": "sha256"}))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(response["object_format"], "sha256");
    let conflicting = reqwest::Client::new()
        .post(format!("http://{first_address}/api/repositories"))
        .bearer_auth("local-test-token")
        .json(&serde_json::json!({"name": "sha256", "object_format": "sha1"}))
        .send()
        .await?;
    assert_eq!(conflicting.status(), reqwest::StatusCode::CONFLICT);

    let first_url = response["clone_url"].as_str().ok_or("clone URL missing")?;
    let source = workspace.path().join("source");
    run_git(
        None,
        &[
            "init",
            "--object-format=sha256",
            "-b",
            "main",
            path_str(&source)?,
        ],
    )
    .await?;
    run_git(Some(&source), &["config", "user.name", "Canopy Test"]).await?;
    run_git(
        Some(&source),
        &["config", "user.email", "test@example.invalid"],
    )
    .await?;
    run_git(Some(&source), &["lfs", "install", "--local"]).await?;
    run_git(Some(&source), &["lfs", "track", "*.lfs"]).await?;
    tokio::fs::write(source.join("file"), b"sha256 repository\n").await?;
    let large_body = vec![0x69; 900_000];
    tokio::fs::write(source.join("large"), &large_body).await?;
    let lfs_body = vec![0x37; 1_100_000];
    tokio::fs::write(source.join("asset.lfs"), &lfs_body).await?;
    run_git(
        Some(&source),
        &["add", ".gitattributes", "file", "large", "asset.lfs"],
    )
    .await?;
    run_git(
        Some(&source),
        &["-c", "commit.gpgsign=false", "commit", "-m", "first"],
    )
    .await?;
    run_git(
        Some(&source),
        &[
            "-c",
            "tag.gpgsign=false",
            "tag",
            "-a",
            "release",
            "-m",
            "release",
        ],
    )
    .await?;
    run_git(
        Some(&source),
        &[
            "-c",
            "http.extraHeader=Authorization: Bearer local-test-token",
            "push",
            first_url,
            "HEAD:refs/heads/main",
            "refs/tags/release",
        ],
    )
    .await?;
    let incompatible = workspace.path().join("incompatible");
    run_git(None, &["init", "-b", "main", path_str(&incompatible)?]).await?;
    run_git(Some(&incompatible), &["config", "user.name", "Canopy Test"]).await?;
    run_git(
        Some(&incompatible),
        &["config", "user.email", "test@example.invalid"],
    )
    .await?;
    tokio::fs::write(incompatible.join("file"), b"sha1 object\n").await?;
    run_git(Some(&incompatible), &["add", "file"]).await?;
    run_git(
        Some(&incompatible),
        &["-c", "commit.gpgsign=false", "commit", "-m", "incompatible"],
    )
    .await?;
    let rejected = Command::new("git")
        .arg("-C")
        .arg(&incompatible)
        .args([
            "-c",
            "http.extraHeader=Authorization: Bearer local-test-token",
            "push",
            first_url,
            "HEAD:refs/heads/main",
        ])
        .output()
        .await?;
    assert!(!rejected.status.success());
    let reason = String::from_utf8_lossy(&rejected.stderr).to_lowercase();
    assert!(reason.contains("hash algorithm") || reason.contains("object format"));
    let expected = run_git(Some(&source), &["rev-parse", "HEAD"]).await?;
    assert_eq!(expected.trim_ascii().len(), 64);
    first.shutdown().await?;

    let second_address = available_address().await?;
    let second = CanopyServer::start(
        config(second_address, workspace.path().join("second")),
        Arc::clone(&store),
    )
    .await?;
    let second_url = format!("http://{second_address}/canopy/sha256.git");
    let clone = workspace.path().join("clone");
    run_git(
        None,
        &[
            "-c",
            "http.extraHeader=Authorization: Bearer local-test-token",
            "clone",
            &second_url,
            path_str(&clone)?,
        ],
    )
    .await?;
    assert_eq!(
        run_git(Some(&clone), &["rev-parse", "HEAD"]).await?,
        expected
    );
    assert_eq!(tokio::fs::read(clone.join("large")).await?, large_body);
    run_git(Some(&clone), &["lfs", "install", "--local"]).await?;
    run_git(
        Some(&clone),
        &[
            "-c",
            "http.extraHeader=Authorization: Bearer local-test-token",
            "lfs",
            "pull",
        ],
    )
    .await?;
    assert!(tokio::fs::read(clone.join("asset.lfs")).await? == lfs_body);
    let browse = format!("http://{second_address}/api/repositories/sha256/browse");
    let view: serde_json::Value = reqwest::Client::new()
        .post(&browse)
        .bearer_auth("local-test-token")
        .json(&serde_json::json!({"repository_id": response["repository_id"], "query": {"kind": "resolve"}}))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(
        view["view"]["resolved"]["oid"],
        String::from_utf8_lossy(expected.trim_ascii()).as_ref()
    );
    assert_eq!(
        run_git(Some(&clone), &["rev-parse", "--show-object-format"])
            .await?
            .trim_ascii(),
        b"sha256"
    );
    assert_eq!(
        run_git(Some(&clone), &["rev-parse", "refs/tags/release"])
            .await?
            .trim_ascii()
            .len(),
        64
    );
    run_git(Some(&clone), &["fsck", "--full", "--strict"]).await?;
    let partial = workspace.path().join("partial");
    run_git(
        None,
        &[
            "-c",
            "http.extraHeader=Authorization: Bearer local-test-token",
            "clone",
            "--filter=blob:none",
            "--no-checkout",
            &second_url,
            path_str(&partial)?,
        ],
    )
    .await?;
    assert_eq!(
        run_git(Some(&partial), &["rev-parse", "--show-object-format"])
            .await?
            .trim_ascii(),
        b"sha256"
    );
    tokio::fs::write(source.join("file"), b"sha256 repository updated\n").await?;
    run_git(Some(&source), &["add", "file"]).await?;
    run_git(
        Some(&source),
        &["-c", "commit.gpgsign=false", "commit", "-m", "second"],
    )
    .await?;
    run_git(
        Some(&source),
        &[
            "-c",
            "http.extraHeader=Authorization: Bearer local-test-token",
            "push",
            &second_url,
            "HEAD:refs/heads/main",
        ],
    )
    .await?;
    run_git(
        Some(&clone),
        &[
            "-c",
            "http.extraHeader=Authorization: Bearer local-test-token",
            "fetch",
            &second_url,
            "main",
        ],
    )
    .await?;
    assert_eq!(
        run_git(Some(&clone), &["rev-parse", "FETCH_HEAD"]).await?,
        run_git(Some(&source), &["rev-parse", "HEAD"]).await?
    );
    second.shutdown().await?;
    Ok(())
}
