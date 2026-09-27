use super::*;
use serde_json::Value;

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;
const AUTH: &str = "http.extraHeader=Authorization: Bearer local-test-token";

#[tokio::test(flavor = "multi_thread")]
async fn stock_git_push_options_are_validated_recorded_and_recovered() -> Result {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let workspace = tempfile::TempDir::new()?;
    let address = available_address().await?;
    let server = CanopyServer::start(
        config(address, workspace.path().join("first")),
        Arc::clone(&store),
    )
    .await?;
    let url = create_repository(address, "options").await?;
    let source = workspace.path().join("source");
    run_git(None, &["init", "-b", "main", path_str(&source)?]).await?;
    run_git(Some(&source), &["config", "user.name", "Canopy Test"]).await?;
    run_git(
        Some(&source),
        &["config", "user.email", "test@example.invalid"],
    )
    .await?;
    tokio::fs::write(source.join("file"), b"push option\n").await?;
    run_git(Some(&source), &["add", "file"]).await?;
    run_git(
        Some(&source),
        &["-c", "commit.gpgsign=false", "commit", "-m", "First"],
    )
    .await?;
    let first_oid = String::from_utf8(run_git(Some(&source), &["rev-parse", "HEAD"]).await?)?
        .trim()
        .to_owned();
    let first = uuid::Uuid::new_v4().to_string();
    let header = format!("http.extraHeader=Idempotency-Key: {first}");
    run_git(
        Some(&source),
        &[
            "-c",
            AUTH,
            "-c",
            &header,
            "push",
            "-o",
            "canopy.note=release candidate",
            "-o",
            "canopy.note=qa passed",
            &url,
            "HEAD:refs/heads/main",
            "HEAD:refs/heads/disposable",
        ],
    )
    .await?;
    let client = reqwest::Client::new();
    let receipt = format!("http://{address}/api/repositories/options/pushes/{first}");
    let record: Value = client
        .get(&receipt)
        .bearer_auth("local-test-token")
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(
        record["push"]["options"],
        serde_json::json!(["canopy.note=release candidate", "canopy.note=qa passed"])
    );
    assert_eq!(record["push"]["actor"], "canopy");
    let viewer = format!("cnp_{}", "88".repeat(32));
    client
        .post(format!("http://{address}/api/accounts"))
        .bearer_auth("local-test-token")
        .json(&serde_json::json!({"name":"viewer","token":viewer,"scope":"read"}))
        .send()
        .await?
        .error_for_status()?;
    client
        .put(format!(
            "http://{address}/api/repositories/options/collaborators/viewer"
        ))
        .bearer_auth("local-test-token")
        .json(&serde_json::json!({"role":"read"}))
        .send()
        .await?
        .error_for_status()?;
    assert_eq!(
        client
            .get(&receipt)
            .bearer_auth(&viewer)
            .send()
            .await?
            .status(),
        reqwest::StatusCode::NOT_FOUND
    );

    tokio::fs::write(source.join("file"), b"second version\n").await?;
    run_git(Some(&source), &["add", "file"]).await?;
    run_git(
        Some(&source),
        &["-c", "commit.gpgsign=false", "commit", "-m", "Second"],
    )
    .await?;
    let rejected = Command::new("git")
        .current_dir(&source)
        .args([
            "-c",
            AUTH,
            "push",
            "-o",
            "ci.skip",
            &url,
            "HEAD:refs/heads/main",
        ])
        .output()
        .await?;
    assert!(!rejected.status.success());
    assert!(String::from_utf8_lossy(&rejected.stderr).contains("canopy.note"));
    let current = run_git(None, &["-c", AUTH, "ls-remote", &url, "refs/heads/main"]).await?;
    assert_eq!(
        String::from_utf8(current)?.trim(),
        format!("{first_oid}\trefs/heads/main")
    );

    let deletion = uuid::Uuid::new_v4().to_string();
    let header = format!("http.extraHeader=Idempotency-Key: {deletion}");
    run_git(
        Some(&source),
        &[
            "-c",
            AUTH,
            "-c",
            &header,
            "push",
            "-o",
            "canopy.note=retire branch",
            &url,
            ":refs/heads/disposable",
        ],
    )
    .await?;
    let deletion_receipt = format!("http://{address}/api/repositories/options/pushes/{deletion}");
    let deleted: Value = client
        .get(&deletion_receipt)
        .bearer_auth("local-test-token")
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(
        deleted["push"]["options"],
        serde_json::json!(["canopy.note=retire branch"])
    );
    server.shutdown().await?;

    let restored_address = available_address().await?;
    let restored = CanopyServer::start(
        config(restored_address, workspace.path().join("restored")),
        store,
    )
    .await?;
    let restored_receipt =
        format!("http://{restored_address}/api/repositories/options/pushes/{first}");
    let recovered: Value = client
        .get(&restored_receipt)
        .bearer_auth("local-test-token")
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(recovered, record);
    let clone = workspace.path().join("clone");
    run_git(
        None,
        &[
            "-c",
            AUTH,
            "clone",
            &format!("http://{restored_address}/canopy/options.git"),
            path_str(&clone)?,
        ],
    )
    .await?;
    assert_eq!(
        run_git(Some(&clone), &["rev-parse", "HEAD"])
            .await?
            .trim_ascii(),
        first_oid.as_bytes()
    );
    run_git(Some(&clone), &["fsck", "--full", "--strict"]).await?;
    restored.shutdown().await?;
    Ok(())
}
