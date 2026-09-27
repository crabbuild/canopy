use super::*;

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;
const AUTH: &str = "http.extraHeader=Authorization: Bearer local-test-token";

async fn refs(path: &Path) -> Result<Vec<u8>> {
    run_git(Some(path), &["show-ref"]).await
}

#[tokio::test(flavor = "multi_thread")]
async fn stock_git_history_refs_and_shallow_fetch_survive_fresh_disk_restore() -> Result {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let workspace = tempfile::TempDir::new()?;
    let address = available_address().await?;
    let server = CanopyServer::start(
        config(address, workspace.path().join("first")),
        Arc::clone(&store),
    )
    .await?;
    let url = create_repository(address, "compatibility").await?;
    let client = reqwest::Client::new();
    let api = format!("http://{address}/api/repositories/compatibility");
    let repository: serde_json::Value = client
        .get(&api)
        .bearer_auth("local-test-token")
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    client
        .put(format!("{api}/branch-rules"))
        .bearer_auth("local-test-token")
        .json(
            &serde_json::json!({"repository_id": repository["repository_id"], "rule": {
                "reference": "refs/heads/開発", "expected_version": 0, "enabled": true,
                "deny_deletions": true, "fast_forward_only": true, "require_pull_request": false,
                "required_approvals": 0, "required_checks": []
            }}),
        )
        .send()
        .await?
        .error_for_status()?;
    let source = workspace.path().join("source");
    run_git(None, &["init", "-b", "main", path_str(&source)?]).await?;
    run_git(Some(&source), &["config", "user.name", "Canopy Test"]).await?;
    run_git(
        Some(&source),
        &["config", "user.email", "test@example.invalid"],
    )
    .await?;
    for revision in 1..=3 {
        tokio::fs::write(source.join("file"), format!("revision {revision}\n")).await?;
        run_git(Some(&source), &["add", "file"]).await?;
        run_git(
            Some(&source),
            &["-c", "commit.gpgsign=false", "commit", "-m", "Revision"],
        )
        .await?;
    }
    run_git(Some(&source), &["tag", "lightweight"]).await?;
    run_git(
        Some(&source),
        &[
            "-c",
            "tag.gpgsign=false",
            "tag",
            "-a",
            "版本",
            "-m",
            "Release",
        ],
    )
    .await?;
    run_git(Some(&source), &["notes", "add", "-m", "retained note"]).await?;
    for branch in ["café", "開発", "🌳"] {
        run_git(Some(&source), &["branch", branch]).await?;
    }
    run_git(Some(&source), &["-c", AUTH, "push", "--mirror", &url]).await?;
    for protocol in ["0", "1", "2"] {
        let clone = workspace.path().join(format!("clone-{protocol}"));
        run_git(
            None,
            &[
                "-c",
                AUTH,
                "-c",
                &format!("protocol.version={protocol}"),
                "clone",
                &url,
                path_str(&clone)?,
            ],
        )
        .await?;
        run_git(Some(&clone), &["fsck", "--strict", "--full"]).await?;
        assert_eq!(tokio::fs::read(clone.join("file")).await?, b"revision 3\n");
    }

    let shallow = workspace.path().join("shallow");
    run_git(
        None,
        &["-c", AUTH, "clone", "--depth=1", &url, path_str(&shallow)?],
    )
    .await?;
    assert_eq!(
        run_git(Some(&shallow), &["rev-list", "--count", "HEAD"]).await?,
        b"1\n"
    );
    run_git(Some(&shallow), &["-c", AUTH, "fetch", "--deepen=1"]).await?;
    assert_eq!(
        run_git(Some(&shallow), &["rev-list", "--count", "HEAD"]).await?,
        b"2\n"
    );
    run_git(Some(&shallow), &["-c", AUTH, "fetch", "--unshallow"]).await?;
    assert_eq!(
        run_git(Some(&shallow), &["rev-list", "--count", "HEAD"]).await?,
        b"3\n"
    );

    tokio::fs::write(source.join("file"), b"pulled revision\n").await?;
    run_git(
        Some(&source),
        &["-c", "commit.gpgsign=false", "commit", "-am", "Incremental"],
    )
    .await?;
    run_git(Some(&source), &["-c", AUTH, "push", &url, "main"]).await?;
    run_git(Some(&shallow), &["-c", AUTH, "pull", "--ff-only"]).await?;
    assert_eq!(
        run_git(Some(&shallow), &["rev-parse", "HEAD"]).await?,
        run_git(Some(&source), &["rev-parse", "HEAD"]).await?
    );
    run_git(
        Some(&shallow),
        &["-c", AUTH, "fetch", "origin", "refs/notes/*:refs/notes/*"],
    )
    .await?;
    assert_eq!(
        run_git(Some(&shallow), &["notes", "show", "HEAD^"]).await?,
        b"retained note\n"
    );
    run_git(
        Some(&source),
        &["-c", AUTH, "push", &url, ":refs/heads/café"],
    )
    .await?;
    run_git(Some(&source), &["branch", "-D", "café"]).await?;
    let clone = workspace.path().join("clone-2");
    run_git(Some(&clone), &["-c", AUTH, "fetch", "--prune"]).await?;
    assert!(
        run_git(Some(&clone), &["for-each-ref", "refs/remotes/origin/café"])
            .await?
            .is_empty()
    );

    let mirror = workspace.path().join("mirror.git");
    run_git(
        None,
        &["-c", AUTH, "clone", "--mirror", &url, path_str(&mirror)?],
    )
    .await?;
    assert_eq!(refs(&mirror).await?, refs(&source).await?);
    let destination = create_repository(address, "mirrored").await?;
    run_git(
        Some(&mirror),
        &["-c", AUTH, "push", "--mirror", &destination],
    )
    .await?;
    server.shutdown().await?;

    // Rebuild native Git caches from durable Cells, including notes, annotated
    // tags, and Unicode refs, with no original local disk state.
    let address = available_address().await?;
    let restored =
        CanopyServer::start(config(address, workspace.path().join("restored")), store).await?;
    for name in ["compatibility", "mirrored"] {
        let clone = workspace.path().join(format!("restored-{name}.git"));
        let url = format!("http://{address}/canopy/{name}.git");
        run_git(
            None,
            &["-c", AUTH, "clone", "--mirror", &url, path_str(&clone)?],
        )
        .await?;
        assert_eq!(refs(&clone).await?, refs(&source).await?);
        run_git(Some(&clone), &["fsck", "--strict", "--full"]).await?;
        assert_eq!(
            run_git(Some(&clone), &["notes", "show", "main^"]).await?,
            b"retained note\n"
        );
    }
    restored.shutdown().await?;
    Ok(())
}
