use super::*;

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;
const AUTH: &str = "http.extraHeader=Authorization: Bearer local-test-token";

async fn oid(path: &Path, revision: &str) -> Result<String> {
    Ok(
        String::from_utf8(run_git(Some(path), &["rev-parse", revision]).await?)?
            .trim()
            .into(),
    )
}

async fn stored_objects(path: &Path) -> Result<String> {
    Ok(String::from_utf8(
        run_git(
            Some(path),
            &[
                "cat-file",
                "--batch-all-objects",
                "--batch-check=%(objectname)",
            ],
        )
        .await?,
    )?)
}

fn cache_contains(root: &Path, oid: &str) -> Result<bool> {
    let suffix = std::path::PathBuf::from("objects")
        .join(&oid[..2])
        .join(&oid[2..]);
    let mut directories = vec![root.to_path_buf()];
    while let Some(directory) = directories.pop() {
        for entry in std::fs::read_dir(directory)? {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                directories.push(entry.path());
            } else if entry.path().ends_with(&suffix) {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

fn packet(line: &str) -> String {
    format!("{:04x}{line}", line.len() + 4)
}

async fn unreachable(url: &str, oid: &str) -> Result {
    let request = format!(
        "{}0001{}{}{}0000",
        packet("command=fetch\n"),
        packet(&format!("want {oid}\n")),
        packet("filter blob:none\n"),
        packet("done\n")
    );
    let response = reqwest::Client::new()
        .post(format!("{url}/git-upload-pack"))
        .bearer_auth("local-test-token")
        .header("Git-Protocol", "version=2")
        .header("Content-Type", "application/x-git-upload-pack-request")
        .body(request)
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::BAD_REQUEST);
    assert!(response.text().await?.contains("not reachable"));
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn filtered_clones_lazy_fetch_reachable_objects_without_hydrating_other_blobs() -> Result {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let workspace = tempfile::TempDir::new()?;
    let address = available_address().await?;
    let server = CanopyServer::start(
        config(address, workspace.path().join("first")),
        Arc::clone(&store),
    )
    .await?;
    let url = create_repository(address, "partial").await?;
    let source = workspace.path().join("source");
    run_git(None, &["init", "-b", "main", path_str(&source)?]).await?;
    run_git(Some(&source), &["config", "user.name", "Canopy Test"]).await?;
    run_git(
        Some(&source),
        &["config", "user.email", "test@example.invalid"],
    )
    .await?;
    run_git(Some(&source), &["config", "commit.gpgsign", "false"]).await?;
    let mut bodies = Vec::new();
    for seed in [b"first".as_slice(), b"second"] {
        let mut body = vec![0; 2 * 1024 * 1024];
        blake3::Hasher::new()
            .update(seed)
            .finalize_xof()
            .fill(&mut body);
        bodies.push(body);
    }
    tokio::fs::write(source.join("one"), &bodies[0]).await?;
    tokio::fs::write(source.join("two"), &bodies[1]).await?;
    tokio::fs::write(source.join("small"), b"small blob\n").await?;
    run_git(Some(&source), &["add", "."]).await?;
    run_git(Some(&source), &["commit", "-m", "Main"]).await?;
    let first_blob = oid(&source, "HEAD:one").await?;
    let second_blob = oid(&source, "HEAD:two").await?;
    let tree = oid(&source, "HEAD^{tree}").await?;
    // Tag peeling must retain explicitly advertised blob targets, even while
    // ordinary large blobs are omitted by a blobless transfer.
    run_git(
        Some(&source),
        &[
            "-c",
            "tag.gpgsign=false",
            "tag",
            "-a",
            "blob-tag",
            "HEAD:small",
            "-m",
            "Blob tag",
        ],
    )
    .await?;
    run_git(Some(&source), &["checkout", "-b", "discarded"]).await?;
    tokio::fs::write(source.join("discarded"), b"unreferenced after deletion\n").await?;
    run_git(Some(&source), &["add", "."]).await?;
    run_git(Some(&source), &["commit", "-m", "Discarded"]).await?;
    let discarded = oid(&source, "HEAD:discarded").await?;
    let discarded_tree = oid(&source, "HEAD^{tree}").await?;
    let discarded_commit = oid(&source, "HEAD").await?;
    run_git(Some(&source), &["-c", AUTH, "push", "--mirror", &url]).await?;
    run_git(
        Some(&source),
        &["-c", AUTH, "push", &url, ":refs/heads/discarded"],
    )
    .await?;
    server.shutdown().await?;

    let address = available_address().await?;
    let restored_disk = workspace.path().join("restored");
    let restored = CanopyServer::start(config(address, restored_disk.clone()), store).await?;
    let url = format!("http://{address}/canopy/partial.git");
    for protocol in ["0", "2"] {
        let clone = workspace.path().join(format!("blobless-{protocol}"));
        run_git(
            None,
            &[
                "-c",
                AUTH,
                "-c",
                &format!("protocol.version={protocol}"),
                "clone",
                "--filter=blob:none",
                "--no-checkout",
                &url,
                path_str(&clone)?,
            ],
        )
        .await?;
        let objects = stored_objects(&clone).await?;
        assert!(!objects.contains(&first_blob));
        assert!(!objects.contains(&second_blob));
        assert!(objects.contains(&tree));
        assert!(!cache_contains(&restored_disk, &second_blob)?);
        assert_eq!(
            run_git(
                Some(&clone),
                &[
                    "-c",
                    AUTH,
                    "-c",
                    &format!("protocol.version={protocol}"),
                    "show",
                    "HEAD:one"
                ]
            )
            .await?,
            bodies[0]
        );
        assert!(cache_contains(&restored_disk, &first_blob)?);
        assert!(!cache_contains(&restored_disk, &second_blob)?);
    }
    for oid in [
        &discarded,
        &discarded_tree,
        &discarded_commit,
        &"ab".repeat(20),
    ] {
        unreachable(&url, oid).await?;
    }

    for filter in [
        "tree:0",
        "blob:limit=1k",
        "combine:tree:0+blob:none",
        "object:type=commit",
    ] {
        let clone = workspace.path().join(format!("filter-{}", filter.len()));
        run_git(
            None,
            &[
                "-c",
                AUTH,
                "clone",
                &format!("--filter={filter}"),
                "--no-checkout",
                &url,
                path_str(&clone)?,
            ],
        )
        .await?;
        let objects = stored_objects(&clone).await?;
        assert!(!objects.contains(&second_blob));
        if filter.contains("tree:0") || filter == "object:type=commit" {
            assert!(!objects.contains(&tree));
        }
        run_git(Some(&clone), &["-c", AUTH, "checkout", "main"]).await?;
        assert_eq!(tokio::fs::read(clone.join("two")).await?, bodies[1]);
        run_git(Some(&clone), &["fsck", "--strict"]).await?;
    }
    // Full preparation can retain orphan bodies in the shared cache. The
    // explicit want gate must still reject each unreachable object type.
    assert!(cache_contains(&restored_disk, &discarded)?);
    for oid in [
        &discarded,
        &discarded_tree,
        &discarded_commit,
        &"ab".repeat(20),
    ] {
        unreachable(&url, oid).await?;
    }
    restored.shutdown().await?;
    Ok(())
}
