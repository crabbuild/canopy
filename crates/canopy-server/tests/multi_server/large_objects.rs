use super::*;

#[tokio::test(flavor = "multi_thread")]
async fn large_tree_commit_and_tag_restore_from_sqlite_after_owner_restart()
-> Result<(), Box<dyn std::error::Error>> {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let workspace = tempfile::TempDir::new()?;
    let local = workspace.path().join("source");
    run_git(None, &["init", "--bare", path_str(&local)?]).await?;
    let blob = b"one shared leaf\n".to_vec();
    let blob_oid = write_object(&local, "blob", &blob).await?;
    let raw_oid = hex::decode(&blob_oid)?;
    let mut tree = Vec::new();
    for index in 0..32_000 {
        tree.extend_from_slice(format!("100644 file-{index:05}\0").as_bytes());
        tree.extend_from_slice(&raw_oid);
    }
    assert!(tree.len() > canopy_server::INLINE_OBJECT_LIMIT);
    let tree_oid = write_object(&local, "tree", &tree).await?;
    let mut commit = format!("tree {tree_oid}\nauthor Canopy <test@example.invalid> 0 +0000\ncommitter Canopy <test@example.invalid> 0 +0000\n\n").into_bytes();
    commit.extend(vec![b'c'; 65 * 1024 * 1024]);
    commit.push(b'\n');
    let commit_oid = write_object(&local, "commit", &commit).await?;
    let mut tag = format!("object {commit_oid}\ntype commit\ntag release\ntagger Canopy <test@example.invalid> 0 +0000\n\n").into_bytes();
    tag.extend(vec![b't'; 1_100_000]);
    tag.push(b'\n');
    let tag_oid = write_object(&local, "tag", &tag).await?;
    run_git(
        Some(&local),
        &["update-ref", "refs/heads/main", &commit_oid],
    )
    .await?;
    run_git(Some(&local), &["symbolic-ref", "HEAD", "refs/heads/main"]).await?;
    run_git(Some(&local), &["update-ref", "refs/tags/release", &tag_oid]).await?;
    let address = available_address().await?;
    let server = CanopyServer::start(
        config(address, workspace.path().join("first")),
        Arc::clone(&store),
    )
    .await?;
    let url = create_repository(address, "large-objects").await?;
    run_git(
        Some(&local),
        &[
            "-c",
            "http.extraHeader=Authorization: Bearer local-test-token",
            "push",
            &url,
            "refs/heads/main",
            "refs/tags/release",
        ],
    )
    .await?;
    server.shutdown().await?;
    let address = available_address().await?;
    let server =
        CanopyServer::start(config(address, workspace.path().join("restored")), store).await?;
    let restored = workspace.path().join("clone");
    let url = format!("http://{address}/canopy/large-objects.git");
    run_git(
        None,
        &[
            "-c",
            "http.extraHeader=Authorization: Bearer local-test-token",
            "clone",
            "--bare",
            &url,
            path_str(&restored)?,
        ],
    )
    .await?;
    for (kind, oid, bytes) in [
        ("blob", blob_oid, blob),
        ("tree", tree_oid, tree),
        ("commit", commit_oid.clone(), commit),
        ("tag", tag_oid.clone(), tag),
    ] {
        assert_eq!(
            run_git(Some(&restored), &["cat-file", kind, &oid]).await?,
            bytes,
            "{kind} body changed during recovery"
        );
    }
    assert_eq!(
        String::from_utf8(run_git(Some(&restored), &["rev-parse", "refs/heads/main"]).await?)?
            .trim(),
        commit_oid
    );
    assert_eq!(
        String::from_utf8(run_git(Some(&restored), &["rev-parse", "refs/tags/release"]).await?)?
            .trim(),
        tag_oid
    );
    run_git(Some(&restored), &["fsck", "--strict", "--full"]).await?;
    server.shutdown().await?;
    Ok(())
}

async fn write_object(
    repository: &Path,
    kind: &str,
    body: &[u8],
) -> Result<String, Box<dyn std::error::Error>> {
    let path = repository.join(format!("raw-{kind}"));
    tokio::fs::write(&path, body).await?;
    let oid = run_git(
        Some(repository),
        &["hash-object", "-w", "-t", kind, path_str(&path)?],
    )
    .await?;
    Ok(String::from_utf8(oid)?.trim().into())
}
