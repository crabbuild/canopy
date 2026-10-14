//! Qualify owner restart through the production resident, not detached handles.
#[path = "support/native_server.rs"]
mod native_server;
use native_server::*;
use object_store::{ObjectStore, memory::InMemory};
use std::sync::Arc;

#[tokio::test(flavor = "multi_thread")]
async fn a_second_node_clones_from_the_published_root_after_local_disk_loss() -> Result {
    let workspace = tempfile::TempDir::new()?;
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let first_disk = workspace.path().join("first-owner");
    let first = start(&first_disk, store.clone()).await?;
    let url = create(&first, "restart", "sha1").await?;
    let source = workspace.path().join("source");
    git(None, &["init", "-b", "main", path(&source)?]).await?;
    git(Some(&source), &["config", "user.name", "Restart Test"]).await?;
    git(
        Some(&source),
        &["config", "user.email", "restart@example.invalid"],
    )
    .await?;
    std::fs::write(source.join("README.md"), b"published Cell root\n")?;
    git(Some(&source), &["add", "README.md"]).await?;
    let gitlink = "74".repeat(20);
    git(
        Some(&source),
        &[
            "update-index",
            "--add",
            "--cacheinfo",
            &format!("160000,{gitlink},submodule"),
        ],
    )
    .await?;
    git(Some(&source), &["commit", "-m", "Durable native graph"]).await?;
    git(
        Some(&source),
        &["tag", "-a", "v1", "-m", "Native annotated tag"],
    )
    .await?;
    git(
        Some(&source),
        &[
            "push",
            &url,
            "HEAD:refs/heads/main",
            "HEAD:refs/heads/reused",
            "refs/tags/v1",
        ],
    )
    .await?;
    let refs = git(None, &["ls-remote", "--refs", &url]).await?;
    first.shutdown().await?;
    std::fs::remove_dir_all(&first_disk)?;
    assert!(!first_disk.exists());
    let second = start(&workspace.path().join("second-owner"), store).await?;
    let url = format!("http://{}/canopy/restart.git", second.local_addr());
    assert_eq!(git(None, &["ls-remote", "--refs", &url]).await?, refs);
    let clone = workspace.path().join("clone");
    git(None, &["clone", "--bare", &url, path(&clone)?]).await?;
    git(Some(&clone), &["fsck", "--strict", "--full"]).await?;
    assert_eq!(
        git(Some(&clone), &["show", "main:README.md"]).await?,
        b"published Cell root\n"
    );
    assert!(
        String::from_utf8(git(Some(&clone), &["ls-tree", "main", "submodule"]).await?)?
            .contains(&gitlink)
    );
    git(Some(&source), &["push", &url, ":refs/heads/reused"]).await?;
    git(Some(&source), &["push", &url, "HEAD:refs/heads/reused"]).await?;
    assert_eq!(git(None, &["ls-remote", "--refs", &url]).await?, refs);
    second.shutdown().await?;
    Ok(())
}
