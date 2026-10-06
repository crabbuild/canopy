//! Stock Git qualification against resident-owned native publication.
#[path = "../support/native_server.rs"]
mod native_server;
use native_server::*;
use object_store::{ObjectStore, memory::InMemory};
use std::sync::Arc;

#[tokio::test(flavor = "multi_thread")]
async fn stock_git_push_and_clone_are_backed_by_one_repository_cell() -> Result {
    let workspace = tempfile::TempDir::new()?;
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let server = start(&workspace.path().join("resident"), store).await?;
    let url = create(&server, "smart", "sha1").await?;
    let client = reqwest::Client::new();
    assert_eq!(
        client
            .post(format!("{url}/git-receive-pack"))
            .bearer_auth(TOKEN)
            .header("Content-Type", "text/plain")
            .body("invalid media")
            .send()
            .await?
            .status(),
        reqwest::StatusCode::UNSUPPORTED_MEDIA_TYPE
    );
    let source = workspace.path().join("source");
    git(None, &["init", "-b", "main", path(&source)?]).await?;
    git(Some(&source), &["config", "user.name", "HTTP Test"]).await?;
    git(
        Some(&source),
        &["config", "user.email", "http@example.invalid"],
    )
    .await?;
    let first = vec![17_u8; 2 * 1024 * 1024];
    let second = vec![29_u8; 2 * 1024 * 1024];
    std::fs::write(source.join("first"), &first)?;
    std::fs::write(source.join("second"), &second)?;
    git(Some(&source), &["add", "."]).await?;
    git(Some(&source), &["commit", "-m", "Native HTTP body"]).await?;
    git(
        Some(&source),
        &["push", "-o", "canopy.note=resident", &url, "main"],
    )
    .await?;
    for version in ["0", "2"] {
        let clone = workspace.path().join(format!("blobless-{version}"));
        git(
            None,
            &[
                "-c",
                &format!("protocol.version={version}"),
                "clone",
                "--filter=blob:none",
                "--no-checkout",
                &url,
                path(&clone)?,
            ],
        )
        .await?;
        assert_eq!(git(Some(&clone), &["show", "HEAD:first"]).await?, first);
        assert_eq!(git(Some(&clone), &["show", "HEAD:second"]).await?, second);
        git(Some(&clone), &["fsck", "--strict"]).await?;
    }
    let guess = "12".repeat(20);
    let want = format!("want {guess}\n");
    let request = format!("{:04x}{want}00000009done\n", want.len() + 4);
    assert_eq!(
        client
            .post(format!("{url}/git-upload-pack"))
            .bearer_auth(TOKEN)
            .header("Content-Type", "application/x-git-upload-pack-request")
            .body(request)
            .send()
            .await?
            .status(),
        reqwest::StatusCode::BAD_REQUEST
    );
    git(
        Some(&source),
        &["push", "-o", "canopy.note=delete", &url, ":main"],
    )
    .await?;
    assert!(git(None, &["ls-remote", "--refs", &url]).await?.is_empty());
    server.shutdown().await?;
    Ok(())
}
