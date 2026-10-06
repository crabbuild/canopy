//! Native catalog publication replaces loose-object ingestion and ref commands.
#[path = "../support/native_server.rs"]
mod native_server;
use canopy_server::RepositoryModule;
use cellule_runtime::CellModule;
use native_server::*;
use object_store::{ObjectStore, memory::InMemory};
use std::sync::Arc;

#[tokio::test(flavor = "multi_thread")]
async fn repository_cell_publishes_objects_and_refs_atomically() -> Result {
    // Hard cutover: fixture setup must not resurrect retired PutObjects (5).
    assert!(
        !RepositoryModule
            .descriptor()
            .commands
            .iter()
            .any(|op| op.id == 5)
    );
    let workspace = tempfile::TempDir::new()?;
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let server = start(&workspace.path().join("resident"), store).await?;
    for format in ["sha1", "sha256"] {
        let name = format!("atomic-{format}");
        let url = create(&server, &name, format).await?;
        let source = workspace.path().join(format!("source-{format}"));
        git(
            None,
            &[
                "init",
                &format!("--object-format={format}"),
                "-b",
                "main",
                path(&source)?,
            ],
        )
        .await?;
        git(Some(&source), &["config", "user.name", "Atomic Test"]).await?;
        git(
            Some(&source),
            &["config", "user.email", "atomic@example.invalid"],
        )
        .await?;
        // Cross the selected-object page boundary with native delta candidates.
        for n in 0..600_u64 {
            let mut body = vec![b'x'; 8192];
            body[..8].copy_from_slice(&n.to_le_bytes());
            std::fs::write(source.join(format!("file-{n:03}")), body)?;
        }
        git(Some(&source), &["add", "."]).await?;
        git(Some(&source), &["commit", "-m", "Native paged objects"]).await?;
        git(
            Some(&source),
            &[
                "push",
                "--atomic",
                &url,
                "HEAD:refs/heads/main",
                "HEAD:refs/heads/sibling",
            ],
        )
        .await?;
        let refs = git(None, &["ls-remote", "--refs", &url]).await?;
        let clone = workspace.path().join(format!("clone-{format}"));
        git(None, &["clone", "--bare", &url, path(&clone)?]).await?;
        git(Some(&clone), &["fsck", "--strict", "--full"]).await?;
        assert_eq!(
            git(Some(&clone), &["rev-parse", "main"]).await?,
            git(Some(&source), &["rev-parse", "HEAD"]).await?
        );
        let api = format!("http://{}/api/repositories/{name}", server.local_addr());
        let client = reqwest::Client::new();
        let repository: serde_json::Value = client
            .get(&api)
            .bearer_auth(TOKEN)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        client
            .put(format!("{api}/branch-rules"))
            .bearer_auth(TOKEN)
            .json(
                &serde_json::json!({"repository_id":repository["repository_id"],"rule":{
                "reference":"refs/heads/main","expected_version":0,"enabled":true,
                "deny_deletions":false,"fast_forward_only":false,"require_pull_request":true,
                "required_approvals":0,"required_checks":[]}}),
            )
            .send()
            .await?
            .error_for_status()?;
        std::fs::write(source.join("later"), b"must not publish a sibling alone")?;
        git(Some(&source), &["add", "."]).await?;
        git(
            Some(&source),
            &["commit", "-m", "Reject entire native plan"],
        )
        .await?;
        let rejected = command(
            Some(&source),
            &[
                "push",
                "--atomic",
                &url,
                "HEAD:refs/heads/main",
                "HEAD:refs/heads/new-sibling",
            ],
        )
        .output()
        .await?;
        assert!(!rejected.status.success());
        let error = String::from_utf8(rejected.stderr)?;
        assert!(error.contains("[remote rejected]"), "{error}");
        assert_eq!(git(None, &["ls-remote", "--refs", &url]).await?, refs);
    }
    server.shutdown().await?;
    Ok(())
}
