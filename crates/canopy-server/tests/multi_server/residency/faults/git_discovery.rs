use super::*;

#[tokio::test(flavor = "multi_thread")]
async fn ref_discovery_does_not_wait_for_a_full_history_restore() -> Result {
    let fixture = Fixture::new().await?;
    let source = fixture.workspace.path().join("source");
    let url = format!("http://{}/canopy/original.git", fixture.address);
    let mut body = vec![0; 2 * 1024 * 1024];
    blake3::Hasher::new().finalize_xof().fill(&mut body);
    tokio::fs::write(source.join("history.bin"), &body).await?;
    run_git(Some(&source), &["add", "history.bin"]).await?;
    run_git(Some(&source), &["commit", "-m", "External history body"]).await?;
    run_git(
        Some(&source),
        &[
            "-c",
            "http.extraHeader=Authorization: Bearer local-test-token",
            "push",
            &url,
            "HEAD:refs/heads/main",
        ],
    )
    .await?;
    let oid = run_git(Some(&source), &["rev-parse", "HEAD"]).await?;
    let advertised = format!("{} refs/heads/main", std::str::from_utf8(&oid)?.trim());
    // Pause the physical native pack body rather than the retired loose-blob
    // layout. This fixture's incompressible history occupies the unique large
    // pack part; manifests and structural metadata remain available.
    let suffix = "/pack.parts/0000000000000000";
    let mut stored = fixture.store.inner.list(None);
    let mut blob = None;
    while let Some(meta) = std::future::poll_fn(|cx| stored.as_mut().poll_next(cx)).await {
        let meta = meta?;
        if meta.location.as_ref().ends_with(suffix) && meta.size >= body.len() as u64 {
            assert!(blob.replace(meta.location).is_none());
        }
    }
    // Publication and maintenance can temporarily exclude the original from
    // eviction. Establish the actual cold precondition with the same bounded
    // helper as the other cold-restore tests before starting discovery.
    fixture.make_original_cold().await?;
    assert!(!fixture.repository_dir.exists());
    *fixture.store.paused_read.lock().unwrap() =
        Some(blob.ok_or("external native pack body missing")?);
    let destination = fixture.workspace.path().join("cold-clone");
    let clone = async {
        run_git(
            None,
            &[
                "-c",
                "http.extraHeader=Authorization: Bearer local-test-token",
                "clone",
                &url,
                path_str(&destination)?,
            ],
        )
        .await
    };
    let discovery = async {
        let result = async {
            fixture.store.wait().await?;
            let metadata = fixture
                .client
                .get(format!(
                    "http://{}/api/repositories/original",
                    fixture.address
                ))
                .bearer_auth("local-test-token");
            timeout(Duration::from_secs(2), metadata.send())
                .await??
                .error_for_status()?;
            let requests = [
                fixture
                    .client
                    .get(format!("{url}/info/refs?service=git-upload-pack")),
                fixture
                    .client
                    .get(format!("{url}/info/refs?service=git-receive-pack")),
                fixture
                    .client
                    .post(format!("{url}/git-upload-pack"))
                    .header("Git-Protocol", "version=2")
                    .header("Content-Type", "application/x-git-upload-pack-request")
                    .body("0014command=ls-refs\n00010009peel\n000csymrefs\n0000"),
            ];
            for request in requests {
                let request = request.bearer_auth("local-test-token");
                let bytes = timeout(Duration::from_secs(2), async {
                    request.send().await?.error_for_status()?.bytes().await
                })
                .await??;
                if !bytes
                    .windows(advertised.len())
                    .any(|part| part == advertised.as_bytes())
                {
                    return Err("native ref listing omitted the published branch".into());
                }
            }
            Ok::<_, Box<dyn std::error::Error>>(())
        }
        .await;
        // Release the fault even when the regression times out, so the first
        // clone and supervised server shutdown finish before reporting failure.
        fixture.store.proceed.notify_one();
        result
    };
    let (cloned, discovered) = tokio::join!(clone, discovery);
    cloned?;
    assert_eq!(
        run_git(Some(&destination), &["rev-parse", "HEAD"]).await?,
        oid
    );
    assert_eq!(
        tokio::fs::read(destination.join("history.bin")).await?,
        body
    );
    run_git(Some(&destination), &["fsck", "--strict", "--full"]).await?;
    fixture.server.shutdown().await?;
    discovered
}
