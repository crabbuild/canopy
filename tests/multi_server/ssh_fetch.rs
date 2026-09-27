use super::*;
use crate::partial_clone::cache_contains;

#[tokio::test(flavor = "multi_thread")]
async fn cold_ssh_blobless_clones_hydrate_only_explicit_blob_wants() -> Result {
    let workspace = tempfile::TempDir::new()?;
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let host = ssh_key::PrivateKey::new(
        ssh_key::private::Ed25519Keypair::from_seed(&[13; 32]).into(),
        "test",
    )?;
    let address = available_address().await?;
    let server = CanopyServer::start(
        server_config(address, workspace.path().join("first"), &host)?,
        store.clone(),
    )
    .await?;
    create_repository(address, "filtered").await?;
    let key = key(workspace.path(), "client").await?;
    register(address, "canopy", &key, "write").await?;
    let ssh_address = server.ssh_addr().ok_or("SSH listener missing")?;
    let known = workspace.path().join("known_hosts");
    known_host(&known, ssh_address, &host).await?;
    let ssh = transport(&key, &known)?;
    let source = workspace.path().join("source");
    git(None, &ssh, &["init", "-b", "main", path_str(&source)?]).await?;
    for (name, value) in [
        ("user.name", "Test"),
        ("user.email", "test@example.invalid"),
        ("commit.gpgsign", "false"),
    ] {
        git(Some(&source), &ssh, &["config", name, value]).await?;
    }
    for (name, byte) in [("first", 0x61), ("other", 0x62)] {
        tokio::fs::write(source.join(name), vec![byte; 2 * 1024 * 1024]).await?;
    }
    tokio::fs::write(source.join("small"), b"explicit blob tag\n").await?;
    git(Some(&source), &ssh, &["add", "."]).await?;
    git(Some(&source), &ssh, &["commit", "-m", "Blobs"]).await?;
    git(
        Some(&source),
        &ssh,
        &[
            "-c",
            "tag.gpgsign=false",
            "tag",
            "-a",
            "blob-tag",
            "HEAD:small",
            "-m",
            "Blob",
        ],
    )
    .await?;
    let first = String::from_utf8(git(Some(&source), &ssh, &["rev-parse", "HEAD:first"]).await?)?
        .trim()
        .to_owned();
    let other = String::from_utf8(git(Some(&source), &ssh, &["rev-parse", "HEAD:other"]).await?)?
        .trim()
        .to_owned();
    let url = format!("ssh://git@{ssh_address}/canopy/filtered.git");
    git(Some(&source), &ssh, &["push", "--mirror", &url]).await?;
    server.shutdown().await?;

    for protocol in ["0", "2"] {
        let address = available_address().await?;
        let cold = workspace.path().join(format!("cold-{protocol}"));
        let restored =
            CanopyServer::start(server_config(address, cold.clone(), &host)?, store.clone())
                .await?;
        let ssh_address = restored.ssh_addr().ok_or("SSH listener missing")?;
        known_host(&known, ssh_address, &host).await?;
        let url = format!("ssh://git@{ssh_address}/canopy/filtered.git");
        let clone = workspace.path().join(format!("filtered-{protocol}"));
        let version = format!("protocol.version={protocol}");
        git(
            None,
            &ssh,
            &[
                "-c",
                &version,
                "clone",
                "--filter=blob:none",
                "--no-checkout",
                &url,
                path_str(&clone)?,
            ],
        )
        .await?;
        let stored = String::from_utf8(
            git(
                Some(&clone),
                &ssh,
                &[
                    "cat-file",
                    "--batch-all-objects",
                    "--batch-check=%(objectname)",
                ],
            )
            .await?,
        )?;
        assert!(!stored.contains(&first) && !stored.contains(&other));
        assert!(
            !cache_contains(&cold, &first)?,
            "v{protocol} hydrated omitted first blob"
        );
        assert!(
            !cache_contains(&cold, &other)?,
            "v{protocol} hydrated omitted other blob"
        );
        assert_eq!(
            git(Some(&clone), &ssh, &["-c", &version, "show", "HEAD:first"]).await?,
            vec![0x61; 2 * 1024 * 1024]
        );
        assert!(cache_contains(&cold, &first)?);
        assert!(
            !cache_contains(&cold, &other)?,
            "lazy fetch hydrated unrelated blob"
        );

        // A later unfiltered request must finish hydration before native Git
        // receives its wants, even though the snapshot cache began blobless.
        let full = workspace.path().join(format!("full-{protocol}"));
        git(
            None,
            &ssh,
            &["-c", &version, "clone", &url, path_str(&full)?],
        )
        .await?;
        assert_eq!(
            tokio::fs::read(full.join("other")).await?,
            vec![0x62; 2 * 1024 * 1024]
        );
        git(Some(&full), &ssh, &["fsck", "--strict", "--full"]).await?;
        restored.shutdown().await?;
    }
    Ok(())
}
