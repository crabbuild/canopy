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

#[tokio::test(flavor = "multi_thread")]
async fn cold_full_fetches_only_hydrate_blobs_reachable_from_requested_tips() -> Result {
    let workspace = tempfile::TempDir::new()?;
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let host = ssh_key::PrivateKey::new(
        ssh_key::private::Ed25519Keypair::from_seed(&[14; 32]).into(),
        "test",
    )?;
    let address = available_address().await?;
    let server = CanopyServer::start(
        server_config(address, workspace.path().join("first"), &host)?,
        store.clone(),
    )
    .await?;
    let url = create_repository(address, "selected").await?;
    let key = key(workspace.path(), "client").await?;
    register(address, "canopy", &key, "write").await?;
    let known = workspace.path().join("known_hosts");
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
    let auth = "http.extraHeader=Authorization: Bearer local-test-token";
    let mut blobs = Vec::new();
    for (branch, byte) in [("main", 0x61), ("other", 0x62), ("deleted", 0x63)] {
        if branch != "main" {
            git(Some(&source), &ssh, &["checkout", "--orphan", branch]).await?;
        }
        tokio::fs::write(source.join("asset"), vec![byte; 2 * 1024 * 1024]).await?;
        git(Some(&source), &ssh, &["add", "."]).await?;
        git(Some(&source), &ssh, &["commit", "-m", branch]).await?;
        blobs.push(
            String::from_utf8(git(Some(&source), &ssh, &["rev-parse", "HEAD:asset"]).await?)?
                .trim()
                .to_owned(),
        );
    }
    git(Some(&source), &ssh, &["-c", auth, "push", "--mirror", &url]).await?;
    git(
        Some(&source),
        &ssh,
        &["-c", auth, "push", &url, ":refs/heads/deleted"],
    )
    .await?;
    server.shutdown().await?;
    for transport_name in ["http", "ssh"] {
        for protocol in ["0", "2"] {
            let address = available_address().await?;
            let name = format!("{transport_name}-{protocol}");
            let cold = workspace.path().join(format!("cold-{name}"));
            let restored =
                CanopyServer::start(server_config(address, cold.clone(), &host)?, store.clone())
                    .await?;
            let ssh_address = restored.ssh_addr().ok_or("SSH listener missing")?;
            known_host(&known, ssh_address, &host).await?;
            let url = if transport_name == "ssh" {
                format!("ssh://git@{ssh_address}/canopy/selected.git")
            } else {
                format!("http://{address}/canopy/selected.git")
            };
            let clone = workspace.path().join(format!("clone-{name}"));
            let version = format!("protocol.version={protocol}");
            git(
                None,
                &ssh,
                &[
                    "-c",
                    auth,
                    "-c",
                    &version,
                    "clone",
                    "--single-branch",
                    "--branch",
                    "main",
                    "--no-tags",
                    &url,
                    path_str(&clone)?,
                ],
            )
            .await?;
            assert_eq!(
                tokio::fs::read(clone.join("asset")).await?,
                vec![0x61; 2 * 1024 * 1024]
            );
            git(Some(&clone), &ssh, &["fsck", "--strict", "--full"]).await?;
            assert!(cache_contains(&cold, &blobs[0])?);
            assert!(
                !cache_contains(&cold, &blobs[1])?,
                "{name}: unrelated branch blob hydrated"
            );
            assert!(
                !cache_contains(&cold, &blobs[2])?,
                "{name}: deleted branch blob hydrated"
            );
            let existing = workspace.path().join(format!("existing-{name}"));
            git(
                None,
                &ssh,
                &[
                    "clone",
                    "--no-hardlinks",
                    path_str(&clone)?,
                    path_str(&existing)?,
                ],
            )
            .await?;
            // A reused ref snapshot must prepare a different requested closure.
            git(
                Some(&clone),
                &ssh,
                &[
                    "-c",
                    auth,
                    "-c",
                    &version,
                    "fetch",
                    "origin",
                    "refs/heads/other:refs/remotes/origin/other",
                ],
            )
            .await?;
            assert_eq!(
                git(Some(&clone), &ssh, &["show", "origin/other:asset"]).await?,
                vec![0x62; 2 * 1024 * 1024]
            );
            assert!(cache_contains(&cold, &blobs[1])?);
            assert!(!cache_contains(&cold, &blobs[2])?);
            git(Some(&clone), &ssh, &["fsck", "--strict", "--full"]).await?;
            restored.shutdown().await?;

            // Negotiation haves may name an unrequested branch whose blob is
            // absent from a cold server. Native thin-pack preparation must work.
            let address = available_address().await?;
            let cold = workspace.path().join(format!("cold-haves-{name}"));
            let restored =
                CanopyServer::start(server_config(address, cold.clone(), &host)?, store.clone())
                    .await?;
            let ssh_address = restored.ssh_addr().ok_or("SSH listener missing")?;
            known_host(&known, ssh_address, &host).await?;
            let url = if transport_name == "ssh" {
                format!("ssh://git@{ssh_address}/canopy/selected.git")
            } else {
                format!("http://{address}/canopy/selected.git")
            };
            git(
                Some(&existing),
                &ssh,
                &["remote", "set-url", "origin", &url],
            )
            .await?;
            git(
                Some(&existing),
                &ssh,
                &[
                    "-c",
                    auth,
                    "-c",
                    &version,
                    "fetch",
                    "origin",
                    "refs/heads/other:refs/remotes/origin/other",
                ],
            )
            .await?;
            assert_eq!(
                git(Some(&existing), &ssh, &["show", "origin/other:asset"]).await?,
                vec![0x62; 2 * 1024 * 1024]
            );
            assert!(cache_contains(&cold, &blobs[1])?);
            assert!(
                !cache_contains(&cold, &blobs[0])?,
                "{name}: negotiation hydrated an unrequested blob"
            );
            assert!(!cache_contains(&cold, &blobs[2])?);
            git(Some(&existing), &ssh, &["fsck", "--strict", "--full"]).await?;
            restored.shutdown().await?;
        }
    }
    Ok(())
}
