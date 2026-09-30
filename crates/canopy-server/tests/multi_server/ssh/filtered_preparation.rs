use super::*;
use crate::partial_clone::cache_contains;

#[tokio::test(flavor = "multi_thread")]
async fn cold_filters_keep_omitted_blobs_out_of_server_cache() -> Result {
    let workspace = tempfile::TempDir::new()?;
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let host = ssh_key::PrivateKey::new(
        ssh_key::private::Ed25519Keypair::from_seed(&[15; 32]).into(),
        "test",
    )?;
    let address = available_address().await?;
    let server = CanopyServer::start(
        server_config(address, workspace.path().join("first"), &host)?,
        store.clone(),
    )
    .await?;
    let url = create_repository(address, "filters").await?;
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
    tokio::fs::create_dir_all(source.join("sub/deep")).await?;
    let paths = ["asset", "sub/asset", "sub/deep/asset"];
    for (index, path) in paths.iter().enumerate() {
        tokio::fs::write(source.join(path), vec![index as u8; 2 * 1024 * 1024]).await?;
    }
    git(Some(&source), &ssh, &["add", "."]).await?;
    git(Some(&source), &ssh, &["commit", "-m", "Nested blobs"]).await?;
    let mut oids = Vec::new();
    for path in paths {
        let revision = format!("HEAD:{path}");
        oids.push(
            String::from_utf8(git(Some(&source), &ssh, &["rev-parse", &revision]).await?)?
                .trim()
                .to_owned(),
        );
    }
    let auth = "http.extraHeader=Authorization: Bearer local-test-token";
    git(Some(&source), &ssh, &["-c", auth, "push", &url, "main"]).await?;
    server.shutdown().await?;

    for transport_name in ["http", "ssh"] {
        for protocol in ["0", "2"] {
            // Git counts a root tree at depth zero and its blobs at depth one.
            for (index, (filter, included)) in [
                ("tree:0", [false, false, false]),
                ("tree:1", [false, false, false]),
                ("tree:2", [true, false, false]),
                ("tree:3", [true, true, false]),
                ("blob:none", [false, false, false]),
                ("object:type=commit", [false, false, false]),
                ("object:type=tree", [false, false, false]),
                ("object:type=tag", [false, false, false]),
                ("blob:limit=1m", [false, false, false]),
                ("object:type=blob", [true, true, true]),
                ("combine:tree:2+blob:none", [false, false, false]),
                ("combine:tree:3+object:type%3Dblob", [true, true, false]),
                (
                    "combine:combine%3Atree%253A3%2Bobject%253Atype%253Dblob+blob:limit=3m",
                    [true, true, false],
                ),
            ]
            .into_iter()
            .enumerate()
            {
                // Git's size filter includes missing blobs until their sizes
                // are available; the client still omits bodies above the limit.
                let hydrated = if filter == "blob:limit=1m" {
                    [true; 3]
                } else {
                    included
                };
                let name = format!("{transport_name}-{protocol}-{index}");
                let cold = workspace.path().join(format!("cold-{name}"));
                let address = available_address().await?;
                let restored = CanopyServer::start(
                    server_config(address, cold.clone(), &host)?,
                    store.clone(),
                )
                .await?;
                let ssh_address = restored.ssh_addr().ok_or("SSH listener missing")?;
                known_host(&known, ssh_address, &host).await?;
                let url = if transport_name == "ssh" {
                    format!("ssh://git@{ssh_address}/canopy/filters.git")
                } else {
                    format!("http://{address}/canopy/filters.git")
                };
                let clone = workspace.path().join(format!("clone-{name}"));
                let version = format!("protocol.version={protocol}");
                let selection = format!("--filter={filter}");
                git(
                    None,
                    &ssh,
                    &[
                        "-c",
                        auth,
                        "-c",
                        &version,
                        "clone",
                        &selection,
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
                for ((oid, expected), hydrated) in oids.iter().zip(included).zip(hydrated) {
                    assert_eq!(
                        stored.lines().any(|line| line == oid),
                        expected,
                        "{name} {filter}: client {oid}"
                    );
                    assert_eq!(
                        cache_contains(&cold, oid)?,
                        hydrated,
                        "{name} {filter}: server {oid}"
                    );
                }
                // Explicit lazy wants must override filtering without filling other
                // omitted bodies through the reused server snapshot.
                assert_eq!(
                    git(
                        Some(&clone),
                        &ssh,
                        &["-c", auth, "-c", &version, "show", "HEAD:sub/deep/asset"]
                    )
                    .await?,
                    vec![2; 2 * 1024 * 1024]
                );
                assert!(cache_contains(&cold, &oids[2])?);
                for (oid, expected) in oids[..2].iter().zip(hydrated) {
                    assert_eq!(
                        cache_contains(&cold, oid)?,
                        expected,
                        "{name} {filter}: lazy fetch hydrated {oid}"
                    );
                }
                git(Some(&clone), &ssh, &["fsck", "--strict", "--full"]).await?;
                restored.shutdown().await?;
            }
        }
    }
    Ok(())
}
