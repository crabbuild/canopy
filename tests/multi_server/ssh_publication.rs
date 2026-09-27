use super::*;
use crate::paused_blobs::PausedBlobs;
use std::{path::PathBuf, process::Stdio, sync::atomic::Ordering};

struct Fixture {
    workspace: tempfile::TempDir,
    store: Arc<PausedBlobs>,
    server: CanopyServer,
    host: ssh_key::PrivateKey,
    key: PathBuf,
    source: PathBuf,
    ssh: String,
    url: String,
}

async fn fixture() -> Result<Fixture> {
    let workspace = tempfile::TempDir::new()?;
    let store = Arc::new(PausedBlobs::default());
    let host = ssh_key::PrivateKey::new(
        ssh_key::private::Ed25519Keypair::from_seed(&[12; 32]).into(),
        "test",
    )?;
    let address = available_address().await?;
    let server = CanopyServer::start(
        server_config(address, workspace.path().join("server"), &host)?,
        store.clone(),
    )
    .await?;
    create_repository(address, "publication").await?;
    let client = reqwest::Client::new();
    client.post(format!("http://{address}/api/accounts")).bearer_auth(AUTH)
        .json(&serde_json::json!({"name":"writer","token":format!("cnp_{}",hex::encode([62;32])),"scope":"write"}))
        .send().await?.error_for_status()?;
    client
        .put(format!(
            "http://{address}/api/repositories/publication/collaborators/writer"
        ))
        .bearer_auth(AUTH)
        .json(&serde_json::json!({"role":"write"}))
        .send()
        .await?
        .error_for_status()?;
    let key = key(workspace.path(), "writer").await?;
    register(address, "writer", &key, "write").await?;
    let ssh_address = server.ssh_addr().ok_or("SSH listener missing")?;
    let known = workspace.path().join("known_hosts");
    known_host(&known, ssh_address, &host).await?;
    let ssh = transport(&key, &known)?;
    let url = format!("ssh://git@{ssh_address}/canopy/publication.git");
    let source = workspace.path().join("source");
    git(None, &ssh, &["init", "-b", "main", path_str(&source)?]).await?;
    for (name, value) in [
        ("user.name", "Test"),
        ("user.email", "test@example.invalid"),
        ("commit.gpgsign", "false"),
    ] {
        git(Some(&source), &ssh, &["config", name, value]).await?;
    }
    git(
        Some(&source),
        &ssh,
        &["commit", "--allow-empty", "-m", "Initial"],
    )
    .await?;
    git(Some(&source), &ssh, &["push", &url, "main"]).await?;
    tokio::fs::write(source.join("large"), vec![0x6b; 2 * 1024 * 1024]).await?;
    git(Some(&source), &ssh, &["add", "large"]).await?;
    git(Some(&source), &ssh, &["commit", "-m", "External blob"]).await?;
    Ok(Fixture {
        workspace,
        store,
        server,
        host,
        key,
        source,
        ssh,
        url,
    })
}

async fn generation(client: &reqwest::Client, address: std::net::SocketAddr) -> Result<i64> {
    let response: serde_json::Value = client
        .get(format!(
            "http://{address}/api/repositories/publication/default-branch"
        ))
        .bearer_auth(AUTH)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    response["generation"]
        .as_i64()
        .ok_or("ref generation missing".into())
}

#[tokio::test(flavor = "multi_thread")]
async fn late_ssh_push_refusals_report_both_refs_and_survive_restore() -> Result {
    for change in ["policy", "access", "storage"] {
        let Fixture {
            workspace,
            store,
            server,
            host,
            key: _,
            source,
            ssh,
            url,
        } = fixture().await?;
        let address = server.local_addr();
        let client = reqwest::Client::new();
        let original = git(None, &ssh, &["ls-remote", "--refs", &url]).await?;
        let before = generation(&client, address).await?;
        store.armed.store(true, Ordering::SeqCst);
        let mut push = git_command(
            Some(&source),
            &ssh,
            &[
                "push",
                "--atomic",
                &url,
                "HEAD:refs/heads/main",
                "HEAD:refs/heads/early-sibling",
            ],
        )
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
        tokio::time::timeout(Duration::from_secs(20), store.entered.notified()).await?;
        assert!(
            push.try_wait()?.is_none(),
            "client completed before publication"
        );
        assert_eq!(
            git(None, &ssh, &["ls-remote", "--refs", &url]).await?,
            original
        );
        let api = format!("http://{address}/api/repositories/publication");
        if change == "policy" {
            let repository: serde_json::Value = client
                .get(&api)
                .bearer_auth(AUTH)
                .send()
                .await?
                .error_for_status()?
                .json()
                .await?;
            client
                .put(format!("{api}/branch-rules"))
                .bearer_auth(AUTH)
                .json(
                    &serde_json::json!({"repository_id":repository["repository_id"],"rule":{
                    "reference":"refs/heads/main","expected_version":0,"enabled":true,
                    "deny_deletions":false,"fast_forward_only":false,"require_pull_request":true,
                    "required_approvals":0,"required_checks":[]}}),
                )
                .send()
                .await?
                .error_for_status()?;
        } else if change == "access" {
            client
                .put(format!("{api}/collaborators/writer"))
                .bearer_auth(AUTH)
                .json(&serde_json::json!({"role":"read"}))
                .send()
                .await?
                .error_for_status()?;
        }
        store.fail.store(change == "storage", Ordering::SeqCst);
        store.proceed.notify_one();
        let output =
            tokio::time::timeout(Duration::from_secs(20), push.wait_with_output()).await??;
        let error = String::from_utf8(output.stderr)?;
        assert!(!output.status.success(), "{change}: {error}");
        let reason = if change == "storage" {
            "Canopy object ingestion failed"
        } else {
            "Canopy publication rejected"
        };
        for reference in ["main", "early-sibling"] {
            assert!(
                error.lines().any(|line| line.contains("[remote rejected]")
                    && line.contains(&format!(" -> {reference} "))
                    && line.contains(reason)),
                "{change}: {error}"
            );
        }
        assert_eq!(
            git(None, &ssh, &["ls-remote", "--refs", &url]).await?,
            original
        );
        assert_eq!(generation(&client, address).await?, before);
        server.shutdown().await?;

        let address = available_address().await?;
        let restored = CanopyServer::start(
            server_config(address, workspace.path().join("restored"), &host)?,
            store,
        )
        .await?;
        let ssh_address = restored.ssh_addr().ok_or("SSH listener missing")?;
        known_host(&workspace.path().join("known_hosts"), ssh_address, &host).await?;
        let url = format!("ssh://git@{ssh_address}/canopy/publication.git");
        assert_eq!(
            git(None, &ssh, &["ls-remote", "--refs", &url]).await?,
            original
        );
        assert_eq!(generation(&client, address).await?, before);
        let clone = workspace.path().join("clone.git");
        git(None, &ssh, &["clone", "--mirror", &url, path_str(&clone)?]).await?;
        git(Some(&clone), &ssh, &["fsck", "--strict", "--full"]).await?;
        restored.shutdown().await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn disconnected_ssh_push_finishes_publication_before_shutdown_releases_cells() -> Result {
    let Fixture {
        workspace,
        store,
        server,
        host,
        key,
        source,
        ssh,
        url: _,
    } = fixture().await?;
    let address = server.local_addr();
    let current = String::from_utf8(git(Some(&source), &ssh, &["rev-parse", "HEAD^"]).await?)?
        .trim()
        .to_owned();
    let next = String::from_utf8(git(Some(&source), &ssh, &["rev-parse", "HEAD"]).await?)?
        .trim()
        .to_owned();
    let commands = format!("{current} {next} refs/heads/main\0report-status atomic\n");
    let mut body = format!("{:04x}{commands}0000", commands.len() + 4).into_bytes();
    body.extend(git(Some(&source), &ssh, &["pack-objects", "--all", "--stdout"]).await?);
    let session = connect(
        server.ssh_addr().ok_or("SSH listener missing")?,
        &host,
        &key,
        "git",
        true,
    )
    .await?;
    let mut channel = session.channel_open_session().await?;
    channel
        .exec(true, "git-receive-pack 'canopy/publication.git'")
        .await?;
    tokio::time::timeout(Duration::from_secs(5), async {
        let mut advertised = Vec::new();
        loop {
            match channel.wait().await.ok_or("SSH advertisement missing")? {
                russh::ChannelMsg::Data { data } => {
                    advertised.extend_from_slice(&data);
                    if advertised.ends_with(b"0000") {
                        return Ok::<_, Box<dyn std::error::Error>>(());
                    }
                }
                russh::ChannelMsg::Close | russh::ChannelMsg::Failure => {
                    return Err("SSH push rejected".into());
                }
                _ => {}
            }
        }
    })
    .await??;
    store.armed.store(true, Ordering::SeqCst);
    channel.data(body.as_slice()).await?;
    channel.eof().await?;
    tokio::time::timeout(Duration::from_secs(20), store.entered.notified()).await?;
    session
        .disconnect(russh::Disconnect::ByApplication, "test disconnect", "")
        .await?;
    // Russh's client loop reports a locally initiated disconnect as Disconnect;
    // awaiting it still proves that the transport task has finished.
    let ended = tokio::time::timeout(Duration::from_secs(5), session).await?;
    assert!(
        matches!(ended, Ok(()) | Err(russh::Error::Disconnect)),
        "{ended:?}"
    );
    drop(channel);
    let draining = tokio::spawn(server.shutdown());
    let client = reqwest::Client::builder()
        .timeout(Duration::from_millis(500))
        .build()?;
    tokio::time::timeout(Duration::from_secs(5), async {
        while client
            .get(format!("http://{address}/readyz"))
            .send()
            .await
            .is_ok()
        {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    assert!(
        !draining.is_finished(),
        "shutdown abandoned an admitted push"
    );
    store.proceed.notify_one();
    tokio::time::timeout(Duration::from_secs(20), draining).await???;

    let address = available_address().await?;
    let restored = CanopyServer::start(
        server_config(address, workspace.path().join("restored"), &host)?,
        store,
    )
    .await?;
    let ssh_address = restored.ssh_addr().ok_or("SSH listener missing")?;
    known_host(&workspace.path().join("known_hosts"), ssh_address, &host).await?;
    let url = format!("ssh://git@{ssh_address}/canopy/publication.git");
    let clone = workspace.path().join("clone");
    git(None, &ssh, &["clone", &url, path_str(&clone)?]).await?;
    assert_eq!(
        String::from_utf8(git(Some(&clone), &ssh, &["rev-parse", "HEAD"]).await?)?.trim(),
        next
    );
    assert_eq!(
        tokio::fs::read(clone.join("large")).await?,
        vec![0x6b; 2 * 1024 * 1024]
    );
    git(Some(&clone), &ssh, &["fsck", "--strict", "--full"]).await?;
    restored.shutdown().await?;
    Ok(())
}
