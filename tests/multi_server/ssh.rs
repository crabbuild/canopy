#[path = "ssh_fetch.rs"]
mod fetch;
#[path = "filtered_preparation.rs"]
mod filtered_preparation;
#[path = "ssh_lfs.rs"]
mod lfs;
#[path = "ssh_publication.rs"]
mod publication;

use super::*;
use canopy_server::ssh::SshConfig;
use std::time::Duration;

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;
const AUTH: &str = "local-test-token";

fn quote(value: &Path) -> Result<String> {
    Ok(format!("'{}'", path_str(value)?.replace('\'', "'\\''")))
}

async fn key(root: &Path, name: &str) -> Result<std::path::PathBuf> {
    let path = root.join(name);
    let output = Command::new("ssh-keygen")
        .args(["-q", "-t", "ed25519", "-N", "", "-f", path_str(&path)?])
        .output()
        .await?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(path)
}

async fn register(
    address: std::net::SocketAddr,
    account: &str,
    key: &Path,
    scope: &str,
) -> Result<String> {
    let id = uuid::Uuid::new_v4().to_string();
    reqwest::Client::new().post(format!("http://{address}/api/accounts/{account}/ssh-keys"))
        .bearer_auth(AUTH).json(&serde_json::json!({"id":id,"public_key":tokio::fs::read_to_string(key.with_extension("pub")).await?,"scope":scope}))
        .send().await?.error_for_status()?;
    Ok(id)
}

fn git_command(path: Option<&Path>, ssh: &str, args: &[&str]) -> Command {
    let mut cmd = Command::new("git");
    cmd.env("GIT_SSH_COMMAND", ssh)
        .env("GIT_TERMINAL_PROMPT", "0")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .args(args)
        .kill_on_drop(true);
    if let Some(path) = path {
        cmd.current_dir(path);
    }
    cmd
}

async fn git(path: Option<&Path>, ssh: &str, args: &[&str]) -> Result<Vec<u8>> {
    let output = tokio::time::timeout(
        Duration::from_secs(20),
        git_command(path, ssh, args).output(),
    )
    .await??;
    assert!(
        output.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(output.stdout)
}

fn transport(key: &Path, known: &Path) -> Result<String> {
    Ok(format!(
        "ssh -F /dev/null -o BatchMode=yes -o IdentitiesOnly=yes -o StrictHostKeyChecking=yes -o UserKnownHostsFile={} -i {}",
        quote(known)?,
        quote(key)?
    ))
}

fn server_config(
    address: std::net::SocketAddr,
    data_dir: std::path::PathBuf,
    host: &ssh_key::PrivateKey,
) -> Result<ServerConfig> {
    let mut cfg = config(address, data_dir);
    cfg.ssh = Some(SshConfig {
        listen: "127.0.0.1:0".parse()?,
        host_key: host.clone(),
    });
    Ok(cfg)
}

async fn known_host(
    path: &Path,
    address: std::net::SocketAddr,
    key: &ssh_key::PrivateKey,
) -> Result {
    tokio::fs::write(
        path,
        format!(
            "[{}]:{} {}\n",
            address.ip(),
            address.port(),
            key.public_key().to_openssh()?
        ),
    )
    .await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn sha256_ssh_push_and_clone() -> Result {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let workspace = tempfile::TempDir::new()?;
    let host = ssh_key::PrivateKey::new(
        ssh_key::private::Ed25519Keypair::from_seed(&[9; 32]).into(),
        "canopy-test",
    )?;
    let address = available_address().await?;
    let server = CanopyServer::start(
        server_config(address, workspace.path().join("node"), &host)?,
        Arc::clone(&store),
    )
    .await?;
    reqwest::Client::new()
        .post(format!("http://{address}/api/repositories"))
        .bearer_auth(AUTH)
        .json(&serde_json::json!({"name": "sha256-ssh", "object_format": "sha256"}))
        .send()
        .await?
        .error_for_status()?;
    let ssh_address = server.ssh_addr().ok_or("SSH listener missing")?;
    let known = workspace.path().join("known_hosts");
    known_host(&known, ssh_address, &host).await?;
    let key = key(workspace.path(), "client").await?;
    register(address, "canopy", &key, "write").await?;
    let ssh = transport(&key, &known)?;
    let url = format!("ssh://git@{ssh_address}/canopy/sha256-ssh.git");
    let source = workspace.path().join("source");
    git(
        None,
        &ssh,
        &[
            "init",
            "--object-format=sha256",
            "-b",
            "main",
            path_str(&source)?,
        ],
    )
    .await?;
    git(Some(&source), &ssh, &["config", "user.name", "Canopy Test"]).await?;
    git(
        Some(&source),
        &ssh,
        &["config", "user.email", "test@example.invalid"],
    )
    .await?;
    tokio::fs::write(source.join("file"), b"sha256 over ssh\n").await?;
    git(Some(&source), &ssh, &["add", "file"]).await?;
    git(
        Some(&source),
        &ssh,
        &["-c", "commit.gpgsign=false", "commit", "-m", "first"],
    )
    .await?;
    git(Some(&source), &ssh, &["push", &url, "HEAD:refs/heads/main"]).await?;
    let clone = workspace.path().join("clone");
    git(None, &ssh, &["clone", &url, path_str(&clone)?]).await?;
    assert_eq!(
        git(Some(&clone), &ssh, &["rev-parse", "HEAD"]).await?,
        git(Some(&source), &ssh, &["rev-parse", "HEAD"]).await?
    );
    git(Some(&clone), &ssh, &["fsck", "--full", "--strict"]).await?;
    server.shutdown().await?;
    let restored_address = available_address().await?;
    let restored = CanopyServer::start(
        server_config(restored_address, workspace.path().join("restored"), &host)?,
        store,
    )
    .await?;
    let restored_ssh_address = restored.ssh_addr().ok_or("SSH listener missing")?;
    known_host(&known, restored_ssh_address, &host).await?;
    let restored_url = format!("ssh://git@{restored_ssh_address}/canopy/sha256-ssh.git");
    let recovered = workspace.path().join("recovered");
    git(None, &ssh, &["clone", &restored_url, path_str(&recovered)?]).await?;
    assert_eq!(
        git(Some(&recovered), &ssh, &["rev-parse", "HEAD"]).await?,
        git(Some(&source), &ssh, &["rev-parse", "HEAD"]).await?
    );
    assert_eq!(
        tokio::fs::read(recovered.join("file")).await?,
        b"sha256 over ssh\n"
    );
    git(Some(&recovered), &ssh, &["fsck", "--full", "--strict"]).await?;
    restored.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn stock_ssh_clone_push_fetch_filters_and_revocation_survive_disk_loss() -> Result {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let workspace = tempfile::TempDir::new()?;
    let host = ssh_key::PrivateKey::new(
        ssh_key::private::Ed25519Keypair::from_seed(&[7; 32]).into(),
        "canopy-test",
    )?;
    let address = available_address().await?;
    let server = CanopyServer::start(
        server_config(address, workspace.path().join("first"), &host)?,
        Arc::clone(&store),
    )
    .await?;
    let ssh_address = server.ssh_addr().ok_or("SSH listener missing")?;
    let known = workspace.path().join("known_hosts");
    known_host(&known, ssh_address, &host).await?;
    let key = key(workspace.path(), "client").await?;
    let key_id = register(address, "canopy", &key, "write").await?;
    let ssh = transport(&key, &known)?;
    create_repository(address, "ssh").await?;
    let url = format!("ssh://git@{ssh_address}/canopy/ssh.git");
    let source = workspace.path().join("source");
    git(None, &ssh, &["init", "-b", "main", path_str(&source)?]).await?;
    let mut large = vec![0; 9 * 1024 * 1024];
    blake3::Hasher::new()
        .update(b"ssh-external-blob")
        .finalize_xof()
        .fill(&mut large);
    tokio::fs::write(source.join("large"), &large).await?;
    git(Some(&source), &ssh, &["config", "user.name", "Canopy Test"]).await?;
    git(
        Some(&source),
        &ssh,
        &["config", "user.email", "test@example.invalid"],
    )
    .await?;
    for revision in 1..=3 {
        tokio::fs::write(source.join("file"), format!("revision {revision}\n")).await?;
        git(Some(&source), &ssh, &["add", "."]).await?;
        git(
            Some(&source),
            &ssh,
            &["-c", "commit.gpgsign=false", "commit", "-m", "Revision"],
        )
        .await?;
    }
    git(Some(&source), &ssh, &["branch", "開発"]).await?;
    git(Some(&source), &ssh, &["tag", "version"]).await?;
    git(Some(&source), &ssh, &["notes", "add", "-m", "SSH note"]).await?;
    git(Some(&source), &ssh, &["checkout", "-b", "discarded"]).await?;
    tokio::fs::write(source.join("file"), b"unreachable contents\n").await?;
    git(
        Some(&source),
        &ssh,
        &["-c", "commit.gpgsign=false", "commit", "-am", "Discarded"],
    )
    .await?;
    let mut unreachable = Vec::new();
    for revision in ["HEAD", "HEAD^{tree}", "HEAD:file"] {
        unreachable.push(
            String::from_utf8(git(Some(&source), &ssh, &["rev-parse", revision]).await?)?
                .trim()
                .to_owned(),
        );
    }
    git(Some(&source), &ssh, &["checkout", "main"]).await?;
    git(Some(&source), &ssh, &["push", "--mirror", &url]).await?;
    git(
        Some(&source),
        &ssh,
        &["push", &url, ":refs/heads/discarded"],
    )
    .await?;
    git(Some(&source), &ssh, &["branch", "-D", "discarded"]).await?;
    let session = connect(ssh_address, &host, &key, "git", true).await?;
    for protocol in ["version=0", "version=2"] {
        for oid in &unreachable {
            let want = format!("want {oid}\n");
            let prefix = if protocol == "version=2" {
                "0012command=fetch\n0001"
            } else {
                ""
            };
            let request = format!("{prefix}{:04x}{want}0000", want.len() + 4);
            let (code, output, error) = rpc(
                &session,
                "git-upload-pack 'canopy/ssh.git'",
                protocol,
                request.as_bytes(),
            )
            .await?;
            assert_eq!(code, 1, "{protocol} {oid}");
            assert!(!output.windows(4).any(|bytes| bytes == b"PACK"));
            assert!(String::from_utf8(error)?.contains("not reachable"));
        }
    }
    session
        .disconnect(russh::Disconnect::ByApplication, "", "")
        .await?;
    for protocol in ["0", "1", "2"] {
        let clone = workspace.path().join(format!("clone-{protocol}"));
        git(
            None,
            &ssh,
            &[
                "-c",
                &format!("protocol.version={protocol}"),
                "clone",
                &url,
                path_str(&clone)?,
            ],
        )
        .await?;
        git(Some(&clone), &ssh, &["fsck", "--strict", "--full"]).await?;
        assert_eq!(tokio::fs::read(clone.join("file")).await?, b"revision 3\n");
        assert_eq!(tokio::fs::read(clone.join("large")).await?, large);
    }
    let shallow = workspace.path().join("shallow");
    git(
        None,
        &ssh,
        &["clone", "--depth=1", &url, path_str(&shallow)?],
    )
    .await?;
    git(Some(&shallow), &ssh, &["fetch", "--unshallow"]).await?;
    assert_eq!(
        git(Some(&shallow), &ssh, &["rev-list", "--count", "HEAD"]).await?,
        b"3\n"
    );
    let filtered = workspace.path().join("filtered");
    git(
        None,
        &ssh,
        &[
            "clone",
            "--filter=blob:none",
            "--no-checkout",
            &url,
            path_str(&filtered)?,
        ],
    )
    .await?;
    assert_eq!(
        git(Some(&filtered), &ssh, &["show", "HEAD:file"]).await?,
        b"revision 3\n"
    );
    tokio::fs::write(source.join("file"), b"incremental\n").await?;
    git(Some(&source), &ssh, &["commit", "-am", "Incremental"]).await?;
    git(Some(&source), &ssh, &["push", &url, "main"]).await?;
    git(Some(&shallow), &ssh, &["pull", "--ff-only"]).await?;
    assert_eq!(
        tokio::fs::read(shallow.join("file")).await?,
        b"incremental\n"
    );
    git(Some(&source), &ssh, &["push", &url, ":refs/heads/開発"]).await?;
    git(Some(&source), &ssh, &["branch", "-D", "開発"]).await?;
    git(Some(&shallow), &ssh, &["fetch", "--prune"]).await?;
    let expected = git(Some(&source), &ssh, &["show-ref"]).await?;
    server.shutdown().await?;
    let address = available_address().await?;
    let restored = CanopyServer::start(
        server_config(address, workspace.path().join("restored"), &host)?,
        store,
    )
    .await?;
    let ssh_address = restored.ssh_addr().ok_or("SSH listener missing")?;
    known_host(&known, ssh_address, &host).await?;
    let url = format!("ssh://git@{ssh_address}/canopy/ssh.git");
    let clone = workspace.path().join("restored.git");
    git(None, &ssh, &["clone", "--mirror", &url, path_str(&clone)?]).await?;
    assert_eq!(git(Some(&clone), &ssh, &["show-ref"]).await?, expected);
    git(Some(&clone), &ssh, &["fsck", "--strict", "--full"]).await?;
    assert_eq!(
        git(Some(&clone), &ssh, &["show", "main:large"]).await?,
        large
    );
    reqwest::Client::new()
        .delete(format!(
            "http://{address}/api/accounts/canopy/ssh-keys/{key_id}"
        ))
        .bearer_auth(AUTH)
        .send()
        .await?
        .error_for_status()?;
    let denied = git_command(None, &ssh, &["ls-remote", &url])
        .output()
        .await?;
    assert!(!denied.status.success(), "revoked key authenticated");
    restored.shutdown().await?;
    Ok(())
}

struct PinnedHost(ssh_key::PublicKey);

impl russh::client::Handler for PinnedHost {
    type Error = russh::Error;

    async fn check_server_key(
        &mut self,
        public: &russh::keys::PublicKeyOrCertificate,
    ) -> std::result::Result<bool, Self::Error> {
        Ok(
            matches!(public, russh::keys::PublicKeyOrCertificate::PublicKey { key, .. } if key.key_data() == self.0.key_data()),
        )
    }
}

async fn connect(
    address: std::net::SocketAddr,
    host: &ssh_key::PrivateKey,
    key: &Path,
    user: &str,
    accepted: bool,
) -> Result<russh::client::Handle<PinnedHost>> {
    let config = russh::client::Config {
        inactivity_timeout: Some(Duration::from_secs(10)),
        ..Default::default()
    };
    let mut session = russh::client::connect(
        Arc::new(config),
        address,
        PinnedHost(host.public_key().clone()),
    )
    .await?;
    let key = ssh_key::PrivateKey::from_openssh(tokio::fs::read(key).await?)?;
    let auth = session
        .authenticate_publickey(
            user,
            russh::keys::PrivateKeyWithHashAlg::new(Arc::new(key), None),
        )
        .await?;
    assert_eq!(auth.success(), accepted);
    Ok(session)
}

async fn exec(
    session: &russh::client::Handle<PinnedHost>,
    command: &str,
) -> Result<(u32, Vec<u8>, Vec<u8>)> {
    rpc(session, command, "version=2", b"").await
}

async fn rpc(
    session: &russh::client::Handle<PinnedHost>,
    command: &str,
    protocol: &str,
    request: &[u8],
) -> Result<(u32, Vec<u8>, Vec<u8>)> {
    tokio::time::timeout(Duration::from_secs(10), async {
        let mut channel = session.channel_open_session().await?;
        channel.set_env(true, "GIT_PROTOCOL", protocol).await?;
        channel.exec(true, command).await?;
        channel.data(request).await?;
        channel.eof().await?;
        let mut code = None;
        let mut output = Vec::new();
        let mut error = Vec::new();
        while let Some(message) = channel.wait().await {
            match message {
                russh::ChannelMsg::ExitStatus { exit_status } => code = Some(exit_status),
                russh::ChannelMsg::Data { data } => output.extend_from_slice(&data),
                russh::ChannelMsg::ExtendedData { data, .. } => error.extend_from_slice(&data),
                _ => {}
            }
        }
        Ok::<_, Box<dyn std::error::Error>>((code.ok_or("SSH exit status missing")?, output, error))
    })
    .await?
}

#[tokio::test(flavor = "multi_thread")]
async fn ssh_channels_enforce_scope_acl_and_revocation_after_login() -> Result {
    let workspace = tempfile::TempDir::new()?;
    let host = ssh_key::PrivateKey::new(
        ssh_key::private::Ed25519Keypair::from_seed(&[8; 32]).into(),
        "test",
    )?;
    let address = available_address().await?;
    let server = CanopyServer::start(
        server_config(address, workspace.path().join("server"), &host)?,
        Arc::new(InMemory::new()),
    )
    .await?;
    let ssh_address = server.ssh_addr().ok_or("SSH listener missing")?;
    create_repository(address, "private").await?;
    let read_key = key(workspace.path(), "reader").await?;
    let id = register(address, "canopy", &read_key, "read").await?;
    let reader = connect(ssh_address, &host, &read_key, "git", true).await?;
    let (code, output, _) = exec(&reader, "git-upload-pack 'canopy/private.git'").await?;
    assert_eq!(code, 0);
    assert!(output.starts_with(b"000eversion 2\n"));
    let (code, output, error) = exec(&reader, "git-receive-pack 'canopy/private.git'").await?;
    assert_eq!((code, output.as_slice()), (1, b"".as_slice()));
    assert!(String::from_utf8(error)?.contains("read-only"));

    // The same authenticated connection must observe registry changes when a
    // new command starts; a successful login cannot retain revoked authority.
    let client = reqwest::Client::new();
    client
        .delete(format!(
            "http://{address}/api/accounts/canopy/ssh-keys/{id}"
        ))
        .bearer_auth(AUTH)
        .send()
        .await?
        .error_for_status()?;
    let (code, output, error) = exec(&reader, "git-upload-pack 'canopy/private.git'").await?;
    assert_eq!((code, output.as_slice()), (1, b"".as_slice()));
    assert!(String::from_utf8(error)?.contains("revoked"));
    reader
        .disconnect(russh::Disconnect::ByApplication, "", "")
        .await?;

    let member_key = key(workspace.path(), "member").await?;
    let token = format!("cnp_{}", hex::encode([61; 32]));
    client
        .post(format!("http://{address}/api/accounts"))
        .bearer_auth(AUTH)
        .json(&serde_json::json!({"name":"member","token":token,"scope":"admin"}))
        .send()
        .await?
        .error_for_status()?;
    client
        .post(format!("http://{address}/api/accounts/member/ssh-keys"))
        .bearer_auth(&token)
        .json(
            &serde_json::json!({"id":uuid::Uuid::new_v4().to_string(),"scope":"write",
            "public_key":tokio::fs::read_to_string(member_key.with_extension("pub")).await?}),
        )
        .send()
        .await?
        .error_for_status()?;
    let member = connect(ssh_address, &host, &member_key, "git", true).await?;
    let (code, output, _) = exec(&member, "git-upload-pack 'canopy/private.git'").await?;
    assert_eq!((code, output.as_slice()), (1, b"".as_slice()));
    client
        .put(format!(
            "http://{address}/api/repositories/private/collaborators/member"
        ))
        .bearer_auth(AUTH)
        .json(&serde_json::json!({"role":"read"}))
        .send()
        .await?
        .error_for_status()?;
    assert_eq!(
        exec(&member, "git-upload-pack 'canopy/private.git'")
            .await?
            .0,
        0
    );
    let (code, output, _) = exec(&member, "git-receive-pack 'canopy/private.git'").await?;
    assert_eq!((code, output.as_slice()), (1, b"".as_slice()));
    member
        .disconnect(russh::Disconnect::ByApplication, "", "")
        .await?;

    for (key, user) in [(&read_key, "git"), (&member_key, "root")] {
        let denied = connect(ssh_address, &host, key, user, false).await?;
        denied
            .disconnect(russh::Disconnect::ByApplication, "", "")
            .await?;
    }
    server.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn ssh_rejects_shell_injection_environment_and_forwarding() -> Result {
    let workspace = tempfile::TempDir::new()?;
    let host = ssh_key::PrivateKey::new(
        ssh_key::private::Ed25519Keypair::from_seed(&[9; 32]).into(),
        "test",
    )?;
    let address = available_address().await?;
    let server = CanopyServer::start(
        server_config(address, workspace.path().join("server"), &host)?,
        Arc::new(InMemory::new()),
    )
    .await?;
    let key = key(workspace.path(), "client").await?;
    register(address, "canopy", &key, "write").await?;
    let session = connect(
        server.ssh_addr().ok_or("SSH listener missing")?,
        &host,
        &key,
        "git",
        true,
    )
    .await?;
    for command in [
        "sh",
        "git-upload-archive 'canopy/repo.git'",
        "git-lfs-transfer canopy/repo.git download",
        "git-lfs-authenticate canopy/repo.git delete",
        "git-lfs-authenticate '../canopy/repo.git' download",
        "git-lfs-authenticate 'canopy/repo.git' download; id",
        "git-upload-pack '../canopy/repo.git'",
        "git-upload-pack 'canopy/repo.git'; id",
        "git-upload-pack 'canopy/$(id).git'",
        "git-upload-pack 'canopy/repo.git'\nwhoami",
        "git-upload-pack --strict 'canopy/repo.git'",
    ] {
        let mut channel = session.channel_open_session().await?;
        channel.exec(true, command).await?;
        assert!(
            matches!(
                tokio::time::timeout(Duration::from_secs(5), channel.wait()).await?,
                Some(russh::ChannelMsg::Failure)
            ),
            "{command}"
        );
        channel.close().await?;
    }
    for request in ["shell", "subsystem", "environment"] {
        let mut channel = session.channel_open_session().await?;
        match request {
            "shell" => channel.request_shell(true).await?,
            "subsystem" => channel.request_subsystem(true, "sftp").await?,
            _ => channel.set_env(true, "GIT_CONFIG_COUNT", "1").await?,
        }
        assert!(
            matches!(
                tokio::time::timeout(Duration::from_secs(5), channel.wait()).await?,
                Some(russh::ChannelMsg::Failure)
            ),
            "{request}"
        );
        channel.close().await?;
    }
    assert!(
        tokio::time::timeout(
            Duration::from_secs(5),
            session.channel_open_direct_tcpip(
                "127.0.0.1",
                address.port() as u32,
                "127.0.0.1",
                1234
            )
        )
        .await?
        .is_err()
    );
    let mut premature = session.channel_open_session().await?;
    premature.data(b"input before exec".as_slice()).await?;
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(5), premature.wait()).await?,
        Some(russh::ChannelMsg::Close)
    ));
    session
        .disconnect(russh::Disconnect::ByApplication, "", "")
        .await?;
    server.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn ssh_fetches_share_http_admission_and_release_on_close_and_shutdown() -> Result {
    let workspace = tempfile::TempDir::new()?;
    let host = ssh_key::PrivateKey::new(
        ssh_key::private::Ed25519Keypair::from_seed(&[10; 32]).into(),
        "test",
    )?;
    let address = available_address().await?;
    let server = CanopyServer::start(
        server_config(address, workspace.path().join("server"), &host)?,
        Arc::new(InMemory::new()),
    )
    .await?;
    let url = create_repository(address, "shared").await?;
    let key = key(workspace.path(), "client").await?;
    register(address, "canopy", &key, "write").await?;
    let session = connect(
        server.ssh_addr().ok_or("SSH listener missing")?,
        &host,
        &key,
        "git",
        true,
    )
    .await?;
    let mut held = Vec::new();
    for _ in 0..4 {
        let mut channel = session.channel_open_session().await?;
        channel.set_env(true, "GIT_PROTOCOL", "version=2").await?;
        channel
            .exec(true, "git-upload-pack 'canopy/shared.git'")
            .await?;
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                match channel
                    .wait()
                    .await
                    .ok_or("channel ended before Git advertisement")?
                {
                    russh::ChannelMsg::Data { data } if data.starts_with(b"000eversion 2\n") => {
                        break Ok::<_, Box<dyn std::error::Error>>(());
                    }
                    russh::ChannelMsg::Failure | russh::ChannelMsg::Close => {
                        return Err("fetch rejected".into());
                    }
                    _ => {}
                }
            }
        })
        .await??;
        held.push(channel);
    }
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()?;
    let refs = format!("{url}/info/refs?service=git-upload-pack");
    let response = client.get(&refs).bearer_auth(AUTH).send().await?;
    assert_eq!(response.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
    held.pop().ok_or("missing held channel")?.close().await?;
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let response = client.get(&refs).bearer_auth(AUTH).send().await?;
            if response.status() == reqwest::StatusCode::OK {
                assert!(
                    response
                        .bytes()
                        .await?
                        .starts_with(b"001e# service=git-upload-pack\n")
                );
                break Ok::<_, Box<dyn std::error::Error>>(());
            }
            assert_eq!(response.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
            tokio::task::yield_now().await;
        }
    })
    .await??;
    // Three native upload-pack workers are still waiting for requests. Drain
    // must cancel those reads before closing Cells and the workspace fence.
    tokio::time::timeout(Duration::from_secs(5), server.shutdown()).await??;
    drop(held);
    drop(session);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn openssh_authenticates_registered_rsa_and_ecdsa_keys() -> Result {
    let workspace = tempfile::TempDir::new()?;
    let host = ssh_key::PrivateKey::new(
        ssh_key::private::Ed25519Keypair::from_seed(&[11; 32]).into(),
        "test",
    )?;
    let address = available_address().await?;
    let server = CanopyServer::start(
        server_config(address, workspace.path().join("server"), &host)?,
        Arc::new(InMemory::new()),
    )
    .await?;
    create_repository(address, "algorithms").await?;
    let ssh_address = server.ssh_addr().ok_or("SSH listener missing")?;
    let known = workspace.path().join("known_hosts");
    known_host(&known, ssh_address, &host).await?;
    let url = format!("ssh://git@{ssh_address}/canopy/algorithms.git");
    for (algorithm, bits) in [
        ("ecdsa", "256"),
        ("ecdsa", "384"),
        ("ecdsa", "521"),
        ("rsa", "2048"),
    ] {
        let path = workspace.path().join(format!("{algorithm}-{bits}"));
        let output = Command::new("ssh-keygen")
            .args([
                "-q",
                "-t",
                algorithm,
                "-b",
                bits,
                "-N",
                "",
                "-f",
                path_str(&path)?,
            ])
            .output()
            .await?;
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        register(address, "canopy", &path, "read").await?;
        let ssh = transport(&path, &known)?;
        assert!(git(None, &ssh, &["ls-remote", &url]).await?.is_empty());
    }
    server.shutdown().await?;
    Ok(())
}
