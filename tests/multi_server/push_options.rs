use super::*;
use serde_json::Value;

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;
const AUTH: &str = "http.extraHeader=Authorization: Bearer local-test-token";

async fn capture_push(
    upstream: std::net::SocketAddr,
) -> Result<(
    std::net::SocketAddr,
    Arc<tokio::sync::Mutex<Option<(Vec<u8>, Option<String>)>>>,
    tokio_util::task::AbortOnDropHandle<()>,
)> {
    use axum::{
        Router,
        body::{Body, to_bytes},
        http::{Request, Response},
    };
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let captured = Arc::new(tokio::sync::Mutex::new(None));
    let pending = Arc::clone(&captured);
    let client = reqwest::Client::new();
    let route = move |request: Request<Body>| {
        let client = client.clone();
        let pending = Arc::clone(&pending);
        async move {
            let (mut parts, body) = request.into_parts();
            let bytes = to_bytes(body, 8 * 1024 * 1024).await.unwrap();
            if parts.method == axum::http::Method::POST
                && parts.uri.path().ends_with("/git-receive-pack")
            {
                let encoding = parts
                    .headers
                    .get("content-encoding")
                    .and_then(|value| value.to_str().ok())
                    .map(str::to_owned);
                *pending.lock().await = Some((bytes.to_vec(), encoding));
            }
            parts.headers.remove("host");
            let response = client
                .request(parts.method, format!("http://{upstream}{}", parts.uri))
                .headers(parts.headers)
                .body(bytes)
                .send()
                .await
                .unwrap();
            let status = response.status();
            let content_type = response.headers().get("content-type").cloned();
            let mut reply = Response::new(Body::from(response.bytes().await.unwrap()));
            *reply.status_mut() = status;
            if let Some(content_type) = content_type {
                reply.headers_mut().insert("content-type", content_type);
            }
            reply
        }
    };
    let task = tokio_util::task::AbortOnDropHandle::new(tokio::spawn(async move {
        axum::serve(listener, Router::new().fallback(route))
            .await
            .unwrap();
    }));
    Ok((address, captured, task))
}

#[tokio::test(flavor = "multi_thread")]
async fn signed_push_binds_registered_key_and_preserves_audit_after_restore() -> Result {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let workspace = tempfile::TempDir::new()?;
    let address = available_address().await?;
    let server = CanopyServer::start(
        config(address, workspace.path().join("first")),
        Arc::clone(&store),
    )
    .await?;
    let url = create_repository(address, "signed").await?;
    let source = workspace.path().join("source");
    run_git(None, &["init", "-b", "main", path_str(&source)?]).await?;
    run_git(Some(&source), &["config", "user.name", "Canopy Test"]).await?;
    run_git(
        Some(&source),
        &["config", "user.email", "test@example.invalid"],
    )
    .await?;
    tokio::fs::write(source.join("file"), b"signed push\n").await?;
    run_git(Some(&source), &["add", "file"]).await?;
    run_git(
        Some(&source),
        &["-c", "commit.gpgsign=false", "commit", "-m", "Signed"],
    )
    .await?;
    let key = workspace.path().join("signing-key");
    let generated = Command::new("ssh-keygen")
        .args(["-q", "-t", "ed25519", "-N", "", "-f", path_str(&key)?])
        .output()
        .await?;
    assert!(
        generated.status.success(),
        "{}",
        String::from_utf8_lossy(&generated.stderr)
    );
    let public_key = tokio::fs::read_to_string(key.with_extension("pub")).await?;
    let key_id = uuid::Uuid::new_v4();
    let client = reqwest::Client::new();
    client
        .post(format!("http://{address}/api/accounts/canopy/ssh-keys"))
        .bearer_auth("local-test-token")
        .json(&serde_json::json!({"id":key_id.to_string(),"public_key":public_key,"scope":"write"}))
        .send()
        .await?
        .error_for_status()?;
    let id = uuid::Uuid::new_v4();
    let header = format!("http.extraHeader=Idempotency-Key: {id}");
    let signing_key = format!("user.signingkey={}", key.display());
    let (proxy, captured, _proxy_task) = capture_push(address).await?;
    let proxy_url = format!("http://{proxy}/canopy/signed.git");
    let signed = Command::new("git")
        .current_dir(&source)
        .args([
            "-c",
            AUTH,
            "-c",
            &header,
            "-c",
            "gpg.format=ssh",
            "-c",
            &signing_key,
            "push",
            "--signed=true",
            "-o",
            "canopy.note=signed",
            &proxy_url,
            "HEAD:refs/heads/main",
        ])
        .output()
        .await?;
    assert!(
        signed.status.success(),
        "{}",
        String::from_utf8_lossy(&signed.stderr)
    );
    let receipt = format!("http://{address}/api/repositories/signed/pushes/{id}");
    let recorded: Value = client
        .get(&receipt)
        .bearer_auth("local-test-token")
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(recorded["push"]["certificate"]["signer"], "canopy");
    assert_eq!(
        recorded["push"]["options"],
        serde_json::json!(["canopy.note=signed"])
    );
    assert!(
        recorded["push"]["certificate"]["key"]
            .as_str()
            .is_some_and(|key| key.starts_with("SHA256:"))
    );
    assert_eq!(
        recorded["push"]["certificate"]["sha256"]
            .as_str()
            .map(str::len),
        Some(64)
    );
    assert!(
        recorded["push"]["certificate"]["recorded_at_ms"]
            .as_i64()
            .is_some()
    );
    run_git(
        Some(&source),
        &["-c", AUTH, "push", &url, ":refs/heads/main"],
    )
    .await?;
    server.shutdown().await?;
    let restored_address = available_address().await?;
    let restored = CanopyServer::start(
        config(restored_address, workspace.path().join("restored")),
        store,
    )
    .await?;
    let restored_url = format!("http://{restored_address}/canopy/signed.git");
    let recovered: Value = client
        .get(format!(
            "http://{restored_address}/api/repositories/signed/pushes/{id}"
        ))
        .bearer_auth("local-test-token")
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(recovered, recorded);
    let (body, encoding) = captured.lock().await.clone().ok_or("signed POST missing")?;
    let send_body = |data: Vec<u8>| {
        let request = client
            .post(format!("{restored_url}/git-receive-pack"))
            .bearer_auth("local-test-token")
            .header("Content-Type", "application/x-git-receive-pack-request")
            .header("Idempotency-Key", uuid::Uuid::new_v4().to_string())
            .body(data);
        if let Some(encoding) = &encoding {
            request.header("Content-Encoding", encoding)
        } else {
            request
        }
    };
    let marker = b"-----BEGIN SSH SIGNATURE-----\n";
    let start = body
        .windows(marker.len())
        .position(|window| window == marker)
        .ok_or("signature marker missing")?
        + marker.len()
        + 4;
    let mut forged = body.clone();
    let byte = forged.get_mut(start).ok_or("signature data missing")?;
    *byte = if *byte == b'A' { b'B' } else { b'A' };
    let forged_report = send_body(forged).send().await?.bytes().await?;
    assert!(
        String::from_utf8_lossy(&forged_report).contains("Canopy signed push verification failed")
    );
    assert!(
        run_git(
            None,
            &["-c", AUTH, "ls-remote", &restored_url, "refs/heads/main"]
        )
        .await?
        .is_empty()
    );
    let report = send_body(body).send().await?.bytes().await?;
    assert!(
        String::from_utf8_lossy(&report)
            .contains("Canopy signed push certificate was already used")
    );
    assert!(
        run_git(
            None,
            &["-c", AUTH, "ls-remote", &restored_url, "refs/heads/main"]
        )
        .await?
        .is_empty()
    );
    client
        .delete(format!(
            "http://{restored_address}/api/accounts/canopy/ssh-keys/{key_id}"
        ))
        .bearer_auth("local-test-token")
        .send()
        .await?
        .error_for_status()?;
    tokio::fs::write(source.join("file"), b"revoked key\n").await?;
    run_git(Some(&source), &["add", "file"]).await?;
    run_git(
        Some(&source),
        &["-c", "commit.gpgsign=false", "commit", "-m", "Revoked"],
    )
    .await?;
    let revoked = Command::new("git")
        .current_dir(&source)
        .args([
            "-c",
            AUTH,
            "-c",
            "gpg.format=ssh",
            "-c",
            &signing_key,
            "push",
            "--signed=true",
            &restored_url,
            "HEAD:refs/heads/main",
        ])
        .output()
        .await?;
    assert!(!revoked.status.success());
    assert!(String::from_utf8_lossy(&revoked.stderr).contains("signed push verification failed"));
    restored.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn mismatched_signed_push_options_return_a_durable_git_rejection() -> Result {
    let workspace = tempfile::TempDir::new()?;
    let address = available_address().await?;
    let server = CanopyServer::start(
        config(address, workspace.path().join("server")),
        Arc::new(InMemory::new()),
    )
    .await?;
    let url = create_repository(address, "signed-options").await?;
    let packet = |line: &str| format!("{:04x}{line}", line.len() + 4);
    let mut body = packet("push-cert\0report-status push-options\n");
    for line in [
        "certificate version 0.1\n".to_owned(),
        "push-option canopy.note=signed\n".to_owned(),
        "\n".to_owned(),
        format!("{} {} refs/heads/main\n", "0".repeat(40), "1".repeat(40)),
        "-----BEGIN SSH SIGNATURE-----\n".to_owned(),
        "-----END SSH SIGNATURE-----\n".to_owned(),
        "push-cert-end\n".to_owned(),
    ] {
        body.push_str(&packet(&line));
    }
    body.push_str("0000");
    body.push_str(&packet("canopy.note=outside"));
    body.push_str("0000");
    let id = uuid::Uuid::new_v4().to_string();
    let client = reqwest::Client::new();
    let send = || {
        client
            .post(format!("{url}/git-receive-pack"))
            .bearer_auth("local-test-token")
            .header("Content-Type", "application/x-git-receive-pack-request")
            .header("Idempotency-Key", &id)
            .body(body.clone())
    };
    let first = send().send().await?;
    assert_eq!(first.status(), reqwest::StatusCode::OK);
    let report = first.bytes().await?;
    assert!(
        String::from_utf8_lossy(&report)
            .contains("ng refs/heads/main Canopy signed push options do not match the request")
    );
    assert_eq!(send().send().await?.bytes().await?, report);
    let record: Value = client
        .get(format!(
            "http://{address}/api/repositories/signed-options/pushes/{id}"
        ))
        .bearer_auth("local-test-token")
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(record["push"]["options"], serde_json::json!([]));
    assert!(
        run_git(None, &["-c", AUTH, "ls-remote", &url, "refs/heads/main"])
            .await?
            .is_empty()
    );
    server.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn stock_git_push_options_are_validated_recorded_and_recovered() -> Result {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let workspace = tempfile::TempDir::new()?;
    let address = available_address().await?;
    let server = CanopyServer::start(
        config(address, workspace.path().join("first")),
        Arc::clone(&store),
    )
    .await?;
    let url = create_repository(address, "options").await?;
    let source = workspace.path().join("source");
    run_git(None, &["init", "-b", "main", path_str(&source)?]).await?;
    run_git(Some(&source), &["config", "user.name", "Canopy Test"]).await?;
    run_git(
        Some(&source),
        &["config", "user.email", "test@example.invalid"],
    )
    .await?;
    tokio::fs::write(source.join("file"), b"push option\n").await?;
    run_git(Some(&source), &["add", "file"]).await?;
    run_git(
        Some(&source),
        &["-c", "commit.gpgsign=false", "commit", "-m", "First"],
    )
    .await?;
    let first_oid = String::from_utf8(run_git(Some(&source), &["rev-parse", "HEAD"]).await?)?
        .trim()
        .to_owned();
    let first = uuid::Uuid::new_v4().to_string();
    let header = format!("http.extraHeader=Idempotency-Key: {first}");
    run_git(
        Some(&source),
        &[
            "-c",
            AUTH,
            "-c",
            &header,
            "push",
            "-o",
            "canopy.note=release candidate",
            "-o",
            "canopy.note=qa passed",
            &url,
            "HEAD:refs/heads/main",
            "HEAD:refs/heads/disposable",
        ],
    )
    .await?;
    let client = reqwest::Client::new();
    let receipt = format!("http://{address}/api/repositories/options/pushes/{first}");
    let record: Value = client
        .get(&receipt)
        .bearer_auth("local-test-token")
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(
        record["push"]["options"],
        serde_json::json!(["canopy.note=release candidate", "canopy.note=qa passed"])
    );
    assert_eq!(record["push"]["actor"], "canopy");
    let viewer = format!("cnp_{}", "88".repeat(32));
    client
        .post(format!("http://{address}/api/accounts"))
        .bearer_auth("local-test-token")
        .json(&serde_json::json!({"name":"viewer","token":viewer,"scope":"read"}))
        .send()
        .await?
        .error_for_status()?;
    client
        .put(format!(
            "http://{address}/api/repositories/options/collaborators/viewer"
        ))
        .bearer_auth("local-test-token")
        .json(&serde_json::json!({"role":"read"}))
        .send()
        .await?
        .error_for_status()?;
    assert_eq!(
        client
            .get(&receipt)
            .bearer_auth(&viewer)
            .send()
            .await?
            .status(),
        reqwest::StatusCode::NOT_FOUND
    );

    tokio::fs::write(source.join("file"), b"second version\n").await?;
    run_git(Some(&source), &["add", "file"]).await?;
    run_git(
        Some(&source),
        &["-c", "commit.gpgsign=false", "commit", "-m", "Second"],
    )
    .await?;
    let rejected = Command::new("git")
        .current_dir(&source)
        .args([
            "-c",
            AUTH,
            "push",
            "-o",
            "ci.skip",
            &url,
            "HEAD:refs/heads/main",
        ])
        .output()
        .await?;
    assert!(!rejected.status.success());
    assert!(String::from_utf8_lossy(&rejected.stderr).contains("canopy.note"));
    let current = run_git(None, &["-c", AUTH, "ls-remote", &url, "refs/heads/main"]).await?;
    assert_eq!(
        String::from_utf8(current)?.trim(),
        format!("{first_oid}\trefs/heads/main")
    );

    let deletion = uuid::Uuid::new_v4().to_string();
    let header = format!("http.extraHeader=Idempotency-Key: {deletion}");
    run_git(
        Some(&source),
        &[
            "-c",
            AUTH,
            "-c",
            &header,
            "push",
            "-o",
            "canopy.note=retire branch",
            &url,
            ":refs/heads/disposable",
        ],
    )
    .await?;
    let deletion_receipt = format!("http://{address}/api/repositories/options/pushes/{deletion}");
    let deleted: Value = client
        .get(&deletion_receipt)
        .bearer_auth("local-test-token")
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(
        deleted["push"]["options"],
        serde_json::json!(["canopy.note=retire branch"])
    );
    server.shutdown().await?;

    let restored_address = available_address().await?;
    let restored = CanopyServer::start(
        config(restored_address, workspace.path().join("restored")),
        store,
    )
    .await?;
    let restored_receipt =
        format!("http://{restored_address}/api/repositories/options/pushes/{first}");
    let recovered: Value = client
        .get(&restored_receipt)
        .bearer_auth("local-test-token")
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(recovered, record);
    let clone = workspace.path().join("clone");
    run_git(
        None,
        &[
            "-c",
            AUTH,
            "clone",
            &format!("http://{restored_address}/canopy/options.git"),
            path_str(&clone)?,
        ],
    )
    .await?;
    assert_eq!(
        run_git(Some(&clone), &["rev-parse", "HEAD"])
            .await?
            .trim_ascii(),
        first_oid.as_bytes()
    );
    run_git(Some(&clone), &["fsck", "--full", "--strict"]).await?;
    restored.shutdown().await?;
    Ok(())
}
