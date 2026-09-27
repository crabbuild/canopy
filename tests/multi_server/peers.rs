use super::*;
use reqwest::{Client, StatusCode};
use serde_json::json;

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

#[tokio::test(flavor = "multi_thread")]
async fn two_live_nodes_route_git_to_distinct_cell_owners_and_recover_the_directory() -> Result {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let files = tempfile::TempDir::new()?;
    let a = available_address().await?;
    let b = available_address().await?;
    let (ca, tls) = tls_config()?;
    let (forward_a, lose_reply, _forward_a) = response_loss_proxy(a).await?;
    let (peer_a, _proxy_a) = proxy(forward_a, Arc::clone(&tls)).await?;
    let (peer_b, _proxy_b) = proxy(b, tls).await?;
    let mut first_config = config(a, files.path().join("first"));
    first_config.peer_endpoint = peer_a;
    first_config.peer_ca_pem = Some(ca.clone());
    let mut second_config = config(b, files.path().join("second"));
    second_config.peer_endpoint = peer_b;
    second_config.peer_ca_pem = Some(ca);
    second_config.node = NodeId::from_bytes(uuid::Uuid::new_v4().into_bytes());
    second_config.signing_key = SigningKey::from_bytes(&[94; 32]);
    let signing_key = first_config.signing_key.clone();
    let target =
        canopy_server::directory::directory_target(first_config.tenant, first_config.application)?;
    let application =
        <canopy_server::CanopyApplication as crab_cell_app::CellApplication>::compile(
            canopy_server::build_descriptor(
                include_bytes!("../../Cargo.lock"),
                env!("CARGO_PKG_VERSION"),
            ),
        )?;
    let layout = crab_cell_runtime::ltx::CellStorageLayout::new(
        crab_storage::Store::new(Arc::clone(&store)),
        first_config.store_prefix.clone(),
        *first_config.application.as_bytes(),
    );
    let authority = crab_cell_runtime::control::authority::CellAuthority::new(layout.clone());
    let tenant = first_config.tenant;
    let application_id = first_config.application;
    let directory = crab_cell_runtime::node::NodeDirectory::new(
        layout,
        first_config.fleet,
        first_config.image,
        application.registry().release_digest(),
    );
    let first = CanopyServer::start(first_config, Arc::clone(&store)).await?;
    create_repository(a, "left").await?;
    let mut untrusted = config(b, files.path().join("untrusted"));
    untrusted.node = second_config.node;
    untrusted.peer_endpoint = second_config.peer_endpoint.clone();
    let rejected = CanopyServer::start(untrusted, Arc::clone(&store)).await;
    assert!(matches!(
        rejected,
        Err(canopy_server::server::ServerError::Directory(_))
    ));
    let second = CanopyServer::start(second_config, store).await?;
    reject_unauthorized_peers(a, &directory, &target, signing_key).await?;
    create_repository(b, "right").await?;
    let client = Client::new();
    let lost_token = format!("cnp_{}", hex::encode([82; 32]));
    let lost_account = json!({"name":"lost-account", "token":lost_token, "scope":"read"});
    lose_reply.store(true, std::sync::atomic::Ordering::SeqCst);
    let lost = client
        .post(format!("http://{b}/api/accounts"))
        .bearer_auth("local-test-token")
        .json(&lost_account)
        .send()
        .await?;
    assert_eq!(lost.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert!(!lose_reply.load(std::sync::atomic::Ordering::SeqCst));
    assert_eq!(
        client
            .get(format!("http://{b}/api/repositories"))
            .bearer_auth(&lost_token)
            .send()
            .await?
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        client
            .post(format!("http://{b}/api/accounts"))
            .bearer_auth("local-test-token")
            .json(&lost_account)
            .send()
            .await?
            .status(),
        StatusCode::OK
    );
    for address in [a, b] {
        let response = client
            .post(format!("http://{address}/internal/cell"))
            .body("unsigned peer request")
            .send()
            .await?;
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }
    let mut repositories = Vec::new();
    for (name, ingress, owner) in [("left", b, a), ("right", a, b)] {
        let local: serde_json::Value = client
            .get(format!("http://{owner}/api/repositories/{name}"))
            .bearer_auth("local-test-token")
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        let repository_id = uuid::Uuid::parse_str(
            local["repository_id"]
                .as_str()
                .ok_or("missing repository ID")?,
        )?;
        let repository_target =
            canopy_server::repository_target(tenant, application_id, *repository_id.as_bytes())?;
        let before = authority
            .load(repository_target.cell_id())
            .await?
            .ok_or("Cell missing")?;
        let detail: serde_json::Value = client
            .get(format!("http://{ingress}/api/repositories/{name}"))
            .bearer_auth("local-test-token")
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        let id = uuid::Uuid::parse_str(
            detail["repository_id"]
                .as_str()
                .ok_or("missing repository ID")?,
        )?;
        assert_eq!(id, repository_id);
        let after = authority
            .load(repository_target.cell_id())
            .await?
            .ok_or("Cell missing")?;
        assert_eq!(
            after.value().root,
            before.value().root,
            "remote read published a new root"
        );
        repositories.push((name, id));
        let source = files.path().join(name);
        run_git(None, &["init", "-b", "main", path_str(&source)?]).await?;
        run_git(Some(&source), &["config", "user.name", "Canopy Test"]).await?;
        run_git(
            Some(&source),
            &["config", "user.email", "canopy@example.invalid"],
        )
        .await?;
        std::fs::write(source.join("README.md"), name.as_bytes())?;
        run_git(Some(&source), &["add", "."]).await?;
        run_git(Some(&source), &["commit", "-m", "Remote Cell fixture"]).await?;
        let url = format!("http://{ingress}/canopy/{name}.git");
        run_git(
            Some(&source),
            &[
                "-c",
                "http.extraHeader=Authorization: Bearer local-test-token",
                "push",
                &url,
                "main",
            ],
        )
        .await?;
        clone(&files, ingress, name, "live").await?;
    }
    for (name, id) in repositories {
        let node = if name == "left" { "first" } else { "second" };
        let other = if node == "first" { "second" } else { "first" };
        let path = format!(
            "runtime-v1/{}/repository.sqlite",
            hex::encode(id.as_bytes())
        );
        assert!(files.path().join(node).join(&path).exists());
        assert!(!files.path().join(other).join(path).exists());
    }
    let token = format!("cnp_{}", hex::encode([81; 32]));
    client
        .post(format!("http://{b}/api/accounts"))
        .bearer_auth("local-test-token")
        .json(&json!({"name":"member", "token":token, "scope":"read"}))
        .send()
        .await?
        .error_for_status()?;
    first.shutdown().await?;
    // B must restore the Directory Cell on demand without restarting its ingress.
    let response = client
        .get(format!("http://{b}/api/repositories"))
        .bearer_auth(&token)
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    for name in ["left", "right"] {
        clone(&files, b, name, "restored").await?;
    }
    second.shutdown().await?;
    Ok(())
}

async fn reject_unauthorized_peers(
    address: std::net::SocketAddr,
    directory: &crab_cell_runtime::node::NodeDirectory,
    target: &crab_cell_runtime::CellTarget,
    key: SigningKey,
) -> Result {
    use crab_cell_runtime::{
        SessionId, peer::PeerOperation, peer::PeerPrincipal, peer::PeerSigner, peer::wire,
    };
    let now = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis(),
    )?;
    let endpoint = format!("http://{address}");
    let nodes = directory.live(now, 10).await?;
    let node = nodes
        .iter()
        .find(|node| node.verifying_key().ok() == Some(key.verifying_key()))
        .ok_or("missing node advertisement")?;
    let operation = PeerOperation::Read(wire::ReadRequest {
        target: Some(wire::Target {
            tenant_id: target.tenant().as_bytes().to_vec(),
            application_id: target.application().as_bytes().to_vec(),
            namespace_id: target.namespace().as_bytes().to_vec(),
            partition: target.partition().to_vec(),
        }),
        timeout_ms: 30_000,
        minimum: None,
        expected: None,
        operation: Some(wire::read_request::Operation::Describe(true)),
    });
    let client = Client::new();
    for (session, key, issued) in [
        (node.session(), SigningKey::from_bytes(&[95; 32]), now),
        (
            SessionId::from_bytes(uuid::Uuid::new_v4().into_bytes()),
            key.clone(),
            now,
        ),
        (node.session(), key.clone(), now - 61_000),
    ] {
        let signer = PeerSigner::new(session, node.release(), key);
        let principal = PeerPrincipal {
            issuer: "canopy".into(),
            subject: "node".into(),
            actions: vec!["canopy.cell".into()],
        };
        let bytes = signer.sign(
            principal,
            issued,
            issued + 60_000,
            30_000,
            operation.clone(),
        )?;
        assert_eq!(
            client
                .post(format!("{endpoint}/internal/cell"))
                .body(bytes)
                .send()
                .await?
                .status(),
            StatusCode::FORBIDDEN
        );
    }
    let signer = PeerSigner::new(node.session(), node.release(), key);
    for (issuer, action) in [("other", "canopy.cell"), ("canopy", "other")] {
        let principal = PeerPrincipal {
            issuer: issuer.into(),
            subject: "node".into(),
            actions: vec![action.into()],
        };
        let bytes = signer.sign(principal, now, now + 60_000, 30_000, operation.clone())?;
        let response = client
            .post(format!("{endpoint}/internal/cell"))
            .body(bytes)
            .send()
            .await?
            .error_for_status()?
            .bytes()
            .await?;
        let reply = crab_cell_runtime::peer::decode_peer_reply(&response)?;
        assert!(matches!(
            reply.outcome,
            Some(wire::peer_reply::Outcome::Error(_))
        ));
    }
    Ok(())
}

async fn clone(
    files: &tempfile::TempDir,
    address: std::net::SocketAddr,
    name: &str,
    phase: &str,
) -> Result {
    let path = files.path().join(format!("{name}-{phase}"));
    let url = format!("http://{address}/canopy/{name}.git");
    run_git(
        None,
        &[
            "-c",
            "http.extraHeader=Authorization: Bearer local-test-token",
            "clone",
            &url,
            path_str(&path)?,
        ],
    )
    .await?;
    assert_eq!(std::fs::read(path.join("README.md"))?, name.as_bytes());
    run_git(Some(&path), &["fsck", "--strict", "--full"]).await?;
    Ok(())
}

pub(super) struct Proxy(tokio::task::JoinHandle<()>);
impl Drop for Proxy {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn response_loss_proxy(
    upstream: std::net::SocketAddr,
) -> Result<(
    std::net::SocketAddr,
    Arc<std::sync::atomic::AtomicBool>,
    Proxy,
)> {
    use axum::{
        Router,
        body::{Body, to_bytes},
        http::{Request, Response},
        routing::post,
    };
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let lose_reply = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let fault = Arc::clone(&lose_reply);
    let client = Client::new();
    let route = post(move |request: Request<Body>| {
        let client = client.clone();
        let fault = Arc::clone(&fault);
        async move {
            let bytes = to_bytes(
                request.into_body(),
                crab_cell_runtime::peer::MAX_PEER_REQUEST_BYTES,
            )
            .await
            .unwrap();
            let mutation = bytes
                .windows(b"lost-account".len())
                .any(|part| part == b"lost-account");
            let response = client
                .post(format!("http://{upstream}/internal/cell"))
                .body(bytes)
                .send()
                .await
                .unwrap();
            let status = response.status();
            let body = response.bytes().await.unwrap();
            // The full reply proves the owner finished dispatch. Replace only
            // this mutation's response; a client retry would incorrectly hide it.
            let mut result = Response::new(Body::from(body));
            *result.status_mut() =
                if mutation && fault.swap(false, std::sync::atomic::Ordering::SeqCst) {
                    StatusCode::SERVICE_UNAVAILABLE
                } else {
                    status
                };
            result
        }
    });
    let task = tokio::spawn(async move {
        axum::serve(listener, Router::new().route("/internal/cell", route))
            .await
            .unwrap();
    });
    Ok((address, lose_reply, Proxy(task)))
}

pub(super) async fn proxy(
    upstream: std::net::SocketAddr,
    config: Arc<tokio_rustls::rustls::ServerConfig>,
) -> Result<(String, Proxy)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("https://{}", listener.local_addr()?);
    let acceptor = tokio_rustls::TlsAcceptor::from(config);
    let task = tokio::spawn(async move {
        let mut tasks = tokio::task::JoinSet::new();
        loop {
            tokio::select! {
                connection = listener.accept() => {
                    let Ok((socket, _)) = connection else { break };
                    let acceptor = acceptor.clone();
                    tasks.spawn(async move {
                        let Ok(mut socket) = acceptor.accept(socket).await else { return };
                        let Ok(mut target) = tokio::net::TcpStream::connect(upstream).await else { return };
                        let _ = tokio::io::copy_bidirectional(&mut socket, &mut target).await;
                    });
                }
                _ = tasks.join_next(), if !tasks.is_empty() => {}
            }
        }
    });
    Ok((endpoint, Proxy(task)))
}

pub(super) fn tls_config() -> Result<(Vec<u8>, Arc<tokio_rustls::rustls::ServerConfig>)> {
    let certified = rcgen::generate_simple_self_signed(vec!["127.0.0.1".into()])?;
    let ca = certified.cert.pem().into_bytes();
    let tls = tokio_rustls::rustls::ServerConfig::builder_with_provider(Arc::new(
        tokio_rustls::rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()?
    .with_no_client_auth()
    .with_single_cert(
        vec![certified.cert.der().clone()],
        tokio_rustls::rustls::pki_types::PrivatePkcs8KeyDer::from(
            certified.signing_key.serialize_der(),
        )
        .into(),
    )?;
    Ok((ca, Arc::new(tls)))
}

#[tokio::test(flavor = "multi_thread")]
async fn moving_repositories_preserve_history_and_serialize_cross_gateway_pushes() -> Result {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();
    use canopy_server::repository_target;
    use crab_cell_runtime::{
        control::ControlState, control::authority::CellAuthority, ltx::CellStorageLayout,
    };
    use crab_storage::Store;

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let files = tempfile::TempDir::new()?;
    let a = available_address().await?;
    let b = available_address().await?;
    let (ca, tls) = tls_config()?;
    let (peer_a, _proxy_a) = proxy(a, Arc::clone(&tls)).await?;
    let (peer_b, _proxy_b) = proxy(b, tls).await?;
    let mut settings_a = config(a, files.path().join("first"));
    settings_a.peer_endpoint = peer_a;
    settings_a.peer_ca_pem = Some(ca.clone());
    let mut settings_b = config(b, files.path().join("second"));
    settings_b.peer_endpoint = peer_b;
    settings_b.peer_ca_pem = Some(ca);
    settings_b.node = NodeId::from_bytes(uuid::Uuid::new_v4().into_bytes());
    settings_b.signing_key = SigningKey::from_bytes(&[94; 32]);
    let tenant = settings_a.tenant;
    let application = settings_a.application;
    let authority = CellAuthority::new(CellStorageLayout::new(
        Store::new(Arc::clone(&store)),
        settings_a.store_prefix.clone(),
        *application.as_bytes(),
    ));
    let first = CanopyServer::start(settings_a, Arc::clone(&store)).await?;
    let second = CanopyServer::start(settings_b, store).await?;
    let source = files.path().join("source");
    run_git(None, &["init", "-b", "main", path_str(&source)?]).await?;
    run_git(Some(&source), &["config", "user.name", "Canopy Test"]).await?;
    run_git(
        Some(&source),
        &["config", "user.email", "canopy@example.invalid"],
    )
    .await?;
    let client = Client::new();
    let mut repositories = Vec::new();
    for index in 0..8 {
        let name = format!("moving-{index}");
        let (owner, ingress) = if index % 2 == 0 { (a, b) } else { (b, a) };
        create_repository(owner, &name).await?;
        let detail: serde_json::Value = client
            .get(format!("http://{owner}/api/repositories/{name}"))
            .bearer_auth("local-test-token")
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        let id = uuid::Uuid::parse_str(detail["repository_id"].as_str().ok_or("missing ID")?)?;
        let target = repository_target(tenant, application, id.into_bytes())?;
        std::fs::write(source.join("README.md"), name.as_bytes())?;
        run_git(Some(&source), &["add", "."]).await?;
        run_git(Some(&source), &["commit", "-m", &name]).await?;
        let url = format!("http://{ingress}/canopy/{name}.git");
        run_git(
            Some(&source),
            &[
                "-c",
                "http.extraHeader=Authorization: Bearer local-test-token",
                "push",
                &url,
                "main",
            ],
        )
        .await?;
        let oid = run_git(Some(&source), &["rev-parse", "HEAD"]).await?;
        repositories.push((name, target, oid));
    }
    let mut moved = false;
    for round in 0..2 {
        for (name, target, oid) in &repositories {
            let before = authority
                .load(target.cell_id())
                .await?
                .ok_or("missing control")?;
            let ingress = if round == 0 { a } else { b };
            let phase = format!("round-{round}");
            clone(&files, ingress, name, &phase).await?;
            let path = files.path().join(format!("{name}-{phase}"));
            assert_eq!(&run_git(Some(&path), &["rev-parse", "HEAD"]).await?, oid);
            let after = authority
                .load(target.cell_id())
                .await?
                .ok_or("missing control")?;
            moved |= before.value().owner != after.value().owner;
            assert_eq!(after.value().state, ControlState::Serving);
            let mut owners = std::collections::HashMap::new();
            for (_, other, _) in &repositories {
                let control = authority
                    .load(other.cell_id())
                    .await?
                    .ok_or("missing control")?;
                if let Some(owner) = &control.value().owner {
                    *owners.entry(owner.session).or_insert(0) += 1;
                }
            }
            assert!(owners.values().all(|count| *count <= 3));
        }
    }
    assert!(moved, "fixture must exercise owner changes");

    // Both clients start from the same ref and submit distinct commits. A request
    // through a remote gateway must not bypass the owner's final ref comparison.
    let (name, _, base) = &repositories[7];
    let left = files.path().join(format!("{name}-round-0"));
    let right = files.path().join(format!("{name}-round-1"));
    for (path, body) in [(&left, "left"), (&right, "right")] {
        run_git(Some(path), &["config", "user.name", "Canopy Test"]).await?;
        run_git(
            Some(path),
            &["config", "user.email", "canopy@example.invalid"],
        )
        .await?;
        std::fs::write(path.join("race.txt"), body)?;
        run_git(Some(path), &["add", "."]).await?;
        run_git(Some(path), &["commit", "-m", body]).await?;
    }
    let lease = format!(
        "--force-with-lease=refs/heads/main:{}",
        std::str::from_utf8(base)?.trim()
    );
    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    let (race_a, _race_a) = push_barrier_proxy(a, Arc::clone(&barrier)).await?;
    let (race_b, _race_b) = push_barrier_proxy(b, barrier).await?;
    let push = |path: std::path::PathBuf, address| {
        let url = format!("http://{address}/canopy/{name}.git");
        let lease = lease.clone();
        async move {
            Command::new("git")
                .current_dir(path)
                .env("GIT_TERMINAL_PROMPT", "0")
                .args([
                    "-c",
                    "credential.helper=",
                    "-c",
                    "http.extraHeader=Authorization: Bearer local-test-token",
                    "push",
                    &lease,
                    &url,
                    "HEAD:refs/heads/main",
                ])
                .output()
                .await
        }
    };
    let (left_result, right_result) =
        tokio::join!(push(left.clone(), race_a), push(right.clone(), race_b));
    let (left_result, right_result) = (left_result?, right_result?);
    assert_ne!(
        left_result.status.success(),
        right_result.status.success(),
        "left: {}; right: {}",
        String::from_utf8_lossy(&left_result.stderr),
        String::from_utf8_lossy(&right_result.stderr)
    );
    let rejected = if left_result.status.success() {
        &right_result
    } else {
        &left_result
    };
    let rejection = String::from_utf8_lossy(&rejected.stderr);
    assert!(
        rejection.contains("[remote rejected]"),
        "expected a ref rejection, not transport or admission failure: {rejection}"
    );
    let winner = if left_result.status.success() {
        &left
    } else {
        &right
    };
    let winner_oid = run_git(Some(winner), &["rev-parse", "HEAD"]).await?;
    first.shutdown().await?;
    for (index, (name, _, oid)) in repositories.iter().enumerate() {
        clone(&files, b, name, "survivor").await?;
        let path = files.path().join(format!("{name}-survivor"));
        let expected = if index == 7 { &winner_oid } else { oid };
        assert_eq!(
            &run_git(Some(&path), &["rev-parse", "HEAD"]).await?,
            expected
        );
        if index == 7 {
            assert_eq!(
                std::fs::read(path.join("race.txt"))?,
                std::fs::read(winner.join("race.txt"))?
            );
        }
    }
    second.shutdown().await?;
    Ok(())
}

async fn push_barrier_proxy(
    upstream: std::net::SocketAddr,
    barrier: Arc<tokio::sync::Barrier>,
) -> Result<(std::net::SocketAddr, Proxy)> {
    use axum::{
        Router,
        body::{Body, to_bytes},
        http::{Request, Response},
    };
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let client = Client::new();
    let route = move |request: Request<Body>| {
        let client = client.clone();
        let barrier = Arc::clone(&barrier);
        async move {
            let (mut parts, body) = request.into_parts();
            let body = to_bytes(body, 1024 * 1024).await.unwrap();
            if parts.uri.path().ends_with("/git-receive-pack") {
                // Both stock clients have advertised the same old ref and sent
                // their entire update before either RPC reaches its gateway.
                tokio::time::timeout(std::time::Duration::from_secs(10), barrier.wait())
                    .await
                    .unwrap();
            }
            parts.headers.remove("host");
            let response = client
                .request(parts.method, format!("http://{upstream}{}", parts.uri))
                .headers(parts.headers)
                .body(body)
                .send()
                .await
                .unwrap();
            let status = response.status();
            let content_type = response.headers().get("content-type").cloned();
            let mut response = Response::new(Body::from(response.bytes().await.unwrap()));
            *response.status_mut() = status;
            if let Some(content_type) = content_type {
                response.headers_mut().insert("content-type", content_type);
            }
            response
        }
    };
    let task = tokio::spawn(async move {
        axum::serve(listener, Router::new().fallback(route))
            .await
            .unwrap();
    });
    Ok((address, Proxy(task)))
}
