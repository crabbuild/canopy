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
    let tls = Arc::new(tls);
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
    let application = <canopy_server::CanopyApplication as cellule_app::CellApplication>::compile(
        canopy_server::build_descriptor(
            include_bytes!("../../Cargo.lock"),
            env!("CARGO_PKG_VERSION"),
        ),
    )?;
    let layout = cellule_runtime::CellStorageLayout::new(
        cellule_store::Store::new(Arc::clone(&store)),
        first_config.store_prefix.clone(),
        *first_config.application.as_bytes(),
    );
    let directory = cellule_runtime::NodeDirectory::new(
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
    for (name, ingress) in [("left", b), ("right", a)] {
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
    directory: &cellule_runtime::NodeDirectory,
    target: &cellule_runtime::CellTarget,
    key: SigningKey,
) -> Result {
    use cellule_runtime::{PeerOperation, PeerPrincipal, PeerSigner, SessionId, peer_wire as wire};
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
        let reply = cellule_runtime::decode_peer_reply(&response)?;
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

struct Proxy(tokio::task::JoinHandle<()>);
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
            let bytes = to_bytes(request.into_body(), cellule_runtime::MAX_PEER_REQUEST_BYTES)
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

async fn proxy(
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
