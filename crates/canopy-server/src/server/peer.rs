//! Signed Cell RPC routed through current durable ownership.

use super::*;
use axum::{
    body::{Body, to_bytes},
    extract::State,
    http::StatusCode,
    response::Response,
};
use cellule_runtime::{
    cell::actor::CellHandle, peer::MAX_PEER_REQUEST_BYTES, peer::PeerAuthorizer,
    peer::PeerCellResolver, peer::PeerDispatcher, peer::PeerPrincipal, peer::PeerRoundTrip,
    peer::PeerSigner, peer::PeerVerifier, peer::VerifiedPeerRequest,
};
use std::{future::Future, pin::Pin};
use tokio::sync::Semaphore;

pub(crate) const PATH: &str = "/internal/cell";

#[derive(Clone)]
pub(crate) struct NodePeer(Arc<PeerState>);

struct PeerState {
    node: Arc<CellNode>,
    layout: CellStorageLayout,
    directory: NodeDirectory,
    target: CellTarget,
    session: SessionId,
    endpoint: String,
    local: Arc<workspace::Workspace>,
    tasks: TaskTracker,
    signer: Arc<PeerSigner>,
    dispatcher: PeerDispatcher,
    resolver: Arc<Resolver>,
    client: reqwest::Client,
    acquisition: Mutex<()>,
    admission: Arc<Semaphore>,
}

impl NodePeer {
    pub(super) fn new(
        config: &ServerConfig,
        node: Arc<CellNode>,
        layout: CellStorageLayout,
        directory: NodeDirectory,
        local: Arc<workspace::Workspace>,
        tasks: TaskTracker,
        session: SessionId,
    ) -> Result<Self, ServerError> {
        endpoint(&config.peer_endpoint)?;
        let registry = node.application().registry();
        let target = directory::directory_target(config.tenant, config.application)?;
        let resolver = Arc::new(Resolver {
            node: Arc::clone(&node),
            layout: layout.clone(),
            session,
        });
        let dispatcher = PeerDispatcher::new(
            Arc::clone(&registry),
            resolver.clone(),
            Arc::new(Authorization {
                tenant: config.tenant,
                application: config.application,
            }),
        );
        let mut client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .no_proxy()
            .connect_timeout(Duration::from_secs(5));
        if let Some(pem) = &config.peer_ca_pem {
            let certificate = reqwest::Certificate::from_pem(pem)
                .map_err(|source| transport_error(source, false))?;
            client = client.add_root_certificate(certificate);
        }
        let client = client
            .build()
            .map_err(|source| transport_error(source, false))?;
        Ok(Self(Arc::new(PeerState {
            node,
            layout,
            directory,
            target,
            session,
            endpoint: config.peer_endpoint.clone(),
            local,
            tasks,
            signer: Arc::new(PeerSigner::new(
                session,
                registry.release_digest(),
                config.signing_key.clone(),
            )),
            dispatcher,
            resolver,
            client,
            acquisition: Mutex::new(()),
            admission: Arc::new(Semaphore::new(32)),
        })))
    }

    pub(super) fn client(&self) -> CellClient {
        CellClient::peer(
            self.0.node.application().registry(),
            Arc::clone(&self.0.signer),
            PeerPrincipal {
                issuer: "canopy".into(),
                subject: "node".into(),
                actions: vec!["canopy.cell".into()],
            },
            Arc::new(self.clone()),
        )
    }

    pub(super) async fn remote_owner(&self, target: &CellTarget) -> Result<bool, ServerError> {
        Ok(self
            .live_owner(target)
            .await?
            .is_some_and(|owner| owner.session() != self.0.session))
    }

    pub(crate) async fn current_owner_fence(
        &self,
        target: &CellTarget,
    ) -> Result<cellule_runtime::registry::OwnerFence, ServerError> {
        self.live_binding(target)
            .await?
            .map(|(_, fence)| fence)
            .ok_or(Error::Fenced.into())
    }

    async fn live_owner(
        &self,
        target: &CellTarget,
    ) -> Result<Option<NodeAdvertisement>, ServerError> {
        Ok(self
            .live_binding(target)
            .await?
            .map(|(advertisement, _)| advertisement))
    }

    async fn live_binding(
        &self,
        target: &CellTarget,
    ) -> Result<Option<(NodeAdvertisement, cellule_runtime::registry::OwnerFence)>, ServerError>
    {
        let authority = CellAuthority::new(self.0.layout.clone());
        let Some(control) = authority.load(target.cell_id()).await? else {
            return Ok(None);
        };
        let Some(owner) = &control.value().owner else {
            return Ok(None);
        };
        let now = unix_now_ms()?;
        let Some(live) = self.0.directory.load_if_live(owner.session, now).await? else {
            return Ok(None);
        };
        if live.advertisement().endpoint() != owner.endpoint {
            return Err(Error::Fenced.into());
        }
        Ok(Some((
            live.advertisement().clone(),
            control.value().owner_fence(),
        )))
    }

    pub(super) async fn ensure_directory(&self) -> Result<(), ServerError> {
        let peer = self.clone();
        // Acquisition is admitted work. A timed-out RPC cannot drop it halfway
        // through SQL restore/publication or release workspace exclusion early.
        self.0
            .tasks
            .spawn(async move {
                let _guard = peer.0.acquisition.lock().await;
                if peer
                    .0
                    .resolver
                    .local_handle(&peer.0.target)
                    .await?
                    .is_some()
                    || peer
                        .live_owner(&peer.0.target)
                        .await?
                        .is_some_and(|owner| owner.session() != peer.0.session)
                {
                    return Ok(());
                }
                acquire_sql_cell(
                    &peer.0.node,
                    &peer.0.layout,
                    &peer.0.directory,
                    SqlCellSpec {
                        target: &peer.0.target,
                        module: DirectoryModule::NAME,
                        schema: directory::SCHEMA,
                        destination: peer.0.local.path().join("directory.sqlite"),
                    },
                    peer.0.session,
                    &peer.0.endpoint,
                )
                .await?;
                Ok::<_, ServerError>(())
            })
            .await?
    }

    async fn verify(&self, bytes: &[u8]) -> cellule_runtime::Result<VerifiedPeerRequest> {
        let now = unix_now_ms().map_err(|e| transport_error(e, false))?;
        let decoded = cellule_runtime::peer::UnverifiedPeerRequest::decode(bytes)?;
        let session = decoded.session();
        let enrolled = self
            .0
            .directory
            .load(session, now)
            .await?
            .ok_or(Error::PeerAuthorization("peer session is absent"))?;
        PeerVerifier::new(
            session,
            self.0.node.application().registry().release_digest(),
            enrolled.advertisement().verifying_key()?,
        )
        .verify_decoded(decoded, now)
    }

    async fn exchange(
        &self,
        target: CellTarget,
        request: Vec<u8>,
        remaining_ms: u32,
    ) -> cellule_runtime::Result<Vec<u8>> {
        let resident = self
            .0
            .node
            .runtime()
            .resident_handle(&target, CatalogRole::Sql)
            .await?
            .is_some();
        let mut owner = if resident {
            None
        } else {
            self.live_owner(&target)
                .await
                .map_err(|e| transport_error(e, false))?
        };
        if !resident && owner.is_none() && target == self.0.target {
            self.ensure_directory()
                .await
                .map_err(|e| transport_error(e, false))?;
            owner = self
                .live_owner(&target)
                .await
                .map_err(|e| transport_error(e, false))?;
        }
        if resident
            || owner
                .as_ref()
                .is_some_and(|owner| owner.session() == self.0.session)
        {
            // Warm local calls need no ownership-store reads. Remote calls reuse
            // the checked route; acquisition runs only when Directory has no live owner.
            let now = unix_now_ms().map_err(|e| transport_error(e, false))?;
            let verified = PeerVerifier::new(
                self.0.session,
                self.0.node.application().registry().release_digest(),
                self.0.signer.verifying_key(),
            )
            .verify(&request, now)?;
            return self.0.dispatcher.dispatch_bytes(&verified, now).await;
        }
        let owner = owner.ok_or(Error::Fenced)?;
        let url = endpoint(owner.endpoint()).map_err(|e| transport_error(e, false))?;
        // Never retry a delivered mutation automatically. Transport failures and
        // invalid responses preserve uncertainty for Cellule's identity resolver.
        let mut response = self
            .0
            .client
            .post(url)
            .header("content-type", "application/octet-stream")
            .timeout(Duration::from_millis(u64::from(remaining_ms)))
            .body(request)
            .send()
            .await
            .and_then(reqwest::Response::error_for_status)
            .map_err(|e| transport_error(e, true))?;
        if response.status() != StatusCode::OK {
            return Err(transport_error(
                Error::Peer("unexpected peer HTTP status"),
                true,
            ));
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|e| transport_error(e, true))?
        {
            if bytes.len().saturating_add(chunk.len()) > MAX_PEER_REQUEST_BYTES {
                return Err(transport_error(
                    Error::Peer("peer response exceeds limit"),
                    true,
                ));
            }
            bytes.extend_from_slice(&chunk);
        }
        Ok(bytes)
    }
}

impl PeerRoundTrip for NodePeer {
    fn send(
        &self,
        target: CellTarget,
        request: Vec<u8>,
        remaining_ms: u32,
    ) -> Pin<Box<dyn Future<Output = cellule_runtime::Result<Vec<u8>>> + Send + 'static>> {
        let peer = self.clone();
        Box::pin(async move { peer.exchange(target, request, remaining_ms).await })
    }
}

#[derive(Clone)]
struct Resolver {
    node: Arc<CellNode>,
    layout: CellStorageLayout,
    session: SessionId,
}
impl Resolver {
    async fn local_handle(
        &self,
        target: &CellTarget,
    ) -> cellule_runtime::Result<Option<CellHandle>> {
        let runtime = self.node.runtime();
        if let Some(handle) = runtime.resident_handle(target, CatalogRole::Sql).await? {
            return Ok(Some(handle));
        }
        // Restored Cells can own authority before their first SQL read makes
        // them resident. Resolve that verified local owner without acquiring twice.
        let Some(control) = CellAuthority::new(self.layout.clone())
            .load(target.cell_id())
            .await?
        else {
            return Ok(None);
        };
        let Some(proof) = CellCatalog::new(self.layout.clone(), target.tenant())
            .lookup(target.cell_id())
            .await?
        else {
            return Ok(None);
        };
        let handle = runtime.local_handle(proof.clone(), &control).await?;
        if handle.is_some()
            || control
                .value()
                .owner
                .as_ref()
                .is_none_or(|owner| owner.session != self.session)
            || !matches!(
                control.value().state,
                cellule_runtime::control::ControlState::Recovering
                    | cellule_runtime::control::ControlState::Serving
            )
        {
            return Ok(handle);
        }
        // A winning claim is visible before restore/actor admission finishes.
        // Wait for that node's own capability, without acquiring again or
        // dispatching any SQL. Runtime lookups retain node-lease checks. The
        // caller's signed RPC deadline still applies; this bounds a missing
        // capability even when the transport does not cancel first.
        match tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                tokio::time::sleep(Duration::from_millis(25)).await;
                if let Some(handle) = runtime.local_handle(proof.clone(), &control).await? {
                    return Ok(Some(handle));
                }
            }
        })
        .await
        {
            Ok(result) => result,
            Err(_) => Ok(None),
        }
    }
}
impl PeerCellResolver for Resolver {
    fn resolve(
        &self,
        target: CellTarget,
    ) -> Pin<Box<dyn Future<Output = cellule_runtime::Result<CellHandle>> + Send + 'static>> {
        let resolver = self.clone();
        Box::pin(async move { resolver.local_handle(&target).await?.ok_or(Error::Fenced) })
    }
}

struct Authorization {
    tenant: TenantId,
    application: ApplicationId,
}
impl PeerAuthorizer for Authorization {
    fn authorize(&self, request: &VerifiedPeerRequest) -> cellule_runtime::Result<()> {
        let target = request.target();
        if target.tenant() != self.tenant
            || target.application() != self.application
            || ![directory::DIRECTORY, crate::REPOSITORIES].contains(&target.namespace())
            || !request.permits("canopy.cell")
            || request.principal().issuer != "canopy"
            || request.principal().subject != "node"
        {
            return Err(Error::PeerAuthorization(
                "peer capability is outside Canopy",
            ));
        }
        Ok(())
    }
}

fn endpoint(value: &str) -> Result<url::Url, ServerError> {
    let mut url = http::validate_public_url(value).map_err(ServerError::Http)?;
    if url.scheme() != "https" {
        return Err(ServerError::Http("peer endpoint requires HTTPS"));
    }
    url.set_path(PATH);
    Ok(url)
}

fn transport_error(source: impl std::error::Error + Send + Sync + 'static, unknown: bool) -> Error {
    if unknown {
        Error::PeerTransportUnknown {
            context: "Canopy peer RPC",
            source: Box::new(source),
        }
    } else {
        Error::PeerTransport {
            context: "Canopy peer routing",
            source: Box::new(source),
        }
    }
}

pub(crate) async fn serve(
    State(peer): State<NodePeer>,
    request: axum::http::Request<Body>,
) -> Response {
    if !peer.0.node.is_ready() {
        return status(StatusCode::SERVICE_UNAVAILABLE);
    }
    let Ok(_permit) = Arc::clone(&peer.0.admission).try_acquire_owned() else {
        return status(StatusCode::SERVICE_UNAVAILABLE);
    };
    let bytes = match tokio::time::timeout(
        Duration::from_secs(30),
        to_bytes(request.into_body(), MAX_PEER_REQUEST_BYTES),
    )
    .await
    {
        Ok(Ok(bytes)) => bytes,
        Ok(Err(_)) => return status(StatusCode::PAYLOAD_TOO_LARGE),
        Err(_) => return status(StatusCode::REQUEST_TIMEOUT),
    };
    let verified = match peer.verify(&bytes).await {
        Ok(verified) => verified,
        Err(_) => return status(StatusCode::FORBIDDEN),
    };
    let now = match unix_now_ms() {
        Ok(now) => now,
        Err(_) => return status(StatusCode::SERVICE_UNAVAILABLE),
    };
    match peer.0.dispatcher.dispatch_bytes(&verified, now).await {
        Ok(bytes) => Response::new(Body::from(bytes)),
        Err(error) => {
            tracing::error!(error = %error, "peer dispatch failed");
            status(StatusCode::SERVICE_UNAVAILABLE)
        }
    }
}

fn status(code: StatusCode) -> Response {
    let mut response = Response::new(Body::empty());
    *response.status_mut() = code;
    response
}

const FORWARD_HOPS: &str = "canopy-forward-hops";

impl NodePeer {
    /// Forward transport bytes to the current resident owner. The verified
    /// fleet advertisement supplies the TLS endpoint; the original credential
    /// is independently authenticated there. Never redirect clients or retry a
    /// consumed request body after an ambiguous receive-pack.
    pub(crate) async fn forward_repository(
        &self,
        target: &CellTarget,
        request: axum::http::Request<Body>,
    ) -> Result<axum::http::Response<Body>, ServerError> {
        let owner = self.live_owner(target).await?.ok_or(Error::Fenced)?;
        if owner.session() == self.0.session {
            // Ownership changed after route selection. Let a new request bind
            // the local capability; this request must not use the stale gateway.
            return Err(Error::Fenced.into());
        }
        let (parts, body) = request.into_parts();
        let hops = match parts.headers.get(FORWARD_HOPS) {
            None => 0,
            Some(value) => value
                .to_str()
                .ok()
                .and_then(|s| s.parse::<u8>().ok())
                .filter(|n| *n < 2)
                .ok_or(Error::PeerAuthorization("repository forwarding hop limit"))?,
        };
        let mut url = endpoint(owner.endpoint())?;
        url.set_path(parts.uri.path());
        url.set_query(parts.uri.query());
        let mut headers = parts.headers;
        strip_connection_headers(&mut headers);
        headers.remove(axum::http::header::HOST);
        headers.insert(
            FORWARD_HOPS,
            axum::http::HeaderValue::from_static(if hops == 0 { "1" } else { "2" }),
        );
        let response = self
            .0
            .client
            .request(parts.method, url)
            .headers(headers)
            .body(reqwest::Body::wrap_stream(body.into_data_stream()))
            .send()
            .await
            .map_err(|e| transport_error(e, true))?;
        let status = response.status();
        let mut headers = response.headers().clone();
        strip_connection_headers(&mut headers);
        let mut result = axum::http::Response::new(Body::from_stream(response.bytes_stream()));
        *result.status_mut() = status;
        *result.headers_mut() = headers;
        Ok(result)
    }
}

fn strip_connection_headers(headers: &mut axum::http::HeaderMap) {
    // Connection may nominate additional hop-local headers. Copy their names
    // before mutation; forwarding credentials never come from extensions.
    let named: Vec<_> = headers
        .get_all(axum::http::header::CONNECTION)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .filter_map(|name| axum::http::HeaderName::from_bytes(name.trim().as_bytes()).ok())
        .collect();
    for name in named {
        headers.remove(name);
    }
    for name in [
        "connection",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailer",
        "transfer-encoding",
        "upgrade",
    ] {
        headers.remove(name);
    }
}
