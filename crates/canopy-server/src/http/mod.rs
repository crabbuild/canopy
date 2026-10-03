//! HTTP ingress for Git repositories with current Cell access checks.

use std::{error::Error as StdError, net::IpAddr, sync::Arc};

use axum::{
    Router,
    body::{Body, Bytes, to_bytes},
    extract::{Path, State},
    http::{HeaderName, HeaderValue, Request, Response, StatusCode, header},
    routing::{any, get, post},
};
use base64::{Engine, engine::general_purpose::STANDARD};
use serde::Deserialize;
use serde_json::{Value, json};
use url::Url;

mod lfs_locks;

use crate::{
    ReadIdentity,
    directory::{LfsOperation, Principal, TokenScope, validate_component},
    git_gateway::{GatewayError, GitGateway},
    git_http::GitHttpRequest,
    lfs::LfsError,
};

/// Identity established by the outer HTTP authentication boundary.
#[derive(Clone)]
pub enum Viewer {
    Anonymous,
    Authenticated(Principal),
}
impl Viewer {
    pub(crate) fn identity(&self) -> ReadIdentity<'_> {
        match self {
            Self::Anonymous => ReadIdentity::Anonymous,
            Self::Authenticated(principal) => ReadIdentity::Account(&principal.account),
        }
    }
    pub(crate) fn principal(&self) -> Option<&Principal> {
        match self {
            Self::Anonymous => None,
            Self::Authenticated(principal) => Some(principal),
        }
    }
}

const MAX_LFS_BATCH_BYTES: usize = 1024 * 1024;
const MAX_LFS_BATCH_OBJECTS: usize = 100;
const LFS_JSON: &str = "application/vnd.git-lfs+json";

/// Git and LFS transport for one Repository Cell.
pub struct GitHttpApi {
    gateway: Arc<GitGateway>,
    repository_path: String,
    public_url: Url,
    ready: Arc<dyn Fn() -> bool + Send + Sync>,
}

impl GitHttpApi {
    pub fn new(
        gateway: Arc<GitGateway>,
        owner: String,
        repository_name: &str,
        public_url: &str,
        ready: Arc<dyn Fn() -> bool + Send + Sync>,
    ) -> Result<Self, &'static str> {
        validate_component(&owner).map_err(|_| "invalid repository owner")?;
        validate_component(repository_name).map_err(|_| "invalid repository name")?;
        let public_url = validate_public_url(public_url)?;
        Ok(Self {
            gateway,
            repository_path: format!("/{owner}/{repository_name}.git"),
            public_url,
            ready,
        })
    }

    pub fn router(self: Arc<Self>) -> Router {
        let git_path = format!("{}/{{*path}}", self.repository_path);
        let lfs_batch_path = format!("{}/info/lfs/objects/batch", self.repository_path);
        let lfs_object_path = format!("{}/info/lfs/objects/{{oid}}", self.repository_path);
        let locks_path = format!("{}/info/lfs/locks", self.repository_path);
        Router::new()
            .route(&git_path, any(git_request))
            .route(&lfs_batch_path, post(lfs_batch))
            .route(&lfs_object_path, get(lfs_get).put(lfs_put))
            .route(&locks_path, get(lfs_locks::handle).post(lfs_locks::handle))
            .route(&format!("{locks_path}/verify"), post(lfs_locks::handle))
            .route(
                &format!("{locks_path}/{{id}}/unlock"),
                post(lfs_locks::handle),
            )
            .with_state(self)
            .layer(axum::middleware::map_response(
                |mut response: Response<Body>| async {
                    // Visibility is mutable; shared HTTP caches must not serve bytes
                    // after a fresh request would be denied by the Repository Cell.
                    response
                        .headers_mut()
                        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
                    response
                },
            ))
    }

    async fn permission(&self, viewer: &Viewer, required: TokenScope) -> Result<(), StatusCode> {
        match viewer.principal() {
            Some(principal) if principal.scope < required => return Err(StatusCode::FORBIDDEN),
            None if required > TokenScope::Read => return Err(StatusCode::UNAUTHORIZED),
            _ => {}
        }
        match self.gateway.access_level(viewer.identity()).await {
            Ok(Some(role)) if role >= required => Ok(()),
            Ok(Some(_)) => Err(StatusCode::FORBIDDEN),
            Ok(None) => Err(if viewer.principal().is_none() {
                StatusCode::UNAUTHORIZED
            } else {
                StatusCode::NOT_FOUND
            }),
            Err(error) => {
                tracing::error!(error = %error, "repository access check failed");
                Err(StatusCode::SERVICE_UNAVAILABLE)
            }
        }
    }
}

pub(crate) fn lfs_grant_allows(
    operation: LfsOperation,
    method: &axum::http::Method,
    path: &str,
) -> bool {
    use axum::http::Method;
    match (method, path) {
        (&Method::POST, "info/lfs/objects/batch") => true,
        (&Method::GET, "info/lfs/locks") => true,
        (&Method::POST, "info/lfs/locks" | "info/lfs/locks/verify") => {
            operation == LfsOperation::Upload
        }
        (&Method::POST, path)
            if path.starts_with("info/lfs/locks/") && path.ends_with("/unlock") =>
        {
            operation == LfsOperation::Upload
        }
        (&Method::GET, path) if path.starts_with("info/lfs/objects/") => {
            operation == LfsOperation::Download
        }
        (&Method::PUT, path) if path.starts_with("info/lfs/objects/") => {
            operation == LfsOperation::Upload
        }
        _ => false,
    }
}

pub(crate) fn validate_public_url(public_url: &str) -> Result<Url, &'static str> {
    let public_url = Url::parse(public_url).map_err(|_| "invalid public URL")?;
    let loopback = public_url.host_str().is_some_and(|host| {
        host.eq_ignore_ascii_case("localhost")
            || host
                .parse::<IpAddr>()
                .is_ok_and(|address| address.is_loopback())
    });
    if !matches!(public_url.scheme(), "https" | "http")
        || (public_url.scheme() == "http" && !loopback)
        || public_url.host_str().is_none()
        || !public_url.username().is_empty()
        || public_url.password().is_some()
        || public_url.path() != "/"
        || public_url.query().is_some()
        || public_url.fragment().is_some()
    {
        return Err("public URL must be a bare HTTP origin");
    }
    Ok(public_url)
}

pub(crate) struct HttpCredential {
    pub user: Option<String>,
    pub token: String,
}

pub(crate) fn credential(authorization: Option<&HeaderValue>) -> Option<HttpCredential> {
    let header = authorization?.to_str().ok()?;
    if let Some(token) = header.strip_prefix("Bearer ") {
        return Some(HttpCredential {
            user: None,
            token: token.to_owned(),
        });
    }
    let decoded = STANDARD.decode(header.strip_prefix("Basic ")?).ok()?;
    let decoded = String::from_utf8(decoded).ok()?;
    let (user, token) = decoded.split_once(':')?;
    Some(HttpCredential {
        user: Some(user.to_owned()),
        token: token.to_owned(),
    })
}

#[derive(Deserialize)]
struct LfsBatchRequest {
    operation: String,
    objects: Vec<LfsBatchObject>,
    transfers: Option<Vec<String>>,
    hash_algo: Option<String>,
}

#[derive(Deserialize)]
struct LfsBatchObject {
    oid: String,
    size: u64,
}

async fn lfs_batch(State(api): State<Arc<GitHttpApi>>, request: Request<Body>) -> Response<Body> {
    if !(api.ready)() {
        return unavailable();
    }
    let Some(principal) = request.extensions().get::<Viewer>().cloned() else {
        return lfs_unauthorized();
    };
    let authorization = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let operation = request.extensions().get::<LfsOperation>().copied();
    let body = match lfs_body(request.into_body(), MAX_LFS_BATCH_BYTES).await {
        Ok(body) => body,
        Err(StatusCode::REQUEST_TIMEOUT) => {
            return lfs_json(
                StatusCode::REQUEST_TIMEOUT,
                json!({"message": "LFS request timed out"}),
            );
        }
        Err(status) => return lfs_json(status, json!({"message": "Batch request is too large"})),
    };
    let Ok(batch) = serde_json::from_slice::<LfsBatchRequest>(&body) else {
        return lfs_json(
            StatusCode::UNPROCESSABLE_ENTITY,
            json!({"message": "Invalid LFS batch request"}),
        );
    };
    if !matches!(batch.operation.as_str(), "upload" | "download")
        || batch.objects.len() > MAX_LFS_BATCH_OBJECTS
        || batch
            .hash_algo
            .as_deref()
            .is_some_and(|algorithm| algorithm != "sha256")
        || batch
            .transfers
            .as_ref()
            .is_some_and(|transfers| !transfers.iter().any(|transfer| transfer == "basic"))
    {
        return lfs_json(
            StatusCode::UNPROCESSABLE_ENTITY,
            json!({"message": "Unsupported LFS batch request"}),
        );
    }
    if operation.is_some_and(|operation| operation.as_str() != batch.operation) {
        return lfs_json(
            StatusCode::FORBIDDEN,
            json!({"message":"LFS credential operation mismatch"}),
        );
    }
    let required = if batch.operation == "upload" {
        TokenScope::Write
    } else {
        TokenScope::Read
    };
    if let Err(status) = api.permission(&principal, required).await {
        return lfs_json(status, json!({"message": "LFS access denied"}));
    }
    let mut objects = Vec::with_capacity(batch.objects.len());
    for requested in batch.objects {
        let Some(oid) = parse_lfs_oid(&requested.oid) else {
            return lfs_json(
                StatusCode::UNPROCESSABLE_ENTITY,
                json!({"message": "Invalid LFS object ID"}),
            );
        };
        if i64::try_from(requested.size).is_err() {
            objects.push(lfs_object_error(
                &requested,
                422,
                "LFS object size overflows storage representation",
            ));
            continue;
        }
        let stored = match api.gateway.lfs().lookup(oid).await {
            Ok(stored) => stored,
            Err(error) => {
                tracing::error!(error = %error, "LFS batch lookup failed");
                return lfs_json(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    json!({"message": "LFS lookup failed"}),
                );
            }
        };
        if stored.is_some_and(|object| object.size != requested.size) {
            objects.push(lfs_object_error(
                &requested,
                422,
                "LFS object size disagrees with its ID",
            ));
            continue;
        }
        // LFS uses this flag to suppress credential discovery. Public downloads
        // are already authorized and must not prompt for an account credential.
        let mut response = json!({
            "oid": requested.oid,
            "size": requested.size,
            "authenticated": true
        });
        let action = match (batch.operation.as_str(), stored.is_some()) {
            ("download", false) => {
                objects.push(lfs_object_error(
                    &requested,
                    404,
                    "LFS object does not exist",
                ));
                continue;
            }
            ("download", true) => Some("download"),
            ("upload", false) => Some("upload"),
            _ => None,
        };
        if let Some(action) = action {
            let href = format!(
                "{}{}/info/lfs/objects/{}",
                api.public_url,
                &api.repository_path[1..],
                requested.oid
            );
            response["actions"] = json!({});
            response["actions"][action] = json!({"href": href});
            if let Some(authorization) = &authorization {
                response["actions"][action]["header"] = json!({"Authorization": authorization});
            }
        }
        objects.push(response);
    }
    if !(api.ready)() {
        return unavailable();
    }
    lfs_json(
        StatusCode::OK,
        json!({"transfer": "basic", "objects": objects, "hash_algo": "sha256"}),
    )
}

async fn lfs_get(
    State(api): State<Arc<GitHttpApi>>,
    Path(oid): Path<String>,
    request: Request<Body>,
) -> Response<Body> {
    if !(api.ready)() {
        return unavailable();
    }
    let Some(principal) = request.extensions().get::<Viewer>() else {
        return lfs_unauthorized();
    };
    if let Err(status) = api.permission(principal, TokenScope::Read).await {
        return lfs_json(status, json!({"message": "LFS access denied"}));
    }
    let Some(oid) = parse_lfs_oid(&oid) else {
        return plain(StatusCode::NOT_FOUND, "LFS object does not exist");
    };
    let admission = request
        .extensions()
        .get::<Arc<crate::AdmissionPermit>>()
        .cloned();
    let range = request.headers().get(header::RANGE).cloned();
    let if_range = request.headers().contains_key(header::IF_RANGE);
    match api.gateway.lfs().get(oid, admission).await {
        Ok(body) if (api.ready)() => {
            let size = body.size();
            let offset = match lfs_tail_range(range.as_ref(), if_range, size) {
                Ok(offset) => offset,
                Err(()) => {
                    let mut response = plain(StatusCode::RANGE_NOT_SATISFIABLE, "");
                    let Ok(value) = HeaderValue::try_from(format!("bytes */{size}")) else {
                        return plain(StatusCode::INTERNAL_SERVER_ERROR, "LFS range header failed");
                    };
                    response.headers_mut().insert(header::CONTENT_RANGE, value);
                    return response;
                }
            };
            let start = offset.unwrap_or(0);
            let mut response = Response::new(Body::from_stream(body.resume_from(start)));
            if offset.is_some() {
                *response.status_mut() = StatusCode::PARTIAL_CONTENT;
                let Ok(value) = HeaderValue::try_from(format!("bytes {start}-{}/{size}", size - 1))
                else {
                    return plain(StatusCode::INTERNAL_SERVER_ERROR, "LFS range header failed");
                };
                response.headers_mut().insert(header::CONTENT_RANGE, value);
            }
            response
                .headers_mut()
                .insert(header::CONTENT_LENGTH, HeaderValue::from(size - start));
            response.headers_mut().insert(
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/octet-stream"),
            );
            response
        }
        Ok(_) => unavailable(),
        Err(LfsError::NotFound) => plain(StatusCode::NOT_FOUND, "LFS object does not exist"),
        Err(error) => {
            tracing::error!(error = %error, "LFS download failed");
            plain(StatusCode::INTERNAL_SERVER_ERROR, "LFS download failed")
        }
    }
}

fn lfs_tail_range(
    range: Option<&HeaderValue>,
    if_range: bool,
    size: u64,
) -> Result<Option<u64>, ()> {
    if if_range {
        return Ok(None);
    }
    let Some((start, end)) = range
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("bytes="))
        .and_then(|value| value.split_once('-'))
    else {
        return Ok(None);
    };
    let Ok(start) = start.parse::<u64>() else {
        return Ok(None);
    };
    if !end.is_empty() && end.parse::<u64>().ok() != size.checked_sub(1) {
        return Ok(None);
    }
    if start >= size {
        return Err(());
    }
    Ok(Some(start))
}

async fn lfs_put(
    State(api): State<Arc<GitHttpApi>>,
    Path(oid): Path<String>,
    request: Request<Body>,
) -> Response<Body> {
    if !(api.ready)() {
        return unavailable();
    }
    let Some(principal) = request.extensions().get::<Viewer>().cloned() else {
        return lfs_unauthorized();
    };
    if let Err(status) = api.permission(&principal, TokenScope::Write).await {
        return lfs_json(status, json!({"message": "LFS access denied"}));
    }
    let Some(oid) = parse_lfs_oid(&oid) else {
        return plain(StatusCode::UNPROCESSABLE_ENTITY, "Invalid LFS object ID");
    };
    let admission = request
        .extensions()
        .get::<Arc<crate::AdmissionPermit>>()
        .cloned();
    let body = request.into_body();
    let Some(principal) = principal.principal() else {
        return lfs_unauthorized();
    };
    match api
        .gateway
        .lfs()
        .put(&principal.account, oid, body, admission)
        .await
    {
        Ok(_) if (api.ready)() => plain(StatusCode::OK, ""),
        Ok(_) => unavailable(),
        Err(LfsError::Corrupt) => plain(
            StatusCode::UNPROCESSABLE_ENTITY,
            "LFS object digest mismatch",
        ),
        Err(LfsError::TooLarge) => plain(
            StatusCode::PAYLOAD_TOO_LARGE,
            "LFS object size overflows storage representation",
        ),
        Err(LfsError::Timeout) => plain(StatusCode::REQUEST_TIMEOUT, "LFS request timed out"),
        Err(LfsError::Body(_)) => plain(StatusCode::BAD_REQUEST, "LFS request body failed"),
        Err(LfsError::Forbidden) => plain(StatusCode::FORBIDDEN, "LFS access denied"),
        Err(error) => {
            tracing::error!(error = %error, "LFS upload failed");
            plain(StatusCode::INTERNAL_SERVER_ERROR, "LFS upload failed")
        }
    }
}

fn parse_lfs_oid(oid: &str) -> Option<[u8; 32]> {
    if oid.len() != 64 || !oid.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return None;
    }
    hex::decode(oid).ok()?.try_into().ok()
}

fn lfs_object_error(requested: &LfsBatchObject, code: u16, message: &'static str) -> Value {
    json!({
        "oid": requested.oid,
        "size": requested.size,
        "error": {"code": code, "message": message}
    })
}

fn lfs_unauthorized() -> Response<Body> {
    let mut response = lfs_json(
        StatusCode::UNAUTHORIZED,
        json!({"message": "Authentication required"}),
    );
    response.headers_mut().insert(
        "LFS-Authenticate",
        HeaderValue::from_static("Basic realm=\"Canopy LFS\""),
    );
    response
}

fn lfs_json(status: StatusCode, value: Value) -> Response<Body> {
    let Ok(body) = serde_json::to_vec(&value) else {
        return plain(
            StatusCode::INTERNAL_SERVER_ERROR,
            "LFS response encoding failed",
        );
    };
    let mut response = Response::new(Body::from(body));
    *response.status_mut() = status;
    response
        .headers_mut()
        .insert(header::CONTENT_TYPE, HeaderValue::from_static(LFS_JSON));
    response
}

async fn git_request(State(api): State<Arc<GitHttpApi>>, request: Request<Body>) -> Response<Body> {
    if !(api.ready)() {
        return unavailable();
    }
    let Some(principal) = request.extensions().get::<Viewer>().cloned() else {
        return unauthorized();
    };
    let method = request.method().as_str().to_owned();
    let Some(suffix) = request.uri().path().strip_prefix(&api.repository_path) else {
        return plain(StatusCode::NOT_FOUND, "Repository does not exist");
    };
    let path_info = format!("/repo.git{suffix}");
    let query = request.uri().query().unwrap_or_default().to_owned();
    let Some(required) = git_required(&method, suffix, &query) else {
        return plain(StatusCode::NOT_FOUND, "Git service does not exist");
    };
    if let Err(status) = api.permission(&principal, required).await {
        return if status == StatusCode::UNAUTHORIZED {
            unauthorized()
        } else {
            plain(status, "Repository access denied")
        };
    }
    let content_type = request
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let protocol_v2 = request
        .headers()
        .get("Git-Protocol")
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value == "version=2");
    let mut encodings = request.headers().get_all(header::CONTENT_ENCODING).iter();
    let gzip = match (encodings.next(), encodings.next()) {
        (None, None) => false,
        (Some(value), None) if value.as_bytes().eq_ignore_ascii_case(b"identity") => false,
        (Some(value), None)
            if value.as_bytes().eq_ignore_ascii_case(b"gzip")
                || value.as_bytes().eq_ignore_ascii_case(b"x-gzip") =>
        {
            true
        }
        _ => {
            return plain(
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                "Unsupported Git content encoding",
            );
        }
    };
    let push_id = if method == "POST" && suffix == "/git-receive-pack" {
        let mut values = request.headers().get_all("Idempotency-Key").iter();
        match (values.next(), values.next()) {
            (None, None) => None,
            (Some(value), None) => {
                let parsed = value.to_str().ok().and_then(|text| {
                    uuid::Uuid::parse_str(text)
                        .ok()
                        .filter(|id| id.to_string() == text)
                });
                let Some(id) = parsed else {
                    return plain(
                        StatusCode::BAD_REQUEST,
                        "Idempotency-Key must be a canonical UUID",
                    );
                };
                Some(id.into_bytes())
            }
            _ => {
                return plain(
                    StatusCode::BAD_REQUEST,
                    "Only one Idempotency-Key is allowed",
                );
            }
        }
    } else {
        None
    };
    let admission = request
        .extensions()
        .get::<Arc<crate::AdmissionPermit>>()
        .cloned();
    match api
        .gateway
        .handle(
            GitHttpRequest {
                method,
                path_info,
                query,
                content_type,
                gzip,
                protocol_v2,
                body: request.into_body(),
                authenticated: principal.principal().is_some(),
            },
            principal.identity(),
            push_id,
            admission,
        )
        .await
    {
        Ok(cgi) if (api.ready)() => {
            let Ok(status) = StatusCode::from_u16(cgi.status) else {
                return plain(StatusCode::BAD_GATEWAY, "Invalid Git backend status");
            };
            let mut response = Response::new(cgi.body);
            *response.status_mut() = status;
            for (name, value) in cgi.headers {
                let (Ok(name), Ok(value)) =
                    (HeaderName::try_from(name), HeaderValue::try_from(value))
                else {
                    return plain(StatusCode::BAD_GATEWAY, "Invalid Git backend header");
                };
                response.headers_mut().append(name, value);
            }
            response
        }
        Ok(_) => unavailable(),
        Err(GatewayError::Unauthorized) => unauthorized(),
        Err(GatewayError::Input(crate::git_input::InputError::Commands)) => plain(
            StatusCode::BAD_REQUEST,
            "Malformed Git receive-pack commands",
        ),
        Err(GatewayError::Input(crate::git_input::InputError::Fetch)) => plain(
            StatusCode::BAD_REQUEST,
            "Malformed Git upload-pack commands",
        ),
        Err(GatewayError::UnreachableWant) => plain(
            StatusCode::BAD_REQUEST,
            "Requested Git object is not reachable from a current repository ref",
        ),
        Err(GatewayError::Input(crate::git_input::InputError::TooLarge)) => {
            plain(StatusCode::PAYLOAD_TOO_LARGE, "Git request is too large")
        }
        Err(GatewayError::Input(crate::git_input::InputError::Timeout)) => {
            plain(StatusCode::REQUEST_TIMEOUT, "Git upload timed out")
        }
        Err(GatewayError::Input(crate::git_input::InputError::Body(_))) => {
            plain(StatusCode::BAD_REQUEST, "Git request body failed")
        }
        Err(GatewayError::Input(crate::git_input::InputError::Gzip(_))) => {
            plain(StatusCode::BAD_REQUEST, "Invalid Git gzip stream")
        }
        Err(GatewayError::Input(crate::git_input::InputError::Budget(_))) => plain(
            StatusCode::INSUFFICIENT_STORAGE,
            "Git upload disk budget exhausted",
        ),
        Err(GatewayError::RefSnapshotBusy) => plain(
            StatusCode::SERVICE_UNAVAILABLE,
            "Repository refs are changing; retry the request",
        ),
        Err(
            GatewayError::Cache(error)
            | GatewayError::Http(crate::git_http::GitHttpError::Cache(error)),
        ) if error.is_admission() => plain(
            StatusCode::INSUFFICIENT_STORAGE,
            "Git cache disk budget exhausted",
        ),
        Err(GatewayError::Push(crate::PushError::Conflict)) => plain(
            StatusCode::CONFLICT,
            "Push ID is bound to another request or account",
        ),
        Err(GatewayError::Objects(crate::git_gateway::ObjectReadError::TooLarge)) => {
            plain(StatusCode::PAYLOAD_TOO_LARGE, "Git object is too large")
        }
        Err(error) => git_failure(error),
    }
}

fn git_failure(error: GatewayError) -> Response<Body> {
    match error {
        GatewayError::Io(error)
        | GatewayError::Http(crate::git_http::GitHttpError::Io(error))
        | GatewayError::Cache(crate::git_cache::CacheError::Io(error))
        | GatewayError::Objects(crate::git_objects::ObjectReadError::Io(error))
            if crate::native_resources::is_exhausted(&error) =>
        {
            plain(
                StatusCode::SERVICE_UNAVAILABLE,
                "Native Git capacity exhausted; retry the request",
            )
        }
        GatewayError::Cell(error) | GatewayError::Push(crate::PushError::Cell(error))
            if cell_unavailable(error.as_ref()) =>
        {
            // The Cell cannot durably publish a Git rejection or prove a push's
            // outcome while it is unavailable. Keep this distinct from a server bug.
            tracing::warn!(error = ?error, "Repository Cell unavailable during Git request");
            plain(
                StatusCode::SERVICE_UNAVAILABLE,
                "Repository Cell unavailable",
            )
        }
        error => {
            tracing::error!(error = ?error, "Git request failed");
            plain(StatusCode::INTERNAL_SERVER_ERROR, "Git request failed")
        }
    }
}

fn cell_unavailable(error: &(dyn StdError + 'static)) -> bool {
    let mut current = Some(error);
    while let Some(source) = current {
        if let Some(runtime) = source.downcast_ref::<cellule_runtime::Error>()
            && matches!(
                runtime,
                cellule_runtime::Error::Fenced
                    | cellule_runtime::Error::RuntimeClosed
                    | cellule_runtime::Error::CellNotActive
                    | cellule_runtime::Error::CellDraining
                    | cellule_runtime::Error::ReplicaUnavailable
                    | cellule_runtime::Error::Deadline
                    | cellule_runtime::Error::Capacity(_)
            )
        {
            return true;
        }
        current = source.source();
    }
    false
}

fn git_required(method: &str, suffix: &str, query: &str) -> Option<TokenScope> {
    match (method, suffix) {
        ("POST", "/git-upload-pack") if query.is_empty() => Some(TokenScope::Read),
        ("POST", "/git-receive-pack") if query.is_empty() => Some(TokenScope::Write),
        ("GET", "/info/refs") => {
            let mut parameters = url::form_urlencoded::parse(query.as_bytes());
            let (name, service) = parameters.next()?;
            if name != "service" || parameters.next().is_some() {
                return None;
            }
            match service.as_ref() {
                "git-upload-pack" => Some(TokenScope::Read),
                "git-receive-pack" => Some(TokenScope::Write),
                _ => None,
            }
        }
        _ => None,
    }
}

fn unauthorized() -> Response<Body> {
    let mut response = plain(StatusCode::UNAUTHORIZED, "Authentication required");
    response.headers_mut().insert(
        header::WWW_AUTHENTICATE,
        HeaderValue::from_static("Basic realm=\"Canopy\""),
    );
    response
}

fn plain(status: StatusCode, message: &'static str) -> Response<Body> {
    let mut response = Response::new(Body::from(message));
    *response.status_mut() = status;
    response
}

fn unavailable() -> Response<Body> {
    plain(StatusCode::SERVICE_UNAVAILABLE, "Canopy node is not ready")
}

async fn lfs_body(body: Body, limit: usize) -> Result<Bytes, StatusCode> {
    tokio::time::timeout(std::time::Duration::from_secs(120), to_bytes(body, limit))
        .await
        .map_err(|_| StatusCode::REQUEST_TIMEOUT)?
        .map_err(|_| StatusCode::PAYLOAD_TOO_LARGE)
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_core::Stream;
    use std::{
        convert::Infallible,
        pin::Pin,
        task::{Context, Poll},
    };

    struct Stalled;
    impl Stream for Stalled {
        type Item = Result<Bytes, Infallible>;
        fn poll_next(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            Poll::Pending
        }
    }

    #[tokio::test(start_paused = true)]
    async fn stalled_lfs_batch_times_out() {
        let started = tokio::time::Instant::now();
        let result = lfs_body(Body::from_stream(Stalled), MAX_LFS_BATCH_BYTES).await;
        assert_eq!(result, Err(StatusCode::REQUEST_TIMEOUT));
        assert_eq!(started.elapsed(), std::time::Duration::from_secs(120));
    }

    #[tokio::test]
    async fn unavailable_cell_returns_503_including_during_push_completion() {
        let source =
            || cellule_runtime::InvocationError::<bool>::NotStarted(cellule_runtime::Error::Fenced);
        for error in [
            GatewayError::Cell(Box::new(source())),
            GatewayError::Push(crate::PushError::Cell(Box::new(source()))),
            GatewayError::Cell(Box::new(
                cellule_runtime::InvocationError::<bool>::NotStarted(
                    cellule_runtime::Error::Facility {
                        name: "Git Cell",
                        source: Box::new(cellule_runtime::Error::Fenced),
                    },
                ),
            )),
        ] {
            let response = git_failure(error);
            assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
            assert_eq!(
                to_bytes(response.into_body(), 1024).await.unwrap().as_ref(),
                b"Repository Cell unavailable"
            );
        }
        assert_eq!(
            git_failure(GatewayError::MalformedCache).status(),
            StatusCode::INTERNAL_SERVER_ERROR
        );
        assert_eq!(
            git_failure(GatewayError::Cell(Box::new(
                cellule_runtime::InvocationError::<bool>::NotStarted(
                    cellule_runtime::Error::Command("invalid command"),
                ),
            )))
            .status(),
            StatusCode::INTERNAL_SERVER_ERROR
        );
    }
    #[test]
    fn native_capacity_returns_503_only_for_typed_admission_exhaustion() {
        use crate::native_resources::{NativeClass, NativeResources, NativeWork};
        let pool = NativeResources::default();
        let scope = pool.scope(NativeClass::Foreground);
        let mut permits = Vec::new();
        while let Ok(permit) = scope.try_admit(NativeWork::Read) {
            permits.push(permit);
        }
        for wrap in [
            GatewayError::Io,
            |error| GatewayError::Http(crate::git_http::GitHttpError::Io(error)),
            |error| GatewayError::Cache(crate::git_cache::CacheError::Io(error)),
            |error| GatewayError::Objects(crate::git_objects::ObjectReadError::Io(error)),
        ] {
            let exhausted = scope.try_admit(NativeWork::Read).err().unwrap();
            assert_eq!(
                git_failure(wrap(exhausted)).status(),
                StatusCode::SERVICE_UNAVAILABLE
            );
            assert_eq!(
                git_failure(wrap(std::io::Error::from(std::io::ErrorKind::WouldBlock))).status(),
                StatusCode::INTERNAL_SERVER_ERROR
            );
        }
    }
}
