//! HTTP ingress for private Git repositories.

use std::{net::IpAddr, sync::Arc};

use axum::{
    Router,
    body::{Body, to_bytes},
    extract::{Path, State},
    http::{HeaderName, HeaderValue, Request, Response, StatusCode, header},
    routing::{any, get, post},
};
use base64::{Engine, engine::general_purpose::STANDARD};
use serde::Deserialize;
use serde_json::{Value, json};
use url::Url;

use crate::{
    directory::{Principal, TokenScope, validate_component},
    git_gateway::{GatewayError, GitGateway},
    git_http::GitHttpRequest,
    lfs::{LfsError, MAX_LFS_BYTES},
};

const MAX_LFS_BATCH_BYTES: usize = 1024 * 1024;
const MAX_LFS_BATCH_OBJECTS: usize = 100;
const LFS_JSON: &str = "application/vnd.git-lfs+json";

/// Git and LFS transport for one private Repository Cell.
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
        Router::new()
            .route(&git_path, any(git_request))
            .route(&lfs_batch_path, post(lfs_batch))
            .route(&lfs_object_path, get(lfs_get).put(lfs_put))
            .with_state(self)
    }

    async fn permission(
        &self,
        principal: &Principal,
        required: TokenScope,
    ) -> Result<(), StatusCode> {
        if principal.scope < required {
            return Err(StatusCode::FORBIDDEN);
        }
        match self.gateway.access_level(&principal.account).await {
            Ok(Some(role)) if role >= required => Ok(()),
            Ok(Some(_)) => Err(StatusCode::FORBIDDEN),
            Ok(None) => Err(StatusCode::NOT_FOUND),
            Err(error) => {
                tracing::error!(error = %error, "repository access check failed");
                Err(StatusCode::SERVICE_UNAVAILABLE)
            }
        }
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
    let Some(principal) = request.extensions().get::<Principal>().cloned() else {
        return lfs_unauthorized();
    };
    let Some(authorization) = request.headers().get(header::AUTHORIZATION) else {
        return lfs_unauthorized();
    };
    let Ok(authorization) = authorization.to_str().map(str::to_owned) else {
        return lfs_unauthorized();
    };
    let Ok(body) = to_bytes(request.into_body(), MAX_LFS_BATCH_BYTES).await else {
        return lfs_json(
            StatusCode::PAYLOAD_TOO_LARGE,
            json!({"message": "Batch request is too large"}),
        );
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
        if requested.size > MAX_LFS_BYTES as u64 {
            objects.push(lfs_object_error(&requested, 422, "LFS object is too large"));
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
            response["actions"][action] = json!({
                "href": href,
                "header": {"Authorization": authorization}
            });
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
    let Some(principal) = request.extensions().get::<Principal>() else {
        return lfs_unauthorized();
    };
    if let Err(status) = api.permission(principal, TokenScope::Read).await {
        return lfs_json(status, json!({"message": "LFS access denied"}));
    }
    let Some(oid) = parse_lfs_oid(&oid) else {
        return plain(StatusCode::NOT_FOUND, "LFS object does not exist");
    };
    match api.gateway.lfs().get(oid).await {
        Ok(body) if (api.ready)() => {
            let mut response = Response::new(Body::from(body));
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

async fn lfs_put(
    State(api): State<Arc<GitHttpApi>>,
    Path(oid): Path<String>,
    request: Request<Body>,
) -> Response<Body> {
    if !(api.ready)() {
        return unavailable();
    }
    let Some(principal) = request.extensions().get::<Principal>().cloned() else {
        return lfs_unauthorized();
    };
    if let Err(status) = api.permission(&principal, TokenScope::Write).await {
        return lfs_json(status, json!({"message": "LFS access denied"}));
    }
    let Some(oid) = parse_lfs_oid(&oid) else {
        return plain(StatusCode::UNPROCESSABLE_ENTITY, "Invalid LFS object ID");
    };
    let Ok(body) = to_bytes(request.into_body(), MAX_LFS_BYTES).await else {
        return plain(StatusCode::PAYLOAD_TOO_LARGE, "LFS object is too large");
    };
    match api.gateway.lfs().put(&principal.account, oid, &body).await {
        Ok(_) if (api.ready)() => plain(StatusCode::OK, ""),
        Ok(_) => unavailable(),
        Err(LfsError::Corrupt) => plain(
            StatusCode::UNPROCESSABLE_ENTITY,
            "LFS object digest mismatch",
        ),
        Err(LfsError::TooLarge) => plain(StatusCode::PAYLOAD_TOO_LARGE, "LFS object is too large"),
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
    let Some(principal) = request.extensions().get::<Principal>().cloned() else {
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
        return plain(status, "Repository access denied");
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
    match api
        .gateway
        .handle(
            GitHttpRequest {
                method,
                path_info,
                query,
                content_type,
                protocol_v2,
                body: request.into_body(),
                authenticated: true,
            },
            &principal.account,
            push_id,
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
        Err(GatewayError::Input(crate::git_input::InputError::TooLarge)) => {
            plain(StatusCode::PAYLOAD_TOO_LARGE, "Git request is too large")
        }
        Err(GatewayError::Input(crate::git_input::InputError::Timeout)) => {
            plain(StatusCode::REQUEST_TIMEOUT, "Git upload timed out")
        }
        Err(GatewayError::Input(crate::git_input::InputError::Body(_))) => {
            plain(StatusCode::BAD_REQUEST, "Git request body failed")
        }
        Err(GatewayError::Input(crate::git_input::InputError::Budget(_))) => plain(
            StatusCode::INSUFFICIENT_STORAGE,
            "Git upload disk budget exhausted",
        ),
        Err(GatewayError::RefConflict) => {
            plain(StatusCode::CONFLICT, "Repository changed during push")
        }
        Err(GatewayError::Push(crate::PushError::Conflict)) => plain(
            StatusCode::CONFLICT,
            "Push ID is bound to another request or account",
        ),
        Err(GatewayError::ObjectTooLarge) => {
            plain(StatusCode::PAYLOAD_TOO_LARGE, "Git object is too large")
        }
        Err(error) => {
            tracing::error!(error = %error, "Git request failed");
            plain(StatusCode::INTERNAL_SERVER_ERROR, "Git request failed")
        }
    }
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
