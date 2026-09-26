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
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use url::Url;

use crate::{
    git_gateway::{GatewayError, GitGateway},
    git_http::GitHttpRequest,
    lfs::{LfsError, MAX_LFS_BYTES},
};

const MAX_HTTP_BODY_BYTES: usize = 64 * 1024 * 1024;
const MAX_LFS_BATCH_BYTES: usize = 1024 * 1024;
const MAX_LFS_BATCH_OBJECTS: usize = 100;
const LFS_JSON: &str = "application/vnd.git-lfs+json";

/// One configured private owner and hashed Git access token.
pub struct GitHttpApi {
    gateway: Arc<GitGateway>,
    owner: String,
    token_digest: [u8; 32],
    public_url: Url,
    ready: Arc<dyn Fn() -> bool + Send + Sync>,
}

impl GitHttpApi {
    pub fn new(
        gateway: Arc<GitGateway>,
        owner: String,
        token: &str,
        public_url: &str,
        ready: Arc<dyn Fn() -> bool + Send + Sync>,
    ) -> Result<Self, &'static str> {
        if owner.is_empty() || token.is_empty() {
            return Err("Git owner and token are required");
        }
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
        Ok(Self {
            gateway,
            owner,
            token_digest: Sha256::digest(token.as_bytes()).into(),
            public_url,
            ready,
        })
    }

    pub fn router(self: Arc<Self>) -> Router {
        Router::new()
            .route("/healthz", get(health))
            .route("/readyz", get(readiness))
            .route("/repo.git/{*path}", any(git_request))
            .route("/repo.git/info/lfs/objects/batch", post(lfs_batch))
            .route(
                "/repo.git/info/lfs/objects/{oid}",
                get(lfs_get).put(lfs_put),
            )
            .with_state(self)
    }

    fn authorized(&self, authorization: Option<&HeaderValue>) -> bool {
        let Some(header) = authorization.and_then(|value| value.to_str().ok()) else {
            return false;
        };
        let token = if let Some(value) = header.strip_prefix("Bearer ") {
            value
        } else if let Some(value) = header.strip_prefix("Basic ") {
            let Ok(decoded) = STANDARD.decode(value) else {
                return false;
            };
            let Ok(decoded) = String::from_utf8(decoded) else {
                return false;
            };
            let Some((user, password)) = decoded.split_once(':') else {
                return false;
            };
            if user != self.owner {
                return false;
            }
            return self.matches_token(password);
        } else {
            return false;
        };
        self.matches_token(token)
    }

    fn matches_token(&self, token: &str) -> bool {
        let candidate: [u8; 32] = Sha256::digest(token.as_bytes()).into();
        bool::from(self.token_digest.ct_eq(&candidate))
    }
}

async fn health() -> Response<Body> {
    plain(StatusCode::OK, "ok")
}

async fn readiness(State(api): State<Arc<GitHttpApi>>) -> Response<Body> {
    if (api.ready)() {
        plain(StatusCode::OK, "ready")
    } else {
        unavailable()
    }
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
    let Some(authorization) = request.headers().get(header::AUTHORIZATION) else {
        return lfs_unauthorized();
    };
    if !api.authorized(Some(authorization)) {
        return lfs_unauthorized();
    }
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
                "{}repo.git/info/lfs/objects/{}",
                api.public_url, requested.oid
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
    if !api.authorized(request.headers().get(header::AUTHORIZATION)) {
        return lfs_unauthorized();
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
    if !api.authorized(request.headers().get(header::AUTHORIZATION)) {
        return lfs_unauthorized();
    }
    let Some(oid) = parse_lfs_oid(&oid) else {
        return plain(StatusCode::UNPROCESSABLE_ENTITY, "Invalid LFS object ID");
    };
    let Ok(body) = to_bytes(request.into_body(), MAX_LFS_BYTES).await else {
        return plain(StatusCode::PAYLOAD_TOO_LARGE, "LFS object is too large");
    };
    match api.gateway.lfs().put(oid, &body).await {
        Ok(_) if (api.ready)() => plain(StatusCode::OK, ""),
        Ok(_) => unavailable(),
        Err(LfsError::Corrupt) => plain(
            StatusCode::UNPROCESSABLE_ENTITY,
            "LFS object digest mismatch",
        ),
        Err(LfsError::TooLarge) => plain(StatusCode::PAYLOAD_TOO_LARGE, "LFS object is too large"),
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
    if !api.authorized(request.headers().get(header::AUTHORIZATION)) {
        return unauthorized();
    }
    let method = request.method().as_str().to_owned();
    let path_info = request.uri().path().to_owned();
    let query = request.uri().query().unwrap_or_default().to_owned();
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
    let Ok(body) = to_bytes(request.into_body(), MAX_HTTP_BODY_BYTES).await else {
        return plain(StatusCode::PAYLOAD_TOO_LARGE, "Git request is too large");
    };
    match api
        .gateway
        .handle(GitHttpRequest {
            method,
            path_info,
            query,
            content_type,
            protocol_v2,
            body: body.to_vec(),
            authenticated: true,
        })
        .await
    {
        Ok(cgi) if (api.ready)() => {
            let Ok(status) = StatusCode::from_u16(cgi.status) else {
                return plain(StatusCode::BAD_GATEWAY, "Invalid Git backend status");
            };
            let mut response = Response::new(Body::from(cgi.body));
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
        Err(GatewayError::RefConflict) => {
            plain(StatusCode::CONFLICT, "Repository changed during push")
        }
        Err(GatewayError::ObjectTooLarge) => {
            plain(StatusCode::PAYLOAD_TOO_LARGE, "Git object is too large")
        }
        Err(error) => {
            tracing::error!(error = %error, "Git request failed");
            plain(StatusCode::INTERNAL_SERVER_ERROR, "Git request failed")
        }
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
