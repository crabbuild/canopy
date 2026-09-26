//! HTTP API for dynamic repository creation and Git routing.

use std::sync::Arc;

use axum::{
    Router,
    body::{Body, to_bytes},
    extract::{Path, Query, State},
    http::{Request, Response, StatusCode, header},
    routing::{any, get},
};
use serde::{Deserialize, Serialize};
use tower::ServiceExt;

use crate::{
    directory::{self, RepositoryEntry},
    http,
    server::RepositoryManager,
};

pub(crate) struct RepositoryHttp {
    manager: Arc<RepositoryManager>,
    token_digest: [u8; 32],
}

impl RepositoryHttp {
    pub(crate) fn new(manager: Arc<RepositoryManager>, token_digest: [u8; 32]) -> Self {
        Self {
            manager,
            token_digest,
        }
    }

    pub(crate) fn router(self: Arc<Self>) -> Router {
        Router::new()
            .route("/healthz", get(health))
            .route("/readyz", get(readiness))
            .route(
                "/api/repositories",
                get(list_repositories).post(create_repository),
            )
            .route("/{owner}/{repository}/{*path}", any(dispatch_repository))
            .with_state(self)
    }

    fn authorized(&self, headers: &axum::http::HeaderMap) -> bool {
        http::authorized(
            &self.manager.owner,
            &self.token_digest,
            headers.get(header::AUTHORIZATION),
        )
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateRepositoryRequest {
    name: String,
}

#[derive(Deserialize)]
struct ListRepositoriesQuery {
    after: Option<String>,
}

#[derive(Serialize)]
struct RepositoryResponse {
    owner: String,
    name: String,
    repository_id: String,
    clone_url: String,
}

fn repository_response(manager: &RepositoryManager, entry: RepositoryEntry) -> RepositoryResponse {
    RepositoryResponse {
        owner: entry.owner.clone(),
        name: entry.name.clone(),
        repository_id: uuid::Uuid::from_bytes(entry.repository_id).to_string(),
        clone_url: format!(
            "{}/{}/{}.git",
            manager.public_url.trim_end_matches('/'),
            entry.owner,
            entry.name
        ),
    }
}

async fn health() -> Response<Body> {
    plain(StatusCode::OK, "ok")
}

async fn readiness(State(state): State<Arc<RepositoryHttp>>) -> Response<Body> {
    if (state.manager.ready)() {
        plain(StatusCode::OK, "ready")
    } else {
        plain(StatusCode::SERVICE_UNAVAILABLE, "Canopy node is not ready")
    }
}

async fn create_repository(
    State(state): State<Arc<RepositoryHttp>>,
    request: Request<Body>,
) -> Response<Body> {
    if !(state.manager.ready)() {
        return plain(StatusCode::SERVICE_UNAVAILABLE, "Canopy node is not ready");
    }
    if !state.authorized(request.headers()) {
        return unauthorized();
    }
    let Ok(body) = to_bytes(request.into_body(), 8192).await else {
        return plain(
            StatusCode::PAYLOAD_TOO_LARGE,
            "Repository request is too large",
        );
    };
    let Ok(input) = serde_json::from_slice::<CreateRepositoryRequest>(&body) else {
        return plain(
            StatusCode::UNPROCESSABLE_ENTITY,
            "Invalid repository request",
        );
    };
    if directory::validate_component(&input.name).is_err() {
        return plain(StatusCode::UNPROCESSABLE_ENTITY, "Invalid repository name");
    }
    match state.manager.create(&input.name).await {
        Ok(entry) if (state.manager.ready)() => {
            json_response(StatusCode::OK, &repository_response(&state.manager, entry))
        }
        Ok(_) => plain(StatusCode::SERVICE_UNAVAILABLE, "Canopy node is not ready"),
        Err(error) => {
            tracing::error!(error = %error, "repository creation failed");
            plain(
                StatusCode::SERVICE_UNAVAILABLE,
                "Repository creation failed",
            )
        }
    }
}

async fn list_repositories(
    State(state): State<Arc<RepositoryHttp>>,
    Query(query): Query<ListRepositoriesQuery>,
    headers: axum::http::HeaderMap,
) -> Response<Body> {
    if !(state.manager.ready)() {
        return plain(StatusCode::SERVICE_UNAVAILABLE, "Canopy node is not ready");
    }
    if !state.authorized(&headers) {
        return unauthorized();
    }
    if query
        .after
        .as_deref()
        .is_some_and(|after| !after.is_empty() && directory::validate_component(after).is_err())
    {
        return plain(
            StatusCode::UNPROCESSABLE_ENTITY,
            "Invalid repository cursor",
        );
    }
    match state
        .manager
        .list(query.after.as_deref().unwrap_or_default())
        .await
    {
        Ok((entries, next)) if (state.manager.ready)() => json_response(
            StatusCode::OK,
            &serde_json::json!({
                "repositories": entries.into_iter().map(|entry| repository_response(&state.manager, entry)).collect::<Vec<_>>(),
                "next_cursor": next,
            }),
        ),
        Ok(_) => plain(StatusCode::SERVICE_UNAVAILABLE, "Canopy node is not ready"),
        Err(error) => {
            tracing::error!(error = %error, "repository listing failed");
            plain(StatusCode::SERVICE_UNAVAILABLE, "Repository listing failed")
        }
    }
}

async fn dispatch_repository(
    State(state): State<Arc<RepositoryHttp>>,
    Path((owner, repository, _path)): Path<(String, String, String)>,
    request: Request<Body>,
) -> Response<Body> {
    if !(state.manager.ready)() {
        return plain(StatusCode::SERVICE_UNAVAILABLE, "Canopy node is not ready");
    }
    if !state.authorized(request.headers()) {
        return unauthorized();
    }
    let Some(name) = repository.strip_suffix(".git") else {
        return plain(StatusCode::NOT_FOUND, "Repository does not exist");
    };
    if directory::validate_component(name).is_err() {
        return plain(StatusCode::NOT_FOUND, "Repository does not exist");
    }
    tracing::debug!(owner, name, path = %request.uri().path(), "routing repository request");
    match state.manager.resolve(&owner, name).await {
        Ok(Some(router)) => {
            let (mut parts, body) = request.into_parts();
            // The inner router must extract only its own captures, especially the LFS OID.
            parts.extensions = axum::http::Extensions::new();
            let request = Request::from_parts(parts, body);
            match router.oneshot(request).await {
                Ok(response) => {
                    tracing::debug!(owner, name, status = %response.status(), "repository response completed");
                    response
                }
                Err(error) => match error {},
            }
        }
        Ok(None) => plain(StatusCode::NOT_FOUND, "Repository does not exist"),
        Err(error) => {
            tracing::error!(error = %error, "repository routing failed");
            plain(StatusCode::SERVICE_UNAVAILABLE, "Repository is unavailable")
        }
    }
}

fn plain(status: StatusCode, message: &'static str) -> Response<Body> {
    let mut response = Response::new(Body::from(message));
    *response.status_mut() = status;
    response
}

fn unauthorized() -> Response<Body> {
    let mut response = plain(StatusCode::UNAUTHORIZED, "Authentication required");
    response.headers_mut().insert(
        header::WWW_AUTHENTICATE,
        axum::http::HeaderValue::from_static("Basic realm=\"Canopy\""),
    );
    response
}

fn json_response(status: StatusCode, value: &impl Serialize) -> Response<Body> {
    let Ok(body) = serde_json::to_vec(value) else {
        return plain(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Response encoding failed",
        );
    };
    let mut response = Response::new(Body::from(body));
    *response.status_mut() = status;
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        axum::http::HeaderValue::from_static("application/json"),
    );
    response
}
