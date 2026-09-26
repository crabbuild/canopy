//! HTTP API for dynamic repository creation and Git routing.

mod default_branch;
mod tokens;

use std::sync::Arc;

use axum::{
    Router,
    body::{Body, to_bytes},
    extract::{Path, Query, State},
    http::{Request, Response, StatusCode, header},
    routing::{any, get},
};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_util::task::TaskTracker;

use crate::{
    directory::{
        self, CreateAccountOutcome, Principal, RenameOutcome, RepositoryEntry, TokenScope,
    },
    http,
    server::{MembershipOutcome, RepositoryManager, ServerError},
    validate_repository_id,
};

pub(crate) struct RepositoryHttp {
    manager: Arc<RepositoryManager>,
    transfers: Arc<Semaphore>,
    tasks: TaskTracker,
}

const MAX_ACTIVE_TRANSFERS: usize = 8;

impl RepositoryHttp {
    pub(crate) fn new(manager: Arc<RepositoryManager>, tasks: TaskTracker) -> Self {
        Self {
            manager,
            transfers: Arc::new(Semaphore::new(MAX_ACTIVE_TRANSFERS)),
            tasks,
        }
    }

    pub(crate) fn router(self: Arc<Self>) -> Router {
        Router::new()
            .route("/healthz", get(health))
            .route("/readyz", get(readiness))
            .route("/api/accounts", axum::routing::post(create_account))
            .route(
                "/api/accounts/{account}/tokens",
                get(tokens::list).post(tokens::issue),
            )
            .route(
                "/api/accounts/{account}/tokens/{id}",
                axum::routing::delete(tokens::revoke),
            )
            .route(
                "/api/repositories",
                get(list_repositories).post(create_repository),
            )
            .route(
                "/api/repositories/{name}",
                get(get_repository).patch(rename_repository),
            )
            .route(
                "/api/repositories/{name}/default-branch",
                get(default_branch::read).put(default_branch::update),
            )
            .route(
                "/api/repositories/{name}/collaborators/{account}",
                axum::routing::put(grant_collaborator).delete(revoke_collaborator),
            )
            .route("/{owner}/{repository}/{*path}", any(dispatch_repository))
            .with_state(self)
    }

    async fn principal(
        &self,
        headers: &axum::http::HeaderMap,
    ) -> Result<Option<Principal>, ServerError> {
        let Some(credential) = http::credential(headers.get(header::AUTHORIZATION)) else {
            return Ok(None);
        };
        let digest = Sha256::digest(credential.token.as_bytes()).into();
        let principal = self.manager.authenticate(digest).await?;
        Ok(principal.filter(|principal| {
            credential
                .user
                .as_deref()
                .is_none_or(|user| user == principal.account)
        }))
    }

    async fn require(
        &self,
        headers: &axum::http::HeaderMap,
        scope: TokenScope,
    ) -> Result<Principal, Response<Body>> {
        match self.principal(headers).await {
            Ok(Some(principal)) if principal.scope >= scope => Ok(principal),
            Ok(Some(_)) => Err(plain(StatusCode::FORBIDDEN, "Token scope is insufficient")),
            Ok(None) => Err(unauthorized()),
            Err(error) => {
                tracing::error!(error = %error, "authentication failed");
                Err(plain(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "Authentication unavailable",
                ))
            }
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateRepositoryRequest {
    name: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateAccountRequest {
    name: String,
    token: String,
    scope: String,
}

fn valid_new_token(token: &str) -> bool {
    token.strip_prefix("cnp_").is_some_and(|secret| {
        secret.len() == 64 && secret.bytes().all(|byte| byte.is_ascii_hexdigit())
    })
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RenameRepositoryRequest {
    name: String,
    repository_id: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct GrantCollaboratorRequest {
    role: String,
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

async fn create_account(
    State(state): State<Arc<RepositoryHttp>>,
    request: Request<Body>,
) -> Response<Body> {
    if !(state.manager.ready)() {
        return plain(StatusCode::SERVICE_UNAVAILABLE, "Canopy node is not ready");
    }
    let principal = match state.require(request.headers(), TokenScope::Admin).await {
        Ok(principal) => principal,
        Err(response) => return response,
    };
    if principal.account != state.manager.owner {
        return plain(StatusCode::FORBIDDEN, "Account creation is restricted");
    }
    let Some(credential) = http::credential(request.headers().get(header::AUTHORIZATION)) else {
        return unauthorized();
    };
    let actor_digest = Sha256::digest(credential.token.as_bytes()).into();
    let Ok(body) = to_bytes(request.into_body(), 8192).await else {
        return plain(
            StatusCode::PAYLOAD_TOO_LARGE,
            "Account request is too large",
        );
    };
    let Ok(input) = serde_json::from_slice::<CreateAccountRequest>(&body) else {
        return plain(StatusCode::UNPROCESSABLE_ENTITY, "Invalid account request");
    };
    let Some(scope) = TokenScope::parse(&input.scope) else {
        return plain(StatusCode::UNPROCESSABLE_ENTITY, "Invalid token scope");
    };
    if directory::validate_component(&input.name).is_err() || !valid_new_token(&input.token) {
        return plain(StatusCode::UNPROCESSABLE_ENTITY, "Invalid account identity");
    }
    let digest = Sha256::digest(input.token.as_bytes()).into();
    match state
        .manager
        .create_account(actor_digest, &input.name, digest, scope)
        .await
    {
        Ok(CreateAccountOutcome::Created(account)) if (state.manager.ready)() => json_response(
            StatusCode::OK,
            &serde_json::json!({"name": account.account, "scope": input.scope}),
        ),
        Ok(CreateAccountOutcome::Created(_)) => {
            plain(StatusCode::SERVICE_UNAVAILABLE, "Canopy node is not ready")
        }
        Ok(CreateAccountOutcome::NameTaken) => plain(StatusCode::CONFLICT, "Account name is taken"),
        Ok(CreateAccountOutcome::Forbidden) => {
            plain(StatusCode::FORBIDDEN, "Account creation is restricted")
        }
        Err(error) => {
            tracing::error!(error = %error, "account creation failed");
            plain(StatusCode::SERVICE_UNAVAILABLE, "Account creation failed")
        }
    }
}

async fn create_repository(
    State(state): State<Arc<RepositoryHttp>>,
    request: Request<Body>,
) -> Response<Body> {
    if !(state.manager.ready)() {
        return plain(StatusCode::SERVICE_UNAVAILABLE, "Canopy node is not ready");
    }
    let principal = match state.require(request.headers(), TokenScope::Write).await {
        Ok(principal) => principal,
        Err(response) => return response,
    };
    if principal.account != state.manager.owner {
        return plain(StatusCode::FORBIDDEN, "Repository creation is restricted");
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
            tracing::error!(error = ?error, "repository creation failed");
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
    let principal = match state.require(&headers, TokenScope::Read).await {
        Ok(principal) => principal,
        Err(response) => return response,
    };
    let after = match query
        .after
        .as_deref()
        .map(uuid::Uuid::parse_str)
        .transpose()
    {
        Ok(after) if after.is_none_or(|id| validate_repository_id(id.into_bytes()).is_ok()) => {
            after.map(uuid::Uuid::into_bytes)
        }
        _ => {
            return plain(
                StatusCode::UNPROCESSABLE_ENTITY,
                "Invalid repository cursor",
            );
        }
    };
    match state.manager.list(&principal.account, after).await {
        Ok((entries, next)) if (state.manager.ready)() => json_response(
            StatusCode::OK,
            &serde_json::json!({
                "repositories": entries.into_iter().map(|entry| repository_response(&state.manager, entry)).collect::<Vec<_>>(),
                "next_cursor": next,
            }),
        ),
        Ok(_) => plain(StatusCode::SERVICE_UNAVAILABLE, "Canopy node is not ready"),
        Err(ServerError::Runtime(cellule_runtime::Error::Capacity(_))) => {
            let mut response = plain(
                StatusCode::SERVICE_UNAVAILABLE,
                "Repository listing capacity is full; retry the request",
            );
            response.headers_mut().insert(
                header::RETRY_AFTER,
                axum::http::HeaderValue::from_static("1"),
            );
            response
        }
        Err(error) => {
            tracing::error!(error = %error, "repository listing failed");
            plain(StatusCode::SERVICE_UNAVAILABLE, "Repository listing failed")
        }
    }
}

async fn get_repository(
    State(state): State<Arc<RepositoryHttp>>,
    Path(name): Path<String>,
    headers: axum::http::HeaderMap,
) -> Response<Body> {
    if !(state.manager.ready)() {
        return plain(StatusCode::SERVICE_UNAVAILABLE, "Canopy node is not ready");
    }
    let principal = match state.require(&headers, TokenScope::Read).await {
        Ok(principal) => principal,
        Err(response) => return response,
    };
    if directory::validate_component(&name).is_err() {
        return plain(StatusCode::NOT_FOUND, "Repository does not exist");
    }
    match state.manager.inspect(&principal.account, &name).await {
        Ok(Some(details)) if (state.manager.ready)() => {
            let repository = repository_response(&state.manager, details.entry);
            json_response(
                StatusCode::OK,
                &serde_json::json!({
                    "owner": repository.owner,
                    "name": repository.name,
                    "repository_id": repository.repository_id,
                    "clone_url": repository.clone_url,
                    "role": details.role.as_str(),
                    "default_branch": details.head.reference,
                    "ref_generation": details.head.generation,
                }),
            )
        }
        Ok(Some(_)) => plain(StatusCode::SERVICE_UNAVAILABLE, "Canopy node is not ready"),
        Ok(None) => plain(StatusCode::NOT_FOUND, "Repository does not exist"),
        Err(error) => {
            tracing::error!(error = %error, "repository metadata read failed");
            plain(StatusCode::SERVICE_UNAVAILABLE, "Repository is unavailable")
        }
    }
}

async fn rename_repository(
    State(state): State<Arc<RepositoryHttp>>,
    Path(old_name): Path<String>,
    request: Request<Body>,
) -> Response<Body> {
    if !(state.manager.ready)() {
        return plain(StatusCode::SERVICE_UNAVAILABLE, "Canopy node is not ready");
    }
    let principal = match state.require(request.headers(), TokenScope::Write).await {
        Ok(principal) => principal,
        Err(response) => return response,
    };
    if principal.account != state.manager.owner {
        return plain(StatusCode::FORBIDDEN, "Repository rename is restricted");
    }
    let Ok(body) = to_bytes(request.into_body(), 8192).await else {
        return plain(
            StatusCode::PAYLOAD_TOO_LARGE,
            "Repository request is too large",
        );
    };
    let Ok(input) = serde_json::from_slice::<RenameRepositoryRequest>(&body) else {
        return plain(
            StatusCode::UNPROCESSABLE_ENTITY,
            "Invalid repository request",
        );
    };
    let Ok(repository_id) = uuid::Uuid::parse_str(&input.repository_id) else {
        return plain(StatusCode::UNPROCESSABLE_ENTITY, "Invalid repository UUID");
    };
    let repository_id = repository_id.into_bytes();
    if directory::validate_component(&old_name).is_err()
        || directory::validate_component(&input.name).is_err()
        || validate_repository_id(repository_id).is_err()
    {
        return plain(
            StatusCode::UNPROCESSABLE_ENTITY,
            "Invalid repository rename",
        );
    }
    match state
        .manager
        .rename(&old_name, &input.name, repository_id)
        .await
    {
        Ok(RenameOutcome::Renamed(entry)) if (state.manager.ready)() => {
            json_response(StatusCode::OK, &repository_response(&state.manager, entry))
        }
        Ok(RenameOutcome::Renamed(_)) => {
            plain(StatusCode::SERVICE_UNAVAILABLE, "Canopy node is not ready")
        }
        Ok(RenameOutcome::NotFound) => plain(StatusCode::NOT_FOUND, "Repository does not exist"),
        Ok(RenameOutcome::NameTaken) => plain(StatusCode::CONFLICT, "Repository name is taken"),
        Err(error) => {
            tracing::error!(error = %error, "repository rename failed");
            plain(StatusCode::SERVICE_UNAVAILABLE, "Repository rename failed")
        }
    }
}

async fn grant_collaborator(
    State(state): State<Arc<RepositoryHttp>>,
    Path((name, account)): Path<(String, String)>,
    request: Request<Body>,
) -> Response<Body> {
    if !(state.manager.ready)() {
        return plain(StatusCode::SERVICE_UNAVAILABLE, "Canopy node is not ready");
    }
    let principal = match state.require(request.headers(), TokenScope::Admin).await {
        Ok(principal) => principal,
        Err(response) => return response,
    };
    if directory::validate_component(&name).is_err()
        || directory::validate_component(&account).is_err()
    {
        return plain(
            StatusCode::UNPROCESSABLE_ENTITY,
            "Invalid collaborator target",
        );
    }
    let Ok(body) = to_bytes(request.into_body(), 8192).await else {
        return plain(
            StatusCode::PAYLOAD_TOO_LARGE,
            "Collaborator request is too large",
        );
    };
    let Ok(input) = serde_json::from_slice::<GrantCollaboratorRequest>(&body) else {
        return plain(
            StatusCode::UNPROCESSABLE_ENTITY,
            "Invalid collaborator request",
        );
    };
    let role = match input.role.as_str() {
        "read" => TokenScope::Read,
        "write" => TokenScope::Write,
        _ => {
            return plain(
                StatusCode::UNPROCESSABLE_ENTITY,
                "Invalid collaborator role",
            );
        }
    };
    match state
        .manager
        .update_member(&name, &principal.account, &account, Some(role))
        .await
    {
        Ok(MembershipOutcome::Updated) if (state.manager.ready)() => json_response(
            StatusCode::OK,
            &serde_json::json!({"account": account, "role": input.role}),
        ),
        outcome => membership_response(outcome),
    }
}

async fn revoke_collaborator(
    State(state): State<Arc<RepositoryHttp>>,
    Path((name, account)): Path<(String, String)>,
    headers: axum::http::HeaderMap,
) -> Response<Body> {
    if !(state.manager.ready)() {
        return plain(StatusCode::SERVICE_UNAVAILABLE, "Canopy node is not ready");
    }
    let principal = match state.require(&headers, TokenScope::Admin).await {
        Ok(principal) => principal,
        Err(response) => return response,
    };
    if directory::validate_component(&name).is_err()
        || directory::validate_component(&account).is_err()
    {
        return plain(
            StatusCode::UNPROCESSABLE_ENTITY,
            "Invalid collaborator target",
        );
    }
    match state
        .manager
        .update_member(&name, &principal.account, &account, None)
        .await
    {
        Ok(MembershipOutcome::Updated) if (state.manager.ready)() => {
            plain(StatusCode::NO_CONTENT, "")
        }
        outcome => membership_response(outcome),
    }
}

fn membership_response(outcome: Result<MembershipOutcome, ServerError>) -> Response<Body> {
    match outcome {
        Ok(MembershipOutcome::Updated) => {
            plain(StatusCode::SERVICE_UNAVAILABLE, "Canopy node is not ready")
        }
        Ok(MembershipOutcome::RepositoryMissing | MembershipOutcome::AccountMissing) => plain(
            StatusCode::NOT_FOUND,
            "Repository or account does not exist",
        ),
        Ok(MembershipOutcome::Forbidden) => {
            plain(StatusCode::FORBIDDEN, "Repository owner required")
        }
        Err(error) => {
            tracing::error!(error = %error, "repository membership change failed");
            plain(
                StatusCode::SERVICE_UNAVAILABLE,
                "Repository membership change failed",
            )
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
    let principal = match state.require(request.headers(), TokenScope::Read).await {
        Ok(principal) => principal,
        Err(response) => return response,
    };
    let Some(name) = repository.strip_suffix(".git") else {
        return plain(StatusCode::NOT_FOUND, "Repository does not exist");
    };
    if directory::validate_component(name).is_err() {
        return plain(StatusCode::NOT_FOUND, "Repository does not exist");
    }
    let Ok(permit) = Arc::clone(&state.transfers).try_acquire_owned() else {
        let mut response = plain(
            StatusCode::SERVICE_UNAVAILABLE,
            "Canopy transfer capacity is full; retry the request",
        );
        response.headers_mut().insert(
            header::RETRY_AFTER,
            axum::http::HeaderValue::from_static("1"),
        );
        return response;
    };
    let permit = Arc::new(permit);
    let name = name.to_owned();
    let manager = Arc::clone(&state.manager);
    // Detached requests retain admission while Cell transitions and blocking
    // workers finish. Shutdown tracks this task before draining the Cell node.
    let task = state.tasks.spawn(async move {
        tracing::debug!(owner, name, path = %request.uri().path(), "routing repository request");
        let response = match manager.resolve(&owner, &name).await {
            Ok(Some(route)) => {
                let (mut parts, body) = request.into_parts();
                // The inner router must extract only its own captures, especially the LFS OID.
                parts.extensions = axum::http::Extensions::new();
                parts.extensions.insert(principal);
                parts.extensions.insert(Arc::<OwnedSemaphorePermit>::clone(&permit));
                let request = Request::from_parts(parts, body);
                let response = route.dispatch(request).await;
                tracing::debug!(owner, name, status = %response.status(), "repository response completed");
                response
            }
            Ok(None) => plain(StatusCode::NOT_FOUND, "Repository does not exist"),
            Err(error) => {
                tracing::error!(error = %error, "repository routing failed");
                plain(StatusCode::SERVICE_UNAVAILABLE, "Repository is unavailable")
            }
        };
        response.map(|body| crate::transfer::response_body(body, permit))
    });
    match task.await {
        Ok(response) => response,
        Err(error) => {
            tracing::error!(error = %error, "repository request task failed");
            plain(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Repository request failed",
            )
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
