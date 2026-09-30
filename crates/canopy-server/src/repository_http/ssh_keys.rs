use super::*;
use crate::directory::{SSH_KEY_PAGE_SIZE, SshKey, SshKeyChange};
use std::time::Duration;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ListQuery {
    after: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RegisterRequest {
    id: String,
    public_key: String,
    scope: String,
}

fn key_id(value: &str) -> Option<[u8; 16]> {
    let id = uuid::Uuid::parse_str(value).ok()?;
    (id.to_string() == value).then(|| id.into_bytes())
}

pub(super) async fn list(
    State(state): State<Arc<RepositoryHttp>>,
    Path(account): Path<String>,
    Query(query): Query<ListQuery>,
    request: Request<Body>,
) -> Response<Body> {
    let actor = match tokens::authorize(&state, request.headers(), &account).await {
        Ok(actor) => actor,
        Err(response) => return response,
    };
    let after = match query.after.as_deref() {
        None => None,
        Some(value) => match key_id(value) {
            Some(id) => Some(id),
            None => return plain(StatusCode::UNPROCESSABLE_ENTITY, "Invalid SSH key cursor"),
        },
    };
    match state.manager.ssh_keys(actor, &account, after).await {
        Ok(Some(keys)) if (state.manager.ready)() => {
            let next = (keys.len() == SSH_KEY_PAGE_SIZE)
                .then(|| {
                    keys.last()
                        .map(|key| uuid::Uuid::from_bytes(key.id).to_string())
                })
                .flatten();
            let keys: Vec<_> = keys
                .iter()
                .map(|key| {
                    serde_json::json!({
                        "id": uuid::Uuid::from_bytes(key.id).to_string(),
                        "fingerprint": key.key.fingerprint(), "public_key": key.key.public_key(),
                        "scope": key.scope.as_str(), "enabled": key.enabled,
                        "created_at_ms": key.created_at_ms,
                    })
                })
                .collect();
            json_response(
                StatusCode::OK,
                &serde_json::json!({"keys": keys, "next_after": next}),
            )
        }
        Ok(None) => plain(StatusCode::NOT_FOUND, "Account is unavailable"),
        Ok(Some(_)) => plain(StatusCode::SERVICE_UNAVAILABLE, "Canopy node is not ready"),
        Err(error) => failed(error),
    }
}

pub(super) async fn register(
    State(state): State<Arc<RepositoryHttp>>,
    Path(account): Path<String>,
    request: Request<Body>,
) -> Response<Body> {
    let actor = match tokens::authorize(&state, request.headers(), &account).await {
        Ok(actor) => actor,
        Err(response) => return response,
    };
    let body = match tokio::time::timeout(
        Duration::from_secs(30),
        to_bytes(request.into_body(), 16 * 1024),
    )
    .await
    {
        Ok(Ok(body)) => body,
        Ok(Err(_)) => {
            return plain(
                StatusCode::PAYLOAD_TOO_LARGE,
                "SSH key request is too large",
            );
        }
        Err(_) => return plain(StatusCode::REQUEST_TIMEOUT, "SSH key request timed out"),
    };
    let Ok(input) = serde_json::from_slice::<RegisterRequest>(&body) else {
        return plain(StatusCode::UNPROCESSABLE_ENTITY, "Invalid SSH key request");
    };
    let Some(id) = key_id(&input.id) else {
        return plain(StatusCode::UNPROCESSABLE_ENTITY, "Invalid SSH key ID");
    };
    let Some(scope @ (TokenScope::Read | TokenScope::Write)) = TokenScope::parse(&input.scope)
    else {
        return plain(
            StatusCode::UNPROCESSABLE_ENTITY,
            "SSH key scope must be read or write",
        );
    };
    let key = match SshKey::parse(&input.public_key) {
        Ok(key) => key,
        Err(error) => {
            return json_response(
                StatusCode::UNPROCESSABLE_ENTITY,
                &serde_json::json!({"message": error.to_string()}),
            );
        }
    };
    changed(
        &state,
        state
            .manager
            .register_ssh_key(actor, &account, id, &key, scope)
            .await,
    )
}

pub(super) async fn revoke(
    State(state): State<Arc<RepositoryHttp>>,
    Path((account, id)): Path<(String, String)>,
    request: Request<Body>,
) -> Response<Body> {
    let actor = match tokens::authorize(&state, request.headers(), &account).await {
        Ok(actor) => actor,
        Err(response) => return response,
    };
    let Some(id) = key_id(&id) else {
        return plain(StatusCode::UNPROCESSABLE_ENTITY, "Invalid SSH key ID");
    };
    changed(
        &state,
        state.manager.revoke_ssh_key(actor, &account, id).await,
    )
}

fn changed(state: &RepositoryHttp, result: Result<SshKeyChange, ServerError>) -> Response<Body> {
    match result {
        Ok(SshKeyChange::Applied) if (state.manager.ready)() => plain(StatusCode::NO_CONTENT, ""),
        Ok(SshKeyChange::Applied) => {
            plain(StatusCode::SERVICE_UNAVAILABLE, "Canopy node is not ready")
        }
        Ok(SshKeyChange::NotFound) => {
            plain(StatusCode::NOT_FOUND, "Account or SSH key is unavailable")
        }
        Ok(SshKeyChange::Conflict) => plain(
            StatusCode::CONFLICT,
            "SSH key ID or key material is already reserved",
        ),
        Ok(SshKeyChange::ActiveLimit) => plain(
            StatusCode::TOO_MANY_REQUESTS,
            "Account has reached its active SSH key limit",
        ),
        Ok(SshKeyChange::IssuanceLimit) => plain(
            StatusCode::TOO_MANY_REQUESTS,
            "Account has reached its SSH key registration limit for the past 24 hours",
        ),
        Err(error) => failed(error),
    }
}

fn failed(error: ServerError) -> Response<Body> {
    tracing::error!(error = %error, "SSH key management failed");
    plain(
        StatusCode::SERVICE_UNAVAILABLE,
        "SSH key management unavailable",
    )
}
