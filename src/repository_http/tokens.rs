use super::*;
use crate::directory::{TOKEN_PAGE_SIZE, TokenChange};
use std::time::Duration;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ListQuery {
    after: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct IssueRequest {
    id: String,
    token: String,
    scope: String,
}

async fn authorize(
    state: &RepositoryHttp,
    headers: &axum::http::HeaderMap,
    account: &str,
) -> Result<[u8; 32], Response<Body>> {
    if !(state.manager.ready)() {
        return Err(plain(
            StatusCode::SERVICE_UNAVAILABLE,
            "Canopy node is not ready",
        ));
    }
    let principal = state.require(headers, TokenScope::Admin).await?;
    if principal.account != account && principal.account != state.manager.owner {
        return Err(plain(
            StatusCode::FORBIDDEN,
            "Token management is restricted",
        ));
    }
    if directory::validate_component(account).is_err() {
        return Err(plain(StatusCode::NOT_FOUND, "Account does not exist"));
    }
    let credential =
        http::credential(headers.get(header::AUTHORIZATION)).ok_or_else(unauthorized)?;
    Ok(Sha256::digest(credential.token.as_bytes()).into())
}

fn token_id(value: &str) -> Option<[u8; 16]> {
    let id = uuid::Uuid::parse_str(value).ok()?;
    (id.to_string() == value).then(|| id.into_bytes())
}

pub(super) async fn list(
    State(state): State<Arc<RepositoryHttp>>,
    Path(account): Path<String>,
    Query(query): Query<ListQuery>,
    request: Request<Body>,
) -> Response<Body> {
    let actor = match authorize(&state, request.headers(), &account).await {
        Ok(actor) => actor,
        Err(response) => return response,
    };
    let after = match query.after.as_deref() {
        None => None,
        Some(value) => match token_id(value) {
            Some(id) => Some(id),
            None => return plain(StatusCode::UNPROCESSABLE_ENTITY, "Invalid token cursor"),
        },
    };
    match state.manager.tokens(actor, &account, after).await {
        Ok(Some(tokens)) if (state.manager.ready)() => {
            let next = (tokens.len() == TOKEN_PAGE_SIZE)
                .then(|| {
                    tokens
                        .last()
                        .map(|token| uuid::Uuid::from_bytes(token.id).to_string())
                })
                .flatten();
            let tokens: Vec<_> = tokens
                .iter()
                .map(|token| {
                    serde_json::json!({
                        "id": uuid::Uuid::from_bytes(token.id).to_string(),
                        "scope": token.scope.as_str(),
                        "enabled": token.enabled,
                        "created_at_ms": token.created_at_ms,
                    })
                })
                .collect();
            json_response(
                StatusCode::OK,
                &serde_json::json!({"tokens": tokens, "next_after": next}),
            )
        }
        Ok(None) => plain(StatusCode::NOT_FOUND, "Account is unavailable"),
        Ok(Some(_)) => plain(StatusCode::SERVICE_UNAVAILABLE, "Canopy node is not ready"),
        Err(error) => failed(error),
    }
}

pub(super) async fn issue(
    State(state): State<Arc<RepositoryHttp>>,
    Path(account): Path<String>,
    request: Request<Body>,
) -> Response<Body> {
    let actor = match authorize(&state, request.headers(), &account).await {
        Ok(actor) => actor,
        Err(response) => return response,
    };
    let body =
        match tokio::time::timeout(Duration::from_secs(30), to_bytes(request.into_body(), 8192))
            .await
        {
            Ok(Ok(body)) => body,
            Ok(Err(_)) => {
                return plain(StatusCode::PAYLOAD_TOO_LARGE, "Token request is too large");
            }
            Err(_) => return plain(StatusCode::REQUEST_TIMEOUT, "Token request timed out"),
        };
    let Ok(input) = serde_json::from_slice::<IssueRequest>(&body) else {
        return plain(StatusCode::UNPROCESSABLE_ENTITY, "Invalid token request");
    };
    let Some(id) = token_id(&input.id) else {
        return plain(StatusCode::UNPROCESSABLE_ENTITY, "Invalid token ID");
    };
    let Some(scope) = TokenScope::parse(&input.scope) else {
        return plain(StatusCode::UNPROCESSABLE_ENTITY, "Invalid token scope");
    };
    if !valid_new_token(&input.token) {
        return plain(StatusCode::UNPROCESSABLE_ENTITY, "Invalid token secret");
    }
    let digest = Sha256::digest(input.token.as_bytes()).into();
    changed(
        &state,
        state
            .manager
            .issue_token(actor, &account, id, digest, scope)
            .await,
    )
}

pub(super) async fn revoke(
    State(state): State<Arc<RepositoryHttp>>,
    Path((account, id)): Path<(String, String)>,
    request: Request<Body>,
) -> Response<Body> {
    let actor = match authorize(&state, request.headers(), &account).await {
        Ok(actor) => actor,
        Err(response) => return response,
    };
    let Some(id) = token_id(&id) else {
        return plain(StatusCode::UNPROCESSABLE_ENTITY, "Invalid token ID");
    };
    changed(
        &state,
        state.manager.revoke_token(actor, &account, id).await,
    )
}

fn changed(state: &RepositoryHttp, result: Result<TokenChange, ServerError>) -> Response<Body> {
    match result {
        Ok(TokenChange::Applied) if (state.manager.ready)() => plain(StatusCode::NO_CONTENT, ""),
        Ok(TokenChange::Applied) => {
            plain(StatusCode::SERVICE_UNAVAILABLE, "Canopy node is not ready")
        }
        Ok(TokenChange::NotFound) => {
            plain(StatusCode::NOT_FOUND, "Account or token is unavailable")
        }
        Ok(TokenChange::Conflict) => plain(
            StatusCode::CONFLICT,
            "Token ID or secret is already reserved",
        ),
        Ok(TokenChange::LastAdmin) => plain(
            StatusCode::CONFLICT,
            "Cannot revoke the site's last admin token",
        ),
        Err(error) => failed(error),
    }
}

fn failed(error: ServerError) -> Response<Body> {
    tracing::error!(error = %error, "token management failed");
    plain(
        StatusCode::SERVICE_UNAVAILABLE,
        "Token management unavailable",
    )
}
