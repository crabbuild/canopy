use super::*;
use crate::directory::DisableAccountOutcome;

pub(super) async fn disable(
    State(state): State<Arc<RepositoryHttp>>,
    Path(account): Path<String>,
    headers: axum::http::HeaderMap,
) -> Response<Body> {
    if !(state.manager.ready)() {
        return plain(StatusCode::SERVICE_UNAVAILABLE, "Canopy node is not ready");
    }
    let principal = match state.require(&headers, TokenScope::Admin).await {
        Ok(principal) => principal,
        Err(response) => return response,
    };
    if principal.account != state.manager.owner {
        return plain(StatusCode::FORBIDDEN, "Account management is restricted");
    }
    if directory::validate_component(&account).is_err() {
        return plain(StatusCode::NOT_FOUND, "Account does not exist");
    }
    let Some(credential) = http::credential(headers.get(header::AUTHORIZATION)) else {
        return unauthorized();
    };
    let digest = Sha256::digest(credential.token.as_bytes()).into();
    match state.manager.disable_account(digest, &account).await {
        Ok(DisableAccountOutcome::Disabled) if (state.manager.ready)() => {
            plain(StatusCode::NO_CONTENT, "")
        }
        Ok(DisableAccountOutcome::Disabled) => {
            plain(StatusCode::SERVICE_UNAVAILABLE, "Canopy node is not ready")
        }
        Ok(DisableAccountOutcome::NotFound) => {
            plain(StatusCode::NOT_FOUND, "Account does not exist")
        }
        Ok(DisableAccountOutcome::Forbidden) => {
            plain(StatusCode::FORBIDDEN, "Account management is restricted")
        }
        Ok(DisableAccountOutcome::SiteOwner) => {
            plain(StatusCode::CONFLICT, "Cannot disable the site owner")
        }
        Err(error) => {
            tracing::error!(error = %error, "account disable failed");
            plain(
                StatusCode::SERVICE_UNAVAILABLE,
                "Account management unavailable",
            )
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct AccountQuery {
    after: Option<String>,
}

pub(super) async fn session(
    State(state): State<Arc<RepositoryHttp>>,
    headers: axum::http::HeaderMap,
) -> Response<Body> {
    if !(state.manager.ready)() {
        return plain(StatusCode::SERVICE_UNAVAILABLE, "Canopy node is not ready");
    }
    let principal = match state.require(&headers, TokenScope::Read).await {
        Ok(principal) => principal,
        Err(response) => return response,
    };
    if !(state.manager.ready)() {
        return plain(StatusCode::SERVICE_UNAVAILABLE, "Canopy node is not ready");
    }
    json_response(
        StatusCode::OK,
        &serde_json::json!({
            "account": principal.account,
            "token_scope": principal.scope.as_str(),
            "token_id": uuid::Uuid::from_bytes(principal.token_id).to_string(),
            "site_admin": principal.account == state.manager.owner && principal.scope == TokenScope::Admin,
        }),
    )
}

pub(super) async fn list(
    State(state): State<Arc<RepositoryHttp>>,
    Query(query): Query<AccountQuery>,
    headers: axum::http::HeaderMap,
) -> Response<Body> {
    if !(state.manager.ready)() {
        return plain(StatusCode::SERVICE_UNAVAILABLE, "Canopy node is not ready");
    }
    let principal = match state.require(&headers, TokenScope::Admin).await {
        Ok(principal) => principal,
        Err(response) => return response,
    };
    if principal.account != state.manager.owner {
        return plain(StatusCode::FORBIDDEN, "Account management is restricted");
    }
    if query
        .after
        .as_deref()
        .is_some_and(|after| directory::validate_component(after).is_err())
    {
        return plain(StatusCode::UNPROCESSABLE_ENTITY, "Invalid account cursor");
    }
    let Some(credential) = http::credential(headers.get(header::AUTHORIZATION)) else {
        return unauthorized();
    };
    let digest = Sha256::digest(credential.token.as_bytes()).into();
    match state.manager.accounts(digest, query.after.as_deref()).await {
        Ok(Some(accounts)) if (state.manager.ready)() => {
            let next = (accounts.len() == directory::ACCOUNT_PAGE_SIZE)
                .then(|| accounts.last().map(|account| &account.name))
                .flatten();
            let entries: Vec<_> = accounts
                .iter()
                .map(
                    |account| serde_json::json!({"name": account.name, "enabled": account.enabled}),
                )
                .collect();
            json_response(
                StatusCode::OK,
                &serde_json::json!({"accounts": entries, "next_after": next}),
            )
        }
        Ok(None) => plain(StatusCode::FORBIDDEN, "Account management is restricted"),
        Ok(Some(_)) => plain(StatusCode::SERVICE_UNAVAILABLE, "Canopy node is not ready"),
        Err(error) => {
            tracing::error!(error = %error, "account listing failed");
            plain(
                StatusCode::SERVICE_UNAVAILABLE,
                "Account management unavailable",
            )
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct AuditQuery {
    before: Option<String>,
}

pub(super) async fn audit(
    State(state): State<Arc<RepositoryHttp>>,
    Query(query): Query<AuditQuery>,
    headers: axum::http::HeaderMap,
) -> Response<Body> {
    if !(state.manager.ready)() {
        return plain(StatusCode::SERVICE_UNAVAILABLE, "Canopy node is not ready");
    }
    let principal = match state.require(&headers, TokenScope::Admin).await {
        Ok(principal) => principal,
        Err(response) => return response,
    };
    if principal.account != state.manager.owner {
        return plain(StatusCode::FORBIDDEN, "Account history is restricted");
    }
    let before = match query.before {
        None => None,
        Some(value) => match value.parse::<i64>() {
            Ok(value) if value > 0 => Some(value),
            _ => return plain(StatusCode::UNPROCESSABLE_ENTITY, "Invalid history cursor"),
        },
    };
    let Some(credential) = http::credential(headers.get(header::AUTHORIZATION)) else {
        return unauthorized();
    };
    let digest = Sha256::digest(credential.token.as_bytes()).into();
    match state.manager.account_events(digest, before).await {
        Ok(Some(events)) if (state.manager.ready)() => {
            let next = (events.len() == directory::AUDIT_PAGE_SIZE)
                .then(|| events.last().map(|event| event.id.to_string()))
                .flatten();
            let entries: Vec<_> = events.iter().map(|event| {
                let mut entry = serde_json::json!({
                "id": event.id.to_string(), "occurred_at_ms": event.occurred_at_ms,
                "actor": event.actor, "actor_token_id": event.actor_token_id.map(|id| uuid::Uuid::from_bytes(id).to_string()),
                "action": event.action, "account": event.account,
                "token_id": event.token_id.map(|id| uuid::Uuid::from_bytes(id).to_string()),
                "scope": event.scope.map(TokenScope::as_str), "expires_at_ms": event.expires_at_ms,
                });
                if let Some(id) = event.ssh_key_id {
                    entry["ssh_key_id"] = uuid::Uuid::from_bytes(id).to_string().into();
                }
                entry
            }).collect();
            json_response(
                StatusCode::OK,
                &serde_json::json!({"events": entries, "next_before": next}),
            )
        }
        Ok(None) => plain(StatusCode::FORBIDDEN, "Account history is restricted"),
        Ok(Some(_)) => plain(StatusCode::SERVICE_UNAVAILABLE, "Canopy node is not ready"),
        Err(error) => {
            tracing::error!(error = %error, "account history failed");
            plain(
                StatusCode::SERVICE_UNAVAILABLE,
                "Account history unavailable",
            )
        }
    }
}
