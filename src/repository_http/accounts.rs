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
