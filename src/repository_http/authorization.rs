use super::*;
use crate::server::RepositoryRoute;

pub(super) async fn authorized_route(
    state: &RepositoryHttp,
    name: &str,
    headers: &axum::http::HeaderMap,
    scope: TokenScope,
) -> Result<(RepositoryRoute, Principal), Response<Body>> {
    if !(state.manager.ready)() {
        return Err(plain(
            StatusCode::SERVICE_UNAVAILABLE,
            "Canopy node is not ready",
        ));
    }
    let principal = state.require(headers, scope).await?;
    if directory::validate_component(name).is_err() {
        return Err(plain(StatusCode::NOT_FOUND, "Repository does not exist"));
    }
    let route = match state.manager.resolve(&state.manager.owner, name).await {
        Ok(Some(route)) => route,
        Ok(None) => return Err(plain(StatusCode::NOT_FOUND, "Repository does not exist")),
        Err(error) => {
            tracing::error!(error = %error, "repository routing failed");
            return Err(plain(
                StatusCode::SERVICE_UNAVAILABLE,
                "Repository is unavailable",
            ));
        }
    };
    match route
        .repository
        .access_level(&principal.account, None)
        .await
    {
        Ok(role) if role.output.is_some_and(|role| role >= scope) => Ok((route, principal)),
        Ok(role) if role.output.is_some() => {
            Err(plain(StatusCode::FORBIDDEN, "Repository owner required"))
        }
        Ok(_) => Err(plain(StatusCode::NOT_FOUND, "Repository does not exist")),
        Err(error) => {
            tracing::error!(error = %error, "repository authorization failed");
            Err(plain(
                StatusCode::SERVICE_UNAVAILABLE,
                "Repository is unavailable",
            ))
        }
    }
}
