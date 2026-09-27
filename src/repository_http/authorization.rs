use super::*;
use crate::{ReadIdentity, server::RepositoryRoute};

pub(super) async fn authorized_route(
    state: &RepositoryHttp,
    name: &str,
    headers: &axum::http::HeaderMap,
    scope: TokenScope,
) -> Result<(RepositoryRoute, Principal), Response<Body>> {
    let principal = state.require(headers, scope).await?;
    let route = scoped_route(
        state,
        name,
        ReadIdentity::Account(&principal.account),
        scope,
    )
    .await?;
    Ok((route, principal))
}

pub(super) async fn readable_route(
    state: &RepositoryHttp,
    name: &str,
    headers: &axum::http::HeaderMap,
) -> Result<(RepositoryRoute, Viewer), Response<Body>> {
    let viewer = state.viewer(headers).await?;
    let route = scoped_route(state, name, viewer.identity(), TokenScope::Read).await?;
    Ok((route, viewer))
}

async fn scoped_route(
    state: &RepositoryHttp,
    name: &str,
    actor: ReadIdentity<'_>,
    scope: TokenScope,
) -> Result<RepositoryRoute, Response<Body>> {
    if !(state.manager.ready)() {
        return Err(plain(
            StatusCode::SERVICE_UNAVAILABLE,
            "Canopy node is not ready",
        ));
    }
    if directory::validate_component(name).is_err() {
        return Err(plain(StatusCode::NOT_FOUND, "Repository does not exist"));
    }
    let missing = || {
        if matches!(actor, ReadIdentity::Anonymous) {
            unauthorized()
        } else {
            plain(StatusCode::NOT_FOUND, "Repository does not exist")
        }
    };
    let route = match state
        .manager
        .resolve(actor, &state.manager.owner, name)
        .await
    {
        Ok(Some(route)) => route,
        Ok(None) => return Err(missing()),
        Err(error) => {
            tracing::error!(error = %error, "repository routing failed");
            return Err(plain(
                StatusCode::SERVICE_UNAVAILABLE,
                "Repository is unavailable",
            ));
        }
    };
    match route.repository.access_level(actor, None).await {
        Ok(role) if role.output.is_some_and(|role| role >= scope) => Ok(route),
        Ok(role) if role.output.is_some() => {
            Err(plain(StatusCode::FORBIDDEN, "Repository owner required"))
        }
        Ok(_) => Err(missing()),
        Err(error) => {
            tracing::error!(error = %error, "repository authorization failed");
            Err(plain(
                StatusCode::SERVICE_UNAVAILABLE,
                "Repository is unavailable",
            ))
        }
    }
}
