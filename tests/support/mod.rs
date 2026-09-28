use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use axum::{
    Router,
    body::Body,
    http::{Request, StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
};
use canopy_server::{
    directory::{Principal, TokenScope},
    http::{GitHttpApi, Viewer},
};
use cellule_runtime::{MutationIdentity, identity::RequestId};

pub fn git_router(api: Arc<GitHttpApi>) -> Router {
    api.router().layer(middleware::from_fn(test_identity))
}

async fn test_identity(mut request: Request<Body>, next: Next) -> Response {
    let account = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| match value {
            "Bearer local-test-token" => Some("canopy"),
            "Bearer late-writer-token" => Some("late-writer"),
            _ => None,
        });
    let Some(account) = account else {
        return StatusCode::UNAUTHORIZED.into_response();
    };
    request
        .extensions_mut()
        .insert(Viewer::Authenticated(Principal {
            token_id: [1; 16],
            account: account.into(),
            scope: TokenScope::Admin,
        }));
    next.run(request).await
}

pub fn identity() -> Result<MutationIdentity, Box<dyn std::error::Error>> {
    let now_ms = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis())?;
    Ok(MutationIdentity {
        request_id: RequestId::from_bytes(uuid::Uuid::new_v4().into_bytes()),
        issued_at_ms: now_ms,
        expires_at_ms: now_ms + 60_000,
    })
}
