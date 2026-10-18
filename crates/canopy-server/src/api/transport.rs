use super::*;

pub(super) async fn dispatch(
    State(state): State<Arc<RepositoryHttp>>,
    Path((owner, repository, path)): Path<(String, String, String)>,
    request: Request<Body>,
) -> Response<Body> {
    if !(state.manager.ready)() {
        return plain(StatusCode::SERVICE_UNAVAILABLE, "Canopy node is not ready");
    }
    let Some(name) = repository.strip_suffix(".git") else {
        return plain(StatusCode::NOT_FOUND, "Repository does not exist");
    };
    if directory::validate_component(name).is_err() {
        return plain(StatusCode::NOT_FOUND, "Repository does not exist");
    }
    // LFS grants are a separate credential namespace. Resolve their repository
    // binding before creating a Viewer; management and Git routes never accept them.
    let token = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|header| header.to_str().ok())
        .and_then(|header| header.strip_prefix("CanopyLfs "));
    let (principal, operation) = if let Some(token) = token {
        match state
            .manager
            .authenticate_lfs_grant(token, &owner, name)
            .await
        {
            Ok(Some(grant)) if http::lfs_grant_allows(grant.operation, request.method(), &path) => {
                (
                    Viewer::Authenticated(grant.principal),
                    Some((grant.operation, grant.repository)),
                )
            }
            Ok(_) => return unauthorized(),
            Err(error) => {
                tracing::warn!(error = ?error, "LFS grant authentication failed");
                return plain(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "LFS authentication unavailable",
                );
            }
        }
    } else {
        match state.viewer(request.headers()).await {
            Ok(principal) => (principal, None),
            Err(response) => return response,
        }
    };
    let permit = match state.transfer_permit(principal.identity()).await {
        Ok(permit) => permit,
        Err(response) => return response,
    };
    let name = name.to_owned();
    let manager = Arc::clone(&state.manager);
    // Detached requests retain admission while Cell transitions and blocking
    // workers finish. Shutdown tracks this task before draining the Cell node.
    let task = state.tasks.spawn(async move {
        tracing::debug!(owner, name, path = %request.uri().path(), "routing repository request");
        let response = match manager.resolve(principal.identity(), &owner, &name).await {
            Ok(Some(route)) => {
                // A rename/recreate can race name lookup. Fence the resolved UUID
                // before the grant's identity reaches any repository handler.
                if operation.is_some_and(|(_, repository)| repository != route.repository.repository_id()) {
                    return unauthorized();
                }
                let (mut parts, body) = request.into_parts();
                // The inner router must extract only its own captures, especially the LFS OID.
                parts.extensions = axum::http::Extensions::new();
                parts.extensions.insert(principal);
                if let Some((operation, _)) = operation {
                    parts.extensions.insert(operation);
                }
                parts.extensions.insert(Arc::<AdmissionPermit>::clone(&permit));
                let request = Request::from_parts(parts, body);
                let response = route.dispatch(request).await;
                tracing::debug!(owner, name, status = %response.status(), "repository response completed");
                response
            }
            Ok(None) => plain(StatusCode::NOT_FOUND, "Repository does not exist"),
            Err(error) => {
                tracing::error!(error = ?error, "repository routing failed");
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
