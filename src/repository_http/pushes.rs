use super::*;

pub(super) async fn read(
    State(state): State<Arc<RepositoryHttp>>,
    Path((name, id)): Path<(String, String)>,
    headers: axum::http::HeaderMap,
) -> Response<Body> {
    let (route, actor) = match authorized_route(&state, &name, &headers, TokenScope::Read).await {
        Ok(value) => value,
        Err(response) => return response,
    };
    let Some(id) = uuid::Uuid::parse_str(&id)
        .ok()
        .filter(|parsed| parsed.to_string() == id)
    else {
        return plain(StatusCode::UNPROCESSABLE_ENTITY, "Invalid push UUID");
    };
    match route
        .repository
        .push_receipt(&actor.account, id.into_bytes())
        .await
    {
        Ok(Some(push)) if (state.manager.ready)() => json_response(
            StatusCode::OK,
            &serde_json::json!({"repository_id":uuid::Uuid::from_bytes(route.repository.repository_id()).to_string(),"push":push}),
        ),
        Ok(None) => plain(StatusCode::NOT_FOUND, "Push does not exist"),
        Ok(Some(_)) => plain(StatusCode::SERVICE_UNAVAILABLE, "Canopy node is not ready"),
        Err(error) => {
            tracing::error!(error = %error, "push receipt read failed");
            plain(
                StatusCode::SERVICE_UNAVAILABLE,
                "Push receipt is unavailable",
            )
        }
    }
}
