use super::*;
use crate::Visibility;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Input {
    repository_id: String,
    expected_generation: i64,
    visibility: Visibility,
}

pub(super) async fn read(
    State(state): State<Arc<RepositoryHttp>>,
    Path(name): Path<String>,
    headers: axum::http::HeaderMap,
) -> Response<Body> {
    let (route, _) = match readable_route(&state, &name, &headers).await {
        Ok(route) => route,
        Err(response) => return response,
    };
    match route.repository.visibility().await {
        Ok(value) if (state.manager.ready)() => json_response(
            StatusCode::OK,
            &serde_json::json!({"repository_id":uuid::Uuid::from_bytes(route.repository.repository_id()).to_string(), "visibility":value.output.visibility, "generation":value.output.generation}),
        ),
        Ok(_) => plain(StatusCode::SERVICE_UNAVAILABLE, "Canopy node is not ready"),
        Err(error) => {
            tracing::error!(error=%error,"visibility read failed");
            plain(
                StatusCode::SERVICE_UNAVAILABLE,
                "Repository visibility is unavailable",
            )
        }
    }
}

pub(super) async fn update(
    State(state): State<Arc<RepositoryHttp>>,
    Path(name): Path<String>,
    request: Request<Body>,
) -> Response<Body> {
    let (route, actor) =
        match authorized_route(&state, &name, request.headers(), TokenScope::Admin).await {
            Ok(route) => route,
            Err(response) => return response,
        };
    let input: Input = match pulls::input(request).await {
        Ok(input) => input,
        Err(response) => return response,
    };
    if !(0..i64::MAX).contains(&input.expected_generation) {
        return plain(
            StatusCode::UNPROCESSABLE_ENTITY,
            "Invalid visibility generation",
        );
    }
    if let Err(response) = pulls::identity(&route, &input.repository_id) {
        return *response;
    }
    match state
        .manager
        .set_visibility(
            &route.repository,
            &actor.account,
            input.expected_generation,
            input.visibility,
        )
        .await
    {
        Ok(true) if (state.manager.ready)() => json_response(
            StatusCode::OK,
            &serde_json::json!({"repository_id":input.repository_id, "visibility":input.visibility, "generation":input.expected_generation + 1}),
        ),
        Ok(false) => plain(
            StatusCode::CONFLICT,
            "Repository generation changed; reload visibility",
        ),
        Ok(_) => plain(StatusCode::SERVICE_UNAVAILABLE, "Canopy node is not ready"),
        Err(error) => {
            tracing::error!(error=%error,"visibility update failed");
            plain(
                StatusCode::SERVICE_UNAVAILABLE,
                "Visibility update failed; read current state before retrying",
            )
        }
    }
}
