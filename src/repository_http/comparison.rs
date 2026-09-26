use super::*;
use crate::git_read::{ComparisonTarget, ReadError, Reader, Side};
use std::time::Duration;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Input {
    repository_id: String,
    target: ComparisonTarget,
    query: View,
}
#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase", deny_unknown_fields)]
enum View {
    Files { after: Option<String> },
    Patch { path_base64: String },
    File { path_base64: String, side: Side },
}

pub(super) async fn compare(
    State(state): State<Arc<RepositoryHttp>>,
    Path((name, number)): Path<(String, i64)>,
    request: Request<Body>,
) -> Response<Body> {
    // Share node admission with Git/LFS. Detached work and output frames retain
    // the permit, including blocking CPU work after its caller disconnects.
    let Ok(permit) = Arc::clone(&state.transfers).try_acquire_owned() else {
        let mut response = plain(
            StatusCode::SERVICE_UNAVAILABLE,
            "Canopy transfer capacity is full; retry the request",
        );
        response.headers_mut().insert(
            header::RETRY_AFTER,
            axum::http::HeaderValue::from_static("1"),
        );
        return response;
    };
    let permit = Arc::new(permit);
    let work_state = Arc::clone(&state);
    let task = state.tasks.spawn(async move {
        let response = serve(&work_state, &name, number, request, Arc::clone(&permit)).await;
        response.map(|body| crate::transfer::response_body(body, permit))
    });
    match task.await {
        Ok(response) => response,
        Err(error) => {
            tracing::error!(error = %error,"comparison task failed");
            plain(StatusCode::SERVICE_UNAVAILABLE, "Comparison is unavailable")
        }
    }
}
async fn serve(
    state: &RepositoryHttp,
    name: &str,
    number: i64,
    request: Request<Body>,
    permit: Arc<OwnedSemaphorePermit>,
) -> Response<Body> {
    let (route, actor) =
        match authorized_route(state, name, request.headers(), TokenScope::Read).await {
            Ok(value) => value,
            Err(response) => return response,
        };
    let body = match tokio::time::timeout(
        Duration::from_secs(30),
        to_bytes(request.into_body(), 32 * 1024),
    )
    .await
    {
        Ok(Ok(body)) => body,
        Ok(Err(_)) => {
            return plain(
                StatusCode::PAYLOAD_TOO_LARGE,
                "Comparison request is too large",
            );
        }
        Err(_) => return plain(StatusCode::REQUEST_TIMEOUT, "Comparison request timed out"),
    };
    let Ok(input) = serde_json::from_slice::<Input>(&body) else {
        return plain(
            StatusCode::UNPROCESSABLE_ENTITY,
            "Invalid comparison request",
        );
    };
    if uuid::Uuid::parse_str(&input.repository_id)
        .ok()
        .is_none_or(|id| {
            id.to_string() != input.repository_id
                || validate_repository_id(id.into_bytes()).is_err()
        })
    {
        return plain(
            StatusCode::UNPROCESSABLE_ENTITY,
            "Invalid repository identity",
        );
    }
    let expected = uuid::Uuid::from_bytes(route.repository.repository_id()).to_string();
    if input.repository_id != expected {
        return plain(StatusCode::CONFLICT, "Repository identity changed");
    }
    if number < 1 {
        return plain(StatusCode::UNPROCESSABLE_ENTITY, "Invalid pull number");
    }
    let reader = Reader::new(Arc::clone(&route.repository), permit);
    let operation = async {
        match input.query {
            View::Files { after } => {
                let comparison = reader
                    .files(&actor.account, number, input.target, after.as_deref())
                    .await?;
                Ok(json_response(
                    StatusCode::OK,
                    &serde_json::json!({"repository_id":expected,"comparison":comparison}),
                ))
            }
            View::Patch { path_base64 } => {
                let patch = reader
                    .patch(&actor.account, number, input.target, &path_base64)
                    .await?;
                Ok(json_response(
                    StatusCode::OK,
                    &serde_json::json!({"repository_id":expected,"patch":patch}),
                ))
            }
            View::File { path_base64, side } => {
                let file = reader
                    .file(&actor.account, number, input.target, &path_base64, side)
                    .await?;
                Ok(json_response(
                    StatusCode::OK,
                    &serde_json::json!({"repository_id":expected,"file":file}),
                ))
            }
        }
    };
    match tokio::time::timeout(Duration::from_secs(120), operation).await {
        Ok(Ok(response)) if (state.manager.ready)() => response,
        Ok(Ok(_)) => plain(StatusCode::SERVICE_UNAVAILABLE, "Canopy node is not ready"),
        Ok(Err(error)) => failed(error),
        Err(_) => plain(
            StatusCode::GATEWAY_TIMEOUT,
            "Comparison exceeded its time limit",
        ),
    }
}
fn failed(error: ReadError) -> Response<Body> {
    let (status, message) = match error {
        ReadError::Missing => (StatusCode::NOT_FOUND, "Pull request or file is unavailable"),
        ReadError::Changed => (
            StatusCode::CONFLICT,
            "Pull request revision changed; read current state",
        ),
        ReadError::Unrelated => (
            StatusCode::CONFLICT,
            "Source and base have unrelated histories",
        ),
        ReadError::Ambiguous => (
            StatusCode::CONFLICT,
            "Comparison requires a unique merge base",
        ),
        ReadError::TooLarge => (
            StatusCode::PAYLOAD_TOO_LARGE,
            "Comparison exceeds its traversal or output limit",
        ),
        ReadError::Invalid => (
            StatusCode::UNPROCESSABLE_ENTITY,
            "Invalid comparison revision or path",
        ),
        error => {
            tracing::error!(error = %error,"comparison failed");
            (
                StatusCode::SERVICE_UNAVAILABLE,
                "Comparison data is unavailable",
            )
        }
    };
    plain(status, message)
}
