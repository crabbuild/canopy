use super::*;
use crate::pulls::{
    PullRevision,
    merge::{MergeOutcome, MergeRequest, MergeStrategy, valid_request},
};
use cellule_runtime::InvocationError;
use std::time::Duration;

const WORK_TIMEOUT_MS: u32 = 120_000;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Input {
    repository_id: String,
    id: String,
    revision: PullRevision,
    strategy: MergeStrategy,
}

pub(super) async fn policy(
    State(state): State<Arc<RepositoryHttp>>,
    Path((name, number)): Path<(String, i64)>,
    headers: axum::http::HeaderMap,
) -> Response<Body> {
    let (route, actor) = match authorized_route(&state, &name, &headers, TokenScope::Read).await {
        Ok(value) => value,
        Err(response) => return response,
    };
    if number < 1 {
        return plain(StatusCode::UNPROCESSABLE_ENTITY, "Invalid pull number");
    }
    match route
        .repository
        .pull_review_policy(&actor.account, number)
        .await
    {
        Ok(result) => match result.output {
            Some(policy) if (state.manager.ready)() => json_response(
                StatusCode::OK,
                &serde_json::json!({"repository_id":uuid::Uuid::from_bytes(route.repository.repository_id()).to_string(),"policy":policy}),
            ),
            None => plain(StatusCode::NOT_FOUND, "Pull request is unavailable"),
            Some(_) => unavailable(),
        },
        Err(error) => failed(error),
    }
}
pub(super) async fn publish(
    State(state): State<Arc<RepositoryHttp>>,
    Path((name, number)): Path<(String, i64)>,
    request: Request<Body>,
) -> Response<Body> {
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
    let work = Arc::clone(&state);
    let task = state.tasks.spawn(async move {
        let response = serve(&work, &name, number, request).await;
        response.map(|body| crate::transfer::response_body(body, permit))
    });
    match task.await {
        Ok(response) => response,
        Err(error) => failed(error),
    }
}
async fn serve(
    state: &RepositoryHttp,
    name: &str,
    number: i64,
    request: Request<Body>,
) -> Response<Body> {
    let (route, actor) = match pulls::writable_route(state, name, request.headers()).await {
        Ok(value) => value,
        Err(response) => return response,
    };
    let input: Input = match pulls::input(request).await {
        Ok(value) => value,
        Err(response) => return response,
    };
    let request = MergeRequest {
        id: input.id,
        revision: input.revision,
        strategy: input.strategy,
    };
    if number < 1 || !valid_request(&request) {
        return plain(StatusCode::UNPROCESSABLE_ENTITY, "Invalid merge request");
    }
    let mut identity = match pulls::identity(&route, &input.repository_id) {
        Ok(value) => value,
        Err(response) => return *response,
    };
    // Preparation can outlast the ordinary 60-second command identity. Cover
    // its work deadline plus the normal publication window, so a valid merge
    // does not expire before reaching the authoritative transaction.
    let Some(expires_at_ms) = identity
        .expires_at_ms
        .checked_add(i64::from(WORK_TIMEOUT_MS))
    else {
        return failed(crate::server::ServerError::Clock);
    };
    identity.expires_at_ms = expires_at_ms;
    let operation = route
        .repository
        .merge_pull(identity, &actor.account, number, request);
    let outcome =
        match tokio::time::timeout(Duration::from_millis(u64::from(WORK_TIMEOUT_MS)), operation)
            .await
        {
            Ok(Ok(result)) if (state.manager.ready)() => result.output,
            Ok(Err(InvocationError::Rejected(rejected))) => rejected.output,
            Ok(Err(error)) => return failed(error),
            Ok(Ok(_)) => return unavailable(),
            Err(_) => {
                return plain(
                    StatusCode::GATEWAY_TIMEOUT,
                    "Merge result is unknown; retry the same request ID and revision",
                );
            }
        };
    let (status, message) = match outcome {
        MergeOutcome::Applied { merge } => {
            return json_response(StatusCode::OK, &serde_json::json!({"merge":merge}));
        }
        MergeOutcome::NotFound => (StatusCode::NOT_FOUND, "Pull request is unavailable"),
        MergeOutcome::Forbidden => (StatusCode::FORBIDDEN, "Repository write access is required"),
        MergeOutcome::Conflict => (
            StatusCode::CONFLICT,
            "Merge identity or pull revision changed",
        ),
        MergeOutcome::ReviewsRequired => {
            (StatusCode::CONFLICT, "Required reviews are not satisfied")
        }
        MergeOutcome::NotFastForward => (
            StatusCode::CONFLICT,
            "Source must descend from the current base for a fast-forward merge",
        ),
        MergeOutcome::BranchPolicy => (
            StatusCode::CONFLICT,
            "Current branch policy rejected the merge",
        ),
    };
    plain(status, message)
}
fn unavailable() -> Response<Body> {
    plain(
        StatusCode::SERVICE_UNAVAILABLE,
        "Merge result is unavailable; retry the same request ID and revision",
    )
}
fn failed(error: impl std::fmt::Display) -> Response<Body> {
    tracing::error!(error = %error,"merge operation failed");
    unavailable()
}
