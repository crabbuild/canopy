use super::*;
use crate::pulls::{
    PullRevision,
    candidates::{CandidateOutcome, CandidateRequest, valid_request},
    merge::MergeStrategy,
};
use std::time::Duration;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Input {
    repository_id: String,
    id: String,
    revision: PullRevision,
    strategy: MergeStrategy,
    message: String,
}

pub(super) async fn read(
    State(state): State<Arc<RepositoryHttp>>,
    Path((name, number, id)): Path<(String, i64, String)>,
    headers: axum::http::HeaderMap,
) -> Response<Body> {
    let (route, actor) = match readable_route(&state, &name, &headers).await {
        Ok(value) => value,
        Err(response) => return response,
    };
    if number < 1
        || uuid::Uuid::parse_str(&id).ok().is_none_or(|value| {
            value.to_string() != id || validate_repository_id(value.into_bytes()).is_err()
        })
    {
        return plain(
            StatusCode::UNPROCESSABLE_ENTITY,
            "Invalid candidate identity",
        );
    }
    match route
        .repository
        .merge_candidate(actor.identity(), number, &id)
        .await
    {
        Ok(result) if (state.manager.ready)() => match result.output {
            Some(candidate) => applied(route.repository.repository_id(), candidate),
            None => plain(StatusCode::NOT_FOUND, "Merge candidate is unavailable"),
        },
        Ok(_) => unavailable(),
        Err(error) => failed(error),
    }
}
pub(super) async fn prepare(
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
    let work = Arc::clone(&state);
    let task = state.tasks.spawn(async move {
        let response = serve(&work, &name, number, request).await;
        response.map(|body| crate::transfer::response_body(body, Arc::new(permit)))
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
    let candidate = CandidateRequest {
        id: input.id,
        revision: input.revision,
        strategy: input.strategy,
        message: input.message,
    };
    if number < 1 || !valid_request(&candidate) {
        return plain(
            StatusCode::UNPROCESSABLE_ENTITY,
            "Invalid merge candidate request",
        );
    }
    if let Err(response) = pulls::identity(&route, &input.repository_id) {
        return *response;
    }
    let outcome = match tokio::time::timeout(
        Duration::from_secs(120),
        route
            .gateway
            .prepare_candidate(&actor.account, number, candidate),
    )
    .await
    {
        Ok(Ok(outcome)) if (state.manager.ready)() => outcome,
        Ok(Ok(_)) => return unavailable(),
        Ok(Err(crate::git_gateway::GatewayError::Http(
            crate::git_http::GitHttpError::TooLarge,
        ))) => {
            return plain(
                StatusCode::PAYLOAD_TOO_LARGE,
                "Native merge result exceeds the candidate limit",
            );
        }
        Ok(Err(error)) => return failed(error),
        Err(_) => {
            return plain(
                StatusCode::GATEWAY_TIMEOUT,
                "Candidate result is unknown; retry the same ID and intent",
            );
        }
    };
    match outcome {
        CandidateOutcome::Applied(candidate) => {
            applied(route.repository.repository_id(), *candidate)
        }
        CandidateOutcome::NotFound => plain(StatusCode::NOT_FOUND, "Pull request is unavailable"),
        CandidateOutcome::Forbidden => {
            plain(StatusCode::FORBIDDEN, "Repository write access is required")
        }
        CandidateOutcome::Conflict => plain(
            StatusCode::CONFLICT,
            "Candidate identity or pull revision changed",
        ),
    }
}
fn applied(
    repository_id: [u8; 16],
    candidate: crate::pulls::candidates::MergeCandidate,
) -> Response<Body> {
    let fetch_ref = matches!(
        candidate.result,
        crate::pulls::candidates::CandidateResult::Ready { .. }
    )
    .then(|| candidate.fetch_ref());
    json_response(
        StatusCode::OK,
        &serde_json::json!({"repository_id":uuid::Uuid::from_bytes(repository_id).to_string(),"candidate":candidate,"fetch_ref":fetch_ref}),
    )
}
fn unavailable() -> Response<Body> {
    plain(
        StatusCode::SERVICE_UNAVAILABLE,
        "Candidate service unavailable; retry the same ID and intent",
    )
}
fn failed(error: impl std::fmt::Display) -> Response<Body> {
    tracing::error!(error = %error, "merge candidate preparation failed");
    unavailable()
}
