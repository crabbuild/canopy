use super::*;
use crate::{
    issues::valid_issue_text,
    pulls::{
        NewPull, NewReview, PULL_PAGE_SIZE, PullChange, PullEdit, PullRevision, PullState,
        REVIEW_PAGE_SIZE, ReviewKind, valid_new, valid_review,
    },
    server::{RepositoryRoute, mutation_identity},
};
use cellule_runtime::{Committed, InvocationError, MutationIdentity, SqlResultSet};
use serde::de::DeserializeOwned;
use std::time::Duration;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ListQuery {
    #[serde(default)]
    after: i64,
    state: Option<PullState>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ReviewQuery {
    #[serde(default)]
    after: i64,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Create {
    repository_id: String,
    id: String,
    title: String,
    body: String,
    draft: bool,
    source_ref: String,
    source_oid: String,
    base_ref: String,
    base_oid: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Edit {
    repository_id: String,
    expected_version: i64,
    title: String,
    body: String,
    state: PullState,
    draft: bool,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Review {
    repository_id: String,
    id: String,
    revision: PullRevision,
    kind: ReviewKind,
    body: String,
}

pub(super) async fn writable_route(
    state: &RepositoryHttp,
    name: &str,
    headers: &axum::http::HeaderMap,
) -> Result<(RepositoryRoute, Principal), Response<Body>> {
    let authorized = authorized_route(state, name, headers, TokenScope::Read).await?;
    if authorized.1.scope < TokenScope::Write {
        return Err(plain(StatusCode::FORBIDDEN, "Token scope is insufficient"));
    }
    Ok(authorized)
}
pub(super) async fn input<T: DeserializeOwned>(
    request: Request<Body>,
) -> Result<T, Response<Body>> {
    let body = match tokio::time::timeout(
        Duration::from_secs(30),
        to_bytes(request.into_body(), 128 * 1024),
    )
    .await
    {
        Ok(Ok(body)) => body,
        Ok(Err(_)) => {
            return Err(plain(
                StatusCode::PAYLOAD_TOO_LARGE,
                "Pull request body is too large",
            ));
        }
        Err(_) => {
            return Err(plain(
                StatusCode::REQUEST_TIMEOUT,
                "Pull request body timed out",
            ));
        }
    };
    serde_json::from_slice(&body).map_err(|_| {
        plain(
            StatusCode::UNPROCESSABLE_ENTITY,
            "Invalid pull request input",
        )
    })
}
fn canonical_id(value: &str) -> Option<[u8; 16]> {
    let id = uuid::Uuid::parse_str(value).ok()?;
    (id.to_string() == value && validate_repository_id(id.into_bytes()).is_ok())
        .then(|| id.into_bytes())
}
pub(super) fn identity(
    route: &RepositoryRoute,
    expected: &str,
) -> Result<MutationIdentity, Box<Response<Body>>> {
    let expected = canonical_id(expected).ok_or_else(|| {
        Box::new(plain(
            StatusCode::UNPROCESSABLE_ENTITY,
            "Invalid repository UUID",
        ))
    })?;
    if route.repository.repository_id() != expected {
        return Err(Box::new(plain(
            StatusCode::CONFLICT,
            "Repository identity changed",
        )));
    }
    mutation_identity().map_err(|error| Box::new(failed(error)))
}
pub(super) async fn list(
    State(state): State<Arc<RepositoryHttp>>,
    Path(name): Path<String>,
    Query(query): Query<ListQuery>,
    headers: axum::http::HeaderMap,
) -> Response<Body> {
    let (route, actor) = match authorized_route(&state, &name, &headers, TokenScope::Read).await {
        Ok(value) => value,
        Err(response) => return response,
    };
    if query.after < 0 {
        return plain(StatusCode::UNPROCESSABLE_ENTITY, "Invalid pull cursor");
    }
    match route
        .repository
        .pulls(&actor.account, query.after, query.state)
        .await
    {
        Ok(result) => match result.output {
            Some(pulls) if (state.manager.ready)() => {
                let next = (pulls.len() == PULL_PAGE_SIZE)
                    .then(|| pulls.last().map(|pull| pull.number))
                    .flatten();
                json_response(
                    StatusCode::OK,
                    &serde_json::json!({"repository_id":uuid::Uuid::from_bytes(route.repository.repository_id()).to_string(), "pulls":pulls,"next_after":next}),
                )
            }
            None => missing(),
            Some(_) => unavailable(),
        },
        Err(error) => failed(error),
    }
}
pub(super) async fn read(
    State(state): State<Arc<RepositoryHttp>>,
    Path((name, number)): Path<(String, i64)>,
    headers: axum::http::HeaderMap,
) -> Response<Body> {
    let (route, actor) = match authorized_route(&state, &name, &headers, TokenScope::Read).await {
        Ok(value) => value,
        Err(response) => return response,
    };
    match route.repository.pull(&actor.account, number).await {
        Ok(result) => match result.output {
            Some(pull) if (state.manager.ready)() => json_response(
                StatusCode::OK,
                &serde_json::json!({"repository_id":uuid::Uuid::from_bytes(route.repository.repository_id()).to_string(),"pull":pull}),
            ),
            None => missing(),
            Some(_) => unavailable(),
        },
        Err(error) => failed(error),
    }
}
pub(super) async fn reviews(
    State(state): State<Arc<RepositoryHttp>>,
    Path((name, number)): Path<(String, i64)>,
    Query(query): Query<ReviewQuery>,
    headers: axum::http::HeaderMap,
) -> Response<Body> {
    let (route, actor) = match authorized_route(&state, &name, &headers, TokenScope::Read).await {
        Ok(value) => value,
        Err(response) => return response,
    };
    if query.after < 0 {
        return plain(StatusCode::UNPROCESSABLE_ENTITY, "Invalid review cursor");
    }
    match route
        .repository
        .pull_reviews(&actor.account, number, query.after)
        .await
    {
        Ok(result) => match result.output {
            Some(reviews) if (state.manager.ready)() => {
                let next = (reviews.len() == REVIEW_PAGE_SIZE)
                    .then(|| reviews.last().map(|review| review.number))
                    .flatten();
                json_response(
                    StatusCode::OK,
                    &serde_json::json!({"repository_id":uuid::Uuid::from_bytes(route.repository.repository_id()).to_string(), "reviews":reviews,"next_after":next}),
                )
            }
            None => missing(),
            Some(_) => unavailable(),
        },
        Err(error) => failed(error),
    }
}
pub(super) async fn create(
    State(state): State<Arc<RepositoryHttp>>,
    Path(name): Path<String>,
    request: Request<Body>,
) -> Response<Body> {
    let (route, actor) = match writable_route(&state, &name, request.headers()).await {
        Ok(value) => value,
        Err(response) => return response,
    };
    let input: Create = match input(request).await {
        Ok(value) => value,
        Err(response) => return response,
    };
    let Some(id) = canonical_id(&input.id) else {
        return plain(StatusCode::UNPROCESSABLE_ENTITY, "Invalid pull UUID");
    };
    let new = NewPull {
        id,
        title: &input.title,
        body: &input.body,
        draft: input.draft,
        source_ref: &input.source_ref,
        source_oid: &input.source_oid,
        base_ref: &input.base_ref,
        base_oid: &input.base_oid,
    };
    if !valid_new(&new) {
        return plain(StatusCode::UNPROCESSABLE_ENTITY, "Invalid pull creation");
    }
    let identity = match identity(&route, &input.repository_id) {
        Ok(value) => value,
        Err(response) => return *response,
    };
    changed(
        &state,
        route
            .repository
            .create_pull(identity, &actor.account, new)
            .await,
        true,
    )
}
pub(super) async fn edit(
    State(state): State<Arc<RepositoryHttp>>,
    Path((name, number)): Path<(String, i64)>,
    request: Request<Body>,
) -> Response<Body> {
    let (route, actor) = match writable_route(&state, &name, request.headers()).await {
        Ok(value) => value,
        Err(response) => return response,
    };
    let input: Edit = match input(request).await {
        Ok(value) => value,
        Err(response) => return response,
    };
    if number < 1
        || input.state == PullState::Merged
        || !(1..i64::MAX).contains(&input.expected_version)
        || !valid_issue_text(&input.title, &input.body)
    {
        return plain(StatusCode::UNPROCESSABLE_ENTITY, "Invalid pull edit");
    }
    let identity = match identity(&route, &input.repository_id) {
        Ok(value) => value,
        Err(response) => return *response,
    };
    changed(
        &state,
        route
            .repository
            .edit_pull(
                identity,
                &actor.account,
                number,
                PullEdit {
                    expected_version: input.expected_version,
                    title: &input.title,
                    body: &input.body,
                    state: input.state,
                    draft: input.draft,
                },
            )
            .await,
        false,
    )
}
pub(super) async fn review(
    State(state): State<Arc<RepositoryHttp>>,
    Path((name, number)): Path<(String, i64)>,
    request: Request<Body>,
) -> Response<Body> {
    let (route, actor) = match writable_route(&state, &name, request.headers()).await {
        Ok(value) => value,
        Err(response) => return response,
    };
    let input: Review = match input(request).await {
        Ok(value) => value,
        Err(response) => return response,
    };
    let Some(id) = canonical_id(&input.id) else {
        return plain(StatusCode::UNPROCESSABLE_ENTITY, "Invalid review UUID");
    };
    let review = NewReview {
        id,
        revision: &input.revision,
        kind: input.kind,
        body: &input.body,
    };
    if number < 1 || !valid_review(&review) {
        return plain(StatusCode::UNPROCESSABLE_ENTITY, "Invalid pull review");
    }
    let identity = match identity(&route, &input.repository_id) {
        Ok(value) => value,
        Err(response) => return *response,
    };
    changed(
        &state,
        route
            .repository
            .review_pull(identity, &actor.account, number, review)
            .await,
        true,
    )
}
fn changed(
    state: &RepositoryHttp,
    result: Result<Committed<PullChange>, InvocationError<Vec<SqlResultSet>>>,
    created: bool,
) -> Response<Body> {
    match result {
        Ok(result) => match result.output {
            PullChange::Applied(number) if (state.manager.ready)() => {
                if created {
                    json_response(StatusCode::OK, &serde_json::json!({"number":number}))
                } else {
                    plain(StatusCode::NO_CONTENT, "")
                }
            }
            PullChange::Applied(_) => unavailable(),
            PullChange::NotFound => missing(),
            PullChange::Forbidden => plain(
                StatusCode::FORBIDDEN,
                "Pull author or eligible repository writer required",
            ),
            PullChange::Conflict => plain(
                StatusCode::CONFLICT,
                "Pull identity, version or branch revision changed",
            ),
        },
        Err(error) => failed(error),
    }
}
fn missing() -> Response<Body> {
    plain(StatusCode::NOT_FOUND, "Pull request is unavailable")
}
fn unavailable() -> Response<Body> {
    plain(
        StatusCode::SERVICE_UNAVAILABLE,
        "Pull request service unavailable; read current state before retrying edits",
    )
}
fn failed(error: impl std::fmt::Display) -> Response<Body> {
    tracing::error!(error = %error, "pull request operation failed");
    unavailable()
}
