use super::*;
use crate::{
    issues::{
        COMMENT_PAGE_SIZE, CommentEdit, ISSUE_PAGE_SIZE, IssueChange, IssueEdit, IssueState,
        NewComment, NewIssue, valid_body, valid_issue_text,
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
    state: Option<IssueState>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct CommentQuery {
    #[serde(default)]
    after: i64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateIssue {
    repository_id: String,
    id: String,
    title: String,
    body: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EditIssue {
    repository_id: String,
    expected_version: i64,
    title: String,
    body: String,
    state: IssueState,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateComment {
    repository_id: String,
    id: String,
    body: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EditComment {
    repository_id: String,
    expected_version: i64,
    body: String,
}

async fn writable_route(
    state: &RepositoryHttp,
    name: &str,
    headers: &axum::http::HeaderMap,
) -> Result<(RepositoryRoute, Principal), Response<Body>> {
    let authorized = authorized_route(state, name, headers, TokenScope::Read).await?;
    // Discussion permits repository readers, while token scope independently
    // restricts mutations. The Cell rechecks read access when publishing.
    if authorized.1.scope < TokenScope::Write {
        return Err(plain(StatusCode::FORBIDDEN, "Token scope is insufficient"));
    }
    Ok(authorized)
}

async fn input<T: DeserializeOwned>(request: Request<Body>) -> Result<T, Response<Body>> {
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
                "Issue request is too large",
            ));
        }
        Err(_) => {
            return Err(plain(
                StatusCode::REQUEST_TIMEOUT,
                "Issue request timed out",
            ));
        }
    };
    serde_json::from_slice(&body)
        .map_err(|_| plain(StatusCode::UNPROCESSABLE_ENTITY, "Invalid issue request"))
}

fn canonical_id(value: &str) -> Option<[u8; 16]> {
    let id = uuid::Uuid::parse_str(value).ok()?;
    (id.to_string() == value && validate_repository_id(id.into_bytes()).is_ok())
        .then(|| id.into_bytes())
}

fn identity(
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
    let (route, actor) = match readable_route(&state, &name, &headers).await {
        Ok(value) => value,
        Err(response) => return response,
    };
    if query.after < 0 {
        return plain(StatusCode::UNPROCESSABLE_ENTITY, "Invalid issue cursor");
    }
    match route
        .repository
        .issues(actor.identity(), query.after, query.state)
        .await
    {
        Ok(result) => match result.output {
            Some(issues) if (state.manager.ready)() => {
                let next = (issues.len() == ISSUE_PAGE_SIZE)
                    .then(|| issues.last().map(|issue| issue.number))
                    .flatten();
                json_response(
                    StatusCode::OK,
                    &serde_json::json!({"repository_id": uuid::Uuid::from_bytes(route.repository.repository_id()).to_string(), "issues": issues, "next_after": next}),
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
    let (route, actor) = match readable_route(&state, &name, &headers).await {
        Ok(value) => value,
        Err(response) => return response,
    };
    match route.repository.issue(actor.identity(), number).await {
        Ok(result) => match result.output {
            Some(issue) if (state.manager.ready)() => json_response(
                StatusCode::OK,
                &serde_json::json!({"repository_id": uuid::Uuid::from_bytes(route.repository.repository_id()).to_string(), "issue": issue}),
            ),
            None => missing(),
            Some(_) => unavailable(),
        },
        Err(error) => failed(error),
    }
}

pub(super) async fn comments(
    State(state): State<Arc<RepositoryHttp>>,
    Path((name, number)): Path<(String, i64)>,
    Query(query): Query<CommentQuery>,
    headers: axum::http::HeaderMap,
) -> Response<Body> {
    let (route, actor) = match readable_route(&state, &name, &headers).await {
        Ok(value) => value,
        Err(response) => return response,
    };
    if query.after < 0 {
        return plain(StatusCode::UNPROCESSABLE_ENTITY, "Invalid comment cursor");
    }
    match route
        .repository
        .issue_comments(actor.identity(), number, query.after)
        .await
    {
        Ok(result) => match result.output {
            Some(comments) if (state.manager.ready)() => {
                let next = (comments.len() == COMMENT_PAGE_SIZE)
                    .then(|| comments.last().map(|comment| comment.number))
                    .flatten();
                json_response(
                    StatusCode::OK,
                    &serde_json::json!({"repository_id": uuid::Uuid::from_bytes(route.repository.repository_id()).to_string(), "comments": comments, "next_after": next}),
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
    let input: CreateIssue = match input(request).await {
        Ok(value) => value,
        Err(response) => return response,
    };
    let Some(id) = canonical_id(&input.id) else {
        return plain(StatusCode::UNPROCESSABLE_ENTITY, "Invalid issue UUID");
    };
    if !valid_issue_text(&input.title, &input.body) {
        return plain(StatusCode::UNPROCESSABLE_ENTITY, "Invalid issue text");
    }
    let identity = match identity(&route, &input.repository_id) {
        Ok(value) => value,
        Err(response) => return *response,
    };
    changed(
        &state,
        route
            .repository
            .create_issue(
                identity,
                &actor.account,
                NewIssue {
                    id,
                    title: &input.title,
                    body: &input.body,
                },
            )
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
    let input: EditIssue = match input(request).await {
        Ok(value) => value,
        Err(response) => return response,
    };
    if number < 1
        || !(1..i64::MAX).contains(&input.expected_version)
        || !valid_issue_text(&input.title, &input.body)
    {
        return plain(StatusCode::UNPROCESSABLE_ENTITY, "Invalid issue update");
    }
    let identity = match identity(&route, &input.repository_id) {
        Ok(value) => value,
        Err(response) => return *response,
    };
    changed(
        &state,
        route
            .repository
            .edit_issue(
                identity,
                &actor.account,
                number,
                IssueEdit {
                    expected_version: input.expected_version,
                    title: &input.title,
                    body: &input.body,
                    state: input.state,
                },
            )
            .await,
        false,
    )
}

pub(super) async fn create_comment(
    State(state): State<Arc<RepositoryHttp>>,
    Path((name, number)): Path<(String, i64)>,
    request: Request<Body>,
) -> Response<Body> {
    let (route, actor) = match writable_route(&state, &name, request.headers()).await {
        Ok(value) => value,
        Err(response) => return response,
    };
    let input: CreateComment = match input(request).await {
        Ok(value) => value,
        Err(response) => return response,
    };
    let Some(id) = canonical_id(&input.id) else {
        return plain(StatusCode::UNPROCESSABLE_ENTITY, "Invalid comment UUID");
    };
    if number < 1 || input.body.trim().is_empty() || !valid_body(&input.body) {
        return plain(StatusCode::UNPROCESSABLE_ENTITY, "Invalid comment text");
    }
    let identity = match identity(&route, &input.repository_id) {
        Ok(value) => value,
        Err(response) => return *response,
    };
    changed(
        &state,
        route
            .repository
            .create_issue_comment(
                identity,
                &actor.account,
                number,
                NewComment {
                    id,
                    body: &input.body,
                },
            )
            .await,
        true,
    )
}

pub(super) async fn edit_comment(
    State(state): State<Arc<RepositoryHttp>>,
    Path((name, issue, number)): Path<(String, i64, i64)>,
    request: Request<Body>,
) -> Response<Body> {
    let (route, actor) = match writable_route(&state, &name, request.headers()).await {
        Ok(value) => value,
        Err(response) => return response,
    };
    let input: EditComment = match input(request).await {
        Ok(value) => value,
        Err(response) => return response,
    };
    if issue < 1
        || number < 1
        || !(1..i64::MAX).contains(&input.expected_version)
        || input.body.trim().is_empty()
        || !valid_body(&input.body)
    {
        return plain(StatusCode::UNPROCESSABLE_ENTITY, "Invalid comment update");
    }
    let identity = match identity(&route, &input.repository_id) {
        Ok(value) => value,
        Err(response) => return *response,
    };
    changed(
        &state,
        route
            .repository
            .edit_issue_comment(
                identity,
                &actor.account,
                issue,
                number,
                CommentEdit {
                    expected_version: input.expected_version,
                    body: &input.body,
                },
            )
            .await,
        false,
    )
}

fn changed(
    state: &RepositoryHttp,
    result: Result<Committed<IssueChange>, InvocationError<Vec<SqlResultSet>>>,
    created: bool,
) -> Response<Body> {
    match result {
        Ok(result) => match result.output {
            IssueChange::Applied(number) if (state.manager.ready)() => {
                if created {
                    json_response(StatusCode::OK, &serde_json::json!({"number": number}))
                } else {
                    plain(StatusCode::NO_CONTENT, "")
                }
            }
            IssueChange::Applied(_) => unavailable(),
            IssueChange::NotFound => missing(),
            IssueChange::Forbidden => plain(
                StatusCode::FORBIDDEN,
                "Issue author or repository writer required",
            ),
            IssueChange::Conflict => {
                plain(StatusCode::CONFLICT, "Issue identity or version conflict")
            }
        },
        Err(error) => failed(error),
    }
}

fn missing() -> Response<Body> {
    plain(StatusCode::NOT_FOUND, "Issue or comment is unavailable")
}
fn unavailable() -> Response<Body> {
    plain(
        StatusCode::SERVICE_UNAVAILABLE,
        "Issue service unavailable; read current state before retrying edits",
    )
}
fn failed(error: impl std::fmt::Display) -> Response<Body> {
    tracing::error!(error = %error, "issue operation failed");
    unavailable()
}
