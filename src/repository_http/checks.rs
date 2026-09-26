use super::*;
use crate::{
    checks::{
        CHECK_PAGE_SIZE, CheckChange, CheckContextEdit, CheckEdit, CheckState, NewCheck,
        valid_summary,
    },
    server::{RepositoryRoute, mutation_identity},
};
use cellule_runtime::{Committed, InvocationError, MutationIdentity, SqlResultSet};
use serde::de::DeserializeOwned;
use std::time::Duration;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Cursor {
    after: Option<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ContextInput {
    repository_id: String,
    expected_version: i64,
    reporter: String,
    enabled: bool,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StartInput {
    repository_id: String,
    id: String,
    context: String,
    context_version: i64,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct UpdateInput {
    repository_id: String,
    expected_version: i64,
    state: CheckState,
    summary: String,
}

async fn writer(
    state: &RepositoryHttp,
    name: &str,
    headers: &axum::http::HeaderMap,
) -> Result<(RepositoryRoute, Principal), Response<Body>> {
    let route = authorized_route(state, name, headers, TokenScope::Read).await?;
    if route.1.scope < TokenScope::Write {
        return Err(plain(StatusCode::FORBIDDEN, "Token scope is insufficient"));
    }
    Ok(route)
}

async fn input<T: DeserializeOwned>(request: Request<Body>) -> Result<T, Response<Body>> {
    let body = match tokio::time::timeout(
        Duration::from_secs(30),
        to_bytes(request.into_body(), 32 * 1024),
    )
    .await
    {
        Ok(Ok(body)) => body,
        Ok(Err(_)) => {
            return Err(plain(
                StatusCode::PAYLOAD_TOO_LARGE,
                "Check request is too large",
            ));
        }
        Err(_) => {
            return Err(plain(
                StatusCode::REQUEST_TIMEOUT,
                "Check request timed out",
            ));
        }
    };
    serde_json::from_slice(&body)
        .map_err(|_| plain(StatusCode::UNPROCESSABLE_ENTITY, "Invalid check request"))
}

fn uuid(value: &str) -> Option<[u8; 16]> {
    let id = uuid::Uuid::parse_str(value).ok()?;
    (id.to_string() == value && validate_repository_id(id.into_bytes()).is_ok())
        .then(|| id.into_bytes())
}

fn oid(value: &str) -> Option<[u8; 20]> {
    if value.len() != 40
        || !value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return None;
    }
    let mut bytes = [0; 20];
    hex::decode_to_slice(value, &mut bytes).ok()?;
    Some(bytes)
}

fn identity(
    route: &RepositoryRoute,
    expected: &str,
) -> Result<MutationIdentity, Box<Response<Body>>> {
    let expected = uuid(expected).ok_or_else(|| {
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

pub(super) async fn contexts(
    State(state): State<Arc<RepositoryHttp>>,
    Path(name): Path<String>,
    Query(cursor): Query<Cursor>,
    headers: axum::http::HeaderMap,
) -> Response<Body> {
    let (route, actor) = match readable_route(&state, &name, &headers).await {
        Ok(value) => value,
        Err(response) => return response,
    };
    if cursor
        .after
        .as_deref()
        .is_some_and(|after| directory::validate_component(after).is_err())
    {
        return plain(StatusCode::UNPROCESSABLE_ENTITY, "Invalid check cursor");
    }
    match route
        .repository
        .check_contexts(actor.identity(), cursor.after.as_deref())
        .await
    {
        Ok(result) => match result.output {
            Some(contexts) if (state.manager.ready)() => {
                let next = (contexts.len() == CHECK_PAGE_SIZE)
                    .then(|| contexts.last().map(|context| context.name.clone()))
                    .flatten();
                json_response(
                    StatusCode::OK,
                    &serde_json::json!({"repository_id": uuid::Uuid::from_bytes(route.repository.repository_id()).to_string(), "contexts":contexts, "next_after":next}),
                )
            }
            None => missing(),
            Some(_) => unavailable(),
        },
        Err(error) => failed(error),
    }
}

pub(super) async fn set_context(
    State(state): State<Arc<RepositoryHttp>>,
    Path((name, context)): Path<(String, String)>,
    request: Request<Body>,
) -> Response<Body> {
    let (route, actor) =
        match authorized_route(&state, &name, request.headers(), TokenScope::Admin).await {
            Ok(value) => value,
            Err(response) => return response,
        };
    let input: ContextInput = match input(request).await {
        Ok(value) => value,
        Err(response) => return response,
    };
    if directory::validate_component(&context).is_err()
        || directory::validate_component(&input.reporter).is_err()
        || !(0..i64::MAX).contains(&input.expected_version)
    {
        return plain(StatusCode::UNPROCESSABLE_ENTITY, "Invalid check context");
    }
    let identity = match identity(&route, &input.repository_id) {
        Ok(value) => value,
        Err(response) => return *response,
    };
    changed(
        &state,
        route
            .repository
            .set_check_context(
                identity,
                &actor.account,
                &context,
                CheckContextEdit {
                    expected_version: input.expected_version,
                    reporter: &input.reporter,
                    enabled: input.enabled,
                },
            )
            .await,
        None,
    )
}

pub(super) async fn commit(
    State(state): State<Arc<RepositoryHttp>>,
    Path((name, commit)): Path<(String, String)>,
    Query(cursor): Query<Cursor>,
    headers: axum::http::HeaderMap,
) -> Response<Body> {
    let (route, actor) = match readable_route(&state, &name, &headers).await {
        Ok(value) => value,
        Err(response) => return response,
    };
    let Some(oid) = oid(&commit) else {
        return plain(StatusCode::UNPROCESSABLE_ENTITY, "Invalid commit OID");
    };
    if cursor
        .after
        .as_deref()
        .is_some_and(|after| directory::validate_component(after).is_err())
    {
        return plain(StatusCode::UNPROCESSABLE_ENTITY, "Invalid check cursor");
    }
    match route
        .repository
        .commit_checks(actor.identity(), oid, cursor.after.as_deref())
        .await
    {
        Ok(result) => match result.output {
            Some(checks) if (state.manager.ready)() => {
                let next = (checks.len() == CHECK_PAGE_SIZE)
                    .then(|| checks.last().map(|check| check.context.name.clone()))
                    .flatten();
                json_response(
                    StatusCode::OK,
                    &serde_json::json!({"repository_id":uuid::Uuid::from_bytes(route.repository.repository_id()).to_string(), "oid":commit, "checks":checks, "next_after":next}),
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
    Path((name, id)): Path<(String, String)>,
    headers: axum::http::HeaderMap,
) -> Response<Body> {
    let (route, actor) = match readable_route(&state, &name, &headers).await {
        Ok(value) => value,
        Err(response) => return response,
    };
    let Some(id) = uuid(&id) else {
        return plain(StatusCode::UNPROCESSABLE_ENTITY, "Invalid check UUID");
    };
    match route.repository.check_run(actor.identity(), id).await {
        Ok(result) => match result.output {
            Some(run) if (state.manager.ready)() => json_response(
                StatusCode::OK,
                &serde_json::json!({"repository_id":uuid::Uuid::from_bytes(route.repository.repository_id()).to_string(), "check":run}),
            ),
            None => missing(),
            Some(_) => unavailable(),
        },
        Err(error) => failed(error),
    }
}

pub(super) async fn start(
    State(state): State<Arc<RepositoryHttp>>,
    Path((name, commit)): Path<(String, String)>,
    request: Request<Body>,
) -> Response<Body> {
    let (route, actor) = match writer(&state, &name, request.headers()).await {
        Ok(value) => value,
        Err(response) => return response,
    };
    let Some(oid) = oid(&commit) else {
        return plain(StatusCode::UNPROCESSABLE_ENTITY, "Invalid commit OID");
    };
    let input: StartInput = match input(request).await {
        Ok(value) => value,
        Err(response) => return response,
    };
    let Some(id) = uuid(&input.id) else {
        return plain(StatusCode::UNPROCESSABLE_ENTITY, "Invalid check UUID");
    };
    if directory::validate_component(&input.context).is_err() || input.context_version < 1 {
        return plain(StatusCode::UNPROCESSABLE_ENTITY, "Invalid check context");
    }
    let identity = match identity(&route, &input.repository_id) {
        Ok(value) => value,
        Err(response) => return *response,
    };
    changed(
        &state,
        route
            .repository
            .start_check(
                identity,
                &actor.account,
                NewCheck {
                    id,
                    oid,
                    context: &input.context,
                    context_version: input.context_version,
                },
            )
            .await,
        Some(id),
    )
}

pub(super) async fn update(
    State(state): State<Arc<RepositoryHttp>>,
    Path((name, id)): Path<(String, String)>,
    request: Request<Body>,
) -> Response<Body> {
    let (route, actor) = match writer(&state, &name, request.headers()).await {
        Ok(value) => value,
        Err(response) => return response,
    };
    let Some(id) = uuid(&id) else {
        return plain(StatusCode::UNPROCESSABLE_ENTITY, "Invalid check UUID");
    };
    let input: UpdateInput = match input(request).await {
        Ok(value) => value,
        Err(response) => return response,
    };
    if !(1..i64::MAX).contains(&input.expected_version)
        || input.state == CheckState::Queued
        || !valid_summary(&input.summary)
    {
        return plain(StatusCode::UNPROCESSABLE_ENTITY, "Invalid check update");
    }
    let identity = match identity(&route, &input.repository_id) {
        Ok(value) => value,
        Err(response) => return *response,
    };
    changed(
        &state,
        route
            .repository
            .update_check(
                identity,
                &actor.account,
                id,
                CheckEdit {
                    expected_version: input.expected_version,
                    state: input.state,
                    summary: &input.summary,
                },
            )
            .await,
        None,
    )
}

fn changed(
    state: &RepositoryHttp,
    result: Result<Committed<CheckChange>, InvocationError<Vec<SqlResultSet>>>,
    id: Option<[u8; 16]>,
) -> Response<Body> {
    match result {
        Ok(result) => match result.output {
            CheckChange::Applied if (state.manager.ready)() => match id {
                Some(id) => json_response(
                    StatusCode::OK,
                    &serde_json::json!({"id":uuid::Uuid::from_bytes(id).to_string()}),
                ),
                None => plain(StatusCode::NO_CONTENT, ""),
            },
            CheckChange::Applied => unavailable(),
            CheckChange::NotFound => missing(),
            CheckChange::Forbidden => plain(
                StatusCode::FORBIDDEN,
                "Check owner or configured reporter required",
            ),
            CheckChange::Conflict => plain(
                StatusCode::CONFLICT,
                "Check identity, policy or run version conflict",
            ),
        },
        Err(error) => failed(error),
    }
}
fn missing() -> Response<Body> {
    plain(StatusCode::NOT_FOUND, "Check or commit is unavailable")
}
fn unavailable() -> Response<Body> {
    plain(
        StatusCode::SERVICE_UNAVAILABLE,
        "Check service unavailable; read current state before retrying edits",
    )
}
fn failed(error: impl std::fmt::Display) -> Response<Body> {
    tracing::error!(error = %error, "check operation failed");
    unavailable()
}
