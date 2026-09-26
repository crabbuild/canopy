use super::*;
use crate::{
    branch_rules::{BranchRuleEdit, RULE_PAGE_SIZE, valid_edit},
    default_branch::valid_default_branch,
    server::mutation_identity,
};
use cellule_runtime::InvocationError;
use std::time::Duration;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Cursor {
    after: Option<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Update {
    repository_id: String,
    rule: BranchRuleEdit,
}

pub(super) async fn list(
    State(state): State<Arc<RepositoryHttp>>,
    Path(name): Path<String>,
    Query(cursor): Query<Cursor>,
    headers: axum::http::HeaderMap,
) -> Response<Body> {
    let (route, actor) = match authorized_route(&state, &name, &headers, TokenScope::Read).await {
        Ok(value) => value,
        Err(response) => return response,
    };
    if cursor
        .after
        .as_deref()
        .is_some_and(|after| !valid_default_branch(after))
    {
        return plain(
            StatusCode::UNPROCESSABLE_ENTITY,
            "Invalid branch rule cursor",
        );
    }
    match route
        .repository
        .branch_rules(&actor.account, cursor.after.as_deref())
        .await
    {
        Ok(result) => match result.output {
            Some(rules) if (state.manager.ready)() => {
                let next = (rules.len() == RULE_PAGE_SIZE)
                    .then(|| rules.last().map(|rule| rule.reference.clone()))
                    .flatten();
                json_response(
                    StatusCode::OK,
                    &serde_json::json!({"repository_id": uuid::Uuid::from_bytes(route.repository.repository_id()).to_string(), "rules":rules, "next_after":next}),
                )
            }
            None => plain(StatusCode::NOT_FOUND, "Repository is unavailable"),
            Some(_) => unavailable(),
        },
        Err(error) => failed(error),
    }
}

pub(super) async fn update(
    State(state): State<Arc<RepositoryHttp>>,
    Path(name): Path<String>,
    request: Request<Body>,
) -> Response<Body> {
    let (route, actor) =
        match authorized_route(&state, &name, request.headers(), TokenScope::Admin).await {
            Ok(value) => value,
            Err(response) => return response,
        };
    let body = match tokio::time::timeout(
        Duration::from_secs(30),
        to_bytes(request.into_body(), 16 * 1024),
    )
    .await
    {
        Ok(Ok(body)) => body,
        Ok(Err(_)) => {
            return plain(
                StatusCode::PAYLOAD_TOO_LARGE,
                "Branch rule request is too large",
            );
        }
        Err(_) => return plain(StatusCode::REQUEST_TIMEOUT, "Branch rule request timed out"),
    };
    let Ok(input) = serde_json::from_slice::<Update>(&body) else {
        return plain(
            StatusCode::UNPROCESSABLE_ENTITY,
            "Invalid branch rule request",
        );
    };
    if !valid_edit(&input.rule) {
        return plain(StatusCode::UNPROCESSABLE_ENTITY, "Invalid branch rule");
    }
    let Ok(id) = uuid::Uuid::parse_str(&input.repository_id) else {
        return plain(StatusCode::UNPROCESSABLE_ENTITY, "Invalid repository UUID");
    };
    if id.to_string() != input.repository_id || validate_repository_id(id.into_bytes()).is_err() {
        return plain(StatusCode::UNPROCESSABLE_ENTITY, "Invalid repository UUID");
    }
    if id.as_bytes() != &route.repository.repository_id() {
        return plain(StatusCode::CONFLICT, "Repository identity changed");
    }
    let identity = match mutation_identity() {
        Ok(value) => value,
        Err(error) => return failed(error),
    };
    match route
        .repository
        .set_branch_rule(identity, &actor.account, input.rule)
        .await
    {
        Ok(result) if result.output && (state.manager.ready)() => plain(StatusCode::NO_CONTENT, ""),
        Err(InvocationError::Rejected(_)) => plain(
            StatusCode::CONFLICT,
            "Branch rule version changed or required context is unavailable",
        ),
        Ok(_) => unavailable(),
        Err(error) => failed(error),
    }
}
fn unavailable() -> Response<Body> {
    plain(
        StatusCode::SERVICE_UNAVAILABLE,
        "Branch rules unavailable; read current state before retrying",
    )
}
fn failed(error: impl std::fmt::Display) -> Response<Body> {
    tracing::error!(error = %error, "branch rule operation failed");
    unavailable()
}
