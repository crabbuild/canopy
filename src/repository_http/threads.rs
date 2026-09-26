use super::*;
use crate::{
    git_read::{ComparisonTarget, Reader, Side},
    issues::{NewComment, valid_body},
    pulls::{
        PullChange,
        threads::{PAGE, ThreadIntent},
    },
};
use std::time::Duration;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Page {
    #[serde(default)]
    after: i64,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Create {
    repository_id: String,
    id: String,
    target: ComparisonTarget,
    path_base64: String,
    side: Side,
    line: i64,
    body: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Reply {
    repository_id: String,
    id: String,
    body: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Resolve {
    repository_id: String,
    expected_version: i64,
    resolved: bool,
}

pub(super) async fn list(
    State(state): State<Arc<RepositoryHttp>>,
    Path((name, number)): Path<(String, i64)>,
    Query(page): Query<Page>,
    headers: axum::http::HeaderMap,
) -> Response<Body> {
    let (route, actor) = match authorized_route(&state, &name, &headers, TokenScope::Read).await {
        Ok(value) => value,
        Err(response) => return response,
    };
    if number < 1 || page.after < 0 {
        return invalid();
    }
    match route
        .repository
        .threads(&actor.account, number, page.after)
        .await
    {
        Ok(Some(threads)) if (state.manager.ready)() => {
            let next = (threads.len() == PAGE)
                .then(|| threads.last().map(|thread| thread.number))
                .flatten();
            json_response(
                StatusCode::OK,
                &serde_json::json!({"repository_id":uuid::Uuid::from_bytes(route.repository.repository_id()).to_string(),"threads":threads,"next_after":next}),
            )
        }
        Ok(None) => pulls::missing(),
        Ok(Some(_)) => pulls::unavailable(),
        Err(error) => pulls::failed(error),
    }
}
pub(super) async fn read(
    State(state): State<Arc<RepositoryHttp>>,
    Path((name, number, thread)): Path<(String, i64, i64)>,
    headers: axum::http::HeaderMap,
) -> Response<Body> {
    let (route, actor) = match authorized_route(&state, &name, &headers, TokenScope::Read).await {
        Ok(value) => value,
        Err(response) => return response,
    };
    if number < 1 || thread < 1 {
        return invalid();
    }
    match route
        .repository
        .thread(&actor.account, number, thread)
        .await
    {
        Ok(Some(thread)) if (state.manager.ready)() => json_response(
            StatusCode::OK,
            &serde_json::json!({"repository_id":uuid::Uuid::from_bytes(route.repository.repository_id()).to_string(),"thread":thread}),
        ),
        Ok(None) => pulls::missing(),
        Ok(Some(_)) => pulls::unavailable(),
        Err(error) => pulls::failed(error),
    }
}
pub(super) async fn comments(
    State(state): State<Arc<RepositoryHttp>>,
    Path((name, number, thread)): Path<(String, i64, i64)>,
    Query(page): Query<Page>,
    headers: axum::http::HeaderMap,
) -> Response<Body> {
    let (route, actor) = match authorized_route(&state, &name, &headers, TokenScope::Read).await {
        Ok(value) => value,
        Err(response) => return response,
    };
    if number < 1 || thread < 1 || page.after < 0 {
        return invalid();
    }
    match route
        .repository
        .thread_comments(&actor.account, number, thread, page.after)
        .await
    {
        Ok(Some(comments)) if (state.manager.ready)() => {
            let next = (comments.len() == PAGE)
                .then(|| comments.last().map(|comment| comment.number))
                .flatten();
            json_response(
                StatusCode::OK,
                &serde_json::json!({"repository_id":uuid::Uuid::from_bytes(route.repository.repository_id()).to_string(),"comments":comments,"next_after":next}),
            )
        }
        Ok(None) => pulls::missing(),
        Ok(Some(_)) => pulls::unavailable(),
        Err(error) => pulls::failed(error),
    }
}
pub(super) async fn create(
    State(state): State<Arc<RepositoryHttp>>,
    Path((name, number)): Path<(String, i64)>,
    request: Request<Body>,
) -> Response<Body> {
    // Anchor verification traverses Git objects. Share its admission and detached
    // lifetime with comparisons, including blocking diff work after cancellation.
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
        let response = create_inner(&work_state, &name, number, request, Arc::clone(&permit)).await;
        response.map(|body| crate::transfer::response_body(body, permit))
    });
    match task.await {
        Ok(response) => response,
        Err(error) => pulls::failed(error),
    }
}
async fn create_inner(
    state: &RepositoryHttp,
    name: &str,
    number: i64,
    request: Request<Body>,
    permit: Arc<OwnedSemaphorePermit>,
) -> Response<Body> {
    let (route, actor) = match pulls::writable_route(state, name, request.headers()).await {
        Ok(value) => value,
        Err(response) => return response,
    };
    let input: Create = match pulls::input(request).await {
        Ok(value) => value,
        Err(response) => return response,
    };
    let Some(id) = pulls::canonical_id(&input.id) else {
        return invalid();
    };
    if number < 1
        || !(1..=20_000).contains(&input.line)
        || !valid_body(&input.body)
        || input.body.trim().is_empty()
        || matches!(input.target, ComparisonTarget::Thread { .. })
    {
        return invalid();
    }
    let identity = match pulls::identity(&route, &input.repository_id) {
        Ok(value) => value,
        Err(response) => return *response,
    };
    let intent = ThreadIntent {
        id,
        target: input.target,
        path_base64: input.path_base64,
        side: input.side,
        line: input.line,
        body: input.body,
    };
    match route
        .repository
        .thread_retry(&actor.account, number, &intent)
        .await
    {
        Ok(Some(result)) => return changed(state, Ok(result), true),
        Ok(None) => (),
        Err(error) => return pulls::failed(error),
    }
    let reader = Reader::new(Arc::clone(&route.repository), permit);
    let verified = tokio::time::timeout(
        Duration::from_secs(120),
        reader.patch(
            &actor.account,
            number,
            intent.target.clone(),
            &intent.path_base64,
        ),
    )
    .await;
    let anchor = match verified {
        Ok(Ok(patch)) => match patch.anchor(intent.side, intent.line) {
            Ok(anchor) => anchor,
            Err(error) => return comparison::failed(error),
        },
        Ok(Err(error)) => return comparison::failed(error),
        Err(_) => {
            return plain(
                StatusCode::GATEWAY_TIMEOUT,
                "Line anchor verification exceeded its time limit",
            );
        }
    };
    changed(
        state,
        route
            .repository
            .create_thread(identity, &actor.account, number, &intent, anchor)
            .await
            .map(|result| result.output),
        true,
    )
}
fn changed(
    state: &RepositoryHttp,
    result: Result<
        PullChange,
        cellule_runtime::InvocationError<Vec<cellule_runtime::SqlResultSet>>,
    >,
    created: bool,
) -> Response<Body> {
    match result {
        Ok(PullChange::Applied(number)) if (state.manager.ready)() => {
            if created {
                json_response(StatusCode::OK, &serde_json::json!({"number":number}))
            } else {
                plain(StatusCode::NO_CONTENT, "")
            }
        }
        Ok(PullChange::Applied(_)) => pulls::unavailable(),
        Ok(PullChange::NotFound) => plain(StatusCode::NOT_FOUND, "Line discussion is unavailable"),
        Ok(PullChange::Conflict) => plain(
            StatusCode::CONFLICT,
            "Discussion identity, version or selected revision changed",
        ),
        Ok(PullChange::Forbidden) => plain(
            StatusCode::FORBIDDEN,
            "Discussion author, pull author or repository writer required",
        ),
        Err(error) => pulls::failed(error),
    }
}
pub(super) async fn reply(
    State(state): State<Arc<RepositoryHttp>>,
    Path((name, number, thread)): Path<(String, i64, i64)>,
    request: Request<Body>,
) -> Response<Body> {
    let (route, actor) = match pulls::writable_route(&state, &name, request.headers()).await {
        Ok(value) => value,
        Err(response) => return response,
    };
    let input: Reply = match pulls::input(request).await {
        Ok(value) => value,
        Err(response) => return response,
    };
    let Some(id) = pulls::canonical_id(&input.id) else {
        return invalid();
    };
    if number < 1 || thread < 1 || !valid_body(&input.body) || input.body.trim().is_empty() {
        return invalid();
    }
    let identity = match pulls::identity(&route, &input.repository_id) {
        Ok(value) => value,
        Err(response) => return *response,
    };
    changed(
        &state,
        route
            .repository
            .reply_thread(
                identity,
                &actor.account,
                number,
                thread,
                NewComment {
                    id,
                    body: &input.body,
                },
            )
            .await
            .map(|result| result.output),
        true,
    )
}
pub(super) async fn resolve(
    State(state): State<Arc<RepositoryHttp>>,
    Path((name, number, thread)): Path<(String, i64, i64)>,
    request: Request<Body>,
) -> Response<Body> {
    let (route, actor) = match pulls::writable_route(&state, &name, request.headers()).await {
        Ok(value) => value,
        Err(response) => return response,
    };
    let input: Resolve = match pulls::input(request).await {
        Ok(value) => value,
        Err(response) => return response,
    };
    if number < 1 || thread < 1 || !(1..i64::MAX).contains(&input.expected_version) {
        return invalid();
    }
    let identity = match pulls::identity(&route, &input.repository_id) {
        Ok(value) => value,
        Err(response) => return *response,
    };
    changed(
        &state,
        route
            .repository
            .resolve_thread(
                identity,
                &actor.account,
                number,
                thread,
                input.expected_version,
                input.resolved,
            )
            .await
            .map(|result| result.output),
        false,
    )
}
fn invalid() -> Response<Body> {
    plain(
        StatusCode::UNPROCESSABLE_ENTITY,
        "Invalid line discussion input",
    )
}
