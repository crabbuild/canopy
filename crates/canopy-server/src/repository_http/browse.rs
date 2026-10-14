use super::*;
use crate::git_read::{ReadError, Reader};
use std::time::Duration;

// A legal 65,535-byte ref cursor can expand sixfold in JSON escapes. Keep
// requests bounded while allowing clients to continue every supported ref page.
const REQUEST_BYTES: usize = 512 * 1024;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Input {
    repository_id: String,
    query: View,
}
#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase", deny_unknown_fields)]
enum View {
    Refs {
        after: Option<String>,
        generation: Option<i64>,
    },
    Resolve {
        reference: Option<String>,
    },
    Tree {
        commit: String,
        path_base64: String,
        after: Option<String>,
    },
    File {
        commit: String,
        path_base64: String,
    },
    History {
        commit: String,
    },
}

pub(super) async fn browse(
    State(state): State<Arc<RepositoryHttp>>,
    Path(name): Path<String>,
    request: Request<Body>,
) -> Response<Body> {
    let actor = match state.viewer(request.headers()).await {
        Ok(actor) => actor,
        Err(response) => return response,
    };
    let permit = match state.transfer_permit(actor.identity()).await {
        Ok(permit) => permit,
        Err(response) => return response,
    };
    let work = Arc::clone(&state);
    let task = state.tasks.spawn(async move {
        let response = serve(&work, &name, request, actor, Arc::clone(&permit)).await;
        response.map(|body| crate::transfer::response_body(body, permit))
    });
    match task.await {
        Ok(response) => response,
        Err(error) => {
            tracing::error!(error=%error,"browse task failed");
            plain(
                StatusCode::SERVICE_UNAVAILABLE,
                "Repository browser is unavailable",
            )
        }
    }
}
async fn serve(
    state: &RepositoryHttp,
    name: &str,
    request: Request<Body>,
    actor: Viewer,
    permit: Arc<AdmissionPermit>,
) -> Response<Body> {
    let route =
        match authorization::scoped_route(state, name, actor.identity(), TokenScope::Read).await {
            Ok(route) => route,
            Err(response) => return response,
        };
    let body = match tokio::time::timeout(
        Duration::from_secs(30),
        to_bytes(request.into_body(), REQUEST_BYTES),
    )
    .await
    {
        Ok(Ok(body)) => body,
        Ok(Err(_)) => return plain(StatusCode::PAYLOAD_TOO_LARGE, "Browse request is too large"),
        Err(_) => return plain(StatusCode::REQUEST_TIMEOUT, "Browse request timed out"),
    };
    let Ok(input) = serde_json::from_slice::<Input>(&body) else {
        return plain(StatusCode::UNPROCESSABLE_ENTITY, "Invalid browse request");
    };
    if let Err(response) = pulls::identity(&route, &input.repository_id) {
        return *response;
    }
    let reader = Reader::new(Arc::clone(&route.repository), permit);
    let operation = async {
        let view = match input.query {
            View::Refs { after, generation } => {
                serde_json::json!({"refs":reader.browser_refs(actor.identity(),after.as_deref().unwrap_or(""),generation).await?})
            }
            View::Resolve { reference } => {
                serde_json::json!({"resolved":reader.browser_ref(actor.identity(),reference.as_deref()).await?})
            }
            View::Tree {
                commit,
                path_base64,
                after,
            } => {
                serde_json::json!({"tree":reader.browser_tree(actor.identity(),&commit,&path_base64,after.as_deref()).await?})
            }
            View::File {
                commit,
                path_base64,
            } => {
                serde_json::json!({"file":reader.browser_file(actor.identity(),&commit,&path_base64).await?})
            }
            View::History { commit } => {
                serde_json::json!({"history":reader.browser_history(actor.identity(),&commit).await?})
            }
        };
        Ok::<_, ReadError>(json_response(
            StatusCode::OK,
            &serde_json::json!({"repository_id":input.repository_id,"view":view}),
        ))
    };
    match tokio::time::timeout(Duration::from_secs(120), operation).await {
        Ok(Ok(response)) if (state.manager.ready)() => response,
        Ok(Ok(_)) => plain(StatusCode::SERVICE_UNAVAILABLE, "Canopy node is not ready"),
        Ok(Err(error)) => failed(error),
        Err(_) => plain(
            StatusCode::GATEWAY_TIMEOUT,
            "Repository view exceeded its time limit",
        ),
    }
}
fn failed(error: ReadError) -> Response<Body> {
    let (status, message) = match error {
        ReadError::Missing => (
            StatusCode::NOT_FOUND,
            "Repository view is unavailable or does not point to a commit",
        ),
        ReadError::Changed => (
            StatusCode::CONFLICT,
            "Repository refs changed; reload the branch list",
        ),
        ReadError::TooLarge => (
            StatusCode::PAYLOAD_TOO_LARGE,
            "Repository view exceeds its traversal or output limit",
        ),
        ReadError::Invalid => (StatusCode::UNPROCESSABLE_ENTITY, "Invalid repository view"),
        error => {
            tracing::error!(error=%error,"repository browse failed");
            (
                StatusCode::SERVICE_UNAVAILABLE,
                "Repository browser is unavailable",
            )
        }
    };
    plain(status, message)
}
