use super::*;
use crate::lfs::locks::LockQuery;

#[derive(Default, Deserialize)]
struct LockBody {
    path: Option<String>,
    #[serde(default)]
    force: bool,
    cursor: Option<String>,
    limit: Option<u32>,
}

pub(super) async fn handle(
    State(api): State<Arc<GitHttpApi>>,
    request: Request<Body>,
) -> Response<Body> {
    if !(api.ready)() {
        return lfs_json(
            StatusCode::SERVICE_UNAVAILABLE,
            json!({"message":"Canopy node is not ready"}),
        );
    }
    let Some(viewer) = request.extensions().get::<Viewer>().cloned() else {
        return lfs_unauthorized();
    };
    let listing = request.method() == axum::http::Method::GET;
    let required = if listing {
        TokenScope::Read
    } else {
        TokenScope::Write
    };
    if let Err(status) = api.permission(&viewer, required).await {
        return if status == StatusCode::UNAUTHORIZED {
            lfs_unauthorized()
        } else {
            lfs_json(status, json!({"message":"LFS lock access denied"}))
        };
    }
    let path = request.uri().path().to_owned();
    let suffix = path.strip_prefix(&api.repository_path).unwrap_or("");
    let result = async {
        let service = api.gateway.lfs();
        if listing {
            let axum::extract::Query(query) =
                axum::extract::Query::<LockQuery>::try_from_uri(request.uri())
                    .map_err(|_| LfsError::InvalidLock("invalid query"))?;
            let page = service.locks(viewer.identity(), query, required).await?;
            let mut body = json!({"locks":page.locks});
            if let Some(cursor) = page.next_cursor {
                body["next_cursor"] = json!(cursor);
            }
            return Ok((StatusCode::OK, body));
        }
        let actor = &viewer.principal().ok_or(LfsError::Forbidden)?.account;
        let bytes =
            lfs_body(request.into_body(), 32 * 1024)
                .await
                .map_err(|status| match status {
                    StatusCode::REQUEST_TIMEOUT => LfsError::Timeout,
                    _ => LfsError::InvalidLock("request body exceeds 32 KiB"),
                })?;
        let body: LockBody = if bytes.is_empty() {
            LockBody::default()
        } else {
            serde_json::from_slice(&bytes)
                .map_err(|_| LfsError::InvalidLock("invalid JSON body"))?
        };
        // Repository ACLs apply to every ref. The optional LFS ref hint does
        // not partition locks: a path remains exclusive across all branches.
        if suffix == "/info/lfs/locks/verify" {
            let query = LockQuery {
                cursor: body.cursor,
                limit: body.limit,
                ..Default::default()
            };
            let page = service.locks(viewer.identity(), query, required).await?;
            let (ours, theirs): (Vec<_>, Vec<_>) = page
                .locks
                .into_iter()
                .partition(|lock| lock.owner.name == *actor);
            let mut body = json!({"ours":ours,"theirs":theirs});
            if let Some(cursor) = page.next_cursor {
                body["next_cursor"] = json!(cursor);
            }
            return Ok((StatusCode::OK, body));
        }
        if let Some(id) = suffix
            .strip_prefix("/info/lfs/locks/")
            .and_then(|s| s.strip_suffix("/unlock"))
        {
            let lock = service.unlock(actor, id, body.force).await?;
            return Ok((StatusCode::OK, json!({"lock":lock})));
        }
        let path = body.path.ok_or(LfsError::InvalidLock("path is required"))?;
        let (created, lock) = service.create_lock(actor, &path).await?;
        Ok(if created {
            (StatusCode::CREATED, json!({"lock":lock}))
        } else {
            (
                StatusCode::CONFLICT,
                json!({"lock":lock,"message":"Path is already locked"}),
            )
        })
    }
    .await;
    match result {
        Ok((status, body)) => lfs_json(status, body),
        Err(error) => {
            let (status, message) = match error {
                LfsError::Forbidden => (StatusCode::FORBIDDEN, "LFS lock access denied"),
                LfsError::NotFound => (StatusCode::NOT_FOUND, "LFS lock does not exist"),
                LfsError::InvalidLock(message) => (StatusCode::UNPROCESSABLE_ENTITY, message),
                LfsError::Timeout => (StatusCode::REQUEST_TIMEOUT, "LFS lock request timed out"),
                _ => {
                    tracing::error!(error = %error, "LFS lock operation failed");
                    (
                        StatusCode::SERVICE_UNAVAILABLE,
                        "LFS lock operation unavailable",
                    )
                }
            };
            lfs_json(status, json!({"message":message}))
        }
    }
}
