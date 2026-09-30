use super::*;
use crate::COLLABORATOR_PAGE_SIZE;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Cursor {
    after: Option<String>,
}

pub(super) async fn list(
    State(state): State<Arc<RepositoryHttp>>,
    Path(name): Path<String>,
    Query(cursor): Query<Cursor>,
    headers: axum::http::HeaderMap,
) -> Response<Body> {
    let (route, principal) =
        match authorized_route(&state, &name, &headers, TokenScope::Admin).await {
            Ok(authorized) => authorized,
            Err(response) => return response,
        };
    if cursor
        .after
        .as_deref()
        .is_some_and(|after| directory::validate_component(after).is_err())
    {
        return plain(
            StatusCode::UNPROCESSABLE_ENTITY,
            "Invalid collaborator cursor",
        );
    }
    match route
        .repository
        .collaborators(&principal.account, cursor.after.as_deref())
        .await
    {
        Ok(page) if (state.manager.ready)() => {
            let Some(members) = page.output else {
                return plain(StatusCode::FORBIDDEN, "Repository owner required");
            };
            let next = if members.len() == COLLABORATOR_PAGE_SIZE {
                members.last().map(|member| member.account.clone())
            } else {
                None
            };
            let members: Vec<_> = members.iter().map(|member| serde_json::json!({"account": member.account, "role": member.role.as_str()})).collect();
            json_response(
                StatusCode::OK,
                &serde_json::json!({
                    "repository_id": uuid::Uuid::from_bytes(route.repository.repository_id()).to_string(),
                    "owner": principal.account,
                    "collaborators": members,
                    "next_after": next,
                }),
            )
        }
        Ok(_) => plain(StatusCode::SERVICE_UNAVAILABLE, "Canopy node is not ready"),
        Err(error) => {
            tracing::error!(error = %error, "collaborator listing failed");
            plain(
                StatusCode::SERVICE_UNAVAILABLE,
                "Collaborator listing unavailable",
            )
        }
    }
}
