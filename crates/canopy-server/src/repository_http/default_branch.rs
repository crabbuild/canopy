use super::*;
use crate::{default_branch::valid_default_branch, server::mutation_identity};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct UpdateRequest {
    repository_id: String,
    reference: String,
    expected_generation: i64,
}

pub(super) async fn read(
    State(state): State<Arc<RepositoryHttp>>,
    Path(name): Path<String>,
    headers: axum::http::HeaderMap,
) -> Response<Body> {
    let (route, actor) = match readable_route(&state, &name, &headers).await {
        Ok(authorized) => authorized,
        Err(response) => return response,
    };
    let head = route
        .repository
        .default_branch_for(actor.identity(), None)
        .await;
    match head {
        Ok(_) if !(state.manager.ready)() => {
            plain(StatusCode::SERVICE_UNAVAILABLE, "Canopy node is not ready")
        }
        Ok(head) => match head.output {
            Some(head) => response(
                route.repository.repository_id(),
                &head.reference,
                head.generation,
            ),
            None => plain(StatusCode::NOT_FOUND, "Repository not found"),
        },
        Err(error) => {
            tracing::error!(error = %error, "default branch read failed");
            plain(
                StatusCode::SERVICE_UNAVAILABLE,
                "Default branch is unavailable",
            )
        }
    }
}

pub(super) async fn update(
    State(state): State<Arc<RepositoryHttp>>,
    Path(name): Path<String>,
    request: Request<Body>,
) -> Response<Body> {
    let (route, principal) =
        match authorized_route(&state, &name, request.headers(), TokenScope::Admin).await {
            Ok(authorized) => authorized,
            Err(response) => return response,
        };
    let Ok(body) = to_bytes(request.into_body(), 8192).await else {
        return plain(
            StatusCode::PAYLOAD_TOO_LARGE,
            "Default branch request is too large",
        );
    };
    let Ok(input) = serde_json::from_slice::<UpdateRequest>(&body) else {
        return plain(
            StatusCode::UNPROCESSABLE_ENTITY,
            "Invalid default branch request",
        );
    };
    if !valid_default_branch(&input.reference)
        || !(0..i64::MAX).contains(&input.expected_generation)
    {
        return plain(
            StatusCode::UNPROCESSABLE_ENTITY,
            "Invalid default branch update",
        );
    }
    let Ok(repository_id) = uuid::Uuid::parse_str(&input.repository_id) else {
        return plain(StatusCode::UNPROCESSABLE_ENTITY, "Invalid repository UUID");
    };
    if repository_id.as_bytes() != &route.repository.repository_id() {
        return plain(StatusCode::CONFLICT, "Repository identity changed");
    }
    let identity = match mutation_identity() {
        Ok(identity) => identity,
        Err(error) => {
            tracing::error!(error = %error, "default branch identity failed");
            return plain(
                StatusCode::SERVICE_UNAVAILABLE,
                "Default branch update failed",
            );
        }
    };
    match route
        .gateway
        .set_default_branch(
            identity,
            &principal.account,
            crate::packs::publication::HeadRequest {
                expected_generation: input.expected_generation,
                reference: input.reference.clone(),
            },
        )
        .await
    {
        Ok(crate::packs::publication::PublicationReply::Published(_))
            if (state.manager.ready)() =>
        {
            response(
                route.repository.repository_id(),
                &input.reference,
                input.expected_generation + 1,
            )
        }
        Ok(crate::packs::publication::PublicationReply::Denied(_)) => plain(
            StatusCode::CONFLICT,
            "Ref generation changed or target branch does not exist",
        ),
        Ok(_) => plain(StatusCode::SERVICE_UNAVAILABLE, "Canopy node is not ready"),
        Err(error) => {
            tracing::error!(error = %error, "default branch update failed");
            plain(
                StatusCode::SERVICE_UNAVAILABLE,
                "Default branch update failed; read current state before retrying",
            )
        }
    }
}

fn response(repository_id: [u8; 16], reference: &str, generation: i64) -> Response<Body> {
    json_response(
        StatusCode::OK,
        &serde_json::json!({
            "repository_id": uuid::Uuid::from_bytes(repository_id).to_string(),
            "reference": reference,
            "generation": generation,
        }),
    )
}
