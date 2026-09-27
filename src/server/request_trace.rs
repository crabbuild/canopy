use std::time::Instant;

use axum::{
    extract::{MatchedPath, Request},
    middleware::Next,
    response::Response,
};
use tracing::Instrument;
use uuid::Uuid;

pub(super) async fn trace(request: Request, next: Next) -> Response {
    if !tracing::enabled!(tracing::Level::DEBUG) {
        return next.run(request).await;
    }
    let started = Instant::now();
    // Correlation is diagnostic only. Parse a bounded UUID rather than logging
    // arbitrary headers, which may contain credentials or attacker-controlled text.
    let request_id = request
        .headers()
        .get("x-request-id")
        .filter(|value| value.as_bytes().len() == 36)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| Uuid::parse_str(value).ok())
        .unwrap_or_else(Uuid::new_v4);
    let route = request
        .extensions()
        .get::<MatchedPath>()
        .map_or("unmatched", MatchedPath::as_str);
    let span = tracing::debug_span!("http_request", %request_id, route);
    async move {
        tracing::debug!("HTTP handler started");
        let response = next.run(request).await;
        tracing::debug!(
            elapsed_seconds = started.elapsed().as_secs_f64(),
            status = response.status().as_u16(),
            "HTTP handler completed"
        );
        response
    }
    .instrument(span)
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{Router, body::Body, http::StatusCode, routing::get};
    use tower::ServiceExt;

    #[tokio::test]
    async fn request_trace_correlates_nested_work_without_recording_private_input()
    -> Result<(), Box<dyn std::error::Error>> {
        let log = tempfile::NamedTempFile::new()?;
        let subscriber = tracing_subscriber::fmt()
            .with_writer(log.reopen()?)
            .with_max_level(tracing::Level::DEBUG)
            .with_ansi(false)
            .without_time()
            .finish();
        let _subscriber = tracing::subscriber::set_default(subscriber);
        let router = Router::new()
            .route(
                "/api/repositories/{name}",
                get(|| async {
                    tracing::debug!("nested repository query");
                    StatusCode::FORBIDDEN
                }),
            )
            .layer(axum::middleware::from_fn(trace));
        let request_id = Uuid::new_v4().to_string();
        for header in [&request_id, "private-invalid-correlation"] {
            let response = router
                .clone()
                .oneshot(
                    Request::builder()
                        .uri("/api/repositories/private-name?token=private-query")
                        .header("authorization", "Bearer private-credential")
                        .header("x-request-id", header)
                        .body(Body::empty())?,
                )
                .await?;
            assert_eq!(response.status(), StatusCode::FORBIDDEN);
        }
        let output = std::fs::read_to_string(log.path())?;
        let nested: Vec<_> = output
            .lines()
            .filter(|line| line.contains("nested repository query"))
            .collect();
        assert_eq!(nested.len(), 2);
        assert!(nested[0].contains(&request_id));
        assert!(!nested[1].contains(&request_id));
        assert!(output.contains("route=\"/api/repositories/{name}\""));
        assert!(output.contains("HTTP handler started"));
        assert!(output.contains("HTTP handler completed"));
        assert!(output.contains("status=403"));
        assert!(
            !output.contains("private-"),
            "request data leaked into tracing"
        );
        Ok(())
    }
}
