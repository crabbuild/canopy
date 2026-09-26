//! Embedded repository interface; authentication stays in the JSON API.

use axum::{
    body::Body,
    http::{Response, header},
};

fn asset(body: &'static str, content_type: &'static str) -> Response<Body> {
    let mut response = Response::new(Body::from(body));
    let headers = response.headers_mut();
    headers.insert(
        header::CONTENT_TYPE,
        header::HeaderValue::from_static(content_type),
    );
    headers.insert(
        header::CACHE_CONTROL,
        header::HeaderValue::from_static("no-store"),
    );
    headers.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        header::HeaderValue::from_static("nosniff"),
    );
    headers.insert(
        header::REFERRER_POLICY,
        header::HeaderValue::from_static("no-referrer"),
    );
    headers.insert(header::CONTENT_SECURITY_POLICY,header::HeaderValue::from_static("default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src 'self' data:; base-uri 'none'; frame-ancestors 'none'; form-action 'self'"));
    response
}
pub(crate) async fn index() -> Response<Body> {
    asset(
        include_str!("../web/index.html"),
        "text/html; charset=utf-8",
    )
}
pub(crate) async fn script() -> Response<Body> {
    asset(
        include_str!("../web/canopy.js"),
        "text/javascript; charset=utf-8",
    )
}
pub(crate) async fn styles() -> Response<Body> {
    asset(include_str!("../web/canopy.css"), "text/css; charset=utf-8")
}

pub(crate) async fn issues() -> Response<Body> {
    asset(
        include_str!("../web/issues.js"),
        "text/javascript; charset=utf-8",
    )
}

pub(crate) async fn issue_styles() -> Response<Body> {
    asset(include_str!("../web/issues.css"), "text/css; charset=utf-8")
}
