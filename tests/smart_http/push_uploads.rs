use super::*;
use canopy_server::git_http::GitHttpRequest;
use futures_core::Stream;
use std::{
    future::Future,
    io,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

struct PausedUpload {
    entered: Option<oneshot::Sender<()>>,
    release: oneshot::Receiver<()>,
}

impl Stream for PausedUpload {
    type Item = Result<axum::body::Bytes, io::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(entered) = self.entered.take() {
            let _ = entered.send(());
        }
        match Pin::new(&mut self.release).poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(_) => Poll::Ready(None),
        }
    }
}

fn push(body: Body) -> GitHttpRequest<Body> {
    GitHttpRequest {
        method: "POST".into(),
        path_info: "/repo.git/git-receive-pack".into(),
        query: String::new(),
        content_type: Some("application/x-git-receive-pack-request".into()),
        gzip: false,
        protocol_v2: false,
        authenticated: true,
        body,
    }
}

pub async fn verify(gateway: &Arc<GitGateway>) -> Result<(), Box<dyn std::error::Error>> {
    let (entered_tx, entered_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let first_gateway = Arc::clone(gateway);
    let stalled = tokio::spawn(async move {
        first_gateway
            .handle(
                push(Body::from_stream(PausedUpload {
                    entered: Some(entered_tx),
                    release: release_rx,
                })),
                "canopy",
                None,
                None,
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), entered_rx).await??;
    let second = tokio::time::timeout(
        Duration::from_secs(5),
        gateway.handle(push(Body::from("invalid")), "canopy", None, None),
    )
    .await;
    let _ = release_tx.send(());
    assert!(stalled.await?.is_err());
    assert!(second?.is_err());
    Ok(())
}
