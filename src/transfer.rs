//! Transfer admission retained by response bodies and their outstanding data frames.

use axum::body::{Body, Bytes, HttpBody};
use http_body::{Frame, SizeHint};
use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tokio::sync::OwnedSemaphorePermit;

pub(crate) fn response_body(body: Body, permit: Arc<OwnedSemaphorePermit>) -> Body {
    Body::new(TransferBody {
        body,
        permit: Some(permit),
    })
}

struct TransferBody {
    body: Body,
    permit: Option<Arc<OwnedSemaphorePermit>>,
}

impl HttpBody for TransferBody {
    type Data = Bytes;
    type Error = axum::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
        let Some(permit) = self.permit.clone() else {
            return Poll::Ready(None);
        };
        let result = Pin::new(&mut self.body).poll_frame(cx);
        if matches!(&result, Poll::Ready(None | Some(Err(_)))) || self.body.is_end_stream() {
            self.permit = None;
        }
        result.map(|frame| {
            frame.map(|result| {
                result.map(|frame| {
                    frame.map_data(|bytes| {
                        // Hyper may retain this frame after the body reports EOF. Binding
                        // admission to Bytes also covers socket backpressure and clones.
                        Bytes::from_owner(TransferBytes {
                            bytes,
                            _permit: permit,
                        })
                    })
                })
            })
        })
    }

    fn is_end_stream(&self) -> bool {
        self.permit.is_none() || self.body.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.body.size_hint()
    }
}

struct TransferBytes {
    bytes: Bytes,
    _permit: Arc<OwnedSemaphorePermit>,
}

impl AsRef<[u8]> for TransferBytes {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{collections::VecDeque, future::poll_fn};
    use tokio::sync::Semaphore;

    struct Frames(VecDeque<Result<Frame<Bytes>, std::io::Error>>);
    impl HttpBody for Frames {
        type Data = Bytes;
        type Error = std::io::Error;
        fn poll_frame(
            mut self: Pin<&mut Self>,
            _: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
            Poll::Ready(self.0.pop_front())
        }
    }

    fn admitted(body: Body) -> (Body, Arc<Semaphore>) {
        let semaphore = Arc::new(Semaphore::new(1));
        let permit = Arc::new(Arc::clone(&semaphore).try_acquire_owned().unwrap());
        (response_body(body, permit), semaphore)
    }

    #[tokio::test]
    async fn admission_covers_trailers_and_outstanding_frame_clones_after_eof() {
        let mut trailers = axum::http::HeaderMap::new();
        trailers.insert("x-test", axum::http::HeaderValue::from_static("complete"));
        let (mut body, semaphore) = admitted(Body::new(Frames(VecDeque::from([
            Ok(Frame::data(Bytes::from_static(b"payload"))),
            Ok(Frame::trailers(trailers)),
        ]))));
        let bytes = poll_fn(|cx| Pin::new(&mut body).poll_frame(cx))
            .await
            .unwrap()
            .unwrap()
            .into_data()
            .unwrap();
        let copied = bytes.clone();
        assert_eq!(bytes, b"payload"[..]);
        assert_eq!(semaphore.available_permits(), 0);
        let trailers = poll_fn(|cx| Pin::new(&mut body).poll_frame(cx))
            .await
            .unwrap()
            .unwrap()
            .into_trailers()
            .unwrap();
        assert_eq!(trailers["x-test"], "complete");
        assert_eq!(semaphore.available_permits(), 0);
        assert!(
            poll_fn(|cx| Pin::new(&mut body).poll_frame(cx))
                .await
                .is_none()
        );
        assert!(body.is_end_stream());
        drop(body);
        drop(bytes);
        assert_eq!(semaphore.available_permits(), 0);
        drop(copied);
        assert_eq!(semaphore.available_permits(), 1);
    }

    #[tokio::test]
    async fn body_errors_release_admission() {
        let (mut body, semaphore) = admitted(Body::new(Frames(VecDeque::from([Err(
            std::io::Error::other("interrupted"),
        )]))));
        assert!(
            poll_fn(|cx| Pin::new(&mut body).poll_frame(cx))
                .await
                .unwrap()
                .is_err()
        );
        assert_eq!(semaphore.available_permits(), 1);
    }

    #[test]
    fn dropping_an_unpolled_body_releases_admission_and_preserves_size_hint() {
        let (body, semaphore) = admitted(Body::from("payload"));
        assert_eq!(body.size_hint().exact(), Some(7));
        assert_eq!(semaphore.available_permits(), 0);
        drop(body);
        assert_eq!(semaphore.available_permits(), 1);
    }
}
