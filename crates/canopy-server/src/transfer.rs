//! Transfer admission retained by response bodies and their outstanding data frames.

use crate::AdmissionPermit;
use axum::body::{Body, Bytes, HttpBody};
use http_body::{Frame, SizeHint};
use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

pub(crate) fn response_body(body: Body, permit: Arc<AdmissionPermit>) -> Body {
    Body::new(TransferBody {
        body,
        permit: Some(permit),
    })
}

struct TransferBody {
    body: Body,
    permit: Option<Arc<AdmissionPermit>>,
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
    _permit: Arc<AdmissionPermit>,
}

impl AsRef<[u8]> for TransferBytes {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ReadIdentity, admission::AccountAdmission};
    use std::{collections::VecDeque, future::poll_fn};

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

    async fn admitted(body: Body) -> (Body, AccountAdmission) {
        let semaphore = AccountAdmission::new(2, "total", "account");
        let permit = Arc::new(semaphore.acquire(ReadIdentity::Anonymous).await.unwrap());
        (response_body(body, permit), semaphore)
    }

    #[tokio::test]
    async fn admission_covers_trailers_and_outstanding_frame_clones_after_eof() {
        let mut trailers = axum::http::HeaderMap::new();
        trailers.insert("x-test", axum::http::HeaderValue::from_static("complete"));
        let (mut body, semaphore) = admitted(Body::new(Frames(VecDeque::from([
            Ok(Frame::data(Bytes::from_static(b"payload"))),
            Ok(Frame::trailers(trailers)),
        ]))))
        .await;
        let bytes = poll_fn(|cx| Pin::new(&mut body).poll_frame(cx))
            .await
            .unwrap()
            .unwrap()
            .into_data()
            .unwrap();
        let copied = bytes.clone();
        assert_eq!(bytes, b"payload"[..]);
        assert!(semaphore.acquire(ReadIdentity::Anonymous).await.is_err());
        let trailers = poll_fn(|cx| Pin::new(&mut body).poll_frame(cx))
            .await
            .unwrap()
            .unwrap()
            .into_trailers()
            .unwrap();
        assert_eq!(trailers["x-test"], "complete");
        assert!(semaphore.acquire(ReadIdentity::Anonymous).await.is_err());
        assert!(
            poll_fn(|cx| Pin::new(&mut body).poll_frame(cx))
                .await
                .is_none()
        );
        assert!(body.is_end_stream());
        drop(body);
        drop(bytes);
        assert!(semaphore.acquire(ReadIdentity::Anonymous).await.is_err());
        drop(copied);
        assert!(semaphore.acquire(ReadIdentity::Anonymous).await.is_ok());
    }

    #[tokio::test]
    async fn body_errors_release_admission() {
        let (mut body, semaphore) = admitted(Body::new(Frames(VecDeque::from([Err(
            std::io::Error::other("interrupted"),
        )]))))
        .await;
        assert!(
            poll_fn(|cx| Pin::new(&mut body).poll_frame(cx))
                .await
                .unwrap()
                .is_err()
        );
        assert!(semaphore.acquire(ReadIdentity::Anonymous).await.is_ok());
    }

    #[tokio::test]
    async fn dropping_an_unpolled_body_releases_admission_and_preserves_size_hint() {
        let (body, semaphore) = admitted(Body::from("payload")).await;
        assert_eq!(body.size_hint().exact(), Some(7));
        assert!(semaphore.acquire(ReadIdentity::Anonymous).await.is_err());
        drop(body);
        assert!(semaphore.acquire(ReadIdentity::Anonymous).await.is_ok());
    }
}
