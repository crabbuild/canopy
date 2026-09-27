use std::{collections::VecDeque, convert::Infallible, future::poll_fn};

use super::*;

#[tokio::test]
async fn account_quotas_preserve_global_capacity_and_recover_after_release() {
    let admission = ActivationAdmission::new();
    let mut permits = Vec::new();
    for actor in [ReadIdentity::Anonymous, ReadIdentity::Account("anonymous")] {
        for _ in 0..16 {
            permits.push(admission.acquire(actor).await.unwrap());
        }
        assert!(admission.acquire(actor).await.is_err());
    }
    assert!(
        admission
            .acquire(ReadIdentity::Account("another"))
            .await
            .is_err()
    );
    drop(permits.pop());
    let another = admission
        .acquire(ReadIdentity::Account("another"))
        .await
        .unwrap();
    assert_eq!(admission.total.available_permits(), 0);
    drop(another);
    drop(permits);
    assert_eq!(admission.total.available_permits(), 32);
}

#[tokio::test]
async fn finished_accounts_do_not_accumulate_activation_state() {
    let admission = ActivationAdmission::new();
    for index in 0..1024 {
        let account = format!("account-{index}");
        drop(
            admission
                .acquire(ReadIdentity::Account(&account))
                .await
                .unwrap(),
        );
    }
    assert_eq!(admission.accounts.lock().await.len(), 1);
}

struct Frames(VecDeque<Frame<Bytes>>);

impl HttpBody for Frames {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Infallible>>> {
        Poll::Ready(self.0.pop_front().map(Ok))
    }
}

#[tokio::test]
async fn a_streamed_response_pins_residency_through_data_and_trailers() {
    let pin = Arc::new(());
    let mut trailers = axum::http::HeaderMap::new();
    trailers.insert("x-test", "complete".parse().unwrap());
    let mut body = PinnedBody {
        body: Body::new(Frames(VecDeque::from([
            Frame::data(Bytes::from_static(b"payload")),
            Frame::trailers(trailers),
        ]))),
        pin: Some(Arc::clone(&pin)),
    };
    let frame = poll_fn(|cx| Pin::new(&mut body).poll_frame(cx))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(frame.into_data().unwrap(), b"payload"[..]);
    assert_eq!(Arc::strong_count(&pin), 2);
    let frame = poll_fn(|cx| Pin::new(&mut body).poll_frame(cx))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(frame.into_trailers().unwrap()["x-test"], "complete");
    assert_eq!(Arc::strong_count(&pin), 2);
    assert!(
        poll_fn(|cx| Pin::new(&mut body).poll_frame(cx))
            .await
            .is_none()
    );
    assert_eq!(Arc::strong_count(&pin), 1);
}

#[test]
fn disconnect_releases_a_response_pin_without_polling_the_body() {
    let pin = Arc::new(());
    let response = Body::new(PinnedBody {
        body: Body::from("unread"),
        pin: Some(Arc::clone(&pin)),
    });
    assert_eq!(Arc::strong_count(&pin), 2);
    drop(response);
    assert_eq!(Arc::strong_count(&pin), 1);
}
