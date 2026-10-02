use std::{collections::VecDeque, convert::Infallible, future::poll_fn};

use super::*;

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

#[test]
fn fenced_cleanup_requires_the_exact_released_published_activation() {
    use cellule_runtime::{
        Digest, SessionId,
        control::{Owner, RootRef},
        identity::IncarnationId,
    };

    let owner = Owner {
        session: SessionId::from_bytes([3; 16]),
        endpoint: "https://owned.test".into(),
    };
    let mut control = Control::initial(
        CellId::from_bytes([1; 32]),
        IncarnationId::from_bytes([2; 16]),
        owner.clone(),
        Digest::from_bytes([4; 32]),
        1,
    )
    .unwrap();
    let fence = control.owner_fence();
    control.state = ControlState::Idle;
    control.owner = None;
    control.root = Some(RootRef {
        digest: Digest::from_bytes([5; 32]),
        txid: 1,
        checksum: 1,
        commit_sequence: 1,
    });
    assert!(released_local_fence(&control, fence));
    for state in [
        ControlState::Recovering,
        ControlState::Serving,
        ControlState::Tombstoned,
    ] {
        let mut unsettled = control.clone();
        unsettled.state = state;
        assert!(!released_local_fence(&unsettled, fence));
    }
    let mut changed = control.clone();
    changed.owner = Some(owner);
    assert!(!released_local_fence(&changed, fence));
    let mut changed = control.clone();
    changed.root = None;
    assert!(!released_local_fence(&changed, fence));
    let mut changed = control.clone();
    changed.epoch += 1;
    assert!(!released_local_fence(&changed, fence));
    let mut changed = control.clone();
    changed.incarnation = IncarnationId::from_bytes([6; 16]);
    assert!(!released_local_fence(&changed, fence));
}
