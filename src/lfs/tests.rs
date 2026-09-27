use super::*;
use bytes::Bytes;
use futures_core::Stream;
use object_store::{ObjectStoreExt, memory::InMemory};
use sha2::{Digest, Sha256};
use std::{
    future::poll_fn,
    pin::Pin,
    task::{Context, Poll},
};

type TestResult = Result<(), Box<dyn std::error::Error>>;

struct DeclaredBody {
    size: u64,
    bytes: Option<Bytes>,
}

impl http_body::Body for DeclaredBody {
    type Data = Bytes;
    type Error = std::io::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Bytes>, Self::Error>>> {
        Poll::Ready(
            self.bytes
                .take()
                .map(|bytes| Ok(http_body::Frame::data(bytes))),
        )
    }

    fn size_hint(&self) -> http_body::SizeHint {
        http_body::SizeHint::with_exact(self.size)
    }
}

#[tokio::test]
async fn truncated_uploads_cannot_publish_bytes() -> TestResult {
    for size in [5 * 1024 * 1024 * 1024 + 1, 20] {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let body = Body::new(DeclaredBody {
            size,
            bytes: Some(Bytes::from_static(b"short")),
        });
        let result =
            upload::receive(store.clone(), [1; 16], object(b"short").sha256, body, None).await;
        assert!(matches!(result, Err(LfsError::Corrupt)));
        let list = store.list_with_delimiter(None).await?;
        assert!(list.objects.is_empty() && list.common_prefixes.is_empty());
    }
    Ok(())
}

fn object(bytes: &[u8]) -> LfsObject {
    LfsObject {
        sha256: Sha256::digest(bytes).into(),
        size: bytes.len() as u64,
        blake3: *blake3::hash(bytes).as_bytes(),
    }
}

#[tokio::test]
async fn corruption_cannot_yield_a_complete_response() -> TestResult {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut bytes = vec![7; CHUNK_BYTES + 23];
    let expected = object(&bytes);
    upload::receive(
        store.clone(),
        [1; 16],
        expected.sha256,
        Body::from(bytes.clone()),
        None,
    )
    .await?;
    bytes[CHUNK_BYTES] = 9;
    store
        .put(
            &crate::external::part(&lfs_path([1; 16], &expected.sha256), 1),
            bytes[CHUNK_BYTES..].to_vec().into(),
        )
        .await?;
    let mut reader = LfsRead::open(store, [1; 16], expected, None).await?;
    let first = poll_fn(|cx| Pin::new(&mut reader).poll_next(cx))
        .await
        .ok_or("missing first range")??;
    assert_eq!(first.len(), CHUNK_BYTES);
    assert!(matches!(
        poll_fn(|cx| Pin::new(&mut reader).poll_next(cx)).await,
        Some(Err(LfsError::Corrupt))
    ));
    assert!(
        poll_fn(|cx| Pin::new(&mut reader).poll_next(cx))
            .await
            .is_none()
    );
    Ok(())
}

#[tokio::test]
async fn replacing_an_object_during_download_rejects_the_next_range() -> TestResult {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let bytes = vec![7; CHUNK_BYTES + 23];
    let expected = object(&bytes);
    let path = lfs_path([1; 16], &expected.sha256);
    upload::receive(
        store.clone(),
        [1; 16],
        expected.sha256,
        Body::from(bytes),
        None,
    )
    .await?;
    let mut reader = LfsRead::open(store.clone(), [1; 16], expected, None).await?;
    poll_fn(|cx| Pin::new(&mut reader).poll_next(cx))
        .await
        .ok_or("missing first range")??;
    store
        .put(&path, vec![9; expected.size as usize].into())
        .await?;
    assert!(matches!(
        poll_fn(|cx| Pin::new(&mut reader).poll_next(cx)).await,
        Some(Err(LfsError::Store(
            object_store::Error::Precondition { .. }
        )))
    ));
    Ok(())
}

struct BrokenInput(bool);
impl Stream for BrokenInput {
    type Item = Result<Bytes, std::io::Error>;
    fn poll_next(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if !self.0 {
            self.0 = true;
            Poll::Ready(Some(Ok(Bytes::from(vec![3; CHUNK_BYTES]))))
        } else {
            Poll::Ready(Some(Err(std::io::Error::other("disconnected"))))
        }
    }
}

#[tokio::test]
async fn disconnected_upload_does_not_publish_a_body_or_leave_staging() -> TestResult {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    assert!(matches!(
        upload::receive(
            store.clone(),
            [1; 16],
            [1; 32],
            Body::from_stream(BrokenInput(false)),
            None
        )
        .await,
        Err(LfsError::Body(_))
    ));
    let list = store.list_with_delimiter(None).await?;
    assert!(list.objects.is_empty() && list.common_prefixes.is_empty());
    Ok(())
}

struct Stalled;
impl Stream for Stalled {
    type Item = Result<Bytes, std::io::Error>;
    fn poll_next(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Pending
    }
}

#[tokio::test(start_paused = true)]
async fn stalled_upload_times_out_and_cleans_staging() -> TestResult {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let started = tokio::time::Instant::now();
    assert!(matches!(
        upload::receive(
            store.clone(),
            [1; 16],
            [1; 32],
            Body::from_stream(Stalled),
            None
        )
        .await,
        Err(LfsError::Timeout)
    ));
    assert_eq!(started.elapsed(), IO_TIMEOUT);
    let list = store.list_with_delimiter(None).await?;
    assert!(list.objects.is_empty() && list.common_prefixes.is_empty());
    Ok(())
}

#[tokio::test]
async fn empty_upload_and_conflicting_destination_preserve_content_identity() -> TestResult {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let empty = object(b"");
    let actual = upload::receive(store.clone(), [1; 16], empty.sha256, Body::empty(), None).await?;
    assert_eq!(actual, empty);
    verify_lfs_object(store.clone(), [1; 16], actual, None).await?;
    let expected = object(b"correct bytes");
    let path = crate::external::part(&lfs_path([1; 16], &expected.sha256), 0);
    store
        .put(&path, Bytes::from_static(b"wrong bytes!!").into())
        .await?;
    assert!(matches!(
        upload::receive(
            store.clone(),
            [1; 16],
            expected.sha256,
            Body::from("correct bytes"),
            None
        )
        .await,
        Err(LfsError::Corrupt)
    ));
    assert_eq!(
        store.get(&path).await?.bytes().await?.as_ref(),
        b"wrong bytes!!"
    );
    Ok(())
}

struct Progressing {
    ticks: tokio::time::Interval,
    remaining: usize,
}

impl Stream for Progressing {
    type Item = Result<Bytes, std::io::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.remaining == 0 {
            return Poll::Ready(None);
        }
        std::task::ready!(self.ticks.poll_tick(cx));
        self.remaining -= 1;
        Poll::Ready(Some(Ok(Bytes::from_static(b"x"))))
    }
}

#[tokio::test(start_paused = true)]
async fn progressing_upload_outlives_the_former_transfer_deadline() -> TestResult {
    let expected = object(&[b'x'; 32]);
    let body = Body::from_stream(Progressing {
        ticks: tokio::time::interval(std::time::Duration::from_secs(100)),
        remaining: 32,
    });
    let started = tokio::time::Instant::now();
    let actual = upload::receive(
        Arc::new(InMemory::new()),
        [1; 16],
        expected.sha256,
        body,
        None,
    )
    .await?;
    assert!(started.elapsed() > std::time::Duration::from_secs(30 * 60));
    assert_eq!(actual, expected);
    Ok(())
}
