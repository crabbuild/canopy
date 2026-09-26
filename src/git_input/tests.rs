use super::*;
use bytes::Bytes;
use std::task::{Context, Poll};
use tokio::sync::mpsc;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

struct Frames(mpsc::Receiver<std::result::Result<Bytes, std::io::Error>>);
impl Stream for Frames {
    type Item = std::result::Result<Bytes, std::io::Error>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.0.poll_recv(cx)
    }
}

fn body(parts: Vec<std::result::Result<Bytes, std::io::Error>>) -> Body {
    let (sender, receiver) = mpsc::channel(parts.len().max(1));
    for part in parts {
        sender.try_send(part).unwrap();
    }
    Body::from_stream(Frames(receiver))
}

#[tokio::test]
async fn chunked_input_preserves_digest_and_rewinds_for_git() -> Result<()> {
    let directory = tempfile::TempDir::new()?;
    let budget = DiskBudget::new(10);
    let input = GitInput::receive(
        body(vec![
            Ok(Bytes::from_static(b"abc")),
            Ok(Bytes::from_static(b"def")),
        ]),
        directory.path(),
        &budget,
        10,
    )
    .await?;
    assert_eq!(input.size(), 6);
    assert_eq!(budget.used(), 6);
    let mut prefix = blake3::Hasher::new();
    prefix.update(b"metadata");
    let mut expected = prefix.clone();
    expected.update(&6u64.to_le_bytes());
    expected.update(b"abcdef");
    assert_eq!(input.digest(prefix).await?, *expected.finalize().as_bytes());
    let mut bytes = Vec::new();
    (&input.spool.file).read_to_end(&mut bytes)?;
    assert_eq!(bytes, b"abcdef");
    drop(input);
    assert_eq!(budget.used(), 0);
    Ok(())
}

#[tokio::test]
async fn limit_failure_reclaims_a_partial_spool() -> Result<()> {
    let directory = tempfile::TempDir::new()?;
    let budget = DiskBudget::new(10);
    let result = GitInput::receive(
        body(vec![
            Ok(Bytes::from_static(b"abc")),
            Ok(Bytes::from_static(b"def")),
        ]),
        directory.path(),
        &budget,
        5,
    )
    .await;
    assert!(matches!(result, Err(InputError::TooLarge)));
    assert_eq!(budget.used(), 0);
    assert_eq!(std::fs::read_dir(directory.path())?.count(), 0);
    Ok(())
}

#[tokio::test]
async fn concurrent_spools_share_disk_admission() -> Result<()> {
    let directory = tempfile::TempDir::new()?;
    let budget = DiskBudget::new(10);
    let first = GitInput::receive(Body::from("123456"), directory.path(), &budget, 10).await?;
    let second = GitInput::receive(Body::from("12345"), directory.path(), &budget, 10).await;
    assert!(matches!(second, Err(InputError::Budget(_))));
    assert_eq!(budget.used(), 6);
    drop(first);
    assert_eq!(budget.used(), 0);
    Ok(())
}

#[tokio::test]
async fn disconnected_input_reclaims_written_bytes() -> Result<()> {
    let directory = tempfile::TempDir::new()?;
    let budget = DiskBudget::new(10);
    let input = body(vec![
        Ok(Bytes::from_static(b"abc")),
        Err(std::io::Error::other("disconnected")),
    ]);
    assert!(matches!(
        GitInput::receive(input, directory.path(), &budget, 10).await,
        Err(InputError::Body(_))
    ));
    assert_eq!(budget.used(), 0);
    Ok(())
}

#[tokio::test]
async fn cancelled_upload_releases_the_file_and_reservation() -> Result<()> {
    let directory = tempfile::TempDir::new()?;
    let path = directory.path().to_path_buf();
    let budget = DiskBudget::new(10);
    let uploader_budget = budget.clone();
    let (sender, receiver) = mpsc::channel(1);
    sender.send(Ok(Bytes::from_static(b"abc"))).await?;
    let upload = tokio::spawn(async move {
        GitInput::receive(
            Body::from_stream(Frames(receiver)),
            &path,
            &uploader_budget,
            10,
        )
        .await
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        while budget.used() != 3 {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    upload.abort();
    assert!(upload.await.is_err());
    tokio::time::timeout(Duration::from_secs(5), async {
        while budget.used() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    assert_eq!(std::fs::read_dir(directory.path())?.count(), 0);
    Ok(())
}
