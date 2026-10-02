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
async fn packet_preflight_stops_before_pack_and_rewinds_after_rejection() -> Result<()> {
    let directory = tempfile::TempDir::new()?;
    let budget = DiskBudget::new(1024);
    for bytes in [
        b"0008test0000PACKnot-read".as_slice(),
        b"0008truncated".as_slice(),
        b"+005x0000".as_slice(),
    ] {
        let input = GitInput::receive(
            Body::from(bytes.to_vec()),
            directory.path(),
            &budget,
            Some(1024),
            None,
        )
        .await?;
        assert!(input.packet_prefix(7).await.is_err());
        assert_eq!(input.prefix(1024).await?, bytes);
        if bytes.ends_with(b"PACKnot-read") {
            assert_eq!(input.packet_prefix(12).await?, b"0008test0000");
        } else {
            assert!(input.packet_prefix(1024).await.is_err());
        }
        assert_eq!(input.prefix(1024).await?, bytes);
    }
    Ok(())
}

#[tokio::test]
async fn option_group_stops_before_pack_and_preserves_the_spool() -> Result<()> {
    let directory = tempfile::TempDir::new()?;
    let bytes = b"0008test00000008note0000PACKnot-read";
    let input = GitInput::receive(
        Body::from(bytes.as_slice()),
        directory.path(),
        &DiskBudget::new(1024),
        Some(1024),
        None,
    )
    .await?;
    assert_eq!(input.packet_prefix(12).await?, b"0008test0000");
    assert_eq!(input.packet_group(12, 12).await?, b"0008note0000");
    assert_eq!(input.prefix(1024).await?, bytes);
    Ok(())
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
        Some(10),
        None,
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
        Some(5),
        None,
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
    let first = GitInput::receive(
        Body::from("123456"),
        directory.path(),
        &budget,
        Some(10),
        None,
    )
    .await?;
    let second = GitInput::receive(
        Body::from("12345"),
        directory.path(),
        &budget,
        Some(10),
        None,
    )
    .await;
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
        GitInput::receive(input, directory.path(), &budget, Some(10), None).await,
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
            Some(10),
            None,
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

fn gzip(bytes: &[u8]) -> Result<Vec<u8>> {
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(bytes)?;
    Ok(encoder.finish()?)
}

#[tokio::test]
async fn gzip_members_preserve_wire_digest_and_release_encoded_admission() -> Result<()> {
    let directory = tempfile::TempDir::new()?;
    let budget = DiskBudget::new(1024);
    let mut wire = gzip(b"abc")?;
    wire.extend(gzip(b"def")?);
    let mut expected = blake3::Hasher::new();
    expected.update(&(wire.len() as u64).to_le_bytes());
    expected.update(&wire);
    let input = GitInput::receive(
        Body::from(wire),
        directory.path(),
        &budget,
        Some(1024),
        None,
    )
    .await?;
    assert_eq!(
        input.digest(blake3::Hasher::new()).await?,
        *expected.finalize().as_bytes()
    );
    let decoded = input
        .decode_gzip(directory.path(), &budget, Some(6))
        .await?;
    assert_eq!(decoded.size(), 6);
    assert_eq!(budget.used(), 6);
    let mut bytes = Vec::new();
    (&decoded.spool.file).read_to_end(&mut bytes)?;
    assert_eq!(bytes, b"abcdef");
    drop(decoded);
    assert_eq!(budget.used(), 0);
    Ok(())
}

#[tokio::test]
async fn gzip_expansion_enforces_the_decoded_limit_and_shared_disk_budget() -> Result<()> {
    let directory = tempfile::TempDir::new()?;
    let budget = DiskBudget::new(200_000);
    let wire = gzip(&vec![b'a'; 100_000])?;
    for (limit, reserve, expected_budget_error) in [(99_999, 0, false), (100_000, 150_000, true)] {
        let occupied = budget.try_reserve(reserve)?;
        let input = GitInput::receive(
            Body::from(wire.clone()),
            directory.path(),
            &budget,
            Some(200_000),
            None,
        )
        .await?;
        let result = input
            .decode_gzip(directory.path(), &budget, Some(limit))
            .await;
        assert!(if expected_budget_error {
            matches!(result, Err(InputError::Budget(_)))
        } else {
            matches!(result, Err(InputError::TooLarge))
        });
        assert_eq!(budget.used(), occupied.bytes());
        drop(occupied);
    }
    let input = GitInput::receive(
        Body::from(wire),
        directory.path(),
        &budget,
        Some(200_000),
        None,
    )
    .await?;
    let decoded = input
        .decode_gzip(directory.path(), &budget, Some(100_000))
        .await?;
    assert_eq!(decoded.size(), 100_000);
    drop(decoded);
    assert_eq!(budget.used(), 0);
    Ok(())
}

#[tokio::test]
async fn invalid_gzip_never_returns_a_partial_decoded_spool() -> Result<()> {
    let directory = tempfile::TempDir::new()?;
    let budget = DiskBudget::new(1 << 20);
    let valid = gzip(&vec![b'x'; 100_000])?;
    let mut checksum = valid.clone();
    let crc = checksum.len() - 8;
    checksum[crc] ^= 1;
    let truncated = valid[..valid.len() - 1].to_vec();
    let mut trailing = valid.clone();
    trailing.extend(b"trailing bytes");
    let mut last_member = valid;
    last_member.extend(&checksum);
    for wire in [
        vec![],
        b"invalid".to_vec(),
        checksum,
        truncated,
        trailing,
        last_member,
    ] {
        let input = GitInput::receive(
            Body::from(wire),
            directory.path(),
            &budget,
            Some(1 << 20),
            None,
        )
        .await?;
        assert!(matches!(
            input
                .decode_gzip(directory.path(), &budget, Some(1 << 20))
                .await,
            Err(InputError::Gzip(_))
        ));
        assert_eq!(budget.used(), 0);
    }
    assert_eq!(std::fs::read_dir(directory.path())?.count(), 0);
    Ok(())
}

#[test]
fn cancelled_decoder_stops_compressed_reads_before_consuming_more_input() -> Result<()> {
    let mut file = tempfile::tempfile()?;
    file.write_all(&gzip(b"data")?)?;
    file.rewind()?;
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let mut decoder = flate2::read::MultiGzDecoder::new(DecodeReader {
        file: &file,
        cancelled: &cancelled,
    });
    assert_eq!(
        decoder.read(&mut [0; 64]).unwrap_err().kind(),
        io::ErrorKind::TimedOut
    );
    assert_eq!(file.stream_position()?, 0);
    Ok(())
}

#[test]
fn cancelling_a_queued_decoder_retains_admission_until_its_worker_exits() -> Result<()> {
    use std::future::Future;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()?;
    runtime.block_on(async {
        let directory = tempfile::TempDir::new()?;
        let budget = DiskBudget::new(1024);
        let wire = gzip(b"queued decoder")?;
        let wire_len = wire.len() as u64;
        let transfers = crate::admission::AccountAdmission::new(2, "total", "account");
        let permit = Arc::new(transfers.acquire(crate::ReadIdentity::Anonymous).await?);
        let input = GitInput::receive(
            Body::from(wire),
            directory.path(),
            &budget,
            Some(1024),
            Some(permit),
        )
        .await?;
        let (entered, ready) = tokio::sync::oneshot::channel();
        let (release, held) = std::sync::mpsc::channel();
        let blocker = tokio::task::spawn_blocking(move || {
            let _ = entered.send(());
            held.recv()
        });
        ready.await?;
        let mut decode = Box::pin(input.decode_gzip(directory.path(), &budget, Some(1024)));
        poll_fn(|cx| {
            assert!(decode.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        drop(decode);
        let retained = budget.used();
        let available = transfers
            .acquire(crate::ReadIdentity::Anonymous)
            .await
            .is_ok();
        release.send(())?;
        blocker.await??;
        assert_eq!(retained, wire_len);
        assert!(!available);
        tokio::time::timeout(Duration::from_secs(5), async {
            while budget.used() != 0
                || transfers
                    .acquire(crate::ReadIdentity::Anonymous)
                    .await
                    .is_err()
            {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        assert_eq!(std::fs::read_dir(directory.path())?.count(), 0);
        Ok(())
    })
}

#[tokio::test(start_paused = true)]
async fn progressing_upload_outlives_the_idle_deadline() -> Result<()> {
    let directory = tempfile::TempDir::new()?;
    let budget = DiskBudget::new(1024);
    let (sender, receiver) = mpsc::channel(1);
    let path = directory.path().to_owned();
    let upload_budget = budget.clone();
    let upload = tokio::spawn(async move {
        GitInput::receive(
            Body::from_stream(Frames(receiver)),
            &path,
            &upload_budget,
            None,
            None,
        )
        .await
    });
    for total in 1..=3 {
        sender.send(Ok(Bytes::from_static(b"x"))).await?;
        while budget.used() != total {
            tokio::task::yield_now().await;
        }
        tokio::time::advance(Duration::from_secs(100)).await;
    }
    drop(sender);
    assert_eq!(upload.await??.size(), 3);
    Ok(())
}
