use super::*;

fn shell(script: &str) -> Command {
    let mut command = Command::new("sh");
    command
        .args(["-c", script])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    command
}

async fn chunk(body: &mut GitBody) -> Option<Result<Bytes, GitHttpError>> {
    poll_fn(|cx| Pin::new(&mut *body).poll_next(cx)).await
}

#[tokio::test]
async fn streaming_backpressure_bounds_queued_output_above_old_pack_limit()
-> Result<(), Box<dyn std::error::Error>> {
    let files = tempfile::TempDir::new()?;
    let completed = files.path().join("completed");
    let mut command = shell(
        "printf 'Content-Type: application/octet-stream\r\n\r\n'; dd if=/dev/zero bs=65536 count=1088 2>/dev/null; touch completed",
    );
    command.current_dir(files.path());
    let mut response = start_stream(command, Vec::new(), files, Duration::from_secs(30)).await?;
    tokio::time::timeout(Duration::from_secs(5), async {
        while response.body.receiver.len() < 4 {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    assert!(
        !completed.exists(),
        "producer must stop when the consumer stops reading"
    );
    let mut count = 0;
    while let Some(part) = chunk(&mut response.body).await {
        let part = part?;
        assert!(part.len() <= CHUNK_BYTES);
        count += part.len();
    }
    assert_eq!(count, 1088 * CHUNK_BYTES);
    Ok(())
}

#[tokio::test]
async fn exit_failure_after_headers_is_a_body_error() -> Result<(), Box<dyn std::error::Error>> {
    let mut response = start_stream(
        shell("printf 'Content-Type: application/octet-stream\r\n\r\npartial'; echo failed >&2; exit 7"),
        Vec::new(), (), Duration::from_secs(5),
    ).await?;
    let mut bytes = Vec::new();
    let error = loop {
        match chunk(&mut response.body).await {
            Some(Ok(part)) => bytes.extend_from_slice(&part),
            Some(Err(error)) => break error,
            None => return Err("nonzero exit became successful EOF".into()),
        }
    };
    assert_eq!(bytes, b"partial");
    assert!(
        matches!(error, GitHttpError::GitExit { status, stderr } if status.code() == Some(7) && stderr.contains("failed"))
    );
    assert!(chunk(&mut response.body).await.is_none());
    Ok(())
}

#[tokio::test]
async fn deadline_after_headers_is_a_body_error() -> Result<(), Box<dyn std::error::Error>> {
    let mut response = start_stream(
        shell("printf 'Content-Type: application/octet-stream\r\n\r\n'; sleep 30"),
        Vec::new(),
        (),
        Duration::from_secs(1),
    )
    .await?;
    assert!(matches!(
        chunk(&mut response.body).await,
        Some(Err(GitHttpError::Timeout))
    ));
    Ok(())
}

#[tokio::test]
async fn malformed_headers_fail_before_exposing_a_stream() -> Result<(), Box<dyn std::error::Error>>
{
    let response = start_stream(
        shell("printf 'not-a-header\r\n\r\n'"),
        Vec::new(),
        (),
        Duration::from_secs(5),
    )
    .await;
    assert!(matches!(response, Err(GitHttpError::MalformedCgi)));
    Ok(())
}

#[tokio::test]
async fn disconnect_kills_the_process_group_and_releases_cache()
-> Result<(), Box<dyn std::error::Error>> {
    struct Cache(Option<oneshot::Sender<()>>);
    impl Drop for Cache {
        fn drop(&mut self) {
            if let Some(sender) = self.0.take() {
                let _ = sender.send(());
            }
        }
    }
    let (released, receiver) = oneshot::channel();
    let mut response = start_stream(
        shell("sleep 30 & child=$!; printf 'Content-Type: text/plain\r\n\r\n%s %s\n' \"$$\" \"$child\"; wait"),
        Vec::new(), Cache(Some(released)), Duration::from_secs(30),
    ).await?;
    let mut ids = Vec::new();
    while !ids.contains(&b'\n') {
        ids.extend_from_slice(
            &chunk(&mut response.body)
                .await
                .ok_or("missing process IDs")??,
        );
    }
    let ids = std::str::from_utf8(&ids)?;
    let ids: Vec<i32> = ids
        .split_whitespace()
        .map(str::parse)
        .collect::<std::result::Result<_, _>>()?;
    assert_eq!(ids.len(), 2);
    drop(response);
    tokio::time::timeout(Duration::from_secs(5), receiver).await??;
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let mut running = false;
            for pid in &ids {
                let status = Command::new("ps")
                    .args(["-o", "stat=", "-p", &pid.to_string()])
                    .output()
                    .await?;
                let status = String::from_utf8_lossy(&status.stdout);
                // An orphan may briefly remain a zombie until the host's init reaps it.
                running |= !status.trim().is_empty() && !status.trim().starts_with('Z');
            }
            if !running {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        Ok::<_, std::io::Error>(())
    })
    .await??;
    Ok(())
}
