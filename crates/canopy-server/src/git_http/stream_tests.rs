use super::*;

fn shell(script: &str) -> Command {
    let mut command = Command::new("sh");
    command
        .args(["-c", script])
        .stdin(Stdio::null())
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
    let mut response = start_stream(
        command,
        files,
        Duration::from_secs(30),
        crate::native_resources::NativeResources::default()
            .scope(crate::native_resources::NativeClass::Foreground)
            .try_admit(crate::native_resources::NativeWork::Read)?,
    )
    .await?;
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
        (), Duration::from_secs(5), crate::native_resources::NativeResources::default().scope(crate::native_resources::NativeClass::Foreground).try_admit(crate::native_resources::NativeWork::Read)?).await?;
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
        (),
        Duration::from_secs(1),
        crate::native_resources::NativeResources::default()
            .scope(crate::native_resources::NativeClass::Foreground)
            .try_admit(crate::native_resources::NativeWork::Read)?,
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
        (),
        Duration::from_secs(5),
        crate::native_resources::NativeResources::default()
            .scope(crate::native_resources::NativeClass::Foreground)
            .try_admit(crate::native_resources::NativeWork::Read)?,
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
    let transfers = Arc::new(tokio::sync::Semaphore::new(1));
    let permit = Arc::clone(&transfers).try_acquire_owned()?;
    let (released, receiver) = oneshot::channel();
    let mut response = start_stream(
        shell("sleep 30 & child=$!; printf 'Content-Type: text/plain\r\n\r\n%s %s\n' \"$$\" \"$child\"; wait"),
        (Cache(Some(released)), permit), Duration::from_secs(30), crate::native_resources::NativeResources::default().scope(crate::native_resources::NativeClass::Foreground).try_admit(crate::native_resources::NativeWork::Read)?).await?;
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
    assert_eq!(transfers.available_permits(), 0);
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
            if !running && transfers.available_permits() == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        Ok::<_, std::io::Error>(())
    })
    .await??;
    Ok(())
}

#[tokio::test]
async fn completed_worker_releases_cache_before_headers_are_polled()
-> Result<(), Box<dyn std::error::Error>> {
    struct Owner {
        cache: Option<Arc<GitCache>>,
        finished: Option<oneshot::Sender<()>>,
    }
    impl Drop for Owner {
        fn drop(&mut self) {
            drop(self.cache.take());
            if let Some(finished) = self.finished.take() {
                let _ = finished.send(());
            }
        }
    }
    let files = tempfile::TempDir::new()?;
    let budget = DiskBudget::new(1 << 20);
    let cache = GitCache::create(
        files.path().into(),
        budget.clone(),
        "refs/heads/main",
        crate::ObjectFormat::Sha1,
        crate::native_resources::NativeResources::default()
            .scope(crate::native_resources::NativeClass::Foreground),
    )
    .await?;
    let mut command = crate::native_git::command(&cache.git_dir())?;
    command
        .args([
            "-c",
            "alias.probe=!printf 'Content-Type: text/plain\\r\\n\\r\\nok'",
            "probe",
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let (finished, wait) = oneshot::channel();
    let owner = Owner {
        cache: Some(cache),
        finished: Some(finished),
    };
    let mut request = Box::pin(start_stream(
        command,
        owner,
        Duration::from_secs(5),
        crate::native_resources::NativeResources::default()
            .scope(crate::native_resources::NativeClass::Foreground)
            .try_admit(crate::native_resources::NativeWork::Read)?,
    ));
    // On this single-thread executor the spawned worker cannot run before
    // the first header await. Leave the caller unpolled until worker cleanup.
    poll_fn(|cx| {
        assert!(request.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    tokio::time::timeout(Duration::from_secs(5), wait).await??;
    assert_eq!(budget.used(), 0);
    let mut response = request.await?;
    while let Some(part) = chunk(&mut response.body).await {
        part?;
    }
    Ok(())
}

#[tokio::test]
async fn failed_spawn_releases_parent_fence_before_cache_cleanup()
-> Result<(), Box<dyn std::error::Error>> {
    const ISOLATED: &str = "CANOPY_TEST_ISOLATED_FAILED_SPAWN";
    if std::env::var(ISOLATED).as_deref() != Ok("1") {
        // This checks the command's parent fence, not unrelated forks that can
        // briefly inherit its CLOEXEC descriptor. An inherited live fence must
        // prevent cleanup; exercise that case separately below.
        let status = Command::new(std::env::current_exe()?)
            .args([
                "--exact",
                "git_http::stream_tests::failed_spawn_releases_parent_fence_before_cache_cleanup",
                "--test-threads=1",
                "--nocapture",
            ])
            .env(ISOLATED, "1")
            .status()
            .await?;
        assert!(status.success());
        return Ok(());
    }
    let resources = crate::native_resources::NativeResources::default();
    let files = tempfile::TempDir::new()?;
    let budget = DiskBudget::new(1 << 20);
    let cache = GitCache::create(
        files.path().into(),
        budget.clone(),
        "refs/heads/main",
        crate::ObjectFormat::Sha1,
        crate::native_resources::NativeResources::default()
            .scope(crate::native_resources::NativeClass::Foreground),
    )
    .await?;
    let mut command = crate::native_git::command(&cache.git_dir())?;
    command.current_dir(files.path().join("missing"));
    assert!(matches!(
        GitProcess::spawn(command, cache, resources.scope(crate::native_resources::NativeClass::Foreground).try_admit(crate::native_resources::NativeWork::Read)?),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound
    ));
    // Concurrent forks can briefly inherit the parent's queued-command fence
    // before exec closes their CLOEXEC descriptors. Cleanup remains charged
    // until that fence is acquired rather than permanently leaking admission.
    tokio::time::timeout(Duration::from_secs(5), async {
        while budget.used() != 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    assert_eq!(budget.used(), 0);
    assert_eq!(
        resources.usage()?,
        crate::native_resources::NativeUsage::default()
    );
    Ok(())
}

// A concurrent fork can inherit the cache fence until it execs, even though
// CLOEXEC remains set in the parent. Failed-spawn cleanup must not undercount
// or delete such a generation; deferred cleanup waits for the inherited fence.
#[cfg(unix)]
#[tokio::test]
async fn inherited_fork_fence_keeps_failed_spawn_cache_charged()
-> Result<(), Box<dyn std::error::Error>> {
    use std::{
        io::{Read, Write},
        os::unix::{io::AsRawFd, net::UnixStream, process::CommandExt},
        thread::JoinHandle,
    };

    struct ForkBarrier {
        control: UnixStream,
        thread: Option<JoinHandle<std::io::Result<std::process::ExitStatus>>>,
    }
    impl ForkBarrier {
        fn finish(&mut self) -> Result<(), Box<dyn std::error::Error>> {
            self.control.write_all(b"X")?;
            let status = self
                .thread
                .take()
                .ok_or("missing helper thread")?
                .join()
                .map_err(|_| "helper thread panicked")??;
            assert!(status.success());
            Ok(())
        }
    }
    impl Drop for ForkBarrier {
        fn drop(&mut self) {
            if let Some(thread) = self.thread.take() {
                let _ = self.control.write_all(b"X");
                let _ = thread.join();
            }
        }
    }

    let resources = crate::native_resources::NativeResources::default();
    let files = tempfile::TempDir::new()?;
    let budget = DiskBudget::new(1 << 20);
    let cache = GitCache::create(
        files.path().into(),
        budget.clone(),
        "refs/heads/main",
        crate::ObjectFormat::Sha1,
        resources.scope(crate::native_resources::NativeClass::Foreground),
    )
    .await?;
    let charged = budget.used();
    assert!(charged > 0);
    let git_dir = cache.git_dir();
    let mut command = crate::native_git::command(&git_dir)?;
    command.current_dir(files.path().join("missing"));

    let (control, child_control) = UnixStream::pair()?;
    control.set_read_timeout(Some(Duration::from_secs(5)))?;
    let executable = std::env::current_exe()?;
    let thread = std::thread::spawn(move || {
        let mut unrelated = std::process::Command::new(executable);
        unrelated
            .arg("--help")
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null());
        // SAFETY: the child uses only async-signal-safe read/write, with an
        // owned live socket and stack/static byte buffers, before exec. Holding
        // this barrier models a concurrent fork's inherited CLOEXEC descriptors.
        unsafe {
            unrelated.pre_exec(move || {
                let fd = child_control.as_raw_fd();
                let mut release = 0_u8;
                if libc::write(fd, b"R".as_ptr().cast(), 1) != 1
                    || libc::read(fd, (&mut release as *mut u8).cast(), 1) != 1
                    || release != b'X'
                {
                    return Err(std::io::Error::from_raw_os_error(libc::EIO));
                }
                Ok(())
            });
        }
        unrelated.spawn()?.wait()
    });
    let mut barrier = ForkBarrier {
        control,
        thread: Some(thread),
    };
    let mut ready = [0_u8];
    barrier.control.read_exact(&mut ready)?;
    assert_eq!(ready, *b"R");
    assert!(matches!(
        GitProcess::spawn(
            command,
            cache,
            resources.scope(crate::native_resources::NativeClass::Foreground)
                .try_admit(crate::native_resources::NativeWork::Read)?,
        ),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound
    ));
    assert_eq!(budget.used(), charged);
    assert!(
        git_dir.exists(),
        "live inherited fence must prevent cache deletion"
    );
    let busy =
        crate::native_git::idle_fence(&git_dir).expect_err("inherited fence should still be held");
    assert_eq!(busy.kind(), std::io::ErrorKind::WouldBlock);
    barrier.finish()?;
    // Once the inherited worker has exec'd and drained its descriptor, the
    // deferred reaper must remove the files before releasing their disk charge.
    tokio::time::timeout(Duration::from_secs(5), async {
        while budget.used() != 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    assert!(
        !git_dir.exists(),
        "released disk admission requires reclamation"
    );
    assert_eq!(
        resources.usage()?,
        crate::native_resources::NativeUsage::default()
    );
    Ok(())
}
