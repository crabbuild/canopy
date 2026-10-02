use super::*;
use std::{future::Future, process::Stdio, time::Duration};
use tokio::{io::AsyncReadExt, sync::oneshot};
type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

fn shell(root: &std::path::Path, script: &str) -> Command {
    let mut command = Command::new("sh");
    command
        .current_dir(root)
        .args(["-c", script])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    command
}
struct Owner(Option<oneshot::Sender<()>>);
impl Drop for Owner {
    fn drop(&mut self) {
        if let Some(done) = self.0.take() {
            let _ = done.send(());
        }
    }
}
async fn ready(path: &std::path::Path) -> Result {
    tokio::time::timeout(Duration::from_secs(5), async {
        while !path.exists() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await?;
    Ok(())
}

#[tokio::test]
async fn closed_stdio_does_not_complete_wait_before_descendant_drain() -> Result {
    let root = tempfile::TempDir::new()?;
    let permits = Arc::new(Semaphore::new(1));
    let permit = Arc::clone(&permits).try_acquire_owned()?;
    let (done, wait_done) = oneshot::channel();
    // The helper closes all standard streams. The leader exits immediately;
    // only the inherited completion descriptor exposes the remaining work.
    let command = shell(
        root.path(),
        "(touch ready; while [ ! -f release ]; do sleep 0.01; done) </dev/null >/dev/null 2>&1 & printf '%s\n' \"$!\"; exit 0",
    );
    let resources = crate::native_resources::NativeResources::default();
    let mut process = GitProcess::spawn(
        command,
        (permit, Owner(Some(done))),
        resources
            .scope(crate::native_resources::NativeClass::Foreground)
            .try_admit(crate::native_resources::NativeWork::Read)?,
    )?;
    let mut output = process.child.stdout.take().ok_or("stdout")?;
    let mut pid = Vec::new();
    tokio::time::timeout(Duration::from_secs(5), output.read_to_end(&mut pid)).await??;
    assert!(!pid.is_empty());
    ready(&root.path().join("ready")).await?;
    {
        let mut waiting = std::pin::pin!(process.wait());
        std::future::poll_fn(|cx| {
            assert!(waiting.as_mut().poll(cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
    }
    assert_eq!(
        resources.usage()?.foreground,
        crate::native_resources::NativeWork::Read.claim()
    );
    assert_eq!(permits.available_permits(), 0);
    std::fs::write(root.path().join("release"), b"drain")?;
    assert!(
        tokio::time::timeout(Duration::from_secs(5), process.wait())
            .await??
            .success()
    );
    assert_eq!(permits.available_permits(), 0);
    drop(process);
    tokio::time::timeout(Duration::from_secs(5), wait_done).await??;
    assert_eq!(permits.available_permits(), 1);
    tokio::time::timeout(Duration::from_secs(5), async {
        while resources.usage().unwrap().foreground.processes != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    assert_eq!(
        resources.usage()?,
        crate::native_resources::NativeUsage::default()
    );
    Ok(())
}

#[tokio::test]
async fn canceled_owner_remains_charged_when_a_descendant_escapes_the_group() -> Result {
    const HELPER_ROOT: &str = "CANOPY_TEST_NATIVE_ESCAPED_ROOT";
    if let Some(root) = std::env::var_os(HELPER_ROOT) {
        let root = std::path::PathBuf::from(root);
        // SAFETY: this branch runs only in the separately spawned fixture
        // process. Its background PID is not the original group leader.
        assert!(unsafe { libc::setsid() } > 0);
        std::fs::write(root.join("ready"), b"escaped")?;
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while !root.join("release").exists() && std::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        return Ok(());
    }
    let root = tempfile::TempDir::new()?;
    let permits = Arc::new(Semaphore::new(1));
    let permit = Arc::clone(&permits).try_acquire_owned()?;
    let (done, mut wait_done) = oneshot::channel();
    // An adversarial helper creates a separate session and closes standard
    // streams. Group SIGKILL cannot stop it; its inherited completion end must
    // keep ownership charged until it exits on release or its fixture deadline.
    let binary = std::env::current_exe()?;
    let binary = binary
        .to_str()
        .ok_or("test binary path")?
        .replace('\'', "'\\''");
    let script = format!(
        "'{binary}' --exact native_git::process::tests::canceled_owner_remains_charged_when_a_descendant_escapes_the_group </dev/null >/dev/null 2>&1 & printf '%s\\n' \"$!\"; exit 0"
    );
    let mut command = shell(root.path(), &script);
    command.env(HELPER_ROOT, root.path());
    let resources = crate::native_resources::NativeResources::default();
    let mut process = GitProcess::spawn(
        command,
        (permit, Owner(Some(done))),
        resources
            .scope(crate::native_resources::NativeClass::Foreground)
            .try_admit(crate::native_resources::NativeWork::Read)?,
    )?;
    let mut output = process.child.stdout.take().ok_or("stdout")?;
    let mut pid = Vec::new();
    tokio::time::timeout(Duration::from_secs(5), output.read_to_end(&mut pid)).await??;
    let pid: i32 = std::str::from_utf8(&pid)?.trim().parse()?;
    ready(&root.path().join("ready")).await?;
    // SAFETY: getpgid is a read-only query for this fixture's announced child.
    assert_eq!(unsafe { libc::getpgid(pid) }, pid);
    drop(process);
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut wait_done)
            .await
            .is_err()
    );
    assert_eq!(
        resources.usage()?.foreground,
        crate::native_resources::NativeWork::Read.claim()
    );
    assert_eq!(permits.available_permits(), 0);
    std::fs::write(root.path().join("release"), b"drain")?;
    tokio::time::timeout(Duration::from_secs(5), wait_done).await??;
    assert_eq!(permits.available_permits(), 1);
    tokio::time::timeout(Duration::from_secs(5), async {
        while resources.usage().unwrap().foreground.processes != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    assert_eq!(
        resources.usage()?,
        crate::native_resources::NativeUsage::default()
    );
    Ok(())
}

#[tokio::test]
async fn closed_daemon_standard_descriptors_cannot_replace_the_completion_end() -> Result {
    const CHILD_ROOT: &str = "CANOPY_TEST_NATIVE_CLOSED_STANDARD_ROOT";
    if let Some(root) = std::env::var_os(CHILD_ROOT) {
        use std::os::fd::{AsRawFd, FromRawFd};
        struct Restore {
            input: std::fs::File,
            output: std::fs::File,
        }
        impl Drop for Restore {
            fn drop(&mut self) {
                // SAFETY: these are the duplicated original descriptors in
                // this isolated test process; restore its stdin/stdout only.
                unsafe {
                    libc::dup2(self.input.as_raw_fd(), 0);
                    libc::dup2(self.output.as_raw_fd(), 1);
                }
            }
        }
        fn duplicate(fd: i32) -> std::io::Result<std::fs::File> {
            // SAFETY: duplicate a live standard descriptor into a fresh slot.
            let copy = unsafe { libc::fcntl(fd, libc::F_DUPFD_CLOEXEC, 3) };
            if copy == -1 {
                return Err(std::io::Error::last_os_error());
            }
            // SAFETY: the successful duplication returned an unowned file.
            Ok(unsafe { std::fs::File::from_raw_fd(copy) })
        }
        let restore = Restore {
            input: duplicate(0)?,
            output: duplicate(1)?,
        };
        // SAFETY: only this separately spawned fixture closes its own standard
        // descriptors. The parent and concurrent tests retain their originals.
        unsafe {
            libc::close(0);
            libc::close(1);
        }
        let root = std::path::PathBuf::from(root);
        let result = async {
            let command = shell(&root, "(touch ready; n=0; while [ ! -f release ] && [ \"$n\" -lt 500 ]; do n=$((n+1)); sleep 0.01; done) </dev/null >/dev/null 2>&1 & exit 0");
            let resources = crate::native_resources::NativeResources::default();
            let mut process = GitProcess::spawn(command, (), resources.scope(crate::native_resources::NativeClass::Foreground).try_admit(crate::native_resources::NativeWork::Read)?)?;
            let mut output = process.child.stdout.take().ok_or("stdout")?;
            let mut bytes = Vec::new();
            tokio::time::timeout(Duration::from_secs(5), output.read_to_end(&mut bytes)).await??;
            ready(&root.join("ready")).await?;
            {
                let mut pending = std::pin::pin!(process.wait());
                std::future::poll_fn(|cx| { assert!(pending.as_mut().poll(cx).is_pending()); std::task::Poll::Ready(()) }).await;
            }
            std::fs::write(root.join("release"), b"drain")?;
            assert!(tokio::time::timeout(Duration::from_secs(5), process.wait()).await??.success());
            drop(output);
            drop(process);
            Ok::<_, Box<dyn std::error::Error>>(())
        }.await;
        drop(restore);
        return result;
    }
    let root = tempfile::TempDir::new()?;
    let output = Command::new(std::env::current_exe()?)
        .args(["--exact", "native_git::process::tests::closed_daemon_standard_descriptors_cannot_replace_the_completion_end"])
        .env(CHILD_ROOT, root.path()).stdin(Stdio::null()).kill_on_drop(true)
        .output().await?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(())
}
