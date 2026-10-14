//! Hold the fork-to-exec descriptor inheritance window open deterministically.
use std::{
    fs::File,
    io::{self, Write},
    os::fd::{FromRawFd, OwnedFd},
};

pub(super) struct PausedChild {
    pid: libc::pid_t,
    release: File,
}

impl PausedChild {
    pub(super) fn new() -> io::Result<Self> {
        let mut gate = [-1; 2];
        // Both ends are owned below; the child uses only async-signal-safe
        // syscalls and _exit after fork, without touching the Tokio runtime.
        if unsafe { libc::pipe2(gate.as_mut_ptr(), libc::O_CLOEXEC) } != 0 {
            return Err(io::Error::last_os_error());
        }
        let read = unsafe { OwnedFd::from_raw_fd(gate[0]) };
        let write = unsafe { OwnedFd::from_raw_fd(gate[1]) };
        let pid = unsafe { libc::fork() };
        if pid == -1 {
            return Err(io::Error::last_os_error());
        }
        if pid == 0 {
            unsafe {
                libc::close(gate[1]);
                let mut byte = 0u8;
                while libc::read(gate[0], std::ptr::from_mut(&mut byte).cast(), 1) == -1 {
                    if *libc::__errno_location() != libc::EINTR {
                        break;
                    }
                }
                libc::_exit(0);
            }
        }
        drop(read);
        Ok(Self {
            pid,
            release: File::from(write),
        })
    }

    pub(super) fn assert_live(&self) {
        let mut status = 0;
        assert_eq!(
            unsafe { libc::waitpid(self.pid, &mut status, libc::WNOHANG) },
            0
        );
    }
}

impl Drop for PausedChild {
    fn drop(&mut self) {
        let _ = self.release.write_all(&[1]);
        let mut status = 0;
        loop {
            let result = unsafe { libc::waitpid(self.pid, &mut status, 0) };
            if result == self.pid || io::Error::last_os_error().raw_os_error() != Some(libc::EINTR)
            {
                break;
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn unpolled_and_refused_startup_deactivate_inherited_listener() -> super::Result {
    use super::*;
    for unpolled in [true, false] {
        let files = tempfile::TempDir::new()?;
        let data = files.path().join("node");
        let store = Arc::new(InMemory::new());
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let other = TcpListener::bind("127.0.0.1:0").await?;
        let startup = CanopyServer::start_with_listener(
            config(other.local_addr()?, data.clone()),
            store.clone(),
            listener,
        );
        let child = PausedChild::new()?;
        if unpolled {
            drop(startup);
        } else {
            assert!(matches!(
                startup.await,
                Err(canopy_server::server::ServerError::Http(
                    "HTTP listener address differs from listen configuration"
                ))
            ));
        }
        child.assert_live();
        let rebound = TcpListener::bind(address).await?;
        assert_eq!(rebound.local_addr()?, address);
        assert!(!data.exists());
        let remaining = store.list_with_delimiter(None).await?;
        assert!(remaining.objects.is_empty() && remaining.common_prefixes.is_empty());
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn shutdown_deactivates_inherited_http_and_ssh_listeners() -> super::Result {
    use super::*;
    let files = tempfile::TempDir::new()?;
    let data = files.path().join("node");
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let mut cfg = config(address, data.clone());
    cfg.ssh = Some(canopy_server::ssh::SshConfig {
        listen: "127.0.0.1:0".parse()?,
        host_key: ssh_key::PrivateKey::new(
            ssh_key::private::Ed25519Keypair::from_seed(&[19; 32]).into(),
            "forked-listener-test",
        )?,
    });
    let server =
        CanopyServer::start_with_listener(cfg, Arc::new(InMemory::new()), listener).await?;
    let ssh = server.ssh_addr().ok_or("SSH listener missing")?;
    let child = PausedChild::new()?;
    server.shutdown().await?;
    child.assert_live();
    let http = TcpListener::bind(address).await?;
    let ssh_rebound = TcpListener::bind(ssh).await?;
    assert_eq!(http.local_addr()?, address);
    assert_eq!(ssh_rebound.local_addr()?, ssh);
    let owner = workspace_lock(&data)?;
    owner.try_lock()?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn startup_error_deactivates_inherited_listener_before_workspace_unlock() -> super::Result {
    use super::*;
    let files = tempfile::TempDir::new()?;
    let data = files.path().join("node");
    let store = Arc::new(PausedStore::default());
    store.arm(ControlState::Serving);
    store.deny.store(true, Ordering::SeqCst);
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let startup = tokio::spawn(CanopyServer::start_with_listener(
        config(address, data.clone()),
        store.clone(),
        listener,
    ));
    store.wait().await?;
    let child = PausedChild::new()?;
    store.proceed.notify_one();
    // Observe the owner fence, rather than waiting for the supervisor return:
    // releasing it first would allow a restart before the socket deactivates.
    wait_for_cleanup(&data).await?;
    child.assert_live();
    let rebound = TcpListener::bind(address).await?;
    assert_eq!(rebound.local_addr()?, address);
    assert!(
        startup.await?.is_err(),
        "injected startup refusal disappeared"
    );
    Ok(())
}
