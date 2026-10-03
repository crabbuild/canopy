//! Own listener shutdown across Linux fork-to-exec descriptor inheritance.
use super::workspace::Workspace;
use std::{io, net::SocketAddr, sync::Arc};
use tokio::net::TcpListener;

/// The reservation drops before its listener, even for an unpolled handoff.
pub(super) struct ReservedListener {
    reservation: ListenerReservation,
    listener: TcpListener,
}

impl ReservedListener {
    pub(super) fn new(listener: TcpListener) -> io::Result<Self> {
        Ok(Self {
            reservation: ListenerReservation::new(&listener)?,
            listener,
        })
    }

    pub(super) fn local_addr(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    pub(super) fn into_parts(self) -> (TcpListener, ListenerReservation) {
        (self.listener, self.reservation)
    }
}

pub(super) struct ListenerReservation {
    #[cfg(target_os = "linux")]
    socket: std::os::fd::OwnedFd,
}

impl ListenerReservation {
    fn new(listener: &TcpListener) -> io::Result<Self> {
        #[cfg(target_os = "linux")]
        {
            use std::os::fd::{AsRawFd, FromRawFd};
            // Keep an owned descriptor, rather than an fd number that could be
            // reused after the serving task drops its listener. CLOEXEC does
            // not close descriptors in a child stalled between fork and exec.
            let fd = unsafe { libc::fcntl(listener.as_raw_fd(), libc::F_DUPFD_CLOEXEC, 3) };
            if fd == -1 {
                let error = io::Error::last_os_error();
                deactivate(listener.as_raw_fd());
                return Err(error);
            }
            Ok(Self {
                // SAFETY: successful duplication transfers a new socket fd.
                socket: unsafe { std::os::fd::OwnedFd::from_raw_fd(fd) },
            })
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = listener;
            Ok(Self {})
        }
    }
}

#[cfg(target_os = "linux")]
fn deactivate(socket: std::os::fd::RawFd) {
    // Linux shutdown removes the shared listening socket from TCP's listen
    // state even when an unrelated child still owns a forked descriptor. This
    // runs only after ingress joins, or while discarding an unused listener.
    loop {
        if unsafe { libc::shutdown(socket, libc::SHUT_RDWR) } == 0 {
            return;
        }
        let error = io::Error::last_os_error();
        match error.raw_os_error() {
            Some(libc::EINTR) => continue,
            Some(libc::ENOTCONN) => return, // Already deactivated.
            _ => tracing::error!(%error, "failed to deactivate owned listener"),
        }
        return;
    }
}

#[cfg(target_os = "linux")]
impl Drop for ListenerReservation {
    fn drop(&mut self) {
        use std::os::fd::AsRawFd;
        deactivate(self.socket.as_raw_fd());
    }
}

/// Field order keeps the workspace locked until both sockets deactivate on
/// every startup error path. During shutdown the serving tasks join first.
#[derive(Default)]
pub(super) struct ListenerReservations {
    pub(super) http: Option<ListenerReservation>,
    pub(super) ssh: Option<ListenerReservation>,
    pub(super) workspace: Option<Arc<Workspace>>,
}

impl ListenerReservations {
    pub(super) fn stop_ingress(&mut self) {
        self.http.take();
        self.ssh.take();
    }
}
