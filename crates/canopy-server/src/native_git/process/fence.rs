use std::{io, os::unix::net::UnixStream as StdStream};
use tokio::{net::UnixStream, process::Command};

pub(super) struct CompletionFence(UnixStream);
impl CompletionFence {
    pub(super) fn install(command: &mut Command) -> io::Result<Self> {
        use std::os::fd::{AsRawFd, FromRawFd};
        let (reader, inherited) = StdStream::pair()?;
        // Standard streams may be closed in a daemon. Keep the inherited end
        // outside 0/1/2, which spawning replaces with the child's standard I/O.
        // SAFETY: duplicate this live socket into a new owned descriptor.
        let fd = unsafe { libc::fcntl(inherited.as_raw_fd(), libc::F_DUPFD_CLOEXEC, 3) };
        if fd == -1 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: successful duplication returned an unowned socket descriptor.
        let inherited = unsafe { StdStream::from_raw_fd(fd) };
        reader.set_nonblocking(true)?;
        let reader = UnixStream::from_std(reader)?;
        // SAFETY: only async-signal-safe fcntl runs after fork. The command
        // retains its end until spawn; exec/fork descendants inherit that end.
        unsafe {
            command.pre_exec(move || {
                let fd = inherited.as_raw_fd();
                let flags = libc::fcntl(fd, libc::F_GETFD);
                if flags == -1 || libc::fcntl(fd, libc::F_SETFD, flags & !libc::FD_CLOEXEC) == -1 {
                    return Err(io::Error::last_os_error());
                }
                Ok(())
            });
        }
        Ok(Self(reader))
    }
    pub(super) fn try_drained(&mut self) -> io::Result<bool> {
        match self.0.try_read(&mut [0]) {
            Ok(0) => Ok(true),
            Ok(_) => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "native completion descriptor carried data",
            )),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => Ok(false),
            Err(error) => Err(error),
        }
    }
    pub(super) async fn drain(&mut self) -> io::Result<()> {
        loop {
            if self.try_drained()? {
                return Ok(());
            }
            self.0.readable().await?;
        }
    }
}
