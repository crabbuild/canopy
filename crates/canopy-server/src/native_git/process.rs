//! Native process ownership survives cancellation until leader reaping and
//! inherited completion descriptors establish that descendant work drained.
use std::{
    io,
    process::ExitStatus,
    sync::{Arc, OnceLock},
};
use tokio::{
    process::{Child, ChildStderr, ChildStdin, ChildStdout, Command},
    sync::{OwnedSemaphorePermit, Semaphore},
};

#[cfg(unix)]
mod fence;

/// Expose standard streams, but keep wait/try_wait private to the guard.
/// Reaping the leader early would invalidate its safe group-signal identity.
pub(crate) struct ChildSlot {
    native: Option<Child>,
    pub(crate) stdin: Option<ChildStdin>,
    pub(crate) stdout: Option<ChildStdout>,
    pub(crate) stderr: Option<ChildStderr>,
}
impl ChildSlot {
    fn new(mut child: Child) -> Self {
        Self {
            stdin: child.stdin.take(),
            stdout: child.stdout.take(),
            stderr: child.stderr.take(),
            native: Some(child),
        }
    }
    pub(crate) fn id(&self) -> Option<u32> {
        self.native.as_ref().and_then(Child::id)
    }
}

pub(crate) struct GitProcess<T: Send + 'static> {
    pub(crate) child: ChildSlot,
    #[cfg(unix)]
    group: Option<i32>,
    #[cfg(unix)]
    fence: Option<fence::CompletionFence>,
    owner: Option<ProcessOwner<T>>,
}
struct ProcessOwner<T> {
    _owner: T,
    _native: crate::native_resources::NativePermit,
}
impl<T: Send + 'static> GitProcess<T> {
    pub(crate) fn spawn(
        mut command: Command,
        owner: T,
        native: crate::native_resources::NativePermit,
    ) -> io::Result<Self> {
        let owner = ProcessOwner {
            _owner: owner,
            _native: native,
        };
        #[cfg(unix)]
        let fence = {
            command.process_group(0);
            match fence::CompletionFence::install(&mut command) {
                Ok(fence) => fence,
                Err(error) => {
                    drop(command);
                    return Err(error);
                }
            }
        };
        let child = command.kill_on_drop(true).spawn();
        // Release parent copies of both inherited descriptors before failed
        // spawn can drop owners. Only the child/descendants retain those ends.
        drop(command);
        let child = child?;
        #[cfg(unix)]
        let group = Some(
            i32::try_from(
                child
                    .id()
                    .ok_or_else(|| io::Error::other("native child has no PID"))?,
            )
            .map_err(io::Error::other)?,
        );
        Ok(Self {
            child: ChildSlot::new(child),
            #[cfg(unix)]
            group,
            #[cfg(unix)]
            fence: Some(fence),
            owner: Some(owner),
        })
    }

    /// Do not reap the group leader while a descendant retains completion
    /// ownership. Its unreaped PID makes cancellation's group signal safe.
    pub(crate) async fn wait(&mut self) -> io::Result<ExitStatus> {
        #[cfg(unix)]
        self.fence
            .as_mut()
            .ok_or_else(|| io::Error::other("native fence is absent"))?
            .drain()
            .await?;
        let status = self
            .child
            .native
            .as_mut()
            .ok_or_else(|| io::Error::other("native child is absent"))?
            .wait()
            .await?;
        #[cfg(unix)]
        {
            self.group = None;
        }
        Ok(status)
    }
}
impl<T: Send + 'static> Drop for GitProcess<T> {
    fn drop(&mut self) {
        #[cfg(unix)]
        if let Some(group) = self.group
            && self.child.id() == u32::try_from(group).ok()
        {
            // SAFETY: spawn created this private process group; its leader
            // remains unreaped. Never signal after a caller reaped it.
            unsafe {
                libc::kill(-group, libc::SIGKILL);
            }
        }
        let Some(child) = self.child.native.take() else {
            return;
        };
        let cleanup = Cleanup {
            child: Some(child),
            #[cfg(unix)]
            fence: self.fence.take(),
            owner: self.owner.take(),
        };
        cleanup.defer();
    }
}

struct Cleanup<T: Send + 'static> {
    child: Option<Child>,
    #[cfg(unix)]
    fence: Option<fence::CompletionFence>,
    owner: Option<T>,
}
impl<T: Send + 'static> Cleanup<T> {
    fn defer(mut self) {
        // Completed wait needs no task or queue credit. Otherwise signaling is
        // not evidence of drain; keep the owner in a bounded supervised reaper.
        if self
            .child
            .as_ref()
            .is_some_and(|child| child.id().is_none())
        {
            #[cfg(unix)]
            let drained = self
                .fence
                .as_mut()
                .is_some_and(|fence| matches!(fence.try_drained(), Ok(true)));
            #[cfg(not(unix))]
            let drained = true;
            if drained {
                self.owner.take();
                return;
            }
        }
        static SLOTS: OnceLock<Arc<Semaphore>> = OnceLock::new();
        let slots = SLOTS.get_or_init(|| Arc::new(Semaphore::new(512)));
        let (Ok(runtime), Ok(slot)) = (
            tokio::runtime::Handle::try_current(),
            Arc::clone(slots).try_acquire_owned(),
        ) else {
            tracing::error!("native reaper unavailable; ownership quarantined until restart");
            return;
        };
        runtime.spawn(self.reap(slot));
    }
    async fn reap(mut self, _slot: OwnedSemaphorePermit) {
        let Some(child) = self.child.as_mut() else {
            return;
        };
        #[cfg(unix)]
        let result = if let Some(fence) = self.fence.as_mut() {
            // The original group signal preceded this task. Reap the leader and
            // drain descendants independently without issuing later PID signals.
            tokio::try_join!(child.wait(), fence.drain()).map(|_| ())
        } else {
            Err(io::Error::other("native cleanup fence is absent"))
        };
        #[cfg(not(unix))]
        let result = child.wait().await.map(|_| ());
        match result {
            Ok(()) => {
                self.owner.take();
            }
            Err(error) => {
                tracing::error!(%error, "native drain failed; ownership quarantined until restart");
            }
        }
    }
}
impl<T: Send + 'static> Drop for Cleanup<T> {
    fn drop(&mut self) {
        if let Some(owner) = self.owner.take() {
            // Shutdown, saturation or uncertain drain cannot release account/
            // process/cache admission while an inherited worker may still run.
            std::mem::forget(owner);
        }
    }
}

#[cfg(all(test, unix))]
mod tests;
