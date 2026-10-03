//! Deferred reclamation keeps admission and alternates until the native fence
//! can be acquired. Brief inherited descriptors must not leak charges forever.

use super::*;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

pub(super) struct Cleanup {
    pub(super) path: PathBuf,
    pub(super) reservation: Option<DiskReservation>,
    pub(super) objects: Option<Arc<GitCache>>,
}

impl Cleanup {
    pub(super) fn defer(self) {
        static SLOTS: OnceLock<Arc<Semaphore>> = OnceLock::new();
        let slots = SLOTS.get_or_init(|| Arc::new(Semaphore::new(512)));
        let (Ok(runtime), Ok(slot)) = (
            tokio::runtime::Handle::try_current(),
            Arc::clone(slots).try_acquire_owned(),
        ) else {
            tracing::error!(path = %self.path.display(), "Git cleanup queue unavailable; admission retained until process restart");
            return; // Drop quarantines the still-owned reservation and alternate.
        };
        runtime.spawn(self.reap(slot));
    }

    async fn reap(mut self, slot: OwnedSemaphorePermit) {
        let mut delay = std::time::Duration::from_millis(10);
        loop {
            match crate::native_git::idle_fence(&self.path.join("repo.git")) {
                Ok(fence) => {
                    // The blocking job owns all charges even if its async waiter
                    // is canceled while filesystem removal is still running.
                    let _ = tokio::task::spawn_blocking(move || {
                        let _slot = slot;
                        let _fence = fence;
                        match fs::remove_dir_all(&self.path) {
                            Ok(()) => {}
                            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                            Err(error) => {
                                tracing::error!(path = %self.path.display(), error = %error, "Git cleanup failed; admission retained until process restart");
                                return;
                            }
                        }
                        // Successful removal permits normal field teardown.
                        self.reservation.take();
                        self.objects.take();
                    }).await;
                    return;
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    tokio::time::sleep(delay).await;
                    delay = (delay * 2).min(std::time::Duration::from_secs(5));
                }
                Err(error) => {
                    tracing::error!(path = %self.path.display(), error = %error, "Git fence acquisition failed; admission retained until process restart");
                    return;
                }
            }
        }
    }
}

impl Drop for Cleanup {
    fn drop(&mut self) {
        // Runtime shutdown, full queue or filesystem failure cannot make bytes
        // uncharged or invalidate an orphan worker's borrowed alternate.
        if let Some(reservation) = self.reservation.take() {
            std::mem::forget(reservation);
        }
        if let Some(objects) = self.objects.take() {
            std::mem::forget(objects);
        }
    }
}
