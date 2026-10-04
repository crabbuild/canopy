//! Shared read-round admission and quiescence for repository recovery owners.
use super::{RecoveryScanLimits, RootRecoveryError};
use crate::{AdmissionPermit, ReadIdentity, admission::AccountAdmission};
use std::sync::{Arc, Mutex};
use tokio::sync::Notify;
use tokio_util::{sync::CancellationToken, task::TaskTracker};

#[derive(Clone)]
pub struct RecoveryScanBudget {
    inner: Arc<Budget>,
}
struct Budget {
    limit: usize,
    admission: AccountAdmission,
    stop: CancellationToken,
    tasks: TaskTracker,
}
impl RecoveryScanBudget {
    pub fn new(limit: u16, tasks: TaskTracker) -> Result<Self, RootRecoveryError> {
        if !(2..=64).contains(&limit) {
            return Err(RootRecoveryError::InvalidScanLimits);
        }
        Ok(Self {
            inner: Arc::new(Budget {
                limit: usize::from(limit),
                admission: AccountAdmission::new(
                    usize::from(limit),
                    "node recovery reads",
                    "account recovery reads",
                ),
                stop: CancellationToken::new(),
                tasks,
            }),
        })
    }
    /// Account comes from trusted repository administration, not a viewer label.
    pub fn settings(&self, limits: RecoveryScanLimits, account: &str) -> RecoveryScanSettings {
        RecoveryScanSettings {
            limits,
            budget: self.clone(),
            account: Arc::from(account),
        }
    }
    /// Stop discovery between owned reads; never cancel a current round.
    pub fn close(&self) {
        self.inner.stop.cancel();
    }
    pub fn in_flight(&self) -> usize {
        self.inner.limit - self.inner.admission.available()
    }
}

#[derive(Clone)]
pub struct RecoveryScanSettings {
    pub limits: RecoveryScanLimits,
    budget: RecoveryScanBudget,
    account: Arc<str>,
}
impl RecoveryScanSettings {
    pub(super) fn validate(&self) -> Result<(), RootRecoveryError> {
        self.limits.validate()?;
        if self.account.is_empty() || self.account.len() > 4096 {
            return Err(RootRecoveryError::InvalidScanLimits);
        }
        Ok(())
    }
    pub(super) async fn acquire(&self) -> Result<AdmissionPermit, cellule_runtime::Error> {
        self.budget
            .inner
            .admission
            .acquire(ReadIdentity::Account(&self.account))
            .await
    }
    pub(super) fn spawn<F>(&self, future: F) -> tokio::task::JoinHandle<F::Output>
    where
        F: std::future::Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.budget.inner.tasks.spawn(future)
    }
    pub(super) async fn delay(&self, control: &ScanControl) {
        let changed = control.inner.changed.notified();
        tokio::pin!(changed);
        changed.as_mut().enable();
        if control.interrupted(self) {
            return;
        }
        tokio::select! {
            _ = tokio::time::sleep(self.limits.interval) => {},
            _ = changed => {},
            _ = self.budget.inner.stop.cancelled() => {},
        }
    }
}

#[derive(Clone, Default)]
pub(super) struct ScanControl {
    inner: Arc<Control>,
}
#[derive(Default)]
struct Control {
    state: Mutex<ControlState>,
    changed: Notify,
}
#[derive(Default)]
struct ControlState {
    paused: bool,
    stopped: bool,
    active: bool,
}
impl ScanControl {
    pub(super) async fn enter(&self, settings: &RecoveryScanSettings) -> Option<ActiveScan> {
        loop {
            let changed = self.inner.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            {
                let mut state = self.inner.state.lock().expect("recovery scan control");
                if state.stopped || settings.budget.inner.stop.is_cancelled() {
                    return None;
                }
                if !state.paused {
                    assert!(!state.active, "one owner per recovery scanner");
                    state.active = true;
                    return Some(ActiveScan(self.clone()));
                }
            }
            tokio::select! {
                _ = changed => {},
                _ = settings.budget.inner.stop.cancelled() => return None,
            }
        }
    }
    pub(super) fn interrupted(&self, settings: &RecoveryScanSettings) -> bool {
        let state = self.inner.state.lock().expect("recovery scan control");
        state.paused || state.stopped || settings.budget.inner.stop.is_cancelled()
    }
    pub(super) async fn pause(&self) {
        self.inner
            .state
            .lock()
            .expect("recovery scan control")
            .paused = true;
        self.inner.changed.notify_waiters();
        loop {
            let changed = self.inner.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if !self
                .inner
                .state
                .lock()
                .expect("recovery scan control")
                .active
            {
                return;
            }
            changed.await;
        }
    }
    pub(super) fn resume(&self) {
        self.inner
            .state
            .lock()
            .expect("recovery scan control")
            .paused = false;
        self.inner.changed.notify_waiters();
    }
    pub(super) fn stop(&self) {
        self.inner
            .state
            .lock()
            .expect("recovery scan control")
            .stopped = true;
        self.inner.changed.notify_waiters();
    }
}
pub(super) struct ActiveScan(ScanControl);
impl Drop for ActiveScan {
    fn drop(&mut self) {
        self.0
            .inner
            .state
            .lock()
            .expect("recovery scan control")
            .active = false;
        self.0.inner.changed.notify_waiters();
    }
}

#[cfg(test)]
mod tests;
