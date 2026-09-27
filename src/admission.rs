//! Bounded account shares of node work, retained until the last worker exits.

use crate::ReadIdentity;
use crab_cell_runtime::Error;
use std::{
    collections::HashMap,
    sync::{Arc, Weak},
};
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore};

/// Node and account admission retained together by work and streamed output.
pub struct AdmissionPermit {
    // Field drop order keeps account ownership covered by the global limit.
    _account: OwnedSemaphorePermit,
    _total: OwnedSemaphorePermit,
}

pub(crate) struct AccountAdmission {
    total: Arc<Semaphore>,
    waiting: Semaphore,
    account_limit: usize,
    total_capacity: &'static str,
    account_capacity: &'static str,
    accounts: Mutex<HashMap<Option<String>, Weak<Semaphore>>>,
}

impl AccountAdmission {
    pub(crate) fn new(
        limit: usize,
        total_capacity: &'static str,
        account_capacity: &'static str,
    ) -> Self {
        Self {
            total: Arc::new(Semaphore::new(limit)),
            waiting: Semaphore::new(limit),
            account_limit: limit / 2,
            total_capacity,
            account_capacity,
            accounts: Mutex::new(HashMap::new()),
        }
    }

    pub(crate) async fn acquire(&self, actor: ReadIdentity<'_>) -> Result<AdmissionPermit, Error> {
        let total = Arc::clone(&self.total)
            .try_acquire_owned()
            .map_err(|_| Error::Capacity(self.total_capacity))?;
        let semaphore = self.account(actor).await;
        let account = semaphore
            .try_acquire_owned()
            .map_err(|_| Error::Capacity(self.account_capacity))?;
        // Release the account owner first so its map entry stays covered by
        // global admission until the semaphore can no longer be retained.
        Ok(AdmissionPermit {
            _account: account,
            _total: total,
        })
    }

    pub(crate) async fn wait(&self, actor: ReadIdentity<'_>) -> Result<AdmissionPermit, Error> {
        // Bound retained HTTP requests before waiting. Acquire the account first:
        // a busy account must not reserve node slots needed by another account.
        let _waiting = self
            .waiting
            .try_acquire()
            .map_err(|_| Error::Capacity(self.total_capacity))?;
        let account = self
            .account(actor)
            .await
            .acquire_owned()
            .await
            .map_err(|_| Error::Capacity(self.account_capacity))?;
        let total = Arc::clone(&self.total)
            .acquire_owned()
            .await
            .map_err(|_| Error::Capacity(self.total_capacity))?;
        Ok(AdmissionPermit {
            _account: account,
            _total: total,
        })
    }

    async fn account(&self, actor: ReadIdentity<'_>) -> Arc<Semaphore> {
        let account = match actor {
            ReadIdentity::Account(account) => Some(account.to_owned()),
            ReadIdentity::Anonymous => None,
        };
        {
            let mut accounts = self.accounts.lock().await;
            // Permits retain their semaphore through detached ownership work.
            // Active or waiting admission bounds this map; expired accounts need no state.
            accounts.retain(|_, semaphore| semaphore.strong_count() != 0);
            if let Some(semaphore) = accounts.get(&account).and_then(Weak::upgrade) {
                semaphore
            } else {
                let semaphore = Arc::new(Semaphore::new(self.account_limit));
                accounts.insert(account, Arc::downgrade(&semaphore));
                semaphore
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn account_quotas_preserve_global_capacity_and_recover_after_release() {
        let admission = AccountAdmission::new(32, "total", "account");
        let mut permits = Vec::new();
        for actor in [ReadIdentity::Anonymous, ReadIdentity::Account("anonymous")] {
            for _ in 0..16 {
                permits.push(admission.acquire(actor).await.unwrap());
            }
            assert!(admission.acquire(actor).await.is_err());
        }
        assert!(
            admission
                .acquire(ReadIdentity::Account("another"))
                .await
                .is_err()
        );
        drop(permits.pop());
        let another = admission
            .acquire(ReadIdentity::Account("another"))
            .await
            .unwrap();
        assert_eq!(admission.total.available_permits(), 0);
        drop(another);
        drop(permits);
        assert_eq!(admission.total.available_permits(), 32);
    }

    #[tokio::test]
    async fn bounded_waiters_leave_node_slots_free_and_release_capacity_on_cancellation() {
        use std::{
            future::{Future, poll_fn},
            task::Poll,
        };
        let admission = AccountAdmission::new(2, "total", "account");
        let actor = ReadIdentity::Account("busy");
        let held = admission.acquire(actor).await.unwrap();
        let mut cancelled = Box::pin(admission.wait(actor));
        let mut pending = Box::pin(admission.wait(actor));
        poll_fn(|cx| {
            assert!(cancelled.as_mut().poll(cx).is_pending());
            assert!(pending.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        assert!(admission.wait(actor).await.is_err());
        let other = admission
            .acquire(ReadIdentity::Account("other"))
            .await
            .unwrap();
        drop(cancelled);
        assert_eq!(admission.waiting.available_permits(), 1);
        drop(held);
        let admitted = pending.await.unwrap();
        assert_eq!(admission.total.available_permits(), 0);
        drop(admitted);
        drop(other);
        assert_eq!(admission.waiting.available_permits(), 2);
        assert_eq!(admission.total.available_permits(), 2);
    }

    #[tokio::test]
    async fn finished_accounts_do_not_accumulate_state() {
        let admission = AccountAdmission::new(32, "total", "account");
        for index in 0..1024 {
            let account = format!("account-{index}");
            drop(
                admission
                    .acquire(ReadIdentity::Account(&account))
                    .await
                    .unwrap(),
            );
        }
        assert_eq!(admission.accounts.lock().await.len(), 1);
    }
}
