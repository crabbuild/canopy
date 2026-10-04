//! Shared authoritative preparation lease; no artifact loads or scratch.
use super::*;
use cellule_runtime::{CellClient, CellTarget, Receipt};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::time::Instant;

#[derive(Clone)]
pub struct PreparationSession {
    pub(super) authority: PreparationAuthority,
    pub(super) client: CellClient,
    pub(super) target: CellTarget,
    pub(super) check: LeaseCheck,
    pub(super) lease: PreparationLease,
    pub(super) deadline: Arc<Mutex<Instant>>,
    pub(super) ceiling: Option<Instant>,
    pub(super) fenced: Arc<AtomicBool>,
    fence_changed: tokio::sync::watch::Sender<bool>,
}
impl PreparationSession {
    pub async fn open(
        client: CellClient,
        target: CellTarget,
        check: LeaseCheck,
        minimum: Option<Receipt>,
        authority: PreparationAuthority,
    ) -> Result<Self, PreparationBaseError> {
        if crate::repository_target(
            target.tenant(),
            target.application(),
            check.token.repository,
        )
        .map_err(|_| PreparationBaseError::Context)?
            != target
            || !authority.matches(&target)
        {
            return Err(PreparationBaseError::Context);
        }
        let (lease, deadline) = probe(&client, &target, &check, minimum, &authority).await?;
        Ok(Self {
            authority,
            client,
            target,
            check,
            lease,
            deadline: Arc::new(Mutex::new(deadline)),
            ceiling: None,
            fenced: Arc::new(AtomicBool::new(false)),
            fence_changed: tokio::sync::watch::channel(false).0,
        })
    }
    pub(super) fn capability(&self) -> (&CellClient, &CellTarget, &LeaseCheck) {
        (&self.client, &self.target, &self.check)
    }
    pub(super) fn live_lease(&self) -> Result<(PreparationLease, Instant), PreparationBaseError> {
        let deadline = *self
            .deadline
            .lock()
            .map_err(|_| PreparationBaseError::Context)?;
        let deadline = self.ceiling.map_or(deadline, |limit| deadline.min(limit));
        if self.fenced.load(Ordering::Acquire) || Instant::now() >= deadline {
            return Err(PreparationBaseError::Inactive);
        }
        Ok((self.lease, deadline))
    }
    pub(super) async fn check_owner(&self) -> Result<(), PreparationBaseError> {
        let result = self
            .authority
            .check(&self.target, self.check.token.owner)
            .await;
        if result.is_err() {
            self.fence();
        }
        result
    }
    pub(super) fn fence(&self) {
        self.fenced.store(true, Ordering::Release);
        // Retain the terminal value even with no observers. A worker subscribing
        // after a fence must not wait for another notification.
        self.fence_changed.send_replace(true);
    }
    pub(super) async fn wait_fenced(&self) {
        let mut changed = self.fence_changed.subscribe();
        while !*changed.borrow_and_update() {
            if changed.changed().await.is_err() {
                return;
            }
        }
    }
    pub(super) async fn refresh(&self, minimum: Receipt) -> Result<(), PreparationBaseError> {
        let result = self.refresh_inner(minimum).await;
        if result.is_err() {
            self.fence();
        }
        result
    }
    async fn refresh_inner(&self, minimum: Receipt) -> Result<(), PreparationBaseError> {
        if self.fenced.load(Ordering::Acquire)
            || self.ceiling.is_some_and(|limit| Instant::now() >= limit)
        {
            return Err(PreparationBaseError::Inactive);
        }
        let (lease, deadline) = probe(
            &self.client,
            &self.target,
            &self.check,
            Some(minimum),
            &self.authority,
        )
        .await?;
        if lease.token != self.lease.token
            || lease.base != self.lease.base
            || lease.format != self.lease.format
        {
            return Err(PreparationBaseError::Context);
        }
        let mut shared = self
            .deadline
            .lock()
            .map_err(|_| PreparationBaseError::Context)?;
        if self.fenced.load(Ordering::Acquire)
            || self.ceiling.is_some_and(|limit| Instant::now() >= limit)
        {
            return Err(PreparationBaseError::Inactive);
        }
        *shared = deadline;
        Ok(())
    }
}
async fn probe(
    client: &CellClient,
    target: &CellTarget,
    check: &LeaseCheck,
    minimum: Option<Receipt>,
    authority: &PreparationAuthority,
) -> Result<(PreparationLease, Instant), PreparationBaseError> {
    // Start before the query, not after its reply, so transport/queue time can
    // only shorten the usable lease. Queries do not replay stored commands.
    let started = Instant::now();
    authority.check(target, check.token.owner).await?;
    let lease = client
        .query::<CheckPreparation>(target, minimum, check.clone())
        .await
        .map_err(|error| PreparationBaseError::Query(Box::new(error)))?
        .output
        .ok_or(PreparationBaseError::Inactive)?;
    if lease.token != check.token
        || lease.observed_at_ms < 0
        || lease.expires_at_ms <= lease.observed_at_ms
    {
        return Err(PreparationBaseError::Context);
    }
    let remaining = (lease.expires_at_ms - lease.observed_at_ms) as u64;
    let deadline = started
        .checked_add(Duration::from_millis(remaining.min(MAX_LEASE_MS)))
        .ok_or(PreparationBaseError::Context)?;
    if Instant::now() >= deadline {
        return Err(PreparationBaseError::Inactive);
    }
    authority.check(target, check.token.owner).await?;
    if Instant::now() >= deadline {
        return Err(PreparationBaseError::Inactive);
    }
    Ok((lease, deadline))
}
