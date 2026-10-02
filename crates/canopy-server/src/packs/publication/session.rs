//! Shared authoritative preparation lease; no artifact loads or scratch.
use super::*;
use cellule_runtime::{CellClient, CellTarget, MutationIdentity, Receipt};
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
    pub(super) client: CellClient,
    pub(super) target: CellTarget,
    pub(super) check: LeaseCheck,
    pub(super) lease: PreparationLease,
    pub(super) deadline: Arc<Mutex<Instant>>,
    pub(super) fenced: Arc<AtomicBool>,
}
impl PreparationSession {
    pub async fn open(
        client: CellClient,
        target: CellTarget,
        check: LeaseCheck,
        minimum: Option<Receipt>,
    ) -> Result<Self, PreparationBaseError> {
        if crate::repository_target(
            target.tenant(),
            target.application(),
            check.token.repository,
        )
        .map_err(|_| PreparationBaseError::Context)?
            != target
        {
            return Err(PreparationBaseError::Context);
        }
        let (lease, deadline) = probe(&client, &target, &check, minimum).await?;
        Ok(Self {
            client,
            target,
            check,
            lease,
            deadline: Arc::new(Mutex::new(deadline)),
            fenced: Arc::new(AtomicBool::new(false)),
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
        if self.fenced.load(Ordering::Acquire) || Instant::now() >= deadline {
            return Err(PreparationBaseError::Inactive);
        }
        Ok((self.lease, deadline))
    }
    /// A recorded renewal result is never a fresh clock observation. Query
    /// after the durability gate even when the command is exact-outcome replay.
    pub async fn renew(
        &self,
        identity: MutationIdentity,
        lease_ms: u64,
    ) -> Result<(), PreparationBaseError> {
        let result = self.renew_inner(identity, lease_ms).await;
        if result.is_err() {
            self.fenced.store(true, Ordering::Release);
        }
        result
    }
    async fn renew_inner(
        &self,
        identity: MutationIdentity,
        lease_ms: u64,
    ) -> Result<(), PreparationBaseError> {
        if self.fenced.load(Ordering::Acquire) {
            return Err(PreparationBaseError::Inactive);
        }
        let committed = self
            .client
            .command::<RenewPreparation>(
                &self.target,
                identity,
                LeaseRequest {
                    check: self.check.clone(),
                    lease_ms,
                },
            )
            .await
            .map_err(|error| PreparationBaseError::Command(Box::new(error)))?;
        let (lease, deadline) = probe(
            &self.client,
            &self.target,
            &self.check,
            Some(committed.receipt),
        )
        .await?;
        if lease.token != self.lease.token
            || lease.base != self.lease.base
            || lease.format != self.lease.format
        {
            return Err(PreparationBaseError::Context);
        }
        *self
            .deadline
            .lock()
            .map_err(|_| PreparationBaseError::Context)? = deadline;
        Ok(())
    }
}
async fn probe(
    client: &CellClient,
    target: &CellTarget,
    check: &LeaseCheck,
    minimum: Option<Receipt>,
) -> Result<(PreparationLease, Instant), PreparationBaseError> {
    // Start before the query, not after its reply, so transport/queue time can
    // only shorten the usable lease. Queries do not replay stored commands.
    let started = Instant::now();
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
    Ok((lease, deadline))
}
