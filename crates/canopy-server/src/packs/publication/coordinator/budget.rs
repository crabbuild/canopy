//! One shared budget for every repository dispatcher on a node. Command credits
//! cover retained originals through uncertainty; transport slots cover dispatch.
use super::*;
use std::sync::Mutex as LedgerMutex;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

#[derive(Clone)]
pub struct PublicationBudget {
    inner: Arc<BudgetInner>,
}
struct BudgetInner {
    limits: PublicationLimits,
    ledger: LedgerMutex<Ledger>,
    dispatch: [Arc<Semaphore>; 2],
}
#[derive(Default)]
struct Ledger {
    counts: [usize; 2],
    bytes: [u64; 2],
    actors: HashMap<String, ActorBudget>,
    closed: bool,
}
struct ActorBudget {
    counts: [usize; 2],
    dispatch: [Arc<Semaphore>; 2],
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PublicationBudgetStats {
    pub foreground: usize,
    pub maintenance: usize,
    pub accounts: usize,
    pub command_bytes: u64,
    pub foreground_dispatch: usize,
    pub maintenance_dispatch: usize,
    pub closed: bool,
}
impl PublicationBudget {
    /// Reuse the dispatcher profile; foreground_burst remains a repository
    /// scheduling setting. This budget reserves independent class shares.
    pub fn new(limits: PublicationLimits) -> Result<Self, PublicationScheduleError> {
        limits.validate()?;
        if limits.maintenance_operations < 2
            || limits.maintenance_in_flight < 2
            || limits.in_flight - limits.maintenance_in_flight < 2
        {
            return Err(PublicationScheduleError::InvalidLimits);
        }
        Ok(Self {
            inner: Arc::new(BudgetInner {
                limits,
                ledger: LedgerMutex::new(Ledger::default()),
                dispatch: [
                    Arc::new(Semaphore::new(
                        limits.in_flight - limits.maintenance_in_flight,
                    )),
                    Arc::new(Semaphore::new(limits.maintenance_in_flight)),
                ],
            }),
        })
    }
    /// Stop new reservations without cancelling admitted dispatch or recovery.
    pub fn close(&self) {
        self.inner.ledger.lock().expect("publication budget").closed = true;
    }
    pub fn stats(&self) -> PublicationBudgetStats {
        let ledger = self.inner.ledger.lock().expect("publication budget");
        let limits = self.inner.limits;
        PublicationBudgetStats {
            foreground: ledger.counts[0],
            maintenance: ledger.counts[1],
            accounts: ledger.actors.len(),
            command_bytes: ledger.bytes.iter().sum(),
            foreground_dispatch: limits.in_flight
                - limits.maintenance_in_flight
                - self.inner.dispatch[0].available_permits(),
            maintenance_dispatch: limits.maintenance_in_flight
                - self.inner.dispatch[1].available_permits(),
            closed: ledger.closed,
        }
    }
    pub(super) fn reserve(
        &self,
        class: PublicationClass,
        actor: &str,
        bytes: u64,
    ) -> Result<BudgetPermit, PublicationScheduleError> {
        let mut ledger = self.inner.ledger.lock().expect("publication budget");
        if ledger.closed {
            return Err(PublicationScheduleError::Closed);
        }
        let at = class.index();
        let limits = self.inner.limits;
        let (operations, ceiling, actor_limit) = match class {
            PublicationClass::Foreground => (
                limits.operations - limits.maintenance_operations,
                limits.command_bytes
                    - limits.maintenance_operations as u64 * MAINTENANCE_RESERVATION,
                limits.per_actor,
            ),
            PublicationClass::Maintenance => (
                limits.maintenance_operations,
                limits.maintenance_operations as u64 * MAINTENANCE_RESERVATION,
                limits.per_actor.min(limits.maintenance_operations / 2),
            ),
        };
        if bytes == 0
            || ledger.counts[at] >= operations
            || ledger
                .actors
                .get(actor)
                .map_or(0, |account| account.counts[at])
                >= actor_limit
            || ceiling
                .checked_sub(bytes)
                .is_none_or(|remaining| ledger.bytes[at] > remaining)
        {
            return Err(PublicationScheduleError::Capacity);
        }
        ledger.counts[at] += 1;
        ledger.bytes[at] += bytes;
        ledger
            .actors
            .entry(actor.to_owned())
            .or_insert_with(|| ActorBudget {
                counts: [0; 2],
                dispatch: [
                    Arc::new(Semaphore::new(
                        limits
                            .per_actor
                            .min((limits.in_flight - limits.maintenance_in_flight) / 2),
                    )),
                    Arc::new(Semaphore::new(
                        limits.per_actor.min(limits.maintenance_in_flight / 2),
                    )),
                ],
            })
            .counts[at] += 1;
        Ok(BudgetPermit {
            inner: Arc::clone(&self.inner),
            class,
            actor: actor.to_owned(),
            bytes,
        })
    }
    pub(super) async fn dispatch(&self, class: PublicationClass, actor: &str) -> DispatchPermit {
        // Semaphores are never closed: closing admission must retain exact
        // recovery after shutdown, including jobs already waiting for a slot.
        let actor_dispatch = {
            let ledger = self.inner.ledger.lock().expect("publication budget");
            Arc::clone(
                &ledger
                    .actors
                    .get(actor)
                    .expect("admitted publication account")
                    .dispatch[class.index()],
            )
        };
        // Account waiters must not occupy node slots while waiting for their
        // account share, including when one account spans many repositories.
        let actor = actor_dispatch
            .acquire_owned()
            .await
            .expect("owned publication account dispatch budget");
        let class = Arc::clone(&self.inner.dispatch[class.index()])
            .acquire_owned()
            .await
            .expect("owned publication dispatch budget");
        DispatchPermit {
            _actor: actor,
            _class: class,
        }
    }
}
pub(super) struct DispatchPermit {
    _actor: OwnedSemaphorePermit,
    _class: OwnedSemaphorePermit,
}
pub(super) struct BudgetPermit {
    inner: Arc<BudgetInner>,
    class: PublicationClass,
    actor: String,
    bytes: u64,
}
impl Drop for BudgetPermit {
    fn drop(&mut self) {
        let mut ledger = self.inner.ledger.lock().expect("publication budget");
        let at = self.class.index();
        ledger.counts[at] -= 1;
        ledger.bytes[at] -= self.bytes;
        let account = ledger
            .actors
            .get_mut(&self.actor)
            .expect("admitted publication account");
        account.counts[at] -= 1;
        if account.counts == [0; 2] {
            ledger.actors.remove(&self.actor);
        }
    }
}

#[cfg(test)]
mod tests;
