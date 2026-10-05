//! Bounded resident generation ownership; observers cannot abandon acquisitions.
use super::*;
use crate::admission::AdmissionPermit;
use std::sync::{
    Weak,
    atomic::{AtomicBool, Ordering},
};
use tokio::{
    sync::Mutex,
    time::{Duration, Instant, timeout_at},
};
use tokio_util::{sync::CancellationToken, task::TaskTracker};

pub const MAX_SERVING_GENERATIONS: u8 = 4;
// Bound detached request observers as well as latency. The producer retains its
// slot and exact release after this wait expires; timeout is never reclamation.
const ROLLOVER_WAIT: Duration = Duration::from_secs(2);
#[derive(Clone, Copy, Debug)]
pub struct ServingPoolLimits {
    pub generations: u8,
    pub lease_ms: u64,
}
impl Default for ServingPoolLimits {
    fn default() -> Self {
        Self {
            generations: MAX_SERVING_GENERATIONS,
            lease_ms: DEFAULT_LEASE_MS,
        }
    }
}
struct Slot {
    requested_generation: u64,
    touched: u64,
    retiring: bool,
    owner: ServingOwner,
}
struct State {
    closed: bool,
    clock: u64,
    slots: Vec<Slot>,
}
struct Inner {
    context: ServingContext,
    coordinator: PublicationCoordinator,
    limits: ServingPoolLimits,
    state: Mutex<State>,
    paused: AtomicBool,
    stop: CancellationToken,
    requests: TaskTracker,
    drain: TaskTracker,
    #[cfg(test)]
    selection_started: std::sync::Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
}
struct Lifetime(Weak<Inner>);
impl Drop for Lifetime {
    fn drop(&mut self) {
        if let Some(inner) = self.0.upgrade() {
            inner.stop.cancel();
        }
    }
}
/// Clones share one residency lifetime and at most four retained generation owners.
#[derive(Clone)]
#[must_use]
pub struct ServingPool {
    inner: Arc<Inner>,
    _lifetime: Arc<Lifetime>,
}
struct Resume {
    inner: Arc<Inner>,
    owners: Vec<ServingOwner>,
}
impl Drop for Resume {
    fn drop(&mut self) {
        for owner in &self.owners {
            owner.resume();
        }
        self.inner.paused.store(false, Ordering::Release);
    }
}
impl ServingPool {
    pub fn new(
        context: ServingContext,
        coordinator: PublicationCoordinator,
        limits: ServingPoolLimits,
    ) -> Result<Self, ServingOwnerError> {
        if !(1..=MAX_SERVING_GENERATIONS).contains(&limits.generations)
            || !(1000..=MAX_LEASE_MS).contains(&limits.lease_ms)
            || coordinator.target() != &context.target_for_handoff()
        {
            return Err(ServingOwnerError::Context);
        }
        let inner = Arc::new(Inner {
            context,
            coordinator,
            limits,
            state: Mutex::new(State {
                closed: false,
                clock: 0,
                slots: Vec::new(),
            }),
            paused: AtomicBool::new(false),
            stop: CancellationToken::new(),
            requests: TaskTracker::new(),
            drain: TaskTracker::new(),
            #[cfg(test)]
            selection_started: std::sync::Mutex::new(None),
        });
        let work = inner.clone();
        inner.drain.spawn(async move {
            work.stop.cancelled().await;
            let owners = {
                let mut state = work.state.lock().await;
                state.closed = true;
                state
                    .slots
                    .iter()
                    .map(|slot| slot.owner.clone())
                    .collect::<Vec<_>>()
            };
            for owner in &owners {
                owner.close();
            }
            work.requests.close();
            work.requests.wait().await;
            // Independent producers keep renewing/draining concurrently. One
            // blocked old generation cannot stop another's exact release.
            futures_util::future::join_all(owners.iter().map(|owner| owner.close_and_drain()))
                .await;
        });
        Ok(Self {
            _lifetime: Arc::new(Lifetime(Arc::downgrade(&inner))),
            inner,
        })
    }
    pub async fn snapshot(
        &self,
        actor: Option<String>,
    ) -> Result<ServingSnapshot, ServingOwnerError> {
        if self.inner.stop.is_cancelled() || self.inner.paused.load(Ordering::Acquire) {
            return Err(ServingReadError::Inactive.into());
        }
        // Admission covers queued selection, acquisition wait and the returned
        // borrow. A canceled observer cannot create unbounded detached waiters.
        let permit = self.inner.context.admit_snapshot(&actor).await?;
        let inner = self.inner.clone();
        self.inner
            .requests
            .spawn(async move { inner.snapshot(actor, permit).await })
            .await
            .map_err(ServingReadError::Task)?
    }
    pub fn close(&self) {
        self.inner.stop.cancel();
    }
    pub async fn close_and_drain(&self) {
        self.close();
        self.inner.drain.close();
        self.inner.drain.wait().await;
    }
    /// A private task owns the whole pause/gate/release handshake. Cancellation
    /// of this observer neither strands paused owners nor abandons accepted drain.
    pub async fn quiesce(&self) -> Result<bool, ServingOwnerError> {
        let inner = self.inner.clone();
        self.inner
            .drain
            .spawn(async move { inner.quiesce().await })
            .await
            .map_err(ServingReadError::Task)?
    }
    #[cfg(test)]
    pub(in crate::packs::publication) fn observe_selection_for_test(
        &self,
    ) -> tokio::sync::oneshot::Receiver<()> {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        *self
            .inner
            .selection_started
            .lock()
            .expect("selection observer") = Some(sender);
        receiver
    }
    #[cfg(test)]
    pub(in crate::packs::publication) async fn owners_for_test(&self) -> Vec<ServingOwner> {
        self.inner
            .state
            .lock()
            .await
            .slots
            .iter()
            .map(|slot| slot.owner.clone())
            .collect()
    }
}
impl Inner {
    async fn snapshot(
        self: Arc<Self>,
        actor: Option<String>,
        permit: AdmissionPermit,
    ) -> Result<ServingSnapshot, ServingOwnerError> {
        if self.stop.is_cancelled() || self.paused.load(Ordering::Acquire) {
            return Err(ServingReadError::Inactive.into());
        }
        let mut rollover_deadline = None;
        // Concurrent viewers may consume a released slot first. Bound retries
        // even under continuous publication; admission already bounds waiters.
        for _ in 0..=self.limits.generations {
            // Permission and current generation can change while release waits.
            #[cfg(test)]
            if let Some(observer) = self
                .selection_started
                .lock()
                .expect("selection observer")
                .take()
            {
                let _ = observer.send(());
            }
            let selected = self.context.select(actor.clone()).await?;
            enum Selection {
                Borrow(ServingOwner),
                Retire(ServingOwner),
            }
            let selection = {
                let mut state = self.state.lock().await;
                if state.closed || self.stop.is_cancelled() || self.paused.load(Ordering::Acquire) {
                    return Err(ServingReadError::Inactive.into());
                }
                state.slots.retain(|slot| !slot.owner.is_drained());
                state.clock = state.clock.saturating_add(1);
                let touched = state.clock;
                if let Some(slot) = state.slots.iter_mut().find(|slot| {
                    if slot.retiring {
                        return false;
                    }
                    let stats = slot.owner.stats();
                    match stats.token {
                        Some(token) => {
                            stats.phase == ServingOwnerPhase::Ready
                                && token.generation == selected.generation
                        }
                        None => {
                            stats.phase == ServingOwnerPhase::Acquiring
                                && slot.requested_generation == selected.generation
                        }
                    }
                }) {
                    slot.touched = touched;
                    Selection::Borrow(slot.owner.clone())
                } else if state.slots.len() >= usize::from(self.limits.generations) {
                    // No slot leaves the inventory before its real producer
                    // exits. Closing is shared; no waiter owns a replacement
                    // release or a second retired-owner inventory.
                    let mut order: Vec<_> = (0..state.slots.len()).collect();
                    order.sort_by_key(|i| state.slots[*i].touched);
                    let retiring = order
                        .iter()
                        .copied()
                        .find(|i| {
                            let slot = &mut state.slots[*i];
                            if !slot.retiring && slot.owner.retire_if_idle() {
                                slot.retiring = true;
                                true
                            } else {
                                false
                            }
                        })
                        .or_else(|| order.into_iter().find(|i| state.slots[*i].retiring));
                    match retiring {
                        Some(i) => Selection::Retire(state.slots[i].owner.clone()),
                        None => return Err(generation_capacity()),
                    }
                } else {
                    let operation = *uuid::Uuid::new_v4().as_bytes();
                    let mut digest = blake3::Hasher::new();
                    digest.update(b"canopy.serving-pool.v1");
                    digest.update(&self.context.repository());
                    digest.update(&operation);
                    let owner = ServingOwner::start(
                        self.context.clone(),
                        self.coordinator.clone(),
                        BeginRequest {
                            repository: self.context.repository(),
                            operation,
                            request_digest: *digest.finalize().as_bytes(),
                            actor: self.context.administrator().to_owned(),
                            lease_ms: self.limits.lease_ms,
                        },
                        crate::server::mutation_identity()
                            .map_err(|error| ServingOwnerError::Clock(Box::new(error)))?,
                    )
                    .await?;
                    state.slots.push(Slot {
                        requested_generation: selected.generation,
                        touched,
                        retiring: false,
                        owner: owner.clone(),
                    });
                    Selection::Borrow(owner)
                }
            };
            match selection {
                Selection::Borrow(owner) => {
                    // Acquisition may accept a newer fact than selection.
                    return Ok(owner.snapshot_admitted(actor, permit).await?);
                }
                Selection::Retire(owner) => {
                    // Never wait for provider I/O or exact release under the
                    // pool lock. Shutdown can cancel this bounded observation
                    // without canceling the independently owned producer.
                    // Initial Cell selection may queue behind unrelated work.
                    // Charge the observation budget only once retirement starts;
                    // subsequent retries share it rather than extending it.
                    let deadline =
                        *rollover_deadline.get_or_insert_with(|| Instant::now() + ROLLOVER_WAIT);
                    let drain = owner.drain_observer();
                    tokio::select! {
                        _ = self.stop.cancelled() => return Err(ServingReadError::Inactive.into()),
                        result = timeout_at(deadline, drain.wait()) => {
                            if result.is_err() {
                                return Err(generation_capacity());
                            }
                        }
                    }
                }
            }
        }
        Err(generation_capacity())
    }
    async fn quiesce(self: Arc<Self>) -> Result<bool, ServingOwnerError> {
        let state = self.state.lock().await;
        if state.closed {
            let drained =
                self.requests.is_empty() && state.slots.iter().all(|slot| slot.owner.is_drained());
            drop(state);
            return Ok(drained && self.coordinator.close_if_idle().await);
        }
        if self.paused.swap(true, Ordering::AcqRel) {
            return Ok(false);
        }
        let pause = Resume {
            inner: self.clone(),
            owners: state.slots.iter().map(|slot| slot.owner.clone()).collect(),
        };
        drop(state);
        let mut tokens = Vec::new();
        for owner in &pause.owners {
            match owner.pause_for_drain() {
                Some(Some(token)) => tokens.push(token),
                Some(None) => {}
                None => return Ok(false),
            }
        }
        let Some(gate) = self.coordinator.reserve_serving_drain(&tokens).await? else {
            return Ok(false);
        };
        // No original can be built between the handshake and exclusive gate.
        // Queries already admitted finish without creating another slot.
        self.stop.cancel();
        for owner in &pause.owners {
            owner.close();
        }
        self.requests.close();
        self.requests.wait().await;
        futures_util::future::join_all(pause.owners.iter().map(|owner| owner.close_and_drain()))
            .await;
        while !gate.close_if_drained().await {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        // Private supervisor can finish concurrently; do not wait its shared
        // tracker here, which also owns this quiesce operation.
        Ok(true)
    }
}

fn generation_capacity() -> ServingOwnerError {
    ServingReadError::Capability(Error::Capacity("repository serving generations")).into()
}
