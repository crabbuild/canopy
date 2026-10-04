//! A generation's producer survives callers and owns renewal through real drain.
use super::*;
use crate::admission::AdmissionPermit;
use cellule_runtime::InvocationError;
use std::sync::{Mutex, Weak};
use tokio::{
    sync::watch,
    time::{Duration, Instant},
};
use tokio_util::task::TaskTracker;

#[derive(Debug, thiserror::Error)]
pub enum ServingOwnerError {
    #[error("serving owner read failed")]
    Read(#[from] ServingReadError),
    #[error("serving owner clock failed")]
    Clock(#[source] Box<crate::server::ServerError>),
    #[error("serving owner scheduling failed")]
    Schedule(#[from] PublicationScheduleError),
    #[error("serving owner command failed")]
    Command(#[source] Arc<PublicationError>),
    #[error("serving owner custody failed")]
    Custody(#[source] Box<CustodyError>),
    #[error("serving owner invariant failed")]
    Context,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ServingOwnerPhase {
    Acquiring,
    Ready,
    Draining,
    Released,
    Denied,
}
#[derive(Clone, Debug)]
pub struct ServingOwnerStats {
    pub phase: ServingOwnerPhase,
    pub token: Option<ServingToken>,
    pub renewals: u64,
    pub retries: u64,
    pub last_error: Option<Arc<ServingOwnerError>>,
}
struct Control {
    closed: bool,
    paused: bool,
    borrowers: usize,
    pin: Option<ServingPin>,
}
struct Inner {
    context: ServingContext,
    coordinator: PublicationCoordinator,
    request: BeginRequest,
    control: Mutex<Control>,
    driver: tokio::sync::Mutex<Driver>,
    changed: tokio::sync::Notify,
    tasks: TaskTracker,
    updates: watch::Sender<ServingOwnerStats>,
    permit: Mutex<Option<AdmissionPermit>>,
    #[cfg(test)]
    fault: std::sync::atomic::AtomicU8,
    #[cfg(test)]
    recovery_gate: tokio::sync::Mutex<Option<RecoveryGate>>,
}
#[cfg(test)]
struct RecoveryGate {
    entered: tokio::sync::oneshot::Sender<()>,
    proceed: tokio::sync::oneshot::Receiver<()>,
}
struct Lifetime(Weak<Inner>);
impl Drop for Lifetime {
    fn drop(&mut self) {
        if let Some(inner) = self.0.upgrade() {
            inner.close();
        }
    }
}
/// Clone shares one producer, one pin and one physical drain. Last handle loss
/// stops new borrows; detached workers keep their private ownership until drain.
#[derive(Clone)]
#[must_use]
pub struct ServingOwner {
    inner: Arc<Inner>,
    _lifetime: Arc<Lifetime>,
}
/// Joining an existing producer does not keep its admission lifetime open.
/// Residency can observe last-handle cleanup without becoming a producer.
#[derive(Clone)]
pub struct ServingDrainObserver {
    tasks: TaskTracker,
    stats: watch::Receiver<ServingOwnerStats>,
}
impl ServingDrainObserver {
    pub async fn wait(&self) -> ServingOwnerStats {
        self.tasks.close();
        self.tasks.wait().await;
        self.stats.borrow().clone()
    }
}
struct Borrow {
    inner: Arc<Inner>,
    _permit: AdmissionPermit,
}
impl Drop for Borrow {
    fn drop(&mut self) {
        let mut state = self.inner.control.lock().expect("serving owner control");
        state.borrowers -= 1;
        drop(state);
        self.inner.changed.notify_waiters();
    }
}
/// Its private guard survives snapshot clones. No raw pin escapes the producer.
#[derive(Clone)]
pub struct ServingSnapshot {
    pin: ServingPin,
    actor: Option<String>,
    _borrow: Arc<Borrow>,
}
impl ServingSnapshot {
    pub fn fact(&self) -> GenerationFact {
        self.pin.fact()
    }
    pub async fn headers(
        &self,
        ids: &[crate::ObjectId],
    ) -> Result<Vec<Option<crate::packs::metadata::ObjectHeader>>, ServingReadError> {
        self.pin.headers(self.actor.clone(), ids).await
    }
    pub async fn edges_page(
        &self,
        ids: &[crate::ObjectId],
        after: Option<(crate::ObjectId, crate::ObjectId)>,
    ) -> Result<ServingEdgePage, ServingReadError> {
        self.pin.edges_page(self.actor.clone(), ids, after).await
    }
    pub async fn body(
        &self,
        oid: crate::ObjectId,
        limit: usize,
    ) -> Result<Option<Vec<u8>>, ServingReadError> {
        self.pin.body(self.actor.clone(), oid, limit).await
    }
    pub async fn resolve_ref(
        &self,
        reference: Option<&str>,
    ) -> Result<ResolvedServingRef, ServingReadError> {
        self.pin.resolve_ref(self.actor.clone(), reference).await
    }
    pub async fn refs_page(
        &self,
        after: &str,
        generation: Option<i64>,
        live_only: bool,
    ) -> Result<crate::refs::RefPage, ServingReadError> {
        self.pin
            .refs_page(self.actor.clone(), after, generation, live_only)
            .await
    }
}
enum Original {
    Command(Arc<ReadyServingCommand>),
    Release(Arc<ReadyServingRelease>),
}
impl Original {
    fn copy(&self) -> ReadyPublication {
        match self {
            Self::Command(value) => value.dispatch_copy().into(),
            Self::Release(value) => value.dispatch_copy().into(),
        }
    }
}
#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    Acquire,
    Renew,
    Release,
}
struct Pending {
    kind: Kind,
    original: Original,
    ticket: Option<PublicationTicket>,
}
struct Driver {
    plan: Option<(Kind, MutationIdentity)>,
    pending: Option<Pending>,
    next_renewal: Instant,
    done: bool,
    renewal_denied: bool,
}
impl ServingOwner {
    pub async fn start(
        context: ServingContext,
        coordinator: PublicationCoordinator,
        request: BeginRequest,
        identity: MutationIdentity,
    ) -> Result<Self, ServingOwnerError> {
        let mut bytes = BoundedEncoder::new(4096).map_err(ServingReadError::Codec)?;
        request
            .encode(&mut bytes)
            .map_err(ServingReadError::Codec)?;
        if coordinator.target() != &context.target_for_handoff()
            || crate::repository_target(
                coordinator.target().tenant(),
                coordinator.target().application(),
                request.repository,
            )
            .map_err(ServingReadError::Capability)?
                != *coordinator.target()
        {
            return Err(ServingOwnerError::Context);
        }
        let permit = context.admit_owner(&request.actor).await?;
        let (updates, _) = watch::channel(ServingOwnerStats {
            phase: ServingOwnerPhase::Acquiring,
            token: None,
            renewals: 0,
            retries: 0,
            last_error: None,
        });
        let inner = Arc::new(Inner {
            context,
            coordinator,
            request,
            control: Mutex::new(Control {
                closed: false,
                paused: false,
                borrowers: 0,
                pin: None,
            }),
            driver: tokio::sync::Mutex::new(Driver {
                plan: Some((Kind::Acquire, identity)),
                pending: None,
                next_renewal: Instant::now(),
                done: false,
                renewal_denied: false,
            }),
            changed: tokio::sync::Notify::new(),
            tasks: TaskTracker::new(),
            updates,
            permit: Mutex::new(Some(permit)),
            #[cfg(test)]
            fault: std::sync::atomic::AtomicU8::new(0),
            #[cfg(test)]
            recovery_gate: tokio::sync::Mutex::new(None),
        });
        inner.tasks.spawn(supervise(inner.clone()));
        Ok(Self {
            _lifetime: Arc::new(Lifetime(Arc::downgrade(&inner))),
            inner,
        })
    }
    pub fn stats(&self) -> ServingOwnerStats {
        self.inner.updates.borrow().clone()
    }
    pub(super) fn is_drained(&self) -> bool {
        self.inner.tasks.is_empty()
    }
    pub(super) fn retire_if_idle(&self) -> bool {
        let mut state = self.inner.control.lock().expect("serving owner control");
        if state.borrowers != 0 {
            return false;
        }
        state.closed = true;
        drop(state);
        self.inner.changed.notify_waiters();
        true
    }
    /// A nonwaiting handshake: the worker cannot create another original after
    /// its driver lock has been observed while paused. Busy originals stay owned.
    pub(super) fn pause_for_drain(&self) -> Option<Option<ServingToken>> {
        self.inner
            .control
            .lock()
            .expect("serving owner control")
            .paused = true;
        let Ok(driver) = self.inner.driver.try_lock() else {
            return None;
        };
        if driver.done {
            return Some(None);
        }
        let state = self.inner.control.lock().expect("serving owner control");
        if state.closed || state.borrowers != 0 || driver.pending.is_some() {
            return None;
        }
        let pin = state.pin.as_ref()?;
        pin.workers_idle().then_some(Some(pin.token()))
    }
    pub(super) fn resume(&self) {
        self.inner
            .control
            .lock()
            .expect("serving owner control")
            .paused = false;
        self.inner.changed.notify_waiters();
    }
    pub fn drain_observer(&self) -> ServingDrainObserver {
        ServingDrainObserver {
            tasks: self.inner.tasks.clone(),
            stats: self.inner.updates.subscribe(),
        }
    }
    #[cfg(test)]
    pub(in crate::packs::publication) fn fault_for_test(&self, point: u8) {
        self.inner
            .fault
            .store(point, std::sync::atomic::Ordering::Release);
    }
    #[cfg(test)]
    pub(in crate::packs::publication) async fn pause_recovery_for_test(
        &self,
    ) -> (
        tokio::sync::oneshot::Sender<()>,
        tokio::sync::oneshot::Receiver<()>,
    ) {
        let (proceed, wait) = tokio::sync::oneshot::channel();
        let (entered, observed) = tokio::sync::oneshot::channel();
        *self.inner.recovery_gate.lock().await = Some(RecoveryGate {
            entered,
            proceed: wait,
        });
        (proceed, observed)
    }
    pub fn close(&self) {
        self.inner.close();
    }
    pub async fn close_and_drain(&self) -> ServingOwnerStats {
        self.close();
        self.inner.tasks.close();
        self.inner.tasks.wait().await;
        self.stats()
    }
    /// Waiters and returned borrows share a separate bounded node/account
    /// budget; long-lived snapshots cannot consume every physical I/O slot.
    pub async fn snapshot(
        &self,
        actor: Option<String>,
    ) -> Result<ServingSnapshot, ServingReadError> {
        let permit = self.inner.context.admit_snapshot(&actor).await?;
        self.snapshot_admitted(actor, permit).await
    }
    pub(super) async fn snapshot_admitted(
        &self,
        actor: Option<String>,
        permit: AdmissionPermit,
    ) -> Result<ServingSnapshot, ServingReadError> {
        let inner = self.inner.clone();
        self.inner
            .context
            .tasks()
            .spawn(async move {
                let mut permit = Some(permit);
                loop {
                    let changed = inner.changed.notified();
                    tokio::pin!(changed);
                    changed.as_mut().enable();
                    let acquired = {
                        let mut state = inner.control.lock().expect("serving owner control");
                        if state.closed || state.paused {
                            return Err(ServingReadError::Inactive);
                        }
                        if let Some(pin) = state.pin.clone() {
                            state.borrowers += 1;
                            Some(ServingSnapshot {
                                pin,
                                actor: actor.clone(),
                                _borrow: Arc::new(Borrow {
                                    inner: inner.clone(),
                                    _permit: permit.take().expect("snapshot admission"),
                                }),
                            })
                        } else {
                            None
                        }
                    };
                    if let Some(snapshot) = acquired {
                        snapshot.pin.authorize(actor).await?;
                        return Ok(snapshot);
                    }
                    changed.await;
                }
            })
            .await?
    }
}
impl Inner {
    #[cfg(test)]
    fn failpoint(&self, point: u8) {
        if self
            .fault
            .compare_exchange(
                point,
                0,
                std::sync::atomic::Ordering::AcqRel,
                std::sync::atomic::Ordering::Acquire,
            )
            .is_ok()
        {
            panic!("injected serving producer panic at {point}");
        }
    }
    fn close(&self) {
        self.control.lock().expect("serving owner control").closed = true;
        self.changed.notify_waiters();
    }
    fn pin(&self) -> Option<ServingPin> {
        self.control
            .lock()
            .expect("serving owner control")
            .pin
            .clone()
    }
    fn update(&self, change: impl FnOnce(&mut ServingOwnerStats)) {
        self.updates.send_modify(change);
    }
    async fn step(&self) -> Result<bool, ServingOwnerError> {
        // Plans, exact bodies and held tickets live outside the supervised task.
        // Panic during any awaited provider work cannot invent a replacement.
        let mut driver = self.driver.lock().await;
        if driver.done {
            return Ok(true);
        }
        if driver.pending.is_none() {
            let (closed, borrowers, pin) = {
                let state = self.control.lock().expect("serving owner control");
                if state.paused && !state.closed {
                    return Ok(false);
                }
                (state.closed, state.borrowers, state.pin.clone())
            };
            // A factory has not registered or submitted anything. Once all
            // borrows end, an unbuilt renewal plan can yield to physical drain.
            // An already built/admitted original never takes this shortcut.
            if closed
                && borrowers == 0
                && driver
                    .plan
                    .as_ref()
                    .is_some_and(|(kind, _)| *kind == Kind::Renew)
            {
                driver.plan.take();
            }
            if driver.plan.is_none() {
                let kind = if closed && borrowers == 0 {
                    Kind::Release
                } else if driver.renewal_denied {
                    return Ok(false);
                } else if Instant::now() >= driver.next_renewal {
                    Kind::Renew
                } else {
                    return Ok(false);
                };
                if pin.is_none() {
                    return Err(ServingOwnerError::Context);
                }
                driver.plan = Some((
                    kind,
                    crate::server::mutation_identity()
                        .map_err(|error| ServingOwnerError::Clock(Box::new(error)))?,
                ));
            }
            let (kind, identity) = driver.plan.expect("owned factory plan");
            let original = match kind {
                Kind::Acquire => Original::Command(Arc::new(
                    ReadyServingCommand::acquire(
                        self.context.client_for_owner(),
                        self.context.target_for_handoff(),
                        self.request.clone(),
                        identity,
                        self.context.authority_for_owner(),
                    )
                    .await
                    .map_err(|error| ServingOwnerError::Custody(Box::new(error)))?,
                )),
                Kind::Renew => Original::Command(Arc::new(
                    pin.ok_or(ServingOwnerError::Context)?
                        .ready_renew(
                            self.request.actor.clone(),
                            self.request.request_digest,
                            identity,
                            self.request.lease_ms,
                        )
                        .await?,
                )),
                Kind::Release => {
                    self.update(|stats| stats.phase = ServingOwnerPhase::Draining);
                    Original::Release(Arc::new(
                        pin.ok_or(ServingOwnerError::Context)?
                            .ready_release(identity)
                            .await?,
                    ))
                }
            };
            driver.pending = Some(Pending {
                kind,
                original,
                ticket: None,
            });
            #[cfg(test)]
            self.failpoint(1);
        }
        let pending = driver.pending.as_mut().expect("owned original");
        if pending.ticket.is_none() {
            match self.coordinator.try_reserve(pending.original.copy()) {
                Ok(ticket) => {
                    pending.ticket = Some(ticket);
                    #[cfg(test)]
                    self.failpoint(2);
                }
                Err(refused) => return Err(refused.reason.into()),
            }
        }
        let ticket = pending.ticket.as_ref().expect("owned held ticket");
        match ticket.state() {
            PublicationState::Held => {
                ticket.activate().await?;
                Ok(false)
            }
            PublicationState::Queued | PublicationState::Running => {
                ticket.wait().await;
                Ok(false)
            }
            PublicationState::Uncertain(_) => {
                #[cfg(test)]
                if let Some(gate) = self.recovery_gate.lock().await.take() {
                    let _ = gate.entered.send(());
                    let _ = gate.proceed.await;
                }
                ticket.recover().await?;
                Ok(false)
            }
            PublicationState::Discarded => Err(ServingOwnerError::Context),
            PublicationState::Finished(Err(error)) => {
                let denied = matches!(
                    &*error,
                    PublicationError::ServingCommand(InvocationError::Rejected(_))
                ) || matches!(&*error, PublicationError::Custody { source, .. } if
                    matches!(&**source, CustodyError::Stopped(_)) ||
                    matches!(&**source, CustodyError::Registration(error) if matches!(&**error, InvocationError::Rejected(_))));
                if denied && pending.kind != Kind::Release {
                    let has_pin = self.pin().is_some();
                    self.close();
                    driver.pending.take();
                    driver.plan.take();
                    driver.renewal_denied = true;
                    if !has_pin {
                        self.update(|stats| {
                            stats.phase = ServingOwnerPhase::Denied;
                            stats.last_error = Some(Arc::new(ServingOwnerError::Command(error)));
                        });
                        driver.done = true;
                        return Ok(true);
                    }
                    return Ok(false);
                }
                if pending.kind == Kind::Release
                    && matches!(
                        &*error,
                        PublicationError::ServingRelease(InvocationError::Rejected(_))
                    )
                {
                    // An immutable known denial releases nothing. A new proof
                    // can be prepared only after this original is settled.
                    driver.pending.take();
                    driver.plan.take();
                    return Err(ServingOwnerError::Command(error));
                }
                // A proven non-execution retries the original; its body never
                // changes because the factory plan remains retained.
                pending.ticket.take();
                Err(ServingOwnerError::Command(error))
            }
            PublicationState::Finished(Ok(PublicationOutcome::ServingRelease(value)))
                if pending.kind == Kind::Release
                    && value.output == ServingReleaseReply::Released =>
            {
                #[cfg(test)]
                self.failpoint(5);
                driver.pending.take();
                driver.plan.take();
                self.control
                    .lock()
                    .expect("serving owner control")
                    .pin
                    .take();
                driver.done = true;
                self.update(|stats| stats.phase = ServingOwnerPhase::Released);
                Ok(true)
            }
            PublicationState::Finished(Ok(PublicationOutcome::ServingCommand(value)))
                if pending.kind != Kind::Release =>
            {
                let ServingReply::Granted(lease) = value.output else {
                    return Err(ServingOwnerError::Context);
                };
                let pin = if pending.kind == Kind::Acquire {
                    let Original::Command(original) = &pending.original else {
                        return Err(ServingOwnerError::Context);
                    };
                    if let Some(pin) = self.pin() {
                        pin
                    } else {
                        let pin = original.retain_acquisition(self.context.clone()).await?;
                        self.control.lock().expect("serving owner control").pin = Some(pin.clone());
                        pin
                    }
                } else {
                    self.pin().ok_or(ServingOwnerError::Context)?
                };
                if lease.token != pin.token() || lease.fact != pin.fact() {
                    return Err(ServingOwnerError::Context);
                }
                #[cfg(test)]
                self.failpoint(if pending.kind == Kind::Acquire { 3 } else { 4 });
                let renewed = pending.kind == Kind::Renew;
                driver.pending.take();
                driver.plan.take();
                self.update(|stats| {
                    stats.token = Some(pin.token());
                    if renewed {
                        stats.renewals = stats.renewals.saturating_add(1);
                    }
                });
                match pin.authorize(Some(self.request.actor.clone())).await {
                    Ok(deadline) => {
                        driver.next_renewal =
                            Instant::now() + deadline.saturating_duration_since(Instant::now()) / 3;
                        self.update(|stats| stats.phase = ServingOwnerPhase::Ready);
                        self.changed.notify_waiters();
                    }
                    Err(error) => {
                        self.close();
                        return Err(error.into());
                    }
                }
                Ok(false)
            }
            _ => Err(ServingOwnerError::Context),
        }
    }
}
async fn run(inner: Arc<Inner>) {
    loop {
        let changed = inner.changed.notified();
        tokio::pin!(changed);
        changed.as_mut().enable();
        match inner.step().await {
            Ok(true) => {
                inner.permit.lock().expect("serving owner admission").take();
                inner.changed.notify_waiters();
                return;
            }
            Ok(false) => {}
            Err(error) => {
                if matches!(
                    &error,
                    ServingOwnerError::Read(
                        ServingReadError::Inactive
                            | ServingReadError::Authority(PreparationBaseError::Inactive)
                    )
                ) {
                    inner.close();
                    // No original was submitted if its factory failed. Wait
                    // for borrowers instead of churning unbuildable renewals.
                    let mut driver = inner.driver.lock().await;
                    if driver.pending.is_none() && inner.pin().is_some() {
                        driver.plan.take();
                        driver.renewal_denied = true;
                    }
                }
                inner.update(|stats| {
                    stats.retries = stats.retries.saturating_add(1);
                    stats.last_error = Some(Arc::new(error));
                });
            }
        }
        tokio::select! { _ = changed => {}, _ = tokio::time::sleep(Duration::from_millis(100)) => {} }
    }
}
async fn supervise(inner: Arc<Inner>) {
    loop {
        match tokio::spawn(run(inner.clone())).await {
            Ok(()) => return,
            Err(error) => inner.update(|stats| {
                stats.retries = stats.retries.saturating_add(1);
                stats.last_error = Some(Arc::new(ServingOwnerError::Read(ServingReadError::Task(
                    error,
                ))));
            }),
        }
    }
}
