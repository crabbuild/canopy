//! Bounded, account-fair dispatch of privately prepared exact commands.
//! Catalog reconciliation, uploads and ref proof construction happen before
//! dispatch. This primitive does not guarantee progress against a moving root;
//! frontier pipelining and production orchestration are separate requirements.
use super::*;
use crate::{git_http::GitHttpResponse, packs::metadata::MetadataLimits};
use cellule_ltx::DiskBudget;
use cellule_runtime::{
    CellClient, CellTarget, Committed, InvocationError, MutationIdentity, PreparedCommand,
};
use std::{
    collections::{HashMap, VecDeque},
    path::Path,
    sync::Arc,
};
use tokio::{
    sync::{Mutex, Notify, watch},
    time::Instant,
};

/// Two encoded copies: retained exact retry and the in-flight transport.
/// Scratch/native work remains independently charged to its DiskBudget.
const COMMAND_RESERVATION: u64 = 8 << 20;
const INLINE_BYTES: u32 = 4 << 20;
mod inputs;
pub use inputs::{NativeInputReadyError, ReadyNativeInputs, RegisteredNativeInputs};
mod policy;
pub use policy::{ReadyRefPolicyPage, RefPolicyReadyError, RefPolicyRefusalFailure};
mod roots;
pub use roots::{ReadyRootPush, RootPushReadyError};
mod recovery;
pub use recovery::{ReadyBoundRecovery, RecoveryBindingFailure};
mod preparation;
pub use preparation::{
    PreparationCommandKind, PreparationCommandOutcome, PreparationReadyError, ReadyPreparation,
};
mod work;
use work::MAINTENANCE_RESERVATION;
pub use work::{
    CompactionReadyError, PublicationClass, PublicationError, PublicationOutcome,
    ReadyCatalogCompaction, ReadyPublication,
};

#[derive(Clone, Copy, Debug)]
pub struct PublicationLimits {
    /// Includes queued, executing and uncertain operations.
    pub operations: usize,
    pub per_actor: usize,
    pub command_bytes: u64,
    /// Commands awaiting durable outcome, not concurrent Cell transactions.
    pub in_flight: usize,
    /// Reserved admitted slots, including uncertain maintenance commands.
    pub maintenance_operations: usize,
    /// Maintenance cannot consume every concurrent durability wait slot.
    pub maintenance_in_flight: usize,
    pub foreground_burst: u8,
}
impl Default for PublicationLimits {
    fn default() -> Self {
        Self {
            operations: 32,
            per_actor: 8,
            command_bytes: 256 << 20,
            in_flight: 8,
            maintenance_operations: 4,
            maintenance_in_flight: 2,
            foreground_burst: 3,
        }
    }
}
impl PublicationLimits {
    fn validate(self) -> Result<(), PublicationScheduleError> {
        if self.operations < 3
            || self.operations > MAX_OPERATIONS as usize
            || self.maintenance_operations == 0
            || self.maintenance_operations >= self.operations
            || self.per_actor == 0
            || self.per_actor >= self.operations - self.maintenance_operations
            || self
                .command_bytes
                .saturating_sub(self.maintenance_operations as u64 * MAINTENANCE_RESERVATION)
                / COMMAND_RESERVATION
                <= self.per_actor as u64
            || self.in_flight == 0
            || self.in_flight > self.operations
            || self.maintenance_in_flight == 0
            || self.maintenance_in_flight > self.in_flight
            || (self.in_flight > 1 && self.maintenance_in_flight == self.in_flight)
            || !(1..=32).contains(&self.foreground_burst)
        {
            return Err(PublicationScheduleError::InvalidLimits);
        }
        Ok(())
    }
}

/// Private factory output. Keep this exact command on admission failure;
/// rebuilding a completion allocates a different response identity.
#[must_use]
pub struct ReadyCatalogPush {
    owner: PushPreparation,
    command: PreparedCommand<CompleteCatalogPush>,
}
#[derive(Clone)]
enum PushPreparation {
    Catalog(Arc<PreparedCatalog>),
    Outcome(Arc<PreparationSession>),
}
impl PushPreparation {
    fn session(&self) -> &PreparationSession {
        match self {
            Self::Catalog(prepared) => &prepared.base.session,
            Self::Outcome(session) => session,
        }
    }
    fn capability(&self) -> (&CellClient, &CellTarget, &LeaseCheck) {
        self.session().capability()
    }
}
impl PreparationSession {
    /// Retain the exact outcome command in the same bounded dispatch/recovery
    /// path as ref publications. No artifacts or catalog upload are needed.
    pub async fn ready_outcome(
        self: &Arc<Self>,
        identity: MutationIdentity,
        request: PushCompletionRequest,
    ) -> Result<ReadyCatalogPush, PushCompletionProofError> {
        let input = self.push_outcome(request).await?;
        input.encode(&mut BoundedEncoder::new(INLINE_BYTES)?)?;
        self.live_lease()?;
        let command = self
            .client
            .prepare_command::<CompleteCatalogPush>(&self.target, identity, input)
            .await
            .map_err(|error| PushCompletionProofError::Command(Box::new(error)))?;
        self.live_lease()?;
        Ok(ReadyCatalogPush {
            owner: PushPreparation::Outcome(Arc::clone(self)),
            command,
        })
    }
}
impl PreparedCatalog {
    pub async fn ready_push(
        self: &Arc<Self>,
        identity: MutationIdentity,
        request: PushCompletionRequest,
        root: &Path,
        budget: DiskBudget,
        limits: MetadataLimits,
    ) -> Result<ReadyCatalogPush, PushCompletionProofError> {
        let outcome_only = request.plan.is_none();
        let input = Box::pin(self.push_completion(request, root, budget, limits)).await?;
        // The reservation has a fixed upper bound even if a registry is later
        // configured with a larger command envelope. Large plans need roots.
        input.encode(&mut BoundedEncoder::new(INLINE_BYTES)?)?;
        self.ensure_live()?;
        let (client, target, _) = self.base.capability();
        let command = client
            .prepare_command::<CompleteCatalogPush>(target, identity, input)
            .await
            .map_err(|error| PushCompletionProofError::Command(Box::new(error)))?;
        self.ensure_live()?;
        Ok(ReadyCatalogPush {
            owner: if outcome_only {
                PushPreparation::Outcome(Arc::new(self.base.session.clone()))
            } else {
                PushPreparation::Catalog(Arc::clone(self))
            },
            command,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum PublicationScheduleError {
    #[error("invalid publication admission limits")]
    InvalidLimits,
    #[error("publication coordinator is closed")]
    Closed,
    #[error("publication admission capacity exceeded")]
    Capacity,
    #[error("publication belongs to another repository coordinator")]
    Foreign,
    #[error("logical publication already admitted")]
    Duplicate,
    #[error("publication does not have an unresolved outcome")]
    NotUncertain,
    #[error("publication is no longer held without execution")]
    NotHeld,
}
pub struct PublicationAdmissionFailure {
    pub reason: PublicationScheduleError,
    pub ready: ReadyPublication,
}
impl std::fmt::Debug for PublicationAdmissionFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PublicationAdmissionFailure")
            .field("reason", &self.reason)
            .finish_non_exhaustive()
    }
}
impl std::fmt::Display for PublicationAdmissionFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.reason.fmt(f)
    }
}
impl std::error::Error for PublicationAdmissionFailure {}

#[derive(Clone, Debug)]
pub enum PublicationState {
    /// Admitted and charged, but cannot execute until explicitly activated.
    Held,
    Queued,
    Running,
    /// Retained in the coordinator and charged until explicit resolution.
    Uncertain(Arc<PublicationError>),
    /// Rejected carries its durable receipt; NotStarted never acknowledges.
    Finished(Result<PublicationOutcome, Arc<PublicationError>>),
    /// Held proof dropped before returning credits; no command was dispatched.
    Discarded,
}
impl PublicationState {
    fn observed(&self) -> bool {
        matches!(
            self,
            Self::Uncertain(_) | Self::Finished(_) | Self::Discarded
        )
    }
}
struct ReadContext {
    client: CellClient,
    target: CellTarget,
    request: BeginRequest,
}
struct Job {
    operation: [u8; 16],
    actor: String,
    // Removed before terminal notification; tickets never retain command
    // payloads or local inventory after the admission charge is released.
    ready: Mutex<Option<ReadyPublication>>,
    class: PublicationClass,
    reservation: u64,
    policy_page: bool,
    root_refusal: bool,
    status: watch::Sender<PublicationState>,
    read: ReadContext,
    admitted: Instant,
}
struct Work {
    job: Arc<Job>,
    recover: bool,
    queued: Instant,
}
struct FairQueue<T> {
    actors: VecDeque<String>,
    queues: HashMap<String, VecDeque<T>>,
}
impl<T> Default for FairQueue<T> {
    fn default() -> Self {
        Self {
            actors: VecDeque::new(),
            queues: HashMap::new(),
        }
    }
}
impl<T> FairQueue<T> {
    fn push(&mut self, actor: String, work: T) {
        let queue = self.queues.entry(actor.clone()).or_default();
        if queue.is_empty() {
            self.actors.push_back(actor);
        }
        queue.push_back(work);
    }
    fn pop(&mut self) -> Option<T> {
        let actor = self.actors.pop_front()?;
        let queue = self.queues.get_mut(&actor)?;
        let work = queue.pop_front();
        if queue.is_empty() {
            self.queues.remove(&actor);
        } else {
            self.actors.push_back(actor);
        }
        work
    }
}
struct ClassQueue<T> {
    foreground: FairQueue<T>,
    maintenance: FairQueue<T>,
    foreground_streak: u8,
}
impl<T> Default for ClassQueue<T> {
    fn default() -> Self {
        Self {
            foreground: FairQueue::default(),
            maintenance: FairQueue::default(),
            foreground_streak: 0,
        }
    }
}
impl<T> ClassQueue<T> {
    fn push(&mut self, class: PublicationClass, actor: String, work: T) {
        match class {
            PublicationClass::Foreground => self.foreground.push(actor, work),
            PublicationClass::Maintenance => self.maintenance.push(actor, work),
        }
    }
    fn pop(&mut self, maintenance_ready: bool, burst: u8) -> Option<T> {
        if maintenance_ready
            && self.foreground_streak >= burst
            && let Some(work) = self.maintenance.pop()
        {
            self.foreground_streak = 0;
            return Some(work);
        }
        if let Some(work) = self.foreground.pop() {
            self.foreground_streak = self.foreground_streak.saturating_add(1);
            return Some(work);
        }
        if maintenance_ready && let Some(work) = self.maintenance.pop() {
            self.foreground_streak = 0;
            return Some(work);
        }
        None
    }
}
#[derive(Default)]
struct State {
    jobs: HashMap<[u8; 16], Arc<Job>>,
    actors: HashMap<String, [usize; 2]>,
    queue: ClassQueue<Work>,
    counts: [usize; 2],
    bytes: [u64; 2],
    worker: bool,
    closed: bool,
}
struct Inner {
    target: CellTarget,
    limits: PublicationLimits,
    state: Mutex<State>,
    drained: Notify,
    changed: Notify,
    #[cfg(test)]
    gate: Mutex<Option<TestGate>>,
    #[cfg(test)]
    fault: std::sync::atomic::AtomicU8,
}
#[cfg(test)]
struct TestGate {
    entered: tokio::sync::oneshot::Sender<()>,
    release: tokio::sync::oneshot::Receiver<()>,
}

/// Keep one service-owned instance per repository. Dropping a waiter does not
/// cancel an admitted command. Close/drain on shutdown and retain the returned
/// unresolved tickets for outcome recovery; this is not a durable local outbox.
#[derive(Clone)]
pub struct PublicationCoordinator {
    inner: Arc<Inner>,
}
#[must_use]
#[derive(Clone)]
pub struct PublicationTicket {
    inner: Arc<Inner>,
    job: Arc<Job>,
}
/// Bounded service snapshot, including uncertain work that is still charged.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PublicationStats {
    pub admitted: usize,
    pub accounts: usize,
    pub held: usize,
    pub queued: usize,
    pub in_flight: usize,
    pub uncertain: usize,
    pub command_bytes: u64,
    pub closed: bool,
    pub foreground: usize,
    pub maintenance: usize,
}
impl PublicationCoordinator {
    pub fn new(
        target: CellTarget,
        limits: PublicationLimits,
    ) -> Result<Self, PublicationScheduleError> {
        limits.validate()?;
        Ok(Self {
            inner: Arc::new(Inner {
                target,
                limits,
                state: Mutex::new(State::default()),
                drained: Notify::new(),
                changed: Notify::new(),
                #[cfg(test)]
                gate: Mutex::new(None),
                #[cfg(test)]
                fault: std::sync::atomic::AtomicU8::new(0),
            }),
        })
    }
    pub(in crate::packs::publication) fn matches_target(&self, target: &CellTarget) -> bool {
        self.inner.target == *target
    }
    /// Non-waiting admission. All unbounded/native work precedes this call.
    /// The account key comes from the private lease, never a request label.
    pub async fn submit(
        &self,
        ready: impl Into<ReadyPublication>,
    ) -> Result<PublicationTicket, Box<PublicationAdmissionFailure>> {
        let ready = ready.into();
        let mut state = self.inner.state.lock().await;
        self.admit(&mut state, ready, false)
    }
    /// Synchronous ownership handoff without dispatch. A lifecycle can retain
    /// this ticket before yielding, so cancellation cannot strand an unowned
    /// admission. Contended admission returns the original ready value.
    pub fn try_reserve(
        &self,
        ready: impl Into<ReadyPublication>,
    ) -> Result<PublicationTicket, Box<PublicationAdmissionFailure>> {
        let ready = ready.into();
        let Ok(mut state) = self.inner.state.try_lock() else {
            return Err(Box::new(PublicationAdmissionFailure {
                reason: PublicationScheduleError::Capacity,
                ready,
            }));
        };
        self.admit(&mut state, ready, true)
    }
    fn admit(
        &self,
        state: &mut State,
        ready: ReadyPublication,
        held: bool,
    ) -> Result<PublicationTicket, Box<PublicationAdmissionFailure>> {
        let class = ready.class();
        let reservation = ready.reservation();
        let at = class.index();
        let (client, target, check) = ready.capability();
        let limits = self.inner.limits;
        let (operation_limit, byte_limit) = match class {
            PublicationClass::Foreground => (
                limits.operations - limits.maintenance_operations,
                limits.command_bytes
                    - limits.maintenance_operations as u64 * MAINTENANCE_RESERVATION,
            ),
            PublicationClass::Maintenance => (
                limits.maintenance_operations,
                limits.maintenance_operations as u64 * MAINTENANCE_RESERVATION,
            ),
        };
        let reason = if target != &self.inner.target {
            Some(PublicationScheduleError::Foreign)
        } else if state.closed {
            Some(PublicationScheduleError::Closed)
        } else if state.jobs.contains_key(&check.token.operation) {
            Some(PublicationScheduleError::Duplicate)
        } else if state.counts[at] >= operation_limit
            || state
                .actors
                .get(&check.actor)
                .map_or(0, |counts| counts[at])
                >= limits.per_actor
            || state.bytes[at] > byte_limit - reservation
        {
            Some(PublicationScheduleError::Capacity)
        } else {
            None
        };
        if let Some(reason) = reason {
            return Err(Box::new(PublicationAdmissionFailure { reason, ready }));
        }
        let read = ReadContext {
            client: client.clone(),
            target: target.clone(),
            request: BeginRequest {
                repository: check.token.repository,
                operation: check.token.operation,
                request_digest: check.token.request_digest,
                actor: check.actor.clone(),
                lease_ms: DEFAULT_LEASE_MS,
            },
        };
        let job = Arc::new(Job {
            operation: check.token.operation,
            actor: check.actor.clone(),
            class,
            reservation,
            policy_page: ready.is_policy_page(),
            root_refusal: ready.is_root_refusal(),
            ready: Mutex::new(Some(ready)),
            status: watch::channel(if held {
                PublicationState::Held
            } else {
                PublicationState::Queued
            })
            .0,
            read,
            admitted: Instant::now(),
        });
        state.actors.entry(job.actor.clone()).or_default()[at] += 1;
        state.counts[at] += 1;
        state.bytes[at] += job.reservation;
        state.jobs.insert(job.operation, Arc::clone(&job));
        if !held {
            enqueue(state, &job, false);
            self.start(state);
            self.inner.changed.notify_one();
        }
        Ok(PublicationTicket {
            inner: Arc::clone(&self.inner),
            job,
        })
    }
    /// Recover only the retained exact command. A fresh response/proof factory
    /// cannot replace it while acceptance is unknown. Recovery joins the fair
    /// queue; an unknown/expired/unreachable resolution retains its reservation.
    pub async fn recover(
        &self,
        ticket: &PublicationTicket,
    ) -> Result<(), PublicationScheduleError> {
        if !Arc::ptr_eq(&self.inner, &ticket.inner) {
            return Err(PublicationScheduleError::Foreign);
        }
        let mut state = self.inner.state.lock().await;
        if !state
            .jobs
            .get(&ticket.job.operation)
            .is_some_and(|job| Arc::ptr_eq(job, &ticket.job))
            || !matches!(*ticket.job.status.borrow(), PublicationState::Uncertain(_))
        {
            return Err(PublicationScheduleError::NotUncertain);
        }
        ticket.job.status.send_replace(PublicationState::Queued);
        enqueue(&mut state, &ticket.job, true);
        self.start(&mut state);
        self.inner.changed.notify_one();
        Ok(())
    }
    fn start(&self, state: &mut State) {
        if !state.worker {
            state.worker = true;
            tokio::spawn(supervise(Arc::clone(&self.inner)));
        }
    }
    /// The retirement supervisor may recover only factory-owned terminal
    /// release commands. A committed release has removed its pin, so lease
    /// discovery alone cannot find this still charged original command.
    pub(in crate::packs::publication) async fn recover_terminal_releases(
        &self,
    ) -> Result<u64, PublicationScheduleError> {
        let jobs: Vec<_> = {
            let state = self.inner.state.lock().await;
            state
                .jobs
                .values()
                .filter(|job| {
                    job.class == PublicationClass::Maintenance
                        && matches!(*job.status.borrow(), PublicationState::Uncertain(_))
                })
                .cloned()
                .collect()
        };
        // Bounded by the existing reserved maintenance admission slots. Never
        // retain a command-body copy, replace an identity or retry compaction.
        let mut recovered = 0;
        for job in jobs {
            let release = matches!(
                &*job.ready.lock().await,
                Some(ReadyPublication::TerminalRelease(_))
            );
            if !release {
                continue;
            }
            let ticket = PublicationTicket {
                inner: Arc::clone(&self.inner),
                job,
            };
            match self.recover(&ticket).await {
                Ok(()) => recovered += 1,
                Err(PublicationScheduleError::NotUncertain) => {}
                Err(error) => return Err(error),
            }
        }
        Ok(recovered)
    }
    /// Stop new work and wait for dispatched commands (without cancellation).
    /// Held activation/discard and recovery remain available after closing.
    /// Close the producer lifecycle first: this does not activate held proofs.
    /// Every returned ticket remains
    /// charged and owns the original command, identity and verification input.
    pub async fn close_and_drain(&self) -> Vec<PublicationTicket> {
        loop {
            let wake = self.inner.drained.notified();
            tokio::pin!(wake);
            wake.as_mut().enable();
            {
                let mut state = self.inner.state.lock().await;
                state.closed = true;
                if !state.worker {
                    return state
                        .jobs
                        .values()
                        .map(|job| PublicationTicket {
                            inner: Arc::clone(&self.inner),
                            job: Arc::clone(job),
                        })
                        .collect();
                }
            }
            wake.await;
        }
    }
    /// Service-internal lookup after its caller loses a ticket. This is not an
    /// externally authorized product query; use completed-request replay there.
    pub async fn pending(&self, operation: [u8; 16]) -> Option<PublicationTicket> {
        self.inner
            .state
            .lock()
            .await
            .jobs
            .get(&operation)
            .map(|job| PublicationTicket {
                inner: Arc::clone(&self.inner),
                job: Arc::clone(job),
            })
    }
    pub async fn stats(&self) -> PublicationStats {
        let state = self.inner.state.lock().await;
        let mut stats = PublicationStats {
            admitted: state.jobs.len(),
            accounts: state.actors.len(),
            held: 0,
            queued: 0,
            in_flight: 0,
            uncertain: 0,
            command_bytes: state.bytes.iter().sum(),
            closed: state.closed,
            foreground: state.counts[0],
            maintenance: state.counts[1],
        };
        for job in state.jobs.values() {
            match *job.status.borrow() {
                PublicationState::Held => stats.held += 1,
                PublicationState::Queued => stats.queued += 1,
                PublicationState::Running => stats.in_flight += 1,
                PublicationState::Uncertain(_) => stats.uncertain += 1,
                PublicationState::Finished(_) | PublicationState::Discarded => {}
            }
        }
        stats
    }
    #[cfg(test)]
    pub(super) async fn pause_for_test(
        &self,
    ) -> (
        tokio::sync::oneshot::Sender<()>,
        tokio::sync::oneshot::Receiver<()>,
    ) {
        let (release, wait) = tokio::sync::oneshot::channel();
        let (entered, start) = tokio::sync::oneshot::channel();
        *self.inner.gate.lock().await = Some(TestGate {
            entered,
            release: wait,
        });
        (release, start)
    }
    #[cfg(test)]
    pub(super) fn fault_for_test(&self, fault: u8) {
        self.inner
            .fault
            .store(fault, std::sync::atomic::Ordering::Release);
    }
    #[cfg(test)]
    pub(super) async fn reservations_for_test(&self) -> (usize, u64, usize) {
        let state = self.inner.state.lock().await;
        (
            state.jobs.len(),
            state.bytes.iter().sum(),
            state.actors.len(),
        )
    }
    #[cfg(test)]
    pub(super) async fn with_admission_for_test<T>(&self, inspect: impl FnOnce() -> T) -> T {
        let _state = self.inner.state.lock().await;
        inspect()
    }
}
impl PublicationTicket {
    pub(in crate::packs::publication) fn is_policy_page(&self) -> bool {
        self.job.policy_page
    }
    pub(in crate::packs::publication) fn is_root_refusal(&self) -> bool {
        self.job.root_refusal
    }
    /// Join the existing fair queue once. Idempotence lets a recovering
    /// lifecycle observe an already activated command without recreating it.
    /// Existing admission can activate after coordinator close.
    pub async fn activate(&self) -> Result<(), PublicationScheduleError> {
        let mut state = self.inner.state.lock().await;
        let held = matches!(self.state(), PublicationState::Held);
        if matches!(self.state(), PublicationState::Discarded) {
            return Err(PublicationScheduleError::NotHeld);
        }
        if !held {
            return Ok(());
        }
        self.job.status.send_replace(PublicationState::Queued);
        enqueue(&mut state, &self.job, false);
        PublicationCoordinator {
            inner: Arc::clone(&self.inner),
        }
        .start(&mut state);
        self.inner.changed.notify_one();
        Ok(())
    }
    /// Only a proven unexecuted held command can be discarded. Holding the
    /// coordinator state lock serializes this with activation; drop private
    /// proof resources before returning their admission credits.
    pub async fn discard_held(&self) -> Result<(), PublicationScheduleError> {
        let mut state = self.inner.state.lock().await;
        if !matches!(self.state(), PublicationState::Held) {
            return Err(PublicationScheduleError::NotHeld);
        }
        self.job.ready.lock().await.take();
        release(&mut state, &self.job);
        self.job.status.send_replace(PublicationState::Discarded);
        self.inner.drained.notify_waiters();
        Ok(())
    }
    /// Restart discovery may retry only the cold capability it actually found.
    /// A live lifecycle or another attempt with the same logical ID keeps its
    /// original observer, session and recovery ownership.
    pub(in crate::packs::publication) async fn recover_discovered(
        &self,
        registered: &RegisteredRootRecovery,
    ) -> Result<bool, PublicationScheduleError> {
        let matches = {
            let ready = self.job.ready.lock().await;
            matches!(&*ready, Some(ReadyPublication::RootRecovery(value)) if value.matches_registered(registered))
        };
        if !matches || !matches!(self.state(), PublicationState::Uncertain(_)) {
            return Ok(false);
        }
        match self.recover().await {
            Ok(()) => Ok(true),
            // The original observer may have requested recovery meanwhile.
            Err(PublicationScheduleError::NotUncertain) => Ok(false),
            Err(error) => Err(error),
        }
    }
    #[cfg(test)]
    pub(in crate::packs::publication) async fn terminal_release_for_test(
        &self,
    ) -> Option<ReadyTerminalRelease> {
        match &*self.job.ready.lock().await {
            Some(ReadyPublication::TerminalRelease(ready)) => Some((**ready).clone()),
            _ => None,
        }
    }
    pub async fn recover(&self) -> Result<(), PublicationScheduleError> {
        PublicationCoordinator {
            inner: Arc::clone(&self.inner),
        }
        .recover(self)
        .await
    }
    pub(super) async fn wait_recovered(&self) {
        let mut status = self.job.status.subscribe();
        while matches!(*status.borrow_and_update(), PublicationState::Uncertain(_)) {
            if status.changed().await.is_err() {
                return;
            }
        }
    }
    pub fn class(&self) -> PublicationClass {
        self.job.class
    }
    pub fn state(&self) -> PublicationState {
        self.job.status.borrow().clone()
    }
    /// Cancellation drops only this observation, never admitted execution.
    pub async fn wait(&self) -> PublicationState {
        let mut status = self.job.status.subscribe();
        loop {
            let observed = status.borrow_and_update().clone();
            if observed.observed() {
                return observed;
            }
            if status.changed().await.is_err() {
                return status.borrow().clone();
            }
        }
    }
    pub async fn response(&self) -> Result<GitHttpResponse, CatalogPushResponseError> {
        let completed = match self.state() {
            PublicationState::Finished(Ok(PublicationOutcome::Push(completed))) => completed,
            _ => return Err(CatalogPushResponseError::Invalid),
        };
        let CatalogCompletionReply::Completed(output) = completed.output else {
            return Err(CatalogPushResponseError::Invalid);
        };
        let read = &self.job.read;
        super::completion::load_response(
            &read.client,
            &read.target,
            &read.request,
            completed.receipt,
            output,
        )
        .await
    }
    /// Read only a known root completion, selected again under current read
    /// authorization. Ticket/receipt DTOs never grant artifact read authority.
    pub async fn root_response(
        &self,
        store: &canopy_object_storage::artifact::ArtifactStore,
    ) -> Result<GitHttpResponse<canopy_object_storage::artifact::ArtifactRead>, RootPushReplayError>
    {
        let completed = match self.state() {
            PublicationState::Finished(Ok(PublicationOutcome::RootPush(completed)))
                if matches!(completed.output, RootCompletionReply::Completed(_)) =>
            {
                completed
            }
            _ => return Err(RootPushReplayError::Context),
        };
        let read = &self.job.read;
        replay_root_push_response(
            &read.client,
            &read.target,
            read.request.clone(),
            Some(completed.receipt),
            store,
        )
        .await?
        .ok_or(RootPushReplayError::Context)
    }
}

fn enqueue(state: &mut State, job: &Arc<Job>, recover: bool) {
    state.queue.push(
        job.class,
        job.actor.clone(),
        Work {
            job: Arc::clone(job),
            recover,
            queued: Instant::now(),
        },
    );
}
fn release(state: &mut State, job: &Job) {
    state.jobs.remove(&job.operation);
    let count = state
        .actors
        .get_mut(&job.actor)
        .expect("admitted actor count");
    let at = job.class.index();
    count[at] -= 1;
    if *count == [0, 0] {
        state.actors.remove(&job.actor);
    }
    state.counts[at] -= 1;
    state.bytes[at] -= job.reservation;
}

async fn supervise(inner: Arc<Inner>) {
    loop {
        if tokio::spawn(run(Arc::clone(&inner))).await.is_ok() {
            return;
        }
        // A panicking dispatch task is an uncertain outcome, never evidence
        // that the transport did not accept the mutation. Preserve its exact
        // command, notify observers and keep other ready accounts progressing.
        let state = inner.state.lock().await;
        for job in state.jobs.values() {
            // Release watch's read guard before awaiting or replacing status.
            let running = matches!(*job.status.borrow(), PublicationState::Running);
            if running && let Some(ready) = job.ready.lock().await.as_ref() {
                job.status
                    .send_replace(PublicationState::Uncertain(Arc::new(ready.pending())));
            }
        }
        drop(state);
    }
}
type DispatchResult = Result<PublicationOutcome, PublicationError>;
async fn run(inner: Arc<Inner>) {
    let mut tasks = tokio::task::JoinSet::new();
    let mut active: HashMap<tokio::task::Id, Arc<Job>> = HashMap::new();
    loop {
        let wake = inner.changed.notified();
        tokio::pin!(wake);
        wake.as_mut().enable();
        {
            let mut state = inner.state.lock().await;
            while tasks.len() < inner.limits.in_flight {
                let maintenance_active = active
                    .values()
                    .filter(|job| job.class == PublicationClass::Maintenance)
                    .count();
                let Some(work) = state.queue.pop(
                    maintenance_active < inner.limits.maintenance_in_flight,
                    inner.limits.foreground_burst,
                ) else {
                    break;
                };
                work.job.status.send_replace(PublicationState::Running);
                tracing::debug!(target: "canopy::publication", event = "dispatch",
                    operation = ?work.job.operation, class = ?work.job.class, recovery = work.recover,
                    queue_wait_us = work.queued.elapsed().as_micros());
                let job = Arc::clone(&work.job);
                let handle = tasks.spawn(dispatch(Arc::clone(&inner), work));
                active.insert(handle.id(), job);
            }
            if tasks.is_empty() {
                state.worker = false;
                inner.drained.notify_waiters();
                return;
            }
        }
        // Multiple commands can await durable publication. The Cell's own
        // transaction order and CAS remain authoritative; dispatch order does
        // not promise network arrival order or overlapping push success.
        tokio::select! {
            result = tasks.join_next_with_id() => {
                match result.expect("active publication task") {
                    Ok((id, outcome)) => {
                        let job = active.remove(&id).expect("active publication identity");
                        finish(&inner, &job, outcome).await;
                    }
                    Err(error) => {
                        let job = active.remove(&error.id()).expect("failed publication identity");
                        let error = job.ready.lock().await.as_ref().expect("failed command retained").pending();
                        finish(&inner, &job, Err(error)).await;
                    }
                }
            }
            _ = wake => {}
        }
    }
}
async fn dispatch(inner: Arc<Inner>, work: Work) -> DispatchResult {
    #[cfg(test)]
    {
        // Do not hold the hook's mutex across a wait: other dispatched jobs
        // must be able to progress while the first transport is suspended.
        let gate = inner.gate.lock().await.take();
        if let Some(gate) = gate {
            let _ = gate.entered.send(());
            let _ = gate.release.await;
        }
    }
    #[cfg(not(test))]
    drop(inner);
    let ready = work
        .job
        .ready
        .lock()
        .await
        .as_ref()
        .expect("admitted publication retains its command")
        .dispatch_copy();
    #[cfg(test)]
    let fault = inner.fault.swap(0, std::sync::atomic::Ordering::AcqRel);
    #[cfg(not(test))]
    let fault = 0;
    ready.dispatch(work.recover, fault).await
}
async fn finish(inner: &Inner, job: &Job, outcome: DispatchResult) {
    let disposition = outcome
        .as_ref()
        .err()
        .map_or("committed", PublicationError::disposition);
    tracing::debug!(target: "canopy::publication", event = "observed", operation = ?job.operation,
        class = ?job.class, disposition, residence_us = job.admitted.elapsed().as_micros());
    let uncertain = outcome
        .as_ref()
        .err()
        .is_some_and(PublicationError::uncertain);
    if uncertain {
        job.status
            .send_replace(PublicationState::Uncertain(Arc::new(outcome.unwrap_err())));
    } else {
        // Drop large resources before making their admission reusable.
        job.ready.lock().await.take();
        let mut state = inner.state.lock().await;
        release(&mut state, job);
        job.status
            .send_replace(PublicationState::Finished(outcome.map_err(Arc::new)));
    }
}

#[cfg(test)]
mod fairness {
    use super::*;
    #[test]
    fn limits_leave_room_for_another_account_and_bound_dispatch_concurrency() {
        assert!(PublicationLimits::default().validate().is_ok());
        for limits in [
            PublicationLimits {
                operations: 0,
                ..PublicationLimits::default()
            },
            PublicationLimits {
                per_actor: 32,
                ..PublicationLimits::default()
            },
            PublicationLimits {
                command_bytes: 8 << 20,
                ..PublicationLimits::default()
            },
            PublicationLimits {
                command_bytes: 64 << 20,
                ..PublicationLimits::default()
            },
            PublicationLimits {
                in_flight: 0,
                ..PublicationLimits::default()
            },
            PublicationLimits {
                in_flight: 33,
                ..PublicationLimits::default()
            },
            PublicationLimits {
                maintenance_operations: 0,
                ..PublicationLimits::default()
            },
            PublicationLimits {
                maintenance_operations: 32,
                ..PublicationLimits::default()
            },
            PublicationLimits {
                maintenance_in_flight: 0,
                ..PublicationLimits::default()
            },
            PublicationLimits {
                maintenance_in_flight: 8,
                ..PublicationLimits::default()
            },
            PublicationLimits {
                foreground_burst: 0,
                ..PublicationLimits::default()
            },
            PublicationLimits {
                foreground_burst: 33,
                ..PublicationLimits::default()
            },
        ] {
            assert_eq!(
                limits.validate(),
                Err(PublicationScheduleError::InvalidLimits)
            );
        }
    }
    #[test]
    fn ready_accounts_rotate_and_each_account_preserves_fifo() {
        let mut queue = FairQueue::default();
        for n in 0..8 {
            queue.push("busy".into(), n);
        }
        queue.push("other".into(), 100);
        queue.push("third".into(), 200);
        assert_eq!(queue.pop(), Some(0));
        // Busy traffic cannot move itself ahead of already ready accounts.
        queue.push("busy".into(), 8);
        queue.push("other".into(), 101);
        assert_eq!(
            (queue.pop(), queue.pop(), queue.pop(), queue.pop()),
            (Some(100), Some(200), Some(1), Some(101))
        );
        for n in 2..9 {
            assert_eq!(queue.pop(), Some(n));
        }
        assert_eq!(queue.pop(), None);
        assert!(queue.actors.is_empty() && queue.queues.is_empty());
        queue.push("busy".into(), 9);
        assert_eq!(queue.pop(), Some(9));
    }
    #[test]
    fn ready_classes_bound_foreground_bursts_and_preserve_blocked_maintenance() {
        let mut queue = ClassQueue::default();
        for n in 0..12 {
            queue.push(PublicationClass::Foreground, "busy".into(), n);
        }
        queue.push(PublicationClass::Maintenance, "admin-a".into(), 100);
        queue.push(PublicationClass::Maintenance, "admin-b".into(), 200);
        assert_eq!(
            (queue.pop(true, 3), queue.pop(true, 3), queue.pop(true, 3)),
            (Some(0), Some(1), Some(2))
        );
        // A running maintenance job at its concurrency cap must not block ready
        // foreground work or remove the waiting maintenance command.
        assert_eq!(queue.pop(false, 3), Some(3));
        assert_eq!(queue.pop(true, 3), Some(100));
        assert_eq!(
            (
                queue.pop(true, 3),
                queue.pop(true, 3),
                queue.pop(true, 3),
                queue.pop(true, 3)
            ),
            (Some(4), Some(5), Some(6), Some(200))
        );
        for n in 7..12 {
            assert_eq!(queue.pop(true, 3), Some(n));
        }
        assert_eq!(queue.pop(true, 3), None);
        queue.push(PublicationClass::Maintenance, "admin-a".into(), 101);
        assert_eq!(queue.pop(false, 3), None);
        assert_eq!(queue.pop(true, 3), Some(101));
    }
}
