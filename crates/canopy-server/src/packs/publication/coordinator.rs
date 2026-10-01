//! Bounded, account-fair dispatch of already prepared final commands.
//! Catalog reconciliation, uploads and ref proof construction happen before
//! dispatch. This primitive does not guarantee progress against a moving root;
//! frontier pipelining and production orchestration are separate requirements.
use super::*;
use crate::{git_http::GitHttpResponse, packs::metadata::MetadataLimits};
use cellule_ltx::DiskBudget;
use cellule_runtime::{
    CellClient, CellTarget, Committed, InvocationError, MutationIdentity, PreparedCommand, Receipt,
    Resolution, cell::executor::StoredOutcome,
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

#[derive(Clone, Copy, Debug)]
pub struct PublicationLimits {
    /// Includes queued, executing and uncertain operations.
    pub operations: usize,
    pub per_actor: usize,
    pub command_bytes: u64,
    /// Commands awaiting durable outcome, not concurrent Cell transactions.
    pub in_flight: usize,
}
impl Default for PublicationLimits {
    fn default() -> Self {
        Self {
            operations: 32,
            per_actor: 8,
            command_bytes: 256 << 20,
            in_flight: 8,
        }
    }
}
impl PublicationLimits {
    fn validate(self) -> Result<(), PublicationScheduleError> {
        if self.operations < 2
            || self.operations > MAX_OPERATIONS as usize
            || self.per_actor == 0
            || self.per_actor >= self.operations
            || self.command_bytes / COMMAND_RESERVATION <= self.per_actor as u64
            || self.in_flight == 0
            || self.in_flight > self.operations
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
    prepared: Arc<PreparedCatalog>,
    command: PreparedCommand<CompleteCatalogPush>,
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
            prepared: Arc::clone(self),
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
}
pub struct PublicationAdmissionFailure {
    pub reason: PublicationScheduleError,
    pub ready: ReadyCatalogPush,
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
    Queued,
    Running,
    /// Retained in the coordinator and charged until explicit resolution.
    Uncertain(Arc<InvocationError<CatalogCompletionReply>>),
    /// Rejected carries its durable receipt; NotStarted never acknowledges.
    Finished(
        Result<Committed<CatalogCompletionReply>, Arc<InvocationError<CatalogCompletionReply>>>,
    ),
}
impl PublicationState {
    fn observed(&self) -> bool {
        matches!(self, Self::Uncertain(_) | Self::Finished(_))
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
    ready: Mutex<Option<ReadyCatalogPush>>,
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
#[derive(Default)]
struct State {
    jobs: HashMap<[u8; 16], Arc<Job>>,
    actors: HashMap<String, usize>,
    queue: FairQueue<Work>,
    bytes: u64,
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
    pub queued: usize,
    pub in_flight: usize,
    pub uncertain: usize,
    pub command_bytes: u64,
    pub closed: bool,
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
    /// Non-waiting admission. All unbounded/native work precedes this call.
    /// The account key comes from the private lease, never a request label.
    pub async fn submit(
        &self,
        ready: ReadyCatalogPush,
    ) -> Result<PublicationTicket, Box<PublicationAdmissionFailure>> {
        let (client, target, check) = ready.prepared.base.capability();
        let mut state = self.inner.state.lock().await;
        let limits = self.inner.limits;
        let reason = if target != &self.inner.target {
            Some(PublicationScheduleError::Foreign)
        } else if state.closed {
            Some(PublicationScheduleError::Closed)
        } else if state.jobs.contains_key(&check.token.operation) {
            Some(PublicationScheduleError::Duplicate)
        } else if state.jobs.len() >= limits.operations
            || state.actors.get(&check.actor).copied().unwrap_or(0) >= limits.per_actor
            || state.bytes > limits.command_bytes - COMMAND_RESERVATION
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
            ready: Mutex::new(Some(ready)),
            status: watch::channel(PublicationState::Queued).0,
            read,
            admitted: Instant::now(),
        });
        *state.actors.entry(job.actor.clone()).or_default() += 1;
        state.bytes += COMMAND_RESERVATION;
        state.jobs.insert(job.operation, Arc::clone(&job));
        state.queue.push(
            job.actor.clone(),
            Work {
                job: Arc::clone(&job),
                recover: false,
                queued: Instant::now(),
            },
        );
        self.start(&mut state);
        self.inner.changed.notify_one();
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
        state.queue.push(
            ticket.job.actor.clone(),
            Work {
                job: Arc::clone(&ticket.job),
                recover: true,
                queued: Instant::now(),
            },
        );
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
    /// Stop new work and wait for dispatched commands (without cancellation).
    /// Recovery remains available after closing. Every returned ticket remains
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
            queued: 0,
            in_flight: 0,
            uncertain: 0,
            command_bytes: state.bytes,
            closed: state.closed,
        };
        for job in state.jobs.values() {
            match *job.status.borrow() {
                PublicationState::Queued => stats.queued += 1,
                PublicationState::Running => stats.in_flight += 1,
                PublicationState::Uncertain(_) => stats.uncertain += 1,
                PublicationState::Finished(_) => {}
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
        (state.jobs.len(), state.bytes, state.actors.len())
    }
}
impl PublicationTicket {
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
            PublicationState::Finished(Ok(completed)) => completed,
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
                    .send_replace(PublicationState::Uncertain(Arc::new(
                        InvocationError::Pending(Box::new(ready.command.evidence().clone())),
                    )));
            }
        }
        drop(state);
    }
}
type DispatchResult =
    Result<Committed<CatalogCompletionReply>, InvocationError<CatalogCompletionReply>>;
async fn run(inner: Arc<Inner>) {
    let mut tasks = tokio::task::JoinSet::new();
    let mut active = HashMap::new();
    loop {
        let wake = inner.changed.notified();
        tokio::pin!(wake);
        wake.as_mut().enable();
        {
            let mut state = inner.state.lock().await;
            while tasks.len() < inner.limits.in_flight {
                let Some(work) = state.queue.pop() else { break };
                work.job.status.send_replace(PublicationState::Running);
                tracing::debug!(target: "canopy::publication", event = "dispatch",
                    operation = ?work.job.operation, recovery = work.recover,
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
                        let evidence = job.ready.lock().await.as_ref().expect("failed command retained").command.evidence().clone();
                        finish(&inner, &job, Err(InvocationError::Pending(Box::new(evidence)))).await;
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
    let command = {
        let ready = work.job.ready.lock().await;
        ready
            .as_ref()
            .expect("admitted publication retains its command")
            .command
            .clone()
    };
    #[cfg(test)]
    let fault = inner.fault.swap(0, std::sync::atomic::Ordering::AcqRel);
    #[cfg(test)]
    let evidence = command.evidence().clone();
    #[cfg(test)]
    let outcome = if fault == 1 {
        Err(InvocationError::Pending(Box::new(evidence.clone())))
    } else if work.recover {
        resolve(&work.job.read.client, command).await
    } else {
        Box::pin(command.execute()).await
    };
    #[cfg(not(test))]
    let outcome = if work.recover {
        resolve(&work.job.read.client, command).await
    } else {
        Box::pin(command.execute()).await
    };
    #[cfg(test)]
    let outcome = if fault == 2 {
        Err(InvocationError::Pending(Box::new(evidence)))
    } else {
        assert_ne!(fault, 3, "injected dispatch panic after execution");
        outcome
    };
    outcome
}
async fn finish(inner: &Inner, job: &Job, outcome: DispatchResult) {
    let disposition = match &outcome {
        Ok(_) => "committed",
        Err(InvocationError::Rejected(_)) => "rejected",
        Err(InvocationError::NotStarted(_)) => "not_started",
        Err(InvocationError::Pending(_)) => "pending",
        Err(InvocationError::InvalidPublishedResult { .. }) => "invalid_published_result",
    };
    tracing::debug!(target: "canopy::publication", event = "observed", operation = ?job.operation,
        disposition, residence_us = job.admitted.elapsed().as_micros());
    let uncertain = matches!(
        &outcome,
        Err(InvocationError::Pending(_) | InvocationError::InvalidPublishedResult { .. })
    );
    if uncertain {
        job.status
            .send_replace(PublicationState::Uncertain(Arc::new(outcome.unwrap_err())));
    } else {
        // Drop large resources before making their admission reusable.
        job.ready.lock().await.take();
        let mut state = inner.state.lock().await;
        state.jobs.remove(&job.operation);
        let count = state
            .actors
            .get_mut(&job.actor)
            .expect("admitted actor count");
        *count -= 1;
        if *count == 0 {
            state.actors.remove(&job.actor);
        }
        state.bytes -= COMMAND_RESERVATION;
        job.status
            .send_replace(PublicationState::Finished(outcome.map_err(Arc::new)));
    }
}

async fn resolve(
    client: &CellClient,
    command: PreparedCommand<CompleteCatalogPush>,
) -> Result<Committed<CatalogCompletionReply>, InvocationError<CatalogCompletionReply>> {
    let evidence = command.evidence().clone();
    match client.resolve(&evidence).await {
        Ok(Resolution::Absent) => Box::pin(command.execute()).await,
        Ok(Resolution::Committed(outcome)) => {
            let receipt = Receipt {
                cell: evidence.target().cell_id(),
                incarnation: evidence.incarnation(),
                commit_sequence: outcome.commit_sequence(),
            };
            let decoded = (|| {
                let mut decoder = BoundedDecoder::new(outcome.result(), 128)?;
                let output = CatalogCompletionReply::decode(&mut decoder)?;
                decoder.finish()?;
                Ok::<_, CodecError>(Committed { output, receipt })
            })()
            .map_err(|source| InvocationError::InvalidPublishedResult {
                receipt,
                source: Box::new(source.into()),
            })?;
            match outcome {
                StoredOutcome::Success { .. } => Ok(decoded),
                StoredOutcome::Rejected { .. } => Err(InvocationError::Rejected(Box::new(decoded))),
            }
        }
        // Unknown, expiration and changed incarnation never prove that an
        // earlier submission failed. Keep exact evidence for logical recovery.
        Ok(Resolution::Unknown | Resolution::Expired) | Err(_) => {
            Err(InvocationError::Pending(Box::new(evidence)))
        }
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
}
