//! Service-owned, bounded input custody. Never infer a live deadline from a
//! replayed command, discard ambiguous evidence, or let an observer cancel work.
use super::*;
use crate::packs::catalog::{CatalogFiles, CatalogIndexes};
use cellule_runtime::{
    CellClient, CellTarget, Committed, InvocationError, MutationIdentity, PreparedCommand, Receipt,
};
use std::{
    any::Any,
    collections::HashMap,
    future::Future,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    sync::{Notify, OwnedSemaphorePermit, Semaphore, watch},
    time::{Instant, sleep_until},
};

mod bound;
use bound::accept_bound;
mod publication;
pub use publication::{StagedPublicationFailure, StagedPublicationTicket};

const COMMAND_BYTES: u32 = 4096;
#[derive(Clone, Copy, Debug)]
pub struct StagingLimits {
    pub operations: usize,
    pub per_actor: usize,
    pub workers: usize,
    pub workers_per_actor: usize,
    pub lease_ms: u64,
    pub renew_before_ms: u64,
    pub lifetime_ms: u64,
    pub bound_lifetime_ms: u64,
}
impl Default for StagingLimits {
    fn default() -> Self {
        Self {
            operations: 32,
            per_actor: 8,
            workers: 64,
            workers_per_actor: 8,
            lease_ms: DEFAULT_LEASE_MS,
            renew_before_ms: DEFAULT_LEASE_MS / 2,
            lifetime_ms: 4 * 60 * 60 * 1000,
            bound_lifetime_ms: DEFAULT_LEASE_MS,
        }
    }
}
impl StagingLimits {
    fn validate(self) -> Result<(), StagingError> {
        if self.operations < 2
            || self.operations > MAX_OPERATIONS as usize
            || self.per_actor == 0
            || self.per_actor >= self.operations
            || self.workers == 0
            || self.workers > MAX_GENERATION_LEASES as usize
            || self.workers_per_actor == 0
            || self.workers_per_actor > self.workers
            || self.lease_ms == 0
            || self.lease_ms > MAX_LEASE_MS
            || self.renew_before_ms == 0
            || self.renew_before_ms >= self.lease_ms
            || self.lifetime_ms < self.lease_ms
            || self.lifetime_ms > 24 * 60 * 60 * 1000
            || self.bound_lifetime_ms == 0
            || self.bound_lifetime_ms > MAX_LEASE_MS
        {
            return Err(StagingError::InvalidLimits);
        }
        Ok(())
    }
}
#[derive(Debug, thiserror::Error)]
pub enum StagingError {
    #[error("invalid staging limits")]
    InvalidLimits,
    #[error("staging admission is closed")]
    Closed,
    #[error("staging admission capacity exceeded")]
    Capacity,
    #[error("staging request belongs to another coordinator")]
    Foreign,
    #[error("logical staging request already admitted")]
    Duplicate,
    #[error("staging has no live matching custody")]
    Inactive,
    #[error("staging is not ready for this action")]
    NotReady,
    #[error("staging context differs")]
    Context,
    #[error("staging clock failed")]
    Clock,
    #[error("staging worker panicked")]
    Worker,
    #[error("input preparation failed")]
    Input(#[source] Box<dyn std::error::Error + Send + Sync>),
    #[error("staging begin failed")]
    Begin(#[source] Box<InvocationError<StagingReply>>),
    #[error("staging claim failed")]
    Claim(#[source] Box<InvocationError<StagingReply>>),
    #[error("input checkpoint registration failed")]
    Checkpoint(#[source] Box<InvocationError<StagingReply>>),
    #[error("staging renewal failed")]
    Renew(#[source] Box<InvocationError<StagingReply>>),
    #[error("bound preparation renewal failed")]
    BoundRenew(#[source] Box<InvocationError<PreparationReply>>),
    #[error("bound preparation claim failed")]
    BoundClaim(#[source] Box<InvocationError<PreparationReply>>),
    #[error("staging bind failed")]
    Bind(#[source] Box<InvocationError<PreparationReply>>),
    #[error("staging query failed")]
    Query(#[source] Box<InvocationError<Option<StagingLease>>>),
    #[error("bound base failed")]
    Base(#[from] PreparationBaseError),
    #[error("final publication admission failed: {0}")]
    PublicationAdmission(PublicationScheduleError),
    #[error("final publication failed: {0}")]
    Publication(#[source] Arc<PublicationError>),
}
impl StagingError {
    fn uncertain(&self) -> bool {
        fn unknown<T>(e: &InvocationError<T>) -> bool {
            matches!(
                e,
                InvocationError::Pending(_) | InvocationError::InvalidPublishedResult { .. }
            )
        }
        match self {
            Self::Begin(e) | Self::Renew(e) | Self::Claim(e) | Self::Checkpoint(e) => unknown(e),
            Self::Bind(e) | Self::BoundRenew(e) | Self::BoundClaim(e) => unknown(e),
            _ => false,
        }
    }
}
#[derive(Clone, Copy, Debug)]
pub struct StagingBound {
    pub lease: PreparationLease,
    pub receipt: Receipt,
}
#[derive(Clone, Debug)]
pub enum StagingState {
    Starting,
    Active(StagingLease),
    Draining(StagingLease),
    Binding,
    RegisteringInputs,
    Resolving,
    Uncertain(Arc<StagingError>),
    Bound(Arc<StagingBound>),
    Finishing,
    Publishing,
    /// Original final outcome; no post-completion lease query can erase it.
    Published(Result<PublicationOutcome, Arc<PublicationError>>),
    Fenced(Arc<StagingError>),
    Stopped,
}
impl StagingState {
    fn terminal(&self) -> bool {
        matches!(
            self,
            Self::Bound(_) | Self::Published(_) | Self::Fenced(_) | Self::Stopped
        )
    }
}
#[must_use]
pub struct ReadyStaging {
    inner: Box<StagingRequest>,
}
struct StagingRequest {
    client: CellClient,
    target: CellTarget,
    request: BeginRequest,
    command: Exact,
    bound_source: Option<LeaseCheck>,
}
impl ReadyStaging {
    pub async fn new(
        client: CellClient,
        target: CellTarget,
        request: BeginRequest,
        identity: MutationIdentity,
    ) -> Result<Self, StagingError> {
        if crate::repository_target(target.tenant(), target.application(), request.repository)
            .map_err(|_| StagingError::Context)?
            != target
        {
            return Err(StagingError::Context);
        }
        request
            .encode(&mut BoundedEncoder::new(COMMAND_BYTES).map_err(|_| StagingError::Context)?)
            .map_err(|_| StagingError::Context)?;
        let command = client
            .prepare_command::<BeginStaging>(&target, identity, request.clone())
            .await
            .map_err(|e| StagingError::Begin(Box::new(e)))?;
        Ok(Self {
            inner: Box::new(StagingRequest {
                client,
                target,
                request,
                command: Exact::Begin(command),
                bound_source: None,
            }),
        })
    }
    /// Resume the same logical staging request through the authoritative Claim
    /// command, with a new creating namespace and independent previous pin.
    pub async fn claim(
        client: CellClient,
        target: CellTarget,
        request: LeaseRequest,
        identity: MutationIdentity,
    ) -> Result<Self, StagingError> {
        let begin = BeginRequest {
            repository: request.check.token.repository,
            operation: request.check.token.operation,
            request_digest: request.check.token.request_digest,
            actor: request.check.actor.clone(),
            lease_ms: request.lease_ms,
        };
        if crate::repository_target(target.tenant(), target.application(), begin.repository)
            .map_err(|_| StagingError::Context)?
            != target
        {
            return Err(StagingError::Context);
        }
        request
            .encode(&mut BoundedEncoder::new(COMMAND_BYTES).map_err(|_| StagingError::Context)?)
            .map_err(|_| StagingError::Context)?;
        let command = client
            .prepare_command::<ClaimStaging>(&target, identity, request)
            .await
            .map_err(|e| StagingError::Claim(Box::new(e)))?;
        Ok(Self {
            inner: Box::new(StagingRequest {
                client,
                target,
                request: begin,
                command: Exact::Claim(command),
                bound_source: None,
            }),
        })
    }
    /// Admit a bound takeover into the same operation/worker lifecycle. The
    /// exact old token is checked at execution; no prior live session is needed.
    pub async fn claim_bound(
        client: CellClient,
        target: CellTarget,
        request: LeaseRequest,
        identity: MutationIdentity,
    ) -> Result<Self, StagingError> {
        let begin = BeginRequest {
            repository: request.check.token.repository,
            operation: request.check.token.operation,
            request_digest: request.check.token.request_digest,
            actor: request.check.actor.clone(),
            lease_ms: request.lease_ms,
        };
        if crate::repository_target(target.tenant(), target.application(), begin.repository)
            .map_err(|_| StagingError::Context)?
            != target
        {
            return Err(StagingError::Context);
        }
        request
            .encode(&mut BoundedEncoder::new(COMMAND_BYTES).map_err(|_| StagingError::Context)?)
            .map_err(|_| StagingError::Context)?;
        let source = request.check.clone();
        let command = client
            .prepare_command::<ClaimPreparation>(&target, identity, request)
            .await
            .map_err(|e| StagingError::BoundClaim(Box::new(e)))?;
        Ok(Self {
            inner: Box::new(StagingRequest {
                client,
                target,
                request: begin,
                command: Exact::BoundClaim(command),
                bound_source: Some(source),
            }),
        })
    }
}
struct ActorAdmission {
    operations: usize,
    workers: Arc<Semaphore>,
}
#[derive(Default)]
struct Admission {
    closed: bool,
    jobs: HashMap<[u8; 16], Arc<Job>>,
    actors: HashMap<String, ActorAdmission>,
}
struct Inner {
    target: CellTarget,
    limits: StagingLimits,
    admission: Mutex<Admission>,
    workers: Arc<Semaphore>,
    drained: Notify,
    #[cfg(test)]
    fault: std::sync::atomic::AtomicU8,
}
struct Local {
    lease: Option<StagingLease>,
    bound: Option<Arc<PreparationSession>>,
    bound_source: Option<LeaseCheck>,
    bound_started: Option<Instant>,
    bound_result: Option<Arc<StagingBound>>,
    bound_renewal: Option<Committed<PreparationReply>>,
    policy_receipt: Option<Receipt>,
    finishing: bool,
    deadline: Instant,
    lifetime: Instant,
    workers: usize,
    seal: bool,
    stop: bool,
    fenced: bool,
    recovery: bool,
    renew: bool,
}
trait RetainedWork: Any + Send + Sync {
    fn fence_completed(&self);
    fn erased(self: Arc<Self>) -> Arc<dyn Any + Send + Sync>;
}
#[derive(Default)]
struct WorkSlots {
    next: u64,
    slots: HashMap<u64, Arc<dyn RetainedWork>>,
}
struct Job {
    client: CellClient,
    target: CellTarget,
    actor: String,
    operation: [u8; 16],
    actor_workers: Arc<Semaphore>,
    local: Mutex<Local>,
    work: Mutex<WorkSlots>,
    exact: Mutex<Option<Exact>>,
    checkpoint: Mutex<Option<Arc<InputRegistration>>>,
    publication: Mutex<Option<PublicationTicket>>,
    status: watch::Sender<StagingState>,
    changed: Notify,
}
#[derive(Clone)]
enum Exact {
    Begin(PreparedCommand<BeginStaging>),
    Claim(PreparedCommand<ClaimStaging>),
    Checkpoint(PreparedCommand<RegisterStagedInputs>),
    BoundCheckpoint(PreparedCommand<RegisterStagedInputs>),
    Renew(PreparedCommand<RenewStaging>),
    Bind(PreparedCommand<BindStaging>),
    BoundClaim(PreparedCommand<ClaimPreparation>),
    BoundRenew(PreparedCommand<RenewPreparation>),
}
enum Outcome {
    Stage(Committed<StagingReply>),
    Bound(Committed<PreparationReply>),
    BoundClaim(Committed<PreparationReply>),
    BoundRenew(Committed<PreparationReply>),
    Checkpoint(Committed<StagingReply>),
    BoundCheckpoint(Committed<StagingReply>),
}
impl Exact {
    fn pending(&self) -> StagingError {
        match self {
            Self::Begin(c) => StagingError::Begin(Box::new(InvocationError::Pending(Box::new(
                c.evidence().clone(),
            )))),
            Self::Claim(c) => StagingError::Claim(Box::new(InvocationError::Pending(Box::new(
                c.evidence().clone(),
            )))),
            Self::Checkpoint(c) | Self::BoundCheckpoint(c) => StagingError::Checkpoint(Box::new(
                InvocationError::Pending(Box::new(c.evidence().clone())),
            )),
            Self::Renew(c) => StagingError::Renew(Box::new(InvocationError::Pending(Box::new(
                c.evidence().clone(),
            )))),
            Self::BoundClaim(c) => StagingError::BoundClaim(Box::new(InvocationError::Pending(
                Box::new(c.evidence().clone()),
            ))),
            Self::BoundRenew(c) => StagingError::BoundRenew(Box::new(InvocationError::Pending(
                Box::new(c.evidence().clone()),
            ))),
            Self::Bind(c) => StagingError::Bind(Box::new(InvocationError::Pending(Box::new(
                c.evidence().clone(),
            )))),
        }
    }
    async fn execute(
        self,
        client: CellClient,
        recover: bool,
        fault: u8,
    ) -> Result<Outcome, StagingError> {
        match self {
            Self::Begin(c) => super::exact::invoke(&client, c, recover, COMMAND_BYTES, fault)
                .await
                .map(Outcome::Stage)
                .map_err(|e| StagingError::Begin(Box::new(e))),
            Self::Claim(c) => super::exact::invoke(&client, c, recover, COMMAND_BYTES, fault)
                .await
                .map(Outcome::Stage)
                .map_err(|e| StagingError::Claim(Box::new(e))),
            Self::Checkpoint(c) => super::exact::invoke(&client, c, recover, COMMAND_BYTES, fault)
                .await
                .map(Outcome::Checkpoint)
                .map_err(|e| StagingError::Checkpoint(Box::new(e))),
            Self::BoundCheckpoint(c) => {
                super::exact::invoke(&client, c, recover, COMMAND_BYTES, fault)
                    .await
                    .map(Outcome::BoundCheckpoint)
                    .map_err(|e| StagingError::Checkpoint(Box::new(e)))
            }
            Self::Renew(c) => super::exact::invoke(&client, c, recover, COMMAND_BYTES, fault)
                .await
                .map(Outcome::Stage)
                .map_err(|e| StagingError::Renew(Box::new(e))),
            Self::BoundClaim(c) => super::exact::invoke(&client, c, recover, COMMAND_BYTES, fault)
                .await
                .map(Outcome::BoundClaim)
                .map_err(|e| StagingError::BoundClaim(Box::new(e))),
            Self::BoundRenew(c) => super::exact::invoke(&client, c, recover, COMMAND_BYTES, fault)
                .await
                .map(Outcome::BoundRenew)
                .map_err(|e| StagingError::BoundRenew(Box::new(e))),
            Self::Bind(c) => super::exact::invoke(&client, c, recover, COMMAND_BYTES, fault)
                .await
                .map(Outcome::Bound)
                .map_err(|e| StagingError::Bind(Box::new(e))),
        }
    }
}

/// Keep one instance per repository in the service. Local ownership is not a
/// durable outbox, a public ACL endpoint, or permission for remote deletion.
#[derive(Clone)]
pub struct StagingCoordinator {
    inner: Arc<Inner>,
}
#[derive(Clone)]
#[must_use]
pub struct StagingTicket {
    inner: Arc<Inner>,
    job: Arc<Job>,
}
#[derive(Clone, Copy, Debug)]
pub struct StagingStats {
    pub admitted: usize,
    pub accounts: usize,
    pub workers: usize,
    pub uncertain: usize,
    pub command_bytes: u64,
    pub closed: bool,
}
impl StagingCoordinator {
    pub fn new(target: CellTarget, limits: StagingLimits) -> Result<Self, StagingError> {
        limits.validate()?;
        Ok(Self {
            inner: Arc::new(Inner {
                target,
                limits,
                admission: Mutex::new(Admission::default()),
                workers: Arc::new(Semaphore::new(limits.workers)),
                drained: Notify::new(),
                #[cfg(test)]
                fault: std::sync::atomic::AtomicU8::new(0),
            }),
        })
    }
    /// Admission is synchronous; a canceled observer never owns the command.
    pub fn submit(
        &self,
        ready: ReadyStaging,
    ) -> Result<StagingTicket, (StagingError, ReadyStaging)> {
        let mut admission = self.inner.admission.lock().expect("staging admission");
        let error = if ready.inner.target != self.inner.target {
            Some(StagingError::Foreign)
        } else if admission.closed {
            Some(StagingError::Closed)
        } else if ready.inner.request.lease_ms != self.inner.limits.lease_ms {
            Some(StagingError::Context)
        } else if admission.jobs.contains_key(&ready.inner.request.operation) {
            Some(StagingError::Duplicate)
        } else if admission.jobs.len() >= self.inner.limits.operations
            || admission
                .actors
                .get(&ready.inner.request.actor)
                .map(|actor| actor.operations)
                .unwrap_or(0)
                >= self.inner.limits.per_actor
        {
            Some(StagingError::Capacity)
        } else {
            None
        };
        if let Some(error) = error {
            return Err((error, ready));
        }
        let actor = admission
            .actors
            .entry(ready.inner.request.actor.clone())
            .or_insert_with(|| ActorAdmission {
                operations: 0,
                workers: Arc::new(Semaphore::new(self.inner.limits.workers_per_actor)),
            });
        actor.operations += 1;
        let actor_workers = Arc::clone(&actor.workers);
        let now = Instant::now();
        let job = Arc::new(Job {
            client: ready.inner.client,
            target: ready.inner.target,
            actor: ready.inner.request.actor,
            operation: ready.inner.request.operation,
            actor_workers,
            local: Mutex::new(Local {
                lease: None,
                bound: None,
                bound_source: ready.inner.bound_source.clone(),
                bound_started: ready.inner.bound_source.as_ref().map(|_| now),
                bound_result: None,
                bound_renewal: None,
                policy_receipt: None,
                finishing: false,
                deadline: now,
                lifetime: now + Duration::from_millis(self.inner.limits.lifetime_ms),
                workers: 0,
                seal: false,
                stop: false,
                fenced: false,
                recovery: false,
                renew: false,
            }),
            work: Mutex::new(WorkSlots::default()),
            exact: Mutex::new(Some(ready.inner.command)),
            checkpoint: Mutex::new(None),
            publication: Mutex::new(None),
            status: watch::channel(StagingState::Starting).0,
            changed: Notify::new(),
        });
        admission.jobs.insert(job.operation, Arc::clone(&job));
        tokio::spawn(supervise(Arc::clone(&self.inner), Arc::clone(&job)));
        Ok(StagingTicket {
            inner: Arc::clone(&self.inner),
            job,
        })
    }
    pub fn pending(&self, operation: [u8; 16]) -> Option<StagingTicket> {
        self.inner
            .admission
            .lock()
            .expect("staging admission")
            .jobs
            .get(&operation)
            .map(|job| StagingTicket {
                inner: Arc::clone(&self.inner),
                job: Arc::clone(job),
            })
    }
    pub fn recover(&self, ticket: &StagingTicket) -> Result<(), StagingError> {
        if !Arc::ptr_eq(&self.inner, &ticket.inner) {
            return Err(StagingError::Foreign);
        }
        if !matches!(ticket.state(), StagingState::Uncertain(_)) {
            return Err(StagingError::NotReady);
        }
        ticket.job.local.lock().expect("staging local").recovery = true;
        ticket.job.status.send_replace(StagingState::Resolving);
        ticket.job.changed.notify_one();
        Ok(())
    }
    pub fn stats(&self) -> StagingStats {
        let a = self.inner.admission.lock().expect("staging admission");
        StagingStats {
            admitted: a.jobs.len(),
            accounts: a.actors.len(),
            workers: self.inner.limits.workers - self.inner.workers.available_permits(),
            uncertain: a
                .jobs
                .values()
                .filter(|j| matches!(*j.status.borrow(), StagingState::Uncertain(_)))
                .count(),
            command_bytes: a
                .jobs
                .values()
                .map(|job| {
                    let copies =
                        2 + u64::from(job.checkpoint.lock().expect("staging checkpoint").is_some());
                    copies * COMMAND_BYTES as u64
                })
                .sum(),
            closed: a.closed,
        }
    }
    /// Stop admission and renew while accepted workers drain. Uncertain exact
    /// commands remain charged and returned; explicit recovery remains possible.
    pub async fn close_and_drain(&self) -> Vec<StagingTicket> {
        loop {
            let wake = self.inner.drained.notified();
            tokio::pin!(wake);
            wake.as_mut().enable();
            {
                let mut a = self.inner.admission.lock().expect("staging admission");
                a.closed = true;
                for job in a.jobs.values() {
                    job.local.lock().expect("staging local").stop = true;
                    job.changed.notify_one();
                }
                if a.jobs.values().all(|j| {
                    matches!(*j.status.borrow(), StagingState::Uncertain(_))
                        && j.local.lock().expect("staging local").workers == 0
                }) {
                    return a
                        .jobs
                        .values()
                        .map(|job| StagingTicket {
                            inner: Arc::clone(&self.inner),
                            job: Arc::clone(job),
                        })
                        .collect();
                }
            }
            wake.await;
        }
    }
    #[cfg(test)]
    pub(super) fn fault_for_test(&self, fault: u8) {
        self.inner
            .fault
            .store(fault, std::sync::atomic::Ordering::Release);
    }
}
struct InputRegistration {
    request: Mutex<Option<(NativeInputCertificate, MutationIdentity)>>,
    result: watch::Sender<Option<Result<Receipt, Arc<StagingError>>>>,
    bound_digest: Option<[u8; 32]>,
    checkpoint_digest: [u8; 32],
}
impl InputRegistration {
    fn finish(&self, result: Result<Receipt, Arc<StagingError>>) {
        self.request
            .lock()
            .expect("staging checkpoint request")
            .take();
        self.result.send_if_modified(|old| {
            if old.is_some() {
                false
            } else {
                *old = Some(result);
                true
            }
        });
    }
}
#[derive(Clone)]
#[must_use]
pub struct StagedInputsTicket {
    job: Arc<Job>,
    registration: Arc<InputRegistration>,
}
impl StagedInputsTicket {
    /// Observe the original durable registration receipt. An uncertain error
    /// retains the exact command in the coordinator; recover and wait again.
    /// A receipt is not a fresh authority or lease observation.
    pub async fn wait(&self) -> Result<Receipt, Arc<StagingError>> {
        let mut result = self.registration.result.subscribe();
        let mut state = self.job.status.subscribe();
        loop {
            if let Some(value) = result.borrow_and_update().clone() {
                return value;
            }
            match state.borrow_and_update().clone() {
                StagingState::Uncertain(error) | StagingState::Fenced(error) => return Err(error),
                _ => {}
            }
            tokio::select! {
                value = result.changed() => { if value.is_err() { return Err(Arc::new(StagingError::Worker)); } },
                value = state.changed() => { if value.is_err() { return Err(Arc::new(StagingError::Worker)); } },
            }
        }
    }
}
impl StagingTicket {
    /// Synchronously transfer one bounded checkpoint request into service
    /// custody. A dropped observer cannot cancel or replace its exact identity.
    pub fn register_inputs(
        &self,
        proof: NativeInputCertificate,
        identity: MutationIdentity,
    ) -> Result<StagedInputsTicket, (StagingError, Box<NativeInputCertificate>)> {
        let check = match proof.scoped_check(&self.job.target) {
            Ok(check) => check,
            Err(_) => return Err((StagingError::Context, Box::new(proof))),
        };
        let (checkpoint_digest, predecessor) = match proof.checkpoint_lineage() {
            Ok(value) => value,
            Err(_) => return Err((StagingError::Context, Box::new(proof))),
        };
        let local = self.job.local.lock().expect("staging local");
        let bound = local.bound.is_some();
        if local.fenced
            || local.finishing
            || local.stop
            || (local.seal && !bound)
            || Instant::now() >= local.deadline.min(local.lifetime)
            || !(matches!(self.state(), StagingState::Active(_)) && !bound
                || matches!(self.state(), StagingState::Bound(_)) && bound)
        {
            return Err((StagingError::Inactive, Box::new(proof)));
        }
        let expected = local
            .bound
            .as_ref()
            .map(|s| s.lease.token)
            .or_else(|| local.lease.map(|s| s.token));
        if expected != Some(check.token) || check.actor != self.job.actor {
            return Err((StagingError::Context, Box::new(proof)));
        }
        let digest = if let Some(session) = &local.bound {
            if session.live_lease().is_err() {
                return Err((StagingError::Inactive, Box::new(proof)));
            }
            match proof.bound_digest(session) {
                Ok(digest) => Some(digest),
                Err(_) => return Err((StagingError::Context, Box::new(proof))),
            }
        } else {
            None
        };
        let mut checkpoint = self.job.checkpoint.lock().expect("staging checkpoint");
        if let Some(old) = checkpoint.as_ref()
            && (bound
                || predecessor != Some(old.checkpoint_digest)
                || !old.result.borrow().as_ref().is_some_and(Result::is_ok))
        {
            return Err((StagingError::Duplicate, Box::new(proof)));
        }
        let registration = Arc::new(InputRegistration {
            request: Mutex::new(Some((proof, identity))),
            result: watch::channel(None).0,
            bound_digest: digest,
            checkpoint_digest,
        });
        *checkpoint = Some(Arc::clone(&registration));
        self.job.changed.notify_one();
        Ok(StagedInputsTicket {
            job: Arc::clone(&self.job),
            registration,
        })
    }
    /// Retrieve the accepted checkpoint observer after cancellation.
    pub fn pending_inputs(&self) -> Option<StagedInputsTicket> {
        self.job
            .checkpoint
            .lock()
            .expect("staging checkpoint")
            .as_ref()
            .map(|registration| StagedInputsTicket {
                job: Arc::clone(&self.job),
                registration: Arc::clone(registration),
            })
    }
    pub fn state(&self) -> StagingState {
        self.job.status.borrow().clone()
    }
    pub async fn wait(&self) -> StagingState {
        let mut status = self.job.status.subscribe();
        loop {
            let state = status.borrow_and_update().clone();
            if !matches!(
                state,
                StagingState::Starting
                    | StagingState::Binding
                    | StagingState::RegisteringInputs
                    | StagingState::Resolving
                    | StagingState::Draining(_)
                    | StagingState::Finishing
                    | StagingState::Publishing
            ) {
                return state;
            }
            if status.changed().await.is_err() {
                return status.borrow().clone();
            }
        }
    }
    pub async fn wait_terminal(&self) -> StagingState {
        let mut status = self.job.status.subscribe();
        loop {
            let state = status.borrow_and_update().clone();
            if state.terminal() || matches!(state, StagingState::Uncertain(_)) {
                return state;
            }
            if status.changed().await.is_err() {
                return status.borrow().clone();
            }
        }
    }
    pub fn seal(&self) -> Result<(), StagingError> {
        let mut l = self.job.local.lock().expect("staging local");
        if l.lease.is_none() || l.stop || l.fenced || self.state().terminal() {
            return Err(StagingError::NotReady);
        }
        if l.seal {
            return Ok(());
        }
        l.seal = true;
        if let Some(lease) = l.lease
            && !matches!(self.state(), StagingState::Uncertain(_))
        {
            self.job.status.send_replace(StagingState::Draining(lease));
        }
        self.job.changed.notify_one();
        Ok(())
    }
    pub fn stop(&self) {
        self.job.local.lock().expect("staging local").stop = true;
        self.job.changed.notify_one();
    }
    pub async fn open_base(
        &self,
        indexes: Arc<CatalogIndexes>,
        files: Arc<CatalogFiles>,
    ) -> Result<PreparationBaseResolver, StagingError> {
        let session = self.bound_session()?;
        let receipt = self.bound_result().ok_or(StagingError::NotReady)?.receipt;
        session.refresh(receipt).await?;
        Ok(PreparationBaseResolver::from_session((*session).clone(), indexes, files).await?)
    }
    /// A fresh live shared session, never authority reconstructed from Bound's
    /// recorded timestamps. Shutdown and lifetime fences also cover every base.
    pub fn bound_session(&self) -> Result<Arc<PreparationSession>, StagingError> {
        let l = self.job.local.lock().expect("staging local");
        if l.stop
            || l.finishing
            || l.fenced
            || Instant::now() >= l.lifetime
            || !matches!(self.state(), StagingState::Bound(_))
        {
            return Err(StagingError::Inactive);
        }
        let session = l.bound.clone().ok_or(StagingError::NotReady)?;
        session.live_lease()?;
        Ok(session)
    }
    /// Known Bind/Claim outcome survives failed fresh custody and graceful stop.
    pub fn bound_result(&self) -> Option<Arc<StagingBound>> {
        self.job
            .local
            .lock()
            .expect("staging local")
            .bound_result
            .clone()
    }
    pub fn bound_renewal(&self) -> Option<Committed<PreparationReply>> {
        self.job
            .local
            .lock()
            .expect("staging local")
            .bound_renewal
            .clone()
    }

    /// Own input work in a task, separately from its observer. A producer error,
    /// panic, expired custody or lost authority fences the session before Bind.
    pub fn spawn<F, Fut, T>(&self, producer: F) -> Result<StagingTask<T>, StagingError>
    where
        F: FnOnce(StagingContext) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, StagingError>> + Send + 'static,
        T: Send + 'static,
    {
        self.spawn_in_phase(false, producer)
    }
    /// Bound verification, reconciliation and publication use the same worker
    /// slots, cancellation/drain and typed result ownership as staging inputs.
    pub fn spawn_bound<F, Fut, T>(&self, producer: F) -> Result<StagingTask<T>, StagingError>
    where
        F: FnOnce(Arc<PreparationSession>) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, StagingError>> + Send + 'static,
        T: Send + 'static,
    {
        let session = self.bound_session()?;
        self.spawn_in_phase(true, move |_| producer(session))
    }
    fn spawn_in_phase<F, Fut, T>(
        &self,
        bound: bool,
        producer: F,
    ) -> Result<StagingTask<T>, StagingError>
    where
        F: FnOnce(StagingContext) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, StagingError>> + Send + 'static,
        T: Send + 'static,
    {
        let permit = Arc::clone(&self.inner.workers)
            .try_acquire_owned()
            .map_err(|_| StagingError::Capacity)?;
        let actor_permit = Arc::clone(&self.job.actor_workers)
            .try_acquire_owned()
            .map_err(|_| StagingError::Capacity)?;
        let context = {
            let mut l = self.job.local.lock().expect("staging local");
            if (l.seal && !bound)
                || l.finishing
                || l.bound.is_some() != bound
                || l.stop
                || l.fenced
                || l.deadline <= Instant::now()
                || matches!(
                    self.state(),
                    StagingState::Uncertain(_) | StagingState::Resolving
                )
            {
                return Err(StagingError::Inactive);
            }
            let (token, format) = if bound {
                let (lease, _) = l
                    .bound
                    .as_ref()
                    .ok_or(StagingError::NotReady)?
                    .live_lease()?;
                (lease.token, lease.format)
            } else {
                let lease = l.lease.ok_or(StagingError::NotReady)?;
                (lease.token, lease.format)
            };
            l.workers += 1;
            StagingContext {
                job: Arc::clone(&self.job),
                token,
                format,
                bound,
            }
        };
        let guard = Activity {
            inner: Arc::clone(&self.inner),
            job: Arc::clone(&self.job),
            permit: Some(permit),
            actor_permit: Some(actor_permit),
        };
        let mut work = self.job.work.lock().expect("staging work");
        let id = work.next.checked_add(1).ok_or(StagingError::Capacity)?;
        work.next = id;
        let slot = Arc::new(WorkSlot {
            result: Mutex::new(None),
            ready: watch::channel(false).0,
            guard: Mutex::new(Some(guard)),
        });
        work.slots.insert(id, slot.clone());
        drop(work);
        let owned = Arc::clone(&slot);
        let job = Arc::clone(&self.job);
        tokio::spawn(async move {
            let observe = context.clone();
            let mut task = tokio::spawn(async move { producer(context).await });
            let result = tokio::select! { result = &mut task => result.unwrap_or(Err(StagingError::Worker)), _ = observe.fenced() => { task.abort(); let _ = task.await; Err(StagingError::Inactive) } };
            let failed = {
                let mut local = job.local.lock().expect("staging local");
                let result = if local.fenced
                    || local
                        .bound
                        .as_ref()
                        .is_some_and(|s| s.live_lease().is_err())
                    || Instant::now() >= local.deadline.min(local.lifetime)
                {
                    drop(result);
                    Err(StagingError::Inactive)
                } else {
                    result
                };
                let failed = result.is_err();
                if failed {
                    local.fenced = true;
                }
                *owned.result.lock().expect("staging result") = Some(result.map_err(Arc::new));
                owned.ready.send_replace(true);
                failed
            };
            if failed {
                job.changed.notify_one();
                job.work.lock().expect("staging work").slots.remove(&id);
                owned.guard.lock().expect("staging guard").take();
            }
        });
        Ok(StagingTask {
            id,
            job: Arc::clone(&self.job),
            slot,
        })
    }
    /// Recover a service-owned result after its observer was dropped. A wrong
    /// result type fails; it cannot reinterpret a physical witness or descriptor.
    pub fn pending_task<T: Send + 'static>(&self, id: u64) -> Option<StagingTask<T>> {
        let slot = self
            .job
            .work
            .lock()
            .expect("staging work")
            .slots
            .get(&id)?
            .clone()
            .erased()
            .downcast::<WorkSlot<T>>()
            .ok()?;
        Some(StagingTask {
            id,
            job: Arc::clone(&self.job),
            slot,
        })
    }
    #[cfg(test)]
    pub(super) fn renew_for_test(&self) {
        self.job.local.lock().expect("staging local").renew = true;
        self.job.changed.notify_one();
    }
}
struct Activity {
    inner: Arc<Inner>,
    job: Arc<Job>,
    permit: Option<OwnedSemaphorePermit>,
    actor_permit: Option<OwnedSemaphorePermit>,
}
impl Drop for Activity {
    fn drop(&mut self) {
        drop(self.actor_permit.take());
        drop(self.permit.take());
        self.job.local.lock().expect("staging local").workers -= 1;
        self.job.changed.notify_one();
        self.inner.drained.notify_waiters();
    }
}
#[derive(Clone)]
pub struct StagingContext {
    job: Arc<Job>,
    token: PreparationToken,
    format: ObjectFormat,
    bound: bool,
}
impl StagingContext {
    pub(super) fn capability(&self) -> (&CellClient, &CellTarget, LeaseCheck) {
        (
            &self.job.client,
            &self.job.target,
            LeaseCheck {
                token: self.token,
                actor: self.job.actor.clone(),
            },
        )
    }
    pub fn token(&self) -> Result<PreparationToken, StagingError> {
        self.ensure_live()?;
        Ok(self.token)
    }
    pub fn format(&self) -> ObjectFormat {
        self.format
    }
    pub fn ensure_live(&self) -> Result<(), StagingError> {
        let l = self.job.local.lock().expect("staging local");
        if l.fenced
            || l.bound.is_some() != self.bound
            || l.deadline <= Instant::now()
            || l.lifetime <= Instant::now()
        {
            Err(StagingError::Inactive)
        } else {
            Ok(())
        }
    }
    async fn fenced(&self) {
        let mut status = self.job.status.subscribe();
        loop {
            let deadline = {
                let l = self.job.local.lock().expect("staging local");
                if l.fenced || l.bound.is_some() != self.bound {
                    return;
                }
                l.deadline.min(l.lifetime)
            };
            if Instant::now() >= deadline {
                return;
            }
            tokio::select! { _ = sleep_until(deadline) => {}, result = status.changed() => { if result.is_err() { return; } } }
        }
    }
}
struct WorkSlot<T> {
    result: Mutex<Option<Result<T, Arc<StagingError>>>>,
    ready: watch::Sender<bool>,
    guard: Mutex<Option<Activity>>,
}
impl<T: Send + 'static> RetainedWork for WorkSlot<T> {
    fn erased(self: Arc<Self>) -> Arc<dyn Any + Send + Sync> {
        self
    }
    fn fence_completed(&self) {
        if *self.ready.borrow() {
            let value = self.result.lock().expect("staging result").take();
            // Drop all owned input resources before returning their credit.
            drop(value);
            *self.result.lock().expect("staging result") =
                Some(Err(Arc::new(StagingError::Inactive)));
            self.guard.lock().expect("staging guard").take();
        }
    }
}
#[must_use]
pub struct StagingTask<T> {
    id: u64,
    job: Arc<Job>,
    slot: Arc<WorkSlot<T>>,
}
impl<T> StagingTask<T> {
    pub fn id(&self) -> u64 {
        self.id
    }
    /// Transfer the result once. Cancellation before readiness preserves it in
    /// the service. Physical resources move before their worker credit releases.
    pub async fn wait(&self) -> Result<T, Arc<StagingError>> {
        let mut ready = self.slot.ready.subscribe();
        while !*ready.borrow_and_update() {
            if ready.changed().await.is_err() {
                return Err(Arc::new(StagingError::Worker));
            }
        }
        let result = self
            .slot
            .result
            .lock()
            .expect("staging result")
            .take()
            .ok_or_else(|| Arc::new(StagingError::NotReady))?;
        self.job
            .work
            .lock()
            .expect("staging work")
            .slots
            .remove(&self.id);
        self.slot.guard.lock().expect("staging guard").take();
        result
    }
}

async fn probe(job: &Job, minimum: Receipt) -> Result<(StagingLease, Instant), StagingError> {
    let started = Instant::now();
    let token = match job.local.lock().expect("staging local").lease {
        Some(l) => l.token,
        None => return Err(StagingError::Context),
    };
    let lease = job
        .client
        .query::<CheckStaging>(
            &job.target,
            Some(minimum),
            LeaseCheck {
                token,
                actor: job.actor.clone(),
            },
        )
        .await
        .map_err(|e| StagingError::Query(Box::new(e)))?
        .output
        .ok_or(StagingError::Inactive)?;
    if lease.token != token
        || lease.observed_at_ms < 0
        || lease.expires_at_ms <= lease.observed_at_ms
    {
        return Err(StagingError::Context);
    }
    let deadline = started
        + Duration::from_millis((lease.expires_at_ms - lease.observed_at_ms) as u64)
            .min(Duration::from_millis(MAX_LEASE_MS));
    if deadline <= Instant::now() {
        return Err(StagingError::Inactive);
    }
    Ok((lease, deadline))
}
async fn supervise(inner: Arc<Inner>, job: Arc<Job>) {
    let mut recover = false;
    loop {
        let task = tokio::spawn(run(Arc::clone(&inner), Arc::clone(&job), recover));
        if task.await.is_ok() {
            return;
        }
        {
            let mut local = job.local.lock().expect("staging local");
            local.fenced = true;
            if let Some(session) = &local.bound {
                session.fence();
            }
        }
        let exact = job.exact.lock().expect("staging exact").clone();
        let Some(exact) = exact else {
            let publication = job.publication.lock().expect("staging publication").clone();
            if let Some(ticket) = publication
                && job.local.lock().expect("staging local").finishing
                && !matches!(
                    ticket.state(),
                    PublicationState::Held | PublicationState::Discarded
                )
            {
                // The other coordinator owns execution and exact evidence.
                // Observe it even if this supervisor lost its local authority.
                if publication::observe(&inner, &job, &ticket).await {
                    return;
                }
            }
            fence_and_drain(&inner, &job, StagingError::Worker).await;
            return;
        };
        job.status
            .send_replace(StagingState::Uncertain(Arc::new(exact.pending())));
        inner.drained.notify_waiters();
        await_recovery(&job).await;
        recover = true;
    }
}
async fn await_recovery(job: &Job) {
    loop {
        let wake = job.changed.notified();
        tokio::pin!(wake);
        wake.as_mut().enable();
        let ready = {
            let mut l = job.local.lock().expect("staging local");
            if (l.lease.is_some() || l.bound.is_some())
                && Instant::now() >= l.deadline.min(l.lifetime)
            {
                l.fenced = true;
            }
            let ready = l.recovery;
            l.recovery = false;
            ready
        };
        if ready {
            return;
        }
        wake.await;
    }
}
async fn run(inner: Arc<Inner>, job: Arc<Job>, mut recover: bool) {
    loop {
        let publication = job.publication.lock().expect("staging publication").clone();
        if let Some(ticket) = publication
            && job.local.lock().expect("staging local").finishing
            && !matches!(
                ticket.state(),
                PublicationState::Held | PublicationState::Discarded
            )
        {
            if publication::observe(&inner, &job, &ticket).await {
                return;
            }
            continue;
        }
        let command = job.exact.lock().expect("staging exact").clone();
        if let Some(command) = command {
            if recover {
                job.status.send_replace(StagingState::Resolving);
            }
            #[cfg(test)]
            let fault = inner.fault.swap(0, std::sync::atomic::Ordering::AcqRel);
            #[cfg(not(test))]
            let fault = 0;
            let pending = command.pending();
            let result = tokio::spawn(command.execute(job.client.clone(), recover, fault))
                .await
                .unwrap_or(Err(pending));
            match result {
                Err(error) if error.uncertain() => {
                    job.status
                        .send_replace(StagingState::Uncertain(Arc::new(error)));
                    inner.drained.notify_waiters();
                    await_recovery(&job).await;
                    recover = true;
                    continue;
                }
                Err(error) => {
                    job.exact.lock().expect("staging exact").take();
                    fence_and_drain(&inner, &job, error).await;
                    return;
                }
                Ok(Outcome::Bound(value)) => {
                    job.exact.lock().expect("staging exact").take();
                    if !accept_bound(&inner, &job, value, false).await {
                        return;
                    }
                    recover = false;
                }
                Ok(Outcome::BoundClaim(value)) => {
                    job.exact.lock().expect("staging exact").take();
                    if !accept_bound(&inner, &job, value, true).await {
                        return;
                    }
                    recover = false;
                }
                Ok(Outcome::BoundCheckpoint(value)) => {
                    job.exact.lock().expect("staging exact").take();
                    let session = job
                        .local
                        .lock()
                        .expect("staging local")
                        .bound
                        .clone()
                        .expect("bound checkpoint session");
                    let registration = job
                        .checkpoint
                        .lock()
                        .expect("staging checkpoint")
                        .clone()
                        .expect("bound checkpoint slot");
                    registration.finish(Ok(value.receipt));
                    let matched = matches!(&value.output, StagingReply::Granted(lease)
                        if lease.token == session.lease.token && lease.format == session.lease.format);
                    let result = if matched {
                        super::inputs::observe_bound_registration(
                            &session,
                            registration.bound_digest.expect("bound checkpoint digest"),
                            value.receipt,
                        )
                        .await
                    } else {
                        Err(PreparationBaseError::Context.into())
                    };
                    if let Err(error) = result {
                        fence_and_drain(&inner, &job, StagingError::Input(Box::new(error))).await;
                        return;
                    }
                    let mut local = job.local.lock().expect("staging local");
                    local.deadline = *session.deadline.lock().expect("bound deadline");
                    job.status.send_replace(bound_state(&local));
                    recover = false;
                }
                Ok(Outcome::BoundRenew(value)) => {
                    job.exact.lock().expect("staging exact").take();
                    let session = {
                        let mut local = job.local.lock().expect("staging local");
                        local.bound_renewal = Some(value.clone());
                        local.bound.clone().expect("bound renewal session")
                    };
                    let matched = matches!(&value.output, PreparationReply::Granted(lease)
                        if lease.token == session.lease.token && lease.base == session.lease.base && lease.format == session.lease.format);
                    let result = if matched {
                        session.refresh(value.receipt).await
                    } else {
                        Err(PreparationBaseError::Context)
                    };
                    if let Err(error) = result {
                        fence_and_drain(&inner, &job, StagingError::Base(error)).await;
                        return;
                    }
                    let (_, _) = match session.live_lease() {
                        Ok(lease) => lease,
                        Err(error) => {
                            fence_and_drain(&inner, &job, StagingError::Base(error)).await;
                            return;
                        }
                    };
                    let mut local = job.local.lock().expect("staging local");
                    local.deadline = *session.deadline.lock().expect("bound deadline");
                    job.status.send_replace(bound_state(&local));
                    recover = false;
                }
                Ok(outcome @ (Outcome::Stage(_) | Outcome::Checkpoint(_))) => {
                    let (value, checkpoint) = match outcome {
                        Outcome::Stage(value) => (value, false),
                        Outcome::Checkpoint(value) => (value, true),
                        Outcome::Bound(_)
                        | Outcome::BoundClaim(_)
                        | Outcome::BoundRenew(_)
                        | Outcome::BoundCheckpoint(_) => {
                            unreachable!()
                        }
                    };
                    let StagingReply::Granted(lease) = value.output else {
                        job.exact.lock().expect("staging exact").take();
                        fence_and_drain(&inner, &job, StagingError::Context).await;
                        return;
                    };
                    if checkpoint {
                        let expected = job.local.lock().expect("staging local").lease;
                        if !expected.is_some_and(|old| {
                            old.token == lease.token && old.format == lease.format
                        }) {
                            fence_and_drain(&inner, &job, StagingError::Context).await;
                            return;
                        }
                        job.checkpoint
                            .lock()
                            .expect("staging checkpoint")
                            .as_ref()
                            .expect("accepted input checkpoint")
                            .finish(Ok(value.receipt));
                    }
                    job.exact.lock().expect("staging exact").take();
                    {
                        let mut l = job.local.lock().expect("staging local");
                        if l.lease.is_none() {
                            l.lease = Some(*lease);
                        }
                    }
                    match probe(&job, value.receipt).await {
                        Ok((lease, deadline)) => {
                            let differs = job
                                .local
                                .lock()
                                .expect("staging local")
                                .lease
                                .is_some_and(|old| {
                                    old.token != lease.token || old.format != lease.format
                                });
                            if differs {
                                fence_and_drain(&inner, &job, StagingError::Context).await;
                                return;
                            }
                            let mut l = job.local.lock().expect("staging local");
                            l.lease = Some(lease);
                            l.deadline = deadline;
                            job.status.send_replace(if l.seal || l.stop {
                                StagingState::Draining(lease)
                            } else {
                                StagingState::Active(lease)
                            });
                        }
                        Err(error) => {
                            fence_and_drain(&inner, &job, error).await;
                            return;
                        }
                    }
                    recover = false;
                }
            }
        }
        enum Next {
            Stop,
            Fence,
            Bind(LeaseCheck),
            Renew(LeaseCheck),
            BoundRenew(LeaseCheck),
            Checkpoint(Arc<InputRegistration>),
            Publish,
            Wait(Instant),
        }
        let next = {
            let wake = job.changed.notified();
            tokio::pin!(wake);
            wake.as_mut().enable();
            let next = {
                let mut l = job.local.lock().expect("staging local");
                let token = l
                    .bound
                    .as_ref()
                    .map(|s| s.lease.token)
                    .or_else(|| l.lease.map(|s| s.token))
                    .expect("active preparation lease");
                let check = LeaseCheck {
                    token,
                    actor: job.actor.clone(),
                };
                let now = Instant::now();
                let due = l
                    .deadline
                    .checked_sub(Duration::from_millis(inner.limits.renew_before_ms))
                    .unwrap_or(now);
                let checkpoint = job
                    .checkpoint
                    .lock()
                    .expect("staging checkpoint")
                    .as_ref()
                    .filter(|slot| {
                        slot.request
                            .lock()
                            .expect("staging checkpoint request")
                            .is_some()
                    })
                    .cloned();
                let stopped_page = l.stop
                    && l.workers == 0
                    && job
                        .publication
                        .lock()
                        .expect("staging publication")
                        .as_ref()
                        .is_some_and(|ticket| {
                            ticket.is_policy_page()
                                && matches!(ticket.state(), PublicationState::Held)
                        });
                if l.fenced || now >= l.deadline.min(l.lifetime) || stopped_page {
                    Next::Fence
                } else if l.stop && !l.finishing && l.workers == 0 && checkpoint.is_none() {
                    Next::Stop
                } else if l.bound.is_none()
                    && l.seal
                    && l.workers == 0
                    && !l.stop
                    && checkpoint.is_none()
                {
                    Next::Bind(check)
                } else if l.finishing
                    && l.workers == 0
                    && checkpoint.is_none()
                    && job
                        .publication
                        .lock()
                        .expect("staging publication")
                        .as_ref()
                        .is_some_and(PublicationTicket::is_root_refusal)
                {
                    // A pre-frozen refusal cannot ACK or publish. Let its final
                    // transaction check custody rather than requiring fresh Write
                    // for a renewal after that permission has been revoked.
                    Next::Publish
                } else if l.renew || now >= due {
                    l.renew = false;
                    if l.bound.is_some() {
                        Next::BoundRenew(check)
                    } else {
                        Next::Renew(check)
                    }
                } else if let Some(registration) = checkpoint {
                    Next::Checkpoint(registration)
                } else if l.finishing && l.workers == 0 {
                    Next::Publish
                } else {
                    Next::Wait(due.min(l.lifetime))
                }
            };
            if let Next::Wait(deadline) = next {
                tokio::select! { _ = wake => {}, _ = sleep_until(deadline) => {} }
                continue;
            }
            // The outer waiter must be dropped before observation/draining
            // registers its own waiter, or notify_one can wake an unused future
            // and leave an accepted recovery request asleep indefinitely.
            next
        };
        match next {
            Next::Publish => {
                let ticket = job
                    .publication
                    .lock()
                    .expect("staging publication")
                    .clone()
                    .expect("accepted final publication");
                let (session, receipt) = {
                    let l = job.local.lock().expect("staging local");
                    let mut receipt = l.bound_result.as_ref().expect("bound receipt").receipt;
                    if let Some(renewal) = &l.bound_renewal
                        && renewal.receipt.commit_sequence > receipt.commit_sequence
                    {
                        receipt = renewal.receipt;
                    }
                    if let Some(policy) = l.policy_receipt
                        && policy.commit_sequence > receipt.commit_sequence
                    {
                        receipt = policy;
                    }
                    if let Some(registration) =
                        job.checkpoint.lock().expect("staging checkpoint").as_ref()
                        && let Some(Ok(registered)) = registration.result.borrow().as_ref()
                        && registered.commit_sequence > receipt.commit_sequence
                    {
                        receipt = *registered;
                    }
                    (l.bound.clone().expect("bound publication session"), receipt)
                };
                let refreshed = if ticket.is_root_refusal() {
                    session.live_lease().map(|_| ())
                } else {
                    session.refresh(receipt).await
                };
                if let Err(error) = refreshed {
                    fence_and_drain(&inner, &job, StagingError::Base(error)).await;
                    return;
                }
                if job.local.lock().expect("staging local").fenced {
                    fence_and_drain(&inner, &job, StagingError::Inactive).await;
                    return;
                }
                if let Err(error) = ticket.activate().await {
                    fence_and_drain(&inner, &job, StagingError::PublicationAdmission(error)).await;
                    return;
                }
                if publication::observe(&inner, &job, &ticket).await {
                    return;
                }
            }
            Next::Stop => {
                let mut local = job.local.lock().expect("staging local");
                local.fenced = true;
                if let Some(session) = &local.bound {
                    session.fence();
                }
                // Keep the original binding receipt observable after graceful stop.
                job.status.send_replace(
                    local
                        .bound_result
                        .clone()
                        .map(StagingState::Bound)
                        .unwrap_or(StagingState::Stopped),
                );
                drop(local);
                remove(&inner, &job);
                return;
            }
            Next::Fence => {
                fence_and_drain(&inner, &job, StagingError::Inactive).await;
                return;
            }
            Next::Wait(_) => unreachable!("wait handled before dropping scheduler waiter"),
            Next::Checkpoint(registration) => {
                job.status.send_replace(StagingState::RegisteringInputs);
                let (proof, identity) = registration
                    .request
                    .lock()
                    .expect("staging checkpoint request")
                    .take()
                    .expect("queued checkpoint");
                match job
                    .client
                    .prepare_command::<RegisterStagedInputs>(&job.target, identity, proof)
                    .await
                {
                    Ok(command) => {
                        *job.exact.lock().expect("staging exact") =
                            Some(if registration.bound_digest.is_some() {
                                Exact::BoundCheckpoint(command)
                            } else {
                                Exact::Checkpoint(command)
                            });
                    }
                    Err(error) => {
                        fence_and_drain(&inner, &job, StagingError::Checkpoint(Box::new(error)))
                            .await;
                        return;
                    }
                }
            }
            Next::Bind(check) => {
                job.local.lock().expect("staging local").bound_started = Some(Instant::now());
                job.status.send_replace(StagingState::Binding);
                let identity = crate::server::mutation_identity().map_err(|_| StagingError::Clock);
                let result = match identity {
                    Ok(id) => job
                        .client
                        .prepare_command::<BindStaging>(&job.target, id, check)
                        .await
                        .map(Exact::Bind)
                        .map_err(|e| StagingError::Bind(Box::new(e))),
                    Err(e) => Err(e),
                };
                match result {
                    Ok(c) => {
                        *job.exact.lock().expect("staging exact") = Some(c);
                    }
                    Err(e) => {
                        fence_and_drain(&inner, &job, e).await;
                        return;
                    }
                }
            }
            Next::BoundRenew(check) => {
                let identity = crate::server::mutation_identity().map_err(|_| StagingError::Clock);
                let result = match identity {
                    Ok(id) => job
                        .client
                        .prepare_command::<RenewPreparation>(
                            &job.target,
                            id,
                            LeaseRequest {
                                check,
                                lease_ms: inner.limits.lease_ms,
                            },
                        )
                        .await
                        .map(Exact::BoundRenew)
                        .map_err(|e| StagingError::BoundRenew(Box::new(e))),
                    Err(e) => Err(e),
                };
                match result {
                    Ok(command) => {
                        *job.exact.lock().expect("staging exact") = Some(command);
                    }
                    Err(error) => {
                        fence_and_drain(&inner, &job, error).await;
                        return;
                    }
                }
            }
            Next::Renew(check) => {
                let identity = crate::server::mutation_identity().map_err(|_| StagingError::Clock);
                let result = match identity {
                    Ok(id) => job
                        .client
                        .prepare_command::<RenewStaging>(
                            &job.target,
                            id,
                            LeaseRequest {
                                check,
                                lease_ms: inner.limits.lease_ms,
                            },
                        )
                        .await
                        .map(Exact::Renew)
                        .map_err(|e| StagingError::Renew(Box::new(e))),
                    Err(e) => Err(e),
                };
                match result {
                    Ok(c) => {
                        *job.exact.lock().expect("staging exact") = Some(c);
                    }
                    Err(e) => {
                        fence_and_drain(&inner, &job, e).await;
                        return;
                    }
                }
            }
        }
    }
}
async fn fence_and_drain(inner: &Inner, job: &Job, error: StagingError) {
    {
        let mut local = job.local.lock().expect("staging local");
        local.fenced = true;
        if let Some(session) = &local.bound {
            session.fence();
        }
    }
    let publication = job.publication.lock().expect("staging publication").clone();
    if let Some(ticket) = publication {
        match ticket.state() {
            PublicationState::Held => {
                if ticket.discard_held().await.is_err()
                    && !matches!(ticket.state(), PublicationState::Discarded)
                {
                    // A service-internal activation raced fencing. Execution
                    // owns exact evidence now; preserve its original outcome.
                    if publication::observe(inner, job, &ticket).await {
                        return;
                    }
                }
            }
            PublicationState::Discarded => {}
            _ if !ticket.is_policy_page() || job.local.lock().expect("staging local").finishing => {
                let terminal = publication::observe(inner, job, &ticket).await;
                if terminal {
                    return;
                }
            }
            _ => {}
        }
    }
    finish_fence(inner, job, error).await;
}
async fn finish_fence(inner: &Inner, job: &Job, error: StagingError) {
    {
        let mut local = job.local.lock().expect("staging local");
        local.fenced = true;
        if let Some(session) = &local.bound {
            session.fence();
        }
    }
    let error = Arc::new(error);
    job.status
        .send_replace(StagingState::Fenced(Arc::clone(&error)));
    if let Some(registration) = job.checkpoint.lock().expect("staging checkpoint").as_ref() {
        registration.finish(Err(error));
    }
    drain_work(job).await;
    remove(inner, job);
}
async fn drain_work(job: &Job) {
    // Release the service's completed-result ownership. In-flight supervisors
    // still own their slots until abort/join and retain their admission guards.
    let slots = std::mem::take(&mut job.work.lock().expect("staging work").slots);
    for slot in slots.values() {
        slot.fence_completed();
    }
    drop(slots);
    loop {
        let wake = job.changed.notified();
        tokio::pin!(wake);
        wake.as_mut().enable();
        if job.local.lock().expect("staging local").workers == 0 {
            return;
        }
        wake.await;
    }
}
fn bound_state(local: &Local) -> StagingState {
    if local.finishing {
        StagingState::Finishing
    } else {
        StagingState::Bound(local.bound_result.clone().expect("bound original result"))
    }
}
fn remove(inner: &Inner, job: &Job) {
    let mut a = inner.admission.lock().expect("staging admission");
    a.jobs.remove(&job.operation);
    let count = a.actors.get_mut(&job.actor).expect("staging actor");
    count.operations -= 1;
    if count.operations == 0 {
        a.actors.remove(&job.actor);
    }
    inner.drained.notify_waiters();
}
