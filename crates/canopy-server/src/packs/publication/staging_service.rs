//! Service-owned, bounded input custody. Never infer a live deadline from a
//! replayed command, discard ambiguous evidence, or let an observer cancel work.
use super::custody::OwnedCustody;
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
mod driver;
mod publication;
mod restore;
mod retirement;
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
    #[error("owned push workflow failed: {0}")]
    DriverFailure(Box<str>),
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
    #[error("restored staging custody command failed")]
    Restoration(#[source] Box<InvocationError<CustodyReply>>),
    #[error("staging bind failed")]
    Bind(#[source] Box<InvocationError<PreparationReply>>),
    #[error("staging custody preparation failed")]
    CustodyReady(#[source] Box<CustodyError>),
    #[error("staging custody protocol failed")]
    Custody {
        evidence: Box<cellule_runtime::PendingMutation>,
        source: Box<CustodyError>,
    },
    #[error("staging query failed")]
    Query(#[source] Box<InvocationError<Option<StagingLease>>>),
    #[error("bound base failed")]
    Base(#[from] PreparationBaseError),
    #[error("final publication admission failed: {0}")]
    PublicationAdmission(PublicationScheduleError),
    #[error("final publication failed: {0}")]
    Publication(#[source] Arc<PublicationError>),
}
impl From<CustodyError> for StagingError {
    fn from(error: CustodyError) -> Self {
        Self::CustodyReady(Box::new(error))
    }
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
            Self::Restoration(e) => unknown(e),
            Self::Custody { source, .. } => source.uncertain(),
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
        if RegisteredCustody::load_latest(&client, &target, request.operation)
            .await?
            .is_some()
        {
            return Err(StagingError::Duplicate);
        }
        let command = OwnedCustody::prepare(
            &client,
            &target,
            CustodyAction::BeginStaging(request.clone()),
            identity,
        )
        .await?;
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
        let command = OwnedCustody::prepare(
            &client,
            &target,
            CustodyAction::ClaimStaging(request),
            identity,
        )
        .await?;
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
        let command = OwnedCustody::prepare(
            &client,
            &target,
            CustodyAction::ClaimPreparation(request),
            identity,
        )
        .await?;
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
    paused: bool,
    retirement_probe: bool,
    retirement_probes: u64,
    retirement_failures: u64,
    retirement_recoveries: u64,
    retirement_restarts: u64,
    jobs: HashMap<[u8; 16], Arc<Job>>,
    actors: HashMap<String, ActorAdmission>,
}
#[cfg(test)]
type CheckpointProbeGate = (
    tokio::sync::oneshot::Sender<()>,
    tokio::sync::oneshot::Receiver<()>,
);
struct Inner {
    authority: PreparationAuthority,
    target: CellTarget,
    limits: StagingLimits,
    budget: StagingBudget,
    resident: Option<(
        CellClient,
        PublicationCoordinator,
        Arc<canopy_object_storage::artifact::ArtifactStore>,
    )>,
    admission: Mutex<Admission>,
    workers: Arc<Semaphore>,
    drained: Notify,
    #[cfg(test)]
    fault: std::sync::atomic::AtomicU8,
    #[cfg(test)]
    checkpoint_probe_gate: Mutex<Option<CheckpointProbeGate>>,
}
struct Local {
    lease: Option<StagingLease>,
    bound: Option<Arc<PreparationSession>>,
    bound_source: Option<LeaseCheck>,
    bound_started: Option<Instant>,
    bound_result: Option<Arc<StagingBound>>,
    bound_renewal: Option<Committed<PreparationReply>>,
    restored_outcome: Option<Arc<Committed<CustodyReply>>>,
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
    driver_started: bool,
    driver_graceful: bool,
    // Diagnostic only: rejected producer values can own physical worker pins.
    driver_failure: Option<Arc<StagingError>>,
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
    authority: PreparationAuthority,
    client: CellClient,
    target: CellTarget,
    actor: String,
    operation: [u8; 16],
    request_digest: [u8; 32],
    driver: Mutex<Option<driver::DriverJoin>>,
    driver_stop: tokio_util::sync::CancellationToken,
    restored_evidence: Option<cellule_runtime::PendingMutation>,
    actor_workers: Arc<Semaphore>,
    operation_permit: Mutex<Option<crate::admission::AdmissionPermit>>,
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
    Restored(OwnedCustody),
    Begin(OwnedCustody),
    Claim(OwnedCustody),
    Checkpoint(PreparedCommand<RegisterStagedInputs>),
    BoundCheckpoint(PreparedCommand<RegisterStagedInputs>),
    Renew(OwnedCustody),
    Bind(OwnedCustody),
    BoundClaim(OwnedCustody),
    BoundRenew(OwnedCustody),
}
enum Outcome {
    Restored(Box<Committed<CustodyReply>>),
    Stage(Committed<StagingReply>),
    Bound(Committed<PreparationReply>),
    BoundClaim(Committed<PreparationReply>),
    BoundRenew(Committed<PreparationReply>),
    Checkpoint(Committed<StagingReply>),
    BoundCheckpoint(Committed<StagingReply>),
}
fn custody_guard(job: &Job) -> Result<(), Error> {
    let local = job
        .local
        .lock()
        .map_err(|_| Error::Command("staging custody poisoned"))?;
    let now = Instant::now();
    if local.fenced
        || now >= local.lifetime
        || ((local.lease.is_some() || local.bound.is_some()) && now >= local.deadline)
    {
        return Err(Error::Command("staging custody inactive"));
    }
    Ok(())
}

impl Exact {
    fn custody_original(&self) -> Option<&OwnedCustody> {
        match self {
            Self::Restored(c)
            | Self::Begin(c)
            | Self::Claim(c)
            | Self::Renew(c)
            | Self::Bind(c)
            | Self::BoundClaim(c)
            | Self::BoundRenew(c) => Some(c),
            Self::Checkpoint(_) | Self::BoundCheckpoint(_) => None,
        }
    }
    fn pending(&self) -> StagingError {
        match self {
            Self::Restored(c) => StagingError::Restoration(Box::new(InvocationError::Pending(
                Box::new(c.evidence().clone()),
            ))),
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
    async fn custody<T>(
        command: OwnedCustody,
        client: &CellClient,
        recover: bool,
        fault: u8,
        job: &Job,
        project: fn(CustodyReply) -> Option<T>,
        role: fn(Box<InvocationError<T>>) -> StagingError,
    ) -> Result<Committed<T>, StagingError> {
        let result = command
            .invoke(client, recover, fault, || custody_guard(job))
            .await
            .map_err(|source| StagingError::Custody {
                evidence: Box::new(command.evidence().clone()),
                source: Box::new(source),
            })?;
        super::custody::project(result, project).map_err(|error| role(Box::new(error)))
    }
    async fn execute(
        self,
        client: CellClient,
        job: Arc<Job>,
        recover: bool,
        fault: u8,
    ) -> Result<Outcome, StagingError> {
        fn stage(reply: CustodyReply) -> Option<StagingReply> {
            match reply {
                CustodyReply::Staging(reply) => Some(reply),
                _ => None,
            }
        }
        fn preparation(reply: CustodyReply) -> Option<PreparationReply> {
            match reply {
                CustodyReply::Preparation(reply) => Some(reply),
                _ => None,
            }
        }
        match self {
            Self::Restored(c) => restore::dispatch(c, &client, &job, recover, fault).await,
            Self::Begin(c) => {
                Self::custody(c, &client, recover, fault, &job, stage, StagingError::Begin)
                    .await
                    .map(Outcome::Stage)
            }
            Self::Claim(c) => {
                Self::custody(c, &client, recover, fault, &job, stage, StagingError::Claim)
                    .await
                    .map(Outcome::Stage)
            }
            Self::Renew(c) => {
                Self::custody(c, &client, recover, fault, &job, stage, StagingError::Renew)
                    .await
                    .map(Outcome::Stage)
            }
            Self::Bind(c) => Self::custody(
                c,
                &client,
                recover,
                fault,
                &job,
                preparation,
                StagingError::Bind,
            )
            .await
            .map(Outcome::Bound),
            Self::BoundClaim(c) => Self::custody(
                c,
                &client,
                recover,
                fault,
                &job,
                preparation,
                StagingError::BoundClaim,
            )
            .await
            .map(Outcome::BoundClaim),
            Self::BoundRenew(c) => Self::custody(
                c,
                &client,
                recover,
                fault,
                &job,
                preparation,
                StagingError::BoundRenew,
            )
            .await
            .map(Outcome::BoundRenew),
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
    /// Read-only exact-original probes, outside the command wire reservation.
    pub retirement_probes: u64,
    pub retirement_failures: u64,
    pub retirement_recoveries: u64,
    pub retirement_restarts: u64,
    pub retirement_running: bool,
}
/// Node-wide capacity shared by every resident repository. Physical worker
/// claims are retained by the existing activity owner, including detached work.
#[derive(Clone)]
pub(crate) struct StagingBudget {
    operations: Arc<crate::admission::AccountAdmission>,
    workers: Arc<crate::admission::AccountAdmission>,
}
impl StagingBudget {
    pub(crate) fn new(operations: usize, workers: usize) -> Result<Self, StagingError> {
        if operations < 2 || workers < 2 {
            return Err(StagingError::InvalidLimits);
        }
        Ok(Self {
            operations: Arc::new(crate::admission::AccountAdmission::new(
                operations,
                "node staging operations",
                "account staging operations",
            )),
            workers: Arc::new(crate::admission::AccountAdmission::new(
                workers,
                "node staging workers",
                "account staging workers",
            )),
        })
    }
    #[cfg(test)]
    pub(crate) fn available(&self) -> (usize, usize) {
        (self.operations.available(), self.workers.available())
    }
}
/// Pauses an idle resident while its other services attempt eviction. A busy
/// serving pool or a canceled eviction restores admission through this guard.
pub(crate) struct StagingQuiescence {
    inner: Arc<Inner>,
}
impl StagingQuiescence {
    pub(crate) fn commit(self) {
        self.inner
            .admission
            .lock()
            .expect("staging admission")
            .closed = true;
    }
}
impl Drop for StagingQuiescence {
    fn drop(&mut self) {
        let mut admission = self.inner.admission.lock().expect("staging admission");
        admission.paused = false;
    }
}
impl StagingCoordinator {
    /// Discovery may defer only to this exact bound attempt, including its
    /// actor, owner epoch, admission sequence and artifact operation. A reused
    /// logical UUID or an unrelated historical pin is insufficient.
    pub(in crate::packs::publication) fn owns_bound(&self, check: &LeaseCheck) -> bool {
        let Some(ticket) = self.pending(check.token.operation) else {
            return false;
        };
        let local = ticket.job.local.lock().expect("staging local");
        local
            .bound
            .as_ref()
            .is_some_and(|session| session.check == *check)
    }
    pub(in crate::packs::publication) fn matches_target(&self, target: &CellTarget) -> bool {
        self.inner.target == *target
    }
    pub fn new(
        target: CellTarget,
        limits: StagingLimits,
        authority: PreparationAuthority,
    ) -> Result<Self, StagingError> {
        limits.validate()?;
        // Standalone coordinators keep their original per-repository limits.
        // Production residents share a node budget through new_with_budget.
        let budget = StagingBudget::new(
            limits.operations.max(limits.per_actor * 2),
            limits.workers.max(limits.workers_per_actor * 2),
        )?;
        Self::new_with_budget(target, limits, authority, budget)
    }
    pub(crate) fn new_with_budget(
        target: CellTarget,
        limits: StagingLimits,
        authority: PreparationAuthority,
        budget: StagingBudget,
    ) -> Result<Self, StagingError> {
        limits.validate()?;
        if !authority.matches(&target) {
            return Err(StagingError::Context);
        }
        Ok(Self {
            inner: Arc::new(Inner {
                authority,
                target,
                limits,
                budget,
                resident: None,
                admission: Mutex::new(Admission::default()),
                workers: Arc::new(Semaphore::new(limits.workers)),
                drained: Notify::new(),
                #[cfg(test)]
                fault: std::sync::atomic::AtomicU8::new(0),
                #[cfg(test)]
                checkpoint_probe_gate: Mutex::new(None),
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
        } else if admission.closed || admission.paused {
            Some(StagingError::Closed)
        } else if !matches!(ready.inner.command, Exact::Restored(_))
            && ready.inner.request.lease_ms != self.inner.limits.lease_ms
        {
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
        let operation_permit = match self
            .inner
            .budget
            .operations
            .try_acquire(crate::ReadIdentity::Account(&ready.inner.request.actor))
        {
            Ok(permit) => permit,
            Err(_) => return Err((StagingError::Capacity, ready)),
        };
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
        let restored_evidence = match &ready.inner.command {
            Exact::Restored(command) => Some(command.evidence().clone()),
            _ => None,
        };
        let job = Arc::new(Job {
            authority: self.inner.authority.clone(),
            client: ready.inner.client,
            target: ready.inner.target,
            actor: ready.inner.request.actor,
            operation: ready.inner.request.operation,
            request_digest: ready.inner.request.request_digest,
            driver: Mutex::new(None),
            driver_stop: tokio_util::sync::CancellationToken::new(),
            restored_evidence,
            actor_workers,
            operation_permit: Mutex::new(Some(operation_permit)),
            local: Mutex::new(Local {
                lease: None,
                bound: None,
                bound_source: ready.inner.bound_source.clone(),
                bound_started: (ready.inner.bound_source.is_some()
                    || matches!(ready.inner.command, Exact::Restored(_)))
                .then_some(now),
                bound_result: None,
                bound_renewal: None,
                restored_outcome: None,
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
                driver_started: false,
                driver_graceful: false,
                driver_failure: None,
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
                    super::custody::RESERVATION
                        + u64::from(job.checkpoint.lock().expect("staging checkpoint").is_some())
                            * u64::from(COMMAND_BYTES)
                })
                .sum(),
            closed: a.closed,
            retirement_probes: a.retirement_probes,
            retirement_failures: a.retirement_failures,
            retirement_recoveries: a.retirement_recoveries,
            retirement_restarts: a.retirement_restarts,
            retirement_running: a.retirement_probe,
        }
    }
    pub(crate) fn try_quiesce(&self) -> Option<StagingQuiescence> {
        let mut admission = self.inner.admission.lock().expect("staging admission");
        if admission.paused || !admission.jobs.is_empty() || admission.retirement_probe {
            return None;
        }
        admission.paused = true;
        Some(StagingQuiescence {
            inner: Arc::clone(&self.inner),
        })
    }
    /// Seal node admission while already-owned receive workflows finish. Other
    /// callback producers retain their existing cancel-and-physical-drain contract.
    pub(crate) fn close_admission(&self) {
        let mut admission = self.inner.admission.lock().expect("staging admission");
        admission.closed = true;
        for job in admission.jobs.values() {
            let mut local = job.local.lock().expect("staging local");
            if !local.driver_graceful {
                local.stop = true;
                job.driver_stop.cancel();
                job.changed.notify_one();
            }
        }
    }

    pub(crate) async fn finish_receive_workflows(&self) {
        let jobs: Vec<_> = self
            .inner
            .admission
            .lock()
            .expect("staging admission")
            .jobs
            .values()
            .cloned()
            .collect();
        // Timeout abandons only this join observer. Forced close below retains
        // and joins every actual controller, worker and uncertain command.
        let _ = tokio::time::timeout(
            Duration::from_secs(30),
            futures_util::future::join_all(jobs.iter().map(|job| driver::drain(job))),
        )
        .await;
    }

    pub(crate) fn close(&self) {
        let mut admission = self.inner.admission.lock().expect("staging admission");
        admission.closed = true;
        for job in admission.jobs.values() {
            job.local.lock().expect("staging local").stop = true;
            job.driver_stop.cancel();
            job.changed.notify_one();
        }
        self.inner.drained.notify_waiters();
    }
    /// Stop admission and renew while accepted workers drain. Uncertain exact
    /// commands remain charged and returned; explicit recovery remains possible.
    pub async fn close_and_drain(&self) -> Vec<StagingTicket> {
        self.close();
        loop {
            let wake = self.inner.drained.notified();
            tokio::pin!(wake);
            wake.as_mut().enable();
            let pending = {
                let a = self.inner.admission.lock().expect("staging admission");
                if a.jobs.values().all(|j| {
                    matches!(*j.status.borrow(), StagingState::Uncertain(_))
                        && j.local.lock().expect("staging local").workers == 0
                }) {
                    Some(
                        a.jobs
                            .values()
                            .map(|job| StagingTicket {
                                inner: Arc::clone(&self.inner),
                                job: Arc::clone(job),
                            })
                            .collect::<Vec<_>>(),
                    )
                } else {
                    None
                }
            };
            if let Some(pending) = pending {
                for ticket in &pending {
                    driver::drain(&ticket.job).await;
                }
                return pending;
            }
            wake.await;
        }
    }
    #[cfg(test)]
    pub(crate) fn pause_checkpoint_probe_for_test(
        &self,
    ) -> (
        tokio::sync::oneshot::Receiver<()>,
        tokio::sync::oneshot::Sender<()>,
    ) {
        let (entered, receive) = tokio::sync::oneshot::channel();
        let (release, wait) = tokio::sync::oneshot::channel();
        *self
            .inner
            .checkpoint_probe_gate
            .lock()
            .expect("checkpoint gate") = Some((entered, wait));
        (receive, release)
    }
    #[cfg(test)]
    pub(crate) fn fault_for_test(&self, fault: u8) {
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
    /// Order the next producer after the controller has installed its fresh
    /// phase. `wait` independently retains the original committed receipt.
    pub(crate) async fn wait_ready(&self) -> Result<Receipt, Arc<StagingError>> {
        let receipt = self.wait().await?;
        let mut state = self.job.status.subscribe();
        loop {
            match state.borrow_and_update().clone() {
                StagingState::Active(_) | StagingState::Bound(_) => return Ok(receipt),
                StagingState::Uncertain(error) | StagingState::Fenced(error) => return Err(error),
                StagingState::Stopped | StagingState::Published(_) => {
                    return Err(Arc::new(StagingError::Inactive));
                }
                _ => {}
            }
            if state.changed().await.is_err() {
                return Err(Arc::new(StagingError::Worker));
            }
        }
    }
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
    #[cfg(test)]
    pub(crate) fn custody_evidence_for_test(
        &self,
    ) -> Option<(
        cellule_runtime::PendingMutation,
        Option<cellule_runtime::PendingMutation>,
    )> {
        let exact = self.job.exact.lock().expect("staging exact");
        let command = match exact.as_ref()? {
            Exact::Restored(command)
            | Exact::Begin(command)
            | Exact::Claim(command)
            | Exact::Renew(command)
            | Exact::Bind(command)
            | Exact::BoundClaim(command)
            | Exact::BoundRenew(command) => command,
            Exact::Checkpoint(_) | Exact::BoundCheckpoint(_) => return None,
        };
        Some((
            command.evidence().clone(),
            command.registration_evidence().cloned(),
        ))
    }
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
    /// Observe final publication without treating the intermediate Bound phase
    /// as completion. Cancellation only drops this watch receiver.
    pub async fn wait_completion(&self) -> StagingState {
        let mut status = self.job.status.subscribe();
        loop {
            let state = status.borrow_and_update().clone();
            if matches!(
                state,
                StagingState::Published(_)
                    | StagingState::Uncertain(_)
                    | StagingState::Fenced(_)
                    | StagingState::Stopped
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
        self.job.driver_stop.cancel();
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
    /// Original registered identity retained even when fresh custody fails.
    /// This is historical evidence, never upload or preparation permission.
    pub fn restored_evidence(&self) -> Option<&cellule_runtime::PendingMutation> {
        self.job.restored_evidence.as_ref()
    }
    /// Original positive or negative outcome, retained before fresh probes.
    pub fn restored_outcome(&self) -> Option<Arc<Committed<CustodyReply>>> {
        self.job
            .local
            .lock()
            .expect("staging local")
            .restored_outcome
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
        F: FnOnce(Arc<PreparationSession>, StagingContext) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, StagingError>> + Send + 'static,
        T: Send + 'static,
    {
        let session = self.bound_session()?;
        self.spawn_in_phase(true, move |context| producer(session, context))
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
        let node_permit = self
            .inner
            .budget
            .workers
            .try_acquire(crate::ReadIdentity::Account(&self.job.actor))
            .map_err(|_| StagingError::Capacity)?;
        let (token, format) = {
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
            (token, format)
        };
        let guard = Arc::new(Activity {
            inner: Arc::clone(&self.inner),
            job: Arc::clone(&self.job),
            permit: Some(permit),
            actor_permit: Some(actor_permit),
            node_permit: Some(node_permit),
        });
        let context = StagingContext {
            job: Arc::clone(&self.job),
            token,
            format,
            bound,
            activity: Arc::clone(&guard),
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
    pub(super) async fn renew_with_identity_for_test(
        &self,
        identity: MutationIdentity,
    ) -> Result<(), StagingError> {
        let (token, bound) = {
            let local = self.job.local.lock().expect("staging local");
            if local.fenced {
                return Err(StagingError::Inactive);
            }
            match &local.bound {
                Some(session) => (session.live_lease()?.0.token, true),
                None => (local.lease.ok_or(StagingError::NotReady)?.token, false),
            }
        };
        let lease = LeaseRequest {
            check: LeaseCheck {
                token,
                actor: self.job.actor.clone(),
            },
            lease_ms: self.inner.limits.lease_ms,
        };
        let action = if bound {
            CustodyAction::RenewPreparation(lease)
        } else {
            CustodyAction::RenewStaging(lease)
        };
        let command =
            OwnedCustody::prepare(&self.job.client, &self.job.target, action, identity).await?;
        let mut exact = self.job.exact.lock().expect("staging exact");
        assert!(exact.is_none(), "test renewal replaced an original");
        *exact = Some(if bound {
            Exact::BoundRenew(command)
        } else {
            Exact::Renew(command)
        });
        drop(exact);
        self.job.changed.notify_one();
        Ok(())
    }
    /// Inject a short custody ceiling only after a test reaches its intended
    /// recovery phase. The real clock and normal fence/drain path still run.
    #[cfg(test)]
    pub(super) fn limit_bound_ceiling_for_test(
        &self,
        remaining: Duration,
    ) -> Result<Instant, StagingError> {
        let mut local = self.job.local.lock().expect("staging local");
        if local.workers != 0 || local.finishing || remaining.is_zero() {
            return Err(StagingError::Context);
        }
        let mut session = (**local.bound.as_ref().ok_or(StagingError::NotReady)?).clone();
        session.live_lease()?;
        let ceiling = (Instant::now() + remaining).min(local.lifetime);
        session.ceiling = Some(ceiling);
        local.bound = Some(Arc::new(session));
        local.lifetime = ceiling;
        self.job.changed.notify_one();
        Ok(ceiling)
    }
    #[cfg(test)]
    pub(super) fn expire_bound_for_test(&self) -> Result<Instant, StagingError> {
        let mut local = self.job.local.lock().expect("staging local");
        let session = local.bound.as_ref().ok_or(StagingError::NotReady)?;
        let deadline = (Instant::now() + Duration::from_millis(100)).min(session.live_lease()?.1);
        *session.deadline.lock().expect("bound deadline") = deadline;
        local.deadline = deadline;
        local.lifetime = local.lifetime.min(deadline);
        self.job.changed.notify_one();
        Ok(deadline)
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
    node_permit: Option<crate::admission::AdmissionPermit>,
}
impl Drop for Activity {
    fn drop(&mut self) {
        drop(self.actor_permit.take());
        drop(self.permit.take());
        drop(self.node_permit.take());
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
    // Clones share the original worker admission. Detached blocking jobs,
    // native descendants and provider readers must retain it until they drain.
    activity: Arc<Activity>,
}
impl StagingContext {
    /// Lifetime ownership only; it grants no custody or publication authority.
    /// Keep this in every physical worker that can outlive its async observer.
    pub(crate) fn physical_owner(&self) -> crate::git_objects::ReadOwner {
        self.activity.clone()
    }
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
            || l.bound
                .as_ref()
                .is_some_and(|session| session.live_lease().is_err())
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
            let (deadline, session) = {
                let l = self.job.local.lock().expect("staging local");
                if l.fenced || l.bound.is_some() != self.bound {
                    return;
                }
                let mut deadline = l.deadline.min(l.lifetime);
                if let Some(session) = &l.bound {
                    let Ok((_, usable_until)) = session.live_lease() else {
                        return;
                    };
                    deadline = deadline.min(usable_until);
                }
                (deadline, l.bound.clone())
            };
            if Instant::now() >= deadline {
                return;
            }
            tokio::select! {
                _ = sleep_until(deadline) => {},
                _ = async {
                    match session {
                        Some(session) => session.wait_fenced().await,
                        None => std::future::pending::<()>().await,
                    }
                } => return,
                result = status.changed() => { if result.is_err() { return; } }
            }
        }
    }
}
struct WorkSlot<T> {
    result: Mutex<Option<Result<T, Arc<StagingError>>>>,
    ready: watch::Sender<bool>,
    guard: Mutex<Option<Arc<Activity>>>,
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
    job.authority.check(&job.target, token.owner).await?;
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
    job.authority.check(&job.target, token.owner).await?;
    if Instant::now() >= deadline {
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
        if exact.custody_original().is_some() {
            retirement::start(Arc::clone(&inner));
        }
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
            let custody = command.custody_original().is_some();
            let pending = command.pending();
            let result =
                tokio::spawn(command.execute(job.client.clone(), Arc::clone(&job), recover, fault))
                    .await
                    .unwrap_or(Err(pending));
            match result {
                Ok(Outcome::Restored(value)) => {
                    job.exact.lock().expect("staging exact").take();
                    if !restore::accept(&inner, &job, *value).await {
                        return;
                    }
                    recover = false;
                }
                Err(error) if error.uncertain() => {
                    job.status
                        .send_replace(StagingState::Uncertain(Arc::new(error)));
                    inner.drained.notify_waiters();
                    if custody {
                        retirement::start(Arc::clone(&inner));
                    }
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
                    #[cfg(test)]
                    checkpoint_probe_for_test(&inner).await;
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
                        Outcome::Restored(_) => unreachable!("restored outcome handled above"),
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
                    #[cfg(test)]
                    if checkpoint {
                        checkpoint_probe_for_test(&inner).await;
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
                {
                    let mut local = job.local.lock().expect("staging local");
                    local.fenced = true;
                    if let Some(session) = &local.bound {
                        session.fence();
                    }
                    // A failed controller must remain a failure before or after
                    // Bind. Preserve the original receipt as historical evidence;
                    // reporting Bound here would strand completion observers.
                    let state = match &local.driver_failure {
                        Some(error) => StagingState::Fenced(error.clone()),
                        None => local
                            .bound_result
                            .clone()
                            .map(StagingState::Bound)
                            .unwrap_or(StagingState::Stopped),
                    };
                    job.status.send_replace(state);
                }
                driver::drain(&job).await;
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
                    Ok(id) => OwnedCustody::prepare(
                        &job.client,
                        &job.target,
                        CustodyAction::BindStaging(check),
                        id,
                    )
                    .await
                    .map(Exact::Bind)
                    .map_err(StagingError::from),
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
                    Ok(id) => OwnedCustody::prepare(
                        &job.client,
                        &job.target,
                        CustodyAction::RenewPreparation(LeaseRequest {
                            check,
                            lease_ms: inner.limits.lease_ms,
                        }),
                        id,
                    )
                    .await
                    .map(Exact::BoundRenew)
                    .map_err(StagingError::from),
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
                    Ok(id) => OwnedCustody::prepare(
                        &job.client,
                        &job.target,
                        CustodyAction::RenewStaging(LeaseRequest {
                            check,
                            lease_ms: inner.limits.lease_ms,
                        }),
                        id,
                    )
                    .await
                    .map(Exact::Renew)
                    .map_err(StagingError::from),
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
    job.driver_stop.cancel();
    if let Some(registration) = job.checkpoint.lock().expect("staging checkpoint").as_ref() {
        registration.finish(Err(error));
    }
    drain_work(job).await;
    driver::drain(job).await;
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
    drop(
        job.operation_permit
            .lock()
            .expect("staging operation permit")
            .take(),
    );
    let count = a.actors.get_mut(&job.actor).expect("staging actor");
    count.operations -= 1;
    if count.operations == 0 {
        a.actors.remove(&job.actor);
    }
    inner.drained.notify_waiters();
}

#[cfg(test)]
async fn checkpoint_probe_for_test(inner: &Inner) {
    let gate = inner
        .checkpoint_probe_gate
        .lock()
        .expect("checkpoint gate")
        .take();
    if let Some((entered, wait)) = gate {
        let _ = entered.send(());
        let _ = wait.await;
    }
}
