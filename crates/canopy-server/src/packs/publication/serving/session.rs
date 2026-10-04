use super::*;
use crate::{
    ReadIdentity,
    admission::AccountAdmission,
    packs::{
        catalog::{CatalogFiles, CatalogIndexes, CatalogReader},
        metadata::{ObjectHeader, PAGE_OBJECTS},
    },
};
use cellule_runtime::{Committed, InvocationError, Receipt, primitives::sql::SqlCell};
use std::sync::Mutex;
use tokio::{sync::Notify, time::Instant};
use tokio_util::{sync::CancellationToken, task::TaskTracker};
mod handoff;
mod reads;
mod refs;
pub use refs::ResolvedServingRef;

#[derive(Debug, thiserror::Error)]
pub enum ServingReadError {
    #[error("serving pin already has a physical drain owner")]
    AlreadyOwned,
    #[error("serving read is inactive or unavailable")]
    Inactive,
    #[error("invalid serving context or budget")]
    Context,
    #[error("serving refs changed while reading pages")]
    Changed,
    #[error("serving ref snapshot failed")]
    RefSnapshot(#[from] crate::packs::ref_state::RefSnapshotError),
    #[error("serving ref index failed")]
    Refs(#[from] crate::packs::ref_state::RefStateError),
    #[error("serving authority failed")]
    Authority(#[from] PreparationBaseError),
    #[error("serving query failed")]
    Query(#[source] Box<InvocationError<Option<ServingLease>>>),
    #[error("serving generation selection failed")]
    Selection(#[source] Box<InvocationError<Option<GenerationFact>>>),
    #[error("serving metadata failed")]
    Metadata(#[from] crate::packs::directory::index::IndexError),
    #[error("serving capability failed")]
    Capability(#[from] Error),
    #[error("serving custody intent failed")]
    Custody(#[source] Box<CustodyError>),
    #[error("serving encoding failed")]
    Codec(#[from] CodecError),
    #[error("serving worker failed")]
    Task(#[from] tokio::task::JoinError),
    #[error("serving release proof query failed")]
    Proof(#[source] Box<InvocationError<Vec<SqlResultSet>>>),
    #[error("serving release command preparation failed")]
    Prepare(#[source] Box<InvocationError<ServingReleaseReply>>),
}
#[derive(Clone)]
pub struct ServingReadBudget {
    inner: Arc<Budget>,
}
struct Budget {
    admission: AccountAdmission,
    owners: AccountAdmission,
    snapshots: AccountAdmission,
    tasks: TaskTracker,
    stop: CancellationToken,
}
impl ServingReadBudget {
    pub fn new(limit: u16, tasks: TaskTracker) -> Result<Self, ServingReadError> {
        if !(2..=64).contains(&limit) {
            return Err(ServingReadError::Context);
        }
        Ok(Self {
            inner: Arc::new(Budget {
                admission: AccountAdmission::new(
                    usize::from(limit),
                    "node serving reads",
                    "account serving reads",
                ),
                owners: AccountAdmission::new(
                    usize::from(limit),
                    "node serving owners",
                    "account serving owners",
                ),
                snapshots: AccountAdmission::new(
                    usize::from(limit),
                    "node serving snapshots",
                    "account serving snapshots",
                ),
                tasks,
                stop: CancellationToken::new(),
            }),
        })
    }
    pub fn close(&self) {
        self.inner.stop.cancel();
    }
}
/// Trusted service configuration. No decoded catalog or lease DTO supplies
/// closure authority: open reobserves the registered pin through this client.
#[derive(Clone)]
pub struct ServingContext {
    client: CellClient,
    target: CellTarget,
    authority: PreparationAuthority,
    indexes: Arc<CatalogIndexes>,
    files: Arc<CatalogFiles>,
    budget: ServingReadBudget,
    administrator: String,
}
impl ServingContext {
    pub(super) fn repository(&self) -> [u8; 16] {
        self.indexes.store().repository()
    }
    pub(super) fn administrator(&self) -> &str {
        &self.administrator
    }
    pub(super) async fn select(
        &self,
        actor: Option<String>,
    ) -> Result<GenerationFact, ServingReadError> {
        if self.budget.inner.stop.is_cancelled() {
            return Err(ServingReadError::Inactive);
        }
        let scope = actor
            .as_deref()
            .map_or(ReadIdentity::Anonymous, ReadIdentity::Account);
        let permit = self.budget.inner.admission.acquire(scope).await?;
        let context = self.clone();
        self.tasks()
            .spawn(async move {
                let _permit = permit;
                if context.budget.inner.stop.is_cancelled() {
                    return Err(ServingReadError::Inactive);
                }
                context
                    .client
                    .query::<SelectServingGeneration>(
                        &context.target,
                        None,
                        ServingSelection {
                            repository: context.repository(),
                            actor,
                        },
                    )
                    .await
                    .map_err(|error| ServingReadError::Selection(Box::new(error)))?
                    .output
                    .ok_or(ServingReadError::Inactive)
            })
            .await?
    }
    pub(super) fn client_for_owner(&self) -> CellClient {
        self.client.clone()
    }
    pub(super) fn authority_for_owner(&self) -> PreparationAuthority {
        self.authority.clone()
    }
    pub(super) async fn admit_owner(
        &self,
        actor: &str,
    ) -> Result<crate::admission::AdmissionPermit, ServingReadError> {
        if self.budget.inner.stop.is_cancelled() {
            return Err(ServingReadError::Inactive);
        }
        Ok(self
            .budget
            .inner
            .owners
            .acquire(ReadIdentity::Account(actor))
            .await?)
    }
    pub(super) async fn admit_snapshot(
        &self,
        actor: &Option<String>,
    ) -> Result<crate::admission::AdmissionPermit, ServingReadError> {
        if self.budget.inner.stop.is_cancelled() {
            return Err(ServingReadError::Inactive);
        }
        let actor = actor
            .as_deref()
            .map_or(ReadIdentity::Anonymous, ReadIdentity::Account);
        actor.validate()?;
        Ok(self.budget.inner.snapshots.acquire(actor).await?)
    }
    pub(super) fn tasks(&self) -> TaskTracker {
        self.budget.inner.tasks.clone()
    }
    pub(super) fn target_for_handoff(&self) -> CellTarget {
        self.target.clone()
    }
    pub fn new(
        client: CellClient,
        target: CellTarget,
        authority: PreparationAuthority,
        indexes: Arc<CatalogIndexes>,
        files: Arc<CatalogFiles>,
        budget: ServingReadBudget,
        administrator: String,
    ) -> Result<Self, ServingReadError> {
        validate_component(&administrator)?;
        if !authority.matches(&target)
            || crate::repository_target(
                target.tenant(),
                target.application(),
                indexes.store().repository(),
            )? != target
        {
            return Err(ServingReadError::Context);
        }
        Ok(Self {
            client,
            target,
            authority,
            indexes,
            files,
            budget,
            administrator,
        })
    }
}
#[derive(Clone)]
pub struct ServingPin {
    inner: Arc<Inner>,
}
struct Inner {
    // Must outlive every active worker and original release command.
    _exclusive: Arc<()>,
    context: ServingContext,
    lease: ServingLease,
    state: Mutex<Workers>,
    changed: Notify,
    reader: tokio::sync::Mutex<Option<Arc<CatalogReader>>>,
    refs: tokio::sync::OnceCell<crate::packs::ref_state::RefStateSnapshot>,
    release: tokio::sync::Mutex<Option<ReleaseCommand>>,
}
#[derive(Default)]
struct Workers {
    closed: bool,
    active: usize,
    released: bool,
}
pub(super) struct Active(Arc<Inner>);
impl Drop for Active {
    fn drop(&mut self) {
        let mut state = self.0.state.lock().expect("serving workers");
        state.active -= 1;
        drop(state);
        self.0.changed.notify_waiters();
    }
}
struct ReleaseCommand {
    command: Arc<PreparedCommand<ReleaseServingPin>>,
    digest: [u8; 32],
}
impl ServingPin {
    pub(super) fn workers_idle(&self) -> bool {
        let state = self.inner.state.lock().expect("serving workers");
        !state.closed && state.active == 0
    }
    pub(super) async fn authorize(
        &self,
        actor: Option<String>,
    ) -> Result<Instant, ServingReadError> {
        Ok(self.inner.observe(actor).await?.1)
    }
    pub async fn open(
        context: ServingContext,
        token: ServingToken,
        actor: Option<String>,
    ) -> Result<Self, ServingReadError> {
        if context.budget.inner.stop.is_cancelled() {
            return Err(ServingReadError::Inactive);
        }
        let scope = actor
            .as_deref()
            .map_or(ReadIdentity::Anonymous, ReadIdentity::Account);
        let permit = context.budget.inner.admission.acquire(scope).await?;
        let exclusive = super::ownership::reserve(&context.target, token)?;
        let tasks = context.budget.inner.tasks.clone();
        tasks
            .spawn(async move {
                let _permit = permit;
                context
                    .authority
                    .check(&context.target, token.owner)
                    .await?;
                let lease = context
                    .client
                    .query::<CheckServingPin>(&context.target, None, ServingCheck { token, actor })
                    .await
                    .map_err(|error| ServingReadError::Query(Box::new(error)))?
                    .output
                    .ok_or(ServingReadError::Inactive)?;
                context
                    .authority
                    .check(&context.target, token.owner)
                    .await?;
                if lease.token != token || lease.format != context.indexes.sources().format() {
                    return Err(ServingReadError::Context);
                }
                Ok(Self {
                    inner: Arc::new(Inner {
                        _exclusive: exclusive,
                        context,
                        lease,
                        state: Mutex::new(Workers::default()),
                        changed: Notify::new(),
                        reader: tokio::sync::Mutex::new(None),
                        refs: tokio::sync::OnceCell::new(),
                        release: tokio::sync::Mutex::new(None),
                    }),
                })
            })
            .await?
    }
    pub fn token(&self) -> ServingToken {
        self.inner.lease.token
    }
    pub fn fact(&self) -> GenerationFact {
        self.inner.lease.fact
    }
    /// Cancellation only detaches observation. The tracked worker retains read
    /// admission and the physical-drain guard until all metadata work finishes.
    pub async fn headers(
        &self,
        actor: Option<String>,
        ids: &[crate::ObjectId],
    ) -> Result<Vec<Option<ObjectHeader>>, ServingReadError> {
        if ids.is_empty()
            || ids.len() > PAGE_OBJECTS
            || ids
                .iter()
                .any(|oid| oid.is_zero() || oid.format() != self.inner.lease.format)
        {
            return Err(ServingReadError::Context);
        }
        let ids = ids.to_vec();
        self.read_owned(actor, move |inner, deadline| async move {
            let reader = {
                let mut reader = inner.reader.lock().await;
                if reader.is_none() {
                    *reader = Some(Arc::new(
                        CatalogReader::open(
                            Arc::clone(&inner.context.indexes),
                            inner.lease.fact.catalog.ok_or(ServingReadError::Context)?,
                        )
                        .await?,
                    ));
                }
                Arc::clone(reader.as_ref().expect("opened serving catalog"))
            };
            if Instant::now() >= deadline {
                return Err(ServingReadError::Inactive);
            }
            Ok(reader
                .headers(&ids, &*inner.context.files, &*inner.context.files)
                .await?)
        })
        .await
    }
    /// Own the exact renewal and its drain guard before yielding to a caller.
    /// The coordinator retains both across held/unknown states and transport loss.
    pub async fn ready_renew(
        &self,
        actor: String,
        request_digest: [u8; 32],
        identity: MutationIdentity,
        lease_ms: u64,
    ) -> Result<ReadyServingCommand, ServingReadError> {
        if self.inner.context.budget.inner.stop.is_cancelled() {
            return Err(ServingReadError::Inactive);
        }
        let guard = {
            let mut state = self.inner.state.lock().expect("serving workers");
            if state.closed {
                return Err(ServingReadError::Inactive);
            }
            state.active += 1;
            Arc::new(Active(Arc::clone(&self.inner)))
        };
        let ctx = &self.inner.context;
        ctx.authority.check(&ctx.target, self.token().owner).await?;
        ReadyServingCommand::renew(
            ctx.client.clone(),
            ctx.target.clone(),
            RenewServingRequest {
                check: ServingCheck {
                    token: self.token(),
                    actor: Some(actor),
                },
                lease_ms,
            },
            request_digest,
            identity,
            guard,
        )
        .await
        .map_err(|error| ServingReadError::Custody(Box::new(error)))
    }
    /// Closing is sticky. Cancellation cannot reopen acquisition while workers
    /// or a retained original release command remain owned by this service.
    pub async fn close_and_drain(&self) {
        self.inner.state.lock().expect("serving workers").closed = true;
        loop {
            let changed = self.inner.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.inner.state.lock().expect("serving workers").active == 0 {
                return;
            }
            changed.await;
        }
    }
    pub async fn ready_release(
        &self,
        identity: MutationIdentity,
    ) -> Result<ReadyServingRelease, ServingReadError> {
        self.close_and_drain().await;
        let mut retained = self.inner.release.lock().await;
        if self.inner.state.lock().expect("serving workers").released {
            return Err(ServingReadError::Inactive);
        }
        if let Some(original) = retained.as_ref() {
            return Ok(ReadyServingRelease {
                inner: Arc::clone(&self.inner),
                command: original.command.clone(),
                digest: original.digest,
            });
        }
        let ctx = &self.inner.context;
        ctx.authority.check(&ctx.target, self.token().owner).await?;
        let sql = SqlCell::<RepositoryModule>::new(ctx.client.clone(), ctx.target.clone())?;
        let seed = sql
            .query(
                None,
                super::super::sql::statement(
                    "SELECT push_cert_seed FROM repository_identity WHERE singleton=1 AND owner=?1",
                    vec![SqlValue::Text(ctx.administrator.clone())],
                ),
            )
            .await
            .map_err(|error| ServingReadError::Proof(Box::new(error)))?;
        let Some([seed]) = super::super::sql::rows(&seed.output)?
            .first()
            .map(Vec::as_slice)
        else {
            return Err(ServingReadError::Inactive);
        };
        let data = DrainData {
            tenant: *ctx.target.tenant().as_bytes(),
            application: *ctx.target.application().as_bytes(),
            token: self.token(),
            administrator: ctx.administrator.clone(),
        };
        let proof = ServingDrainProof(super::super::certificate::CertificateEnvelope::seal(
            &data,
            &super::super::sql::fixed(seed)?,
        )?);
        let mut bytes = BoundedEncoder::new(1024)?;
        proof.encode(&mut bytes)?;
        let digest = *blake3::hash(&bytes.finish()).as_bytes();
        let command = ctx
            .client
            .prepare_command::<ReleaseServingPin>(&ctx.target, identity, proof)
            .await
            .map_err(|error| ServingReadError::Prepare(Box::new(error)))?;
        let command = Arc::new(command);
        *retained = Some(ReleaseCommand {
            command: command.clone(),
            digest,
        });
        Ok(ReadyServingRelease {
            inner: Arc::clone(&self.inner),
            command,
            digest,
        })
    }
}
impl Inner {
    async fn observe(&self, actor: Option<String>) -> Result<(Receipt, Instant), ServingReadError> {
        let ctx = &self.context;
        ctx.authority
            .check(&ctx.target, self.lease.token.owner)
            .await?;
        let started = Instant::now();
        let observed = ctx
            .client
            .query::<CheckServingPin>(
                &ctx.target,
                None,
                ServingCheck {
                    token: self.lease.token,
                    actor,
                },
            )
            .await
            .map_err(|error| ServingReadError::Query(Box::new(error)))?;
        let lease = observed.output.ok_or(ServingReadError::Inactive)?;
        if lease.token != self.lease.token
            || lease.fact != self.lease.fact
            || lease.format != self.lease.format
        {
            return Err(ServingReadError::Context);
        }
        let remaining = u64::try_from(lease.expires_at_ms - lease.observed_at_ms)
            .map_err(|_| ServingReadError::Inactive)?;
        let deadline = started
            .checked_add(std::time::Duration::from_millis(
                remaining.min(MAX_LEASE_MS),
            ))
            .ok_or(ServingReadError::Context)?;
        ctx.authority
            .check(&ctx.target, self.lease.token.owner)
            .await?;
        if Instant::now() >= deadline {
            return Err(ServingReadError::Inactive);
        }
        Ok((observed.receipt, deadline))
    }
}
#[must_use]
pub struct ReadyServingRelease {
    inner: Arc<Inner>,
    command: Arc<PreparedCommand<ReleaseServingPin>>,
    digest: [u8; 32],
}
impl ReadyServingRelease {
    pub(in crate::packs::publication) fn token(&self) -> ServingToken {
        self.inner.lease.token
    }

    pub fn evidence(&self) -> &cellule_runtime::PendingMutation {
        self.command.evidence()
    }
    pub(in crate::packs::publication) fn dispatch_copy(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            command: self.command.clone(),
            digest: self.digest,
        }
    }
    pub(in crate::packs::publication) fn context(
        &self,
    ) -> (&CellClient, &CellTarget, BeginRequest) {
        let ctx = &self.inner.context;
        (
            &ctx.client,
            &ctx.target,
            BeginRequest {
                repository: self.inner.lease.token.repository,
                operation: self.inner.lease.token.reader,
                request_digest: self.digest,
                actor: ctx.administrator.clone(),
                lease_ms: DEFAULT_LEASE_MS,
            },
        )
    }
    pub(in crate::packs::publication) fn pending(&self) -> PublicationError {
        PublicationError::ServingRelease(InvocationError::Pending(Box::new(
            self.command.evidence().clone(),
        )))
    }
    pub(in crate::packs::publication) async fn dispatch(
        self,
        recover: bool,
        fault: u8,
    ) -> Result<Committed<ServingReleaseReply>, InvocationError<ServingReleaseReply>> {
        let client = self.inner.context.client.clone();
        let inner = Arc::clone(&self.inner);
        let result = super::super::exact::invoke_guarded(
            &client,
            (*self.command).clone(),
            recover,
            128,
            fault,
            move || {
                let state = self.inner.state.lock().expect("serving workers");
                if !state.closed || state.active != 0 {
                    return Err(Error::Command("serving workers have not drained"));
                }
                Ok(())
            },
        )
        .await;
        if !matches!(
            &result,
            Err(InvocationError::Pending(_) | InvocationError::InvalidPublishedResult { .. })
        ) {
            // Drop the cached original before the coordinator releases credits.
            let mut retained = inner.release.lock().await;
            if matches!(&result,Ok(value) if value.output==ServingReleaseReply::Released) {
                inner.state.lock().expect("serving workers").released = true;
            }
            retained.take();
        }
        result
    }
}
