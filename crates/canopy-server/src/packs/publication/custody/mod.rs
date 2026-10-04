//! Exact custody commands, including the interval before an artifact namespace exists.
//! This is command metadata, never an object inventory or a custody permission.
use super::{
    certificate::CertificateEnvelope,
    recovery::{Stamp, phase::Recorded},
    sql::*,
    *,
};
use cellule_runtime::{
    CellClient, CellTarget, Committed, InvocationError, MutationIdentity, PendingMutation,
    PreparedCommandSnapshot, primitives::sql::SqlCell,
};
mod codec;
mod commands;
mod dispatch;
mod scan;
mod stop;
pub use commands::{ExecuteCustody, RegisterCustodyIntent};
pub(super) use dispatch::{OwnedCustody, RESERVATION};
pub use scan::{CustodyScanStats, CustodySupervisor};
pub use stop::{
    CustodyStopFact, CustodyStopInput, CustodyStopOutcome, CustodyStopReply, ReadyCustodyStop,
    StopCustodyIntent,
};

const INPUT_BYTES: u32 = 1024;
const INTENT_BYTES: u32 = 4096;
const MAX_STEPS: u32 = 65_535;
const DOMAIN: &[u8] = b"canopy.custody-command-intent.v2\0";

/// Creating and serving requests retain separate exact command histories.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum CustodyPurpose {
    Creating,
    Serving,
}
impl CustodyPurpose {
    fn number(self) -> u8 {
        match self {
            Self::Creating => 0,
            Self::Serving => 1,
        }
    }
    fn parse(value: u8) -> Result<Self, CodecError> {
        match value {
            0 => Ok(Self::Creating),
            1 => Ok(Self::Serving),
            _ => Err(CodecError::Invalid("custody purpose")),
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct CustodyKey {
    purpose: CustodyPurpose,
    operation: [u8; 16],
}
impl From<[u8; 16]> for CustodyKey {
    fn from(operation: [u8; 16]) -> Self {
        Self {
            purpose: CustodyPurpose::Creating,
            operation,
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CustodyAction {
    BeginPreparation(BeginRequest),
    ClaimPreparation(LeaseRequest),
    RenewPreparation(LeaseRequest),
    BeginStaging(BeginRequest),
    ClaimStaging(LeaseRequest),
    RenewStaging(LeaseRequest),
    BindStaging(LeaseCheck),
    AcquireServing(BeginRequest),
    RenewServing {
        request: RenewServingRequest,
        request_digest: [u8; 32],
    },
}
impl CustodyAction {
    fn identity(&self) -> ([u8; 16], [u8; 16], [u8; 32], &str) {
        match self {
            Self::BeginPreparation(r) | Self::BeginStaging(r) | Self::AcquireServing(r) => {
                (r.repository, r.operation, r.request_digest, &r.actor)
            }
            Self::ClaimPreparation(r)
            | Self::RenewPreparation(r)
            | Self::ClaimStaging(r)
            | Self::RenewStaging(r) => identity_of(&r.check),
            Self::BindStaging(r) => identity_of(r),
            Self::RenewServing {
                request,
                request_digest,
            } => (
                request.check.token.repository,
                request.check.token.reader,
                *request_digest,
                request.check.actor.as_deref().unwrap_or(""),
            ),
        }
    }
    fn purpose(&self) -> CustodyPurpose {
        if matches!(self, Self::AcquireServing(_) | Self::RenewServing { .. }) {
            CustodyPurpose::Serving
        } else {
            CustodyPurpose::Creating
        }
    }
    fn key(&self) -> CustodyKey {
        CustodyKey {
            purpose: self.purpose(),
            operation: self.identity().1,
        }
    }
    fn staging(&self) -> bool {
        matches!(
            self,
            Self::BeginStaging(_) | Self::ClaimStaging(_) | Self::RenewStaging(_)
        )
    }
    fn begin(&self) -> bool {
        matches!(
            self,
            Self::BeginPreparation(_) | Self::BeginStaging(_) | Self::AcquireServing(_)
        )
    }
}
fn identity_of(check: &LeaseCheck) -> ([u8; 16], [u8; 16], [u8; 32], &str) {
    (
        check.token.repository,
        check.token.operation,
        check.token.request_digest,
        &check.actor,
    )
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CustodyReply {
    Preparation(PreparationReply),
    Staging(StagingReply),
    Serving(ServingReply),
}
impl CustodyReply {
    fn rejected(&self) -> bool {
        matches!(
            self,
            Self::Preparation(PreparationReply::Denied(_))
                | Self::Staging(StagingReply::Denied(_))
                | Self::Serving(ServingReply::Denied(_))
        )
    }
}

/// Private ordinal/predecessor fields prevent a factory from replacing an
/// unsettled head. Decoded values remain untrusted until receiver verification.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CustodyRequest {
    step: u32,
    previous: Option<[u8; 32]>,
    action: CustodyAction,
}
#[derive(Clone, Debug, PartialEq, Eq)]
struct Header {
    purpose: CustodyPurpose,
    tenant: [u8; 16],
    application: [u8; 16],
    incarnation: IncarnationId,
    stamp: Stamp,
    repository: [u8; 16],
    operation: [u8; 16],
    request_digest: [u8; 32],
    actor: String,
    step: u32,
    previous: Option<[u8; 32]>,
    bundle_digest: [u8; 32],
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CustodyIntent {
    certificate: CertificateEnvelope,
    snapshot: PreparedCommandSnapshot,
    body: Vec<u8>,
}
impl CustodyIntent {
    fn request(&self) -> Result<CustodyRequest, CodecError> {
        decode(&self.body, INPUT_BYTES)
    }
    fn header(&self) -> Result<Header, CodecError> {
        self.certificate.data()
    }
    fn validate(&self, target: &CellTarget, seed: &[u8; 32]) -> cellule_runtime::Result<Header> {
        let header = self.header()?;
        let request = self.request()?;
        let evidence = self.snapshot.evidence();
        let (repository, operation, digest, actor) = request.action.identity();
        if header.purpose != request.action.purpose()
            || !self.certificate.authenticated(seed)
            || header.tenant != *target.tenant().as_bytes()
            || header.application != *target.application().as_bytes()
            || crate::repository_target(target.tenant(), target.application(), repository)?
                != *target
            || evidence.target() != target
            || header.incarnation != evidence.incarnation()
            || header.stamp != Stamp::of(evidence)
            || header.repository != repository
            || header.operation != operation
            || header.request_digest != digest
            || header.actor != actor
            || header.step != request.step
            || header.previous != request.previous
            || header.bundle_digest != bundle_digest(&self.snapshot, &self.body)?
        {
            return Err(Error::Command("custody intent binding differs"));
        }
        Ok(header)
    }
    fn encoded(&self) -> Result<Vec<u8>, CodecError> {
        encode(self, INTENT_BYTES)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum CustodyError {
    #[error("custody command clock failed")]
    Clock(#[source] Box<crate::server::ServerError>),
    #[error("custody command encoding failed")]
    Codec(#[from] CodecError),
    #[error("custody command binding failed")]
    Capability(#[from] Error),
    #[error("custody command query failed")]
    Query(#[source] Box<InvocationError<Vec<SqlResultSet>>>),
    #[error("custody command preparation failed")]
    Preparation(#[source] Box<InvocationError<CustodyReply>>),
    #[error("custody intent registration failed")]
    Registration(#[source] Box<InvocationError<RootRecoveryReply>>),
    #[error("custody command has an unsettled predecessor")]
    Unsettled(Box<PendingMutation>),
    #[error("custody command head differs")]
    Context,
    #[error("custody original was retired without an execution result")]
    Stopped(Box<CustodyStopFact>),
    #[error("custody owner observation failed")]
    Owner(#[source] Box<PreparationBaseError>),
    #[error("custody stop preparation failed")]
    StopPreparation(#[source] Box<InvocationError<CustodyStopReply>>),
    #[error("invalid custody scan limits")]
    InvalidScanLimits,
}

impl CustodyError {
    pub(super) fn uncertain(&self) -> bool {
        match self {
            Self::Registration(error) => matches!(
                &**error,
                InvocationError::Pending(_) | InvocationError::InvalidPublishedResult { .. }
            ),
            Self::Preparation(error) => matches!(
                &**error,
                InvocationError::Pending(_) | InvocationError::InvalidPublishedResult { .. }
            ),
            Self::Clock(_)
            | Self::Stopped(_)
            | Self::Owner(_)
            | Self::InvalidScanLimits
            | Self::StopPreparation(_) => false,
            // Failure to authenticate or observe metadata is never proof of
            // absence. Keep the owned original until its disposition is known.
            Self::Query(_)
            | Self::Codec(_)
            | Self::Capability(_)
            | Self::Unsettled(_)
            | Self::Context => true,
        }
    }
}

/// Retains the original SDK snapshot/body even if registration loses its reply.
/// Registration must become discoverable before this command can execute.
#[derive(Clone)]
#[must_use]
pub struct PreparedCustody {
    intent: CustodyIntent,
}
#[derive(Clone)]
pub struct RegisteredCustody {
    intent: CustodyIntent,
    phase: Option<Recorded>,
    stopped: Option<stop::StopRecord>,
}

fn encode(value: &impl WireValue, limit: u32) -> Result<Vec<u8>, CodecError> {
    let mut encoder = BoundedEncoder::new(limit)?;
    value.encode(&mut encoder)?;
    Ok(encoder.finish())
}
fn decode<T: WireValue>(bytes: &[u8], limit: u32) -> Result<T, CodecError> {
    let mut decoder = BoundedDecoder::new(bytes, limit)?;
    let value = T::decode(&mut decoder)?;
    decoder.finish()?;
    Ok(value)
}
fn bundle_digest(snapshot: &PreparedCommandSnapshot, body: &[u8]) -> Result<[u8; 32], CodecError> {
    let mut hash = blake3::Hasher::new();
    hash.update(DOMAIN);
    let bytes = snapshot.to_bytes()?;
    for part in [bytes.as_slice(), body] {
        hash.update(&(part.len() as u64).to_be_bytes());
        hash.update(part);
    }
    Ok(*hash.finalize().as_bytes())
}
fn seed_statement() -> SqlStatement {
    SqlStatement {
        sql: "SELECT push_cert_seed FROM repository_identity WHERE singleton=1".into(),
        parameters: vec![],
    }
}
impl Header {
    fn key(&self) -> CustodyKey {
        CustodyKey {
            purpose: self.purpose,
            operation: self.operation,
        }
    }
}
fn row_statement(key: impl Into<CustodyKey>, step: Option<u32>) -> SqlStatement {
    let key = key.into();
    let mut parameters = vec![
        blob(key.operation),
        SqlValue::Integer(i64::from(key.purpose.number())),
    ];
    let sql = if let Some(step) = step {
        parameters.push(SqlValue::Integer(i64::from(step)));
        "SELECT step,incarnation,request_id,intent,phase,stopped FROM catalog_custody_commands WHERE operation=?1 AND purpose=?2 AND step=?3"
    } else {
        "SELECT step,incarnation,request_id,intent,phase,stopped FROM catalog_custody_commands WHERE operation=?1 AND purpose=?2 ORDER BY step DESC LIMIT 1"
    };
    SqlStatement {
        sql: sql.into(),
        parameters,
    }
}
fn from_sets(
    sets: &[SqlResultSet],
    target: &CellTarget,
    key: impl Into<CustodyKey>,
) -> cellule_runtime::Result<Option<RegisteredCustody>> {
    let key = key.into();
    let Some(row) = rows(sets)?.first() else {
        return Ok(None);
    };
    let [
        SqlValue::Integer(step),
        incarnation,
        request_id,
        SqlValue::Blob(bytes),
        phase,
        stopped,
    ] = row.as_slice()
    else {
        return Err(Error::Command("invalid custody command row"));
    };
    let intent: CustodyIntent = decode(bytes, INTENT_BYTES)?;
    let seed =
        super::attestation::seed(sets.get(1..).ok_or(Error::Command("custody seed absent"))?)?;
    let header = intent.validate(target, &seed)?;
    if i64::from(header.step) != *step
        || header.key() != key
        || fixed::<16>(incarnation)? != *header.incarnation.as_bytes()
        || fixed::<16>(request_id)? != *intent.snapshot.evidence().identity().request_id.as_bytes()
    {
        return Err(Error::Command("custody command row binding differs"));
    }
    let phase = match phase {
        SqlValue::Null => None,
        SqlValue::Blob(bytes) => Some(decode::<Recorded>(bytes, 1024)?),
        _ => return Err(Error::Command("invalid custody command phase")),
    };
    if let Some(phase) = &phase {
        codec::validate_phase(phase, &intent.request()?)?;
    }
    let stopped = stop::record(stopped, &intent, &seed)?;
    if phase.is_some() && stopped.is_some() {
        return Err(Error::Command("custody execution and retirement coexist"));
    }
    Ok(Some(RegisteredCustody {
        intent,
        phase,
        stopped,
    }))
}
async fn load(
    client: &CellClient,
    target: &CellTarget,
    key: impl Into<CustodyKey>,
    step: Option<u32>,
) -> Result<Option<RegisteredCustody>, CustodyError> {
    let key = key.into();
    let sql = SqlCell::<RepositoryModule>::new(client.clone(), target.clone())?;
    let output = sql
        .query(
            None,
            SqlBatch {
                statements: vec![row_statement(key, step), seed_statement()],
            },
        )
        .await
        .map_err(|error| CustodyError::Query(Box::new(error)))?;
    Ok(from_sets(&output.output, target, key)?)
}
impl PreparedCustody {
    pub async fn prepare(
        client: &CellClient,
        target: &CellTarget,
        action: CustodyAction,
        identity: MutationIdentity,
    ) -> Result<Self, CustodyError> {
        let (repository, _, digest, actor) = action.identity();
        if crate::repository_target(target.tenant(), target.application(), repository)? != *target {
            return Err(CustodyError::Context);
        }
        let head = load(client, target, action.key(), None).await?;
        let (step, previous) = if let Some(head) = head {
            let header = head.intent.header()?;
            if header.repository != repository
                || header.request_digest != digest
                || header.actor != actor
            {
                return Err(CustodyError::Context);
            }
            if !head.closed() {
                return Err(CustodyError::Unsettled(Box::new(head.evidence().clone())));
            }
            (
                header.step.checked_add(1).ok_or(CustodyError::Context)?,
                Some(*blake3::hash(&head.intent.encoded()?).as_bytes()),
            )
        } else {
            (0, None)
        };
        let request = CustodyRequest {
            step,
            previous,
            action,
        };
        let command = client
            .prepare_command::<ExecuteCustody>(target, identity, request)
            .await
            .map_err(|error| CustodyError::Preparation(Box::new(error)))?;
        let snapshot = command.snapshot();
        let body = command.input_bytes().to_vec();
        let request: CustodyRequest = decode(&body, INPUT_BYTES)?;
        let (repository, operation, request_digest, actor) = request.action.identity();
        let header = Header {
            purpose: request.action.purpose(),
            tenant: *target.tenant().as_bytes(),
            application: *target.application().as_bytes(),
            incarnation: command.evidence().incarnation(),
            stamp: Stamp::of(command.evidence()),
            repository,
            operation,
            request_digest,
            actor: actor.into(),
            step,
            previous,
            bundle_digest: bundle_digest(&snapshot, &body)?,
        };
        let sql = SqlCell::<RepositoryModule>::new(client.clone(), target.clone())?;
        let output = sql
            .query(
                None,
                SqlBatch {
                    statements: vec![seed_statement()],
                },
            )
            .await
            .map_err(|error| CustodyError::Query(Box::new(error)))?;
        let seed = super::attestation::seed(&output.output)?;
        let intent = CustodyIntent {
            certificate: CertificateEnvelope::seal(&header, &seed)?,
            snapshot,
            body,
        };
        intent.encoded()?;
        Ok(Self { intent })
    }
    pub fn evidence(&self) -> &PendingMutation {
        self.intent.snapshot.evidence()
    }
    #[cfg(test)]
    pub(super) fn command_for_test(
        &self,
        client: &CellClient,
    ) -> cellule_runtime::Result<cellule_runtime::PreparedCommand<ExecuteCustody>> {
        client.restore_command::<ExecuteCustody>(
            self.intent.snapshot.clone(),
            self.intent.body.clone(),
        )
    }
    #[cfg(test)]
    pub(super) fn intent_for_test(&self) -> CustodyIntent {
        self.intent.clone()
    }
    pub async fn register(
        &self,
        client: &CellClient,
        identity: MutationIdentity,
    ) -> Result<RegisteredCustody, CustodyError> {
        let header = self.intent.header()?;
        let target = self.evidence().target();
        if let Some(saved) = load(client, target, header.key(), Some(header.step)).await? {
            return if saved.intent == self.intent {
                Ok(saved)
            } else {
                Err(CustodyError::Context)
            };
        }
        // The domain pointer proves registration even after the registration
        // identity expires. Absence after an uncertain reply proves nothing.
        let result = client
            .command::<RegisterCustodyIntent>(target, identity, self.intent.clone())
            .await;
        let saved = load(client, target, header.key(), Some(header.step)).await?;
        if let Some(saved) = saved {
            if saved.intent == self.intent {
                return Ok(saved);
            }
            return Err(CustodyError::Context);
        }
        match result {
            Err(error) => Err(CustodyError::Registration(Box::new(error))),
            Ok(_) => Err(CustodyError::Context),
        }
    }
}
impl RegisteredCustody {
    /// Trusted private service inventory; loading does not grant current Write.
    pub async fn load_latest(
        client: &CellClient,
        target: &CellTarget,
        operation: [u8; 16],
    ) -> Result<Option<Self>, CustodyError> {
        load(client, target, operation, None).await
    }
    pub async fn load_for(
        client: &CellClient,
        target: &CellTarget,
        purpose: CustodyPurpose,
        operation: [u8; 16],
    ) -> Result<Option<Self>, CustodyError> {
        load(client, target, CustodyKey { purpose, operation }, None).await
    }
    pub fn evidence(&self) -> &PendingMutation {
        self.intent.snapshot.evidence()
    }
    pub fn action(&self) -> Result<CustodyAction, CodecError> {
        Ok(self.intent.request()?.action)
    }
    pub fn settled(&self) -> bool {
        self.phase.is_some()
    }
    /// Logical closure is separate from an original execution result.
    pub fn closed(&self) -> bool {
        self.phase.is_some() || self.stopped.is_some()
    }
    pub fn stop_fact(&self) -> Option<CustodyStopFact> {
        self.stopped
            .as_ref()
            .map(|record| record.fact(self.evidence().target()))
    }
    pub async fn recover_preparation(
        &self,
        client: &CellClient,
    ) -> Result<Committed<PreparationReply>, InvocationError<PreparationReply>> {
        project(self.recover(client).await, |reply| match reply {
            CustodyReply::Preparation(reply) => Some(reply),
            _ => None,
        })
    }
    pub async fn recover_serving(
        &self,
        client: &CellClient,
    ) -> Result<Committed<ServingReply>, InvocationError<ServingReply>> {
        project(self.recover(client).await, |reply| match reply {
            CustodyReply::Serving(reply) => Some(reply),
            _ => None,
        })
    }
    pub async fn recover_staging(
        &self,
        client: &CellClient,
    ) -> Result<Committed<StagingReply>, InvocationError<StagingReply>> {
        project(self.recover(client).await, |reply| match reply {
            CustodyReply::Staging(reply) => Some(reply),
            _ => None,
        })
    }
    pub async fn recover(
        &self,
        client: &CellClient,
    ) -> Result<Committed<CustodyReply>, InvocationError<CustodyReply>> {
        self.recover_guarded(client, || Ok(())).await
    }
    async fn recover_guarded(
        &self,
        client: &CellClient,
        before_execute: impl FnOnce() -> Result<(), Error> + Send,
    ) -> Result<Committed<CustodyReply>, InvocationError<CustodyReply>> {
        let evidence = self.evidence();
        let recover = async {
            let header = self.intent.header()?;
            let current = load(client, evidence.target(), header.key(), Some(header.step))
                .await
                .map_err(|_| Error::Command("custody phase query failed"))?
                .ok_or(Error::Command("custody intent disappeared"))?;
            if current.intent != self.intent {
                return Err(Error::Command("custody recovery binding differs"));
            }
            if current.stopped.is_some() {
                return Err(Error::Command("custody original retired without execution"));
            }
            if let Some(phase) = current.phase {
                return Ok(Some(phase.committed(evidence)?));
            }
            Ok::<_, Error>(None::<Committed<CustodyReply>>)
        }
        .await;
        // Keep original evidence on storage/codec failures, rather than
        // misclassifying them as permission to execute or replace the head.
        match recover {
            Ok(Some(known)) => return normalize(known),
            Ok(None) => {}
            Err(_) => return Err(InvocationError::Pending(Box::new(evidence.clone()))),
        }
        if super::exact::known::<ExecuteCustody>(client, evidence, 512)
            .await?
            .is_some()
        {
            // Atomic receiver publication requires a domain phase whenever SDK
            // reports acceptance. Missing application knowledge is corruption.
            return Err(InvocationError::Pending(Box::new(evidence.clone())));
        }
        before_execute().map_err(InvocationError::NotStarted)?;
        let command = client
            .restore_command::<ExecuteCustody>(
                self.intent.snapshot.clone(),
                self.intent.body.clone(),
            )
            .map_err(InvocationError::NotStarted)?;
        normalize(command.execute().await?)
    }
}
fn normalize(
    committed: Committed<CustodyReply>,
) -> Result<Committed<CustodyReply>, InvocationError<CustodyReply>> {
    if committed.output.rejected() {
        Err(InvocationError::Rejected(Box::new(committed)))
    } else {
        Ok(committed)
    }
}

pub(super) fn project<T>(
    result: Result<Committed<CustodyReply>, InvocationError<CustodyReply>>,
    output: impl FnOnce(CustodyReply) -> Option<T>,
) -> Result<Committed<T>, InvocationError<T>> {
    let committed = |value: Committed<CustodyReply>| {
        let receipt = value.receipt;
        output(value.output)
            .map(|output| Committed { output, receipt })
            .ok_or(InvocationError::InvalidPublishedResult {
                receipt,
                source: Box::new(Error::Command("custody reply purpose differs")),
            })
    };
    match result {
        Ok(value) => committed(value),
        Err(InvocationError::Rejected(value)) => {
            Err(InvocationError::Rejected(Box::new(committed(*value)?)))
        }
        Err(InvocationError::Pending(evidence)) => Err(InvocationError::Pending(evidence)),
        Err(InvocationError::NotStarted(error)) => Err(InvocationError::NotStarted(error)),
        Err(InvocationError::InvalidPublishedResult { receipt, source }) => {
            Err(InvocationError::InvalidPublishedResult { receipt, source })
        }
    }
}

/// A recorded grant can authorize allocating a *new* attempt after the old row
/// was reaped. It never reinstates the old namespace, generation pin or clock.
pub(super) fn restart_matches(
    context: &CommandContext<'_, '_>,
    check: &LeaseCheck,
    staging: bool,
) -> cellule_runtime::Result<bool> {
    let sets = context.sql(&SqlBatch { statements: vec![SqlStatement {
        sql: "SELECT step,incarnation,request_id,intent,phase,stopped FROM catalog_custody_commands INDEXED BY catalog_custody_grants WHERE purpose=0 AND operation=?1 AND granted_incarnation=?2 AND granted_attempt=?3 ORDER BY step DESC LIMIT 1".into(),
        parameters: vec![blob(check.token.operation), blob(check.token.owner.incarnation.as_bytes()), number(check.token.attempt)?],
    }, seed_statement()] })?;
    let Some(saved) = from_sets(&sets, context.target(), check.token.operation)? else {
        return Ok(false);
    };
    let header = saved.intent.header()?;
    if header.actor != check.actor {
        return Ok(false);
    }
    let Some(phase) = saved.phase else {
        return Err(Error::Command("custody restart grant is unsettled"));
    };
    Ok(match phase.decode_reply::<CustodyReply>()? {
        CustodyReply::Preparation(PreparationReply::Granted(lease)) if !staging => {
            lease.token == check.token
        }
        CustodyReply::Staging(StagingReply::Granted(lease)) if staging => {
            lease.token == check.token
        }
        _ => false,
    })
}
