//! Durable exact root commands reuse attempt pins and immutable input artifacts.
//! The first registered bundle wins. No final command is submitted before its
//! pointer is known durable; an uncertain registration cannot authorize a loser.
use super::*;
use super::{certificate::CertificateEnvelope, sql::*};
use crate::packs::{
    directory::index::codec::{artifact, fixed as wire_fixed, read_artifact},
    input_artifact::StoredInputRoot,
};
use canopy_object_storage::artifact::{
    ArtifactDescriptor, ArtifactKey, ArtifactKind, ArtifactStore,
};
use cellule_runtime::{
    CellClient, CellTarget, Committed, InvocationError, MutationIdentity, PendingMutation,
    PreparedCommand, PreparedCommandSnapshot, primitives::sql::SqlCell,
};
pub(in crate::packs::publication) mod archive;
mod codec;
mod initialization;
mod ready;
mod supervisor;
pub use archive::{
    ReadyTerminalRelease, ReleaseTerminalRecovery, TerminalReleaseCertificate,
    TerminalReleaseInput, TerminalReleaseReply,
};
pub use supervisor::{RecoveryScanLimits, RecoveryScanStats, RecoverySupervisor};
#[cfg(test)]
mod tests;
pub use ready::ReadyRootRecovery;
mod registration;
pub use registration::RegisterRootRecovery;
pub(in crate::packs::publication) mod phase;
pub(in crate::packs::publication) use phase::execute;
#[cfg(test)]
pub(in crate::packs::publication) use phase::normalize_root;

const ROOT_BYTES: u32 = 8192;
const DOMAIN: &[u8] = b"canopy.publication-command-recovery.v4\0";

#[derive(Debug, thiserror::Error)]
pub enum RootRecoveryError {
    #[error("closed native audit graph failed")]
    Audit(#[from] NativeResultError),
    #[error("terminal release command preparation failed")]
    Release(#[source] Box<InvocationError<TerminalReleaseReply>>),
    #[error("invalid restart recovery scan limits")]
    InvalidScanLimits,
    #[error("root command recovery encoding failed")]
    Codec(#[from] CodecError),
    #[error("root command recovery artifact failed")]
    Artifact(#[from] canopy_object_storage::artifact::ArtifactError),
    #[error("root command recovery metadata failed")]
    Root(#[from] crate::packs::InputRootError),
    #[error("root command recovery capability failed")]
    Capability(#[from] Error),
    #[error("root command recovery preparation is inactive")]
    Base(#[from] PreparationBaseError),
    #[error("root command recovery registration failed")]
    Registration(#[source] Box<InvocationError<RootRecoveryReply>>),
    #[error("root command recovery query failed")]
    Query(#[source] Box<InvocationError<Vec<SqlResultSet>>>),
    #[error("root command recovery binding differs")]
    Context,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RootRecoveryCertificate(CertificateEnvelope);
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RootRecoveryReply {
    Registered,
    Denied(PreparationDenial),
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Kind {
    Publish,
    Outcome,
    Policy,
    Initialization,
}
impl Kind {
    fn body_limit(self) -> u32 {
        if self == Self::Initialization {
            INITIALIZATION_BYTES
        } else if self == Self::Policy {
            REF_POLICY_PAGE_BYTES
        } else {
            ROOT_COMPLETION_BYTES
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::packs::publication) struct Stamp {
    identity: MutationIdentity,
    digest: [u8; 32],
}
impl Stamp {
    pub(in crate::packs::publication) fn of(evidence: &PendingMutation) -> Self {
        Self {
            identity: evidence.identity(),
            digest: *evidence.operation_digest().as_bytes(),
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
struct Record {
    check: LeaseCheck,
    tenant: [u8; 16],
    application: [u8; 16],
    kind: Kind,
    primary: Stamp,
    refusal: Option<Stamp>,
    previous: Option<StoredInputRoot>,
    step: u64,
    root: StoredInputRoot,
}
#[derive(Clone, Debug, PartialEq, Eq)]
struct SavedCommand {
    snapshot: PreparedCommandSnapshot,
    body: ArtifactDescriptor,
}
#[derive(Clone, Debug, PartialEq, Eq)]
struct Bundle {
    kind: Kind,
    primary: SavedCommand,
    refusal: Option<SavedCommand>,
}

enum AttemptError<T> {
    Invocation(InvocationError<T>),
    Phase(Box<RootRecoveryError>),
}
impl<T> From<InvocationError<T>> for AttemptError<T> {
    fn from(error: InvocationError<T>) -> Self {
        Self::Invocation(error)
    }
}
impl<T> AttemptError<T> {
    fn publication(
        self,
        evidence: &PendingMutation,
        wrap: impl FnOnce(InvocationError<T>) -> PublicationError,
    ) -> PublicationError {
        match self {
            Self::Invocation(error) => wrap(error),
            Self::Phase(source) => PublicationError::Recovery {
                evidence: Box::new(evidence.clone()),
                source,
            },
        }
    }
}
/// Trusted service recovery capability, never a response/read authorization.
/// Loading needs no current write lease. Dispatch observes the durable phase
/// before SDK resolution; proven absence alone permits reacquiring live custody.
#[derive(Clone)]
pub struct RegisteredRootRecovery {
    record: Record,
    bundle: Bundle,
    certificate: RootRecoveryCertificate,
}
impl RegisteredRootRecovery {
    pub(in crate::packs::publication) fn matches_original(
        &self,
        kind: Kind,
        primary: &PendingMutation,
        refusal: Option<&PendingMutation>,
        session: &PreparationSession,
        store: &ArtifactStore,
    ) -> bool {
        self.record.kind == kind
            && self.record.check == session.check
            && self.evidence() == primary
            && self
                .bundle
                .refusal
                .as_ref()
                .map(|saved| saved.snapshot.evidence())
                == refusal
            && primary.target() == &session.target
            && target_matches(primary.target(), store, &session.check).is_ok()
    }
    pub fn evidence(&self) -> &PendingMutation {
        self.bundle.primary.snapshot.evidence()
    }
    pub fn token(&self) -> PreparationToken {
        self.record.check.token
    }
    /// Reconstruct from the independent pin, including after operation removal,
    /// lease expiry or write revocation. This uses the existing private SQL
    /// capability, which must never be exposed on a product surface.
    pub async fn load(
        client: &CellClient,
        target: &CellTarget,
        store: &ArtifactStore,
        check: &LeaseCheck,
    ) -> Result<Option<Self>, RootRecoveryError> {
        target_matches(target, store, check)?;
        let loaded = Self::load_pin(
            client,
            target,
            store,
            check.token.owner.incarnation,
            check.token.attempt,
            Some(check),
        )
        .await?;
        match loaded {
            Some(value) => Ok(Some(value)),
            None => Self::load_archive(client, target, store, check).await,
        }
    }
    async fn load_pin(
        client: &CellClient,
        target: &CellTarget,
        store: &ArtifactStore,
        incarnation: IncarnationId,
        attempt: u64,
        expected: Option<&LeaseCheck>,
    ) -> Result<Option<Self>, RootRecoveryError> {
        let sql = SqlCell::<RepositoryModule>::new(client.clone(), target.clone())?;
        let result = sql.query(None, SqlBatch { statements: vec![
            SqlStatement { sql: "SELECT recovery,operation,owner_epoch,artifact_operation FROM catalog_leases WHERE incarnation=?1 AND admission_sequence=?2".into(), parameters: vec![blob(incarnation.as_bytes()), number(attempt)?] },
            SqlStatement { sql: "SELECT push_cert_seed FROM repository_identity WHERE singleton=1 AND repository_id=?1".into(), parameters: vec![blob(store.repository())] },
        ] }).await.map_err(|error| RootRecoveryError::Query(Box::new(error)))?;
        let Some([value, operation, epoch, artifact_operation]) =
            rows(&result.output)?.first().map(Vec::as_slice)
        else {
            return Ok(None);
        };
        let operation = fixed::<16>(operation)?;
        let epoch = u64::from_be_bytes(fixed::<8>(epoch)?);
        let artifact_operation = fixed::<16>(artifact_operation)?;
        if expected.is_some_and(|check| {
            check.token.operation != operation
                || check.token.owner.epoch != epoch
                || check.token.artifact_operation != artifact_operation
        }) {
            return Ok(None);
        }
        let SqlValue::Blob(bytes) = value else {
            if *value == SqlValue::Null {
                return Ok(None);
            }
            return Err(RootRecoveryError::Context);
        };
        let mut d = BoundedDecoder::new(bytes, CERTIFICATE_BYTES)?;
        let certificate = RootRecoveryCertificate::decode(&mut d)?;
        d.finish()?;
        let seed =
            super::attestation::seed(result.output.get(1..).ok_or(RootRecoveryError::Context)?)?;
        if !certificate.0.authenticated(&seed) {
            return Err(RootRecoveryError::Context);
        }
        let record = certificate.0.data::<Record>()?;
        if expected.is_some_and(|check| record.check != *check)
            || record.check.token.owner.incarnation != incarnation
            || record.check.token.attempt != attempt
            || record.check.token.operation != operation
            || record.check.token.owner.epoch != epoch
            || record.check.token.artifact_operation != artifact_operation
            || record.tenant != *target.tenant().as_bytes()
            || record.application != *target.application().as_bytes()
        {
            return Err(RootRecoveryError::Context);
        }
        target_matches(target, store, &record.check)?;
        let bundle = record.root.read::<Bundle>(store, ROOT_BYTES).await?;
        if bundle.kind != record.kind
            || Stamp::of(bundle.primary.snapshot.evidence()) != record.primary
            || bundle
                .refusal
                .as_ref()
                .map(|value| Stamp::of(value.snapshot.evidence()))
                != record.refusal
            || std::iter::once(&bundle.primary)
                .chain(bundle.refusal.iter())
                .any(|value| {
                    value.snapshot.evidence().target() != target
                        || value.snapshot.evidence().incarnation() != incarnation
                })
        {
            return Err(RootRecoveryError::Context);
        }
        Ok(Some(Self {
            record,
            bundle,
            certificate,
        }))
    }
    #[cfg(test)]
    pub(in crate::packs::publication) fn command_bodies_for_test(
        &self,
    ) -> Vec<(ArtifactKey, ArtifactDescriptor)> {
        std::iter::once(&self.bundle.primary)
            .chain(self.bundle.refusal.iter())
            .map(|saved| {
                (
                    ArtifactKey {
                        operation: self.token().artifact_operation,
                        binding_digest: saved.body.digest,
                        kind: ArtifactKind::InputBody,
                    },
                    saved.body,
                )
            })
            .collect()
    }
    pub(in crate::packs::publication) async fn dispatch(
        &self,
        client: &CellClient,
        store: &ArtifactStore,
    ) -> Result<Committed<RootCompletionReply>, PublicationError> {
        self.dispatch_root(client, store, None).await
    }
    async fn dispatch_root(
        &self,
        client: &CellClient,
        store: &ArtifactStore,
        original: Option<&PreparationSession>,
    ) -> Result<Committed<RootCompletionReply>, PublicationError> {
        let result = match self.record.kind {
            Kind::Publish => {
                self.dispatch_command::<CompleteRootPush>(client, store, false, original)
                    .await
            }
            Kind::Outcome => {
                self.dispatch_command::<CompleteRootOutcome>(client, store, false, original)
                    .await
            }
            Kind::Policy | Kind::Initialization => {
                Err(AttemptError::Invocation(InvocationError::NotStarted(
                    Error::Command("recovery kind requires typed phase dispatch"),
                )))
            }
        };
        match result {
            Ok(value) => phase::normalize_root(Ok(value)).map_err(PublicationError::RootPush),
            Err(error) => Err(error.publication(self.evidence(), PublicationError::RootPush)),
        }
    }
    pub(super) async fn dispatch_any(
        &self,
        client: &CellClient,
        store: &ArtifactStore,
        refusing: &std::sync::atomic::AtomicBool,
    ) -> Result<PublicationOutcome, PublicationError> {
        self.dispatch_bound(
            client,
            store,
            refusing,
            None,
            #[cfg(test)]
            None,
        )
        .await
    }
    pub(super) async fn dispatch_bound(
        &self,
        client: &CellClient,
        store: &ArtifactStore,
        refusing: &std::sync::atomic::AtomicBool,
        original: Option<&PreparationSession>,
        #[cfg(test)] refusal_fault: Option<&std::sync::atomic::AtomicU8>,
    ) -> Result<PublicationOutcome, PublicationError> {
        if self.record.kind == Kind::Initialization {
            return self
                .dispatch_initialization(client, store, original)
                .await
                .map(PublicationOutcome::Initialization);
        }
        if self.record.kind != Kind::Policy {
            let result = match original {
                Some(original) => self.dispatch_root(client, store, Some(original)).await,
                None => self.dispatch(client, store).await,
            };
            return result.map(PublicationOutcome::RootPush);
        }
        let result = self
            .dispatch_command::<RegisterRefPolicyPage>(client, store, false, original)
            .await;
        let refused = match &result {
            Ok(value) => {
                matches!(value.output, RefPolicyReply::Denied(_))
                    || matches!(value.output, RefPolicyReply::Registered(progress) if !progress.valid)
            }
            Err(AttemptError::Invocation(InvocationError::Rejected(_))) => true,
            _ => false,
        };
        if refused {
            let journal = self
                .current_journal(client, Some(store))
                .await
                .map_err(|source| PublicationError::Recovery {
                    evidence: Box::new(self.evidence().clone()),
                    source: Box::new(source),
                })?;
            if !journal
                .refused(&self.record)
                .map_err(|source| PublicationError::Recovery {
                    evidence: Box::new(self.evidence().clone()),
                    source: Box::new(RootRecoveryError::Codec(source)),
                })?
            {
                return Err(PublicationError::PolicyPage(InvocationError::Pending(
                    Box::new(self.evidence().clone()),
                )));
            }
            refusing.store(true, std::sync::atomic::Ordering::Release);
            let evidence = self
                .bundle
                .refusal
                .as_ref()
                .ok_or_else(|| PublicationError::Recovery {
                    evidence: Box::new(self.evidence().clone()),
                    source: Box::new(RootRecoveryError::Context),
                })?
                .snapshot
                .evidence();
            // The phase-specific probe is test-only and consumed once at
            // the actual fallback boundary. Recovery retains this same command.
            #[cfg(test)]
            let fault = refusal_fault
                .map(|fault| fault.swap(0, std::sync::atomic::Ordering::AcqRel))
                .unwrap_or(0);
            #[cfg(test)]
            if fault == 1 {
                return Err(PublicationError::RootPush(InvocationError::Pending(
                    Box::new(evidence.clone()),
                )));
            }
            let outcome = self
                .dispatch_command::<CompleteRootOutcome>(client, store, true, original)
                .await;
            #[cfg(test)]
            if fault == 2 {
                return Err(PublicationError::RootPush(InvocationError::Pending(
                    Box::new(evidence.clone()),
                )));
            }
            #[cfg(test)]
            assert_ne!(
                fault, 3,
                "injected registered refusal panic after execution"
            );
            return match outcome {
                Ok(value) => phase::normalize_root(Ok(value))
                    .map(PublicationOutcome::RootPush)
                    .map_err(PublicationError::RootPush),
                Err(error) => Err(error.publication(evidence, PublicationError::RootPush)),
            };
        }
        result
            .map(PublicationOutcome::PolicyPage)
            .map_err(|error| error.publication(self.evidence(), PublicationError::PolicyPage))
    }
    async fn known<C: Command>(
        &self,
        client: &CellClient,
        store: &ArtifactStore,
        refusal: bool,
    ) -> Result<Option<Committed<C::Output>>, AttemptError<C::Output>> {
        let journal = self
            .current_journal(client, Some(store))
            .await
            .map_err(|error| AttemptError::Phase(Box::new(error)))?;
        let saved = self.saved(refusal).map_err(|error| {
            InvocationError::NotStarted(Error::Facility {
                name: "frozen publication command",
                source: Box::new(error),
            })
        })?;
        if let Some(recorded) = if refusal {
            journal.refusal
        } else {
            journal.primary
        } {
            return recorded
                .committed(saved.snapshot.evidence())
                .map(Some)
                .map_err(|source| AttemptError::Phase(Box::new(RootRecoveryError::Codec(source))));
        }
        super::exact::known::<C>(client, saved.snapshot.evidence(), 512)
            .await
            .map_err(AttemptError::Invocation)
    }
    async fn dispatch_command<C: Command>(
        &self,
        client: &CellClient,
        store: &ArtifactStore,
        refusal: bool,
        original: Option<&PreparationSession>,
    ) -> Result<Committed<C::Output>, AttemptError<C::Output>> {
        if let Some(known) = self.known::<C>(client, store, refusal).await? {
            return Ok(known);
        }
        let command = match self.restore::<C>(client, store, refusal).await {
            Ok(command) => command,
            Err(error) => {
                if let Some(known) = self.known::<C>(client, store, refusal).await? {
                    return Ok(known);
                }
                return Err(AttemptError::Invocation(InvocationError::NotStarted(
                    Error::Facility {
                        name: "publication command restore",
                        source: Box::new(error),
                    },
                )));
            }
        };
        let refusal_only = match self.refusal_only(&command, refusal) {
            Ok(value) => value,
            Err(error) => {
                if let Some(known) = self.known::<C>(client, store, refusal).await? {
                    return Ok(known);
                }
                return Err(AttemptError::Invocation(InvocationError::NotStarted(
                    Error::Facility {
                        name: "original refusal purpose",
                        source: Box::new(error),
                    },
                )));
            }
        };
        // An original authenticated refusal cannot publish. The command still
        // checks actual owner, live operation/pin, expiry and exact checkpoint;
        // opening a new Write-dependent session would prevent revocation from
        // ever reaching its already frozen terminal negative result.
        // Live bound work already owns the original session. Replacing it
        // would change both fencing semantics and refusal behavior: the final
        // transaction must still be able to select rejection after ACL loss.
        // Standalone recovery reacquires custody for positive work.
        // Initialization performs no new preparation or native work. Its
        // frozen receiver checks Admin, the actual owner, live pin, checkpoint
        // and pristine roots in the committing transaction. Requiring a fresh
        // Write query here would hide an expired/revoked attempt before that
        // original command could record its definitive denial. Bound live
        // initialization still retains and checks its original local guard.
        let session =
            if refusal_only || self.record.kind == Kind::Initialization || original.is_some() {
                None
            } else {
                match PreparationSession::open(
                    client.clone(),
                    self.evidence().target().clone(),
                    self.record.check.clone(),
                    None,
                )
                .await
                {
                    Ok(session) => Some(session),
                    Err(error) => {
                        if let Some(known) = self.known::<C>(client, store, refusal).await? {
                            return Ok(known);
                        }
                        return Err(AttemptError::Invocation(InvocationError::NotStarted(
                            Error::Facility {
                                name: "publication recovery custody",
                                source: Box::new(error),
                            },
                        )));
                    }
                }
            };
        // A command can settle while body I/O or custody acquisition is in flight.
        if let Some(known) = self.known::<C>(client, store, refusal).await? {
            return Ok(known);
        }
        if let Some(session) = session {
            session.live_lease().map_err(|error| {
                InvocationError::NotStarted(Error::Facility {
                    name: "publication recovery guard",
                    source: Box::new(error),
                })
            })?;
        }
        if let Some(original) = original
            && let Err(error) = original.live_lease()
        {
            // A retained known receipt wins even if fencing raced with body
            // loading or fresh custody. Proven absence keeps the original
            // lifecycle fence/clock; a newly opened session cannot replace it.
            if let Some(known) = self.known::<C>(client, store, refusal).await? {
                return Ok(known);
            }
            return Err(AttemptError::Invocation(InvocationError::NotStarted(
                Error::Facility {
                    name: "original publication custody",
                    source: Box::new(error),
                },
            )));
        }
        Box::pin(command.execute())
            .await
            .map_err(AttemptError::Invocation)
    }
    fn refusal_only<C: Command>(
        &self,
        command: &PreparedCommand<C>,
        refusal: bool,
    ) -> Result<bool, CodecError> {
        if self.record.kind != Kind::Outcome && !refusal {
            return Ok(false);
        }
        if C::MODULE != CompleteRootOutcome::MODULE || C::ID != CompleteRootOutcome::ID {
            return Err(CodecError::Invalid("refusal command purpose differs"));
        }
        // Restore has already checked the original body digest and SDK
        // contract. Inspect its authenticated selection constraint unchanged.
        let mut d = BoundedDecoder::new(command.input_bytes(), ROOT_COMPLETION_BYTES)?;
        let input = RootOutcomeCompletion::decode(&mut d)?;
        d.finish()?;
        Ok(input.refusal)
    }
    fn saved(&self, refusal: bool) -> Result<&SavedCommand, RootRecoveryError> {
        if refusal {
            self.bundle
                .refusal
                .as_ref()
                .ok_or(RootRecoveryError::Context)
        } else {
            Ok(&self.bundle.primary)
        }
    }
    async fn restore<C: Command>(
        &self,
        client: &CellClient,
        store: &ArtifactStore,
        refusal: bool,
    ) -> Result<PreparedCommand<C>, RootRecoveryError> {
        target_matches(self.evidence().target(), store, &self.record.check)?;
        let saved = self.saved(refusal)?;
        let limit = if refusal {
            ROOT_COMPLETION_BYTES
        } else {
            self.record.kind.body_limit()
        };
        if saved.body.size == 0 || saved.body.size > u64::from(limit) {
            return Err(RootRecoveryError::Context);
        }
        let key = ArtifactKey {
            operation: self.token().artifact_operation,
            binding_digest: saved.body.digest,
            kind: ArtifactKind::InputBody,
        };
        let mut reader = store.read(key, saved.body).await?;
        let mut bytes = Vec::with_capacity(saved.body.size as usize);
        while let Some(part) = reader.next().await? {
            bytes.extend_from_slice(&part);
        }
        Ok(client.restore_command::<C>(saved.snapshot.clone(), bytes)?)
    }
}
fn target_matches(
    target: &CellTarget,
    store: &ArtifactStore,
    check: &LeaseCheck,
) -> Result<(), RootRecoveryError> {
    if store.repository() != check.token.repository
        || crate::repository_target(
            target.tenant(),
            target.application(),
            check.token.repository,
        )? != *target
    {
        return Err(RootRecoveryError::Context);
    }
    Ok(())
}

pub(super) async fn persist<C: Command>(
    session: &PreparationSession,
    command: &PreparedCommand<C>,
    kind: Kind,
    store: &ArtifactStore,
    identity: MutationIdentity,
    fault: u8,
) -> Result<RegisteredRootRecovery, RootRecoveryError> {
    persist_full(session, command, kind, None, None, store, identity, fault).await
}
#[expect(
    clippy::too_many_arguments,
    reason = "one registration binds primary, refusal and settled predecessor"
)]
pub(super) async fn persist_full<C: Command>(
    session: &PreparationSession,
    command: &PreparedCommand<C>,
    kind: Kind,
    refusal: Option<&PreparedCommand<CompleteRootOutcome>>,
    previous: Option<&RegisteredRootRecovery>,
    store: &ArtifactStore,
    identity: MutationIdentity,
    fault: u8,
) -> Result<RegisteredRootRecovery, RootRecoveryError> {
    let (client, target, check) = session.capability();
    target_matches(target, store, check)?;
    session.live_lease()?;
    if command.evidence().target() != target
        || command.evidence().incarnation() != check.token.owner.incarnation
        || command.input_bytes().is_empty()
        || command.input_bytes().len() > kind.body_limit() as usize
        || (kind == Kind::Policy) != refusal.is_some()
    {
        return Err(RootRecoveryError::Context);
    }
    // Body first, metadata second, pin last. A crash before registration cannot
    // have submitted a final command. Concurrent candidates cannot overwrite
    // the winner; uncertain registration recovers via the canonical pin query.
    let mut bytes = command.input_bytes();
    let digest = *blake3::hash(bytes).as_bytes();
    let body = store
        .put(
            ArtifactKey {
                operation: check.token.artifact_operation,
                binding_digest: digest,
                kind: ArtifactKind::InputBody,
            },
            bytes.len() as u64,
            digest,
            &mut bytes,
        )
        .await?;
    let primary = SavedCommand {
        snapshot: command.snapshot(),
        body,
    };
    let refusal = if let Some(command) = refusal {
        if command.evidence().target() != target
            || command.evidence().incarnation() != check.token.owner.incarnation
            || command.input_bytes().is_empty()
            || command.input_bytes().len() > ROOT_COMPLETION_BYTES as usize
        {
            return Err(RootRecoveryError::Context);
        }
        let mut bytes = command.input_bytes();
        let digest = *blake3::hash(bytes).as_bytes();
        let body = store
            .put(
                ArtifactKey {
                    operation: check.token.artifact_operation,
                    binding_digest: digest,
                    kind: ArtifactKind::InputBody,
                },
                bytes.len() as u64,
                digest,
                &mut bytes,
            )
            .await?;
        Some(SavedCommand {
            snapshot: command.snapshot(),
            body,
        })
    } else {
        None
    };
    let bundle = Bundle {
        kind,
        primary,
        refusal,
    };
    let previous_step = if let Some(value) = previous {
        Some(
            value
                .record
                .step
                .checked_add(1)
                .filter(|step| *step <= 65_535)
                .ok_or(RootRecoveryError::Context)?,
        )
    } else {
        None
    };
    let previous = if let Some(previous) = previous {
        if previous.record.check != *check {
            return Err(RootRecoveryError::Context);
        }
        Some(previous.settled_frame(client, store).await?)
    } else {
        None
    };
    let root =
        StoredInputRoot::upload(store, check.token.artifact_operation, &bundle, ROOT_BYTES).await?;
    let record = Record {
        check: check.clone(),
        tenant: *target.tenant().as_bytes(),
        application: *target.application().as_bytes(),
        kind,
        primary: Stamp::of(command.evidence()),
        refusal: bundle
            .refusal
            .as_ref()
            .map(|value| Stamp::of(value.snapshot.evidence())),
        previous,
        step: match previous {
            None => 0,
            Some(_) => previous_step.ok_or(RootRecoveryError::Context)?,
        },
        root,
    };
    let sql = SqlCell::<RepositoryModule>::new(client.clone(), target.clone())?;
    let queried = sql.query(None, statement("SELECT push_cert_seed FROM repository_identity WHERE singleton=1 AND repository_id=?1", vec![blob(check.token.repository)])).await.map_err(|error| RootRecoveryError::Query(Box::new(error)))?;
    let seed = super::attestation::seed(&queried.output)?;
    let certificate = RootRecoveryCertificate(CertificateEnvelope::seal(&record, &seed)?);
    session.live_lease()?;
    // Do not submit the final command if this result is uncertain. The caller
    // can load the winning pin after restart without restoring this registration.
    let registration = client
        .prepare_command::<RegisterRootRecovery>(target, identity, certificate)
        .await
        .map_err(|error| RootRecoveryError::Registration(Box::new(error)))?;
    super::exact::invoke_guarded(client, registration, false, 128, fault, || {
        session
            .live_lease()
            .map(|_| ())
            .map_err(|_| Error::Command("inactive recovery registration"))
    })
    .await
    .map_err(|error| RootRecoveryError::Registration(Box::new(error)))?;
    let registered = RegisteredRootRecovery::load(client, target, store, check)
        .await?
        .ok_or(RootRecoveryError::Context)?;
    if registered.evidence() != command.evidence() {
        return Err(RootRecoveryError::Context);
    }
    Ok(registered)
}
