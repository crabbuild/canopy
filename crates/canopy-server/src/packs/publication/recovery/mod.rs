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
mod codec;
mod ready;
#[cfg(test)]
mod tests;
pub use ready::ReadyRootRecovery;
pub(super) use ready::reservation as ready_reservation;
mod registration;
pub use registration::RegisterRootRecovery;

const ROOT_BYTES: u32 = 4096;
const DOMAIN: &[u8] = b"canopy.root-command-recovery.v1\0";

#[derive(Debug, thiserror::Error)]
pub enum RootRecoveryError {
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
}
#[derive(Clone, Debug, PartialEq, Eq)]
struct Record {
    check: LeaseCheck,
    tenant: [u8; 16],
    application: [u8; 16],
    kind: Kind,
    root: StoredInputRoot,
}
#[derive(Clone, Debug, PartialEq, Eq)]
struct Bundle {
    snapshot: PreparedCommandSnapshot,
    body: ArtifactDescriptor,
}

/// Trusted service recovery capability, never a response/read authorization.
/// Loading needs no current write lease. Dispatch resolves the original SDK
/// outcome first; proven absence alone permits reacquiring live custody.
#[derive(Clone)]
pub struct RegisteredRootRecovery {
    record: Record,
    bundle: Bundle,
}
impl RegisteredRootRecovery {
    pub fn evidence(&self) -> &PendingMutation {
        self.bundle.snapshot.evidence()
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
        let sql = SqlCell::<RepositoryModule>::new(client.clone(), target.clone())?;
        let result = sql.query(None, SqlBatch { statements: vec![
            SqlStatement { sql: "SELECT recovery FROM catalog_leases WHERE incarnation=?1 AND admission_sequence=?2 AND operation=?3 AND owner_epoch=?4 AND artifact_operation=?5".into(), parameters: vec![blob(check.token.owner.incarnation.as_bytes()), number(check.token.attempt)?, blob(check.token.operation), blob(check.token.owner.epoch.to_be_bytes()), blob(check.token.artifact_operation)] },
            SqlStatement { sql: "SELECT push_cert_seed FROM repository_identity WHERE singleton=1 AND repository_id=?1".into(), parameters: vec![blob(check.token.repository)] },
        ] }).await.map_err(|error| RootRecoveryError::Query(Box::new(error)))?;
        let Some([value]) = rows(&result.output)?.first().map(Vec::as_slice) else {
            return Ok(None);
        };
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
        if record.check != *check
            || record.tenant != *target.tenant().as_bytes()
            || record.application != *target.application().as_bytes()
        {
            return Err(RootRecoveryError::Context);
        }
        let bundle = record.root.read::<Bundle>(store, ROOT_BYTES).await?;
        if bundle.snapshot.evidence().target() != target
            || bundle.snapshot.evidence().incarnation() != check.token.owner.incarnation
        {
            return Err(RootRecoveryError::Context);
        }
        Ok(Some(Self { record, bundle }))
    }
    pub(in crate::packs::publication) async fn dispatch(
        &self,
        client: &CellClient,
        store: &ArtifactStore,
    ) -> Result<Committed<RootCompletionReply>, InvocationError<RootCompletionReply>> {
        match self.record.kind {
            Kind::Publish => {
                self.dispatch_command::<CompleteRootPush>(client, store)
                    .await
            }
            Kind::Outcome => {
                self.dispatch_command::<CompleteRootOutcome>(client, store)
                    .await
            }
        }
    }
    async fn dispatch_command<C: Command<Output = RootCompletionReply>>(
        &self,
        client: &CellClient,
        store: &ArtifactStore,
    ) -> Result<Committed<RootCompletionReply>, InvocationError<RootCompletionReply>> {
        if let Some(known) = super::exact::known::<C>(client, self.evidence(), 512).await? {
            return Ok(known);
        }
        let command = match self.restore::<C>(client, store).await {
            Ok(command) => command,
            Err(error) => {
                if let Some(known) = super::exact::known::<C>(client, self.evidence(), 512).await? {
                    return Ok(known);
                }
                return Err(InvocationError::NotStarted(Error::Facility {
                    name: "root command restore",
                    source: Box::new(error),
                }));
            }
        };
        let session = match PreparationSession::open(
            client.clone(),
            self.evidence().target().clone(),
            self.record.check.clone(),
            None,
        )
        .await
        {
            Ok(session) => session,
            Err(error) => {
                // The original can finish while body I/O or the custody query
                // is in flight. A missing operation must not hide that receipt.
                if let Some(known) = super::exact::known::<C>(client, self.evidence(), 512).await? {
                    return Ok(known);
                }
                return Err(InvocationError::NotStarted(Error::Facility {
                    name: "root recovery custody",
                    source: Box::new(error),
                }));
            }
        };
        // Resolve again after artifact I/O and the fresh custody query. A racing
        // completion still returns its original receipt before the local guard.
        super::exact::resolve(client, command, 512, move || {
            session
                .live_lease()
                .map(|_| ())
                .map_err(|_| Error::Command("inactive durable root preparation"))
        })
        .await
    }
    async fn restore<C: Command>(
        &self,
        client: &CellClient,
        store: &ArtifactStore,
    ) -> Result<PreparedCommand<C>, RootRecoveryError> {
        target_matches(self.evidence().target(), store, &self.record.check)?;
        // This bound is checked before opening or allocating the body. The
        // snapshot's broader global wire ceiling cannot widen a root command.
        if self.bundle.body.size == 0 || self.bundle.body.size > u64::from(ROOT_COMPLETION_BYTES) {
            return Err(RootRecoveryError::Context);
        }
        let key = ArtifactKey {
            operation: self.token().artifact_operation,
            binding_digest: self.bundle.body.digest,
            kind: ArtifactKind::InputBody,
        };
        let mut reader = store.read(key, self.bundle.body).await?;
        let mut bytes = Vec::with_capacity(self.bundle.body.size as usize);
        while let Some(part) = reader.next().await? {
            bytes.extend_from_slice(&part);
        }
        Ok(client.restore_command::<C>(self.bundle.snapshot.clone(), bytes)?)
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
    let (client, target, check) = session.capability();
    target_matches(target, store, check)?;
    session.live_lease()?;
    if command.evidence().target() != target
        || command.evidence().incarnation() != check.token.owner.incarnation
        || command.input_bytes().is_empty()
        || command.input_bytes().len() > ROOT_COMPLETION_BYTES as usize
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
    let bundle = Bundle {
        snapshot: command.snapshot(),
        body,
    };
    let root =
        StoredInputRoot::upload(store, check.token.artifact_operation, &bundle, ROOT_BYTES).await?;
    let record = Record {
        check: check.clone(),
        tenant: *target.tenant().as_bytes(),
        application: *target.application().as_bytes(),
        kind,
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
