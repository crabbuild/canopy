//! Durable creating-input custody. These proofs authorize retaining/reopening
//! descriptors only; physical/canonical/closure/ref proof remains independent.
use super::*;
use super::{certificate::CertificateEnvelope, commands::*, sql::*};
use crate::packs::{
    directory::index::{
        IndexError,
        codec::{fixed as wire_fixed, read_reference, reference},
    },
    sources::{NativeInputIndex, NativeInputRoot, NativePackDescriptor},
};
use crate::{git_gateway::preflight::SavedPushRequest, packs::wire_request::WireRequestRoot};
use canopy_object_storage::artifact::ArtifactStore;
use cellule_runtime::{CellClient, CellTarget, InvocationError, primitives::sql::SqlCell};
use std::sync::Arc;

mod custody;
#[cfg(test)]
mod limits_tests;
pub(in crate::packs) use custody::RetainedNativeInput;
pub(super) use custody::verify_digest;

const DOMAIN: &[u8] = b"canopy.staged-native-inputs.v3\0";
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NativeInputCertificate(CertificateEnvelope);
#[derive(Clone, Debug, PartialEq, Eq)]
struct Inputs {
    tenant: [u8; 16],
    application: [u8; 16],
    token: PreparationToken,
    actor: String,
    format: ObjectFormat,
    root: Option<NativeInputRoot>,
    wire_request: Option<WireRequestRoot>,
    native_result: Option<NativeResultRoot>,
    source: Option<(PreparationToken, [u8; 32])>,
    previous: Option<[u8; 32]>,
}
impl Inputs {
    fn validate(&self) -> Result<(), CodecError> {
        validate_component(&self.actor).map_err(|_| CodecError::Invalid("input actor"))?;
        if let Some(root) = self.root {
            root.validate(self.format)
                .map_err(|_| CodecError::Invalid("input root"))?;
            codec::artifact_valid(root.operation)?;
            if self.source.is_none() && root.operation != self.token.artifact_operation {
                return Err(CodecError::Invalid("input root namespace"));
            }
        }
        if let Some(wire) = self.wire_request {
            wire.validate()?;
            if self.source.is_none() && wire.operation() != self.token.artifact_operation {
                return Err(CodecError::Invalid("wire request namespace"));
            }
        }
        if let Some(native) = self.native_result {
            native.validate()?;
            if self.wire_request.is_none()
                || self.source.is_none() && native.operation() != self.token.artifact_operation
            {
                return Err(CodecError::Invalid("native result namespace"));
            }
        }
        if self.source.is_some_and(|(source, _)| {
            source.repository != self.token.repository
                || source.operation != self.token.operation
                || source.request_digest != self.token.request_digest
        }) {
            return Err(CodecError::Invalid("input adoption source"));
        }
        if self.previous == Some([0; 32]) {
            return Err(CodecError::Invalid("input predecessor"));
        }
        Ok(())
    }
}
impl WireValue for Inputs {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        self.validate()?;
        e.write_bytes(DOMAIN)?;
        e.write_bytes(&self.tenant)?;
        e.write_bytes(&self.application)?;
        self.token.encode(e)?;
        e.write_text(&self.actor)?;
        e.write_u8(self.format.bytes() as u8)?;
        e.write_bool(self.root.is_some())?;
        if let Some(root) = self.root {
            reference(e, root)?;
        }
        e.write_bool(self.wire_request.is_some())?;
        if let Some(root) = self.wire_request {
            root.encode(e)?;
        }
        e.write_bool(self.native_result.is_some())?;
        if let Some(root) = self.native_result {
            root.encode(e)?;
        }
        e.write_bool(self.source.is_some())?;
        if let Some((token, digest)) = self.source {
            token.encode(e)?;
            e.write_bytes(&digest)?;
        }
        e.write_bool(self.previous.is_some())?;
        if let Some(previous) = self.previous {
            e.write_bytes(&previous)?;
        }
        Ok(())
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        if d.read_bytes()? != DOMAIN {
            return Err(CodecError::Invalid("input proof domain"));
        }
        let tenant = wire_fixed(d)?;
        let application = wire_fixed(d)?;
        let token = PreparationToken::decode(d)?;
        let actor = d.read_text()?.into();
        let format = match d.read_u8()? {
            20 => ObjectFormat::Sha1,
            32 => ObjectFormat::Sha256,
            _ => return Err(CodecError::Invalid("input format")),
        };
        let root = if d.read_bool()? {
            Some(read_reference(d, format)?)
        } else {
            None
        };
        let wire_request = if d.read_bool()? {
            Some(WireRequestRoot::decode(d)?)
        } else {
            None
        };
        let native_result = if d.read_bool()? {
            Some(NativeResultRoot::decode(d)?)
        } else {
            None
        };
        let source = if d.read_bool()? {
            Some((PreparationToken::decode(d)?, wire_fixed(d)?))
        } else {
            None
        };
        let previous = if d.read_bool()? {
            Some(wire_fixed(d)?)
        } else {
            None
        };
        let value = Self {
            tenant,
            application,
            token,
            actor,
            format,
            root,
            wire_request,
            native_result,
            source,
            previous,
        };
        value.validate()?;
        Ok(value)
    }
}
impl NativeInputCertificate {
    pub(super) fn scoped_check(&self, target: &CellTarget) -> Result<LeaseCheck, CodecError> {
        let data: Inputs = self.0.data()?;
        if data.tenant != *target.tenant().as_bytes()
            || data.application != *target.application().as_bytes()
            || crate::repository_target(
                target.tenant(),
                target.application(),
                data.token.repository,
            )
            .map_err(|_| CodecError::Invalid("input target"))?
                != *target
        {
            return Err(CodecError::Invalid("input target"));
        }
        Ok(LeaseCheck {
            token: data.token,
            actor: data.actor,
        })
    }
    pub(super) fn bound_digest(
        &self,
        session: &PreparationSession,
    ) -> Result<[u8; 32], CodecError> {
        let data: Inputs = self.0.data()?;
        if self.scoped_check(&session.target)? != session.check
            || data.format != session.lease.format
            || data.source.is_none()
        {
            return Err(CodecError::Invalid("bound input checkpoint context"));
        }
        Ok(*blake3::hash(&self.bytes()?).as_bytes())
    }
    pub fn token(&self) -> Result<PreparationToken, CodecError> {
        Ok(self.0.data::<Inputs>()?.token)
    }
    pub fn root(&self) -> Result<Option<NativeInputRoot>, CodecError> {
        Ok(self.0.data::<Inputs>()?.root)
    }
    pub fn wire_request(&self) -> Result<Option<WireRequestRoot>, CodecError> {
        Ok(self.0.data::<Inputs>()?.wire_request)
    }
    pub fn native_result(&self) -> Result<Option<NativeResultRoot>, CodecError> {
        Ok(self.0.data::<Inputs>()?.native_result)
    }
    pub(super) fn checkpoint_lineage(&self) -> Result<([u8; 32], Option<[u8; 32]>), CodecError> {
        Ok((
            *blake3::hash(&self.bytes()?).as_bytes(),
            self.0.data::<Inputs>()?.previous,
        ))
    }
    fn bytes(&self) -> Result<Vec<u8>, CodecError> {
        let mut e = BoundedEncoder::new(CERTIFICATE_BYTES)?;
        self.encode(&mut e)?;
        Ok(e.finish())
    }
    fn from_bytes(bytes: &[u8]) -> Result<Self, CodecError> {
        let mut d = BoundedDecoder::new(bytes, CERTIFICATE_BYTES)?;
        let value = Self::decode(&mut d)?;
        d.finish()?;
        Ok(value)
    }
}
impl WireValue for NativeInputCertificate {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        self.0.data::<Inputs>()?;
        self.0.encode(e)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        let value = Self(CertificateEnvelope::decode(d)?);
        value.0.data::<Inputs>()?;
        Ok(value)
    }
}
#[derive(Debug, thiserror::Error)]
pub enum InputCheckpointError {
    #[error("input custody failed")]
    Staging(#[from] StagingError),
    #[error("bound input custody failed")]
    Preparation(#[from] PreparationBaseError),
    #[error("bound input custody query failed")]
    BoundCustody(#[source] Box<InvocationError<Option<PreparationLease>>>),
    #[error("input index failed")]
    Index(#[from] IndexError),
    #[error("input proof failed")]
    Codec(#[from] CodecError),
    #[error("input issuer capability failed")]
    Capability(#[from] Error),
    #[error("input issuer query failed")]
    Query(#[source] Box<InvocationError<Vec<SqlResultSet>>>),
    #[error("input custody query failed")]
    Custody(#[source] Box<InvocationError<Option<StagingLease>>>),
    #[error("retained input query failed")]
    Retained(#[source] Box<InvocationError<Option<NativeInputCertificate>>>),
}
impl StagingContext {
    pub(crate) async fn check_push_identity(
        &self,
        expected: &CellTarget,
        input: &BeginRequest,
        format: ObjectFormat,
    ) -> Result<(), InputCheckpointError> {
        self.ensure_live()?;
        let (client, target, check) = self.capability();
        if expected != target
            || input.repository != check.token.repository
            || input.operation != check.token.operation
            || input.request_digest != check.token.request_digest
            || input.actor != check.actor
            || format != self.format()
        {
            return Err(StagingError::Context.into());
        }
        let live = client
            .query::<CheckStaging>(target, None, check.clone())
            .await
            .map_err(|e| InputCheckpointError::Custody(Box::new(e)))?
            .output
            .ok_or(StagingError::Inactive)?;
        if live.token != check.token || live.format != format {
            return Err(StagingError::Context.into());
        }
        self.ensure_live()?;
        Ok(())
    }
    pub(crate) async fn push_checkpoint(
        &self,
    ) -> Result<(NativeInputCertificate, CellTarget, LeaseCheck, ObjectFormat), InputCheckpointError>
    {
        self.ensure_live()?;
        let (client, target, check) = self.capability();
        let proof = client
            .query::<CheckStagedInputs>(target, None, check.clone())
            .await
            .map_err(|e| InputCheckpointError::Retained(Box::new(e)))?;
        let value = proof.output.ok_or(StagingError::Inactive)?;
        let inputs: Inputs = value.0.data()?;
        if value.scoped_check(target)? != check || inputs.format != self.format() {
            return Err(StagingError::Context.into());
        }
        let live = client
            .query::<CheckStaging>(target, Some(proof.receipt), check.clone())
            .await
            .map_err(|e| InputCheckpointError::Custody(Box::new(e)))?
            .output
            .ok_or(StagingError::Inactive)?;
        if live.token != check.token || live.format != self.format() {
            return Err(StagingError::Context.into());
        }
        self.ensure_live()?;
        Ok((value, target.clone(), check, self.format()))
    }
    /// Seals an authenticated descriptor inventory in the creating namespace.
    /// The iterator is consumed incrementally; it need not own a complete list.
    /// Pair existence/decoding is independently established by PhysicalVerifier.
    pub async fn seal_native_inputs<I>(
        &self,
        store: Arc<ArtifactStore>,
        inputs: I,
    ) -> Result<NativeInputCertificate, InputCheckpointError>
    where
        I: IntoIterator<Item = NativePackDescriptor>,
        I::IntoIter: Send,
    {
        self.seal_inputs(store, inputs, None).await
    }
    /// Retains the original encoded push alongside the native inventory in the
    /// same immutable checkpoint. Neither input establishes publication proof.
    pub async fn seal_push_inputs<I>(
        &self,
        store: Arc<ArtifactStore>,
        inputs: I,
        request: SavedPushRequest,
    ) -> Result<NativeInputCertificate, InputCheckpointError>
    where
        I: IntoIterator<Item = NativePackDescriptor>,
        I::IntoIter: Send,
    {
        let (_, target, check) = self.capability();
        let wire = request.scoped_root(target, &check, self.format())?;
        self.seal_inputs(store, inputs, Some(wire)).await
    }
    async fn seal_inputs<I>(
        &self,
        store: Arc<ArtifactStore>,
        inputs: I,
        wire_request: Option<WireRequestRoot>,
    ) -> Result<NativeInputCertificate, InputCheckpointError>
    where
        I: IntoIterator<Item = NativePackDescriptor>,
        I::IntoIter: Send,
    {
        let token = self.token()?;
        if store.repository() != token.repository {
            return Err(StagingError::Context.into());
        }
        let index = NativeInputIndex::new(store, self.format());
        let mut root = None;
        for native in inputs {
            self.ensure_live()?;
            if native.operation != token.artifact_operation {
                return Err(StagingError::Context.into());
            }
            codec::artifact_valid(native.operation)?;
            root = Some(index.insert(root, token.artifact_operation, native).await?);
        }
        self.sign_inputs(root, wire_request, None, None, None).await
    }
    /// Retains an exact immutable input root under the successor pin. Adoption
    /// does not copy an input inventory or recertify native bodies.
    pub async fn adopt_native_inputs(
        &self,
        store: Arc<ArtifactStore>,
        prior: &NativeInputCertificate,
    ) -> Result<NativeInputCertificate, InputCheckpointError> {
        self.ensure_live()?;
        let (client, target, check) = self.capability();
        let (root, wire_request, native_result, source) =
            adoption(client, target, &check, self.format(), store, prior).await?;
        self.sign_inputs(root, wire_request, native_result, Some(source), None)
            .await
    }
    /// Path-copy the registered input tree, preserving its request and every
    /// old descriptor. Command 29 compares the exact prior checkpoint digest.
    pub async fn append_native_inputs<I>(
        &self,
        store: Arc<ArtifactStore>,
        prior: &NativeInputCertificate,
        inputs: I,
    ) -> Result<NativeInputCertificate, InputCheckpointError>
    where
        I: IntoIterator<Item = NativePackDescriptor>,
        I::IntoIter: Send,
    {
        self.append_inputs(store, prior, inputs, None).await
    }
    pub async fn append_native_result<I>(
        &self,
        store: Arc<ArtifactStore>,
        prior: &NativeInputCertificate,
        inputs: I,
        result: SavedNativeResult,
    ) -> Result<NativeInputCertificate, InputCheckpointError>
    where
        I: IntoIterator<Item = NativePackDescriptor>,
        I::IntoIter: Send,
    {
        self.append_inputs(store, prior, inputs, Some(result)).await
    }
    async fn append_inputs<I>(
        &self,
        store: Arc<ArtifactStore>,
        prior: &NativeInputCertificate,
        inputs: I,
        result: Option<SavedNativeResult>,
    ) -> Result<NativeInputCertificate, InputCheckpointError>
    where
        I: IntoIterator<Item = NativePackDescriptor>,
        I::IntoIter: Send,
    {
        let (current, target, check, format) = self.push_checkpoint().await?;
        if &current != prior || store.repository() != check.token.repository {
            return Err(StagingError::Context.into());
        }
        let data: Inputs = prior.0.data()?;
        let native_result = if let Some(result) = result {
            if data.native_result.is_some() {
                return Err(StagingError::Context.into());
            }
            Some(result.scoped_root(
                &target,
                &check,
                format,
                data.wire_request.ok_or(StagingError::Context)?,
                prior.checkpoint_lineage()?.0,
            )?)
        } else {
            data.native_result
        };
        let index = NativeInputIndex::new(store, format);
        let mut root = data.root;
        for native in inputs {
            self.ensure_live()?;
            if native.operation != check.token.artifact_operation {
                return Err(StagingError::Context.into());
            }
            root = Some(
                index
                    .insert(root, check.token.artifact_operation, native)
                    .await?,
            );
        }
        let (current, _, _, _) = self.push_checkpoint().await?;
        if &current != prior || prior.scoped_check(&target)? != check {
            return Err(StagingError::Context.into());
        }
        if data.native_result.is_some() && root != data.root {
            return Err(StagingError::Context.into());
        }
        if root == data.root && native_result == data.native_result {
            return Ok(prior.clone());
        }
        self.sign_inputs(
            root,
            data.wire_request,
            native_result,
            data.source,
            Some(*blake3::hash(&prior.bytes()?).as_bytes()),
        )
        .await
    }
    async fn sign_inputs(
        &self,
        root: Option<NativeInputRoot>,
        wire_request: Option<WireRequestRoot>,
        native_result: Option<NativeResultRoot>,
        source: Option<(PreparationToken, [u8; 32])>,
        previous: Option<[u8; 32]>,
    ) -> Result<NativeInputCertificate, InputCheckpointError> {
        let token = self.token()?;
        let (client, target, check) = self.capability();
        let live = client
            .query::<CheckStaging>(target, None, check.clone())
            .await
            .map_err(|e| InputCheckpointError::Custody(Box::new(e)))?
            .output
            .ok_or(StagingError::Inactive)?;
        if live.token != token || live.format != self.format() {
            return Err(StagingError::Context.into());
        }
        let proof = issue_inputs(
            client,
            target,
            Inputs {
                tenant: *target.tenant().as_bytes(),
                application: *target.application().as_bytes(),
                token: check.token,
                actor: check.actor,
                format: self.format(),
                root,
                wire_request,
                native_result,
                source,
                previous,
            },
        )
        .await?;
        self.ensure_live()?;
        Ok(proof)
    }
}
impl PreparationSession {
    pub(crate) async fn push_checkpoint(
        &self,
    ) -> Result<(NativeInputCertificate, CellTarget, LeaseCheck, ObjectFormat), InputCheckpointError>
    {
        let (lease, _) = self.live_lease()?;
        let (client, target, check) = self.capability();
        let proof = client
            .query::<CheckStagedInputs>(target, None, check.clone())
            .await
            .map_err(|e| InputCheckpointError::Retained(Box::new(e)))?;
        let value = proof.output.ok_or(PreparationBaseError::Inactive)?;
        let inputs: Inputs = value.0.data()?;
        if value.scoped_check(target)? != *check || inputs.format != lease.format {
            return Err(PreparationBaseError::Context.into());
        }
        self.refresh(proof.receipt).await?;
        Ok((value, target.clone(), check.clone(), lease.format))
    }
    /// Recover registered native inputs after a bound preparation Claim. Its
    /// immutable generation floor and input custody are independent facts.
    pub async fn adopt_native_inputs(
        &self,
        store: Arc<ArtifactStore>,
        prior: &NativeInputCertificate,
    ) -> Result<NativeInputCertificate, InputCheckpointError> {
        let (lease, _) = self.live_lease()?;
        let (client, target, check) = self.capability();
        let (root, wire_request, native_result, source) =
            adoption(client, target, check, lease.format, store, prior).await?;
        let current = client
            .query::<CheckPreparation>(target, None, check.clone())
            .await
            .map_err(|e| InputCheckpointError::BoundCustody(Box::new(e)))?
            .output
            .ok_or(PreparationBaseError::Inactive)?;
        if current.token != lease.token
            || current.base != lease.base
            || current.format != lease.format
        {
            return Err(PreparationBaseError::Context.into());
        }
        let proof = issue_inputs(
            client,
            target,
            Inputs {
                tenant: *target.tenant().as_bytes(),
                application: *target.application().as_bytes(),
                token: check.token,
                actor: check.actor.clone(),
                format: lease.format,
                root,
                wire_request,
                native_result,
                source: Some(source),
                previous: None,
            },
        )
        .await?;
        self.live_lease()?;
        Ok(proof)
    }
}
async fn adoption(
    client: &CellClient,
    target: &CellTarget,
    check: &LeaseCheck,
    format: ObjectFormat,
    store: Arc<ArtifactStore>,
    prior: &NativeInputCertificate,
) -> Result<
    (
        Option<NativeInputRoot>,
        Option<WireRequestRoot>,
        Option<NativeResultRoot>,
        (PreparationToken, [u8; 32]),
    ),
    InputCheckpointError,
> {
    let data: Inputs = prior.0.data()?;
    if store.repository() != check.token.repository
        || data.token.repository != check.token.repository
        || data.token.operation != check.token.operation
        || data.token.request_digest != check.token.request_digest
        || data.actor != check.actor
        || data.format != format
        || data.tenant != *target.tenant().as_bytes()
        || data.application != *target.application().as_bytes()
    {
        return Err(StagingError::Context.into());
    }
    let retained = client
        .query::<CheckStagedInputs>(
            target,
            None,
            LeaseCheck {
                token: data.token,
                actor: check.actor.clone(),
            },
        )
        .await
        .map_err(|e| InputCheckpointError::Retained(Box::new(e)))?
        .output;
    if retained.as_ref() != Some(prior) {
        return Err(StagingError::Inactive.into());
    }
    if let Some(root) = data.root {
        NativeInputIndex::new(Arc::clone(&store), format)
            .validate_root(root)
            .await?;
    }
    if let Some(root) = data.wire_request {
        let record = root
            .read(&store)
            .await
            .map_err(|error| StagingError::Input(Box::new(error)))?;
        if !record.matches(target, check, format) {
            return Err(StagingError::Context.into());
        }
    }
    if let Some(root) = data.native_result {
        root.check_request(
            &store,
            data.wire_request.ok_or(StagingError::Context)?,
            target,
            check,
            format,
        )
        .await
        .map_err(|error| StagingError::Input(Box::new(error)))?;
    }
    // The checkpoint's exact immutable root is retained by the source pin and
    // checked again in the destination write. No historical leaf scan/copy is
    // needed to transfer custody. Physical reconstruction must exhaust it.
    Ok((
        data.root,
        data.wire_request,
        data.native_result,
        (data.token, *blake3::hash(&prior.bytes()?).as_bytes()),
    ))
}
async fn issue_inputs(
    client: &CellClient,
    target: &CellTarget,
    data: Inputs,
) -> Result<NativeInputCertificate, InputCheckpointError> {
    let result=SqlCell::<RepositoryModule>::new(client.clone(),target.clone())?.query(None,statement("SELECT push_cert_seed FROM repository_identity WHERE singleton=1 AND repository_id=?1 AND object_format=?2",vec![blob(data.token.repository),SqlValue::Text(data.format.as_str().into())])).await.map_err(|e|InputCheckpointError::Query(Box::new(e)))?;
    let seed = attestation::seed(&result.output)?;
    Ok(NativeInputCertificate(CertificateEnvelope::seal(
        &data, &seed,
    )?))
}
fn denied(reason: PreparationDenial) -> CommandResult<StagingReply> {
    CommandResult::Rejected(StagingReply::Denied(reason))
}
pub struct RegisterStagedInputs;
impl Command for RegisterStagedInputs {
    const MODULE: &'static str = RepositoryModule::NAME;
    const ID: u32 = 29;
    const CODEC_VERSION: u32 = 1;
    type Input = NativeInputCertificate;
    type Output = StagingReply;
    fn execute(
        context: &mut CommandContext<'_, '_>,
        proof: NativeInputCertificate,
    ) -> cellule_runtime::Result<CommandResult<StagingReply>> {
        let data: Inputs = proof.0.data()?;
        if data.tenant != *context.target().tenant().as_bytes()
            || data.application != *context.target().application().as_bytes()
            || data.token.owner != context.owner_fence()
        {
            return Ok(denied(PreparationDenial::Stale));
        }
        let Some(format) = authorized(
            context,
            data.token.repository,
            &data.actor,
            TokenScope::Write,
        )?
        else {
            return Ok(denied(PreparationDenial::Unauthorized));
        };
        let seed = attestation::seed(&context.sql(&statement(
            "SELECT push_cert_seed FROM repository_identity WHERE singleton=1",
            vec![],
        ))?)?;
        if format != data.format || !proof.0.authenticated(&seed) {
            return Ok(denied(PreparationDenial::Conflict));
        }
        let Some(row) = load(context, data.token)? else {
            return Ok(denied(PreparationDenial::Missing));
        };
        if !matched(
            &row,
            &LeaseCheck {
                token: data.token,
                actor: data.actor,
            },
        ) {
            return Ok(denied(PreparationDenial::Stale));
        }
        if row.generation.is_some() && data.source.is_none() {
            return Ok(denied(PreparationDenial::Conflict));
        }
        check_pin(context, &row)?;
        let now = now(context.now_ms())?;
        if row.expires <= now {
            return Ok(denied(PreparationDenial::Expired));
        }
        let bytes = proof.bytes()?;
        let existing = context.sql(&checkpoint(data.token)?)?;
        let Some([old, digest, SqlValue::Integer(_)]) = rows(&existing)?.first().map(Vec::as_slice)
        else {
            return Err(Error::Command("input pin is absent"));
        };
        match (old, digest) {
            (SqlValue::Null, SqlValue::Null) => {
                if data.previous.is_some() {
                    return Ok(denied(PreparationDenial::Conflict));
                }
                if let Some((source, expected)) = data.source {
                    let retained = context.sql(&checkpoint(source)?)?;
                    let Some(
                        [
                            SqlValue::Blob(source_bytes),
                            source_digest,
                            SqlValue::Integer(expires),
                        ],
                    ) = rows(&retained)?.first().map(Vec::as_slice)
                    else {
                        return Ok(denied(PreparationDenial::Missing));
                    };
                    if *expires <= now {
                        return Ok(denied(PreparationDenial::Expired));
                    }
                    if fixed::<32>(source_digest)? != expected
                        || *blake3::hash(source_bytes).as_bytes() != expected
                    {
                        return Ok(denied(PreparationDenial::Conflict));
                    }
                    let source_proof = NativeInputCertificate::from_bytes(source_bytes)?;
                    let previous: Inputs = source_proof.0.data()?;
                    if !source_proof.0.authenticated(&seed)
                        || previous.token != source
                        || previous.actor != row.actor
                        || previous.format != format
                        || previous.tenant != data.tenant
                        || previous.application != data.application
                        || previous.root != data.root
                        || previous.wire_request != data.wire_request
                        || previous.native_result != data.native_result
                    {
                        return Ok(denied(PreparationDenial::Conflict));
                    }
                }
                context.sql(&statement("UPDATE catalog_leases SET input_checkpoint=?1,input_checkpoint_digest=?2 WHERE incarnation=?3 AND admission_sequence=?4", vec![blob(&bytes),blob(blake3::hash(&bytes).as_bytes()),blob(data.token.owner.incarnation.as_bytes()),number(data.token.attempt)?]))?;
            }
            (SqlValue::Blob(old), SqlValue::Blob(digest))
                if old == &bytes && digest.as_slice() == blake3::hash(&bytes).as_bytes() => {}
            (SqlValue::Blob(old), SqlValue::Blob(digest))
                if data.previous == Some(*blake3::hash(old).as_bytes())
                    && digest.as_slice() == blake3::hash(old).as_bytes() =>
            {
                // Only the private append issuer can attest complete preservation
                // of the old tree. SQL still compares the exact predecessor and
                // rejects updates after Bind or beyond the bounded revision cap.
                let prior = NativeInputCertificate::from_bytes(old)?;
                let prior_data: Inputs = prior.0.data()?;
                if row.generation.is_some()
                    || !prior.0.authenticated(&seed)
                    || prior_data.token != data.token
                    || prior_data.actor != row.actor
                    || prior_data.tenant != data.tenant
                    || prior_data.application != data.application
                    || prior_data.format != data.format
                    || prior_data.source != data.source
                    || prior_data.wire_request != data.wire_request
                    || prior_data.native_result.is_some()
                    || (data.root == prior_data.root
                        && data.native_result == prior_data.native_result)
                    || (data.root.is_none() && prior_data.root.is_some())
                    || data
                        .native_result
                        .is_some_and(|root| root.operation() != data.token.artifact_operation)
                    || prior_data.root.is_some_and(|old| {
                        data.root.is_none_or(|new| {
                            new.record_count < old.record_count
                                || new.object_count < old.object_count
                        })
                    })
                {
                    return Ok(denied(PreparationDenial::Conflict));
                }
                let changed=context.sql(&statement("UPDATE catalog_leases SET input_checkpoint=?1,input_checkpoint_digest=?2,input_checkpoint_previous_digest=?3,input_checkpoint_revision=input_checkpoint_revision+1 WHERE incarnation=?4 AND admission_sequence=?5 AND input_checkpoint_digest=?3 AND input_checkpoint_revision<256",vec![blob(&bytes),blob(blake3::hash(&bytes).as_bytes()),blob(digest),blob(data.token.owner.incarnation.as_bytes()),number(data.token.attempt)?]))?;
                if changed.first().is_none_or(|set| set.rows_affected != 1) {
                    return Ok(denied(PreparationDenial::Capacity));
                }
            }
            _ => return Ok(denied(PreparationDenial::Conflict)),
        }
        Ok(CommandResult::Success(StagingReply::Granted(Box::new(
            StagingLease {
                token: row.token,
                format,
                observed_at_ms: now,
                expires_at_ms: row.expires,
            },
        ))))
    }
}
fn checkpoint(token: PreparationToken) -> cellule_runtime::Result<SqlBatch> {
    Ok(statement(
        "SELECT input_checkpoint,input_checkpoint_digest,expires_at_ms FROM catalog_leases WHERE incarnation=?1 AND admission_sequence=?2 AND operation=?3 AND owner_epoch=?4 AND artifact_operation=?5",
        vec![
            blob(token.owner.incarnation.as_bytes()),
            number(token.attempt)?,
            blob(token.operation),
            blob(token.owner.epoch.to_be_bytes()),
            blob(token.artifact_operation),
        ],
    ))
}
pub struct CheckStagedInputs;
impl Query for CheckStagedInputs {
    const MODULE: &'static str = RepositoryModule::NAME;
    const ID: u32 = 30;
    const CODEC_VERSION: u32 = 1;
    type Input = LeaseCheck;
    type Output = Option<NativeInputCertificate>;
    fn execute(
        context: &mut QueryContext<'_>,
        check: LeaseCheck,
    ) -> cellule_runtime::Result<Self::Output> {
        validate_component(&check.actor)?;
        if !decode_access(&context.sql(&SqlBatch {
            statements: vec![access_statement(&check.actor)],
        })?)?
        .is_some_and(|r| r >= TokenScope::Write)
        {
            return Ok(None);
        }
        let Some(format) = identity(
            &context.sql(&statement(IDENTITY, vec![]))?,
            check.token.repository,
        )?
        else {
            return Ok(None);
        };
        let Some(row) = operation(
            &context.sql(&statement(OPERATION, vec![blob(check.token.operation)]))?,
            check.token.repository,
            check.token.operation,
        )?
        else {
            return Ok(None);
        };
        if row.actor != check.actor || row.token.request_digest != check.token.request_digest {
            return Ok(None);
        }
        let results = context.sql(&checkpoint(check.token)?)?;
        let Some([SqlValue::Blob(bytes), digest, SqlValue::Integer(expires)]) =
            rows(&results)?.first().map(Vec::as_slice)
        else {
            return Ok(None);
        };
        if *expires <= now(context.now_ms())?
            || fixed::<32>(digest)? != *blake3::hash(bytes).as_bytes()
        {
            return Ok(None);
        }
        let proof = NativeInputCertificate::from_bytes(bytes)?;
        let data: Inputs = proof.0.data()?;
        let seed = attestation::seed(&context.sql(&statement(
            "SELECT push_cert_seed FROM repository_identity WHERE singleton=1",
            vec![],
        ))?)?;
        if !proof.0.authenticated(&seed)
            || data.token != check.token
            || data.actor != check.actor
            || data.format != format
            || crate::repository_target(
                cellule_runtime::TenantId::from_bytes(data.tenant),
                cellule_runtime::ApplicationId::from_bytes(data.application),
                data.token.repository,
            )?
            .cell_id()
                != context.cell_id()
        {
            return Ok(None);
        }
        Ok(Some(proof))
    }
}

/// Command-local custody barrier for newly published catalogs using borrowed
/// native inputs. Completed outcome replay precedes this check upstream.
pub(super) fn retention_matches(
    context: &CommandContext<'_, '_>,
    data: &certificate::CertificateData,
) -> cellule_runtime::Result<bool> {
    let Some(expected) = data.input_checkpoint_digest else {
        return Ok(true);
    };
    let retained = context.sql(&checkpoint(data.token)?)?;
    let Some([SqlValue::Blob(bytes), digest, SqlValue::Integer(expires)]) =
        rows(&retained)?.first().map(Vec::as_slice)
    else {
        return Ok(false);
    };
    if *expires <= now(context.now_ms())?
        || fixed::<32>(digest)? != expected
        || *blake3::hash(bytes).as_bytes() != expected
    {
        return Ok(false);
    }
    let proof = NativeInputCertificate::from_bytes(bytes)?;
    let inputs: Inputs = proof.0.data()?;
    let seed = attestation::seed(&context.sql(&statement(
        "SELECT push_cert_seed FROM repository_identity WHERE singleton=1",
        vec![],
    ))?)?;
    Ok(proof.0.authenticated(&seed)
        && inputs.token == data.token
        && inputs.actor == data.actor
        && inputs.format == data.catalog.format
        && inputs.tenant == data.tenant
        && inputs.application == data.application)
}

/// A durable receipt is not usable custody. Check the exact independent input
/// pin, then observe the same live bound attempt/floor with a fresh clock.
pub(super) async fn observe_bound_registration(
    session: &PreparationSession,
    digest: [u8; 32],
    minimum: cellule_runtime::Receipt,
) -> Result<(), InputCheckpointError> {
    let current = session
        .client
        .query::<CheckStagedInputs>(&session.target, Some(minimum), session.check.clone())
        .await
        .map_err(|e| InputCheckpointError::Retained(Box::new(e)))?
        .output
        .ok_or(PreparationBaseError::Inactive)?;
    if *blake3::hash(&current.bytes()?).as_bytes() != digest {
        return Err(PreparationBaseError::Context.into());
    }
    session.refresh(minimum).await?;
    Ok(())
}
