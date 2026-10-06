//! Authenticated native completion custody, before canonical/final publication.
//! Artifact decoding cannot create a native witness without registered custody.
use super::*;
use crate::packs::{
    input_artifact::{INPUT_ROOT_BYTES, StoredInputRoot},
    wire_request::{WireRequestError, WireRequestRoot},
};
use crate::{
    directory::DirectoryCell, git_http::GitHttpResponse, git_input::InputError,
    push::VerifiedPushCertificate,
};
use canopy_object_storage::artifact::{
    ArtifactDescriptor, ArtifactKey, ArtifactKind, ArtifactStore,
};
use cellule_ltx::DiskBudget;
use cellule_runtime::CellTarget;
use std::path::Path;

mod codec;
mod plan;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NativeResultRoot(StoredInputRoot);
impl NativeResultRoot {
    pub fn operation(self) -> [u8; 16] {
        self.0.operation
    }
    pub fn artifact(self) -> ArtifactDescriptor {
        self.0.artifact
    }
    pub(super) fn validate(self) -> Result<(), CodecError> {
        self.0.validate(INPUT_ROOT_BYTES)
    }
    pub(super) async fn read(
        self,
        store: &ArtifactStore,
    ) -> Result<ResultRecord, NativeResultError> {
        let record: ResultRecord = self.0.read(store, INPUT_ROOT_BYTES).await?;
        if record.operation != self.operation() {
            return Err(NativeResultError::Context);
        }
        Ok(record)
    }
    pub(super) async fn check_request(
        self,
        store: &ArtifactStore,
        request: WireRequestRoot,
        target: &CellTarget,
        check: &LeaseCheck,
        format: ObjectFormat,
    ) -> Result<(), NativeResultError> {
        let record = self.read(store).await?;
        if record.request != request || !request.read(store).await?.matches(target, check, format) {
            return Err(NativeResultError::Context);
        }
        Ok(())
    }
}
/// Only the native-result retention factory can supply a checkpoint attachment.
pub struct SavedNativeResult {
    root: NativeResultRoot,
    target: CellTarget,
    check: LeaseCheck,
    format: ObjectFormat,
    request: WireRequestRoot,
    previous: [u8; 32],
}
impl SavedNativeResult {
    pub(super) fn scoped_root(
        self,
        target: &CellTarget,
        check: &LeaseCheck,
        format: ObjectFormat,
        request: WireRequestRoot,
        previous: [u8; 32],
    ) -> Result<NativeResultRoot, CodecError> {
        if self.target != *target
            || self.check != *check
            || self.format != format
            || self.request != request
            || self.previous != previous
            || self.root.operation() != check.token.artifact_operation
        {
            return Err(CodecError::Invalid("native result custody context"));
        }
        Ok(self.root)
    }
}
pub(super) struct ResultRecord {
    pub(super) operation: [u8; 16],
    request: WireRequestRoot,
    pub(super) response: GitHttpResponse<ArtifactDescriptor>,
    plan: Option<ArtifactDescriptor>,
    pub(super) options: Vec<String>,
    signed: Option<SignedPushAnnotation<ArtifactDescriptor>>,
}
impl ResultRecord {
    /// Closed audit retains plan and signed bytes, plus the selected response
    /// separately. It never makes the original wire request a permanent root.
    pub(super) fn audit_bodies(&self) -> Vec<(ArtifactKey, ArtifactDescriptor)> {
        self.plan
            .into_iter()
            .chain(self.signed.iter().map(|value| value.body))
            .map(|body| (body_key(self.operation, body), body))
            .collect()
    }
    pub(super) fn has_plan(&self) -> bool {
        self.plan.is_some()
    }
}
#[derive(Debug, thiserror::Error)]
pub enum NativeResultError {
    #[error("native result metadata transport failed")]
    InputRoot(#[from] crate::packs::InputRootError),
    #[error("native result codec failed")]
    Codec(#[from] CodecError),
    #[error("native result artifact failed")]
    Artifact(#[from] canopy_object_storage::artifact::ArtifactError),
    #[error("native result root failed")]
    Root(#[from] WireRequestError),
    #[error("native result spool failed")]
    Input(#[from] InputError),
    #[error("native result custody failed")]
    Checkpoint(#[from] InputCheckpointError),
    #[error("native result plan failed")]
    Refs(#[from] RefProofError),
    #[error("native result report failed")]
    Report(#[from] crate::push::PushError),
    #[error("native result hashing task failed")]
    Task(#[from] tokio::task::JoinError),
    #[error("native signer lookup failed")]
    Signers(#[source] Box<cellule_runtime::InvocationError<Vec<SqlResultSet>>>),
    #[error("native result context differs or signing key is no longer authorized")]
    Context,
}
pub(super) fn body_key(operation: [u8; 16], body: ArtifactDescriptor) -> ArtifactKey {
    ArtifactKey {
        operation,
        binding_digest: body.digest,
        kind: ArtifactKind::InputBody,
    }
}
pub(super) async fn retain_body(
    store: &ArtifactStore,
    operation: [u8; 16],
    body: Vec<u8>,
) -> Result<ArtifactDescriptor, NativeResultError> {
    let (body, digest) = tokio::task::spawn_blocking(move || {
        let digest = *blake3::hash(&body).as_bytes();
        (body, digest)
    })
    .await?;
    Ok(store
        .put(
            ArtifactKey {
                operation,
                binding_digest: digest,
                kind: ArtifactKind::InputBody,
            },
            body.len() as u64,
            digest,
            &mut body.as_slice(),
        )
        .await?)
}
async fn reopen_body(
    store: &ArtifactStore,
    operation: [u8; 16],
    body: ArtifactDescriptor,
) -> Result<Vec<u8>, NativeResultError> {
    if body.size > crate::push::MAX_RESPONSE_BYTES as u64 {
        return Err(CodecError::Limit.into());
    }
    let mut reader = store.read(body_key(operation, body), body).await?;
    let mut bytes = Vec::with_capacity(body.size as usize);
    while let Some(part) = reader.next().await? {
        bytes.extend_from_slice(&part);
    }
    Ok(bytes)
}
fn validate_plan(
    plan: Option<&crate::PushPlan>,
    check: &LeaseCheck,
    format: ObjectFormat,
) -> Result<(), NativeResultError> {
    if let Some(plan) = plan {
        if plan.actor != check.actor {
            return Err(NativeResultError::Context);
        }
        super::ref_proof::shape(plan, format)?;
    }
    Ok(())
}
impl StagingContext {
    /// Preserve exact native bytes and versioned intent. Canonical verification
    /// and final authority/CAS still gate acknowledgement independently.
    pub async fn retain_native_result(
        &self,
        store: &ArtifactStore,
        prior: &NativeInputCertificate,
        request: PushCompletionRequest,
        directory: &Path,
        budget: &DiskBudget,
    ) -> Result<SavedNativeResult, NativeResultError> {
        let (current, target, check, format) = self.push_checkpoint().await?;
        if current != *prior
            || store.repository() != check.token.repository
            || prior.native_result()?.is_some()
        {
            return Err(NativeResultError::Context);
        }
        let wire = prior.wire_request()?.ok_or(NativeResultError::Context)?;
        if !wire.read(store).await?.matches(&target, &check, format) {
            return Err(NativeResultError::Context);
        }
        let signed =
            super::completion::scoped_signed_annotation(&target, &check, request.certificate)?;
        super::completion::validate_payload(&request.response, &request.options, signed.as_ref())?;
        validate_plan(request.plan.as_ref(), &check, format)?;
        crate::push::report::publication_matches(&request.response, request.plan.as_ref())?;
        let operation = check.token.artifact_operation;
        let GitHttpResponse {
            status,
            headers,
            body,
        } = request.response;
        let response = GitHttpResponse {
            status,
            headers,
            body: retain_body(store, operation, body).await?,
        };
        let signed = if let Some(signed) = signed {
            Some(SignedPushAnnotation {
                body: retain_body(store, operation, signed.body).await?,
                signer: signed.signer,
                key: signed.key,
            })
        } else {
            None
        };
        let plan = if let Some(plan) = request.plan {
            Some(plan::retain(plan, store, operation, directory, budget).await?)
        } else {
            None
        };
        let record = ResultRecord {
            operation,
            request: wire,
            response,
            plan,
            options: request.options,
            signed,
        };
        let root = NativeResultRoot(
            StoredInputRoot::upload(store, operation, &record, INPUT_ROOT_BYTES).await?,
        );
        let (current, _, _, _) = self.push_checkpoint().await?;
        if current != *prior {
            return Err(NativeResultError::Context);
        }
        Ok(SavedNativeResult {
            root,
            target,
            check,
            format,
            request: wire,
            previous: prior.checkpoint_lineage()?.0,
        })
    }
    pub async fn reopen_native_result(
        &self,
        store: &ArtifactStore,
        directory: &Path,
        budget: &DiskBudget,
        signers: Option<&DirectoryCell>,
    ) -> Result<PushCompletionRequest, NativeResultError> {
        let (proof, target, check, format) = self.push_checkpoint().await?;
        let result = reopen(
            &proof,
            (&target, &check, format),
            store,
            directory,
            budget,
            signers,
        )
        .await?;
        let (current, _, _, _) = self.push_checkpoint().await?;
        if current != proof {
            return Err(NativeResultError::Context);
        }
        Ok(result)
    }
}
impl PreparationSession {
    pub async fn reopen_native_result(
        &self,
        store: &ArtifactStore,
        directory: &Path,
        budget: &DiskBudget,
        signers: Option<&DirectoryCell>,
    ) -> Result<PushCompletionRequest, NativeResultError> {
        let (proof, target, check, format) = self.push_checkpoint().await?;
        let result = reopen(
            &proof,
            (&target, &check, format),
            store,
            directory,
            budget,
            signers,
        )
        .await?;
        let (current, _, _, _) = self.push_checkpoint().await?;
        if current != proof {
            return Err(NativeResultError::Context);
        }
        Ok(result)
    }
}
async fn reopen(
    proof: &NativeInputCertificate,
    custody: (&CellTarget, &LeaseCheck, ObjectFormat),
    store: &ArtifactStore,
    directory: &Path,
    budget: &DiskBudget,
    signers: Option<&DirectoryCell>,
) -> Result<PushCompletionRequest, NativeResultError> {
    let (target, check, format) = custody;
    let root = proof.native_result()?.ok_or(NativeResultError::Context)?;
    let wire = proof.wire_request()?.ok_or(NativeResultError::Context)?;
    let record = root.read(store).await?;
    if record.request != wire || !wire.read(store).await?.matches(target, check, format) {
        return Err(NativeResultError::Context);
    }
    let plan = if let Some(plan) = record.plan {
        Some(plan::reopen(store, record.operation, plan, directory, budget).await?)
    } else {
        None
    };
    validate_plan(plan.as_ref(), check, format)?;
    let response = GitHttpResponse {
        status: record.response.status,
        headers: record.response.headers,
        body: reopen_body(store, record.operation, record.response.body).await?,
    };
    let signed = if let Some(signed) = record.signed {
        Some(SignedPushAnnotation {
            body: reopen_body(store, record.operation, signed.body).await?,
            signer: signed.signer,
            key: signed.key,
        })
    } else {
        None
    };
    super::completion::validate_payload(&response, &record.options, signed.as_ref())?;
    crate::push::report::publication_matches(&response, plan.as_ref())?;
    let certificate = if let Some(signed) = signed {
        let directory = signers
            .filter(|directory| directory.matches_repository_scope(target))
            .ok_or(NativeResultError::Context)?;
        let keys = directory
            .push_signers(&check.actor)
            .await
            .map_err(|error| NativeResultError::Signers(Box::new(error)))?;
        if signed.signer != check.actor || !keys.iter().any(|key| key.fingerprint() == signed.key) {
            return Err(NativeResultError::Context);
        }
        // This constructor is reached only from exact MAC-registered custody,
        // complete authenticated bodies and fresh scoped key authorization.
        Some(VerifiedPushCertificate {
            target: target.clone(),
            request_digest: check.token.request_digest,
            body: signed.body,
            signer: signed.signer,
            key: signed.key,
        })
    } else {
        None
    };
    Ok(PushCompletionRequest {
        plan,
        response,
        options: record.options,
        certificate,
    })
}

pub(super) async fn backup_graph(
    root: NativeResultRoot,
    active: bool,
    inventory: &mut crate::packs::backup::Inventory<'_>,
) -> crate::packs::directory::index::WalkResult<()> {
    let record = root.read(&inventory.store()).await?;
    inventory.input(root.operation(), root.artifact()).await?;
    for (key, body) in record.audit_bodies() {
        inventory.artifact(key, body).await?;
    }
    if active {
        super::backup::wire(record.request, inventory).await?;
        inventory
            .body(record.operation, record.response.body)
            .await?;
    }
    Ok(())
}
