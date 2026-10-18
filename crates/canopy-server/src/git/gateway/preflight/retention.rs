use super::*;
use crate::packs::{
    publication::{
        InputCheckpointError, LeaseCheck, NativeInputCertificate, PreparationSession,
        StagingContext, StagingError,
    },
    wire_request::{WireRequest, WireRequestError, WireRequestRoot},
};
use canopy_object_storage::artifact::ArtifactStore;

/// Only an owned authenticated preflight can create this local signing input.
pub struct SavedPushRequest {
    root: WireRequestRoot,
    target: CellTarget,
    identity: BeginRequest,
    format: crate::ObjectFormat,
}
impl SavedPushRequest {
    pub(crate) fn scoped_root(
        &self,
        target: &CellTarget,
        check: &LeaseCheck,
        format: crate::ObjectFormat,
    ) -> Result<WireRequestRoot, InputCheckpointError> {
        if self.target != *target
            || self.format != format
            || self.identity.repository != check.token.repository
            || self.identity.operation != check.token.operation
            || self.identity.request_digest != check.token.request_digest
            || self.identity.actor != check.actor
            || self.root.operation() != check.token.artifact_operation
        {
            return Err(StagingError::Context.into());
        }
        Ok(self.root)
    }
}
#[derive(Debug, thiserror::Error)]
pub enum RequestRetentionError {
    #[error("request preflight failed")]
    Gateway(#[from] GatewayError),
    #[error("request spool failed")]
    Input(#[from] InputError),
    #[error("request root failed")]
    Wire(#[from] WireRequestError),
    #[error("request checkpoint failed")]
    Checkpoint(#[from] InputCheckpointError),
    #[error("request custody differs")]
    Context,
}
impl EncodedPush {
    pub async fn retain(
        self,
        context: &StagingContext,
        store: &ArtifactStore,
    ) -> Result<(Self, SavedPushRequest), RequestRetentionError> {
        context
            .check_push_identity(&self.target, &self.identity, self.format)
            .await?;
        let token = context.token().map_err(InputCheckpointError::from)?;
        if store.repository() != token.repository {
            return Err(RequestRetentionError::Context);
        }
        let Self {
            request,
            identity,
            format,
            target,
            content_digest,
        } = self;
        let GitHttpRequest {
            method,
            path_info,
            query,
            content_type,
            gzip,
            protocol_v2,
            authenticated,
            body,
        } = request;
        let (body, artifact) = body
            .retain(store, token.artifact_operation, content_digest)
            .await?;
        let root = WireRequestRoot::upload(
            store,
            WireRequest {
                tenant: target.tenant(),
                application: target.application(),
                operation: token.artifact_operation,
                identity: identity.clone(),
                format,
                request: GitHttpRequest {
                    method: method.clone(),
                    path_info: path_info.clone(),
                    query: query.clone(),
                    content_type: content_type.clone(),
                    gzip,
                    protocol_v2,
                    authenticated,
                    body: artifact,
                },
            },
        )
        .await?;
        context
            .check_push_identity(&target, &identity, format)
            .await?;
        let saved = SavedPushRequest {
            root,
            target: target.clone(),
            identity: identity.clone(),
            format,
        };
        Ok((
            Self {
                request: GitHttpRequest {
                    method,
                    path_info,
                    query,
                    content_type,
                    gzip,
                    protocol_v2,
                    authenticated,
                    body,
                },
                identity,
                format,
                target,
                content_digest,
            },
            saved,
        ))
    }
}
async fn reopen(
    proof: &NativeInputCertificate,
    custody: (&CellTarget, &LeaseCheck, crate::ObjectFormat),
    store: &ArtifactStore,
    directory: &Path,
    budget: &DiskBudget,
    limit: Option<u64>,
    admission: Option<Arc<AdmissionPermit>>,
) -> Result<EncodedPush, RequestRetentionError> {
    let (target, check, format) = custody;
    let root = proof
        .wire_request()
        .map_err(WireRequestError::from)?
        .ok_or(RequestRetentionError::Context)?;
    let record = root.read(store).await?;
    if !record.matches(target, check, format) {
        return Err(RequestRetentionError::Context);
    }
    if limit.is_some_and(|limit| record.request.body.size > limit) {
        return Err(InputError::TooLarge.into());
    }
    let key = record.body_key();
    let artifact = store
        .read(key, record.request.body)
        .await
        .map_err(WireRequestError::from)?;
    let body = GitInput::reopen(artifact, directory, budget, limit, admission).await?;
    let request = record.request;
    let encoded = EncodedPush::new(
        GitHttpRequest {
            method: request.method,
            path_info: request.path_info,
            query: request.query,
            content_type: request.content_type,
            gzip: request.gzip,
            protocol_v2: request.protocol_v2,
            authenticated: request.authenticated,
            body,
        },
        target,
        record.identity.repository,
        record.format,
        &record.identity.actor,
        record.identity.operation,
    )
    .await?;
    if encoded.identity != record.identity {
        return Err(RequestRetentionError::Context);
    }
    Ok(encoded)
}
impl StagingContext {
    pub async fn reopen_push_request(
        &self,
        store: &ArtifactStore,
        directory: &Path,
        budget: &DiskBudget,
        limit: Option<u64>,
        admission: Option<Arc<AdmissionPermit>>,
    ) -> Result<EncodedPush, RequestRetentionError> {
        let (proof, target, check, format) = self.push_checkpoint().await?;
        let encoded = reopen(
            &proof,
            (&target, &check, format),
            store,
            directory,
            budget,
            limit,
            admission,
        )
        .await?;
        let (current, _, _, _) = self.push_checkpoint().await?;
        if current != proof {
            return Err(RequestRetentionError::Context);
        }
        Ok(encoded)
    }
}
impl PreparationSession {
    pub async fn reopen_push_request(
        &self,
        store: &ArtifactStore,
        directory: &Path,
        budget: &DiskBudget,
        limit: Option<u64>,
        admission: Option<Arc<AdmissionPermit>>,
    ) -> Result<EncodedPush, RequestRetentionError> {
        let (proof, target, check, format) = self.push_checkpoint().await?;
        let encoded = reopen(
            &proof,
            (&target, &check, format),
            store,
            directory,
            budget,
            limit,
            admission,
        )
        .await?;
        let (current, _, _, _) = self.push_checkpoint().await?;
        if current != proof {
            return Err(RequestRetentionError::Context);
        }
        Ok(encoded)
    }
}
