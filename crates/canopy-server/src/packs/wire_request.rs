//! Immutable original request bytes and metadata. This root is custody, not
//! signature, native-result, ref-CAS or publication authority.
use super::directory::index::codec::{artifact, fixed, read_artifact};
use crate::{ObjectFormat, git_http::GitHttpRequest, packs::publication::BeginRequest};
use canopy_object_storage::artifact::{
    ArtifactDescriptor, ArtifactKey, ArtifactKind, ArtifactStore,
};
use cellule_runtime::{
    ApplicationId, TenantId,
    codec::{BoundedDecoder, BoundedEncoder, CodecError, WireValue},
};

const DOMAIN: &[u8] = b"canopy.wire-request.v1\0";
pub const REQUEST_ROOT_BYTES: u32 = 64 << 10;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WireRequestRoot(super::input_artifact::StoredInputRoot);
impl WireRequestRoot {
    pub fn operation(self) -> [u8; 16] {
        self.0.operation
    }
    pub fn artifact(self) -> ArtifactDescriptor {
        self.0.artifact
    }
    pub(crate) fn validate(self) -> Result<(), CodecError> {
        self.0.validate(REQUEST_ROOT_BYTES)
    }
    pub(crate) async fn upload(
        store: &ArtifactStore,
        record: WireRequest,
    ) -> Result<Self, WireRequestError> {
        record.validate()?;
        if record.identity.repository != store.repository() {
            return Err(WireRequestError::Context);
        }
        Ok(Self(
            super::input_artifact::StoredInputRoot::upload(
                store,
                record.operation,
                &record,
                REQUEST_ROOT_BYTES,
            )
            .await?,
        ))
    }
    pub(crate) async fn read(self, store: &ArtifactStore) -> Result<WireRequest, WireRequestError> {
        let record: WireRequest = self.0.read(store, REQUEST_ROOT_BYTES).await?;
        if record.operation != self.operation() || record.identity.repository != store.repository()
        {
            return Err(WireRequestError::Context);
        }
        Ok(record)
    }
}
impl WireValue for WireRequestRoot {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        self.validate()?;
        self.0.encode(e)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        let value = Self(super::input_artifact::StoredInputRoot::decode(d)?);
        value.validate()?;
        Ok(value)
    }
}
pub(crate) struct WireRequest {
    pub tenant: TenantId,
    pub application: ApplicationId,
    pub operation: [u8; 16],
    pub identity: BeginRequest,
    pub format: ObjectFormat,
    pub request: GitHttpRequest<ArtifactDescriptor>,
}
impl WireRequest {
    pub(crate) fn matches(
        &self,
        target: &cellule_runtime::CellTarget,
        check: &crate::packs::publication::LeaseCheck,
        format: ObjectFormat,
    ) -> bool {
        self.tenant == target.tenant()
            && self.application == target.application()
            && self.format == format
            && self.identity.repository == check.token.repository
            && self.identity.operation == check.token.operation
            && self.identity.request_digest == check.token.request_digest
            && self.identity.actor == check.actor
            && crate::repository_target(self.tenant, self.application, self.identity.repository)
                .is_ok_and(|expected| expected == *target)
    }
    fn validate(&self) -> Result<(), CodecError> {
        super::publication::codec::artifact_valid(self.operation)?;
        crate::repository_target(self.tenant, self.application, self.identity.repository)
            .map_err(|_| CodecError::Invalid("wire request target"))?;
        if self.operation == [0; 16]
            || self.request.method != "POST"
            || self.request.path_info != "/repo.git/git-receive-pack"
            || !self.request.authenticated
            || self.request.body.size > canopy_object_storage::external::MAX_ARTIFACT_BYTES
            || self.request.body.manifest_digest == [0; 32]
        {
            return Err(CodecError::Invalid("wire request metadata"));
        }
        Ok(())
    }
    pub(crate) fn body_key(&self) -> ArtifactKey {
        ArtifactKey {
            operation: self.operation,
            binding_digest: self.request.body.digest,
            kind: ArtifactKind::InputBody,
        }
    }
}
impl WireValue for WireRequest {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        self.validate()?;
        e.write_bytes(DOMAIN)?;
        e.write_bytes(self.tenant.as_bytes())?;
        e.write_bytes(self.application.as_bytes())?;
        e.write_bytes(&self.operation)?;
        self.identity.encode(e)?;
        e.write_u8(self.format.bytes() as u8)?;
        e.write_text(&self.request.method)?;
        e.write_text(&self.request.path_info)?;
        e.write_text(&self.request.query)?;
        e.write_bool(self.request.content_type.is_some())?;
        if let Some(content_type) = &self.request.content_type {
            e.write_text(content_type)?;
        }
        e.write_bool(self.request.gzip)?;
        e.write_bool(self.request.protocol_v2)?;
        artifact(e, self.request.body)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        if d.read_bytes()? != DOMAIN {
            return Err(CodecError::Invalid("wire request domain"));
        }
        let tenant = TenantId::from_bytes(fixed(d)?);
        let application = ApplicationId::from_bytes(fixed(d)?);
        let operation = fixed(d)?;
        let identity = BeginRequest::decode(d)?;
        let format = match d.read_u8()? {
            20 => ObjectFormat::Sha1,
            32 => ObjectFormat::Sha256,
            _ => return Err(CodecError::Invalid("wire request format")),
        };
        let request = GitHttpRequest {
            method: d.read_text()?.into(),
            path_info: d.read_text()?.into(),
            query: d.read_text()?.into(),
            content_type: if d.read_bool()? {
                Some(d.read_text()?.into())
            } else {
                None
            },
            gzip: d.read_bool()?,
            protocol_v2: d.read_bool()?,
            body: read_artifact(d)?,
            authenticated: true,
        };
        let value = Self {
            tenant,
            application,
            operation,
            identity,
            format,
            request,
        };
        value.validate()?;
        Ok(value)
    }
}
#[derive(Debug, thiserror::Error)]
pub enum WireRequestError {
    #[error("original request metadata transport failed")]
    Root(#[from] super::InputRootError),
    #[error("wire request artifact failed")]
    Artifact(#[from] canopy_object_storage::artifact::ArtifactError),
    #[error("wire request codec failed")]
    Codec(#[from] CodecError),
    #[error("wire request context differs")]
    Context,
}
