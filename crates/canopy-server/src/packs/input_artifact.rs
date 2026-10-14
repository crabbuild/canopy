//! Shared bounded metadata roots for retained input/result custody.
use super::directory::index::codec::{artifact, fixed, read_artifact};
use canopy_object_storage::artifact::{
    ArtifactDescriptor, ArtifactKey, ArtifactKind, ArtifactStore,
};
use cellule_runtime::codec::{BoundedDecoder, BoundedEncoder, CodecError, WireValue};

pub(crate) const INPUT_ROOT_BYTES: u32 = 128 << 10;
pub(crate) const MAX_INPUT_ROOT_BYTES: u32 = 256 << 10;
#[derive(Debug, thiserror::Error)]
pub enum InputRootError {
    #[error("retained input root codec failed")]
    Codec(#[from] CodecError),
    #[error("retained input root artifact failed")]
    Artifact(#[from] canopy_object_storage::artifact::ArtifactError),
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct StoredInputRoot {
    pub operation: [u8; 16],
    pub artifact: ArtifactDescriptor,
}
impl StoredInputRoot {
    pub fn validate(self, limit: u32) -> Result<(), CodecError> {
        super::publication::codec::artifact_valid(self.operation)?;
        if limit == 0
            || limit > MAX_INPUT_ROOT_BYTES
            || self.artifact.size == 0
            || self.artifact.size > u64::from(limit)
            || self.artifact.manifest_digest == [0; 32]
        {
            return Err(CodecError::Invalid("retained input root"));
        }
        Ok(())
    }
    fn key(self) -> ArtifactKey {
        ArtifactKey {
            operation: self.operation,
            binding_digest: self.artifact.digest,
            kind: ArtifactKind::InputRoot,
        }
    }
    pub async fn upload<T: WireValue>(
        store: &ArtifactStore,
        operation: [u8; 16],
        value: &T,
        limit: u32,
    ) -> Result<Self, InputRootError> {
        super::publication::codec::artifact_valid(operation)?;
        if limit == 0 || limit > MAX_INPUT_ROOT_BYTES {
            return Err(CodecError::Limit.into());
        }
        let mut e = BoundedEncoder::new(limit)?;
        value.encode(&mut e)?;
        let bytes = e.finish();
        let digest = *blake3::hash(&bytes).as_bytes();
        let key = ArtifactKey {
            operation,
            binding_digest: digest,
            kind: ArtifactKind::InputRoot,
        };
        let artifact = store
            .put(key, bytes.len() as u64, digest, &mut bytes.as_slice())
            .await?;
        Ok(Self {
            operation,
            artifact,
        })
    }
    pub async fn read<T: WireValue>(
        self,
        store: &ArtifactStore,
        limit: u32,
    ) -> Result<T, InputRootError> {
        self.validate(limit)?;
        let mut reader = store.read(self.key(), self.artifact).await?;
        let mut bytes = Vec::with_capacity(self.artifact.size as usize);
        while let Some(part) = reader.next().await? {
            bytes.extend_from_slice(&part);
        }
        let mut d = BoundedDecoder::new(&bytes, limit)?;
        let value = T::decode(&mut d)?;
        d.finish()?;
        Ok(value)
    }
}
impl WireValue for StoredInputRoot {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        self.validate(MAX_INPUT_ROOT_BYTES)?;
        e.write_bytes(&self.operation)?;
        artifact(e, self.artifact)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        let value = Self {
            operation: fixed(d)?,
            artifact: read_artifact(d)?,
        };
        value.validate(MAX_INPUT_ROOT_BYTES)?;
        Ok(value)
    }
}
