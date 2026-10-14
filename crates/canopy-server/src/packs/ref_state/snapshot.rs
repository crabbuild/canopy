use super::*;
use crate::packs::{
    InputRootError,
    directory::index::codec::{fixed, read_reference, reference},
    input_artifact::StoredInputRoot,
};
use cellule_runtime::codec::WireValue;

const DOMAIN: &[u8] = b"canopy.ref-state-snapshot.v1\0";
const ROOT_BYTES: u32 = 256 << 10;
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RefStateSnapshot {
    pub repository: [u8; 16],
    pub format: ObjectFormat,
    pub generation: u64,
    pub default_branch: String,
    pub root: Option<RefStateRoot>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RefStateSnapshotRoot(StoredInputRoot);
#[derive(Debug, thiserror::Error)]
pub enum RefSnapshotError {
    #[error("ref snapshot transport failed")]
    Root(#[from] InputRootError),
    #[error("ref snapshot codec failed")]
    Codec(#[from] CodecError),
    #[error("ref snapshot context differs")]
    Context,
}
struct Record {
    operation: [u8; 16],
    snapshot: RefStateSnapshot,
}
impl RefStateSnapshotRoot {
    pub fn operation(self) -> [u8; 16] {
        self.0.operation
    }
    pub fn artifact(self) -> canopy_object_storage::artifact::ArtifactDescriptor {
        self.0.artifact
    }
    /// Stores a descriptor, not a publication certificate or traversal proof.
    pub async fn upload(
        store: &ArtifactStore,
        operation: [u8; 16],
        snapshot: RefStateSnapshot,
    ) -> Result<Self, RefSnapshotError> {
        if snapshot.repository != store.repository() {
            return Err(RefSnapshotError::Context);
        }
        Ok(Self(
            StoredInputRoot::upload(
                store,
                operation,
                &Record {
                    operation,
                    snapshot,
                },
                ROOT_BYTES,
            )
            .await?,
        ))
    }
    pub async fn read(self, store: &ArtifactStore) -> Result<RefStateSnapshot, RefSnapshotError> {
        let record: Record = self.0.read(store, ROOT_BYTES).await?;
        if record.operation != self.operation() || record.snapshot.repository != store.repository()
        {
            return Err(RefSnapshotError::Context);
        }
        Ok(record.snapshot)
    }
}
impl WireValue for RefStateSnapshotRoot {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        self.0.validate(ROOT_BYTES)?;
        self.0.encode(e)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        let root = StoredInputRoot::decode(d)?;
        root.validate(ROOT_BYTES)?;
        Ok(Self(root))
    }
}
impl Record {
    fn validate(&self) -> Result<(), CodecError> {
        super::super::publication::codec::artifact_valid(self.operation)?;
        let snapshot = &self.snapshot;
        if crate::validate_repository_id(snapshot.repository).is_err()
            || snapshot.generation > i64::MAX as u64
            || snapshot.generation == 0 && snapshot.root.is_some()
            || snapshot.default_branch.len() > MAX_NAME_BYTES
            || !snapshot.default_branch.starts_with("refs/heads/")
            || !crate::refs::valid_ref_name(&snapshot.default_branch)
        {
            return Err(CodecError::Invalid("ref snapshot"));
        }
        if let Some(root) = &snapshot.root {
            root.validate(snapshot.format)
                .map_err(|_| CodecError::Invalid("ref snapshot index"))?;
            super::super::publication::codec::artifact_valid(root.operation)?;
        }
        Ok(())
    }
}
impl WireValue for Record {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        self.validate()?;
        e.write_bytes(DOMAIN)?;
        e.write_bytes(&self.operation)?;
        e.write_bytes(&self.snapshot.repository)?;
        e.write_u8(self.snapshot.format.bytes() as u8)?;
        e.write_u64(self.snapshot.generation)?;
        e.write_text(&self.snapshot.default_branch)?;
        e.write_bool(self.snapshot.root.is_some())?;
        if let Some(root) = &self.snapshot.root {
            reference(e, root.clone())?;
        }
        Ok(())
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        if d.read_bytes()? != DOMAIN {
            return Err(CodecError::Invalid("ref snapshot domain"));
        }
        let operation = fixed(d)?;
        let repository = fixed(d)?;
        let format = match d.read_u8()? {
            20 => ObjectFormat::Sha1,
            32 => ObjectFormat::Sha256,
            _ => return Err(CodecError::Invalid("ref snapshot format")),
        };
        let generation = d.read_u64()?;
        let default_branch = d.read_text()?.to_owned();
        let root = if d.read_bool()? {
            Some(read_reference(d, format)?)
        } else {
            None
        };
        let record = Self {
            operation,
            snapshot: RefStateSnapshot {
                repository,
                format,
                generation,
                default_branch,
                root,
            },
        };
        record.validate()?;
        Ok(record)
    }
}
