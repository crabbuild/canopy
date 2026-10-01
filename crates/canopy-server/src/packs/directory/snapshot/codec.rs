use super::super::index::{
    NODE_BYTES,
    codec::{fixed, read_reference, reference},
};
use super::*;
use cellule_runtime::codec::{BoundedDecoder, BoundedEncoder};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StoredSnapshot {
    pub repository: [u8; 16],
    pub operation: [u8; 16],
    pub format: ObjectFormat,
    pub artifact: ArtifactDescriptor,
}
impl StoredSnapshot {
    fn key(self) -> ArtifactKey {
        ArtifactKey {
            operation: self.operation,
            binding_digest: self.artifact.digest,
            kind: ArtifactKind::CatalogNode,
        }
    }
}
impl DirectorySnapshot {
    pub(in crate::packs::directory) fn encode(
        &self,
        operation: [u8; 16],
    ) -> Result<Vec<u8>, IndexError> {
        self.validate()?;
        let mut encoder = BoundedEncoder::new(NODE_BYTES)?;
        encoder.write_bytes(b"canopy.directory-root.v2\0")?;
        encoder.write_bytes(&self.repository)?;
        encoder.write_bytes(&operation)?;
        encoder.write_u8(self.format.bytes() as u8)?;
        encoder.write_count(self.level_zero.len())?;
        for root in &self.level_zero {
            reference(&mut encoder, *root)?;
        }
        encoder.write_count(self.levels.len())?;
        for root in &self.levels {
            encoder.write_bool(root.is_some())?;
            if let Some(root) = root {
                reference(&mut encoder, *root)?;
            }
        }
        Ok(encoder.finish())
    }
    pub(in crate::packs::directory) fn decode(
        bytes: &[u8],
    ) -> Result<(Self, [u8; 16]), IndexError> {
        let mut decoder = BoundedDecoder::new(bytes, NODE_BYTES)?;
        if decoder.read_bytes()? != b"canopy.directory-root.v2\0" {
            return Err(IndexError::Integrity);
        }
        let repository = fixed(&mut decoder)?;
        let operation = fixed(&mut decoder)?;
        let format = match decoder.read_u8()? {
            20 => ObjectFormat::Sha1,
            32 => ObjectFormat::Sha256,
            _ => return Err(IndexError::Integrity),
        };
        let count = decoder.read_count()?;
        if count > LEVEL_ZERO_ROOTS {
            return Err(IndexError::Limit);
        }
        let mut level_zero = Vec::with_capacity(count);
        for _ in 0..count {
            level_zero.push(read_reference(&mut decoder, format)?);
        }
        let count = decoder.read_count()?;
        if count > MAX_LEVELS {
            return Err(IndexError::Limit);
        }
        let mut levels = Vec::with_capacity(count);
        for _ in 0..count {
            levels.push(if decoder.read_bool()? {
                Some(read_reference(&mut decoder, format)?)
            } else {
                None
            });
        }
        decoder.finish()?;
        let snapshot = Self {
            repository,
            format,
            level_zero,
            levels,
        };
        snapshot.validate()?;
        Ok((snapshot, operation))
    }
    /// Publishes immutable directory bytes, not refs or a successful push.
    pub async fn upload(
        &self,
        store: &ArtifactStore,
        operation: [u8; 16],
    ) -> Result<StoredSnapshot, IndexError> {
        if self.repository != store.repository() {
            return Err(IndexError::Integrity);
        }
        let bytes = self.encode(operation)?;
        let digest = *blake3::hash(&bytes).as_bytes();
        let key = ArtifactKey {
            operation,
            binding_digest: digest,
            kind: ArtifactKind::CatalogNode,
        };
        let artifact = store
            .put(key, bytes.len() as u64, digest, &mut bytes.as_slice())
            .await?;
        Ok(StoredSnapshot {
            repository: self.repository,
            operation,
            format: self.format,
            artifact,
        })
    }
    pub async fn download(
        store: &ArtifactStore,
        stored: StoredSnapshot,
    ) -> Result<Self, IndexError> {
        if stored.repository != store.repository()
            || stored.artifact.size == 0
            || stored.artifact.size > u64::from(NODE_BYTES)
        {
            return Err(IndexError::Integrity);
        }
        let mut reader = store.read(stored.key(), stored.artifact).await?;
        let bytes = reader.next().await?.ok_or(IndexError::Integrity)?;
        if reader.next().await?.is_some() {
            return Err(IndexError::Integrity);
        }
        let (snapshot, operation) = Self::decode(&bytes)?;
        if snapshot.repository != stored.repository
            || snapshot.format != stored.format
            || operation != stored.operation
        {
            return Err(IndexError::Integrity);
        }
        Ok(snapshot)
    }
}
