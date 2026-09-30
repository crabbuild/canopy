//! Immutable external bytes for large Git blobs.

use std::{sync::Arc, time::Duration};

use bytes::Bytes;
use canopy_git_format::{ObjectFormat, ObjectHasher, ObjectId, ObjectKind};
use object_store::{ObjectStore, path::Path};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncRead, AsyncReadExt};

mod read;
pub use read::LargeBlobRead;

const CHUNK_BYTES: usize = crate::external::PART_BYTES;
const IO_TIMEOUT: Duration = Duration::from_secs(120);

#[derive(Clone, Copy, Debug)]
pub struct LargeBlobReference {
    pub oid: ObjectId,
    pub size: u64,
    pub blake3: [u8; 32],
    pub sha256: [u8; 32],
}

#[derive(Debug, thiserror::Error)]
pub enum LargeBlobError {
    #[error("large blob store failed")]
    Store(#[from] object_store::Error),
    #[error("large blob input failed")]
    Io(#[from] std::io::Error),
    #[error("large blob worker failed")]
    Task(#[from] tokio::task::JoinError),
    #[error("large blob transfer timed out")]
    Timeout,
    #[error("large blob bytes disagree with their SQLite reference")]
    Corrupt,
}

struct Hashes {
    oid: ObjectHasher,
    sha256: Sha256,
    blake3: blake3::Hasher,
}

impl Hashes {
    fn new(format: ObjectFormat, size: u64) -> Self {
        let oid = ObjectHasher::new(format, ObjectKind::Blob, size);
        Self {
            oid,
            sha256: Sha256::new(),
            blake3: blake3::Hasher::new(),
        }
    }

    async fn update(mut self, bytes: Bytes) -> Result<(Self, Bytes), LargeBlobError> {
        Ok(tokio::task::spawn_blocking(move || {
            self.oid.update(&bytes);
            self.sha256.update(&bytes);
            self.blake3.update(&bytes);
            (self, bytes)
        })
        .await?)
    }

    fn finish(self, size: u64) -> LargeBlobReference {
        LargeBlobReference {
            oid: self.oid.finalize(),
            size,
            sha256: self.sha256.finalize().into(),
            blake3: *self.blake3.finalize().as_bytes(),
        }
    }
}

/// Repository-scoped content-addressed body store.
pub struct LargeBlobStore {
    store: Arc<dyn ObjectStore>,
    repository_id: [u8; 16],
}

impl LargeBlobStore {
    pub fn new(store: Arc<dyn ObjectStore>, repository_id: [u8; 16]) -> Self {
        Self {
            store,
            repository_id,
        }
    }

    /// Streams exactly `size` bytes, verifying the Git OID before immutable publication.
    /// The caller retains this future through cleanup; abrupt process loss may leave staging.
    pub async fn put(
        &self,
        oid: ObjectId,
        size: u64,
        input: &mut (impl AsyncRead + Unpin),
    ) -> Result<LargeBlobReference, LargeBlobError> {
        let stage = Path::from(format!(
            "repos/{}/git-blob-staging/{}",
            hex::encode(self.repository_id),
            uuid::Uuid::new_v4()
        ));
        let mut upload = crate::external::Upload::new(self.store.clone(), stage).await?;
        let result = async {
            let mut hashes = Hashes::new(oid.format(), size);
            let mut remaining = size;
            loop {
                let length = remaining.min(CHUNK_BYTES as u64) as usize;
                let mut bytes = vec![0; length];
                tokio::time::timeout(IO_TIMEOUT, input.read_exact(&mut bytes))
                    .await
                    .map_err(|_| LargeBlobError::Timeout)??;
                let (updated, bytes) = hashes.update(Bytes::from(bytes)).await?;
                hashes = updated;
                upload.write(bytes).await?;
                remaining -= length as u64;
                if remaining == 0 {
                    break;
                }
            }
            let reference = hashes.finish(size);
            if reference.oid != oid {
                return Err(LargeBlobError::Corrupt);
            }
            upload
                .publish(&blob_path(self.repository_id, &reference.sha256), size)
                .await?;
            self.verify(&reference).await?;
            Ok(reference)
        }
        .await;
        let cleanup = upload.cleanup().await;
        if result.is_err()
            && let Err(error) = &cleanup
        {
            tracing::warn!(error = %error, "Git blob staging cleanup failed");
        }
        let reference = result?;
        cleanup?;
        Ok(reference)
    }

    /// Opens a bounded reader that validates all three hashes before returning its final range.
    pub async fn read(
        &self,
        reference: &LargeBlobReference,
    ) -> Result<LargeBlobRead, LargeBlobError> {
        LargeBlobRead::open(self.store.clone(), self.repository_id, *reference).await
    }

    /// Checks an immutable object's bytes without materializing the full body.
    pub async fn verify(&self, reference: &LargeBlobReference) -> Result<(), LargeBlobError> {
        let mut read = self.read(reference).await?;
        while read.next().await?.is_some() {}
        Ok(())
    }
}

pub fn blob_path(repository_id: [u8; 16], sha256: &[u8; 32]) -> Path {
    Path::from(format!(
        "repos/{}/git-blobs/{}",
        hex::encode(repository_id),
        hex::encode(sha256)
    ))
}

#[cfg(test)]
mod tests;
