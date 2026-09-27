//! Immutable external bytes for large Git blobs.

use std::{sync::Arc, time::Duration};

use bytes::Bytes;
use object_store::{ObjectStore, ObjectStoreExt, path::Path};
use sha1::Sha1;
use sha2::{Digest, Sha256};
use tokio::io::{AsyncRead, AsyncReadExt};

mod read;
pub use read::LargeBlobRead;

// S3's conditional multipart copy currently copies one part, capped at 5 GiB.
pub const MAX_EXTERNAL_BLOB_BYTES: u64 = 5 * 1024 * 1024 * 1024;
const CHUNK_BYTES: usize = 8 * 1024 * 1024;
const IO_TIMEOUT: Duration = Duration::from_secs(120);

#[derive(Clone, Copy, Debug)]
pub struct LargeBlobReference {
    pub oid: [u8; 20],
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
    #[error("large blob exceeds configured byte ceiling")]
    TooLarge,
    #[error("large blob bytes disagree with their SQLite reference")]
    Corrupt,
}

struct Hashes {
    oid: Sha1,
    sha256: Sha256,
    blake3: blake3::Hasher,
}

impl Hashes {
    fn new(size: u64) -> Self {
        let mut oid = Sha1::new();
        oid.update(format!("blob {size}\0").as_bytes());
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
            oid: self.oid.finalize().into(),
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
        oid: [u8; 20],
        size: u64,
        input: &mut (impl AsyncRead + Unpin),
    ) -> Result<LargeBlobReference, LargeBlobError> {
        if size > MAX_EXTERNAL_BLOB_BYTES {
            return Err(LargeBlobError::TooLarge);
        }
        let stage = Path::from(format!(
            "repos/{}/git-blob-staging/{}",
            hex::encode(self.repository_id),
            uuid::Uuid::new_v4()
        ));
        let mut upload = tokio::time::timeout(IO_TIMEOUT, self.store.put_multipart(&stage))
            .await
            .map_err(|_| LargeBlobError::Timeout)??;
        let mut completed = false;
        let result = tokio::time::timeout(Duration::from_secs(30 * 60), async {
            let mut hashes = Hashes::new(size);
            let mut remaining = size;
            loop {
                let length = remaining.min(CHUNK_BYTES as u64) as usize;
                let mut bytes = vec![0; length];
                tokio::time::timeout(IO_TIMEOUT, input.read_exact(&mut bytes))
                    .await
                    .map_err(|_| LargeBlobError::Timeout)??;
                let (updated, bytes) = hashes.update(Bytes::from(bytes)).await?;
                hashes = updated;
                tokio::time::timeout(IO_TIMEOUT, upload.put_part(bytes.into()))
                    .await
                    .map_err(|_| LargeBlobError::Timeout)??;
                remaining -= length as u64;
                if remaining == 0 {
                    break;
                }
            }
            let reference = hashes.finish(size);
            if reference.oid != oid {
                return Err(LargeBlobError::Corrupt);
            }
            upload.complete().await?;
            completed = true;
            // Competing pushes may adopt verified bytes, never replace a canonical
            // object. Read back the destination before returning publishable metadata.
            match self
                .store
                .copy_if_not_exists(&stage, &blob_path(self.repository_id, &reference.sha256))
                .await
            {
                Ok(()) | Err(object_store::Error::AlreadyExists { .. }) => {}
                Err(error) => return Err(error.into()),
            }
            self.verify(&reference).await?;
            Ok(reference)
        })
        .await
        .map_err(|_| LargeBlobError::Timeout)
        .and_then(|result| result);
        if !completed {
            match tokio::time::timeout(IO_TIMEOUT, upload.abort()).await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => tracing::warn!(error = %error, "Git blob multipart abort failed"),
                Err(_) => tracing::warn!("Git blob multipart abort timed out"),
            }
        }
        let cleanup = match tokio::time::timeout(IO_TIMEOUT, self.store.delete(&stage)).await {
            Ok(Ok(())) | Ok(Err(object_store::Error::NotFound { .. })) => Ok(()),
            Ok(Err(error)) => Err(LargeBlobError::Store(error)),
            Err(_) => Err(LargeBlobError::Timeout),
        };
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

pub(crate) fn blob_path(repository_id: [u8; 16], sha256: &[u8; 32]) -> Path {
    Path::from(format!(
        "repos/{}/git-blobs/{}",
        hex::encode(repository_id),
        hex::encode(sha256)
    ))
}

#[cfg(test)]
mod tests;
