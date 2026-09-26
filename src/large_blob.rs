//! Immutable external bytes for large Git blobs.

use std::sync::Arc;

use bytes::Bytes;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, path::Path};
use sha2::{Digest, Sha256};

use crate::{ObjectKind, object_id};

pub const MAX_EXTERNAL_BLOB_BYTES: usize = 64 * 1024 * 1024;

#[derive(Clone, Copy)]
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
    #[error("large blob exceeds configured byte ceiling")]
    TooLarge,
    #[error("large blob bytes disagree with their SQLite reference")]
    Corrupt,
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

    pub async fn put(&self, body: &[u8]) -> Result<LargeBlobReference, LargeBlobError> {
        if body.len() > MAX_EXTERNAL_BLOB_BYTES {
            return Err(LargeBlobError::TooLarge);
        }
        let reference = LargeBlobReference {
            oid: object_id(ObjectKind::Blob, body),
            size: u64::try_from(body.len()).map_err(|_| LargeBlobError::TooLarge)?,
            blake3: *blake3::hash(body).as_bytes(),
            sha256: Sha256::digest(body).into(),
        };
        let path = self.path(&reference.sha256);
        match self
            .store
            .put_opts(
                &path,
                Bytes::copy_from_slice(body).into(),
                PutOptions {
                    mode: PutMode::Create,
                    ..PutOptions::default()
                },
            )
            .await
        {
            Ok(_) => {}
            Err(object_store::Error::AlreadyExists { .. }) => {
                let existing = self.get(&reference).await?;
                if existing != body {
                    return Err(LargeBlobError::Corrupt);
                }
            }
            Err(error) => return Err(error.into()),
        }
        Ok(reference)
    }

    pub async fn get(&self, reference: &LargeBlobReference) -> Result<Vec<u8>, LargeBlobError> {
        if reference.size > MAX_EXTERNAL_BLOB_BYTES as u64 {
            return Err(LargeBlobError::TooLarge);
        }
        let result = self.store.get(&self.path(&reference.sha256)).await?;
        if result.meta.size != reference.size {
            return Err(LargeBlobError::Corrupt);
        }
        let body = result.bytes().await?;
        if body.len() as u64 != reference.size
            || Sha256::digest(&body).as_slice() != reference.sha256
            || blake3::hash(&body).as_bytes() != &reference.blake3
            || object_id(ObjectKind::Blob, &body) != reference.oid
        {
            return Err(LargeBlobError::Corrupt);
        }
        Ok(body.to_vec())
    }

    fn path(&self, sha256: &[u8; 32]) -> Path {
        blob_path(self.repository_id, sha256)
    }
}

pub(crate) fn blob_path(repository_id: [u8; 16], sha256: &[u8; 32]) -> Path {
    Path::from(format!(
        "repos/{}/git-blobs/{}",
        hex::encode(repository_id),
        hex::encode(sha256)
    ))
}
