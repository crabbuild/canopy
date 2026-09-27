use super::*;
use axum::body::HttpBody;
use bytes::{Bytes, BytesMut};
use futures_core::Stream;
use object_store::{MultipartUpload, ObjectStoreExt};
use sha2::{Digest, Sha256};
use std::{future::poll_fn, pin::Pin, time::Duration};

const UPLOAD_TIMEOUT: Duration = Duration::from_secs(30 * 60);

pub(super) async fn receive(
    store: Arc<dyn ObjectStore>,
    repository_id: [u8; 16],
    oid: [u8; 32],
    body: Body,
    admission: Option<Arc<AdmissionPermit>>,
) -> Result<LfsObject, LfsError> {
    let declared = body.size_hint().exact();
    if declared.is_some_and(|size| size > MAX_LFS_BYTES) {
        return Err(LfsError::TooLarge);
    }
    let stage = Path::from(format!(
        "repos/{}/lfs-staging/{}",
        hex::encode(repository_id),
        uuid::Uuid::new_v4()
    ));
    let mut upload = tokio::time::timeout(IO_TIMEOUT, store.put_multipart(&stage))
        .await
        .map_err(|_| LfsError::Timeout)??;
    let mut completed = false;
    let result = tokio::time::timeout(UPLOAD_TIMEOUT, async {
        let object = parts(upload.as_mut(), oid, body, declared, admission.clone()).await?;
        upload.complete().await?;
        completed = true;
        // Only verified bytes reach the canonical key. Concurrent uploads must
        // adopt the same immutable body; they may never overwrite one another.
        match store
            .copy_if_not_exists(&stage, &lfs_path(repository_id, &oid))
            .await
        {
            Ok(()) | Err(object_store::Error::AlreadyExists { .. }) => {}
            Err(error) => return Err(error.into()),
        }
        verify_lfs_object(store.clone(), repository_id, object, admission).await?;
        Ok(object)
    })
    .await
    .map_err(|_| LfsError::Timeout)
    .and_then(|result| result);
    // The supervised caller retains this upload through disconnect and timeout.
    // Ambiguous completion still requires deleting the unique staging key.
    if !completed {
        match tokio::time::timeout(IO_TIMEOUT, upload.abort()).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => tracing::warn!(error = %error, "LFS multipart abort failed"),
            Err(_) => tracing::warn!("LFS multipart abort timed out"),
        }
    }
    let cleanup = match tokio::time::timeout(IO_TIMEOUT, store.delete(&stage)).await {
        Ok(Ok(())) | Ok(Err(object_store::Error::NotFound { .. })) => Ok(()),
        Ok(Err(error)) => Err(LfsError::Store(error)),
        Err(_) => Err(LfsError::Timeout),
    };
    if result.is_err()
        && let Err(error) = &cleanup
    {
        tracing::warn!(error = %error, "LFS staging cleanup failed");
    }
    let object = result?;
    cleanup?;
    Ok(object)
}

struct Hashes {
    sha256: Sha256,
    blake3: blake3::Hasher,
    size: u64,
    _admission: Option<Arc<AdmissionPermit>>,
}

impl Hashes {
    async fn update(mut self, bytes: Bytes) -> Result<(Self, Bytes), LfsError> {
        Ok(tokio::task::spawn_blocking(move || {
            self.sha256.update(&bytes);
            self.blake3.update(&bytes);
            self.size += bytes.len() as u64;
            (self, bytes)
        })
        .await?)
    }
}

async fn parts(
    upload: &mut dyn MultipartUpload,
    oid: [u8; 32],
    body: Body,
    declared: Option<u64>,
    admission: Option<Arc<AdmissionPermit>>,
) -> Result<LfsObject, LfsError> {
    let mut hashes = Hashes {
        sha256: Sha256::new(),
        blake3: blake3::Hasher::new(),
        size: 0,
        _admission: admission,
    };
    let mut input = body.into_data_stream();
    let mut buffer = BytesMut::with_capacity(CHUNK_BYTES);
    let mut received = 0_u64;
    loop {
        let frame =
            tokio::time::timeout(IO_TIMEOUT, poll_fn(|cx| Pin::new(&mut input).poll_next(cx)))
                .await
                .map_err(|_| LfsError::Timeout)?;
        let Some(frame) = frame else { break };
        let mut frame = frame?;
        received = received
            .checked_add(frame.len() as u64)
            .filter(|size| *size <= MAX_LFS_BYTES)
            .ok_or(LfsError::TooLarge)?;
        if declared.is_some_and(|size| received > size) {
            return Err(LfsError::Corrupt);
        }
        while !frame.is_empty() {
            let length = frame.len().min(CHUNK_BYTES - buffer.len());
            buffer.extend_from_slice(&frame.split_to(length));
            if buffer.len() == CHUNK_BYTES {
                let (updated, bytes) = hashes.update(buffer.split().freeze()).await?;
                hashes = updated;
                upload.put_part(bytes.into()).await?;
            }
        }
    }
    if declared.is_some_and(|size| size != received) {
        return Err(LfsError::Corrupt);
    }
    if !buffer.is_empty() || received == 0 {
        let (updated, bytes) = hashes.update(buffer.freeze()).await?;
        hashes = updated;
        upload.put_part(bytes.into()).await?;
    }
    if hashes.sha256.finalize().as_slice() != oid {
        return Err(LfsError::Corrupt);
    }
    Ok(LfsObject {
        sha256: oid,
        size: hashes.size,
        blake3: *hashes.blake3.finalize().as_bytes(),
    })
}
