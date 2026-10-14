use super::*;
use crate::external::Upload;
use axum::body::HttpBody;
use bytes::{Bytes, BytesMut};
use futures_core::Stream;
use sha2::{Digest, Sha256};
use std::{future::poll_fn, pin::Pin};

pub(super) async fn receive(
    store: Arc<dyn ObjectStore>,
    repository_id: [u8; 16],
    oid: [u8; 32],
    body: Body,
    admission: Option<Arc<AdmissionPermit>>,
) -> Result<LfsObject, LfsError> {
    let declared = body.size_hint().exact();
    let stage = Path::from(format!(
        "repos/{}/lfs-staging/{}",
        hex::encode(repository_id),
        uuid::Uuid::new_v4()
    ));
    let mut upload = Upload::new(store.clone(), stage).await?;
    let result = async {
        let body = parts(&mut upload, oid, body, declared, admission.clone()).await?;
        let parts_digest = upload
            .publish_hashed(&lfs_path(repository_id, &oid), body.size, &body.digests)
            .await?;
        let object = LfsObject {
            sha256: oid,
            size: body.size,
            parts_digest,
        };
        verify_lfs_object(store.clone(), repository_id, object, admission).await?;
        Ok::<_, LfsError>(object)
    }
    .await;
    let cleanup = upload.cleanup().await;
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
    size: u64,
    digests: Vec<[u8; 32]>,
    _admission: Option<Arc<AdmissionPermit>>,
}

struct UploadedBody {
    size: u64,
    digests: Vec<[u8; 32]>,
}

impl Hashes {
    async fn update(mut self, bytes: Bytes) -> Result<(Self, Bytes), LfsError> {
        Ok(tokio::task::spawn_blocking(move || {
            self.sha256.update(&bytes);
            self.digests.push(*blake3::hash(&bytes).as_bytes());
            self.size += bytes.len() as u64;
            (self, bytes)
        })
        .await?)
    }
}

async fn parts(
    upload: &mut Upload,
    oid: [u8; 32],
    body: Body,
    declared: Option<u64>,
    admission: Option<Arc<AdmissionPermit>>,
) -> Result<UploadedBody, LfsError> {
    let mut hashes = Hashes {
        sha256: Sha256::new(),
        size: 0,
        digests: Vec::new(),
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
            .filter(|size| i64::try_from(*size).is_ok())
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
                upload.write(bytes).await?;
            }
        }
    }
    if declared.is_some_and(|size| size != received) {
        return Err(LfsError::Corrupt);
    }
    if !buffer.is_empty() || received == 0 {
        let (updated, bytes) = hashes.update(buffer.freeze()).await?;
        hashes = updated;
        upload.write(bytes).await?;
    }
    if hashes.sha256.finalize().as_slice() != oid {
        return Err(LfsError::Corrupt);
    }
    Ok(UploadedBody {
        size: hashes.size,
        digests: hashes.digests,
    })
}
