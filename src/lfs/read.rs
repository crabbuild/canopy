use super::*;
use bytes::{Bytes, BytesMut};
use futures_core::Stream;
use object_store::{GetOptions, GetRange, ObjectMeta, ObjectStoreExt};
use sha2::{Digest, Sha256};
use std::{
    future::{Future, poll_fn},
    pin::Pin,
    task::{Context, Poll},
};

type NextRead = Pin<Box<dyn Future<Output = Result<(ReadState, Bytes), LfsError>> + Send>>;

/// Backpressured LFS reader; a corrupt object cannot yield its final range.
pub struct LfsRead {
    size: u64,
    next: Option<NextRead>,
}

struct ReadState {
    store: Arc<dyn ObjectStore>,
    path: Path,
    meta: ObjectMeta,
    expected: LfsObject,
    offset: u64,
    sha256: Sha256,
    blake3: blake3::Hasher,
    // Hash jobs retain transfer admission after the HTTP body is dropped.
    _admission: Option<Arc<OwnedSemaphorePermit>>,
}

impl LfsRead {
    pub(super) async fn open(
        store: Arc<dyn ObjectStore>,
        repository_id: [u8; 16],
        expected: LfsObject,
        admission: Option<Arc<OwnedSemaphorePermit>>,
    ) -> Result<Self, LfsError> {
        if expected.size > MAX_LFS_BYTES {
            return Err(LfsError::TooLarge);
        }
        let path = lfs_path(repository_id, &expected.sha256);
        let meta = tokio::time::timeout(IO_TIMEOUT, store.head(&path))
            .await
            .map_err(|_| LfsError::Timeout)??;
        if meta.size != expected.size {
            return Err(LfsError::Corrupt);
        }
        let state = ReadState {
            store,
            path,
            meta,
            expected,
            offset: 0,
            sha256: Sha256::new(),
            blake3: blake3::Hasher::new(),
            _admission: admission,
        };
        let next = if expected.size == 0 {
            state.verify()?;
            None
        } else {
            Some(Box::pin(state.read()) as NextRead)
        };
        Ok(Self {
            size: expected.size,
            next,
        })
    }

    /// Returns the verified metadata size advertised by the HTTP response.
    pub fn size(&self) -> u64 {
        self.size
    }
}

impl Stream for LfsRead {
    type Item = Result<Bytes, LfsError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let Some(next) = self.next.as_mut() else {
            return Poll::Ready(None);
        };
        let result = std::task::ready!(next.as_mut().poll(cx));
        self.next = None;
        Poll::Ready(Some(match result {
            Ok((state, bytes)) => {
                if state.offset < state.expected.size {
                    self.next = Some(Box::pin(state.read()));
                }
                Ok(bytes)
            }
            Err(error) => Err(error),
        }))
    }
}

impl ReadState {
    async fn read(self) -> Result<(Self, Bytes), LfsError> {
        tokio::time::timeout(IO_TIMEOUT, self.read_range())
            .await
            .map_err(|_| LfsError::Timeout)?
    }

    async fn read_range(mut self) -> Result<(Self, Bytes), LfsError> {
        let end = self.expected.size.min(self.offset + CHUNK_BYTES as u64);
        let range = self.offset..end;
        let result = self
            .store
            .get_opts(
                &self.path,
                GetOptions {
                    range: Some(GetRange::Bounded(range.clone())),
                    if_match: self.meta.e_tag.clone(),
                    version: self.meta.version.clone(),
                    ..GetOptions::default()
                },
            )
            .await?;
        if result.meta.size != self.expected.size || result.range != range {
            return Err(LfsError::Corrupt);
        }
        let mut input = result.into_stream();
        let length = (end - self.offset) as usize;
        let mut bytes = BytesMut::with_capacity(length);
        while let Some(chunk) = poll_fn(|cx| input.as_mut().poll_next(cx)).await {
            let chunk = chunk?;
            if chunk.len() > length - bytes.len() {
                return Err(LfsError::Corrupt);
            }
            bytes.extend_from_slice(&chunk);
        }
        if bytes.len() != length {
            return Err(LfsError::Corrupt);
        }
        let bytes = bytes.freeze();
        tokio::task::spawn_blocking(move || {
            self.sha256.update(&bytes);
            self.blake3.update(&bytes);
            self.offset = end;
            // Withhold the final range until both hashes match. Content-Length
            // must never let HTTP report success before verification finishes.
            if self.offset == self.expected.size {
                self.verify()?;
            }
            Ok((self, bytes))
        })
        .await?
    }

    fn verify(&self) -> Result<(), LfsError> {
        if self.sha256.clone().finalize().as_slice() != self.expected.sha256
            || self.blake3.finalize().as_bytes() != &self.expected.blake3
        {
            return Err(LfsError::Corrupt);
        }
        Ok(())
    }
}

pub(crate) async fn verify_lfs_object(
    store: Arc<dyn ObjectStore>,
    repository_id: [u8; 16],
    object: LfsObject,
    admission: Option<Arc<OwnedSemaphorePermit>>,
) -> Result<(), LfsError> {
    let mut reader = LfsRead::open(store, repository_id, object, admission).await?;
    while let Some(chunk) = poll_fn(|cx| Pin::new(&mut reader).poll_next(cx)).await {
        chunk?;
    }
    Ok(())
}
