use super::*;
use bytes::Bytes;
use futures_core::Stream;
use object_store::ObjectMeta;
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
    emit_from: u64,
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
    _admission: Option<Arc<AdmissionPermit>>,
}

impl LfsRead {
    pub(super) async fn open(
        store: Arc<dyn ObjectStore>,
        repository_id: [u8; 16],
        expected: LfsObject,
        admission: Option<Arc<AdmissionPermit>>,
    ) -> Result<Self, LfsError> {
        let path = lfs_path(repository_id, &expected.sha256);
        let meta = crate::external::open(store.as_ref(), &path, expected.size).await?;
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
            emit_from: 0,
            next,
        })
    }

    /// Returns the verified metadata size advertised by the HTTP response.
    pub fn size(&self) -> u64 {
        self.size
    }

    /// Skips bytes already held by a resuming client while hashing every part.
    pub(crate) fn resume_from(mut self, offset: u64) -> Self {
        self.emit_from = offset;
        self
    }
}

impl Stream for LfsRead {
    type Item = Result<Bytes, LfsError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            let Some(next) = self.next.as_mut() else {
                return Poll::Ready(None);
            };
            let result = std::task::ready!(next.as_mut().poll(cx));
            self.next = None;
            match result {
                Ok((state, bytes)) => {
                    let begin = state.offset - bytes.len() as u64;
                    let skip =
                        self.emit_from.saturating_sub(begin).min(bytes.len() as u64) as usize;
                    if state.offset < state.expected.size {
                        self.next = Some(Box::pin(state.read()));
                    }
                    if skip < bytes.len() {
                        return Poll::Ready(Some(Ok(bytes.slice(skip..))));
                    }
                }
                Err(error) => return Poll::Ready(Some(Err(error))),
            }
        }
    }
}

impl ReadState {
    async fn read(self) -> Result<(Self, Bytes), LfsError> {
        tokio::time::timeout(IO_TIMEOUT, self.read_range())
            .await
            .map_err(|_| LfsError::Timeout)?
    }

    async fn read_range(mut self) -> Result<(Self, Bytes), LfsError> {
        let bytes = crate::external::read(
            self.store.as_ref(),
            &self.path,
            &self.meta,
            self.expected.size,
            self.offset,
        )
        .await?;
        let end = self.offset + bytes.len() as u64;
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
    admission: Option<Arc<AdmissionPermit>>,
) -> Result<(), LfsError> {
    let mut reader = LfsRead::open(store, repository_id, object, admission).await?;
    while let Some(chunk) = poll_fn(|cx| Pin::new(&mut reader).poll_next(cx)).await {
        chunk?;
    }
    Ok(())
}
