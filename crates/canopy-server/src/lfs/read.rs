use super::*;
use bytes::Bytes;
use futures_core::Stream;
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
    state: Option<ReadState>,
    next: Option<NextRead>,
}

struct ReadState {
    store: Arc<dyn ObjectStore>,
    path: Path,
    manifest: crate::external::Manifest,
    expected: LfsObject,
    offset: u64,
    sha256: Sha256,
    verify_full: bool,
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
        let manifest = crate::external::open_hashed(
            store.as_ref(),
            &path,
            expected.size,
            expected.parts_digest,
        )
        .await?;
        let state = ReadState {
            store,
            path,
            manifest,
            expected,
            offset: 0,
            sha256: Sha256::new(),
            verify_full: true,
            _admission: admission,
        };
        let state = if expected.size == 0 {
            state.verify()?;
            None
        } else {
            Some(state)
        };
        Ok(Self {
            size: expected.size,
            emit_from: 0,
            state,
            next: None,
        })
    }

    /// Returns the verified metadata size advertised by the HTTP response.
    pub fn size(&self) -> u64 {
        self.size
    }

    /// Starts at the part containing the client's existing prefix.
    pub(crate) fn resume_from(mut self, offset: u64) -> Self {
        self.emit_from = offset;
        if let Some(state) = self.state.as_mut() {
            state.offset = offset / CHUNK_BYTES as u64 * CHUNK_BYTES as u64;
            state.verify_full = offset == 0;
        }
        self
    }
}

impl Stream for LfsRead {
    type Item = Result<Bytes, LfsError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            if self.next.is_none() {
                let Some(state) = self.state.take() else {
                    return Poll::Ready(None);
                };
                self.next = Some(Box::pin(state.read()));
            }
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
                        self.state = Some(state);
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
            &self.manifest,
            self.expected.size,
            self.offset,
        )
        .await?;
        let end = self.offset + bytes.len() as u64;
        tokio::task::spawn_blocking(move || {
            let part = self.offset / CHUNK_BYTES as u64;
            if self.manifest.part_digest(part) != Some(*blake3::hash(&bytes).as_bytes()) {
                return Err(LfsError::Corrupt);
            }
            if self.verify_full {
                self.sha256.update(&bytes);
            }
            self.offset = end;
            // A full download withholds its final range until the LFS OID
            // matches. Tail reads verify each transmitted part before yielding.
            if self.verify_full && self.offset == self.expected.size {
                self.verify()?;
            }
            Ok((self, bytes))
        })
        .await?
    }

    fn verify(&self) -> Result<(), LfsError> {
        if self.expected.size == 0
            && self.manifest.part_digest(0) != Some(*blake3::hash(b"").as_bytes())
        {
            return Err(LfsError::Corrupt);
        }
        if self.sha256.clone().finalize().as_slice() != self.expected.sha256 {
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
