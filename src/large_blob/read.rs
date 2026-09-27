use super::*;
use bytes::BytesMut;
use object_store::{GetOptions, GetRange, ObjectMeta};
use std::future::poll_fn;

/// Sequential bounded ranges from one immutable Git blob.
pub struct LargeBlobRead {
    store: Arc<dyn ObjectStore>,
    path: Path,
    meta: ObjectMeta,
    reference: LargeBlobReference,
    offset: u64,
    hashes: Option<Hashes>,
}

impl LargeBlobRead {
    pub(super) async fn open(
        store: Arc<dyn ObjectStore>,
        repository_id: [u8; 16],
        reference: LargeBlobReference,
    ) -> Result<Self, LargeBlobError> {
        if reference.size > MAX_EXTERNAL_BLOB_BYTES {
            return Err(LargeBlobError::TooLarge);
        }
        let path = blob_path(repository_id, &reference.sha256);
        let meta = tokio::time::timeout(IO_TIMEOUT, store.head(&path))
            .await
            .map_err(|_| LargeBlobError::Timeout)??;
        if meta.size != reference.size {
            return Err(LargeBlobError::Corrupt);
        }
        let read = Self {
            store,
            path,
            meta,
            reference,
            offset: 0,
            hashes: Some(Hashes::new(reference.size)),
        };
        if reference.size == 0 {
            read.check(Hashes::new(0))?;
        }
        Ok(read)
    }

    pub(crate) fn reference(&self) -> LargeBlobReference {
        self.reference
    }

    /// Returns at most 8 MiB; an error or canceled read invalidates the reader.
    pub async fn next(&mut self) -> Result<Option<Bytes>, LargeBlobError> {
        if self.offset == self.reference.size {
            return Ok(None);
        }
        // Taking the verifier poisons cancellation/error paths. A partially read
        // range cannot be silently retried with a truncated checksum history.
        let hashes = self.hashes.take().ok_or(LargeBlobError::Corrupt)?;
        let bytes = tokio::time::timeout(IO_TIMEOUT, self.range())
            .await
            .map_err(|_| LargeBlobError::Timeout)??;
        let (hashes, bytes) = hashes.update(bytes).await?;
        let end = self.offset + bytes.len() as u64;
        if end == self.reference.size {
            self.check(hashes)?;
        } else {
            self.hashes = Some(hashes);
        }
        self.offset = end;
        Ok(Some(bytes))
    }

    async fn range(&self) -> Result<Bytes, LargeBlobError> {
        let end = self.reference.size.min(self.offset + CHUNK_BYTES as u64);
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
        if result.meta.size != self.reference.size || result.range != range {
            return Err(LargeBlobError::Corrupt);
        }
        let length = (end - self.offset) as usize;
        let mut bytes = BytesMut::with_capacity(length);
        let mut input = result.into_stream();
        while let Some(chunk) = poll_fn(|cx| input.as_mut().poll_next(cx)).await {
            let chunk = chunk?;
            if chunk.len() > length - bytes.len() {
                return Err(LargeBlobError::Corrupt);
            }
            bytes.extend_from_slice(&chunk);
        }
        if bytes.len() != length {
            return Err(LargeBlobError::Corrupt);
        }
        Ok(bytes.freeze())
    }

    fn check(&self, hashes: Hashes) -> Result<(), LargeBlobError> {
        let actual = hashes.finish(self.reference.size);
        if actual.oid != self.reference.oid
            || actual.sha256 != self.reference.sha256
            || actual.blake3 != self.reference.blake3
        {
            return Err(LargeBlobError::Corrupt);
        }
        Ok(())
    }
}
