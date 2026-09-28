use super::*;

/// Sequential bounded ranges from one immutable Git blob.
pub struct LargeBlobRead {
    store: Arc<dyn ObjectStore>,
    path: Path,
    manifest: crate::external::Manifest,
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
        let path = blob_path(repository_id, &reference.sha256);
        let manifest = crate::external::open(store.as_ref(), &path, reference.size).await?;
        let read = Self {
            store,
            path,
            manifest,
            reference,
            offset: 0,
            hashes: Some(Hashes::new(reference.oid.format(), reference.size)),
        };
        if reference.size == 0 {
            read.check(Hashes::new(reference.oid.format(), 0))?;
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
        Ok(crate::external::read(
            self.store.as_ref(),
            &self.path,
            &self.manifest,
            self.reference.size,
            self.offset,
        )
        .await?)
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
