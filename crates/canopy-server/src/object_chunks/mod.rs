//! Staged SQLite chunks, verified before an immutable object record is published.

use cellule_runtime::{
    Error, InvocationError, MutationIdentity, identity::RequestId, primitives::sql::SqlBatch,
    primitives::sql::SqlResultSet, primitives::sql::SqlStatement, primitives::sql::SqlValue,
    registry::CommandContext,
};

use crate::{
    INLINE_OBJECT_LIMIT, ObjectKind, ObjectStorage, RepositoryCell, StoredObject, object_id,
};

pub(crate) const CHUNK_BYTES: usize = 512 * 1024;

#[derive(Debug, thiserror::Error)]
pub enum ObjectStageError {
    #[error("only non-blob objects above the inline limit can be staged")]
    Invalid,
    #[error("object chunk staging failed")]
    Cell(#[from] InvocationError<Vec<SqlResultSet>>),
}

impl RepositoryCell {
    /// Stages a large non-blob object in SQLite; put_objects must verify and publish the returned record.
    pub async fn stage_object(
        &self,
        identity: MutationIdentity,
        kind: ObjectKind,
        body: &[u8],
    ) -> Result<StoredObject, ObjectStageError> {
        if kind == ObjectKind::Blob
            || body.len() <= INLINE_OBJECT_LIMIT
            || i64::try_from(body.len()).is_err()
        {
            return Err(ObjectStageError::Invalid);
        }
        let upload = *identity.request_id.as_bytes();
        let mut statements = vec![SqlStatement {
            sql: "INSERT INTO object_uploads (id) VALUES (?1)".into(),
            parameters: vec![SqlValue::Blob(upload.to_vec())],
        }];
        for (part, bytes) in body.chunks(CHUNK_BYTES).enumerate() {
            statements.push(SqlStatement {
                sql: "INSERT INTO object_chunks (upload_id, part, body) VALUES (?1, ?2, ?3)".into(),
                parameters: vec![
                    SqlValue::Blob(upload.to_vec()),
                    SqlValue::Integer(part as i64),
                    SqlValue::Blob(bytes.to_vec()),
                ],
            });
            let mut hash = blake3::Hasher::new();
            hash.update(b"canopy-object-chunk-v1");
            hash.update(&upload);
            hash.update(&(part as u64).to_le_bytes());
            let mut request_id = [0; 16];
            request_id.copy_from_slice(&hash.finalize().as_bytes()[..16]);
            self.sql
                .batch(
                    MutationIdentity {
                        request_id: RequestId::from_bytes(request_id),
                        issued_at_ms: identity.issued_at_ms,
                        expires_at_ms: identity.expires_at_ms,
                    },
                    SqlBatch {
                        statements: std::mem::take(&mut statements),
                    },
                )
                .await?;
        }
        Ok(StoredObject {
            oid: object_id(self.object_format(), kind, body),
            kind,
            storage: ObjectStorage::Chunked {
                upload,
                size: body.len() as u64,
                blake3: *blake3::hash(body).as_bytes(),
            },
        })
    }

    pub(crate) async fn chunked_body(
        &self,
        oid: crate::ObjectId,
        kind: ObjectKind,
        upload: [u8; 16],
        size: u64,
        digest: [u8; 32],
    ) -> Result<Vec<u8>, InvocationError<Vec<SqlResultSet>>> {
        let invalid =
            || InvocationError::NotStarted(Error::Command("incomplete or corrupt object chunks"));
        let mut chunks = Chunks::new(oid, kind, upload, size, digest).ok_or_else(invalid)?;
        while !chunks.complete() {
            let result = self.sql.query(None, chunks.query()).await?;
            if !chunks.append(&result.output) {
                return Err(invalid());
            }
        }
        chunks.finish().ok_or_else(invalid)
    }
}

/// The caller's command transaction binds verification and publication atomically.
pub(crate) fn body(
    context: &CommandContext<'_, '_>,
    oid: crate::ObjectId,
    kind: ObjectKind,
    upload: [u8; 16],
    size: u64,
    digest: [u8; 32],
) -> cellule_runtime::Result<Option<Vec<u8>>> {
    let Some(mut chunks) = Chunks::new(oid, kind, upload, size, digest) else {
        return Ok(None);
    };
    while !chunks.complete() {
        let result = context.sql(&chunks.snapshot_query())?;
        if !chunks.append_snapshot(&result) {
            return Ok(None);
        }
    }
    Ok(chunks.finish())
}

struct Chunks {
    oid: crate::ObjectId,
    kind: ObjectKind,
    upload: [u8; 16],
    size: usize,
    digest: [u8; 32],
    body: Vec<u8>,
    part: usize,
}

impl Chunks {
    fn new(
        oid: crate::ObjectId,
        kind: ObjectKind,
        upload: [u8; 16],
        size: u64,
        digest: [u8; 32],
    ) -> Option<Self> {
        let size = usize::try_from(size).ok()?;
        if kind == ObjectKind::Blob || size <= INLINE_OBJECT_LIMIT || i64::try_from(size).is_err() {
            return None;
        }
        Some(Self {
            oid,
            kind,
            upload,
            size,
            digest,
            body: Vec::new(),
            part: 0,
        })
    }

    fn query(&self) -> SqlBatch {
        SqlBatch { statements: vec![SqlStatement {
            // Count also rejects extra chunks; each result carries at most one 512 KiB body.
            sql: "SELECT body, (SELECT COUNT(*) FROM object_chunks WHERE upload_id = ?1) FROM object_chunks WHERE upload_id = ?1 AND part = ?2".into(),
            parameters: vec![SqlValue::Blob(self.upload.to_vec()), SqlValue::Integer(self.part as i64)],
        }] }
    }

    fn snapshot_query(&self) -> SqlBatch {
        if self.part == 0 {
            return self.query();
        }
        // The command transaction keeps the count checked by the first read
        // stable. Recounting every part scans this upload quadratically.
        SqlBatch {
            statements: vec![SqlStatement {
                sql: "SELECT body FROM object_chunks WHERE upload_id = ?1 AND part = ?2".into(),
                parameters: vec![
                    SqlValue::Blob(self.upload.to_vec()),
                    SqlValue::Integer(self.part as i64),
                ],
            }],
        }
    }

    fn append_snapshot(&mut self, results: &[SqlResultSet]) -> bool {
        if self.part == 0 {
            return self.append(results);
        }
        let Some([SqlValue::Blob(bytes)]) = results
            .first()
            .and_then(|set| set.rows.first())
            .map(Vec::as_slice)
        else {
            return false;
        };
        self.append_bytes(bytes)
    }

    fn append(&mut self, results: &[SqlResultSet]) -> bool {
        let Some([SqlValue::Blob(bytes), SqlValue::Integer(count)]) = results
            .first()
            .and_then(|set| set.rows.first())
            .map(Vec::as_slice)
        else {
            return false;
        };
        if *count != self.size.div_ceil(CHUNK_BYTES) as i64 {
            return false;
        }
        self.append_bytes(bytes)
    }

    fn append_bytes(&mut self, bytes: &[u8]) -> bool {
        if bytes.len() != CHUNK_BYTES.min(self.size - self.body.len()) {
            return false;
        }
        self.body.extend_from_slice(bytes);
        self.part += 1;
        true
    }

    fn complete(&self) -> bool {
        self.body.len() == self.size
    }

    fn finish(self) -> Option<Vec<u8>> {
        (object_id(self.oid.format(), self.kind, &self.body) == self.oid
            && blake3::hash(&self.body).as_bytes() == &self.digest)
            .then_some(self.body)
    }
}

#[cfg(test)]
mod tests;
