//! Staged SQLite chunks, verified before an immutable object record is published.

use cellule_runtime::{
    CommandContext, Error, InvocationError, MutationIdentity, RequestId, SqlBatch, SqlResultSet,
    SqlStatement, SqlValue,
};

use crate::{
    INLINE_OBJECT_LIMIT, ObjectKind, ObjectStorage, RepositoryCell, StoredObject, object_id,
};

pub(crate) const CHUNK_BYTES: usize = 512 * 1024;
pub const MAX_SQLITE_OBJECT_BYTES: usize = 64 * 1024 * 1024;

#[derive(Debug, thiserror::Error)]
pub enum ObjectStageError {
    #[error("only non-blob objects above the inline limit and at most 64 MiB can be staged")]
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
            || !(INLINE_OBJECT_LIMIT + 1..=MAX_SQLITE_OBJECT_BYTES).contains(&body.len())
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
            oid: object_id(kind, body),
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
        oid: [u8; 20],
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
    oid: [u8; 20],
    kind: ObjectKind,
    upload: [u8; 16],
    size: u64,
    digest: [u8; 32],
) -> cellule_runtime::Result<Option<Vec<u8>>> {
    let Some(mut chunks) = Chunks::new(oid, kind, upload, size, digest) else {
        return Ok(None);
    };
    while !chunks.complete() {
        if !chunks.append(&context.sql(&chunks.query())?) {
            return Ok(None);
        }
    }
    Ok(chunks.finish())
}

struct Chunks {
    oid: [u8; 20],
    kind: ObjectKind,
    upload: [u8; 16],
    size: usize,
    digest: [u8; 32],
    body: Vec<u8>,
    part: usize,
}

impl Chunks {
    fn new(
        oid: [u8; 20],
        kind: ObjectKind,
        upload: [u8; 16],
        size: u64,
        digest: [u8; 32],
    ) -> Option<Self> {
        let size = usize::try_from(size).ok()?;
        if kind == ObjectKind::Blob
            || !(INLINE_OBJECT_LIMIT + 1..=MAX_SQLITE_OBJECT_BYTES).contains(&size)
        {
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

    fn append(&mut self, results: &[SqlResultSet]) -> bool {
        let Some([SqlValue::Blob(bytes), SqlValue::Integer(count)]) = results
            .first()
            .and_then(|set| set.rows.first())
            .map(Vec::as_slice)
        else {
            return false;
        };
        if *count != self.size.div_ceil(CHUNK_BYTES) as i64
            || bytes.len() != CHUNK_BYTES.min(self.size - self.body.len())
        {
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
        (object_id(self.kind, &self.body) == self.oid
            && blake3::hash(&self.body).as_bytes() == &self.digest)
            .then_some(self.body)
    }
}
