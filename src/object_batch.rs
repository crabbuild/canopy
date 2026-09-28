//! Bounded, atomic publication of verified repository object records.

use std::collections::BTreeSet;

use cellule_runtime::{
    CellModule, Command, Committed, Error, InvocationError, MutationIdentity, Observed,
    codec::BoundedDecoder, codec::BoundedEncoder, codec::CodecError, codec::WireValue,
    primitives::sql::SqlBatch, primitives::sql::SqlResultSet, primitives::sql::SqlStatement,
    primitives::sql::SqlValue, registry::CommandContext, registry::CommandResult,
};

use crate::{
    INLINE_OBJECT_LIMIT, ObjectKind, ObjectStorage, RepositoryCell, RepositoryModule, StoredObject,
    object_id,
};

pub(crate) const MAX_OBJECTS: usize = 128;
pub(crate) const VERIFY_BATCH_BYTES: u64 = 64 * 1024 * 1024;
pub(crate) const INPUT_LIMIT: u32 = 4 * 1024 * 1024;
// Leave room for record metadata inside Cellule's bounded command wire format.
const INLINE_BATCH_BYTES: usize = 3 * 1024 * 1024;

/// Batches small objects together and verifies oversized SQLite objects individually.
#[derive(Default)]
pub struct ObjectBatch {
    objects: Vec<StoredObject>,
    inline_bytes: usize,
    verified_bytes: u64,
}

impl ObjectBatch {
    /// Adds a record, returning it unchanged if the batch's count or byte budget is full.
    pub fn try_push(&mut self, object: StoredObject) -> Result<(), StoredObject> {
        let bytes = match &object.storage {
            ObjectStorage::Inline(body) => body.len(),
            ObjectStorage::External { .. } | ObjectStorage::Chunked { .. } => 0,
        };
        let verified = match &object.storage {
            ObjectStorage::Inline(body) => body.len() as u64,
            ObjectStorage::Chunked { size, .. } => *size,
            ObjectStorage::External { .. } => 0,
        };
        if self.objects.len() == MAX_OBJECTS
            || bytes > INLINE_OBJECT_LIMIT
            || bytes > INLINE_BATCH_BYTES - self.inline_bytes
            || i64::try_from(verified).is_err()
            || (!self.objects.is_empty()
                && verified > VERIFY_BATCH_BYTES.saturating_sub(self.verified_bytes))
        {
            return Err(object);
        }
        self.inline_bytes += bytes;
        self.verified_bytes += verified;
        self.objects.push(object);
        Ok(())
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.objects.is_empty()
    }
}

impl WireValue for ObjectBatch {
    fn encode(&self, encoder: &mut BoundedEncoder) -> Result<(), CodecError> {
        if self.is_empty() {
            return Err(CodecError::Invalid("object batch is empty"));
        }
        encoder.write_count(self.objects.len())?;
        for object in &self.objects {
            encoder.write_bytes(&object.oid)?;
            encoder.write_u8(match object.kind {
                ObjectKind::Blob => 0,
                ObjectKind::Tree => 1,
                ObjectKind::Commit => 2,
                ObjectKind::Tag => 3,
            })?;
            match &object.storage {
                ObjectStorage::Inline(body) => {
                    encoder.write_u8(0)?;
                    encoder.write_bytes(body)?;
                }
                ObjectStorage::Chunked {
                    upload,
                    size,
                    blake3,
                } => {
                    encoder.write_u8(2)?;
                    encoder.write_bytes(upload)?;
                    encoder.write_u64(*size)?;
                    encoder.write_bytes(blake3)?;
                }
                ObjectStorage::External {
                    size,
                    blake3,
                    sha256,
                } => {
                    encoder.write_u8(1)?;
                    encoder.write_u64(*size)?;
                    encoder.write_bytes(blake3)?;
                    encoder.write_bytes(sha256)?;
                }
            }
        }
        Ok(())
    }

    fn decode(decoder: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        let count = decoder.read_count()?;
        if !(1..=MAX_OBJECTS).contains(&count) {
            return Err(CodecError::Invalid("object count is outside batch bounds"));
        }
        let mut batch = Self::default();
        for _ in 0..count {
            let oid = crate::ObjectId::try_from(decoder.read_bytes()?)
                .map_err(|_| CodecError::Invalid("invalid Git object ID"))?;
            let kind = match decoder.read_u8()? {
                0 => ObjectKind::Blob,
                1 => ObjectKind::Tree,
                2 => ObjectKind::Commit,
                3 => ObjectKind::Tag,
                _ => return Err(CodecError::Invalid("invalid Git object kind")),
            };
            let storage = match decoder.read_u8()? {
                0 => {
                    let body = decoder.read_bytes()?;
                    if body.len() > INLINE_OBJECT_LIMIT
                        || body.len() > INLINE_BATCH_BYTES - batch.inline_bytes
                    {
                        return Err(CodecError::Invalid(
                            "object batch exceeds inline byte budget",
                        ));
                    }
                    ObjectStorage::Inline(body.to_vec())
                }
                1 => ObjectStorage::External {
                    size: decoder.read_u64()?,
                    blake3: fixed(decoder)?,
                    sha256: fixed(decoder)?,
                },
                2 => ObjectStorage::Chunked {
                    upload: fixed(decoder)?,
                    size: decoder.read_u64()?,
                    blake3: fixed(decoder)?,
                },
                _ => return Err(CodecError::Invalid("invalid Git object storage")),
            };
            batch
                .try_push(StoredObject { oid, kind, storage })
                .map_err(|_| CodecError::Invalid("object batch exceeds bounds"))?;
        }
        Ok(batch)
    }
}

fn fixed<const N: usize>(decoder: &mut BoundedDecoder<'_>) -> Result<[u8; N], CodecError> {
    decoder
        .read_bytes()?
        .try_into()
        .map_err(|_| CodecError::Invalid("invalid object identity width"))
}

pub(crate) struct PutObjects;

impl Command for PutObjects {
    const MODULE: &'static str = RepositoryModule::NAME;
    const ID: u32 = 5;
    const CODEC_VERSION: u32 = 3;
    type Input = ObjectBatch;
    type Output = ();

    fn execute(
        context: &mut CommandContext<'_, '_>,
        batch: Self::Input,
    ) -> cellule_runtime::Result<CommandResult<()>> {
        let identity = context.sql(&SqlBatch {
            statements: vec![SqlStatement {
                sql: "SELECT object_format FROM repository_identity WHERE singleton = 1".into(),
                parameters: Vec::new(),
            }],
        })?;
        let Some([SqlValue::Text(format)]) = identity
            .first()
            .and_then(|set| set.rows.first())
            .map(Vec::as_slice)
        else {
            return Err(Error::Command("repository identity is absent"));
        };
        let format = crate::ObjectFormat::parse(format)
            .ok_or(Error::Command("invalid repository object format"))?;
        if batch
            .objects
            .iter()
            .any(|object| object.oid.format() != format)
        {
            return Ok(CommandResult::Rejected(()));
        }
        for object in batch.objects {
            let (size, digest, storage, body, sha256, chunk_id) = match object.storage {
                ObjectStorage::Inline(body) => {
                    if object_id(object.oid.format(), object.kind, &body) != object.oid {
                        return Ok(CommandResult::Rejected(()));
                    }
                    (
                        body.len() as i64,
                        blake3::hash(&body).as_bytes().to_vec(),
                        "inline",
                        SqlValue::Blob(body),
                        SqlValue::Null,
                        SqlValue::Null,
                    )
                }
                ObjectStorage::External {
                    size,
                    blake3,
                    sha256,
                } => {
                    if object.kind != ObjectKind::Blob || i64::try_from(size).is_err() {
                        return Ok(CommandResult::Rejected(()));
                    }
                    (
                        size as i64,
                        blake3.to_vec(),
                        "external",
                        SqlValue::Null,
                        SqlValue::Blob(sha256.to_vec()),
                        SqlValue::Null,
                    )
                }
                ObjectStorage::Chunked {
                    upload,
                    size,
                    blake3,
                } => {
                    if crate::object_chunks::body(
                        context,
                        object.oid,
                        object.kind,
                        upload,
                        size,
                        blake3,
                    )?
                    .is_none()
                    {
                        return Ok(CommandResult::Rejected(()));
                    }
                    (
                        size as i64,
                        blake3.to_vec(),
                        "chunked",
                        SqlValue::Null,
                        SqlValue::Null,
                        SqlValue::Blob(upload.to_vec()),
                    )
                }
            };
            let expected = vec![
                SqlValue::Text(object.kind.git_name().into()),
                SqlValue::Integer(size),
                SqlValue::Blob(digest),
                SqlValue::Text(storage.into()),
                body,
                sha256,
            ];
            let mut parameters = vec![SqlValue::Blob(object.oid.to_vec())];
            parameters.extend(expected.iter().cloned());
            parameters.push(chunk_id);
            let result = context.sql(&SqlBatch { statements: vec![
                SqlStatement {
                    sql: "INSERT INTO objects (oid, kind, size, digest, storage, body, external_sha256, chunk_id) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8) ON CONFLICT(oid) DO NOTHING".into(),
                    parameters,
                },
                SqlStatement {
                    sql: "SELECT kind, size, digest, storage, body, external_sha256 FROM objects WHERE oid = ?1".into(),
                    parameters: vec![SqlValue::Blob(object.oid.to_vec())],
                },
            ]})?;
            // A collision or corrupt existing record rejects the entire application
            // savepoint. No earlier record in this batch may survive that rejection.
            if result.get(1).and_then(|set| set.rows.first()) != Some(&expected) {
                return Ok(CommandResult::Rejected(()));
            }
        }
        Ok(CommandResult::Success(()))
    }
}

impl RepositoryCell {
    /// Publishes a bounded object batch atomically; external bytes must already be verified and uploaded.
    pub async fn put_objects(
        &self,
        identity: MutationIdentity,
        batch: ObjectBatch,
    ) -> Result<Committed<()>, InvocationError<()>> {
        self.application
            .command::<PutObjects>(&self.target, identity, batch)
            .await
    }

    /// Returns recorded IDs from a nonempty candidate list of at most 128 objects.
    pub async fn existing_objects(
        &self,
        candidates: &[crate::ObjectId],
    ) -> Result<Observed<BTreeSet<crate::ObjectId>>, InvocationError<Vec<SqlResultSet>>> {
        if !(1..=MAX_OBJECTS).contains(&candidates.len()) {
            return Err(InvocationError::NotStarted(Error::Command(
                "object lookup count is outside bounds",
            )));
        }
        let placeholders = vec!["?"; candidates.len()].join(",");
        let result = self
            .sql
            .query(
                None,
                SqlBatch {
                    statements: vec![SqlStatement {
                        sql: format!("SELECT oid FROM objects WHERE oid IN ({placeholders})"),
                        parameters: candidates
                            .iter()
                            .map(|oid| SqlValue::Blob(oid.to_vec()))
                            .collect(),
                    }],
                },
            )
            .await?;
        let rows = &result
            .output
            .first()
            .ok_or(InvocationError::NotStarted(Error::Command(
                "missing object lookup result",
            )))?
            .rows;
        let mut objects = BTreeSet::new();
        for row in rows {
            let [SqlValue::Blob(oid)] = row.as_slice() else {
                return Err(InvocationError::NotStarted(Error::Command(
                    "invalid object lookup row",
                )));
            };
            objects.insert(oid.as_slice().try_into().map_err(|_| {
                InvocationError::NotStarted(Error::Command("invalid stored Git OID"))
            })?);
        }
        Ok(Observed {
            output: objects,
            receipt: result.receipt,
        })
    }
}

#[cfg(test)]
mod tests;
