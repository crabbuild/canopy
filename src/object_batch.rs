//! Bounded atomic metadata publication after immutable native-pack verification.

use crate::{
    INLINE_OBJECT_LIMIT, ObjectKind, ObjectStorage, PackedObject, RepositoryCell, RepositoryModule,
    StoredObject,
};
use cellule_runtime::{
    CellModule, Command, Committed, Error, InvocationError, MutationIdentity, Observed,
    codec::{BoundedDecoder, BoundedEncoder, CodecError, WireValue},
    primitives::sql::{SqlBatch, SqlResultSet, SqlStatement, SqlValue},
    registry::{CommandContext, CommandResult},
};
use std::collections::{BTreeMap, BTreeSet};

pub(crate) const MAX_OBJECTS: usize = 512;
pub(crate) const VERIFY_BATCH_BYTES: u64 = 64 * 1024 * 1024;
pub(crate) const INPUT_LIMIT: u32 = 4 * 1024 * 1024;
const INPUT_BYTES: usize = 3 * 1024 * 1024;

#[derive(Default)]
pub struct ObjectBatch {
    objects: Vec<StoredObject>,
    input_bytes: usize,
}
impl ObjectBatch {
    pub fn try_push(&mut self, object: StoredObject) -> Result<(), StoredObject> {
        let bytes = match &object.storage {
            ObjectStorage::Input(body) => body.len(),
            ObjectStorage::Packed(_) => 0,
        };
        if self.objects.len() == MAX_OBJECTS
            || bytes > INLINE_OBJECT_LIMIT
            || bytes > INPUT_BYTES - self.input_bytes
        {
            return Err(object);
        }
        self.input_bytes += bytes;
        self.objects.push(object);
        Ok(())
    }
    pub fn is_empty(&self) -> bool {
        self.objects.is_empty()
    }
}

fn kind_tag(kind: ObjectKind) -> u8 {
    match kind {
        ObjectKind::Blob => 0,
        ObjectKind::Tree => 1,
        ObjectKind::Commit => 2,
        ObjectKind::Tag => 3,
    }
}
fn kind_decode(tag: u8) -> Result<ObjectKind, CodecError> {
    match tag {
        0 => Ok(ObjectKind::Blob),
        1 => Ok(ObjectKind::Tree),
        2 => Ok(ObjectKind::Commit),
        3 => Ok(ObjectKind::Tag),
        _ => Err(CodecError::Invalid("invalid object kind")),
    }
}
fn fixed<const N: usize>(decoder: &mut BoundedDecoder<'_>) -> Result<[u8; N], CodecError> {
    decoder
        .read_bytes()?
        .try_into()
        .map_err(|_| CodecError::Invalid("invalid identity width"))
}

impl WireValue for ObjectBatch {
    fn encode(&self, encoder: &mut BoundedEncoder) -> Result<(), CodecError> {
        if self.is_empty() {
            return Err(CodecError::Invalid("empty object batch"));
        }
        encoder.write_count(self.objects.len())?;
        for object in &self.objects {
            encoder.write_bytes(&object.oid)?;
            encoder.write_u8(kind_tag(object.kind))?;
            match &object.storage {
                ObjectStorage::Input(body) => {
                    encoder.write_u8(0)?;
                    encoder.write_bytes(body)?;
                }
                ObjectStorage::Packed(value) => {
                    encoder.write_u8(1)?;
                    encoder.write_bytes(&value.repository)?;
                    encoder.write_bytes(&value.oid)?;
                    encoder.write_u8(kind_tag(value.kind))?;
                    encoder.write_bytes(
                        &serde_json::to_vec(&value.pack)
                            .map_err(|_| CodecError::Invalid("pack identity"))?,
                    )?;
                    encoder.write_u64(value.size)?;
                    encoder.write_bytes(&value.digest)?;
                    encoder.write_u8(u8::from(value.graph_valid))?;
                    encoder.write_u64(value.graph_count)?;
                    encoder.write_bytes(&value.graph_digest)?;
                    encoder.write_bytes(value.rebase_metadata.as_deref().unwrap_or_default())?;
                }
            }
        }
        Ok(())
    }
    fn decode(decoder: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        let count = decoder.read_count()?;
        if !(1..=MAX_OBJECTS).contains(&count) {
            return Err(CodecError::Invalid("object count"));
        }
        let mut batch = Self::default();
        for _ in 0..count {
            let oid = decoder
                .read_bytes()?
                .try_into()
                .map_err(|_| CodecError::Invalid("object OID"))?;
            let kind = kind_decode(decoder.read_u8()?)?;
            let storage = match decoder.read_u8()? {
                0 => ObjectStorage::Input(decoder.read_bytes()?.to_vec()),
                1 => {
                    let repository = fixed(decoder)?;
                    let token_oid = decoder
                        .read_bytes()?
                        .try_into()
                        .map_err(|_| CodecError::Invalid("pack object OID"))?;
                    let token_kind = kind_decode(decoder.read_u8()?)?;
                    let pack = serde_json::from_slice(decoder.read_bytes()?)
                        .map_err(|_| CodecError::Invalid("pack identity"))?;
                    let size = decoder.read_u64()?;
                    let digest = fixed(decoder)?;
                    let graph_valid = match decoder.read_u8()? {
                        0 => false,
                        1 => true,
                        _ => return Err(CodecError::Invalid("graph validity")),
                    };
                    ObjectStorage::Packed(Box::new(PackedObject {
                        repository,
                        oid: token_oid,
                        kind: token_kind,
                        pack,
                        size,
                        digest,
                        graph_valid,
                        graph_count: decoder.read_u64()?,
                        graph_digest: fixed(decoder)?,
                        dependencies: None,
                        rebase_metadata: match decoder.read_bytes()? {
                            [] => None,
                            bytes if bytes.len() == 65 => Some(bytes.to_vec()),
                            _ => return Err(CodecError::Invalid("rebase metadata width")),
                        },
                    }))
                }
                _ => return Err(CodecError::Invalid("object storage")),
            };
            batch
                .try_push(StoredObject { oid, kind, storage })
                .map_err(|_| CodecError::Invalid("object batch bounds"))?;
        }
        Ok(batch)
    }
}

pub(crate) struct PutObjects;
impl Command for PutObjects {
    const MODULE: &'static str = RepositoryModule::NAME;
    const ID: u32 = 5;
    const CODEC_VERSION: u32 = 4;
    type Input = ObjectBatch;
    type Output = ();
    fn execute(
        context: &mut CommandContext<'_, '_>,
        batch: ObjectBatch,
    ) -> cellule_runtime::Result<CommandResult<()>> {
        let identity = context.sql(&SqlBatch {
            statements: vec![SqlStatement {
                sql: "SELECT object_format, repository_id FROM repository_identity WHERE singleton = 1".into(),
                parameters: vec![],
            }],
        })?;
        let Some([SqlValue::Text(format), SqlValue::Blob(repository)]) = identity
            .first()
            .and_then(|set| set.rows.first())
            .map(Vec::as_slice)
        else {
            return Err(Error::Command("repository identity absent"));
        };
        let format = crate::ObjectFormat::parse(format)
            .ok_or(Error::Command("invalid repository format"))?;
        let mut records = BTreeMap::new();
        let mut packs = BTreeMap::new();
        for object in batch.objects {
            let ObjectStorage::Packed(value) = object.storage else {
                return Ok(CommandResult::Rejected(()));
            };
            if value.repository.as_slice() != repository.as_slice()
                || object.oid != value.oid
                || object.kind != value.kind
                || object.oid.format() != format
                || value.pack.format != format
                || value.pack.pack_bytes == 0
                || value.pack.index_bytes == 0
                || (object.kind == ObjectKind::Blob
                    && (!value.graph_valid
                        || value.graph_count != 0
                        || value.graph_digest != *blake3::hash(b"").as_bytes()
                        || value.rebase_metadata.is_some()))
                || i64::try_from(value.size).is_err()
                || i64::try_from(value.pack.pack_bytes).is_err()
                || i64::try_from(value.pack.index_bytes).is_err()
                || value.graph_count > VERIFY_BATCH_BYTES / (format.bytes() as u64 + 1)
            {
                return Ok(CommandResult::Rejected(()));
            }
            let expected = vec![
                SqlValue::Text(object.kind.git_name().into()),
                SqlValue::Integer(value.size as i64),
                SqlValue::Blob(value.digest.to_vec()),
                SqlValue::Integer(i64::from(value.graph_valid)),
                SqlValue::Integer(value.graph_count as i64),
                SqlValue::Blob(value.graph_digest.to_vec()),
                value.rebase_metadata.map_or(SqlValue::Null, SqlValue::Blob),
            ];
            if let Some((previous, _)) =
                records.insert(object.oid, (expected.clone(), value.pack.pack_digest))
                && previous != expected
            {
                return Ok(CommandResult::Rejected(()));
            }
            let pack = &value.pack;
            let expected = vec![
                SqlValue::Text(pack.format.as_str().into()),
                SqlValue::Integer(pack.pack_bytes as i64),
                SqlValue::Blob(pack.index_digest.to_vec()),
                SqlValue::Integer(pack.index_bytes as i64),
            ];
            if let Some(previous) = packs.insert(pack.pack_digest, expected.clone())
                && previous != expected
            {
                return Ok(CommandResult::Rejected(()));
            }
        }
        for (digest, expected) in packs {
            let mut parameters = vec![SqlValue::Blob(digest.to_vec())];
            parameters.extend(expected.iter().cloned());
            let result = context.sql(&SqlBatch { statements: vec![
                SqlStatement { sql: "INSERT INTO git_packs (digest, format, bytes, index_digest, index_bytes) VALUES (?, ?, ?, ?, ?) ON CONFLICT DO NOTHING".into(), parameters },
                SqlStatement { sql: "SELECT format, bytes, index_digest, index_bytes FROM git_packs WHERE digest = ?".into(), parameters: vec![SqlValue::Blob(digest.to_vec())] },
            ]})?;
            if result.get(1).and_then(|set| set.rows.first()) != Some(&expected) {
                return Ok(CommandResult::Rejected(()));
            }
        }
        let mut parameters = Vec::with_capacity(records.len() * 9);
        for (oid, (expected, pack)) in &records {
            parameters.push(SqlValue::Blob(oid.to_vec()));
            parameters.extend(expected.iter().cloned());
            parameters.push(SqlValue::Blob(pack.to_vec()));
        }
        let result = context.sql(&SqlBatch { statements: vec![
            SqlStatement { sql: format!("INSERT INTO objects (oid, kind, size, digest, graph_valid, graph_count, graph_digest, rebase_metadata, pack) VALUES {} ON CONFLICT(oid) DO NOTHING", vec!["(?, ?, ?, ?, ?, ?, ?, ?, ?)"; records.len()].join(",")), parameters },
            SqlStatement { sql: format!("SELECT oid, kind, size, digest, graph_valid, graph_count, graph_digest, rebase_metadata FROM objects WHERE oid IN ({}) ORDER BY oid", vec!["?"; records.len()].join(",")), parameters: records.keys().map(|oid| SqlValue::Blob(oid.to_vec())).collect() },
        ]})?;
        let rows = &result
            .get(1)
            .ok_or(Error::Command("missing object collision check"))?
            .rows;
        if rows.len() != records.len() {
            return Ok(CommandResult::Rejected(()));
        }
        for (row, (oid, (expected, _))) in rows.iter().zip(&records) {
            let Some((SqlValue::Blob(stored), fields)) = row.split_first() else {
                return Err(Error::Command("invalid object collision check"));
            };
            if stored.as_slice() != oid.as_ref() || fields != expected.as_slice() {
                return Ok(CommandResult::Rejected(()));
            }
        }
        // Verified blobs have no dependencies. Publish their closure proofs
        // with the metadata batch rather than scheduling per-blob graph work.
        let blobs: Vec<_> = records
            .iter()
            .filter(|(_, (expected, _))| expected[0] == SqlValue::Text("blob".into()))
            .map(|(oid, _)| *oid)
            .collect();
        if !blobs.is_empty() {
            context.sql(&SqlBatch {
                statements: vec![SqlStatement {
                    sql: format!(
                        "INSERT INTO object_closure (oid) VALUES {} ON CONFLICT DO NOTHING",
                        vec!["(?)"; blobs.len()].join(",")
                    ),
                    parameters: blobs
                        .iter()
                        .map(|oid| SqlValue::Blob(oid.to_vec()))
                        .collect(),
                }],
            })?;
        }
        Ok(CommandResult::Success(()))
    }
}

impl RepositoryCell {
    pub async fn put_objects(
        &self,
        identity: MutationIdentity,
        mut batch: ObjectBatch,
    ) -> Result<Committed<()>, InvocationError<()>> {
        let input: Vec<_> = batch
            .objects
            .iter()
            .filter_map(|object| match &object.storage {
                ObjectStorage::Input(body)
                    if object.oid.format() == self.object_format
                        && crate::object_id(self.object_format, object.kind, body)
                            == object.oid =>
                {
                    Some((object.kind, body.clone()))
                }
                _ => None,
            })
            .collect();
        if !input.is_empty() {
            let source = self
                .packs
                .prepare(self.object_format, input)
                .await
                .map_err(crate::object_packs::pack_invocation)?;
            let pack = self
                .packs
                .publish(source.clone())
                .await
                .map_err(crate::object_packs::pack_invocation)?;
            for object in &mut batch.objects {
                if let ObjectStorage::Input(body) = &object.storage
                    && object.oid.format() == self.object_format
                    && crate::object_id(self.object_format, object.kind, body) == object.oid
                {
                    object.storage = self
                        .record_from_pack(source.clone(), pack.clone(), object.oid)
                        .await
                        .map_err(crate::object_packs::pack_invocation)?
                        .storage;
                }
            }
        }
        self.stage_dependencies(identity, &mut batch.objects)
            .await
            .map_err(|error| {
                InvocationError::NotStarted(Error::Facility {
                    name: "pack dependency staging",
                    source: Box::new(error),
                })
            })?;
        self.application
            .command::<PutObjects>(&self.target, identity, batch)
            .await
    }

    pub async fn existing_objects(
        &self,
        candidates: &[crate::ObjectId],
    ) -> Result<Observed<BTreeSet<crate::ObjectId>>, InvocationError<Vec<SqlResultSet>>> {
        if !(1..=MAX_OBJECTS).contains(&candidates.len()) {
            return Err(InvocationError::NotStarted(Error::Command(
                "object lookup count outside bounds",
            )));
        }
        let result = self
            .sql
            .query(
                None,
                SqlBatch {
                    statements: vec![SqlStatement {
                        sql: format!(
                            "SELECT oid FROM objects WHERE oid IN ({})",
                            vec!["?"; candidates.len()].join(",")
                        ),
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
            .ok_or_else(|| InvocationError::NotStarted(Error::Command("missing object lookup")))?
            .rows;
        let mut objects = BTreeSet::new();
        for row in rows {
            let [SqlValue::Blob(oid)] = row.as_slice() else {
                return Err(InvocationError::NotStarted(Error::Command(
                    "invalid object lookup",
                )));
            };
            objects.insert(
                oid.as_slice().try_into().map_err(|_| {
                    InvocationError::NotStarted(Error::Command("invalid stored OID"))
                })?,
            );
        }
        Ok(Observed {
            output: objects,
            receipt: result.receipt,
        })
    }
}

#[cfg(test)]
mod tests;
