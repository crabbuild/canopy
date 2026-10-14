//! Verified pack metadata and transient graph preparation. No Git bodies enter SQLite.

use crate::{
    ObjectId, ObjectKind, ObjectStorage, RepositoryCell, StoredObject,
    git_pack::{OwnedPack, PackError, PackIdentity},
    graph::Edge,
};
use cellule_runtime::{
    Error, InvocationError, MutationIdentity,
    identity::RequestId,
    primitives::sql::{SqlBatch, SqlResultSet, SqlStatement, SqlValue},
};
use std::sync::Arc;

// SHA-256 rows encode at most 122 bytes each, including SQL placeholders.
// 8,192 rows stay below Cellule SQL's 1 MiB input and 32,766 parameter limits.
const DEPENDENCY_WRITE_ROWS: usize = 8_192;

#[derive(Clone)]
pub struct PackedObject {
    pub(crate) repository: [u8; 16],
    pub(crate) oid: ObjectId,
    pub(crate) kind: ObjectKind,
    pub(crate) pack: PackIdentity,
    pub(crate) size: u64,
    pub(crate) digest: [u8; 32],
    pub(crate) graph_valid: bool,
    pub(crate) graph_count: u64,
    pub(crate) graph_digest: [u8; 32],
    pub(crate) dependencies: Option<Vec<Edge>>,
    pub(crate) rebase_metadata: Option<Vec<u8>>,
}

impl PackedObject {
    pub fn size(&self) -> u64 {
        self.size
    }
    pub fn pack(&self) -> &PackIdentity {
        &self.pack
    }

    pub(crate) fn verified(
        repository: [u8; 16],
        pack: PackIdentity,
        oid: ObjectId,
        kind: ObjectKind,
        size: u64,
        digest: [u8; 32],
        dependencies: Option<Vec<Edge>>,
    ) -> Self {
        let mut hash = blake3::Hasher::new();
        if let Some(edges) = &dependencies {
            for edge in edges {
                hash.update(&dependency_key(*edge));
            }
        }
        Self {
            repository,
            oid,
            kind,
            pack,
            size,
            digest,
            graph_valid: dependencies.is_some(),
            graph_count: dependencies.as_ref().map_or(0, |edges| edges.len() as u64),
            graph_digest: *hash.finalize().as_bytes(),
            dependencies,
            rebase_metadata: None,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ObjectStageError {
    #[error("object pack preparation failed")]
    Pack(#[from] PackError),
    #[error("object metadata staging failed")]
    Cell(#[from] InvocationError<Vec<SqlResultSet>>),
}

pub(crate) fn pack_invocation<T>(error: PackError) -> InvocationError<T> {
    InvocationError::NotStarted(Error::Facility {
        name: "Git native pack",
        source: Box::new(error),
    })
}

pub(crate) fn dependency_key((oid, kind): Edge) -> Vec<u8> {
    let mut key = oid.to_vec();
    key.push(match kind {
        None => 0,
        Some(ObjectKind::Blob) => 1,
        Some(ObjectKind::Tree) => 2,
        Some(ObjectKind::Commit) => 3,
        Some(ObjectKind::Tag) => 4,
    });
    key
}

pub(crate) fn decode_dependency(
    format: crate::ObjectFormat,
    key: &[u8],
) -> cellule_runtime::Result<Edge> {
    if key.len() != format.bytes() + 1 {
        return Err(Error::Command("invalid packed dependency"));
    }
    let oid = key[..format.bytes()]
        .try_into()
        .map_err(|_| Error::Command("invalid packed dependency OID"))?;
    let kind = match key[format.bytes()] {
        0 => None,
        1 => Some(ObjectKind::Blob),
        2 => Some(ObjectKind::Tree),
        3 => Some(ObjectKind::Commit),
        4 => Some(ObjectKind::Tag),
        _ => return Err(Error::Command("invalid packed dependency kind")),
    };
    Ok((oid, kind))
}

impl RepositoryCell {
    /// Creates and publishes a native pack before exposing a prepared reference.
    pub async fn stage_object(
        &self,
        kind: ObjectKind,
        body: &[u8],
    ) -> Result<StoredObject, ObjectStageError> {
        let source = self
            .packs
            .prepare(self.object_format, vec![(kind, body.to_vec())])
            .await?;
        let pack = self.packs.publish(Arc::clone(&source)).await?;
        let oid = crate::object_id(self.object_format, kind, body);
        self.record_from_pack(source, pack, oid)
            .await
            .map_err(Into::into)
    }

    pub(crate) async fn record_from_pack(
        &self,
        source: Arc<OwnedPack>,
        pack: PackIdentity,
        oid: ObjectId,
    ) -> Result<StoredObject, PackError> {
        let repository = self.id;
        tokio::task::spawn_blocking(move || {
            let (kind, body) = source
                .data()?
                .object(oid, 64 * 1024 * 1024)?
                .ok_or(PackError::Integrity)?;
            let mut storage = PackedObject::verified(
                repository,
                pack,
                oid,
                kind,
                body.len() as u64,
                *blake3::hash(&body).as_bytes(),
                crate::graph::edges(oid.format(), kind, &body),
            );
            storage.rebase_metadata = crate::pulls::candidates::rebase_metadata(kind, &body);
            Ok(StoredObject {
                oid,
                kind,
                storage: ObjectStorage::Packed(Box::new(storage)),
            })
        })
        .await?
    }

    pub(crate) async fn stage_dependencies(
        &self,
        identity: MutationIdentity,
        objects: &mut [StoredObject],
    ) -> Result<(), InvocationError<Vec<SqlResultSet>>> {
        let mut parameters = Vec::new();
        let mut page = 0;
        for object in objects {
            let ObjectStorage::Packed(value) = &mut object.storage else {
                continue;
            };
            let Some(edges) = value.dependencies.take() else {
                continue;
            };
            for edge in edges {
                parameters.push(SqlValue::Blob(value.pack.pack_digest.to_vec()));
                parameters.push(SqlValue::Blob(value.oid.to_vec()));
                parameters.push(SqlValue::Blob(dependency_key(edge)));
                if parameters.len() == DEPENDENCY_WRITE_ROWS * 3 {
                    self.stage_dependency_page(identity, page, std::mem::take(&mut parameters))
                        .await?;
                    page += 1;
                }
            }
        }
        if !parameters.is_empty() {
            self.stage_dependency_page(identity, page, parameters)
                .await?;
        }
        Ok(())
    }

    async fn stage_dependency_page(
        &self,
        identity: MutationIdentity,
        page: u64,
        parameters: Vec<SqlValue>,
    ) -> Result<(), InvocationError<Vec<SqlResultSet>>> {
        let mut hash = blake3::Hasher::new();
        hash.update(b"canopy-pack-dependencies-v3");
        hash.update(identity.request_id.as_bytes());
        hash.update(&page.to_le_bytes());
        let mut request = [0; 16];
        request.copy_from_slice(&hash.finalize().as_bytes()[..16]);
        self.sql
            .batch(
                MutationIdentity {
                    request_id: RequestId::from_bytes(request),
                    ..identity
                },
                dependency_batch(parameters),
            )
            .await?;
        Ok(())
    }

    pub(crate) async fn dependencies(
        &self,
        oid: ObjectId,
    ) -> Result<Option<Vec<Edge>>, InvocationError<Vec<SqlResultSet>>> {
        let result = self.sql.query(None, SqlBatch { statements: vec![SqlStatement {
            sql: "SELECT pack, graph_valid, graph_count, graph_digest FROM objects WHERE oid = ?1".into(),
            parameters: vec![SqlValue::Blob(oid.to_vec())],
        }]}).await?;
        let Some(
            [
                SqlValue::Blob(pack),
                SqlValue::Integer(valid),
                SqlValue::Integer(count),
                SqlValue::Blob(digest),
            ],
        ) = result
            .output
            .first()
            .and_then(|set| set.rows.first())
            .map(Vec::as_slice)
        else {
            return Err(InvocationError::NotStarted(Error::Command(
                "invalid packed graph metadata",
            )));
        };
        if *valid != 1 {
            return Ok(None);
        }
        let mut cursor = Vec::new();
        let mut edges = Vec::new();
        let mut hash = blake3::Hasher::new();
        loop {
            let page = self
                .sql
                .query(Some(result.receipt), dependency_query(pack, oid, &cursor))
                .await?;
            let rows = &page
                .output
                .first()
                .ok_or_else(|| {
                    InvocationError::NotStarted(Error::Command("missing packed dependencies"))
                })?
                .rows;
            if rows.is_empty() {
                break;
            }
            for row in rows {
                let [SqlValue::Blob(key)] = row.as_slice() else {
                    return Err(InvocationError::NotStarted(Error::Command(
                        "invalid packed dependency row",
                    )));
                };
                if key <= &cursor || edges.len() as i64 >= *count {
                    return Err(InvocationError::NotStarted(Error::Command(
                        "corrupt packed dependency count",
                    )));
                }
                hash.update(key);
                edges.push(
                    decode_dependency(oid.format(), key).map_err(InvocationError::NotStarted)?,
                );
                cursor.clone_from(key);
            }
        }
        if edges.len() as i64 != *count || hash.finalize().as_bytes().as_slice() != digest {
            return Err(InvocationError::NotStarted(Error::Command(
                "corrupt packed dependencies",
            )));
        }
        Ok(Some(edges))
    }
}

fn dependency_batch(parameters: Vec<SqlValue>) -> SqlBatch {
    SqlBatch {
        statements: vec![SqlStatement {
            sql: format!(
                "INSERT INTO object_dependencies (pack, parent, dependency) VALUES {} ON CONFLICT DO NOTHING",
                vec!["(?, ?, ?)"; parameters.len() / 3].join(",")
            ),
            parameters,
        }],
    }
}

pub(crate) fn dependency_query(pack: &[u8], oid: ObjectId, after: &[u8]) -> SqlBatch {
    SqlBatch { statements: vec![SqlStatement {
        sql: "SELECT dependency FROM object_dependencies WHERE pack = ?1 AND parent = ?2 AND dependency > ?3 ORDER BY dependency LIMIT 512".into(),
        parameters: vec![SqlValue::Blob(pack.to_vec()), SqlValue::Blob(oid.to_vec()), SqlValue::Blob(after.to_vec())],
    }]}
}

#[cfg(test)]
mod tests {
    use super::*;
    use cellule_runtime::codec::{BoundedEncoder, WireValue};

    #[test]
    fn sha256_dependency_write_fits_published_sql_limits() {
        let mut parameters = Vec::new();
        for _ in 0..DEPENDENCY_WRITE_ROWS {
            parameters.extend([
                SqlValue::Blob(vec![1; 32]),
                SqlValue::Blob(vec![2; 32]),
                SqlValue::Blob(vec![3; 33]),
            ]);
        }
        assert!(parameters.len() < 32_766);
        // Exercise the SDK's wire encoder at the actual SQL input limit.
        let mut encoder = BoundedEncoder::new(1 << 20).unwrap();
        dependency_batch(parameters).encode(&mut encoder).unwrap();
    }
}
