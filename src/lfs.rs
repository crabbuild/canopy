//! Git LFS basic transfers with SQLite metadata and external immutable bytes.

use std::{
    error::Error as StdError,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use crate::AdmissionPermit;
use axum::body::Body;
use cellule_runtime::{
    Error, InvocationError, MutationIdentity, Observed, identity::RequestId,
    primitives::sql::SqlBatch, primitives::sql::SqlResultSet, primitives::sql::SqlStatement,
    primitives::sql::SqlValue,
};
use object_store::{ObjectStore, path::Path};

use crate::{
    RepositoryCell,
    access::{access_statement, decode_access},
    directory::{TokenScope, validate_component},
};

pub(crate) mod locks;
mod read;
#[cfg(test)]
mod tests;
mod upload;
pub use read::LfsRead;
pub(crate) use read::verify_lfs_object;

const CHUNK_BYTES: usize = crate::external::PART_BYTES;
const IO_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(120);

/// Verified external LFS object described by its repository Cell.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LfsObject {
    pub sha256: [u8; 32],
    pub size: u64,
    pub parts_digest: [u8; 32],
}

#[derive(Debug, thiserror::Error)]
pub enum LfsError {
    #[error("LFS Cell operation failed")]
    Cell(#[source] Box<dyn StdError + Send + Sync>),
    #[error("LFS object store failed")]
    Store(#[from] object_store::Error),
    #[error("LFS request body failed")]
    Body(#[from] axum::Error),
    #[error("LFS transfer timed out")]
    Timeout,
    #[error("LFS transfer task failed")]
    Task(#[from] tokio::task::JoinError),
    #[error("LFS object is not recorded")]
    NotFound,
    #[error("LFS write access denied")]
    Forbidden,
    #[error("invalid LFS lock request: {0}")]
    InvalidLock(&'static str),
    #[error("LFS object size overflows its storage representation")]
    TooLarge,
    #[error("LFS object identity or stored bytes are corrupt")]
    Corrupt,
    #[error("system clock cannot create a mutation identity")]
    Clock,
}

/// Repository-scoped LFS transfer service.
pub struct LfsService {
    repository: Arc<RepositoryCell>,
    store: Arc<dyn ObjectStore>,
}

impl LfsService {
    pub fn new(repository: Arc<RepositoryCell>, store: Arc<dyn ObjectStore>) -> Self {
        Self { repository, store }
    }

    pub async fn lookup(&self, oid: [u8; 32]) -> Result<Option<LfsObject>, LfsError> {
        Ok(self
            .repository
            .lfs_object(oid)
            .await
            .map_err(|error| LfsError::Cell(Box::new(error)))?
            .output)
    }

    /// Streams verified immutable bytes before publishing their SQLite reference.
    /// The supervised transfer retains admission through cleanup and publication.
    pub async fn put(
        &self,
        actor: &str,
        oid: [u8; 32],
        body: Body,
        admission: Option<Arc<AdmissionPermit>>,
    ) -> Result<LfsObject, LfsError> {
        let repository = self.repository.clone();
        let store = self.store.clone();
        let actor = actor.to_owned();
        tokio::spawn(async move {
            let object = upload::receive(
                store,
                repository.repository_id(),
                oid,
                body,
                admission.clone(),
            )
            .await?;
            let committed = repository
                .record_lfs_object(mutation_identity()?, &actor, object)
                .await
                .map_err(|error| LfsError::Cell(Box::new(error)))?;
            if !committed.output {
                return Err(LfsError::Forbidden);
            }
            Ok(object)
        })
        .await?
    }

    /// Opens a bounded stream that verifies identity before yielding its last bytes.
    pub async fn get(
        &self,
        oid: [u8; 32],
        admission: Option<Arc<AdmissionPermit>>,
    ) -> Result<LfsRead, LfsError> {
        let object = self.lookup(oid).await?.ok_or(LfsError::NotFound)?;
        LfsRead::open(
            self.store.clone(),
            self.repository.repository_id(),
            object,
            admission,
        )
        .await
    }
}

impl RepositoryCell {
    /// Reads one published LFS object reference.
    pub async fn lfs_object(
        &self,
        oid: [u8; 32],
    ) -> Result<Observed<Option<LfsObject>>, InvocationError<Vec<SqlResultSet>>> {
        let result = self
            .sql
            .query(
                None,
                SqlBatch {
                    statements: vec![SqlStatement {
                        sql: "SELECT size, digest FROM lfs_objects WHERE sha256 = ?1".into(),
                        parameters: vec![SqlValue::Blob(oid.to_vec())],
                    }],
                },
            )
            .await?;
        let object = result
            .output
            .first()
            .and_then(|set| set.rows.first())
            .map(|row| {
                let [SqlValue::Integer(size), SqlValue::Blob(parts_digest)] = row.as_slice() else {
                    return Err(Error::Command("invalid LFS object row"));
                };
                let parts_digest = parts_digest
                    .as_slice()
                    .try_into()
                    .map_err(|_| Error::Command("invalid LFS part digest"))?;
                let size =
                    u64::try_from(*size).map_err(|_| Error::Command("invalid LFS object size"))?;
                Ok(LfsObject {
                    sha256: oid,
                    size,
                    parts_digest,
                })
            })
            .transpose()
            .map_err(InvocationError::NotStarted)?;
        Ok(Observed {
            output: object,
            receipt: result.receipt,
        })
    }

    /// Publishes metadata for an already durable LFS body, returning false without write access.
    pub async fn record_lfs_object(
        &self,
        identity: MutationIdentity,
        actor: &str,
        object: LfsObject,
    ) -> Result<cellule_runtime::Committed<bool>, InvocationError<Vec<SqlResultSet>>> {
        validate_component(actor).map_err(InvocationError::NotStarted)?;
        let size = i64::try_from(object.size).map_err(|_| {
            InvocationError::NotStarted(Error::Command("LFS object size overflows SQLite"))
        })?;
        let committed = self
            .sql
            .batch(
                identity,
                SqlBatch {
                    statements: vec![SqlStatement {
                        sql: "INSERT INTO lfs_objects (sha256, size, digest) SELECT ?1, ?2, ?3 WHERE EXISTS (SELECT 1 FROM repository_identity WHERE owner = ?4) OR EXISTS (SELECT 1 FROM repository_members WHERE account = ?4 AND role = 'write') ON CONFLICT(sha256) DO NOTHING".into(),
                        parameters: vec![
                            SqlValue::Blob(object.sha256.to_vec()),
                            SqlValue::Integer(size),
                            SqlValue::Blob(object.parts_digest.to_vec()),
                            SqlValue::Text(actor.into()),
                        ],
                    }, access_statement(actor)],
                },
            )
            .await?;
        let access =
            committed
                .output
                .get(1..)
                .ok_or_else(|| InvocationError::InvalidPublishedResult {
                    receipt: committed.receipt,
                    source: Box::new(Error::Command("LFS access result missing")),
                })?;
        let authorized = decode_access(access)
            .map_err(|error| InvocationError::InvalidPublishedResult {
                receipt: committed.receipt,
                source: Box::new(error),
            })?
            .is_some_and(|role| role >= TokenScope::Write);
        if !authorized {
            return Ok(cellule_runtime::Committed {
                output: false,
                receipt: committed.receipt,
            });
        }
        let expected = [
            SqlValue::Integer(size),
            SqlValue::Blob(object.parts_digest.to_vec()),
        ];
        if self
            .sql
            .query(
                Some(committed.receipt),
                SqlBatch {
                    statements: vec![SqlStatement {
                        sql: "SELECT size, digest FROM lfs_objects WHERE sha256 = ?1".into(),
                        parameters: vec![SqlValue::Blob(object.sha256.to_vec())],
                    }],
                },
            )
            .await?
            .output
            .first()
            .and_then(|set| set.rows.first())
            .map(Vec::as_slice)
            != Some(expected.as_slice())
        {
            return Err(InvocationError::InvalidPublishedResult {
                receipt: committed.receipt,
                source: Box::new(Error::Command("conflicting LFS object identity")),
            });
        }
        Ok(cellule_runtime::Committed {
            output: true,
            receipt: committed.receipt,
        })
    }
}

fn mutation_identity() -> Result<MutationIdentity, LfsError> {
    let now_ms = i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| LfsError::Clock)?
            .as_millis(),
    )
    .map_err(|_| LfsError::Clock)?;
    Ok(MutationIdentity {
        request_id: RequestId::from_bytes(uuid::Uuid::new_v4().into_bytes()),
        issued_at_ms: now_ms,
        expires_at_ms: now_ms.checked_add(60_000).ok_or(LfsError::Clock)?,
    })
}

pub(crate) fn lfs_path(repository_id: [u8; 16], sha256: &[u8; 32]) -> Path {
    Path::from(format!(
        "repos/{}/lfs/{}",
        hex::encode(repository_id),
        hex::encode(sha256)
    ))
}
