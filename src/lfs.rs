//! Git LFS basic transfers with SQLite metadata and external immutable bytes.

use std::{
    error::Error as StdError,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;
use cellule_runtime::{
    Error, InvocationError, MutationIdentity, Observed, RequestId, SqlBatch, SqlResultSet,
    SqlStatement, SqlValue,
};
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, path::Path};
use sha2::{Digest, Sha256};

use crate::{
    RepositoryCell,
    access::{access_statement, decode_access},
    directory::{TokenScope, validate_component},
};

pub const MAX_LFS_BYTES: usize = 64 * 1024 * 1024;

/// Verified external LFS object described by its repository Cell.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LfsObject {
    pub sha256: [u8; 32],
    pub size: u64,
    pub blake3: [u8; 32],
}

#[derive(Debug, thiserror::Error)]
pub enum LfsError {
    #[error("LFS Cell operation failed")]
    Cell(#[source] Box<dyn StdError + Send + Sync>),
    #[error("LFS object store failed")]
    Store(#[from] object_store::Error),
    #[error("LFS object is not recorded")]
    NotFound,
    #[error("LFS write access denied")]
    Forbidden,
    #[error("LFS object exceeds the configured byte ceiling")]
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

    /// Uploads immutable bytes before publishing their SQLite reference.
    pub async fn put(
        &self,
        actor: &str,
        oid: [u8; 32],
        body: &[u8],
    ) -> Result<LfsObject, LfsError> {
        if body.len() > MAX_LFS_BYTES {
            return Err(LfsError::TooLarge);
        }
        if Sha256::digest(body).as_slice() != oid {
            return Err(LfsError::Corrupt);
        }
        let object = LfsObject {
            sha256: oid,
            size: u64::try_from(body.len()).map_err(|_| LfsError::TooLarge)?,
            blake3: *blake3::hash(body).as_bytes(),
        };
        match self
            .store
            .put_opts(
                &self.path(oid),
                Bytes::copy_from_slice(body).into(),
                PutOptions {
                    mode: PutMode::Create,
                    ..PutOptions::default()
                },
            )
            .await
        {
            Ok(_) => {}
            Err(object_store::Error::AlreadyExists { .. }) => {
                if self.read_bytes(object).await? != body {
                    return Err(LfsError::Corrupt);
                }
            }
            Err(error) => return Err(error.into()),
        }
        let identity = mutation_identity()?;
        let committed = self
            .repository
            .record_lfs_object(identity, actor, object)
            .await
            .map_err(|error| LfsError::Cell(Box::new(error)))?;
        if !committed.output {
            return Err(LfsError::Forbidden);
        }
        Ok(object)
    }

    pub async fn get(&self, oid: [u8; 32]) -> Result<Vec<u8>, LfsError> {
        let object = self.lookup(oid).await?.ok_or(LfsError::NotFound)?;
        self.read_bytes(object).await
    }

    async fn read_bytes(&self, object: LfsObject) -> Result<Vec<u8>, LfsError> {
        read_lfs_object(self.store.as_ref(), self.repository.repository_id(), object).await
    }

    fn path(&self, oid: [u8; 32]) -> Path {
        lfs_path(self.repository.repository_id(), &oid)
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
                let [SqlValue::Integer(size), SqlValue::Blob(digest)] = row.as_slice() else {
                    return Err(Error::Command("invalid LFS object row"));
                };
                let blake3 = digest
                    .as_slice()
                    .try_into()
                    .map_err(|_| Error::Command("invalid LFS digest"))?;
                let size =
                    u64::try_from(*size).map_err(|_| Error::Command("invalid LFS object size"))?;
                Ok(LfsObject {
                    sha256: oid,
                    size,
                    blake3,
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
                            SqlValue::Blob(object.blake3.to_vec()),
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
            SqlValue::Blob(object.blake3.to_vec()),
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

pub(crate) async fn read_lfs_object(
    store: &dyn ObjectStore,
    repository_id: [u8; 16],
    object: LfsObject,
) -> Result<Vec<u8>, LfsError> {
    if object.size > MAX_LFS_BYTES as u64 {
        return Err(LfsError::TooLarge);
    }
    let result = store.get(&lfs_path(repository_id, &object.sha256)).await?;
    if result.meta.size != object.size {
        return Err(LfsError::Corrupt);
    }
    let body = result.bytes().await?;
    if u64::try_from(body.len()).ok() != Some(object.size)
        || Sha256::digest(&body).as_slice() != object.sha256
        || blake3::hash(&body).as_bytes() != &object.blake3
    {
        return Err(LfsError::Corrupt);
    }
    Ok(body.to_vec())
}
