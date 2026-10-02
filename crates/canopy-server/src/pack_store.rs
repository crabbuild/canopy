//! Immutable pack artifacts; SQLite retains canonical identities and closure.
use crate::{
    ObjectId, ObjectKind, RepositoryCell,
    blob::{LargeBlobReference, LargeBlobStore},
    git_cache::GitCache,
    git_gateway::GatewayError,
    git_http::GitProcess,
};
use cellule_ltx::DiskBudget;
use cellule_runtime::{
    Error, InvocationError, MutationIdentity,
    primitives::sql::{SqlBatch, SqlResultSet, SqlStatement, SqlValue},
};
use object_store::ObjectStore;
use std::{collections::BTreeMap, path::PathBuf, sync::Arc};
use tokio::sync::Mutex;

#[derive(Clone)]
pub(crate) struct PackRecord {
    pub hash: ObjectId,
    pub pack: LargeBlobReference,
    pub index: LargeBlobReference,
    pub approved: bool,
    pub covered_through: i64,
}

pub(crate) struct PackReader {
    store: LargeBlobStore,
    root: PathBuf,
    budget: DiskBudget,
    format: crate::ObjectFormat,
    cache: Mutex<Option<Arc<GitCache>>>,
    installation: Mutex<()>,
    private: Mutex<Option<([u8; 32], Arc<GitCache>)>>,
}
impl PackReader {
    pub(crate) fn new(
        store: Arc<dyn ObjectStore>,
        repository: [u8; 16],
        root: PathBuf,
        budget: DiskBudget,
        format: crate::ObjectFormat,
    ) -> Self {
        Self {
            store: LargeBlobStore::new(store, repository),
            root,
            budget,
            format,
            cache: Mutex::new(None),
            installation: Mutex::new(()),
            private: Mutex::new(None),
        }
    }
    pub(crate) async fn cache(&self) -> Result<Arc<GitCache>, GatewayError> {
        let mut cache = self.cache.lock().await;
        if cache.is_none() {
            *cache = Some(
                GitCache::create(
                    self.root.clone(),
                    self.budget.clone(),
                    "refs/heads/main",
                    self.format,
                )
                .await?,
            );
        }
        Ok(Arc::clone(
            cache.as_ref().ok_or(GatewayError::MalformedCache)?,
        ))
    }
    pub(crate) async fn replace(&self, old: &Arc<GitCache>, next: Arc<GitCache>) {
        let mut cache = self.cache.lock().await;
        if cache.as_ref().is_some_and(|cache| Arc::ptr_eq(cache, old)) {
            *cache = Some(next);
        }
    }
    pub(crate) async fn install(
        &self,
        cache: &Arc<GitCache>,
        record: &PackRecord,
    ) -> Result<(), GatewayError> {
        let _installation = self.installation.lock().await;
        if cache.has_durable_pack(record.pack.sha256) {
            return Ok(());
        }
        cache
            .install_pack(
                record.hash,
                self.store.read(&record.pack).await?,
                self.store.read(&record.index).await?,
            )
            .await?;
        if record.approved {
            cache.mark_durable_pack(record.pack.sha256);
        }
        Ok(())
    }
    pub(crate) async fn native_reader(
        &self,
        record: PackRecord,
        oid: ObjectId,
        size: u64,
        digest: [u8; 32],
    ) -> Result<NativePackedRead, GatewayError> {
        // Incomplete archives stay in one bounded private cache. Their foreign
        // members cannot enter a transport cache; each extracted body is checked.
        let cache = if record.approved {
            self.cache().await?
        } else {
            let mut private = self.private.lock().await;
            if private
                .as_ref()
                .is_none_or(|(sha, _)| *sha != record.pack.sha256)
            {
                *private = Some((
                    record.pack.sha256,
                    GitCache::create(
                        self.root.clone(),
                        self.budget.clone(),
                        "refs/heads/main",
                        self.format,
                    )
                    .await?,
                ));
            }
            Arc::clone(&private.as_ref().ok_or(GatewayError::MalformedCache)?.1)
        };
        self.install(&cache, &record).await?;
        if !record.approved {
            cache.mark_durable_pack(record.pack.sha256);
        }
        let mut command = crate::native_git::command(&cache.git_dir())?;
        command
            .args(["cat-file", "blob", &hex::encode(oid)])
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null());
        let mut process = GitProcess::spawn(command, Arc::clone(&cache))?;
        let output = process
            .child
            .stdout
            .take()
            .ok_or(GatewayError::MalformedCache)?;
        Ok(NativePackedRead {
            process,
            output,
            oid,
            size,
            remaining: size,
            expected: digest,
            canonical: Some(crate::git_format::ObjectHasher::new(
                oid.format(),
                ObjectKind::Blob,
                size,
            )),
            digest: blake3::Hasher::new(),
            finished: false,
        })
    }
    pub(crate) async fn read_blob(
        &self,
        record: PackRecord,
        oid: ObjectId,
        size: u64,
        digest: [u8; 32],
    ) -> Result<Vec<u8>, GatewayError> {
        if size > crate::INLINE_OBJECT_LIMIT as u64 {
            return Err(GatewayError::MalformedCache);
        }
        let mut reader = self.native_reader(record, oid, size, digest).await?;
        let mut body = Vec::with_capacity(size as usize);
        while let Some(bytes) = reader.next().await? {
            body.extend_from_slice(&bytes);
        }
        Ok(body)
    }
    pub(crate) async fn upload(&self, path: PathBuf) -> Result<LargeBlobReference, GatewayError> {
        let hash_path = path.clone();
        let (oid, size) = tokio::task::spawn_blocking(move || {
            use std::io::Read;
            let mut file = std::fs::File::open(hash_path)?;
            let size = file.metadata()?.len();
            let mut hash = crate::git_format::ObjectHasher::new(
                crate::ObjectFormat::Sha256,
                ObjectKind::Blob,
                size,
            );
            let mut buffer = vec![0; 8 << 20];
            loop {
                let count = file.read(&mut buffer)?;
                if count == 0 {
                    break;
                }
                hash.update(&buffer[..count]);
            }
            Ok::<_, std::io::Error>((hash.finalize(), size))
        })
        .await??;
        Ok(self
            .store
            .put(oid, size, &mut tokio::fs::File::open(path).await?)
            .await?)
    }
}

/// Constant-memory extraction used only when an incomplete pack cannot be
/// admitted as a whole. Verification precedes the final returned body range.
pub(crate) struct NativePackedRead {
    process: GitProcess<Arc<GitCache>>,
    output: tokio::process::ChildStdout,
    pub(crate) oid: ObjectId,
    pub(crate) size: u64,
    remaining: u64,
    expected: [u8; 32],
    canonical: Option<crate::git_format::ObjectHasher>,
    digest: blake3::Hasher,
    finished: bool,
}
impl NativePackedRead {
    pub(crate) async fn next(&mut self) -> Result<Option<bytes::Bytes>, GatewayError> {
        use tokio::io::AsyncReadExt;
        if self.finished {
            return Ok(None);
        }
        let mut bytes = vec![0; self.remaining.min(8 << 20) as usize];
        tokio::time::timeout(
            std::time::Duration::from_secs(120),
            self.output.read_exact(&mut bytes),
        )
        .await
        .map_err(|_| crate::git_http::GitHttpError::Timeout)??;
        self.remaining -= bytes.len() as u64;
        self.canonical
            .as_mut()
            .ok_or(GatewayError::MalformedCache)?
            .update(&bytes);
        self.digest.update(&bytes);
        if self.remaining == 0 {
            let mut trailing = [0];
            let eof = tokio::time::timeout(
                std::time::Duration::from_secs(120),
                self.output.read(&mut trailing),
            )
            .await
            .map_err(|_| crate::git_http::GitHttpError::Timeout)??;
            if eof != 0
                || self
                    .canonical
                    .take()
                    .ok_or(GatewayError::MalformedCache)?
                    .finalize()
                    != self.oid
                || self.digest.finalize().as_bytes() != &self.expected
            {
                return Err(GatewayError::MalformedCache);
            }
            let status = tokio::time::timeout(
                std::time::Duration::from_secs(120),
                self.process.child.wait(),
            )
            .await
            .map_err(|_| crate::git_http::GitHttpError::Timeout)??;
            self.process.disarm();
            if !status.success() {
                return Err(GatewayError::MalformedCache);
            }
            self.finished = true;
        }
        Ok(Some(bytes.into()))
    }
}

const COLUMNS: &str = "sha256, pack_hash, pack_oid, pack_size, pack_digest, index_oid, index_size, index_digest, index_sha256, approved, covered_through";
type ReadError = InvocationError<Vec<SqlResultSet>>;
fn invalid() -> ReadError {
    InvocationError::NotStarted(Error::Command("invalid durable pack record"))
}
fn decode(row: &[SqlValue]) -> Result<PackRecord, ReadError> {
    let [
        SqlValue::Blob(sha),
        SqlValue::Blob(hash),
        SqlValue::Blob(pack_oid),
        SqlValue::Integer(pack_size),
        SqlValue::Blob(pack_digest),
        SqlValue::Blob(index_oid),
        SqlValue::Integer(index_size),
        SqlValue::Blob(index_digest),
        SqlValue::Blob(index_sha),
        SqlValue::Integer(approved),
        SqlValue::Integer(covered_through),
    ] = row
    else {
        return Err(invalid());
    };
    Ok(PackRecord {
        hash: hash.as_slice().try_into().map_err(|_| invalid())?,
        pack: LargeBlobReference {
            oid: pack_oid.as_slice().try_into().map_err(|_| invalid())?,
            size: (*pack_size).try_into().map_err(|_| invalid())?,
            blake3: pack_digest.as_slice().try_into().map_err(|_| invalid())?,
            sha256: sha.as_slice().try_into().map_err(|_| invalid())?,
        },
        index: LargeBlobReference {
            oid: index_oid.as_slice().try_into().map_err(|_| invalid())?,
            size: (*index_size).try_into().map_err(|_| invalid())?,
            blake3: index_digest.as_slice().try_into().map_err(|_| invalid())?,
            sha256: index_sha.as_slice().try_into().map_err(|_| invalid())?,
        },
        approved: *approved == 1,
        covered_through: *covered_through,
    })
}
impl RepositoryCell {
    pub(crate) async fn pack_record(&self, sha: [u8; 32]) -> Result<PackRecord, ReadError> {
        let result = self
            .sql
            .query(
                None,
                SqlBatch {
                    statements: vec![SqlStatement {
                        sql: format!("SELECT {COLUMNS} FROM git_packs WHERE sha256 = ?1"),
                        parameters: vec![SqlValue::Blob(sha.to_vec())],
                    }],
                },
            )
            .await?;
        decode(
            result
                .output
                .first()
                .and_then(|set| set.rows.first())
                .ok_or_else(invalid)?,
        )
    }
    pub(crate) async fn approved_packs(&self, after: &[u8]) -> Result<Vec<PackRecord>, ReadError> {
        let result = self.sql.query(None, SqlBatch { statements: vec![SqlStatement { sql: format!("SELECT {COLUMNS} FROM git_packs WHERE approved = 1 AND sha256 > ?1 ORDER BY sha256 LIMIT 128"), parameters: vec![SqlValue::Blob(after.to_vec())] }] }).await?;
        result
            .output
            .first()
            .ok_or_else(invalid)?
            .rows
            .iter()
            .map(|row| decode(row))
            .collect()
    }
    pub(crate) async fn register_pack(
        &self,
        identity: MutationIdentity,
        record: &PackRecord,
    ) -> Result<(), ReadError> {
        let values = vec![
            SqlValue::Blob(record.pack.sha256.to_vec()),
            SqlValue::Blob(record.hash.to_vec()),
            SqlValue::Blob(record.pack.oid.to_vec()),
            SqlValue::Integer(record.pack.size.try_into().map_err(|_| invalid())?),
            SqlValue::Blob(record.pack.blake3.to_vec()),
            SqlValue::Blob(record.index.oid.to_vec()),
            SqlValue::Integer(record.index.size.try_into().map_err(|_| invalid())?),
            SqlValue::Blob(record.index.blake3.to_vec()),
            SqlValue::Blob(record.index.sha256.to_vec()),
        ];
        let result = self.sql.batch(identity, SqlBatch { statements: vec![
            SqlStatement { sql: "INSERT INTO git_packs (sha256, pack_hash, pack_oid, pack_size, pack_digest, index_oid, index_size, index_digest, index_sha256) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9) ON CONFLICT DO NOTHING".into(), parameters: values },
        ] }).await?;
        drop(result);
        let stored = self.pack_record(record.pack.sha256).await?;
        if stored.hash != record.hash
            || stored.pack.oid != record.pack.oid
            || stored.pack.size != record.pack.size
            || stored.pack.blake3 != record.pack.blake3
            || stored.index.oid != record.index.oid
            || stored.index.size != record.index.size
            || stored.index.blake3 != record.index.blake3
            || stored.index.sha256 != record.index.sha256
        {
            return Err(invalid());
        }
        Ok(())
    }
    pub(crate) async fn approve_pack(
        &self,
        identity: MutationIdentity,
        sha: [u8; 32],
        verified_count: usize,
    ) -> Result<(), ReadError> {
        self.sql
            .batch(
                identity,
                SqlBatch {
                    statements: vec![SqlStatement {
                        // Every unique index member has a matching canonical SQL row.
                        // Equal cardinality proves this pack covers the entire immutable
                        // object table at this transaction, without scanning it on recovery.
                        sql: "UPDATE git_packs SET approved = 1, covered_through = CASE WHEN ?2 = (SELECT COUNT(oid) FROM objects) THEN (SELECT COALESCE(MAX(sequence), 0) FROM objects) ELSE covered_through END WHERE sha256 = ?1".into(),
                        parameters: vec![SqlValue::Blob(sha.to_vec()), SqlValue::Integer(verified_count.try_into().map_err(|_| invalid())?)],
                    }],
                },
            )
            .await?;
        Ok(())
    }
    pub(crate) async fn canonical_headers(
        &self,
        ids: &[ObjectId],
    ) -> Result<BTreeMap<ObjectId, (ObjectKind, u64, [u8; 32])>, ReadError> {
        if ids.is_empty() || ids.len() > crate::object_batch::MAX_BATCH_OBJECTS {
            return Err(invalid());
        }
        let placeholders = vec!["?"; ids.len()].join(",");
        let result = self.sql.query(None, SqlBatch { statements: vec![SqlStatement { sql: format!("SELECT oid, kind, size, digest FROM objects WHERE oid IN ({placeholders})"), parameters: ids.iter().map(|oid| SqlValue::Blob(oid.to_vec())).collect() }] }).await?;
        let mut headers = BTreeMap::new();
        for row in &result.output.first().ok_or_else(invalid)?.rows {
            let [
                SqlValue::Blob(oid),
                SqlValue::Text(kind),
                SqlValue::Integer(size),
                SqlValue::Blob(digest),
            ] = row.as_slice()
            else {
                return Err(invalid());
            };
            let kind = match kind.as_str() {
                "blob" => ObjectKind::Blob,
                "tree" => ObjectKind::Tree,
                "commit" => ObjectKind::Commit,
                "tag" => ObjectKind::Tag,
                _ => return Err(invalid()),
            };
            headers.insert(
                oid.as_slice().try_into().map_err(|_| invalid())?,
                (
                    kind,
                    (*size).try_into().map_err(|_| invalid())?,
                    digest.as_slice().try_into().map_err(|_| invalid())?,
                ),
            );
        }
        Ok(headers)
    }
}
