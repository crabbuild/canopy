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
    Error, InvocationError,
    primitives::sql::{SqlBatch, SqlResultSet, SqlStatement, SqlValue},
};
use object_store::ObjectStore;
use std::{path::PathBuf, sync::Arc};
use tokio::sync::Mutex;

#[derive(Clone)]
pub(crate) struct PackRecord {
    pub hash: ObjectId,
    pub pack: LargeBlobReference,
    pub index: LargeBlobReference,
    pub approved: bool,
}

pub(crate) struct PackReader {
    store: LargeBlobStore,
    root: PathBuf,
    budget: DiskBudget,
    format: crate::ObjectFormat,
    native: crate::native_resources::NativeScope,
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
        native: crate::native_resources::NativeScope,
    ) -> Self {
        Self {
            store: LargeBlobStore::new(store, repository),
            root,
            budget,
            format,
            native,
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
                    self.native.clone(),
                )
                .await?,
            );
        }
        Ok(Arc::clone(
            cache.as_ref().ok_or(GatewayError::MalformedCache)?,
        ))
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
                        self.native.clone(),
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
        let mut process = GitProcess::spawn(
            command,
            Arc::clone(&cache),
            cache
                .native
                .try_admit(crate::native_resources::NativeWork::Read)?,
        )?;
        let output = process
            .child
            .stdout
            .take()
            .ok_or(GatewayError::MalformedCache)?;
        Ok(NativePackedRead {
            process,
            output,
            oid,
            #[cfg(test)]
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
    #[cfg(test)]
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
    #[cfg(test)]
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
            let status =
                tokio::time::timeout(std::time::Duration::from_secs(120), self.process.wait())
                    .await
                    .map_err(|_| crate::git_http::GitHttpError::Timeout)??;
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
        SqlValue::Integer(_covered_through),
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
}
