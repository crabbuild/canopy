//! Supervised native pack creation with disk admission before every write.

use super::{OwnedPack, PackError, PackIdentity, PackStore};
use crate::{ObjectId, git_cache::GitCache};
use std::{io::Read, path::Path, process::Stdio, sync::Arc};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const COPY_BYTES: usize = 1024 * 1024;

impl PackStore {
    /// Creates a verified native pair from bounded in-memory ingress. Bodies
    /// are transient: callers publish the pair before recording its metadata.
    pub async fn prepare(
        &self,
        format: crate::ObjectFormat,
        objects: Vec<(crate::ObjectKind, Vec<u8>)>,
    ) -> Result<Arc<OwnedPack>, PackError> {
        if objects.is_empty() || objects.len() > 512 {
            return Err(PackError::Limit);
        }
        let bytes = objects
            .iter()
            .try_fold(0usize, |sum, (_, body)| sum.checked_add(body.len()))
            .ok_or(PackError::Limit)?;
        if bytes > 64 * 1024 * 1024 {
            return Err(PackError::Limit);
        }
        let store = self.clone();
        tokio::spawn(async move {
            let cache = GitCache::create(
                store.local_root().into(),
                store.disk_budget().clone(),
                "refs/heads/main",
                format,
            )
            .await
            .map_err(|error| match error {
                crate::git_cache::CacheError::Io(error) => PackError::Io(error),
                crate::git_cache::CacheError::Budget(error) => PackError::Budget(error),
                error => PackError::Decode(Box::new(error)),
            })?;
            let mut included = Vec::with_capacity(objects.len());
            for (kind, body) in objects {
                let oid = crate::object_id(format, kind, &body);
                cache
                    .store_object(oid, kind, body)
                    .await
                    .map_err(|error| PackError::Decode(Box::new(error)))?;
                included.push(oid);
            }
            included.sort_unstable();
            included.dedup();
            store.build(cache, included, Vec::new(), false).await
        })
        .await?
    }

    /// Produces a self-contained pack containing the selected new history.
    /// Native Git may reuse in-pack deltas but is never asked for a thin pack.
    /// The supervised task retains the cache, native fence, files and admission
    /// if the request which dispatched it is cancelled.
    pub(crate) async fn build(
        &self,
        cache: Arc<GitCache>,
        included: Vec<ObjectId>,
        excluded: Vec<ObjectId>,
        walk: bool,
    ) -> Result<Arc<OwnedPack>, PackError> {
        let store = self.clone();
        tokio::spawn(async move { store.build_inner(cache, included, excluded, walk).await })
            .await?
    }

    async fn build_inner(
        &self,
        cache: Arc<GitCache>,
        included: Vec<ObjectId>,
        excluded: Vec<ObjectId>,
        walk: bool,
    ) -> Result<Arc<OwnedPack>, PackError> {
        if included.is_empty()
            || included
                .iter()
                .chain(&excluded)
                .any(|oid| oid.format() != cache.object_format())
            || (!walk && !excluded.is_empty())
        {
            return Err(PackError::Integrity);
        }
        let directory = tempfile::Builder::new()
            .prefix("canopy-pack-build-")
            .tempdir_in(cache.git_dir())?;
        let owned = OwnedPack::pending(directory, self.disk_budget().try_reserve(0)?)
            .with_native_owner(Arc::clone(&cache));
        let path = owned.directory()?.join("objects.pack");
        let mut output = tokio::fs::File::create(&path).await?;
        let mut command = crate::native_git::command(&cache.git_dir())?;
        command.args([
            "-c",
            "core.bigFileThreshold=8m",
            "pack-objects",
            "--stdout",
            "--delta-base-offset",
            "--no-reuse-delta",
        ]);
        if walk {
            command.arg("--revs");
        }
        let mut child = command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()?;
        let mut input = child.stdin.take().ok_or(PackError::Integrity)?;
        let mut stream = child.stdout.take().ok_or(PackError::Integrity)?;
        let writer = tokio::spawn(async move {
            for (prefix, roots) in [("", included), ("^", excluded)] {
                for oid in roots {
                    input
                        .write_all(format!("{prefix}{}\n", hex::encode(oid)).as_bytes())
                        .await?;
                }
            }
            input.shutdown().await
        });
        let transfer = async {
            let mut buffer = vec![0; COPY_BYTES];
            loop {
                let count = stream.read(&mut buffer).await?;
                if count == 0 {
                    break;
                }
                owned.reservation()?.try_grow(count as u64)?;
                output.write_all(&buffer[..count]).await?;
            }
            output.sync_all().await?;
            Ok::<_, PackError>(())
        }
        .await;
        if transfer.is_err() {
            let _ = child.start_kill();
        }
        // Join both pipes and the worker before returning even after an I/O or
        // admission error. The owner also checks the descendant fence on drop.
        let status = child.wait().await?;
        let supplied = writer.await?;
        transfer?;
        supplied?;
        if !status.success() {
            return Err(PackError::Integrity);
        }
        drop(output);
        // Native index-pack writes its temporary index itself. Reserve its
        // worst-case v2 size first (including all large-offset entries).
        let mut header = [0; 12];
        tokio::fs::File::open(&path)
            .await?
            .read_exact(&mut header)
            .await?;
        if &header[..4] != b"PACK"
            || u32::from_be_bytes(header[4..8].try_into().map_err(|_| PackError::Integrity)?) != 2
        {
            return Err(PackError::Integrity);
        }
        let objects = u64::from(u32::from_be_bytes(
            header[8..12].try_into().map_err(|_| PackError::Integrity)?,
        ));
        let index_bound = objects
            .checked_mul(cache.object_format().bytes() as u64 + 16)
            .and_then(|bytes| bytes.checked_add(4096))
            .ok_or(PackError::Limit)?;
        if index_bound > self.index_limit() as u64 {
            return Err(PackError::Limit);
        }
        owned.reservation()?.try_grow(index_bound)?;
        let index_path = path.with_extension("idx");
        let status =
            crate::native_git::command_with_extra_fence(owned.directory()?, &cache.git_dir())?
                .arg("--git-dir")
                .arg(cache.git_dir())
                .args(["index-pack", "--index-version=2", "-o"])
                .arg(&index_path)
                .arg(&path)
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .kill_on_drop(true)
                .status()
                .await?;
        if !status.success() {
            return Err(PackError::Integrity);
        }
        let format = cache.object_format();
        let index_limit = self.index_limit();
        let decode_limit = self.decode_limit();
        tokio::task::spawn_blocking(move || {
            let (pack_bytes, pack_digest) = file_identity(&path)?;
            let (index_bytes, index_digest) = file_identity(&index_path)?;
            let identity = PackIdentity {
                format,
                pack_digest,
                pack_bytes,
                index_digest,
                index_bytes,
            };
            owned.reservation()?.resize(
                pack_bytes
                    .checked_add(index_bytes)
                    .ok_or(PackError::Limit)?,
            )?;
            let mut owned = owned;
            owned.verify(identity, index_limit, decode_limit)?;
            Ok(Arc::new(owned))
        })
        .await?
    }
}

fn file_identity(path: &Path) -> Result<(u64, [u8; 32]), PackError> {
    let mut file = std::fs::File::open(path)?;
    let bytes = file.metadata()?.len();
    let mut digest = blake3::Hasher::new();
    let mut buffer = vec![0; COPY_BYTES];
    let mut read = 0;
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        read += count as u64;
        digest.update(&buffer[..count]);
    }
    if read != bytes {
        return Err(PackError::Integrity);
    }
    Ok((bytes, *digest.finalize().as_bytes()))
}

#[cfg(test)]
mod tests;
