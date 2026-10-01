//! Immutable cache generations: never repack/delete files beneath an active reader.
use super::*;
use crate::git_http::{GitHttpError, GitProcess, WORKER_DEADLINE, read_bounded};
use std::process::Stdio;
use tokio::io::AsyncWriteExt;

fn worker_error(error: GitHttpError) -> CacheError {
    io::Error::other(error).into()
}

// Follow physical pack order to retain delta-base locality while verifying large
// histories. Hash order forces needless repeated decompression of distant bases.
fn index_order(path: &Path, format: crate::ObjectFormat) -> io::Result<Vec<crate::ObjectId>> {
    let data = fs::read(path)?;
    let validated = crate::git_format::pack_index::PackIndex::open(path, format)?;
    let count = validated.len() as usize;
    let width = format.bytes();
    let offsets = 1032_usize
        .checked_add(
            count
                .checked_mul(width + 4)
                .ok_or_else(|| io::Error::other("pack index overflow"))?,
        )
        .ok_or_else(|| io::Error::other("pack index overflow"))?;
    let large = offsets
        .checked_add(
            count
                .checked_mul(4)
                .ok_or_else(|| io::Error::other("pack index overflow"))?,
        )
        .ok_or_else(|| io::Error::other("pack index overflow"))?;
    let payload_end = data
        .len()
        .checked_sub(2 * width)
        .ok_or_else(|| io::Error::other("truncated index"))?;
    if large > payload_end {
        return Err(io::Error::other("truncated offsets"));
    }
    let mut ordered = Vec::with_capacity(count);
    for n in 0..count {
        let at = offsets + n * 4;
        let raw = u32::from_be_bytes(data[at..at + 4].try_into().unwrap());
        let offset = if raw & 0x8000_0000 == 0 {
            u64::from(raw)
        } else {
            let at = large
                .checked_add(
                    ((raw & 0x7fff_ffff) as usize)
                        .checked_mul(8)
                        .ok_or_else(|| io::Error::other("offset overflow"))?,
                )
                .ok_or_else(|| io::Error::other("offset overflow"))?;
            let end = at
                .checked_add(8)
                .filter(|end| *end <= payload_end)
                .ok_or_else(|| io::Error::other("truncated large offset"))?;
            u64::from_be_bytes(data[at..end].try_into().unwrap())
        };
        let oid = data[1032 + n * width..1032 + (n + 1) * width]
            .try_into()
            .map_err(|_| io::Error::other("invalid OID"))?;
        ordered.push((offset, oid));
    }
    ordered.sort_unstable();
    Ok(ordered.into_iter().map(|(_, oid)| oid).collect())
}

impl GitCache {
    pub(crate) fn has_durable_pack(&self, sha: [u8; 32]) -> bool {
        self.durable_packs
            .read()
            .expect("durable inventory poisoned")
            .contains(&sha)
    }
    pub(crate) fn mark_durable_pack(&self, sha: [u8; 32]) {
        self.durable_packs
            .write()
            .expect("durable inventory poisoned")
            .insert(sha);
    }
    pub(crate) async fn pack_sources(
        self: &Arc<Self>,
    ) -> Result<Vec<(crate::ObjectId, PathBuf, PathBuf, Vec<crate::ObjectId>)>, CacheError> {
        let cache = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            let mut sources = Vec::new();
            for entry in fs::read_dir(cache.git_dir().join("objects/pack"))? {
                let index = entry?.path();
                if index.extension().and_then(|v| v.to_str()) != Some("idx") {
                    continue;
                }
                let name = index
                    .file_stem()
                    .and_then(|v| v.to_str())
                    .and_then(|v| v.strip_prefix("pack-"))
                    .ok_or_else(|| io::Error::other("invalid pack filename"))?;
                let hash = crate::ObjectId::from_hex(name)
                    .map_err(|_| io::Error::other("invalid pack hash"))?;
                if hash.format() != cache.object_format {
                    return Err(io::Error::other("invalid pack format").into());
                }
                let ids = index_order(&index, cache.object_format)?;
                sources.push((hash, index.with_extension("pack"), index, ids));
            }
            Ok(sources)
        })
        .await?
    }
    pub(crate) async fn install_pack(
        self: &Arc<Self>,
        hash: crate::ObjectId,
        pack: LargeBlobRead,
        index: LargeBlobRead,
    ) -> Result<(), CacheError> {
        if hash.format() != self.object_format {
            return Err(io::Error::other("pack format mismatch").into());
        }
        self.write_artifact(
            format!("objects/pack/pack-{}.pack", hex::encode(hash)),
            pack,
        )
        .await?;
        self.write_artifact(
            format!("objects/pack/pack-{}.idx", hex::encode(hash)),
            index,
        )
        .await?;
        let cache = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            let path = cache
                .git_dir()
                .join(format!("objects/pack/pack-{}.idx", hex::encode(hash)));
            let index = crate::git_format::pack_index::PackIndex::open(&path, cache.object_format)?;
            if index.pack_checksum() != hash {
                return Err(io::Error::other("index/pack binding mismatch").into());
            }
            cache.register_checked_index(index)?;
            cache
                .pack_files
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            cache
                .write_generation
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        })
        .await?
    }
    async fn write_artifact(
        self: &Arc<Self>,
        relative: String,
        mut reader: LargeBlobRead,
    ) -> Result<(), CacheError> {
        let destination = self.git_dir().join(relative);
        // Retry a canceled pair without downloading/charging a second full pack.
        // File names alone (possibly SHA-1) are insufficient: verify all hashes.
        let expected = reader.reference();
        let existing = destination.clone();
        if tokio::task::spawn_blocking(move || -> io::Result<bool> {
            use sha2::Digest;
            use std::io::Read;
            let mut file = match File::open(existing) {
                Ok(file) => file,
                Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
                Err(error) => return Err(error),
            };
            if file.metadata()?.len() != expected.size {
                return Err(io::Error::other("cached artifact size mismatch"));
            }
            let mut oid = crate::git_format::ObjectHasher::new(
                expected.oid.format(),
                ObjectKind::Blob,
                expected.size,
            );
            let mut sha = sha2::Sha256::new();
            let mut blake = blake3::Hasher::new();
            let mut buffer = vec![0; 8 << 20];
            loop {
                let n = file.read(&mut buffer)?;
                if n == 0 {
                    break;
                }
                oid.update(&buffer[..n]);
                sha.update(&buffer[..n]);
                blake.update(&buffer[..n]);
            }
            if oid.finalize() != expected.oid
                || <[u8; 32]>::from(sha.finalize()) != expected.sha256
                || *blake.finalize().as_bytes() != expected.blake3
            {
                return Err(io::Error::other("cached artifact digest mismatch"));
            }
            Ok(true)
        })
        .await??
        {
            return Ok(());
        }
        let (file, temporary) = tempfile::NamedTempFile::new_in(
            destination
                .parent()
                .ok_or_else(|| io::Error::other("missing pack directory"))?,
        )?
        .into_parts();
        let mut writer = CacheWriter {
            file,
            cache: Arc::clone(self),
        };
        while let Some(bytes) = reader.next().await? {
            writer = tokio::task::spawn_blocking(move || {
                writer.write_all(&bytes)?;
                Ok::<_, io::Error>(writer)
            })
            .await??;
        }
        tokio::task::spawn_blocking(move || {
            writer.flush()?;
            writer.file.sync_all()?;
            drop(writer);
            if destination.exists() {
                // Compare complete bytes, not merely Git's SHA-1 filename.
                use std::io::Read;
                let mut old = File::open(&destination)?;
                let mut new = File::open(&temporary)?;
                let mut a = vec![0; 1 << 20];
                let mut b = vec![0; 1 << 20];
                loop {
                    let n = old.read(&mut a)?;
                    let m = new.read(&mut b)?;
                    if n != m || a[..n] != b[..m] {
                        return Err(io::Error::other(
                            "cached pack differs from authoritative bytes",
                        ));
                    }
                    if n == 0 {
                        break;
                    }
                }
                return Ok(());
            }
            temporary
                .persist_noclobber(destination)
                .map_err(|error| error.error)?;
            Ok(())
        })
        .await??;
        Ok(())
    }
    /// Reuse only packs whose *every* object was verified and durably recorded
    /// during this ingestion. Extra/unverified objects disable this optimization.
    pub(crate) async fn retain_verified_packs(
        self: &Arc<Self>,
        source: Arc<Self>,
        verified: HashSet<crate::ObjectId>,
    ) -> Result<usize, CacheError> {
        let cache = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            let mut retained = 0;
            for entry in fs::read_dir(source.git_dir().join("objects/pack"))? {
                let path = entry?.path();
                if path.extension().and_then(|v| v.to_str()) != Some("idx") {
                    continue;
                }
                let index =
                    crate::git_format::pack_index::PackIndex::open(&path, cache.object_format)?;
                let mut approved = true;
                for oid in index.ids() {
                    if !verified.contains(&oid?) {
                        approved = false;
                        break;
                    }
                }
                if !approved {
                    continue;
                }
                let pack = path.with_extension("pack");
                // Charge before native-cache file copies. Failure keeps the
                // attempted bytes charged until reconciliation or cache drop.
                let bytes = fs::metadata(&path)?.len() + fs::metadata(&pack)?.len();
                cache
                    .reservation()?
                    .try_grow(bytes)
                    .map_err(io::Error::other)?;
                for input in [&pack, &path] {
                    let destination = cache.git_dir().join("objects/pack").join(
                        input
                            .file_name()
                            .ok_or_else(|| io::Error::other("missing pack name"))?,
                    );
                    if !destination.exists() {
                        let temporary =
                            tempfile::NamedTempFile::new_in(destination.parent().unwrap())?;
                        fs::copy(input, temporary.path())?;
                        temporary
                            .persist_noclobber(&destination)
                            .map_err(|error| error.error)?;
                    }
                }
                retained += index.len() as usize;
                cache
                    .write_generation
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                cache
                    .pack_files
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                cache.register_index(
                    &cache.git_dir().join("objects/pack").join(
                        path.file_name()
                            .ok_or_else(|| io::Error::other("missing index filename"))?,
                    ),
                )?;
            }
            // Caller serializes ingestion/hydration while this operation runs.
            cache.reservation()?.resize(tree_bytes(cache.root())?)?;
            Ok(retained)
        })
        .await?
    }

    /// Enumerate a captured cache into a new self-contained pack. Old objects
    /// and packs are untouched; dropping the last old reader reclaims them.
    pub(crate) async fn repacked(
        self: &Arc<Self>,
        root: PathBuf,
        budget: DiskBudget,
    ) -> Result<Arc<Self>, CacheError> {
        let next = Self::create(root, budget, "refs/heads/main", self.object_format).await?;
        // Native writes bypass CacheWriter. Reserve conservative scratch room
        // before starting, then reconcile the completed generation. This is
        // admission, not a hard OS disk quota (the deployment owns that quota).
        let reserve = self
            .bytes()?
            .checked_add(
                self.indexed_entries()
                    .checked_mul(96)
                    .ok_or_else(|| io::Error::other("maintenance index estimate overflow"))?,
            )
            .and_then(|n| n.checked_add(64 << 20))
            .ok_or_else(|| io::Error::other("maintenance disk estimate overflow"))?;
        next.reservation()?
            .try_grow(reserve)
            .map_err(io::Error::other)?;
        let coverage = self.prepared.lock().await.clone();
        let durable = self
            .durable_packs
            .read()
            .map_err(|_| io::Error::other("durable inventory poisoned"))?
            .clone();
        let run = async {
            let mut listing_command = crate::native_git::command(&self.git_dir())?;
            listing_command
                .args([
                    "cat-file",
                    "--batch-all-objects",
                    "--batch-check=%(objectname)",
                ])
                .stdout(Stdio::piped())
                .stderr(Stdio::piped());
            let mut listing = GitProcess::spawn(listing_command, Arc::clone(self))?;
            let mut pack_command = crate::native_git::command(&self.git_dir())?;
            pack_command
                .args([
                    "pack-objects",
                    "--index-version=2",
                    "--delta-base-offset",
                    "--threads=1",
                ])
                .arg(next.git_dir().join("objects/pack/pack"))
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped());
            let mut packing =
                GitProcess::spawn(pack_command, (Arc::clone(self), Arc::clone(&next)))?;
            let mut input = packing
                .child
                .stdin
                .take()
                .ok_or(GitHttpError::Interrupted)?;
            let mut output = listing
                .child
                .stdout
                .take()
                .ok_or(GitHttpError::Interrupted)?;
            let (_, listing_stderr, hash, packing_stderr) = tokio::try_join!(
                async {
                    tokio::io::copy(&mut output, &mut input).await?;
                    input.shutdown().await?;
                    drop(input);
                    Ok::<_, GitHttpError>(())
                },
                read_bounded(
                    listing
                        .child
                        .stderr
                        .take()
                        .ok_or(GitHttpError::Interrupted)?,
                    64 << 10
                ),
                read_bounded(
                    packing
                        .child
                        .stdout
                        .take()
                        .ok_or(GitHttpError::Interrupted)?,
                    128
                ),
                read_bounded(
                    packing
                        .child
                        .stderr
                        .take()
                        .ok_or(GitHttpError::Interrupted)?,
                    64 << 10
                ),
            )?;
            finish(&mut listing, listing_stderr).await?;
            finish(&mut packing, packing_stderr).await?;
            let hash = std::str::from_utf8(&hash)
                .map_err(|_| GitHttpError::MalformedCgi)?
                .trim();
            let oid = crate::ObjectId::from_hex(hash).map_err(|_| GitHttpError::MalformedCgi)?;
            if oid.format() != self.object_format {
                return Err(GitHttpError::MalformedCgi);
            }
            let pack = next
                .git_dir()
                .join(format!("objects/pack/pack-{hash}.pack"));
            let mut command = crate::native_git::command(&next.git_dir())?;
            command
                .args(["index-pack", "--verify"])
                .arg(&pack)
                .stdout(Stdio::piped())
                .stderr(Stdio::piped());
            let mut verify = GitProcess::spawn(command, Arc::clone(&next))?;
            let (_, stderr) = tokio::try_join!(
                read_bounded(
                    verify
                        .child
                        .stdout
                        .take()
                        .ok_or(GitHttpError::Interrupted)?,
                    128
                ),
                read_bounded(
                    verify
                        .child
                        .stderr
                        .take()
                        .ok_or(GitHttpError::Interrupted)?,
                    64 << 10
                )
            )?;
            let status = verify.child.wait().await?;
            verify.disarm();
            if !status.success() {
                return Err(GitHttpError::GitExit {
                    status,
                    stderr: String::from_utf8_lossy(&stderr).into_owned(),
                });
            }
            let next_copy = Arc::clone(&next);
            tokio::task::spawn_blocking(move || {
                next_copy.register_index(&pack.with_extension("idx"))?;
                Ok::<_, io::Error>(())
            })
            .await
            .map_err(io::Error::other)??;
            Ok::<_, GitHttpError>(())
        };
        let result = tokio::time::timeout(WORKER_DEADLINE, run)
            .await
            .map_err(|_| GitHttpError::Timeout)
            .and_then(|v| v);
        next.reconcile().await?;
        result.map_err(worker_error)?;
        next.pack_files
            .store(1, std::sync::atomic::Ordering::Relaxed);
        *next.prepared.lock().await = coverage;
        *next
            .durable_packs
            .write()
            .map_err(|_| io::Error::other("durable inventory poisoned"))? = durable;
        Ok(next)
    }
}

async fn finish<T>(process: &mut GitProcess<T>, stderr: Vec<u8>) -> Result<(), GitHttpError> {
    let status = process.child.wait().await?;
    process.disarm();
    if !status.success() {
        return Err(GitHttpError::GitExit {
            status,
            stderr: String::from_utf8_lossy(&stderr).into_owned(),
        });
    }
    Ok(())
}
