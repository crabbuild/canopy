//! Immutable cache generations: never repack/delete files beneath an active reader.
use super::*;
#[cfg(test)]
use crate::git_http::{GitHttpError, GitProcess, WORKER_DEADLINE, read_bounded};
#[cfg(test)]
use std::process::Stdio;
#[cfg(test)]
use tokio::io::AsyncWriteExt;

#[cfg(test)]
fn worker_error(error: GitHttpError) -> CacheError {
    io::Error::other(error).into()
}

// Follow physical pack order to retain delta-base locality while verifying large
// histories. Hash order forces needless repeated decompression of distant bases.
#[cfg(test)]
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
    #[cfg(test)]
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
    #[cfg(test)]
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
    #[cfg(test)]
    pub(crate) async fn repacked(
        self: &Arc<Self>,
        root: PathBuf,
        budget: DiskBudget,
    ) -> Result<Arc<Self>, CacheError> {
        let next = Self::create(
            root,
            budget,
            "refs/heads/main",
            self.object_format,
            self.native.clone(),
        )
        .await?;
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
        let maintenance = self
            .native
            .for_class(crate::native_resources::NativeClass::Maintenance);
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
            let mut listing = GitProcess::spawn(
                listing_command,
                Arc::clone(self),
                maintenance.try_admit(crate::native_resources::NativeWork::Read)?,
            )?;
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
            let mut packing = GitProcess::spawn(
                pack_command,
                (Arc::clone(self), Arc::clone(&next)),
                maintenance.try_admit(crate::native_resources::NativeWork::Pack)?,
            )?;
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
            // Both native workers drained; return their claims before validation.
            drop(listing);
            drop(packing);
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
                .args(["index-pack", "--threads=2", "--verify"])
                .arg(&pack)
                .stdout(Stdio::piped())
                .stderr(Stdio::piped());
            let mut verify = GitProcess::spawn(
                command,
                Arc::clone(&next),
                maintenance.try_admit(crate::native_resources::NativeWork::Pack)?,
            )?;
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
            let status = verify.wait().await?;
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
        Ok(next)
    }
}

#[cfg(test)]
async fn finish<T: Send + 'static>(
    process: &mut GitProcess<T>,
    stderr: Vec<u8>,
) -> Result<(), GitHttpError> {
    let status = process.wait().await?;
    if !status.success() {
        return Err(GitHttpError::GitExit {
            status,
            stderr: String::from_utf8_lossy(&stderr).into_owned(),
        });
    }
    Ok(())
}
