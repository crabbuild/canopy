//! Disposable Git files charged to the node's shared disk budget.

use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::{self, BufWriter, Write},
    path::{Path, PathBuf},
    sync::{Arc, OnceLock, RwLock},
};

use cellule_ltx::{DiskBudget, DiskReservation, LtxError};
use flate2::{Compression, write::ZlibEncoder};
use tokio::sync::Mutex;

use crate::{ObjectKind, RefExpectation, object_id, refs::valid_ref_name};

pub(crate) const CACHE_PREFIX: &str = "canopy-git-";

#[derive(Debug, thiserror::Error)]
pub enum CacheError {
    #[error("Git cache HEAD must name a valid branch")]
    InvalidHead,
    #[error("Git cache I/O failed")]
    Io(#[from] io::Error),
    #[error("Git cache disk admission failed")]
    Budget(#[from] LtxError),
    #[error("Git cache worker failed")]
    Task(#[from] tokio::task::JoinError),
}

impl CacheError {
    pub(crate) fn is_admission(&self) -> bool {
        matches!(self, Self::Budget(_))
            || matches!(self, Self::Io(error) if error.get_ref().is_some_and(|source| source.is::<LtxError>()))
    }
}

pub(crate) enum ReceiveHook {
    Update,
    PreReceive,
}

pub(crate) struct GitCache {
    object_format: crate::ObjectFormat,
    directory: tempfile::TempDir,
    reservation: Option<DiskReservation>,
    objects: Option<Arc<GitCache>>,
    // Only durable hydration writes this cache. Stripe by OID so concurrent
    // fetches share a completed loose object without serializing all objects.
    object_writes: OnceLock<[Arc<Mutex<()>>; 64]>,
    packs: RwLock<BTreeMap<[u8; 32], Arc<crate::git_pack::OwnedPack>>>,
    pack_writes: Arc<Mutex<()>>,
}

impl GitCache {
    pub(crate) async fn create(
        root: PathBuf,
        budget: DiskBudget,
        head: &str,
        object_format: crate::ObjectFormat,
    ) -> Result<Arc<Self>, CacheError> {
        Self::create_with_objects(root, budget, head, object_format, None).await
    }

    pub(crate) async fn create_with_objects(
        root: PathBuf,
        budget: DiskBudget,
        head: &str,
        object_format: crate::ObjectFormat,
        objects: Option<Arc<GitCache>>,
    ) -> Result<Arc<Self>, CacheError> {
        if !crate::default_branch::valid_default_branch(head) {
            return Err(CacheError::InvalidHead);
        }
        let head = format!("ref: {head}\n");
        tokio::task::spawn_blocking(move || {
            let cache = Arc::new(Self {
                object_format,
                // Native workers change cwd to this cache; their paths must stay
                // absolute even when the node's data directory is relative.
                directory: tempfile::Builder::new().prefix(CACHE_PREFIX).tempdir_in(fs::canonicalize(root)?)?,
                reservation: Some(budget.try_reserve(0)?),
                objects,
                object_writes: OnceLock::new(),
                packs: RwLock::new(BTreeMap::new()),
                pack_writes: Arc::new(Mutex::new(())),
            });
            for directory in ["objects/info", "objects/pack", "refs/heads", "refs/tags", "hooks"] {
                fs::create_dir_all(cache.git_dir().join(directory))?;
            }
            // Build an unpublished bare cache directly, without template hooks or
            // unaccounted init subprocess writes. Git remains the wire implementation.
            cache.write_file("HEAD", head.as_bytes())?;
            let version = u8::from(object_format == crate::ObjectFormat::Sha256);
            let extension = if version == 1 { "[extensions]\nobjectformat = sha256\n" } else { "" };
            cache.write_file("config", format!("[core]\nrepositoryformatversion = {version}\nbare = true\nlogallrefupdates = false\n[receive]\nautogc = false\n[gc]\nauto = 0\n{extension}").as_bytes())?;
            if let Some(objects) = &cache.objects {
                if objects.object_format != object_format || objects.objects.is_some() || objects.root().parent() != cache.root().parent() {
                    return Err(io::Error::new(io::ErrorKind::InvalidInput, "object cache must be a direct sibling").into());
                }
                let name = objects.root().file_name().and_then(|name| name.to_str())
                    .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "invalid object cache path"))?;
                // Relative to objects/, and generated entirely from TempDir's
                // ASCII name, so host path quoting cannot redirect Git reads.
                cache.write_file("objects/info/alternates", format!("../../../{name}/repo.git/objects\n").as_bytes())?;
            }
            Ok(cache)
        }).await?
    }

    pub(crate) fn object_format(&self) -> crate::ObjectFormat {
        self.object_format
    }

    pub(crate) fn root(&self) -> &Path {
        self.directory.path()
    }

    pub(crate) fn git_dir(&self) -> PathBuf {
        self.root().join("repo.git")
    }

    fn reservation(&self) -> io::Result<&DiskReservation> {
        self.reservation
            .as_ref()
            .ok_or_else(|| io::Error::other("Git cache accounting is closed"))
    }

    pub(crate) fn bytes(&self) -> io::Result<u64> {
        Ok(self.reservation()?.bytes())
    }

    fn writer(self: &Arc<Self>, relative: &Path) -> io::Result<CacheWriter> {
        let path = self.git_dir().join(relative);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        Ok(CacheWriter {
            file: OpenOptions::new().write(true).create_new(true).open(path)?,
            cache: Arc::clone(self),
        })
    }

    fn write_file(self: &Arc<Self>, relative: &str, bytes: &[u8]) -> io::Result<()> {
        self.writer(Path::new(relative))?.write_all(bytes)
    }

    pub(crate) async fn missing_objects(
        self: &Arc<Self>,
        ids: Vec<crate::ObjectId>,
    ) -> Result<Vec<crate::ObjectId>, CacheError> {
        let cache = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            let mut missing = Vec::new();
            for oid in ids {
                if !cache.object_present(oid)? {
                    missing.push(oid);
                }
            }
            Ok(missing)
        })
        .await?
    }

    fn object_path(&self, oid: crate::ObjectId) -> PathBuf {
        let hex = hex::encode(oid);
        self.git_dir()
            .join("objects")
            .join(&hex[..2])
            .join(&hex[2..])
    }

    fn object_present(&self, oid: crate::ObjectId) -> io::Result<bool> {
        for pack in self
            .packs
            .read()
            .map_err(|_| io::Error::other("Git pack cache lock poisoned"))?
            .values()
        {
            if pack
                .data()
                .and_then(|data| data.contains(oid))
                .map_err(io::Error::other)?
            {
                return Ok(true);
            }
        }
        match fs::symlink_metadata(self.object_path(oid)) {
            Ok(metadata) if metadata.is_file() => Ok(true),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
            Err(error) => Err(error),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid cached object",
            )),
        }
    }

    fn object_write_lock(&self, oid: crate::ObjectId) -> Arc<Mutex<()>> {
        let stripes = self
            .object_writes
            .get_or_init(|| std::array::from_fn(|_| Arc::new(Mutex::new(()))));
        Arc::clone(&stripes[oid[0] as usize % stripes.len()])
    }

    fn object_writer(
        self: &Arc<Self>,
        oid: crate::ObjectId,
    ) -> io::Result<(ZlibEncoder<CacheWriter>, tempfile::TempPath, PathBuf)> {
        let destination = self.object_path(oid);
        let parent = destination
            .parent()
            .ok_or_else(|| io::Error::other("missing object directory"))?;
        fs::create_dir_all(parent)?;
        let (file, temporary) = tempfile::NamedTempFile::new_in(parent)?.into_parts();
        let writer = CacheWriter {
            file,
            cache: Arc::clone(self),
        };
        Ok((
            ZlibEncoder::new(writer, Compression::default()),
            temporary,
            destination,
        ))
    }

    /// Installs a native pair with the index renamed last. Git never observes
    /// an index naming an incomplete pack. The cache retains the immutable
    /// source owner and charges linked files conservatively to its own budget.
    pub(crate) async fn install_pack(
        self: &Arc<Self>,
        pack: Arc<crate::git_pack::OwnedPack>,
    ) -> Result<(), CacheError> {
        let write = Arc::clone(&self.pack_writes).lock_owned().await;
        let cache = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            let _write = write;
            let identity = pack.data().map_err(io::Error::other)?.identity();
            if identity.format != cache.object_format {
                return Err(
                    io::Error::new(io::ErrorKind::InvalidData, "Git pack format differs").into(),
                );
            }
            if cache
                .packs
                .read()
                .map_err(|_| io::Error::other("Git pack cache lock poisoned"))?
                .contains_key(&identity.pack_digest)
            {
                return Ok(());
            }
            let source = pack.directory().map_err(io::Error::other)?;
            let stem = format!("objects/pack/pack-{}", hex::encode(identity.pack_digest));
            for (extension, bytes) in [("pack", identity.pack_bytes), ("idx", identity.index_bytes)]
            {
                let destination = cache.git_dir().join(format!("{stem}.{extension}"));
                let pending = cache.git_dir().join(format!("{stem}.{extension}.pending"));
                cache.reservation()?.try_grow(bytes)?;
                // Source and cache share the configured scratch filesystem.
                // Count each link's bytes even though physical blocks are shared.
                fs::hard_link(source.join(format!("objects.{extension}")), &pending)?;
                fs::rename(pending, destination)?;
            }
            cache
                .packs
                .write()
                .map_err(|_| io::Error::other("Git pack cache lock poisoned"))?
                .insert(identity.pack_digest, pack);
            Ok(())
        })
        .await?
    }

    pub(crate) async fn store_receive_hook(
        self: &Arc<Self>,
        hook: ReceiveHook,
        bytes: Vec<u8>,
    ) -> Result<(), CacheError> {
        let path = match hook {
            ReceiveHook::Update => "hooks/update",
            ReceiveHook::PreReceive => "hooks/pre-receive",
        };
        let cache = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            cache.write_file(path, &bytes)?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                fs::set_permissions(
                    cache.git_dir().join(path),
                    fs::Permissions::from_mode(0o700),
                )?;
            }
            Ok(())
        })
        .await?
    }

    pub(crate) async fn store_push_signers(
        self: &Arc<Self>,
        bytes: Vec<u8>,
    ) -> Result<PathBuf, CacheError> {
        let cache = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            cache.write_file("hooks/canopy-push-signers", &bytes)?;
            Ok(cache.git_dir().join("hooks/canopy-push-signers"))
        })
        .await?
    }

    pub(crate) async fn store_object(
        self: &Arc<Self>,
        oid: crate::ObjectId,
        kind: ObjectKind,
        body: Vec<u8>,
    ) -> Result<(), CacheError> {
        let write = self.object_write_lock(oid).lock_owned().await;
        let cache = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            let _write = write;
            if oid.format() != cache.object_format || object_id(oid.format(), kind, &body) != oid {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "Git OID mismatch").into());
            }
            if cache.object_present(oid)? {
                return Ok(());
            }
            let (mut encoder, temporary, destination) = cache.object_writer(oid)?;
            encoder.write_all(format!("{} {}\0", kind.git_name(), body.len()).as_bytes())?;
            encoder.write_all(&body)?;
            drop(encoder.finish()?);
            temporary
                .persist_noclobber(destination)
                .map_err(|error| error.error)?;
            Ok(())
        })
        .await?
    }

    /// Streams one selected object through native Git without making omitted
    /// pack entries available to the serving cache. The immutable pair remains
    /// the durable authority; this loose file is a disposable projection.
    pub(crate) async fn store_native_object(
        self: &Arc<Self>,
        source: Arc<crate::git_pack::OwnedPack>,
        reference: crate::PackedObject,
        root: PathBuf,
        budget: DiskBudget,
    ) -> Result<(), CacheError> {
        let oid = reference.oid;
        let kind = reference.kind;
        let size = reference.size;
        let digest = reference.digest;
        let write = self.object_write_lock(oid).lock_owned().await;
        let cache = Arc::clone(self);
        tokio::spawn(async move {
            let _write = write;
            if cache.object_present(oid)? {
                return Ok(());
            }
            let input = GitCache::create(root, budget, "refs/heads/main", oid.format()).await?;
            input.install_pack(source).await?;
            let mut child = crate::native_git::command(&input.git_dir())?
                .args(["cat-file", kind.git_name(), &hex::encode(oid)])
                .stdin(std::process::Stdio::null())
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::null())
                .kill_on_drop(true)
                .spawn()?;
            let mut reader = child
                .stdout
                .take()
                .ok_or_else(|| io::Error::other("missing native object stream"))?;
            let owner = Arc::clone(&cache);
            let mut pending = tokio::task::spawn_blocking(move || {
                let (mut encoder, temporary, destination) = owner.object_writer(oid)?;
                encoder.write_all(format!("{} {size}\0", kind.git_name()).as_bytes())?;
                Ok::<_, CacheError>((
                    encoder,
                    temporary,
                    destination,
                    crate::git_format::ObjectHasher::new(oid.format(), kind, size),
                    blake3::Hasher::new(),
                ))
            })
            .await??;
            let transferred = async {
                use tokio::io::AsyncReadExt;
                let mut count = 0u64;
                loop {
                    let mut buffer = vec![0; 1024 * 1024];
                    let bytes = tokio::time::timeout(
                        std::time::Duration::from_secs(120),
                        reader.read(&mut buffer),
                    )
                    .await
                    .map_err(io::Error::other)??;
                    if bytes == 0 {
                        break;
                    }
                    count = count
                        .checked_add(bytes as u64)
                        .filter(|count| *count <= size)
                        .ok_or_else(|| io::Error::other("native object length differs"))?;
                    buffer.truncate(bytes);
                    pending = tokio::task::spawn_blocking(move || {
                        pending.3.update(&buffer);
                        pending.4.update(&buffer);
                        pending.0.write_all(&buffer)?;
                        Ok::<_, CacheError>(pending)
                    })
                    .await??;
                }
                Ok::<_, CacheError>((count, pending))
            }
            .await;
            if transferred.is_err() {
                let _ = child.start_kill();
            }
            let status = child.wait().await?;
            let (count, (encoder, temporary, destination, hash, body_hash)) = transferred?;
            if !status.success()
                || count != size
                || hash.finalize() != oid
                || body_hash.finalize().as_bytes() != &digest
            {
                return Err(io::Error::other("corrupt native object stream").into());
            }
            tokio::task::spawn_blocking(move || {
                drop(encoder.finish()?);
                temporary
                    .persist_noclobber(destination)
                    .map_err(|error| error.error)?;
                Ok::<_, CacheError>(())
            })
            .await?
        })
        .await?
    }

    pub(crate) async fn store_refs(
        self: &Arc<Self>,
        refs: &BTreeMap<String, RefExpectation>,
    ) -> Result<(), CacheError> {
        let refs: Vec<_> = refs
            .iter()
            .filter_map(|(name, state)| state.oid.map(|oid| (name.clone(), oid)))
            .collect();
        if refs.is_empty() {
            return Ok(());
        }
        let cache = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            let mut packed = BufWriter::new(cache.writer(Path::new("packed-refs"))?);
            packed.write_all(b"# pack-refs with: sorted\n")?;
            for (name, oid) in refs {
                if !valid_ref_name(&name) {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "invalid stored ref name",
                    )
                    .into());
                }
                writeln!(packed, "{} {name}", hex::encode(oid))?;
            }
            packed.flush()?;
            Ok(())
        })
        .await?
    }

    /// Measures native Git's completed writes before the gateway can publish refs.
    pub(crate) async fn reconcile(self: &Arc<Self>) -> Result<(), CacheError> {
        let cache = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            cache.reservation()?.resize(tree_bytes(cache.root())?)?;
            Ok(())
        })
        .await?
    }
}

impl Drop for GitCache {
    fn drop(&mut self) {
        let cleanup = || {
            let _worker = crate::native_git::idle_fence(&self.git_dir())?;
            fs::remove_dir_all(self.root())
        };
        if let Err(error) = cleanup()
            && error.kind() != io::ErrorKind::NotFound
        {
            tracing::error!(path = %self.root().display(), error = %error, "Git cache cleanup failed; disk admission retained until process restart");
            // Releasing capacity while files remain would undercount disk use.
            // Quarantine this reservation for the remaining process lifetime.
            if let Some(reservation) = self.reservation.take() {
                std::mem::forget(reservation);
            }
            // An orphan keeps this generation's native fence. Its alternate
            // must survive too, until startup fences every generation together.
            if let Some(objects) = self.objects.take() {
                std::mem::forget(objects);
            }
            // TempDir must not retry deletion after a worker fence rejected it.
            // Startup reclaims this directory once all descendants have exited.
            self.directory.disable_cleanup(true);
        }
    }
}

struct CacheWriter {
    file: File,
    cache: Arc<GitCache>,
}

impl Write for CacheWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.cache
            .reservation()?
            .try_grow(bytes.len() as u64)
            .map_err(io::Error::other)?;
        // A cancelled hydrator's blocking write can overlap the next hydrator.
        // Keep the full attempt charged, including short/error writes: a racy
        // read-then-resize refund could erase the other writer's reservation.
        self.file.write(bytes)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

fn tree_bytes(path: &Path) -> io::Result<u64> {
    let mut bytes = 0_u64;
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let metadata = entry.metadata()?;
        let size = if metadata.is_dir() {
            tree_bytes(&entry.path())?
        } else if metadata.is_file() {
            metadata.len()
        } else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unsupported Git cache file",
            ));
        };
        bytes = bytes
            .checked_add(size)
            .ok_or_else(|| io::Error::other("Git cache size overflow"))?;
    }
    Ok(bytes)
}

#[cfg(test)]
#[path = "git_cache/tests.rs"]
mod tests;
