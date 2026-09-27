//! Disposable Git files charged to the node's shared disk budget.

use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::{self, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

use crab_ltx::{CrabError, DiskBudget, DiskReservation};
use flate2::{Compression, write::ZlibEncoder};

use crate::{
    ObjectKind, RefExpectation,
    large_blob::{LargeBlobError, LargeBlobRead},
    object_id,
    refs::valid_ref_name,
};

pub(crate) const CACHE_PREFIX: &str = "canopy-git-";

#[derive(Debug, thiserror::Error)]
pub enum CacheError {
    #[error("Git cache HEAD must name a valid branch")]
    InvalidHead,
    #[error("Git cache I/O failed")]
    Io(#[from] io::Error),
    #[error("Git cache disk admission failed")]
    Budget(#[from] CrabError),
    #[error("Git blob hydration failed")]
    Blob(#[from] LargeBlobError),
    #[error("Git cache worker failed")]
    Task(#[from] tokio::task::JoinError),
}

impl CacheError {
    pub(crate) fn is_admission(&self) -> bool {
        matches!(self, Self::Budget(_))
            || matches!(self, Self::Io(error) if error.get_ref().is_some_and(|source| source.is::<CrabError>()))
    }
}

pub(crate) struct GitCache {
    directory: tempfile::TempDir,
    reservation: Option<DiskReservation>,
    objects: Option<Arc<GitCache>>,
}

impl GitCache {
    pub(crate) async fn create(
        root: PathBuf,
        budget: DiskBudget,
        head: &str,
    ) -> Result<Arc<Self>, CacheError> {
        Self::create_with_objects(root, budget, head, None).await
    }

    pub(crate) async fn create_with_objects(
        root: PathBuf,
        budget: DiskBudget,
        head: &str,
        objects: Option<Arc<GitCache>>,
    ) -> Result<Arc<Self>, CacheError> {
        if !crate::default_branch::valid_default_branch(head) {
            return Err(CacheError::InvalidHead);
        }
        let head = format!("ref: {head}\n");
        tokio::task::spawn_blocking(move || {
            let cache = Arc::new(Self {
                // Native workers change cwd to this cache; their paths must stay
                // absolute even when the node's data directory is relative.
                directory: tempfile::Builder::new().prefix(CACHE_PREFIX).tempdir_in(fs::canonicalize(root)?)?,
                reservation: Some(budget.try_reserve(0)?),
                objects,
            });
            for directory in ["objects/info", "objects/pack", "refs/heads", "refs/tags", "hooks"] {
                fs::create_dir_all(cache.git_dir().join(directory))?;
            }
            // Build an unpublished bare cache directly, without template hooks or
            // unaccounted init subprocess writes. Git remains the wire implementation.
            cache.write_file("HEAD", head.as_bytes())?;
            cache.write_file("config", b"[core]\nrepositoryformatversion = 0\nbare = true\nlogallrefupdates = false\n[receive]\nautogc = false\n[gc]\nauto = 0\n")?;
            if let Some(objects) = &cache.objects {
                if objects.objects.is_some() || objects.root().parent() != cache.root().parent() {
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
        ids: Vec<[u8; 20]>,
    ) -> Result<Vec<[u8; 20]>, CacheError> {
        let cache = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            let mut missing = Vec::new();
            for oid in ids {
                match fs::symlink_metadata(cache.object_path(oid)) {
                    Ok(metadata) if metadata.is_file() => {}
                    Err(error) if error.kind() == io::ErrorKind::NotFound => missing.push(oid),
                    Err(error) => return Err(CacheError::Io(error)),
                    _ => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "invalid cached object",
                        )
                        .into());
                    }
                }
            }
            Ok(missing)
        })
        .await?
    }

    fn object_path(&self, oid: [u8; 20]) -> PathBuf {
        let hex = hex::encode(oid);
        self.git_dir()
            .join("objects")
            .join(&hex[..2])
            .join(&hex[2..])
    }

    fn object_writer(
        self: &Arc<Self>,
        oid: [u8; 20],
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

    pub(crate) async fn store_update_hook(
        self: &Arc<Self>,
        bytes: Vec<u8>,
    ) -> Result<(), CacheError> {
        let cache = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            cache.write_file("hooks/update", &bytes)?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                fs::set_permissions(
                    cache.git_dir().join("hooks/update"),
                    fs::Permissions::from_mode(0o700),
                )?;
            }
            Ok(())
        })
        .await?
    }

    pub(crate) async fn store_object(
        self: &Arc<Self>,
        oid: [u8; 20],
        kind: ObjectKind,
        body: Vec<u8>,
    ) -> Result<(), CacheError> {
        let cache = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            if object_id(kind, &body) != oid {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "Git OID mismatch").into());
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

    pub(crate) async fn store_blob(
        self: &Arc<Self>,
        mut reader: LargeBlobRead,
    ) -> Result<(), CacheError> {
        let reference = reader.reference();
        let cache = Arc::clone(self);
        let (mut encoder, temporary, destination) = tokio::task::spawn_blocking(move || {
            let (mut encoder, temporary, destination) = cache.object_writer(reference.oid)?;
            encoder.write_all(format!("blob {}\0", reference.size).as_bytes())?;
            Ok::<_, CacheError>((encoder, temporary, destination))
        })
        .await??;
        while let Some(bytes) = reader.next().await? {
            // The writer owns the cache reservation until each compression worker
            // exits, including when its async waiter is canceled.
            encoder = tokio::task::spawn_blocking(move || {
                encoder.write_all(&bytes)?;
                Ok::<_, CacheError>(encoder)
            })
            .await??;
        }
        tokio::task::spawn_blocking(move || {
            drop(encoder.finish()?);
            temporary
                .persist_noclobber(destination)
                .map_err(|error| error.error)?;
            Ok::<_, CacheError>(())
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
        let cache = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            for (name, oid) in refs {
                if !valid_ref_name(&name) {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "invalid stored ref name",
                    )
                    .into());
                }
                cache.write_file(&name, format!("{}\n", hex::encode(oid)).as_bytes())?;
            }
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
