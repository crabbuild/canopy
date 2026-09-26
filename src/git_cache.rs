//! Disposable Git files charged to the node's shared disk budget.

use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::{self, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

use cellule_ltx::{DiskBudget, DiskReservation, LtxError};
use flate2::{Compression, write::ZlibEncoder};

use crate::{ObjectKind, RefExpectation, object_id, refs::valid_ref_name};

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

pub(crate) struct GitCache {
    directory: tempfile::TempDir,
    reservation: Option<DiskReservation>,
}

impl GitCache {
    pub(crate) async fn create(
        root: PathBuf,
        budget: DiskBudget,
        head: &str,
    ) -> Result<Arc<Self>, CacheError> {
        if !crate::default_branch::valid_default_branch(head) {
            return Err(CacheError::InvalidHead);
        }
        let head = format!("ref: {head}\n");
        tokio::task::spawn_blocking(move || {
            let cache = Arc::new(Self {
                directory: tempfile::TempDir::new_in(root)?,
                reservation: Some(budget.try_reserve(0)?),
            });
            for directory in ["objects/info", "objects/pack", "refs/heads", "refs/tags", "hooks"] {
                fs::create_dir_all(cache.git_dir().join(directory))?;
            }
            // Build an unpublished bare cache directly, without template hooks or
            // unaccounted init subprocess writes. Git remains the wire implementation.
            cache.write_file("HEAD", head.as_bytes())?;
            cache.write_file("config", b"[core]\nrepositoryformatversion = 0\nbare = true\nlogallrefupdates = false\n[receive]\nautogc = false\n[gc]\nauto = 0\n")?;
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

    fn writer(&self, relative: &Path) -> io::Result<CacheWriter<'_>> {
        let path = self.git_dir().join(relative);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        Ok(CacheWriter {
            file: OpenOptions::new().write(true).create_new(true).open(path)?,
            reservation: self.reservation()?,
        })
    }

    fn write_file(&self, relative: &str, bytes: &[u8]) -> io::Result<()> {
        self.writer(Path::new(relative))?.write_all(bytes)
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
            let hex = hex::encode(oid);
            let path = Path::new("objects").join(&hex[..2]).join(&hex[2..]);
            let mut encoder = ZlibEncoder::new(cache.writer(&path)?, Compression::default());
            encoder.write_all(format!("{} {}\0", kind.git_name(), body.len()).as_bytes())?;
            encoder.write_all(&body)?;
            encoder.finish()?;
            Ok(())
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
        if let Err(error) = fs::remove_dir_all(self.root())
            && error.kind() != io::ErrorKind::NotFound
        {
            tracing::error!(path = %self.root().display(), error = %error, "Git cache cleanup failed; disk admission retained until process restart");
            // Releasing capacity while files remain would undercount disk use.
            // Quarantine this reservation for the remaining process lifetime.
            if let Some(reservation) = self.reservation.take() {
                std::mem::forget(reservation);
            }
        }
    }
}

struct CacheWriter<'a> {
    file: File,
    reservation: &'a DiskReservation,
}

impl Write for CacheWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.reservation
            .try_grow(bytes.len() as u64)
            .map_err(io::Error::other)?;
        // Retain the full attempted charge on an I/O error; cache destruction is
        // the cleanup boundary. Successful short writes release unused admission.
        let count = self.file.write(bytes)?;
        if count < bytes.len() {
            self.reservation
                .resize(self.reservation.bytes() - (bytes.len() - count) as u64)
                .map_err(io::Error::other)?;
        }
        Ok(count)
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
