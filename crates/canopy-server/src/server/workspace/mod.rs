//! Reclaimable local state, guarded against live nodes and orphan Git workers.

use std::{
    fs::{self, File},
    io::{self, Read, Write},
    path::{Path, PathBuf},
    sync::atomic::{AtomicBool, Ordering},
};

const MARKER: &str = ".canopy-runtime";
const FORMAT: &[u8] = b"canopy-runtime-v1\n";

pub(crate) struct Workspace {
    root: PathBuf,
    owner: Option<File>,
    requires_drain: AtomicBool,
}

impl Workspace {
    pub(crate) fn open(directory: &Path) -> io::Result<Self> {
        fs::create_dir_all(directory)?;
        let directory = fs::canonicalize(directory)?;
        let owner = crate::native_git::lock_file(&directory.join(".canopy-owner.lock"))?;
        owner.try_lock().map_err(io::Error::from)?;
        let root = directory.join("runtime-v1");
        #[cfg(unix)]
        let created = {
            use std::os::unix::fs::DirBuilderExt;
            fs::DirBuilder::new().mode(0o700).create(&root)
        };
        #[cfg(not(unix))]
        let created = fs::create_dir(&root);
        match created {
            Ok(()) => File::create_new(root.join(MARKER))?.write_all(FORMAT)?,
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                if !fs::symlink_metadata(&root)?.is_dir() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "unrecognized Canopy runtime directory",
                    ));
                }
                let mut marker = Vec::new();
                let marker_path = root.join(MARKER);
                if !fs::symlink_metadata(&marker_path)?.is_file() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "invalid Canopy runtime marker",
                    ));
                }
                File::open(marker_path)?
                    .take(FORMAT.len() as u64 + 1)
                    .read_to_end(&mut marker)?;
                if marker != FORMAT {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "unrecognized Canopy runtime format",
                    ));
                }
                // Acquire every worker fence before deleting anything. A Git
                // descendant may still be running after its server was killed.
                let _workers = worker_fences(&root)?;
                for entry in fs::read_dir(&root)? {
                    let entry = entry?;
                    if entry.file_name() == MARKER {
                        continue;
                    }
                    if entry.file_type()?.is_dir() {
                        fs::remove_dir_all(entry.path())?;
                    } else {
                        fs::remove_file(entry.path())?;
                    }
                }
            }
            Err(error) => return Err(error),
        }
        Ok(Self {
            root,
            owner: Some(owner),
            requires_drain: AtomicBool::new(false),
        })
    }

    pub(crate) fn path(&self) -> &Path {
        &self.root
    }

    pub(crate) fn require_drain(&self) {
        self.requires_drain.store(true, Ordering::Release);
    }

    pub(crate) fn confirm_drained(&self) {
        self.requires_drain.store(false, Ordering::Release);
    }
}

impl Drop for Workspace {
    fn drop(&mut self) {
        if self.requires_drain.load(Ordering::Acquire) {
            // Runtime destruction and unwinding can bypass async cleanup. Keep
            // exclusion until process exit unless SQL worker closure was proven.
            tracing::error!(path = %self.root.display(), "unconfirmed Cell drain; workspace exclusion retained until process restart");
            if let Some(owner) = self.owner.take() {
                std::mem::forget(owner);
            }
        }
    }
}

fn worker_fences(root: &Path) -> io::Result<Vec<File>> {
    let mut fences = Vec::new();
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        if !entry
            .file_name()
            .as_encoded_bytes()
            .starts_with(crate::git_cache::CACHE_PREFIX.as_bytes())
        {
            continue;
        }
        if !entry.file_type()?.is_dir() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid Git cache directory",
            ));
        }
        let Some(file) = crate::native_git::idle_fence(&entry.path().join("repo.git"))? else {
            continue;
        };
        // Windows does not inherit the Unix worker fence across exec. Never
        // infer a crashed worker is gone from the parent lock alone there.
        if !cfg!(unix) {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "orphan Git cache recovery requires Unix; stop all workers before manually removing the runtime directory",
            ));
        }
        fences.push(file);
    }
    Ok(fences)
}

#[cfg(test)]
mod tests;
