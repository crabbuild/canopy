//! Capture request-private native inputs in the admitted creating namespace.
use super::*;
use crate::packs::{
    directory::index::IndexError,
    metadata::{
        MetadataError,
        transport::{PinnedFile, upload_file},
    },
    publication::{StagingContext, StagingError},
    sources::NativePackDescriptor,
    verification::PhysicalLimits,
};
use canopy_object_storage::{
    artifact::{ArtifactKind, ArtifactStore},
    external::MAX_ARTIFACT_BYTES,
};
use std::fs::File;

const MAX_CAPTURE_PACKS: usize = 32;
#[derive(Debug, thiserror::Error)]
pub enum NativeCaptureError {
    #[error("native input custody failed")]
    Staging(#[from] StagingError),
    #[error("native input context or inventory is invalid")]
    Context,
    #[error("native input exceeds its admitted limits")]
    Limit,
    #[error("native input binding failed")]
    Binding(#[from] IndexError),
    #[error("native input upload failed")]
    Upload(#[from] MetadataError),
    #[error("native input cache failed")]
    Cache(#[from] CacheError),
    #[error("native input I/O failed")]
    Io(#[from] std::io::Error),
    #[error("native input worker failed")]
    Task(#[from] tokio::task::JoinError),
}
struct CapturePin {
    // Native commands require a shared lock; this exclusive fence keeps every
    // captured file immutable while hash/upload background jobs retain it.
    _fence: File,
    cache: Arc<GitCache>,
    _owner: crate::git_objects::ReadOwner,
}
impl Drop for CapturePin {
    fn drop(&mut self) {
        // CLOEXEC only closes accidental copies when unrelated children exec.
        // Closing this parent's descriptor alone can leave the exclusive lock
        // alive in such a child. All capture readers have drained when the last
        // Arc drops, so explicitly release their lock before cache ownership.
        // Native workers' shared locks still follow their descendants instead.
        if let Err(error) = self._fence.unlock() {
            // Failure stays conservative: native admission still probes the
            // lock, and cache cleanup defers while any inherited lock survives.
            tracing::error!(error = %error, "completed native capture fence unlock failed");
        }
    }
}
struct InputFile {
    path: PathBuf,
    _pin: Arc<CapturePin>,
}
impl PinnedFile for InputFile {
    fn open(&self) -> std::io::Result<File> {
        File::open(&self.path)
    }
}
struct Pair {
    native: NativePackDescriptor,
    pack: Arc<InputFile>,
    index: Arc<InputFile>,
}
impl GitHttpBackend {
    /// Git writes its verified push certificate as a request-private loose
    /// blob. The immutable native result retains these exact audit bytes; this
    /// disposable blob is not an incoming pack or a reachable Git object.
    pub(crate) async fn remove_disposable_certificate(
        &self,
        context: &StagingContext,
        oid: crate::ObjectId,
    ) -> Result<(), NativeCaptureError> {
        context.ensure_live()?;
        if oid.format() != context.format() {
            return Err(NativeCaptureError::Context);
        }
        let cache = self.cache.clone();
        let activity = context.physical_owner();
        tokio::task::spawn_blocking(move || {
            let _activity = activity;
            let fence = crate::native_git::lock_file(
                &cache.git_dir().join(crate::native_git::WORKER_LOCK),
            )?;
            fence.try_lock().map_err(std::io::Error::from)?;
            let hex = hex::encode(oid);
            let directory = cache.git_dir().join("objects").join(&hex[..2]);
            match std::fs::remove_file(directory.join(&hex[2..])) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
            match std::fs::remove_dir(directory) {
                Ok(()) => {}
                Err(error)
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::NotFound | std::io::ErrorKind::DirectoryNotEmpty
                    ) => {}
                Err(error) => return Err(error.into()),
            }
            fence.unlock()?;
            Ok::<_, NativeCaptureError>(())
        })
        .await??;
        self.cache.reconcile_owned(context.physical_owner()).await?;
        context.ensure_live()?;
        Ok(())
    }

    /// Run inside a StagingTicket producer after native receive completes. The
    /// returned inputs establish authenticated bytes, not physical decoding,
    /// closure, ref authorization or a durable completed network response.
    /// No object bodies, OID inventory or legacy Git rows are constructed here.
    pub async fn stage_native_packs(
        &self,
        context: &StagingContext,
        store: &ArtifactStore,
        limits: PhysicalLimits,
    ) -> Result<Vec<NativePackDescriptor>, NativeCaptureError> {
        let token = context.token()?;
        if token.repository != store.repository() || context.format() != self.cache.object_format {
            return Err(NativeCaptureError::Context);
        }
        if limits.max_pack_bytes > MAX_ARTIFACT_BYTES || limits.max_index_bytes > MAX_ARTIFACT_BYTES
        {
            return Err(NativeCaptureError::Limit);
        }
        let _selection = self.cache.selection.lock().await;
        self.cache.reconcile_owned(context.physical_owner()).await?;
        let cache = Arc::clone(&self.cache);
        let format = context.format();
        let owner = context.physical_owner();
        let claim = cache
            .native
            .try_admit(crate::native_resources::NativeWork::Read)?;
        let pairs = tokio::task::spawn_blocking(move || {
            let _claim = claim;
            let fence = crate::native_git::lock_file(
                &cache.git_dir().join(crate::native_git::WORKER_LOCK),
            )?;
            fence.try_lock().map_err(std::io::Error::from)?;
            let pin = Arc::new(CapturePin {
                cache,
                _fence: fence,
                _owner: owner,
            });
            for entry in std::fs::read_dir(pin.cache.git_dir().join("objects"))? {
                let entry = entry?;
                if !entry.file_type()?.is_dir()
                    || !matches!(entry.file_name().to_str(), Some("pack" | "info"))
                {
                    return Err(NativeCaptureError::Context);
                }
            }
            let root = pin.cache.git_dir().join("objects/pack");
            let mut paths = Vec::new();
            for entry in std::fs::read_dir(&root)? {
                let entry = entry?;
                let name = entry.file_name();
                let name = name.to_str().ok_or(NativeCaptureError::Context)?;
                if !name.ends_with(".pack") {
                    continue;
                }
                if paths.len() == MAX_CAPTURE_PACKS || !entry.file_type()?.is_file() {
                    return Err(NativeCaptureError::Limit);
                }
                paths.push(entry.path());
            }
            paths.sort();
            let mut pairs = Vec::with_capacity(paths.len());
            for path in paths {
                let index_path = path.with_extension("idx");
                if !std::fs::symlink_metadata(&index_path)?.is_file() {
                    return Err(NativeCaptureError::Context);
                }
                if std::fs::metadata(&path)?.len() > limits.max_pack_bytes
                    || std::fs::metadata(&index_path)?.len() > limits.max_index_bytes
                {
                    return Err(NativeCaptureError::Limit);
                }
                if NativePackDescriptor::is_empty_pair(format, &path, &index_path)? {
                    continue;
                }
                let native = NativePackDescriptor::inspect_files(
                    token.repository,
                    token.artifact_operation,
                    format,
                    &path,
                    &index_path,
                )?;
                if path.file_name().and_then(|name| name.to_str())
                    != Some(format!("pack-{}.pack", hex::encode(native.git_checksum)).as_str())
                {
                    return Err(NativeCaptureError::Context);
                }
                pairs.push(Pair {
                    native,
                    pack: Arc::new(InputFile {
                        path,
                        _pin: Arc::clone(&pin),
                    }),
                    index: Arc::new(InputFile {
                        path: index_path,
                        _pin: Arc::clone(&pin),
                    }),
                });
            }
            Ok::<_, NativeCaptureError>(pairs)
        })
        .await??;
        context.ensure_live()?;
        let mut inputs = Vec::with_capacity(pairs.len());
        for Pair {
            mut native,
            pack,
            index,
        } in pairs
        {
            context.ensure_live()?;
            native.pack = upload_file(
                pack,
                store,
                native.key(ArtifactKind::Pack)?,
                native.pack.size,
                native.pack.digest,
            )
            .await?;
            context.ensure_live()?;
            native.index = upload_file(
                index,
                store,
                native.key(ArtifactKind::Index)?,
                native.index.size,
                native.index.digest,
            )
            .await?;
            context.ensure_live()?;
            inputs.push(native);
        }
        context.ensure_live()?;
        Ok(inputs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::Future;
    #[cfg(unix)]
    #[test]
    fn completed_capture_releases_fence_despite_unrelated_pre_exec_inheritance()
    -> Result<(), Box<dyn std::error::Error>> {
        use std::io::{Read, Write};
        use std::os::{
            fd::AsRawFd,
            unix::{net::UnixStream, process::CommandExt},
        };
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let root = tempfile::TempDir::new()?;
        let backend = runtime.block_on(GitHttpBackend::initialize(
            root.path().into(),
            DiskBudget::new(1 << 20),
            "refs/heads/main",
            crate::ObjectFormat::Sha1,
            crate::native_resources::NativeResources::default()
                .scope(crate::native_resources::NativeClass::Foreground),
        ))?;
        let fence =
            crate::native_git::lock_file(&backend.git_dir().join(crate::native_git::WORKER_LOCK))?;
        fence.try_lock().map_err(std::io::Error::from)?;
        let captured = Arc::new(InputFile {
            path: backend.git_dir().join("config"),
            _pin: Arc::new(CapturePin {
                _fence: fence,
                cache: backend.cache.clone(),
                _owner: Arc::new(()),
            }),
        });
        let retained = captured.clone();
        let (mut ready, child_ready) = UnixStream::pair()?;
        let (mut release, child_release) = UnixStream::pair()?;
        ready.set_read_timeout(Some(Duration::from_secs(5)))?;
        let child = std::thread::spawn(move || {
            let mut command = std::process::Command::new("true");
            // SAFETY: only async-signal-safe read/write run after fork. Socket
            // owners are captured until spawn returns; the parent controls EOF.
            unsafe {
                command.pre_exec(move || {
                    let mut byte = 1u8;
                    if libc::write(child_ready.as_raw_fd(), (&byte as *const u8).cast(), 1) != 1
                        || libc::read(child_release.as_raw_fd(), (&mut byte as *mut u8).cast(), 1)
                            != 1
                    {
                        return Err(std::io::Error::last_os_error());
                    }
                    Ok(())
                });
            }
            command.status()
        });
        let ready_result = ready.read_exact(&mut [0]);
        drop(captured);
        // Another input/upload owner still protects the pair.
        let retained_blocks = crate::native_git::command(&backend.git_dir())
            .is_err_and(|e| e.kind() == std::io::ErrorKind::WouldBlock);
        drop(retained);
        // Capture is fully complete. An unrelated child cannot mutate this
        // cache; its accidentally inherited descriptor must not retain custody.
        let admission = crate::native_git::command(&backend.git_dir());
        // Release the task-owned child before asserting on any failure.
        let released = release.write_all(&[1]);
        drop(release);
        let status = child.join().map_err(|_| "foreign child thread panicked")?;
        ready_result?;
        released?;
        assert!(status?.success());
        assert!(retained_blocks);
        assert!(
            admission.is_ok(),
            "finished capture retained by foreign fork: {:?}",
            admission.err()
        );
        Ok(())
    }

    #[test]
    fn canceled_queued_capture_upload_retains_cache_fence_and_disk()
    -> Result<(), Box<dyn std::error::Error>> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .max_blocking_threads(1)
            .enable_all()
            .build()?;
        let root = tempfile::TempDir::new()?;
        let budget = DiskBudget::new(1 << 20);
        let backend = runtime.block_on(GitHttpBackend::initialize(
            root.path().into(),
            budget.clone(),
            "refs/heads/main",
            crate::ObjectFormat::Sha256,
            crate::native_resources::NativeResources::default()
                .scope(crate::native_resources::NativeClass::Foreground),
        ))?;
        let path = backend.git_dir().join("config");
        let bytes = std::fs::read(&path)?;
        let fence =
            crate::native_git::lock_file(&backend.git_dir().join(crate::native_git::WORKER_LOCK))?;
        fence.try_lock().map_err(std::io::Error::from)?;
        let weak = Arc::downgrade(&backend.cache);
        let captured = Arc::new(InputFile {
            path,
            _pin: Arc::new(CapturePin {
                _fence: fence,
                cache: Arc::clone(&backend.cache),
                _owner: Arc::new(()),
            }),
        });
        let (ready_tx, ready_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let blocker = runtime.spawn_blocking(move || {
            ready_tx.send(()).unwrap();
            release_rx.recv().unwrap();
        });
        ready_rx.recv_timeout(Duration::from_secs(5))?;
        let store = ArtifactStore::new(Arc::new(object_store::memory::InMemory::new()), [1; 16]);
        let mut upload = Box::pin(upload_file(
            captured,
            &store,
            canopy_object_storage::artifact::ArtifactKey {
                operation: [2; 16],
                binding_digest: [3; 32],
                kind: ArtifactKind::Pack,
            },
            bytes.len() as u64,
            *blake3::hash(&bytes).as_bytes(),
        ));
        let pending = {
            let _entered = runtime.enter();
            let mut cx = Context::from_waker(std::task::Waker::noop());
            upload.as_mut().poll(&mut cx).is_pending()
        };
        drop(upload);
        let git_dir = backend.git_dir();
        drop(backend);
        let retained = weak.upgrade().is_some();
        let charged = budget.used();
        let locked = crate::native_git::command(&git_dir)
            .is_err_and(|error| error.kind() == std::io::ErrorKind::WouldBlock);
        // Release before assertions so a failure cannot strand runtime shutdown.
        release_tx.send(())?;
        runtime.block_on(async {
            blocker.await?;
            tokio::time::timeout(Duration::from_secs(5), async {
                while budget.used() != 0 {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await?;
            Ok::<_, Box<dyn std::error::Error>>(())
        })?;
        assert!(pending && retained && locked);
        assert!(charged > 0);
        assert!(weak.upgrade().is_none());
        assert!(!git_dir.exists());
        Ok(())
    }
}
