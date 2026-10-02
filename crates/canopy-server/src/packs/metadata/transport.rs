use super::*;
use bytes::Bytes;
use canopy_object_storage::artifact::{
    ArtifactDescriptor, ArtifactKey, ArtifactKind, ArtifactStore,
};
use futures_core::Stream;
use std::{
    future::Future,
    io::{Seek, SeekFrom, Write},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tokio_util::io::StreamReader;

/// A certified catalog must bind both the canonical inventory and exact stored
/// bytes. The manifest digest authenticates every part before it reaches disk.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StoredSegment {
    pub segment: SegmentDescriptor,
    pub artifact: ArtifactDescriptor,
}
impl StoredSegment {
    fn validate(self, store: &ArtifactStore) -> Result<(), MetadataError> {
        validate_identity(self.segment.identity)?;
        if self.segment.identity.repository != store.repository()
            || self.segment.size != self.artifact.size
            || self.segment.digest != self.artifact.digest
        {
            return Err(MetadataError::Integrity);
        }
        Ok(())
    }
    fn key(self) -> ArtifactKey {
        key(self.segment.identity)
    }
}
fn key(identity: SegmentIdentity) -> ArtifactKey {
    ArtifactKey {
        operation: identity.operation,
        binding_digest: identity.pack_digest,
        kind: ArtifactKind::Metadata,
    }
}

impl MetadataSegment {
    /// Upload owns a segment pin. Each background file read also owns the pin,
    /// so cancellation cannot release disk admission while a read is queued.
    pub async fn upload(
        self: Arc<Self>,
        store: &ArtifactStore,
    ) -> Result<StoredSegment, MetadataError> {
        let segment = self.descriptor();
        if segment.identity.repository != store.repository() {
            return Err(MetadataError::Integrity);
        }
        let artifact = upload_file(
            self,
            store,
            key(segment.identity),
            segment.size,
            segment.digest,
        )
        .await?;
        Ok(StoredSegment { segment, artifact })
    }

    /// Admission precedes file creation and provider reads. Blocking writes,
    /// sync, hashing and SQLite open retain the spool admission through cancel.
    /// A canceled/failed download never returns a usable metadata connection.
    pub async fn download(
        root: &Path,
        budget: DiskBudget,
        store: &ArtifactStore,
        stored: StoredSegment,
        limits: MetadataLimits,
    ) -> Result<Arc<Self>, MetadataError> {
        Self::download_for_reader(root, budget, store, stored, limits, None).await
    }

    pub(in crate::packs) async fn download_for_reader(
        root: &Path,
        budget: DiskBudget,
        store: &ArtifactStore,
        stored: StoredSegment,
        limits: MetadataLimits,
        reader: Option<ReaderAdmission>,
    ) -> Result<Arc<Self>, MetadataError> {
        stored.validate(store)?;
        let admitted = download_file_for_reader(
            root,
            budget,
            store,
            stored.key(),
            stored.artifact,
            limits,
            reader,
        )
        .await?;
        tokio::task::spawn_blocking(move || {
            Ok(Arc::new(Self::open_admitted(
                admitted,
                stored.segment,
                limits.cache_kib,
            )?))
        })
        .await?
    }
}

pub(crate) trait PinnedFile: Send + Sync + 'static {
    fn path(&self) -> &Path;
}
impl PinnedFile for MetadataSegment {
    fn path(&self) -> &Path {
        MetadataSegment::path(self)
    }
}

pub(crate) async fn upload_file<T: PinnedFile>(
    owner: Arc<T>,
    store: &ArtifactStore,
    key: ArtifactKey,
    size: u64,
    digest: [u8; 32],
) -> Result<ArtifactDescriptor, MetadataError> {
    let source = tokio::task::spawn_blocking(move || {
        let file = File::open(owner.path())?;
        if file.metadata()?.len() != size {
            return Err(MetadataError::Integrity);
        }
        Ok::<_, MetadataError>(Arc::new(ReadPin {
            file: Mutex::new(file),
            _owner: owner,
            size,
        }))
    })
    .await??;
    let mut input = StreamReader::new(SegmentStream {
        source,
        offset: 0,
        job: None,
        failed: false,
    });
    Ok(store.put(key, size, digest, &mut input).await?)
}

pub(in crate::packs) async fn download_file_for_reader(
    root: &Path,
    budget: DiskBudget,
    store: &ArtifactStore,
    key: ArtifactKey,
    artifact: ArtifactDescriptor,
    limits: MetadataLimits,
    reader: Option<ReaderAdmission>,
) -> Result<AdmittedFile, MetadataError> {
    if limits.cache_kib == 0
        || limits.cache_kib > i32::MAX as u32
        || artifact.size > limits.max_file_bytes
        || limits.max_file_bytes > canopy_object_storage::external::MAX_ARTIFACT_BYTES
    {
        return Err(MetadataError::Limit);
    }
    let reservation = budget.try_reserve(artifact.size)?;
    let root = root.to_owned();
    let spool = tokio::task::spawn_blocking(move || {
        Ok::<_, MetadataError>(Arc::new(DownloadSpool {
            admitted: Mutex::new(
                AdmittedFile::new(
                    tempfile::Builder::new()
                        .prefix("canopy-metadata-download-")
                        .tempfile_in(root)?,
                    reservation,
                )
                .with_reader(reader),
            ),
        }))
    })
    .await??;
    let mut reader = store.read(key, artifact).await?;
    while let Some(bytes) = reader.next().await? {
        let spool = Arc::clone(&spool);
        tokio::task::spawn_blocking(move || {
            let mut admitted = spool
                .admitted
                .lock()
                .map_err(|_| MetadataError::Integrity)?;
            admitted.file_mut().write_all(&bytes)?;
            Ok::<_, MetadataError>(())
        })
        .await??;
    }
    tokio::task::spawn_blocking(move || {
        let spool = Arc::try_unwrap(spool).map_err(|_| MetadataError::Integrity)?;
        let admitted = spool
            .admitted
            .into_inner()
            .map_err(|_| MetadataError::Integrity)?;
        admitted.file().as_file().sync_all()?;
        Ok(admitted)
    })
    .await?
}

// Field order closes/unlinks the private file before releasing admission.
struct DownloadSpool {
    admitted: Mutex<AdmittedFile>,
}

// Unlike a bare tokio::fs::File, each pending blocking task owns its admission
// pin. Read handles have independent offsets, including concurrent uploads.
struct ReadPin<T: PinnedFile> {
    file: Mutex<File>,
    _owner: Arc<T>,
    size: u64,
}
struct SegmentStream<T: PinnedFile> {
    source: Arc<ReadPin<T>>,
    offset: u64,
    job: Option<tokio::task::JoinHandle<io::Result<Bytes>>>,
    failed: bool,
}
impl<T: PinnedFile> Stream for SegmentStream<T> {
    type Item = io::Result<Bytes>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.failed || self.offset == self.source.size {
            return Poll::Ready(None);
        }
        if self.job.is_none() {
            let source = Arc::clone(&self.source);
            let offset = self.offset;
            let length = (source.size - offset).min(64 << 10) as usize;
            self.job = Some(tokio::task::spawn_blocking(move || {
                let mut file = source
                    .file
                    .lock()
                    .map_err(|_| io::Error::other("metadata read lock poisoned"))?;
                file.seek(SeekFrom::Start(offset))?;
                let mut buffer = vec![0; length];
                file.read_exact(&mut buffer)?;
                Ok(Bytes::from(buffer))
            }));
        }
        let result = match Pin::new(self.job.as_mut().expect("scheduled read")).poll(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(result) => result,
        };
        self.job = None;
        match result.unwrap_or_else(|error| Err(io::Error::other(error))) {
            Ok(bytes) => {
                self.offset += bytes.len() as u64;
                Poll::Ready(Some(Ok(bytes)))
            }
            Err(error) => {
                self.failed = true;
                Poll::Ready(Some(Err(error)))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canceled_queued_read_keeps_file_and_admission_until_the_worker_finishes()
    -> Result<(), Box<dyn std::error::Error>> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .max_blocking_threads(1)
            .enable_all()
            .build()?;
        let (_root, segment, budget) = runtime.block_on(super::super::tests::prepared_segment())?;
        let path = segment.path().to_owned();
        let weak = Arc::downgrade(&segment);
        let charged = budget.used();
        assert!(charged > 0);
        let (ready_tx, ready_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let blocker = runtime.spawn_blocking(move || {
            ready_tx.send(()).unwrap();
            release_rx.recv().unwrap();
        });
        ready_rx.recv_timeout(std::time::Duration::from_secs(5))?;
        let mut stream = SegmentStream {
            source: Arc::new(ReadPin {
                file: Mutex::new(File::open(&path)?),
                size: segment.descriptor().size,
                _owner: segment,
            }),
            offset: 0,
            job: None,
            failed: false,
        };
        {
            let _entered = runtime.enter();
            let mut context = Context::from_waker(std::task::Waker::noop());
            assert!(Pin::new(&mut stream).poll_next(&mut context).is_pending());
        }
        assert!(stream.job.is_some());
        drop(stream);
        assert_eq!(budget.used(), charged);
        assert!(weak.upgrade().is_some());
        assert!(path.exists());
        // Always release before assertions that could unwind: a stopped
        // blocking worker must not hang runtime shutdown if this test fails.
        release_tx.send(())?;
        runtime.block_on(async {
            blocker.await?;
            tokio::time::timeout(std::time::Duration::from_secs(5), async {
                while budget.used() != 0 {
                    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                }
            })
            .await?;
            Ok::<_, Box<dyn std::error::Error>>(())
        })?;
        assert!(weak.upgrade().is_none());
        assert!(!path.exists());
        Ok(())
    }
}
