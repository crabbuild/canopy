use super::{CanonicalObject, EdgeSink, ObjectReadError};
use crate::{
    ObjectId, ObjectKind,
    packs::metadata::{AdmittedFile, MetadataError, PAGE_OBJECTS, TypedEdge, kind_code},
};
use cellule_ltx::DiskBudget;
use std::{
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

/// A decoded canonical witness with privately owned, admitted dependencies.
/// Construction is restricted to a successful complete native inspection.
/// It is not a physical-pack or graph-closure certificate.
pub struct VerifiedObject {
    object: CanonicalObject,
    file: Option<AdmittedFile>,
    bytes: u64,
    digest: [u8; 32],
}
impl VerifiedObject {
    pub fn object(&self) -> CanonicalObject {
        self.object
    }

    /// Replay exact occurrence bytes in bounded pages. SQL deduplicates typed
    /// edges; the digest detects changed scratch bytes before transaction commit.
    pub(in crate::packs) fn replay(
        &mut self,
        mut append: impl FnMut(&[TypedEdge]) -> Result<(), MetadataError>,
    ) -> Result<(), MetadataError> {
        let Some(file) = self.file.as_mut() else {
            return if self.bytes == 0 && self.digest == *blake3::hash(&[]).as_bytes() {
                Ok(())
            } else {
                Err(MetadataError::Integrity)
            };
        };
        let width = self.object.oid.len();
        let stride = width + 1;
        if !self.bytes.is_multiple_of(stride as u64)
            || file.file().as_file().metadata()?.len() != self.bytes
        {
            return Err(MetadataError::Integrity);
        }
        let input = file.file_mut().as_file_mut();
        input.seek(SeekFrom::Start(0))?;
        let mut buffer = [0; PAGE_OBJECTS * 33];
        let mut remaining = self.bytes;
        let mut hash = blake3::Hasher::new();
        while remaining != 0 {
            let size = remaining.min((PAGE_OBJECTS * stride) as u64) as usize;
            input.read_exact(&mut buffer[..size])?;
            hash.update(&buffer[..size]);
            let mut edges = Vec::with_capacity(size / stride);
            for record in buffer[..size].chunks_exact(stride) {
                let child =
                    ObjectId::try_from(&record[..width]).map_err(|_| MetadataError::Integrity)?;
                let expected_kind = match record[width] {
                    1 => ObjectKind::Blob,
                    2 => ObjectKind::Tree,
                    3 => ObjectKind::Commit,
                    4 => ObjectKind::Tag,
                    _ => return Err(MetadataError::Integrity),
                };
                if child.is_zero() {
                    return Err(MetadataError::Integrity);
                }
                edges.push(TypedEdge {
                    child,
                    expected_kind,
                });
            }
            append(&edges)?;
            remaining -= size as u64;
        }
        if hash.finalize().as_bytes() != &self.digest {
            return Err(MetadataError::Integrity);
        }
        Ok(())
    }
}

struct State {
    file: Option<AdmittedFile>,
    bytes: u64,
    hash: blake3::Hasher,
}
pub(super) struct DiskSink {
    parent: ObjectId,
    root: PathBuf,
    budget: DiskBudget,
    limit: u64,
    failed: bool,
    state: Arc<Mutex<State>>,
}
impl DiskSink {
    pub(super) fn new(parent: ObjectId, root: &Path, budget: DiskBudget, limit: u64) -> Self {
        Self {
            parent,
            root: root.to_owned(),
            budget,
            limit,
            failed: false,
            state: Arc::new(Mutex::new(State {
                file: None,
                bytes: 0,
                hash: blake3::Hasher::new(),
            })),
        }
    }
    pub(super) fn complete(
        self,
        object: CanonicalObject,
    ) -> Result<VerifiedObject, ObjectReadError> {
        if self.failed || object.oid != self.parent {
            return Err(ObjectReadError::Malformed);
        }
        let state = Arc::try_unwrap(self.state)
            .map_err(|_| ObjectReadError::Malformed)?
            .into_inner()
            .map_err(|_| ObjectReadError::Malformed)?;
        Ok(VerifiedObject {
            object,
            file: state.file,
            bytes: state.bytes,
            digest: *state.hash.finalize().as_bytes(),
        })
    }
}
impl EdgeSink for DiskSink {
    async fn append(
        &mut self,
        parent: ObjectId,
        edges: &[TypedEdge],
    ) -> Result<(), ObjectReadError> {
        if self.failed {
            return Err(ObjectReadError::Malformed);
        }
        // Set before any await. A canceled append cannot produce a witness;
        // queued blocking jobs retain the file and admission independently.
        self.failed = true;
        if parent != self.parent || edges.is_empty() || edges.len() > PAGE_OBJECTS {
            return Err(ObjectReadError::Malformed);
        }
        let mut bytes = Vec::with_capacity(edges.len() * (parent.len() + 1));
        for edge in edges {
            if edge.child.format() != parent.format() || edge.child.is_zero() {
                return Err(ObjectReadError::Malformed);
            }
            bytes.extend_from_slice(&edge.child);
            bytes.push(kind_code(edge.expected_kind));
        }
        let state = Arc::clone(&self.state);
        let root = self.root.clone();
        let budget = self.budget.clone();
        let limit = self.limit;
        tokio::task::spawn_blocking(move || {
            let mut state = state.lock().map_err(|_| ObjectReadError::Malformed)?;
            let next = state
                .bytes
                .checked_add(bytes.len() as u64)
                .filter(|size| *size <= limit)
                .ok_or(ObjectReadError::TooLarge)?;
            if state.file.is_none() {
                let reservation = budget
                    .try_reserve(bytes.len() as u64)
                    .map_err(std::io::Error::other)?;
                let file = tempfile::Builder::new()
                    .prefix("canopy-verified-edges-")
                    .tempfile_in(root)?;
                state.file = Some(AdmittedFile::new(file, reservation));
            } else {
                state
                    .file
                    .as_mut()
                    .ok_or(ObjectReadError::Malformed)?
                    .reservation()
                    .try_grow(bytes.len() as u64)
                    .map_err(std::io::Error::other)?;
            }
            state
                .file
                .as_mut()
                .ok_or(ObjectReadError::Malformed)?
                .file_mut()
                .as_file_mut()
                .write_all(&bytes)?;
            state.hash.update(&bytes);
            state.bytes = next;
            Ok::<_, ObjectReadError>(())
        })
        .await??;
        self.failed = false;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ObjectFormat,
        packs::metadata::{
            MetadataBuilder,
            tests::{fixture, limits},
        },
    };
    use std::future::Future;
    type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

    #[test]
    fn canceled_queued_write_retains_file_admission_and_cannot_complete() -> Result {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .max_blocking_threads(1)
            .enable_all()
            .build()?;
        let root = tempfile::TempDir::new()?;
        let budget = DiskBudget::new(100);
        let parent = crate::object_id(ObjectFormat::Sha256, ObjectKind::Tree, b"tree");
        let child = crate::object_id(ObjectFormat::Sha256, ObjectKind::Blob, b"blob");
        let edge = [TypedEdge {
            child,
            expected_kind: ObjectKind::Blob,
        }];
        let mut sink = DiskSink::new(parent, root.path(), budget.clone(), 100);
        runtime.block_on(sink.append(parent, &edge))?;
        assert_eq!(budget.used(), 33);
        let entered = Arc::new(tokio::sync::Notify::new());
        let worker_entered = entered.clone();
        let (release, wait) = std::sync::mpsc::channel();
        let _blocker = runtime.spawn_blocking(move || {
            worker_entered.notify_one();
            wait.recv().expect("release blocker");
        });
        runtime.block_on(entered.notified());
        runtime.block_on(async {
            let mut pending = std::pin::pin!(sink.append(parent, &edge));
            std::future::poll_fn(|context| {
                assert!(pending.as_mut().poll(context).is_pending());
                std::task::Poll::Ready(())
            })
            .await;
            // Drop the confirmed queued append here, before releasing its worker.
        });
        assert!(
            sink.complete(CanonicalObject {
                oid: parent,
                kind: ObjectKind::Tree,
                size: 4,
                digest: [0; 32]
            })
            .is_err()
        );
        assert_eq!(budget.used(), 33);
        assert_eq!(std::fs::read_dir(root.path())?.count(), 1);
        release.send(())?;
        runtime.block_on(runtime.spawn_blocking(|| ()))?;
        assert_eq!(budget.used(), 0);
        assert_eq!(std::fs::read_dir(root.path())?.count(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn late_scratch_corruption_rolls_back_entire_witness_batch_and_poison_sealing() -> Result
    {
        for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
            let fixture = fixture(format, 1600).await?;
            let root = tempfile::TempDir::new()?;
            let budget = DiskBudget::new(128 << 20);
            let mut builder =
                MetadataBuilder::new(root.path(), budget.clone(), fixture.identity, limits())?;
            let tree = fixture
                .objects
                .values()
                .find(|(object, _)| object.kind == ObjectKind::Tree)
                .ok_or("tree")?
                .0
                .oid;
            let blob = fixture
                .objects
                .values()
                .find(|(object, _)| object.kind == ObjectKind::Blob)
                .ok_or("blob")?
                .0
                .oid;
            let mut verifier = super::super::CanonicalVerifier::new(fixture.root.path(), format)?;
            let first = verifier
                .inspect_to_disk(blob, root.path(), budget.clone(), 1 << 20)
                .await?;
            assert!(first.file.is_none());
            let mut corrupt = verifier
                .inspect_to_disk(tree, root.path(), budget.clone(), 1 << 20)
                .await?;
            let file = corrupt
                .file
                .as_mut()
                .ok_or("spool")?
                .file_mut()
                .as_file_mut();
            file.seek(SeekFrom::Start(0))?;
            let mut byte = [0];
            file.read_exact(&mut byte)?;
            byte[0] ^= 1; // valid OID bytes; detection occurs after all replay pages.
            file.seek(SeekFrom::Start(0))?;
            file.write_all(&byte)?;
            verifier.finish().await?;
            assert!(matches!(
                builder.put_verified_batch(vec![first, corrupt]),
                Err(MetadataError::Integrity)
            ));
            let path = std::fs::read_dir(root.path())?
                .find_map(|entry| {
                    entry
                        .ok()
                        .filter(|entry| {
                            entry
                                .file_name()
                                .to_string_lossy()
                                .starts_with("canopy-metadata-")
                        })
                        .map(|entry| entry.path())
                })
                .ok_or("metadata")?;
            let database = rusqlite::Connection::open_with_flags(
                path,
                rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
            )?;
            for table in ["objects", "object_edges"] {
                let count: u64 =
                    database.query_row(&format!("SELECT count(*) FROM {table}"), [], |row| {
                        row.get(0)
                    })?;
                assert_eq!(count, 0);
            }
            drop(database);
            assert!(matches!(
                builder.put_objects(&[fixture.objects[&blob].0]),
                Err(MetadataError::Integrity)
            ));
            assert!(matches!(
                builder.seal(&fixture.index),
                Err(MetadataError::Integrity)
            ));
            assert_eq!(budget.used(), 0);
            assert_eq!(std::fs::read_dir(root.path())?.count(), 0);
        }
        Ok(())
    }
}
