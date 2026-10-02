use super::{CanonicalObject, EdgeSink, ObjectReadError};
use crate::{
    ObjectId, ObjectKind,
    packs::metadata::{AdmittedFile, MetadataError, PAGE_OBJECTS, TypedEdge, kind_code},
};
use cellule_ltx::DiskBudget;
use std::{
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

/// A decoded canonical witness with privately owned, admitted dependencies.
/// Construction is restricted to a successful complete native inspection.
/// It is not a physical-pack or graph-closure certificate.
pub struct VerifiedObject {
    object: CanonicalObject,
    range: Option<EdgeRange>,
    bytes: u64,
    digest: [u8; 32],
}
struct EdgeRange {
    storage: Arc<Mutex<Storage>>,
    offset: u64,
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
        let Some(range) = self.range.as_ref() else {
            return if self.bytes == 0 && self.digest == *blake3::hash(&[]).as_bytes() {
                Ok(())
            } else {
                Err(MetadataError::Integrity)
            };
        };
        let mut storage = range.storage.lock().map_err(|_| MetadataError::Integrity)?;
        let width = self.object.oid.len();
        let stride = width + 1;
        if storage.failed
            || self.bytes == 0
            || !self.bytes.is_multiple_of(stride as u64)
            || range
                .offset
                .checked_add(self.bytes)
                .is_none_or(|end| end > storage.bytes)
        {
            return Err(MetadataError::Integrity);
        }
        let stored_bytes = storage.bytes;
        let file = storage.file.as_mut().ok_or(MetadataError::Integrity)?;
        if file.file().as_file().metadata()?.len() != stored_bytes {
            return Err(MetadataError::Integrity);
        }
        let input = file.file_mut().as_file_mut();
        input.seek(SeekFrom::Start(range.offset))?;
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

struct Storage {
    file: Option<AdmittedFile>,
    bytes: u64,
    failed: bool,
}

/// One append-only dependency file for a bounded native-object batch. Each
/// complete witness owns an immutable range; keeping any witness or queued job
/// alive retains the complete file and its disk admission.
pub(super) struct EdgeSpool {
    root: PathBuf,
    budget: DiskBudget,
    storage: Arc<Mutex<Storage>>,
    writing: Arc<AtomicBool>,
}
impl EdgeSpool {
    pub(super) fn new(root: &Path, budget: DiskBudget) -> Self {
        Self {
            root: root.to_owned(),
            budget,
            storage: Arc::new(Mutex::new(Storage {
                file: None,
                bytes: 0,
                failed: false,
            })),
            writing: Arc::new(AtomicBool::new(false)),
        }
    }
    pub(super) fn sink(&self, parent: ObjectId, limit: u64) -> Result<DiskSink, ObjectReadError> {
        // Hold exclusivity for an entire object, including between edge pages.
        // Queued writes retain this permit after observer cancellation.
        self.writing
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| ObjectReadError::Malformed)?;
        Ok(DiskSink {
            parent,
            root: self.root.clone(),
            budget: self.budget.clone(),
            limit,
            failed: false,
            state: Arc::new(Mutex::new(State {
                storage: Arc::clone(&self.storage),
                offset: None,
                bytes: 0,
                hash: blake3::Hasher::new(),
                _writer: AppendPermit(Arc::clone(&self.writing)),
            })),
        })
    }
}
struct AppendPermit(Arc<AtomicBool>);
impl Drop for AppendPermit {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}
struct State {
    storage: Arc<Mutex<Storage>>,
    offset: Option<u64>,
    bytes: u64,
    hash: blake3::Hasher,
    _writer: AppendPermit,
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
        // A fresh private spool cannot have another writer.
        EdgeSpool::new(root, budget)
            .sink(parent, limit)
            .expect("fresh edge spool")
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
        let range = state.offset.map(|offset| EdgeRange {
            storage: Arc::clone(&state.storage),
            offset,
        });
        Ok(VerifiedObject {
            object,
            range,
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
            let storage = Arc::clone(&state.storage);
            let mut storage = storage.lock().map_err(|_| ObjectReadError::Malformed)?;
            if storage.failed {
                return Err(ObjectReadError::Malformed);
            }
            let offset = state.offset.unwrap_or(storage.bytes);
            if offset.checked_add(state.bytes) != Some(storage.bytes) {
                return Err(ObjectReadError::Malformed);
            }
            let stored_next = storage
                .bytes
                .checked_add(bytes.len() as u64)
                .ok_or(ObjectReadError::TooLarge)?;
            // A partial write cannot be adopted by a later object. Failure
            // poisons this file; all retained ranges reject replay.
            storage.failed = true;
            if storage.file.is_none() {
                let reservation = budget
                    .try_reserve(bytes.len() as u64)
                    .map_err(std::io::Error::other)?;
                let file = tempfile::Builder::new()
                    .prefix("canopy-verified-edges-")
                    .tempfile_in(root)?;
                storage.file = Some(AdmittedFile::new(file, reservation));
            } else {
                storage
                    .file
                    .as_mut()
                    .ok_or(ObjectReadError::Malformed)?
                    .reservation()
                    .try_grow(bytes.len() as u64)
                    .map_err(std::io::Error::other)?;
            }
            let stored_bytes = storage.bytes;
            let file = storage.file.as_mut().ok_or(ObjectReadError::Malformed)?;
            if file.file().as_file().metadata()?.len() != stored_bytes {
                return Err(ObjectReadError::Malformed);
            }
            let output = file.file_mut().as_file_mut();
            output.seek(SeekFrom::Start(stored_bytes))?;
            output.write_all(&bytes)?;
            storage.bytes = stored_next;
            storage.failed = false;
            state.offset = Some(offset);
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
mod tests;
