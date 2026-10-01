//! Checked, immutable SQLite metadata shards with bounded buffers/cache.
//!
//! A shard covers an exact contiguous ordinal slice of a checked native index.
//! Shards can therefore split very large physical packs without one unbounded
//! metadata file. Catalog verification must cover every ordinal exactly once.

use crate::{ObjectFormat, ObjectId, ObjectKind, git_format::pack_index::PackIndex};
use cellule_ltx::{DiskBudget, DiskReservation, LtxError};
use rusqlite::{Connection, OpenFlags, OptionalExtension, params};
use std::{
    fs::File,
    io::{self, Read},
    path::Path,
    sync::Mutex,
};

mod writer;
pub use writer::MetadataBuilder;
pub(super) mod transport;
pub use transport::StoredSegment;

pub const PAGE_OBJECTS: usize = 512;
const APPLICATION_ID: u32 = 1_128_353_357;
const SCHEMA: &str = include_str!("schema.sql");

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CanonicalObject {
    pub oid: ObjectId,
    pub kind: ObjectKind,
    pub size: u64,
    pub digest: [u8; 32],
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ObjectHeader {
    pub object: CanonicalObject,
    pub edge_count: u64,
    pub edge_digest: [u8; 32],
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TypedEdge {
    pub child: ObjectId,
    pub expected_kind: ObjectKind,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SegmentIdentity {
    pub repository: [u8; 16],
    pub operation: [u8; 16],
    pub format: ObjectFormat,
    pub pack_digest: [u8; 32],
    pub git_checksum: ObjectId,
    pub first_ordinal: u32,
    pub object_count: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SegmentDescriptor {
    pub identity: SegmentIdentity,
    pub edge_count: u64,
    pub inventory_digest: [u8; 32],
    pub first_oid: ObjectId,
    pub last_oid: ObjectId,
    pub size: u64,
    pub digest: [u8; 32],
}

#[derive(Clone, Copy, Debug)]
pub struct MetadataLimits {
    /// One shard's main database; scratch reserves 3x for rollback journals.
    pub max_file_bytes: u64,
    pub cache_kib: u32,
}
impl Default for MetadataLimits {
    fn default() -> Self {
        Self {
            max_file_bytes: 256 << 20,
            cache_kib: 8 << 10,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum MetadataError {
    #[error("metadata I/O failed")]
    Io(#[from] io::Error),
    #[error("metadata SQLite operation failed")]
    Sql(#[source] rusqlite::Error),
    #[error("metadata disk admission failed")]
    Budget(#[from] LtxError),
    #[error("metadata identity, graph or artifact is invalid")]
    Integrity,
    #[error("the same Git object ID has conflicting canonical metadata")]
    IdentityConflict,
    #[error("the expected object placement is absent or changed")]
    PlacementConflict,
    #[error("metadata input exceeds its bounded batch or shard limit")]
    Limit,
    #[error("metadata task failed")]
    Task(#[from] tokio::task::JoinError),
    #[error("metadata artifact transfer failed")]
    Artifact(#[from] canopy_object_storage::artifact::ArtifactError),
}
impl From<rusqlite::Error> for MetadataError {
    fn from(error: rusqlite::Error) -> Self {
        if error.sqlite_error_code() == Some(rusqlite::ErrorCode::DiskFull) {
            Self::Limit
        } else {
            Self::Sql(error)
        }
    }
}

pub(super) fn kind_code(kind: ObjectKind) -> u8 {
    match kind {
        ObjectKind::Blob => 1,
        ObjectKind::Tree => 2,
        ObjectKind::Commit => 3,
        ObjectKind::Tag => 4,
    }
}
pub(super) fn kind(name: &str) -> rusqlite::Result<ObjectKind> {
    match name {
        "blob" => Ok(ObjectKind::Blob),
        "tree" => Ok(ObjectKind::Tree),
        "commit" => Ok(ObjectKind::Commit),
        "tag" => Ok(ObjectKind::Tag),
        _ => Err(rusqlite::Error::InvalidQuery),
    }
}
pub(super) fn oid(bytes: Vec<u8>) -> rusqlite::Result<ObjectId> {
    bytes.try_into().map_err(|_| rusqlite::Error::InvalidQuery)
}
pub(super) fn digest(bytes: Vec<u8>) -> rusqlite::Result<[u8; 32]> {
    bytes.try_into().map_err(|_| rusqlite::Error::InvalidQuery)
}
pub(super) fn unsigned(value: i64) -> rusqlite::Result<u64> {
    value.try_into().map_err(|_| rusqlite::Error::InvalidQuery)
}
pub(super) fn canonical(row: &rusqlite::Row<'_>) -> rusqlite::Result<CanonicalObject> {
    Ok(CanonicalObject {
        oid: oid(row.get(0)?)?,
        kind: kind(&row.get::<_, String>(1)?)?,
        size: unsigned(row.get(2)?)?,
        digest: digest(row.get(3)?)?,
    })
}
pub(super) fn header(row: &rusqlite::Row<'_>) -> rusqlite::Result<ObjectHeader> {
    Ok(ObjectHeader {
        object: canonical(row)?,
        edge_count: unsigned(row.get(4)?)?,
        edge_digest: digest(row.get(5)?)?,
    })
}

pub(super) fn edge_seed(parent: ObjectId) -> [u8; 32] {
    let mut hash = blake3::Hasher::new();
    hash.update(b"canopy.edges.v1\0");
    hash.update(&[parent.format().bytes() as u8]);
    hash.update(&parent);
    *hash.finalize().as_bytes()
}
pub(super) fn fold(previous: [u8; 32], ordinal: u64, record: &[u8]) -> [u8; 32] {
    let mut hash = blake3::Hasher::new();
    hash.update(&previous);
    hash.update(&ordinal.to_le_bytes());
    hash.update(record);
    *hash.finalize().as_bytes()
}
pub(super) fn inventory_seed(identity: SegmentIdentity) -> [u8; 32] {
    let mut hash = blake3::Hasher::new();
    hash.update(b"canopy.segment.v1\0");
    hash.update(&[identity.format.bytes() as u8]);
    hash.update(&identity.pack_digest);
    hash.update(&u64::from(identity.first_ordinal).to_le_bytes());
    hash.update(&u64::from(identity.object_count).to_le_bytes());
    *hash.finalize().as_bytes()
}
pub(super) fn fold_header(previous: [u8; 32], ordinal: u64, header: ObjectHeader) -> [u8; 32] {
    let mut record = [0; 113];
    let width = header.object.oid.len();
    record[..width].copy_from_slice(&header.object.oid);
    record[width] = kind_code(header.object.kind);
    record[width + 1..width + 9].copy_from_slice(&header.object.size.to_le_bytes());
    record[width + 9..width + 41].copy_from_slice(&header.object.digest);
    record[width + 41..width + 49].copy_from_slice(&header.edge_count.to_le_bytes());
    record[width + 49..width + 81].copy_from_slice(&header.edge_digest);
    fold(previous, ordinal, &record[..width + 81])
}

fn validate_identity(identity: SegmentIdentity) -> Result<(), MetadataError> {
    if identity.git_checksum.format() != identity.format
        || identity.git_checksum.is_zero()
        || identity.object_count == 0
        || identity
            .first_ordinal
            .checked_add(identity.object_count)
            .is_none()
    {
        return Err(MetadataError::Integrity);
    }
    Ok(())
}
pub(super) fn file_digest(path: &Path, expected_size: u64) -> Result<[u8; 32], MetadataError> {
    file_digest_with(path, expected_size, || Ok(()))
}
pub(super) fn file_digest_with<E: From<MetadataError>>(
    path: &Path,
    expected_size: u64,
    mut checkpoint: impl FnMut() -> Result<(), E>,
) -> Result<[u8; 32], E> {
    checkpoint()?;
    let mut file = File::open(path).map_err(MetadataError::from)?;
    if file.metadata().map_err(MetadataError::from)?.len() != expected_size {
        return Err(MetadataError::Integrity.into());
    }
    let mut hash = blake3::Hasher::new();
    let mut buffer = [0; 64 << 10];
    let mut size = 0_u64;
    loop {
        checkpoint()?;
        let count = file.read(&mut buffer).map_err(MetadataError::from)?;
        if count == 0 {
            break;
        }
        size = size.checked_add(count as u64).ok_or(MetadataError::Limit)?;
        if size > expected_size {
            return Err(MetadataError::Integrity.into());
        }
        hash.update(&buffer[..count]);
    }
    if size != expected_size {
        return Err(MetadataError::Integrity.into());
    }
    Ok(*hash.finalize().as_bytes())
}

/// Owns the verified local file and disk admission. Readers share bounded SQLite
/// lookup, never an in-memory copy of all rows. The file must stay immutable.
pub struct MetadataSegment {
    connection: Mutex<Connection>,
    admitted: AdmittedFile,
    descriptor: SegmentDescriptor,
}
// Reader slots and the private workspace follow the file through queued jobs.
pub(super) struct ReaderAdmission {
    pub(super) _slot: tokio::sync::OwnedSemaphorePermit,
    pub(super) _root: std::sync::Arc<tempfile::TempDir>,
}

// Keep file cleanup ahead of budget release on validation errors too.
pub(super) struct AdmittedFile {
    file: Option<tempfile::NamedTempFile>,
    reservation: Option<DiskReservation>,
    reader: Option<ReaderAdmission>,
    workspace: Option<std::sync::Arc<tempfile::TempDir>>,
}
impl AdmittedFile {
    pub(super) fn new(file: tempfile::NamedTempFile, reservation: DiskReservation) -> Self {
        Self {
            file: Some(file),
            reservation: Some(reservation),
            reader: None,
            workspace: None,
        }
    }
    pub(super) fn retain_workspace(&mut self, workspace: std::sync::Arc<tempfile::TempDir>) {
        self.workspace = Some(workspace);
    }
    pub(super) fn with_reader(mut self, reader: Option<ReaderAdmission>) -> Self {
        self.reader = reader;
        self
    }
    pub(super) fn file(&self) -> &tempfile::NamedTempFile {
        self.file.as_ref().expect("admitted file is owned")
    }
    pub(super) fn file_mut(&mut self) -> &mut tempfile::NamedTempFile {
        self.file.as_mut().expect("admitted file is owned")
    }
    pub(super) fn reservation(&mut self) -> &mut DiskReservation {
        self.reservation.as_mut().expect("admission is owned")
    }
    pub(super) fn clean_journal(&self) -> io::Result<()> {
        let mut path = self.file().path().as_os_str().to_owned();
        path.push("-journal");
        match std::fs::remove_file(Path::new(&path)) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error),
        }
    }
}
impl Drop for AdmittedFile {
    fn drop(&mut self) {
        let journal = self.clean_journal();
        let Some(file) = self.file.take() else {
            return;
        };
        let main = file.close();
        if let Err(error) = journal.and(main) {
            tracing::warn!(%error, "metadata cleanup failed; retaining disk admission for workspace recovery");
            if let Some(reservation) = self.reservation.take() {
                std::mem::forget(reservation);
            }
        }
    }
}
impl MetadataSegment {
    /// The caller owns/admitted the downloaded file and supplies a descriptor
    /// from a certified catalog. Bytes are hashed before SQLite opens them.
    pub fn open(
        file: tempfile::NamedTempFile,
        reservation: DiskReservation,
        descriptor: SegmentDescriptor,
        cache_kib: u32,
    ) -> Result<Self, MetadataError> {
        Self::open_admitted(AdmittedFile::new(file, reservation), descriptor, cache_kib)
    }
    fn open_admitted(
        mut admitted: AdmittedFile,
        descriptor: SegmentDescriptor,
        cache_kib: u32,
    ) -> Result<Self, MetadataError> {
        validate_identity(descriptor.identity)?;
        if cache_kib == 0
            || cache_kib > i32::MAX as u32
            || descriptor.size > admitted.reservation().bytes()
            || descriptor.first_oid.format() != descriptor.identity.format
            || descriptor.last_oid.format() != descriptor.identity.format
            || descriptor.first_oid.is_zero()
            || descriptor.first_oid > descriptor.last_oid
            || file_digest(admitted.file().path(), descriptor.size)? != descriptor.digest
        {
            return Err(MetadataError::Integrity);
        }
        let connection = Connection::open_with_flags(
            admitted.file().path(),
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;
        connection.execute_batch(
            "PRAGMA query_only=ON; PRAGMA trusted_schema=OFF; PRAGMA mmap_size=0;",
        )?;
        connection.pragma_update(None, "cache_size", -(cache_kib as i64))?;
        let app: u32 = connection.pragma_query_value(None, "application_id", |row| row.get(0))?;
        let version: u32 = connection.pragma_query_value(None, "user_version", |row| row.get(0))?;
        if app != APPLICATION_ID || version != 1 {
            return Err(MetadataError::Integrity);
        }
        let stored = connection.query_row("SELECT repository_id, operation_id, object_format, pack_digest, git_checksum, first_ordinal, object_count, edge_count, inventory_digest FROM segment_identity WHERE singleton = 1", [], |row| {
            Ok((row.get::<_, Vec<u8>>(0)?,row.get::<_, Vec<u8>>(1)?,row.get::<_, String>(2)?,row.get::<_, Vec<u8>>(3)?,row.get::<_, Vec<u8>>(4)?,row.get::<_, u32>(5)?,row.get::<_, u32>(6)?,row.get::<_, u64>(7)?,row.get::<_, Vec<u8>>(8)?))
        })?;
        let identity = descriptor.identity;
        if stored
            != (
                identity.repository.to_vec(),
                identity.operation.to_vec(),
                identity.format.as_str().into(),
                identity.pack_digest.to_vec(),
                identity.git_checksum.to_vec(),
                identity.first_ordinal,
                identity.object_count,
                descriptor.edge_count,
                descriptor.inventory_digest.to_vec(),
            )
        {
            return Err(MetadataError::Integrity);
        }
        Ok(Self {
            admitted,
            descriptor,
            connection: Mutex::new(connection),
        })
    }
    pub fn descriptor(&self) -> SegmentDescriptor {
        self.descriptor
    }
    pub fn path(&self) -> &Path {
        self.admitted.file().path()
    }
    fn connection(&self) -> Result<std::sync::MutexGuard<'_, Connection>, MetadataError> {
        self.connection.lock().map_err(|_| MetadataError::Integrity)
    }
    pub fn header(&self, oid: ObjectId) -> Result<Option<ObjectHeader>, MetadataError> {
        Ok(self.headers(&[oid])?.pop().flatten())
    }
    pub fn headers(&self, ids: &[ObjectId]) -> Result<Vec<Option<ObjectHeader>>, MetadataError> {
        if ids.len() > PAGE_OBJECTS {
            return Err(MetadataError::Limit);
        }
        let connection = self.connection()?;
        let mut statement = connection.prepare_cached(
            "SELECT oid, kind, size, digest, edge_count, edge_digest FROM objects WHERE oid = ?1",
        )?;
        ids.iter()
            .map(|oid| {
                if oid.format() != self.descriptor.identity.format {
                    return Ok(None);
                }
                Ok(statement.query_row([oid.as_ref()], header).optional()?)
            })
            .collect()
    }
    pub fn headers_after(
        &self,
        after: Option<ObjectId>,
    ) -> Result<Vec<ObjectHeader>, MetadataError> {
        if after.is_some_and(|oid| oid.format() != self.descriptor.identity.format) {
            return Err(MetadataError::Integrity);
        }
        let connection = self.connection()?;
        let mut statement = connection.prepare_cached("SELECT oid, kind, size, digest, edge_count, edge_digest FROM objects WHERE oid > ?1 ORDER BY oid LIMIT ?2")?;
        Ok(statement
            .query_map(
                params![
                    after.as_ref().map_or(&[][..], AsRef::<[u8]>::as_ref),
                    PAGE_OBJECTS as i64
                ],
                header,
            )?
            .collect::<rusqlite::Result<_>>()?)
    }
    pub fn edges_after(
        &self,
        parent: ObjectId,
        after: Option<ObjectId>,
    ) -> Result<Vec<TypedEdge>, MetadataError> {
        if parent.format() != self.descriptor.identity.format
            || after.is_some_and(|oid| oid.format() != parent.format())
        {
            return Err(MetadataError::Integrity);
        }
        let connection = self.connection()?;
        let mut statement = connection.prepare_cached("SELECT child, expected_kind FROM object_edges WHERE parent = ?1 AND child > ?2 ORDER BY child LIMIT ?3")?;
        Ok(statement
            .query_map(
                params![
                    parent.as_ref(),
                    after.as_ref().map_or(&[][..], AsRef::<[u8]>::as_ref),
                    PAGE_OBJECTS as i64
                ],
                |row| {
                    Ok(TypedEdge {
                        child: oid(row.get(0)?)?,
                        expected_kind: kind(&row.get::<_, String>(1)?)?,
                    })
                },
            )?
            .collect::<rusqlite::Result<_>>()?)
    }
}

#[cfg(test)]
pub(super) mod tests;
