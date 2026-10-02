//! Immutable canonical object directory runs. A published leveled index chooses
//! at most one run per nonoverlapping level, plus bounded overlapping level 0.
//! Runs bind source descriptors, but do not themselves attest graph closure.

use super::metadata::{
    self, AdmittedFile, MetadataError, MetadataLimits, MetadataSegment, ObjectHeader, PAGE_OBJECTS,
    ReaderAdmission, digest, file_digest, fold_header, header,
    transport::{PinnedFile, download_file_for_reader, upload_file},
};
use crate::{ObjectFormat, ObjectId};
use canopy_object_storage::artifact::{
    ArtifactDescriptor, ArtifactKey, ArtifactKind, ArtifactStore,
};
use cellule_ltx::{DiskBudget, DiskReservation};
use rusqlite::{Connection, OpenFlags, OptionalExtension, params};
use std::{
    path::Path,
    sync::{Arc, Mutex},
};

mod coverage;
pub use coverage::RunCoverage;
mod writer;
pub use writer::DirectoryBuilder;
mod partition;
pub use partition::DirectoryPartitioner;
pub mod index;
pub mod snapshot;

pub const RUN_TARGET_BYTES: u64 = 64 << 20;
const APPLICATION_ID: u32 = 1_128_353_358;
const SCHEMA: &str = include_str!("schema.sql");

/// Identifies a metadata artifact incarnation, not only its content digest.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct SegmentKey {
    pub operation: [u8; 16],
    pub digest: [u8; 32],
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DirectoryEntry {
    pub header: ObjectHeader,
    pub source: SegmentKey,
    /// Changes only when a trusted compaction switches physical placement.
    /// Canonical inventory digests deliberately exclude this version.
    pub location_version: u64,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RunDescriptor {
    pub repository: [u8; 16],
    pub operation: [u8; 16],
    pub format: ObjectFormat,
    pub object_count: u64,
    pub first_oid: ObjectId,
    pub last_oid: ObjectId,
    pub inventory_digest: [u8; 32],
    pub size: u64,
    pub digest: [u8; 32],
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StoredRun {
    /// Complete authenticated physical file identity; projections never change it.
    pub run: RunDescriptor,
    pub artifact: ArtifactDescriptor,
    /// Exact contiguous OID coverage indexed in this catalog. Always present,
    /// including whole-file runs; no legacy/sliced representation switch.
    pub coverage: RunCoverage,
}
impl RunDescriptor {
    pub fn validate(self) -> Result<(), MetadataError> {
        if self.object_count == 0
            || self.object_count > i64::MAX as u64
            || self.first_oid.format() != self.format
            || self.last_oid.format() != self.format
            || self.first_oid.is_zero()
            || self.first_oid > self.last_oid
            || (self.object_count == 1) != (self.first_oid == self.last_oid)
            || self.size < 12 << 10
            || !self.size.is_multiple_of(4096)
            || self.size > canopy_object_storage::external::MAX_ARTIFACT_BYTES
        {
            return Err(MetadataError::Integrity);
        }
        Ok(())
    }
    fn key(self) -> ArtifactKey {
        ArtifactKey {
            operation: self.operation,
            binding_digest: self.digest,
            kind: ArtifactKind::DirectoryRun,
        }
    }
}
impl StoredRun {
    pub fn validate(self) -> Result<(), MetadataError> {
        self.run.validate()?;
        self.coverage.validate(self.run)?;
        if self.run.size != self.artifact.size || self.run.digest != self.artifact.digest {
            return Err(MetadataError::Integrity);
        }
        Ok(())
    }
}
fn entry(row: &rusqlite::Row<'_>) -> rusqlite::Result<DirectoryEntry> {
    Ok(DirectoryEntry {
        header: header(row)?,
        source: SegmentKey {
            operation: row
                .get::<_, Vec<u8>>(6)?
                .try_into()
                .map_err(|_| rusqlite::Error::InvalidQuery)?,
            digest: digest(row.get(7)?)?,
        },
        location_version: metadata::unsigned(row.get(8)?)?,
    })
}
pub(super) fn inventory_seed(format: ObjectFormat) -> [u8; 32] {
    let mut hash = blake3::Hasher::new();
    hash.update(b"canopy.directory.v1\0");
    hash.update(&[format.bytes() as u8]);
    *hash.finalize().as_bytes()
}

/// A verified private file, bounded SQLite page cache, and owned disk charge.
/// The connection closes before file cleanup; cancellation retains file pins.
pub struct DirectoryRun {
    connection: Mutex<Connection>,
    admitted: AdmittedFile,
    descriptor: RunDescriptor,
}
impl PinnedFile for DirectoryRun {
    fn path(&self) -> &Path {
        self.admitted.file().path()
    }
}
impl DirectoryRun {
    pub fn open(
        file: tempfile::NamedTempFile,
        reservation: DiskReservation,
        descriptor: RunDescriptor,
        cache_kib: u32,
    ) -> Result<Self, MetadataError> {
        Self::open_admitted(AdmittedFile::new(file, reservation), descriptor, cache_kib)
    }
    fn open_admitted(
        mut admitted: AdmittedFile,
        descriptor: RunDescriptor,
        cache_kib: u32,
    ) -> Result<Self, MetadataError> {
        descriptor.validate()?;
        if cache_kib == 0
            || cache_kib > i32::MAX as u32
            || descriptor.size > admitted.reservation().bytes()
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
        let stored = connection.query_row("SELECT repository_id,operation_id,object_format,object_count,first_oid,last_oid,inventory_digest FROM directory_identity WHERE singleton=1", [], |row| {
            Ok((row.get::<_,Vec<u8>>(0)?,row.get::<_,Vec<u8>>(1)?,row.get::<_,String>(2)?,row.get::<_,u64>(3)?,row.get::<_,Vec<u8>>(4)?,row.get::<_,Vec<u8>>(5)?,row.get::<_,Vec<u8>>(6)?))
        })?;
        if stored
            != (
                descriptor.repository.to_vec(),
                descriptor.operation.to_vec(),
                descriptor.format.as_str().to_owned(),
                descriptor.object_count,
                descriptor.first_oid.to_vec(),
                descriptor.last_oid.to_vec(),
                descriptor.inventory_digest.to_vec(),
            )
        {
            return Err(MetadataError::Integrity);
        }
        Ok(Self {
            connection: Mutex::new(connection),
            admitted,
            descriptor,
        })
    }
    pub fn descriptor(&self) -> RunDescriptor {
        self.descriptor
    }
    pub fn path(&self) -> &Path {
        PinnedFile::path(self)
    }
    fn connection(&self) -> Result<std::sync::MutexGuard<'_, Connection>, MetadataError> {
        self.connection.lock().map_err(|_| MetadataError::Integrity)
    }
    pub fn find(&self, oid: ObjectId) -> Result<Option<DirectoryEntry>, MetadataError> {
        Ok(self.find_batch(&[oid])?.pop().flatten())
    }
    /// One prepared indexed statement and connection lock for a bounded batch.
    pub fn find_batch(
        &self,
        ids: &[ObjectId],
    ) -> Result<Vec<Option<DirectoryEntry>>, MetadataError> {
        if ids.len() > PAGE_OBJECTS {
            return Err(MetadataError::Limit);
        }
        let connection = self.connection()?;
        let mut statement = connection.prepare_cached("SELECT oid,kind,size,digest,edge_count,edge_digest,source_operation,source_digest,location_version FROM objects WHERE oid=?1")?;
        ids.iter()
            .map(|oid| {
                if oid.format() != self.descriptor.format
                    || *oid < self.descriptor.first_oid
                    || *oid > self.descriptor.last_oid
                {
                    return Ok(None);
                }
                Ok(statement.query_row([oid.as_ref()], entry).optional()?)
            })
            .collect()
    }
    pub fn entries_after(
        &self,
        after: Option<ObjectId>,
    ) -> Result<Vec<DirectoryEntry>, MetadataError> {
        if after.is_some_and(|oid| oid.format() != self.descriptor.format) {
            return Err(MetadataError::Integrity);
        }
        let connection = self.connection()?;
        let mut statement = connection.prepare_cached("SELECT oid,kind,size,digest,edge_count,edge_digest,source_operation,source_digest,location_version FROM objects WHERE oid>?1 ORDER BY oid LIMIT ?2")?;
        Ok(statement
            .query_map(
                params![
                    after.as_ref().map_or(&[][..], AsRef::<[u8]>::as_ref),
                    PAGE_OBJECTS as i64
                ],
                entry,
            )?
            .collect::<rusqlite::Result<_>>()?)
    }
    #[cfg(test)]
    pub(in crate::packs) fn verify_inventory(&self) -> Result<u64, MetadataError> {
        self.verify_coverage(self.descriptor.coverage())
    }
    pub async fn upload(
        self: Arc<Self>,
        store: &ArtifactStore,
    ) -> Result<StoredRun, MetadataError> {
        let run = self.descriptor;
        if run.repository != store.repository() {
            return Err(MetadataError::Integrity);
        }
        let artifact = upload_file(self, store, run.key(), run.size, run.digest).await?;
        Ok(StoredRun {
            run,
            artifact,
            coverage: run.coverage(),
        })
    }
    pub async fn download(
        root: &Path,
        budget: DiskBudget,
        store: &ArtifactStore,
        stored: StoredRun,
        limits: MetadataLimits,
    ) -> Result<Arc<Self>, MetadataError> {
        Self::download_for_reader(root, budget, store, stored, limits, None).await
    }
    pub(in crate::packs) async fn download_for_reader(
        root: &Path,
        budget: DiskBudget,
        store: &ArtifactStore,
        stored: StoredRun,
        limits: MetadataLimits,
        reader: Option<ReaderAdmission>,
    ) -> Result<Arc<Self>, MetadataError> {
        stored.validate()?;
        if stored.run.repository != store.repository() {
            return Err(MetadataError::Integrity);
        }
        let admitted = download_file_for_reader(
            root,
            budget,
            store,
            stored.run.key(),
            stored.artifact,
            limits,
            reader,
        )
        .await?;
        tokio::task::spawn_blocking(move || {
            Ok(Arc::new(Self::open_admitted(
                admitted,
                stored.run,
                limits.cache_kib,
            )?))
        })
        .await?
    }
}

#[cfg(test)]
pub(in crate::packs) mod tests;
