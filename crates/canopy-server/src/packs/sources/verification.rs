use super::super::metadata::MetadataError;
use super::*;
use crate::git_format::pack_index::PackIndex;
use std::path::Path;

/// Exact native artifact binding checked once for all metadata shards of a
/// physical pack. The service retains the original admitted immutable files.
/// This handle carries no canonical-body, closure or authorization certificate.
pub struct VerifiedPackBinding {
    pub(super) binding: NativePackDescriptor,
    pub(super) index: PackIndex,
}
impl VerifiedPackBinding {
    pub fn index(&self) -> &PackIndex {
        &self.index
    }
    pub fn verify_source(&self, source: SourceRecord) -> Result<(), IndexError> {
        let expected = self.binding;
        source.validate(expected.repository, expected.format)?;
        let identity = source.metadata.segment.identity;
        if source.native() != expected {
            return Err(IndexError::Integrity);
        }
        let first = self
            .index
            .ids_from(identity.first_ordinal)
            .map_err(MetadataError::from)?
            .next()
            .transpose()
            .map_err(MetadataError::from)?;
        let last_ordinal = identity.first_ordinal + identity.object_count - 1;
        let last = self
            .index
            .ids_from(last_ordinal)
            .map_err(MetadataError::from)?
            .next()
            .transpose()
            .map_err(MetadataError::from)?;
        if first != Some(source.metadata.segment.first_oid)
            || last != Some(source.metadata.segment.last_oid)
        {
            return Err(IndexError::Integrity);
        }
        Ok(())
    }
}
impl SourceRecord {
    /// Validates a directory's preferred-source pointer against the exact
    /// verified metadata shard. This is independent of authorization/closure.
    pub fn verify_directory_entry(
        self,
        segment: &super::super::metadata::MetadataSegment,
        entry: super::super::directory::DirectoryEntry,
    ) -> Result<(), IndexError> {
        self.verify_directory_entries(segment, &[entry])
    }
    pub fn verify_directory_entries(
        self,
        segment: &super::super::metadata::MetadataSegment,
        entries: &[super::super::directory::DirectoryEntry],
    ) -> Result<(), IndexError> {
        if entries.len() > super::super::metadata::PAGE_OBJECTS {
            return Err(IndexError::Limit);
        }
        let descriptor = self.metadata.segment;
        self.validate(descriptor.identity.repository, descriptor.identity.format)?;
        if segment.descriptor() != descriptor {
            return Err(IndexError::Integrity);
        }
        for entry in entries {
            if entry.source != self.key()
                || entry.location_version == 0
                || entry.location_version > i64::MAX as u64
                || entry.header.object.oid.format() != descriptor.identity.format
                || entry.header.object.oid < descriptor.first_oid
                || entry.header.object.oid > descriptor.last_oid
            {
                return Err(IndexError::Integrity);
            }
        }
        let ids: Vec<_> = entries
            .iter()
            .map(|entry| entry.header.object.oid)
            .collect();
        let headers = segment.headers(&ids)?;
        if headers.len() != entries.len() {
            return Err(IndexError::Integrity);
        }
        for (entry, actual) in entries.iter().zip(headers) {
            if actual != Some(entry.header) {
                return Err(MetadataError::IdentityConflict.into());
            }
        }
        Ok(())
    }
    /// Bounded blocking validation of exact local artifact bytes, native pack
    /// header/trailer, checked index and shard endpoints. The service must pin
    /// admitted immutable files and run this on its blocking verification pool.
    /// This does not decode objects or certify canonical body/graph closure.
    pub fn verify_native_files(
        self,
        pack_path: &Path,
        index_path: &Path,
    ) -> Result<VerifiedPackBinding, IndexError> {
        let identity = self.metadata.segment.identity;
        self.validate(identity.repository, identity.format)?;
        let verified = self.native().verify_files(pack_path, index_path)?;
        verified.verify_source(self)?;
        Ok(verified)
    }
}

/// Constant-space exact ordinal partition check. Feed shards in native ordinal
/// order after independently verifying each immutable metadata file. Equal row
/// counts, pack membership and this partition check are not closure proofs.
pub struct PackCoverage {
    first: SourceRecord,
    next_ordinal: u32,
    last_oid: Option<ObjectId>,
    poisoned: bool,
}
impl PackCoverage {
    pub fn new(first: SourceRecord) -> Result<Self, IndexError> {
        let identity = first.metadata.segment.identity;
        first.validate(identity.repository, identity.format)?;
        Ok(Self {
            first,
            next_ordinal: 0,
            last_oid: None,
            poisoned: false,
        })
    }
    pub fn add(&mut self, source: SourceRecord) -> Result<(), IndexError> {
        if self.poisoned {
            return Err(IndexError::Integrity);
        }
        self.poisoned = true;
        let expected = self.first.metadata.segment.identity;
        source.validate(expected.repository, expected.format)?;
        let segment = source.metadata.segment;
        let identity = segment.identity;
        if source.pack != self.first.pack
            || source.index != self.first.index
            || source.pack_object_count != self.first.pack_object_count
            || identity.operation != expected.operation
            || identity.git_checksum != expected.git_checksum
            || identity.first_ordinal != self.next_ordinal
            || self.last_oid.is_some_and(|last| last >= segment.first_oid)
        {
            return Err(IndexError::Integrity);
        }
        self.next_ordinal = identity
            .first_ordinal
            .checked_add(identity.object_count)
            .ok_or(IndexError::Integrity)?;
        self.last_oid = Some(segment.last_oid);
        self.poisoned = false;
        Ok(())
    }
    pub fn finish(self) -> Result<(), IndexError> {
        if self.poisoned
            || self.last_oid.is_none()
            || self.next_ordinal != self.first.pack_object_count
        {
            return Err(IndexError::Integrity);
        }
        Ok(())
    }
}
