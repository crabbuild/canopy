//! Immutable source bindings share the directory's bounded path-copy tree.
//! Membership binds artifact incarnations; canonical bodies, typed closure,
//! authorization, retained-root pins and Cell publication remain separate proofs.

use super::{
    directory::{
        SegmentKey,
        index::{self, IndexError, IndexKey, IndexRecord, NodeRef, RangeIndex},
    },
    metadata::{SegmentDescriptor, SegmentIdentity, StoredSegment},
};
use crate::{ObjectFormat, ObjectId};
use canopy_object_storage::artifact::{ArtifactDescriptor, ArtifactKey, ArtifactKind};

mod codec;
mod verification;
pub use verification::{PackCoverage, VerifiedPackBinding};
mod native;
pub use native::NativePackDescriptor;
mod resolve;
pub use resolve::{ResolvedSource, SourceLoader};

pub type SourceIndex = RangeIndex<SourceRecord>;
pub type SourceRoot = NodeRef<SourceRecord>;
pub const SOURCE_FANOUT: usize = 128;

/// One metadata shard and its exact native pack/index artifact incarnations.
/// Several shards may bind the same pack; coverage requires an exact partition.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SourceRecord {
    pub metadata: StoredSegment,
    pub pack: ArtifactDescriptor,
    pub index: ArtifactDescriptor,
    pub pack_object_count: u32,
}
impl SourceRecord {
    /// Shared pack/index context, available before metadata shards are built.
    /// This is a runtime value; the source/catalog wire contract is unchanged.
    pub fn native(self) -> NativePackDescriptor {
        let identity = self.metadata.segment.identity;
        NativePackDescriptor {
            repository: identity.repository,
            operation: identity.operation,
            format: identity.format,
            git_checksum: identity.git_checksum,
            object_count: self.pack_object_count,
            pack: self.pack,
            index: self.index,
        }
    }
    pub fn key(self) -> SegmentKey {
        SegmentKey {
            operation: self.metadata.segment.identity.operation,
            digest: self.metadata.artifact.digest,
        }
    }
    pub fn pack_key(self) -> ArtifactKey {
        self.artifact_key(ArtifactKind::Pack)
    }
    pub fn index_key(self) -> ArtifactKey {
        self.artifact_key(ArtifactKind::Index)
    }
    pub fn metadata_key(self) -> ArtifactKey {
        self.artifact_key(ArtifactKind::Metadata)
    }
    fn artifact_key(self, kind: ArtifactKind) -> ArtifactKey {
        ArtifactKey {
            operation: self.metadata.segment.identity.operation,
            binding_digest: self.pack.digest,
            kind,
        }
    }
    pub fn validate(self, repository: [u8; 16], format: ObjectFormat) -> Result<(), IndexError> {
        let segment = self.metadata.segment;
        let identity = segment.identity;
        let end = identity
            .first_ordinal
            .checked_add(identity.object_count)
            .ok_or(IndexError::Integrity)?;
        self.native().validate(repository, format)?;
        if identity.repository != repository
            || identity.format != format
            || identity.git_checksum.format() != format
            || identity.git_checksum.is_zero()
            || identity.object_count == 0
            || end > self.pack_object_count
            || identity.pack_digest != self.pack.digest
            || segment.size != self.metadata.artifact.size
            || segment.digest != self.metadata.artifact.digest
            || segment.size < 4096
            || !segment.size.is_multiple_of(4096)
            || segment.edge_count > i64::MAX as u64
            || segment.first_oid.format() != format
            || segment.last_oid.format() != format
            || segment.first_oid.is_zero()
            || segment.first_oid > segment.last_oid
            || (identity.object_count == 1) != (segment.first_oid == segment.last_oid)
            || self.metadata.artifact.size > canopy_object_storage::external::MAX_ARTIFACT_BYTES
        {
            return Err(IndexError::Integrity);
        }
        Ok(())
    }
}

#[cfg(test)]
pub(in crate::packs) mod tests;
