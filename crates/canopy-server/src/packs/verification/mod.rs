//! Canonical decoded-object inspection with bounded typed edge emission.
//! This is not a physical pack, graph closure or publication certificate.

use super::metadata::CanonicalObject;
pub use crate::git_objects::{EdgeSink, ObjectReadError};
use crate::{ObjectFormat, ObjectId, git_objects::GitObjects};
use std::path::Path;

mod spool;
pub use spool::VerifiedObject;
pub(super) mod physical;
pub use physical::{
    PhysicalError, PhysicalLimits, PhysicalPackWitness, PhysicalPartition, PhysicalVerifier,
};

/// The service retains its admitted private native workspace and process
/// resources. A physical-pack verifier must isolate the exact verified pack,
/// without alternates, before using these decoded-object witnesses.
pub struct CanonicalVerifier {
    native: GitObjects,
    format: ObjectFormat,
}
impl CanonicalVerifier {
    pub fn new(git_dir: &Path, format: ObjectFormat) -> Result<Self, ObjectReadError> {
        Ok(Self {
            native: GitObjects::batch(git_dir)?,
            format,
        })
    }
    /// Sink output remains private until this completes. Discard the whole
    /// affected preparation after an error/cancellation. Once native inspection
    /// starts, either event poisons the actor, including a canceled sink write.
    pub async fn inspect(
        &mut self,
        oid: ObjectId,
        sink: &mut impl EdgeSink,
    ) -> Result<CanonicalObject, ObjectReadError> {
        if oid.format() != self.format || oid.is_zero() {
            return Err(ObjectReadError::Malformed);
        }
        self.native.inspect_graph(oid, sink).await
    }
    pub async fn finish(self) -> Result<(), ObjectReadError> {
        self.native.finish().await
    }

    /// Retain dependency occurrences on admitted disk until the complete native
    /// frame and canonical hashes have been verified. Blobs create no spool.
    /// This witness still requires physical-pack isolation and closure checks.
    pub async fn inspect_to_disk(
        &mut self,
        oid: ObjectId,
        root: &Path,
        budget: cellule_ltx::DiskBudget,
        max_edge_bytes: u64,
    ) -> Result<VerifiedObject, ObjectReadError> {
        let mut sink = spool::DiskSink::new(oid, root, budget, max_edge_bytes);
        let object = self.inspect(oid, &mut sink).await?;
        sink.complete(object)
    }

    /// Reuse one dependency file across an admitted physical-verification page.
    /// Each witness retains its exact immutable range and complete digest.
    async fn inspect_to_spool(
        &mut self,
        oid: ObjectId,
        spool: &spool::EdgeSpool,
        max_edge_bytes: u64,
    ) -> Result<VerifiedObject, ObjectReadError> {
        let mut sink = spool.sink(oid, max_edge_bytes)?;
        let object = self.inspect(oid, &mut sink).await?;
        sink.complete(object)
    }
}

#[cfg(test)]
mod tests;
