//! Admitted operation-local graph closure. Historical graph rows stay in
//! immutable metadata; only incoming objects and queried base headers enter
//! this disposable spool. A witness is conditional on a trusted certified base
//! lookup and is not an authorization, owner fence or publication certificate.
use super::{
    catalog::StoredCatalog,
    directory::{self, RunDescriptor},
    metadata::{
        self, AdmittedFile, MetadataError, MetadataLimits, MetadataSegment, ObjectHeader,
        PAGE_OBJECTS,
    },
    verification::{PhysicalError, PhysicalPackWitness, PhysicalPartition},
};
use crate::{ObjectFormat, ObjectId, ObjectKind};
use cellule_ltx::DiskBudget;
use rusqlite::{Connection, OptionalExtension, params};
use std::{
    future::Future,
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

mod copy;
mod graph;
mod retained;
mod spool;
mod verifier;
mod witness;
pub(in crate::packs) use retained::RetainedClosure;
use spool::Spool;
pub use verifier::ClosureVerifier;
pub use witness::ClosureWitness;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ClosureBase {
    pub catalog: StoredCatalog,
    pub generation: u64,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ClosureContext {
    pub repository: [u8; 16],
    pub operation: [u8; 16],
    pub format: ObjectFormat,
    pub base: Option<ClosureBase>,
}
impl ClosureContext {
    fn validate(self) -> Result<(), ClosureError> {
        if let Some(base) = self.base {
            base.catalog.validate()?;
            if base.catalog.repository != self.repository
                || base.catalog.format != self.format
                || base.generation == 0
                || base.generation > i64::MAX as u64
            {
                return Err(ClosureError::Integrity);
            }
        }
        Ok(())
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BaseObject {
    pub header: ObjectHeader,
    /// Must come from authoritative closure facts for the selected generation.
    /// Presence in a raw catalog or physical index cannot set this to true.
    pub certified: bool,
}
pub struct BaseBatch {
    pub base: ClosureBase,
    /// One result per requested OID in exactly the same order, at most 512.
    pub objects: Vec<Option<BaseObject>>,
}
/// Trusted service boundary. Retain the selected generation lease throughout
/// verification and derive certification from authoritative facts. A raw
/// CatalogReader alone cannot implement this contract. Publication must CAS
/// the same base and revalidate overlaps if it changes.
pub trait BaseResolver: Sync {
    fn resolve(
        &self,
        base: ClosureBase,
        ids: &[ObjectId],
    ) -> impl Future<Output = Result<BaseBatch, ClosureError>> + Send;
}
#[derive(Debug, thiserror::Error)]
pub enum ClosureError {
    #[error("closure metadata failed")]
    Metadata(#[from] MetadataError),
    #[error("closure physical input failed")]
    Physical(#[from] PhysicalError),
    #[error("closure catalog lookup failed")]
    Catalog(#[from] super::directory::index::IndexError),
    #[error("closure worker failed")]
    Task(#[from] tokio::task::JoinError),
    #[error("closure input or generation binding is inconsistent")]
    Integrity,
    #[error("dependency {0:?} is missing")]
    Missing(ObjectId),
    #[error("dependency {oid:?} requires {expected:?}, found {actual:?}")]
    Kind {
        oid: ObjectId,
        expected: ObjectKind,
        actual: ObjectKind,
    },
    #[error("base object {0:?} has no closure certificate")]
    Uncertified(ObjectId),
    #[error("incoming dependencies contain a cycle")]
    Cycle,
    #[error("certified base generation lease expired")]
    LeaseExpired,
    #[error("closure verification was canceled")]
    Canceled,
}
impl From<rusqlite::Error> for ClosureError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Metadata(error.into())
    }
}
struct CancelGuard {
    canceled: Arc<AtomicBool>,
    complete: bool,
}
impl CancelGuard {
    fn new(canceled: Arc<AtomicBool>) -> Self {
        Self {
            canceled,
            complete: false,
        }
    }
    fn complete(&mut self) {
        self.complete = true;
    }
}
impl Drop for CancelGuard {
    fn drop(&mut self) {
        if !self.complete {
            self.canceled.store(true, Ordering::Release);
        }
    }
}

#[cfg(test)]
mod tests;
