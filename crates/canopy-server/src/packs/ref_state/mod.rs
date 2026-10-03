//! Conditional immutable ref state; raw roots do not confer publication rights.
//! Final owner/ACL/policy/root-CAS and durable outcome publication remain Cell
//! responsibilities. This data plane is not selected by the serving path yet.
use super::directory::index::{IndexError, NodeRef, RangeCursor, RangeIndex, ReadStats};
use crate::{ObjectFormat, PushPlan, RefExpectation};
use canopy_object_storage::artifact::ArtifactStore;
use cellule_runtime::codec::{BoundedDecoder, BoundedEncoder, CodecError};
use std::sync::Arc;

mod record;
pub use record::{RefNameKey, RefStateRecord};
mod snapshot;
mod transition;
pub use snapshot::{RefSnapshotError, RefStateSnapshot, RefStateSnapshotRoot};
#[cfg(test)]
mod tests;

pub type RefStateRoot = NodeRef<RefStateRecord>;
pub type RefStateTree = RangeIndex<RefStateRecord>;
pub const MAX_NAME_BYTES: usize = 65_535;

#[derive(Debug, thiserror::Error)]
pub enum RefStateError {
    #[error("ref state index failed")]
    Index(#[from] IndexError),
    #[error("ref state codec failed")]
    Codec(#[from] CodecError),
    #[error("ref plan shape failed")]
    Shape(#[from] super::publication::RefProofError),
    #[error("ref expectation changed")]
    Changed,
    #[error("live ref namespace conflicts")]
    Namespace,
}

pub struct RefStateIndex {
    tree: RefStateTree,
}
/// Conditional path-copy result. Its plan digest is the existing canonical
/// PushPlan digest, not a substitute for membership, ancestry or policy proof.
pub struct RefTransition {
    base: Option<RefStateRoot>,
    root: RefStateRoot,
    plan_digest: [u8; 32],
}
impl RefTransition {
    pub fn base(&self) -> Option<RefStateRoot> {
        self.base.clone()
    }
    pub fn root(&self) -> RefStateRoot {
        self.root.clone()
    }
    pub fn plan_digest(&self) -> [u8; 32] {
        self.plan_digest
    }
}
impl RefStateIndex {
    pub fn new(store: Arc<ArtifactStore>, format: ObjectFormat) -> Self {
        Self {
            tree: RefStateTree::new(store, format),
        }
    }
    pub fn repository(&self) -> [u8; 16] {
        self.tree.repository()
    }
    pub fn format(&self) -> ObjectFormat {
        self.tree.format()
    }
    pub fn stats(&self) -> ReadStats {
        self.tree.stats()
    }
    pub fn clear_cache(&self) -> Result<(), IndexError> {
        self.tree.clear_cache()
    }
    pub async fn read(
        &self,
        root: Option<RefStateRoot>,
        name: &str,
    ) -> Result<Option<RefExpectation>, RefStateError> {
        if !crate::refs::valid_ref_name(name) {
            return Err(CodecError::Invalid("ref name").into());
        }
        Ok(self
            .tree
            .find(root, RefNameKey::new(name)?)
            .await?
            .map(|record| record.state))
    }
    /// Seek a bounded-height ordered cursor. Live cursors skip authenticated
    /// zero-weight subtrees; ordinary cursors retain deletion versions.
    pub fn cursor(
        &self,
        root: Option<RefStateRoot>,
        after: Option<RefNameKey>,
        live_only: bool,
    ) -> Result<RangeCursor<'_, RefStateRecord>, IndexError> {
        if live_only {
            self.tree.positive_cursor(root, after)
        } else {
            self.tree.cursor(root, after)
        }
    }
}
