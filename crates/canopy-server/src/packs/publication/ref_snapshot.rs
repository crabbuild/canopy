//! Query-derived joint catalog/ref preparation. Public root descriptors and
//! conditional tree transitions cannot construct this private prepared value.
use super::*;
use crate::{
    PushPlan,
    packs::ref_state::{RefSnapshotError, RefStateError, RefStateIndex, RefStateSnapshot},
};
use std::sync::Arc;
use tokio::time::timeout_at;

#[derive(Debug, thiserror::Error)]
pub enum RefSnapshotPreparationError {
    #[error("ref snapshot preparation lease is inactive")]
    Base(#[from] PreparationBaseError),
    #[error("ref snapshot metadata failed")]
    Snapshot(#[from] RefSnapshotError),
    #[error("ref snapshot transition failed")]
    State(#[from] RefStateError),
    #[error("selected generation has no immutable ref snapshot")]
    Unavailable,
    #[error("selected ref snapshot context differs")]
    Context,
}

/// Conditional output bound to the catalog's query-derived retained generation.
/// Membership, policy/check facts and final authority still need to be bound by
/// the final certificate factory; this value itself grants no write authority.
pub struct PreparedRefSnapshot {
    base: GenerationFact,
    snapshot: RefStateSnapshotRoot,
    plan_digest: [u8; 32],
}
impl PreparedRefSnapshot {
    pub fn base(&self) -> GenerationFact {
        self.base
    }
    pub fn snapshot(&self) -> RefStateSnapshotRoot {
        self.snapshot
    }
    pub fn plan_digest(&self) -> [u8; 32] {
        self.plan_digest
    }
}
impl PreparedCatalog {
    /// Read exactly this prepared catalog's selected ref metadata from its own
    /// trusted store capability. No caller-selected root/store/transition is
    /// accepted and no empty fallback can hide an unconverted SQL ref state.
    pub async fn prepare_ref_snapshot(
        &self,
        plan: &PushPlan,
    ) -> Result<PreparedRefSnapshot, RefSnapshotPreparationError> {
        let (_, deadline) = self.base.live_lease()?;
        timeout_at(deadline, async {
            if plan.actor != self.base.capability().2.actor {
                return Err(RefSnapshotPreparationError::Context);
            }
            let base = self.base();
            let selected = base.refs.ok_or(RefSnapshotPreparationError::Unavailable)?;
            let store = self.base.indexes().store();
            let old = selected.read(&store).await?;
            if old.repository != self.token().repository
                || old.format != self.catalog().format
                || old.generation > base.generation
                || old.generation >= i64::MAX as u64
            {
                return Err(RefSnapshotPreparationError::Context);
            }
            let index = RefStateIndex::new(Arc::clone(&store), old.format);
            let transition = index
                .prepare(old.root, self.token().artifact_operation, plan)
                .await?;
            self.ensure_live()?;
            let snapshot = RefStateSnapshotRoot::upload(
                &store,
                self.token().artifact_operation,
                RefStateSnapshot {
                    repository: old.repository,
                    format: old.format,
                    generation: old.generation + 1,
                    default_branch: old.default_branch,
                    root: Some(transition.root()),
                },
            )
            .await?;
            self.ensure_live()?;
            Ok(PreparedRefSnapshot {
                base,
                snapshot,
                plan_digest: transition.plan_digest(),
            })
        })
        .await
        .map_err(|_| PreparationBaseError::Inactive)?
    }
}
