//! Certified directory compaction. This changes neither refs nor native packs
//! and grants no authority to delete old artifacts or shorten retained floors.
use super::*;
use crate::packs::{
    catalog::CatalogSnapshot,
    directory::{
        DirectoryBuilder, DirectoryPartitioner, RUN_TARGET_BYTES,
        index::{NodeRef, codec::reference},
        snapshot::LEVEL_ZERO_ROOTS,
    },
    metadata::{MetadataError, MetadataLimits},
};
use cellule_ltx::DiskBudget;
use std::{path::Path, sync::Arc};
use tokio::time::timeout_at;
mod prepare;
mod publish;
pub use publish::{
    CheckCompletedCompaction, CompactionReply, PublishCatalogCompaction, PublishedCompaction,
};

#[derive(Clone, Copy)]
pub struct CompactionLimits {
    pub input_runs: u32,
    pub input_bytes: u64,
    pub spool: MetadataLimits,
    pub output: MetadataLimits,
}
impl Default for CompactionLimits {
    fn default() -> Self {
        Self {
            input_runs: 128,
            input_bytes: 256 << 20,
            spool: MetadataLimits::default(),
            output: MetadataLimits {
                max_file_bytes: RUN_TARGET_BYTES,
                ..MetadataLimits::default()
            },
        }
    }
}
impl CompactionLimits {
    fn validate(self) -> Result<(), CatalogPreparationError> {
        DirectoryPartitioner::validate_limits(self.output)?;
        if self.input_runs == 0
            || self.input_runs > 4096
            || self.input_bytes == 0
            || self.input_bytes > 16 << 30
        {
            return Err(MetadataError::Limit.into());
        }
        Ok(())
    }
}
/// Only preparation from query-derived certified inputs constructs this object.
/// It retains exact selected input roots for rebinding against a moving frontier.
pub struct PreparedCompaction {
    base: Arc<PreparationBaseResolver>,
    selected: Vec<NodeRef>,
    output: NodeRef,
    catalog: StoredCatalog,
    object_count: u64,
    edge_count: u64,
    input_count: u64,
    inputs_digest: [u8; 32],
    inventory_digest: [u8; 32],
}
impl PreparedCompaction {
    pub fn catalog(&self) -> StoredCatalog {
        self.catalog
    }
    pub fn token(&self) -> PreparationToken {
        self.base.context_token()
    }
    pub fn object_count(&self) -> u64 {
        self.object_count
    }
    pub fn input_count(&self) -> u64 {
        self.input_count
    }
    pub fn inventory_digest(&self) -> [u8; 32] {
        self.inventory_digest
    }
    pub fn base(&self) -> GenerationFact {
        self.base.generation_fact()
    }
    pub async fn certificate(&self) -> Result<CatalogCertificate, CatalogAttestationError> {
        let (_, deadline) = self.base.live_lease()?;
        let (_, target, check) = self.base.capability();
        let data = certificate::CertificateData {
            compaction: true,
            tenant: *target.tenant().as_bytes(),
            application: *target.application().as_bytes(),
            token: self.token(),
            actor: check.actor.clone(),
            retention_floor: self.base.retention_floor().generation,
            retention_certificate: self.base.retention_floor().certificate,
            base: self.base(),
            catalog: self.catalog,
            object_count: self.object_count,
            edge_count: self.edge_count,
            input_count: self.input_count,
            inputs_digest: self.inputs_digest,
            inventory_digest: self.inventory_digest,
            refs_digest: None,
            completion_digest: None,
        };
        timeout_at(
            deadline,
            attestation::issue_data_certificate(&self.base, data),
        )
        .await
        .map_err(|_| PreparationBaseError::Inactive)?
    }
    /// Exact selected roots must still occupy level zero. If another compaction
    /// replaced/moved one, reject rather than resurrecting an obsolete placement.
    /// Concurrent ingress keeps its own roots and the current source tree.
    pub async fn reconcile(&self) -> Result<Self, CatalogPreparationError> {
        let (_, deadline) = self.base.live_lease()?;
        timeout_at(deadline, async {
            let base = Arc::new(self.base.select_current().await?);
            let catalog = if base.generation_fact() == self.base() {
                self.catalog
            } else {
                replacement(&base, &self.selected, self.output).await?
            };
            base.live_lease()?;
            Ok(Self {
                base,
                catalog,
                selected: self.selected.clone(),
                output: self.output,
                object_count: self.object_count,
                edge_count: self.edge_count,
                input_count: self.input_count,
                inputs_digest: self.inputs_digest,
                inventory_digest: self.inventory_digest,
            })
        })
        .await
        .map_err(|_| PreparationBaseError::Inactive)?
    }
}
async fn replacement(
    base: &PreparationBaseResolver,
    selected: &[NodeRef],
    output: NodeRef,
) -> Result<StoredCatalog, CatalogPreparationError> {
    let (mut directory, sources) = base.catalog_parts();
    if selected.len() < 2
        || selected
            .iter()
            .any(|root| !directory.level_zero.contains(root))
    {
        return Err(crate::packs::directory::index::IndexError::Stale.into());
    }
    directory.level_zero.retain(|root| !selected.contains(root));
    let indexes = base.indexes();
    directory.append(indexes.ranges(), output).await?;
    let store = indexes.store();
    let operation = base.context_token().artifact_operation;
    Ok(CatalogSnapshot {
        directory: directory.upload(&store, operation).await?,
        sources,
    }
    .upload(&store, operation)
    .await?)
}
