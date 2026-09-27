//! Durable release admission and operation-bound fleet maintenance.

use std::sync::Arc;

use crab_cell_runtime::{
    Digest, Error, Registry, Result, cell::application::ApplicationIdentity,
    cell::application::ApplicationIdentityStore, cell::catalog::CellCatalog, control::ControlState,
    control::authority::CellAuthority, identity::RequestId, ltx::CellStorageLayout,
    node::NodeDirectory, recovery::release::ReleaseRecord, recovery::release::ReleaseState,
    recovery::release::ReleaseStore,
};
use crab_storage::Store;
use object_store::path::Path;
use serde::Serialize;

mod backup;
pub use backup::{BackupError, BackupReport};
mod recovery;
mod root;
pub use recovery::WorkerConfig;

/// Application-wide admission shared by nodes and offline administration.
#[derive(Clone)]
pub struct Deployment {
    identity: ApplicationIdentity,
    prefix: Path,
    identities: ApplicationIdentityStore,
    layout: CellStorageLayout,
    releases: ReleaseStore,
    nodes: NodeDirectory,
    registry: Arc<Registry>,
    image: String,
    image_digest: Digest,
}

/// A current release observation with conservative fleet drain evidence.
#[derive(Serialize)]
pub struct DeploymentStatus {
    pub release: serde_json::Value,
    pub advertised_sessions: usize,
    pub unsettled_cells: usize,
    pub drained: bool,
}

impl Deployment {
    /// Binds deployment operations to one identity, compiled release and image.
    pub fn new(
        store: Store,
        prefix: Path,
        identity: ApplicationIdentity,
        fleet: Digest,
        image: Digest,
        registry: Arc<Registry>,
    ) -> Result<Self> {
        let layout = CellStorageLayout::new(
            store.clone(),
            prefix.clone(),
            *identity.application().as_bytes(),
        );
        Ok(Self {
            releases: ReleaseStore::new(layout.clone(), identity)?,
            nodes: NodeDirectory::new(layout.clone(), fleet, image, registry.release_digest()),
            identities: ApplicationIdentityStore::new(store, prefix.clone()),
            prefix,
            layout,
            identity,
            registry,
            image: format!("sha256:{}", hex::encode(image.as_bytes())),
            image_digest: image,
        })
    }

    /// Initializes an empty deployment or resumes its exact first activation.
    /// Existing deployments require the same ready release; upgrades are explicit.
    pub(crate) async fn initialize(&self) -> Result<()> {
        self.claim_service_root().await?;
        self.identities.initialize(self.identity).await?;
        let mut record = self.releases.load().await?.map(|r| r.record().clone());
        // A deterministic operation permits concurrent first nodes and process
        // death between bootstrap phases to adopt only this exact first release.
        let digest = self.registry.release_digest();
        let mut operation = [0; 16];
        operation.copy_from_slice(&digest.as_bytes()[..16]);
        operation[0] |= 1;
        let operation = RequestId::from_bytes(operation);
        if record.is_none() {
            let catalog = CellCatalog::new(self.layout.clone(), self.identity.tenant());
            for shard in 0..=u8::MAX {
                if catalog.scan_shard(shard).await?.revision() != 0 {
                    record = self.releases.load().await?.map(|r| r.record().clone());
                    if record.is_none() {
                        return Err(Error::Release(
                            "existing Cell catalog has no release selection",
                        ));
                    }
                    break;
                }
            }
            if record.is_none() {
                match self
                    .releases
                    .prepare(
                        self.registry.release_bytes(),
                        digest,
                        0,
                        &self.image,
                        operation,
                    )
                    .await
                {
                    Ok(prepared) => record = Some(prepared),
                    Err(error) => {
                        record = self.releases.load().await?.map(|r| r.record().clone());
                        if record.is_none() {
                            return Err(error);
                        }
                    }
                }
            }
        }
        let record = record.ok_or(Error::Release("release selection is absent"))?;
        if record.current().is_none()
            && record.desired() == Some(digest)
            && record.operation() == operation
            && record.desired_image() == self.image
            && matches!(
                record.state(),
                ReleaseState::Prepared | ReleaseState::Activating
            )
        {
            let activating = if record.state() == ReleaseState::Prepared {
                self.releases
                    .start_activation(record.revision(), operation)
                    .await?
            } else {
                record
            };
            if activating.state() != ReleaseState::Ready {
                self.releases
                    .complete_activation(activating.revision(), operation)
                    .await?;
            }
        }
        self.require_ready().await
    }

    /// Rejects nodes when identity, selected release, image or phase differs.
    pub async fn require_ready(&self) -> Result<()> {
        self.require_service_root().await?;
        self.identities.layout(self.identity).await?;
        let record = self.record().await?;
        self.require_compiled(&record).await?;
        if record.state() != ReleaseState::Ready {
            return Err(Error::Release("deployment is not ready for node admission"));
        }
        Ok(())
    }

    async fn require_maintenance(&self, operation: RequestId) -> Result<()> {
        self.require_service_root().await?;
        self.identities.layout(self.identity).await?;
        let record = self.record().await?;
        self.require_compiled(&record).await?;
        if record.state() != ReleaseState::Maintenance || record.operation() != operation {
            return Err(Error::Release("exact maintenance operation is not active"));
        }
        Ok(())
    }

    async fn record(&self) -> Result<ReleaseRecord> {
        Ok(self
            .releases
            .load()
            .await?
            .ok_or(Error::Release("release selection is absent"))?
            .record()
            .clone())
    }

    async fn require_compiled(&self, record: &ReleaseRecord) -> Result<()> {
        let digest = self.registry.release_digest();
        if record.current() != Some(digest)
            || record.desired() != Some(digest)
            || record.desired_image() != self.image
            || self.releases.descriptor(digest).await? != self.registry.release_bytes()
        {
            return Err(Error::Release(
                "deployment requires a different compiled release or image",
            ));
        }
        Ok(())
    }

    /// Closes fleet admission under an explicit, retryable maintenance operation.
    pub async fn begin_maintenance(&self, operation: RequestId) -> Result<ReleaseRecord> {
        self.require_service_root().await?;
        self.identities.layout(self.identity).await?;
        let record = self.record().await?;
        self.require_compiled(&record).await?;
        let prepared = match record.state() {
            ReleaseState::Ready if record.operation() == operation => return Ok(record),
            ReleaseState::Ready => {
                self.releases
                    .prepare(
                        self.registry.release_bytes(),
                        self.registry.release_digest(),
                        record.revision(),
                        &self.image,
                        operation,
                    )
                    .await?
            }
            ReleaseState::Prepared if record.operation() == operation => record,
            ReleaseState::Maintenance if record.operation() == operation => return Ok(record),
            _ => {
                return Err(Error::Release(
                    "another deployment operation is in progress",
                ));
            }
        };
        self.releases
            .start_maintenance(prepared.revision(), operation)
            .await
    }

    /// Observes drain without treating expired advertisements as closed writers.
    pub async fn status(&self, now_ms: i64) -> Result<DeploymentStatus> {
        self.require_service_root().await?;
        self.identities.layout(self.identity).await?;
        let before = self.record().await?;
        self.require_compiled(&before).await?;
        let advertised_sessions = self.nodes.advertised_sessions(now_ms, 4096).await?.len();
        let catalog = CellCatalog::new(self.layout.clone(), self.identity.tenant());
        let authority = CellAuthority::new(self.layout.clone());
        let mut unsettled_cells = 0;
        for shard in 0..=u8::MAX {
            let mut scan = catalog.scan_shard(shard).await?;
            while let Some(page) = scan.next_page().await? {
                for proof in page.entries() {
                    let control = authority.load(proof.entry().cell()).await?;
                    let settled = control.is_some_and(|c| settled(c.value()));
                    if !settled {
                        unsettled_cells += 1;
                    }
                }
            }
        }
        if before != self.record().await? {
            return Err(Error::Release("release changed during drain observation"));
        }
        Ok(DeploymentStatus {
            drained: before.state() == ReleaseState::Maintenance
                && advertised_sessions == 0
                && unsettled_cells == 0,
            release: serde_json::from_slice(&before.encode()?)?,
            advertised_sessions,
            unsettled_cells,
        })
    }

    /// Reopens the same compiled release only after the exact operation drains.
    pub async fn end_maintenance(
        &self,
        operation: RequestId,
        now_ms: i64,
    ) -> Result<ReleaseRecord> {
        self.require_service_root().await?;
        self.identities.layout(self.identity).await?;
        let record = self.record().await?;
        self.require_compiled(&record).await?;
        if record.operation() != operation {
            return Err(Error::Release("maintenance operation differs"));
        }
        if record.state() == ReleaseState::Ready {
            return Ok(record);
        }
        if !self.status(now_ms).await?.drained {
            return Err(Error::Release("deployment writers have not drained"));
        }
        self.releases
            .complete_maintenance(record.revision(), operation)
            .await
    }
}

fn settled(control: &crab_cell_runtime::control::Control) -> bool {
    control.owner.is_none()
        && (control.state == ControlState::Tombstoned
            || control.state == ControlState::Idle && control.root.is_some())
}

#[cfg(test)]
mod tests;
