//! Resident owners join discovery before release and retain exact uncertain work.
use super::*;
use crate::packs::publication::{
    CustodySupervisor, MaintenanceRequest, PreparationAuthority, PublicationCoordinator,
    PublicationLimits, PublicationState, RecoveryScanLimits, RecoverySupervisor,
};
use canopy_object_storage::artifact::ArtifactStore;

pub(super) struct RecoveryServices {
    pub(super) coordinator: PublicationCoordinator,
    workers: Mutex<Option<Workers>>,
}
struct Workers {
    roots: RecoverySupervisor,
    custody: CustodySupervisor,
}
impl RecoveryServices {
    pub(super) async fn start(
        manager: &RepositoryManager,
        entry: &RepositoryEntry,
        repository: &RepositoryCell,
        client: CellClient,
    ) -> Result<Self, ServerError> {
        let target = repository.target.clone();
        let authority = PreparationAuthority::node(manager.peer.clone(), target.clone());
        let maintenance = MaintenanceRequest {
            repository: entry.repository_id,
            actor: entry.owner.clone(),
            owner: manager.peer.current_owner_fence(&target).await?,
        };
        let coordinator = PublicationCoordinator::new(
            target.clone(),
            PublicationLimits::default(),
            manager.publication_budget.clone(),
        )
        .map_err(|error| ServerError::CatalogRecovery(Box::new(error)))?;
        let settings = manager
            .recovery_scans
            .settings(RecoveryScanLimits::default(), &entry.owner);
        let roots = RecoverySupervisor::start_retiring(
            client.clone(),
            target.clone(),
            ArtifactStore::new(Arc::clone(&manager.external_store), entry.repository_id),
            coordinator.clone(),
            settings.clone(),
            authority.clone(),
            maintenance,
        )
        .map_err(|error| ServerError::CatalogRecovery(Box::new(error)))?;
        let custody = match CustodySupervisor::start(
            client,
            target,
            coordinator.clone(),
            settings,
            authority,
        ) {
            Ok(custody) => custody,
            Err(error) => {
                // A partially constructed owner must join its first worker before
                // giving up the residency transition or its workspace ownership.
                let _ = roots.shutdown().await;
                return Err(ServerError::CatalogRecovery(Box::new(error)));
            }
        };
        Ok(Self {
            coordinator,
            workers: Mutex::new(Some(Workers { roots, custody })),
        })
    }

    pub(super) async fn quiesce(&self) -> bool {
        let mut workers = self.workers.lock().await;
        if let Some(active) = workers.as_ref() {
            tokio::join!(active.roots.pause(), active.custody.pause());
        }
        if !self.coordinator.close_if_idle().await {
            if let Some(active) = workers.as_ref() {
                active.roots.resume();
                active.custody.resume();
            }
            return false;
        }
        join(workers.take()).await;
        true
    }

    async fn drain(&self) {
        join(self.workers.lock().await.take()).await;
        loop {
            let pending = self.coordinator.close_and_drain().await;
            if pending.is_empty() {
                return;
            }
            for ticket in pending {
                // Held final work belongs to its producer lifecycle. Do not
                // activate/discard it or substitute an unknown outcome here.
                if matches!(ticket.state(), PublicationState::Uncertain(_))
                    && let Err(error) = ticket.recover().await
                    && error != crate::packs::publication::PublicationScheduleError::NotUncertain
                {
                    tracing::warn!(?error, "exact repository recovery deferred during drain");
                }
            }
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }
    }
}
async fn join(workers: Option<Workers>) {
    if let Some(workers) = workers {
        let (roots, custody) = tokio::join!(workers.roots.shutdown(), workers.custody.shutdown());
        // A failed discovery task is not evidence about admitted commands.
        // Its control guard has drained; the coordinator remains owned below.
        if let Err(error) = roots {
            tracing::error!(?error, "repository root scanner failed");
        }
        if let Err(error) = custody {
            tracing::error!(?error, "repository custody scanner failed");
        }
    }
}
impl RepositoryManager {
    pub(in crate::server) async fn drain_recovery(&self) {
        self.recovery_scans.close();
        self.publication_budget.close();
        // The existing residency cap bounds this inventory. No independent
        // durable queue or historical-repository registry is introduced.
        let services: Vec<_> = self
            .loaded
            .lock()
            .await
            .values()
            .filter_map(|repository| repository.recovery.as_ref().map(Arc::clone))
            .collect();
        // One producer-held command must not prevent other repositories from
        // resolving their exact originals. All futures remain owned by this
        // drain; the existing residency cap bounds their concurrent inventory.
        futures_util::future::join_all(services.iter().map(|service| service.drain())).await;
    }
}
