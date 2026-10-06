//! Resident owners join discovery before release and retain exact uncertain work.
use super::*;
use crate::packs::catalog::{CatalogFileLimits, CatalogFiles, CatalogIndexes};
use crate::packs::publication::{
    CustodySupervisor, MaintenanceRequest, PreparationAuthority, PublicationCoordinator,
    PublicationLimits, PublicationState, RecoveryScanLimits, RecoverySupervisor, ServingContext,
    ServingPool, ServingPoolLimits, StagingCoordinator, StagingLimits, StagingState,
};
use canopy_object_storage::artifact::ArtifactStore;

pub(super) struct RecoveryServices {
    pub(super) coordinator: PublicationCoordinator,
    pub(super) serving: Arc<ServingPool>,
    pub(super) staging: Arc<StagingCoordinator>,
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
        if manager.serving_stop.is_cancelled() {
            return Err(Error::CellDraining.into());
        }
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
        let store = Arc::new(ArtifactStore::new(
            Arc::clone(&manager.external_store),
            entry.repository_id,
        ));
        let staging = Arc::new(
            StagingCoordinator::new_resident(
                client.clone(),
                target.clone(),
                StagingLimits::default(),
                authority.clone(),
                manager.staging_budget.clone(),
                coordinator.clone(),
                store.clone(),
            )
            .map_err(|error| ServerError::CatalogRecovery(Box::new(error)))?,
        );
        let settings = manager
            .recovery_scans
            .settings(RecoveryScanLimits::default(), &entry.owner);
        let serving = Arc::new(
            ServingPool::new(
                ServingContext::new(
                    client.clone(),
                    target.clone(),
                    authority.clone(),
                    Arc::new(CatalogIndexes::new(store.clone(), entry.object_format)),
                    Arc::new(
                        CatalogFiles::new(
                            manager.local.path(),
                            manager.disk_budget.clone(),
                            store,
                            entry.object_format,
                            CatalogFileLimits::default(),
                        )
                        .map_err(|error| ServerError::CatalogRecovery(Box::new(error)))?
                        .with_native(
                            manager
                                .native
                                .scope(crate::native_resources::NativeClass::Foreground),
                        ),
                    ),
                    manager.serving_reads.clone(),
                    entry.owner.clone(),
                )
                .map_err(|error| ServerError::CatalogRecovery(Box::new(error)))?,
                coordinator.clone(),
                ServingPoolLimits::default(),
            )
            .map_err(|error| ServerError::CatalogRecovery(Box::new(error)))?,
        );
        let roots = match RecoverySupervisor::start_resident(
            client.clone(),
            target.clone(),
            ArtifactStore::new(Arc::clone(&manager.external_store), entry.repository_id),
            (coordinator.clone(), staging.clone()),
            settings.clone(),
            authority.clone(),
            maintenance,
        ) {
            Ok(roots) => roots,
            Err(error) => {
                serving.close_and_drain().await;
                return Err(ServerError::CatalogRecovery(Box::new(error)));
            }
        };
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
                serving.close_and_drain().await;
                return Err(ServerError::CatalogRecovery(Box::new(error)));
            }
        };
        Ok(Self {
            coordinator,
            serving,
            staging,
            workers: Mutex::new(Some(Workers { roots, custody })),
        })
    }

    pub(super) async fn quiesce(&self) -> bool {
        let Some(staging) = self.staging.try_quiesce() else {
            return false;
        };
        let mut workers = self.workers.lock().await;
        if let Some(active) = workers.as_ref() {
            tokio::join!(active.roots.pause(), active.custody.pause());
        }
        let closed = match self.serving.quiesce().await {
            Ok(closed) => closed,
            Err(error) => {
                tracing::warn!(?error, "serving drain refused repository eviction");
                false
            }
        };
        if !closed {
            if let Some(active) = workers.as_ref() {
                active.roots.resume();
                active.custody.resume();
            }
            return false;
        }
        staging.commit();
        self.serving.close_and_drain().await;
        join(workers.take()).await;
        true
    }

    async fn drain_staging(&self) {
        loop {
            let pending = self.staging.close_and_drain().await;
            if pending.is_empty() && !self.staging.stats().retirement_running {
                return;
            }
            // Exact settlement can remove the last job while its read-only
            // retirement owner is finishing a round. Keep the Cell and node
            // workspace until that independently owned scanner exits too.
            for ticket in pending {
                if matches!(ticket.state(), StagingState::Uncertain(_))
                    && let Err(error) = self.staging.recover(&ticket)
                {
                    tracing::warn!(?error, "exact staging recovery deferred during drain");
                }
            }
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }
    }
    pub(super) async fn drain(&self) {
        self.drain_staging().await;
        self.serving.close_and_drain().await;
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
    pub(in crate::server) async fn drain_serving(&self) {
        let services: Vec<_> = {
            let loaded = self.loaded.lock().await;
            // Share the publication barrier with constructor registration. A
            // late pool cannot escape this inventory or the node tracker join.
            self.serving_stop.cancel();
            loaded
                .values()
                .filter_map(|repository| repository.recovery.as_ref().map(Arc::clone))
                .collect()
        };
        for service in &services {
            service.staging.close_admission();
        }
        futures_util::future::join_all(
            services
                .iter()
                .map(|service| service.staging.finish_receive_workflows()),
        )
        .await;
        // Producer capabilities may retain serving generations and exact held
        // publication work. Drain them before closing either lower service.
        futures_util::future::join_all(services.iter().map(|service| service.drain_staging()))
            .await;
        for service in &services {
            service.serving.close();
        }
        futures_util::future::join_all(
            services
                .iter()
                .map(|service| service.serving.close_and_drain()),
        )
        .await;
        self.serving_reads.close();
    }
    pub(in crate::server) async fn drain_recovery(&self) {
        self.drain_serving().await;
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
