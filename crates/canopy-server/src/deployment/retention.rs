//! Product maintenance drain around Cellule's authoritative object collector.

use super::*;
use crate::{
    REPOSITORY_DATABASE_LIMIT_BYTES,
    server::{ServerError, unix_now_ms, workspace},
};
use cellule_ltx::{DiskBudget, Host};
use cellule_runtime::recovery::retention::{CellGarbageCollector, GarbageCollectionPolicy};

/// Counts from a verified live-root/backup-pin mark and bounded immutable sweep.
#[derive(Debug, Serialize)]
pub struct GarbageCollectionReport {
    pub listed_objects: u64,
    pub immutable_candidates: u64,
    pub reachable_objects: u64,
    pub retained_objects: u64,
    pub grace_objects: u64,
    pub eligible_objects: u64,
    pub deleted_objects: u64,
    pub current_controls: u64,
    pub retained_pins: u64,
    pub complete: bool,
}

impl Deployment {
    /// Reclaims old immutable Cell objects after every server, reader, backup
    /// worker and recovery owner has drained. Leaves the release in maintenance.
    /// Same-workspace retries resume a killed worker; dropping the caller cannot
    /// release exclusion before the sweep finishes. Git packs/LFS are separate
    /// product objects and are not deleted by the Cellule collector.
    pub async fn collect_garbage(
        &self,
        operation: RequestId,
        config: WorkerConfig,
        grace_ms: u64,
        max_deletes: u64,
    ) -> std::result::Result<GarbageCollectionReport, ServerError> {
        let deployment = self.clone();
        tokio::spawn(async move {
            deployment
                .collect(operation, config, grace_ms, max_deletes)
                .await
        })
        .await?
    }

    async fn collect(
        &self,
        operation: RequestId,
        config: WorkerConfig,
        grace_ms: u64,
        max_deletes: u64,
    ) -> std::result::Result<GarbageCollectionReport, ServerError> {
        self.require_maintenance(operation).await?;
        // Validate policy before claiming any persistent exclusion.
        GarbageCollectionPolicy::new(unix_now_ms()?, grace_ms, max_deletes)?;
        let data_dir = config.data_dir.clone();
        let local =
            tokio::task::spawn_blocking(move || workspace::Workspace::open(&data_dir)).await??;
        let worker = maintenance::Worker::new(&config, local.path())?;
        let claim =
            maintenance::acquire(self, operation, maintenance::Action::Collect, Some(worker))
                .await?;
        let result = async {
            self.require_maintenance(operation).await?;
            if !self.status(unix_now_ms()?).await?.drained {
                return Err(
                    Error::Retention("deployment readers and writers have not drained").into(),
                );
            }
            claim.require_current(self).await?;
            let host = Host::default()
                .with_local_disk_budget(DiskBudget::new(config.local_disk_limit_bytes));
            let collector = CellGarbageCollector::new(
                self.layout.clone(),
                self.identity,
                crate::replica_limits(REPOSITORY_DATABASE_LIMIT_BYTES, 64 * 1024 * 1024),
                host,
            )?;
            let report = collector
                .collect(
                    &self.record().await?,
                    local.path(),
                    GarbageCollectionPolicy::new(unix_now_ms()?, grace_ms, max_deletes)?,
                )
                .await?;
            claim.require_current(self).await?;
            Ok::<_, ServerError>(GarbageCollectionReport {
                listed_objects: report.listed_objects(),
                immutable_candidates: report.immutable_candidates(),
                reachable_objects: report.reachable_objects(),
                retained_objects: report.retained_objects(),
                grace_objects: report.grace_objects(),
                eligible_objects: report.eligible_objects(),
                deleted_objects: report.deleted_objects(),
                current_controls: report.current_controls(),
                retained_pins: report.retained_pins(),
                complete: report.complete(),
            })
        }
        .await;
        // The collector has joined all work on return. Errors retain the release
        // fence but allow a fresh bounded pass, including partial provider failure.
        claim.finish(self).await?;
        result
    }
}
