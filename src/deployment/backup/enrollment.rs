use super::*;
use crate::server::{LEASE_MS, RENEW_INTERVAL, renew_node_lease, workspace};
use cellule_runtime::{
    NodeAdvertisement, NodeCapacity, NodeFailureDomain, NodeLeaseGuard, SessionId,
};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

impl Deployment {
    pub(super) async fn enrolled_backup<F, Fut>(
        &self,
        backup: Option<RequestId>,
        config: WorkerConfig,
        run: F,
    ) -> BackupResult<BackupReport>
    where
        F: FnOnce(Host, PathBuf, NodeLeaseGuard) -> Fut,
        Fut: std::future::Future<Output = BackupResult<BackupReport>>,
    {
        self.backup_admission(backup).await?;
        let local =
            tokio::task::spawn_blocking(move || workspace::Workspace::open(&config.data_dir))
                .await??;
        let host =
            Host::default().with_local_disk_budget(DiskBudget::new(config.local_disk_limit_bytes));
        let session = SessionId::from_bytes(uuid::Uuid::new_v4().into_bytes());
        let fleet = self.nodes.fleet();
        let image = self.image_digest;
        let release = self.registry.release_digest();
        let modules = self.registry.module_digests();
        let sign = move |progress, now: i64| {
            NodeAdvertisement::sign(
                config.node,
                session,
                config.endpoint.clone(),
                fleet,
                Digest::from_bytes(
                    *blake3::hash(config.signing_key.verifying_key().as_bytes()).as_bytes(),
                ),
                image,
                release,
                &config.signing_key,
                progress,
                now,
                now.checked_add(LEASE_MS)
                    .ok_or(Error::Node("lease time overflow"))?,
                modules.clone(),
                vec![1],
                NodeFailureDomain::default(),
                NodeCapacity::default(),
            )
        };
        let now = unix_now_ms()?;
        let guard = NodeLeaseGuard::new(now, now + LEASE_MS)?;
        let observed = Arc::new(Mutex::new(self.nodes.create(sign(1, now)?, now).await?));
        let stop = CancellationToken::new();
        let renewal_stop = stop.clone();
        let renewal_guard = guard.clone();
        let renewal_observed = observed.clone();
        let deployment = self.clone();
        let renewal = tokio::spawn(async move {
            let outcome = async {
                let mut progress = 1_u64;
                loop {
                    tokio::select! {
                        () = renewal_stop.cancelled() => return Ok::<_, BackupError>(()),
                        () = tokio::time::sleep(RENEW_INTERVAL) => {},
                    }
                    deployment.backup_admission(backup).await?;
                    renewal_guard.check()?;
                    progress = progress
                        .checked_add(1)
                        .ok_or(BackupError::Invalid("lease progress overflow"))?;
                    let now = unix_now_ms()?;
                    let mut current = renewal_observed.lock().await;
                    *current = deployment
                        .nodes
                        .refresh(&current, sign(progress, now)?, now)
                        .await?;
                    renew_node_lease(&renewal_guard, &current)?;
                }
            }
            .await;
            if outcome.is_err() {
                renewal_guard.fence();
            }
            outcome
        });
        // All callers supervise this whole future. Enrollment survives client
        // cancellation until pin publication and immutable copying have settled.
        let result = async {
            self.backup_admission(backup).await?;
            run(host, local.path().to_path_buf(), guard.clone()).await
        }
        .await;
        stop.cancel();
        let renewed = renewal.await?;
        let withdrawn = self
            .nodes
            .withdraw(&*observed.lock().await, unix_now_ms()?)
            .await;
        renewed?;
        withdrawn?;
        if let Err(error) = &result {
            tracing::error!(error = %error, "backup operation failed");
        }
        result
    }

    async fn backup_admission(&self, backup: Option<RequestId>) -> Result<()> {
        match backup {
            None => self.require_ready().await,
            Some(id) => {
                root::require_backup(self.layout.store(), &self.prefix, id).await?;
                self.identities.layout(self.identity).await?;
                let record = self.record().await?;
                self.require_compiled(&record).await?;
                if record.state() != ReleaseState::Ready {
                    return Err(Error::Backup("backup release changed"));
                }
                Ok(())
            }
        }
    }
}
