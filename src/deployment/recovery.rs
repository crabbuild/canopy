//! Enrolled, lease-fenced recovery of an interrupted maintenance drain.

use super::*;
use crate::{
    CanopyApplication, REPOSITORIES, REPOSITORY_DATABASE_LIMIT_BYTES, RepositoryModule,
    build_descriptor,
    directory::{self, DirectoryModule},
    server::{
        LEASE_MS, RENEW_INTERVAL, ServerError, SqlCellSpec, acquire_provisioned_sql_cell,
        unix_now_ms, workspace,
    },
};
use cellule_app::CellApplication;
use cellule_host::CellNodeBuilder;
use cellule_ltx::{DiskBudget, Host};
use cellule_runtime::{
    CellModule, CellTarget, NodeAdvertisement, NodeCapacity, NodeFailureDomain, NodeId,
    NodeLeaseGuard, SessionId, SqlWorkerPool,
};
use ed25519_dalek::SigningKey;
use std::path::PathBuf;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

/// Local workspace and signing identity for a deployment operation worker.
pub struct WorkerConfig {
    pub node: NodeId,
    pub signing_key: SigningKey,
    pub endpoint: String,
    pub data_dir: PathBuf,
    pub local_disk_limit_bytes: u64,
}

impl Deployment {
    /// Recovers expired owners and releases their Cells under one maintenance operation.
    /// Live owners and unresolved node logs fail closed. Cancellation retains cleanup.
    pub async fn recover_maintenance(
        &self,
        operation: RequestId,
        config: WorkerConfig,
    ) -> std::result::Result<(), ServerError> {
        let deployment = self.clone();
        // The task owns every acquisition and its drain. Dropping the caller must
        // not abandon SQL workers or release local exclusion before they close.
        tokio::spawn(async move {
            let result = deployment.recover(operation, config).await;
            if let Err(error) = &result {
                tracing::error!(error = %error, "maintenance recovery failed");
            }
            result
        })
        .await?
    }

    async fn recover(
        &self,
        operation: RequestId,
        config: WorkerConfig,
    ) -> std::result::Result<(), ServerError> {
        self.require_maintenance(operation).await?;
        let data_dir = config.data_dir;
        let local =
            tokio::task::spawn_blocking(move || workspace::Workspace::open(&data_dir)).await??;
        let probe = cellule_store::probe_storage(
            self.layout.store(),
            &self.layout.application_prefix().join("canopy-probe"),
            unix_now_ms()?,
        )
        .await?;
        let application = Arc::new(CanopyApplication::compile(build_descriptor(
            include_bytes!("../../Cargo.lock"),
            env!("CARGO_PKG_VERSION"),
        ))?);
        if application.registry().release_digest() != self.registry.release_digest() {
            return Err(Error::Release("recovery binary differs from deployment").into());
        }
        let session = SessionId::from_bytes(uuid::Uuid::new_v4().into_bytes());
        let node = CellNodeBuilder::new(application)
            .with_runtime(SqlWorkerPool::new(1, 1)?, 64 * 1024 * 1024)
            .with_replica_host(
                Host::default()
                    .with_local_disk_budget(DiskBudget::new(config.local_disk_limit_bytes)),
            )
            .with_session(session)
            .build()?;
        node.require_storage_capabilities(&probe)?;
        let stop = CancellationToken::new();
        let tasks = node.install_task_group(stop.clone(), stop.clone())?;
        let now = unix_now_ms()?;
        let guard = NodeLeaseGuard::new(now, now + LEASE_MS)?;
        node.install_node_lease_for_startup(guard.clone())?;
        let fleet = self.nodes.fleet();
        let image = self.image_digest;
        let release = self.registry.release_digest();
        let modules = self.registry.module_digests();
        let endpoint = config.endpoint;
        let signing_endpoint = endpoint.clone();
        let sign = move |progress, now: i64| {
            NodeAdvertisement::sign(
                config.node,
                session,
                signing_endpoint.clone(),
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
        let observed = Arc::new(Mutex::new(self.nodes.create(sign(1, now)?, now).await?));
        let result = async {
            self.require_maintenance(operation).await?;
            let renewal = self.clone();
            let renewal_observed = Arc::clone(&observed);
            let renewal_stop = stop.clone();
            tasks.spawn(async move {
                let mut progress = 1_u64;
                loop {
                    tokio::select! {
                        () = renewal_stop.cancelled() => return Ok::<_, ServerError>(()),
                        () = tokio::time::sleep(RENEW_INTERVAL) => {},
                    }
                    renewal.require_maintenance(operation).await?;
                    guard.check()?;
                    progress = progress.checked_add(1).ok_or(ServerError::Clock)?;
                    let now = unix_now_ms()?;
                    let mut observed = renewal_observed.lock().await;
                    *observed = renewal
                        .nodes
                        .refresh(&observed, sign(progress, now)?, now)
                        .await?;
                    guard.renew(now, now + LEASE_MS)?;
                }
            })?;
            local.require_drain();
            node.start()?;
            // Fence advertisements even when their owner died before publishing
            // a catalog entry. Heartbeat expiry alone is never a drain proof.
            for previous in self.nodes.advertised_sessions(unix_now_ms()?, 4096).await? {
                if previous != session {
                    self.nodes
                        .claim_expired_for_takeover(previous, session, unix_now_ms()?)
                        .await?;
                }
            }
            let catalog = CellCatalog::new(self.layout.clone(), self.identity.tenant());
            let authority = CellAuthority::new(self.layout.clone());
            for shard in 0..=u8::MAX {
                let mut scan = catalog.scan_shard(shard).await?;
                while let Some(page) = scan.next_page().await? {
                    for proof in page.entries() {
                        self.require_maintenance(operation).await?;
                        let entry = proof.entry();
                        if authority
                            .load(entry.cell())
                            .await?
                            .is_some_and(|c| settled(c.value()))
                        {
                            continue;
                        }
                        let (module, schema, max_database_bytes) = if entry.namespace()
                            == directory::DIRECTORY
                        {
                            (DirectoryModule::NAME, directory::SCHEMA, 64 * 1024 * 1024)
                        } else if entry.namespace() == REPOSITORIES {
                            (
                                RepositoryModule::NAME,
                                include_str!("../schema.sql"),
                                REPOSITORY_DATABASE_LIMIT_BYTES,
                            )
                        } else {
                            return Err(Error::Release("unknown maintenance Cell namespace").into());
                        };
                        if !self.registry.is_current_cell(
                            entry.namespace(),
                            entry.role(),
                            entry.initial_code(),
                            entry.initial_schema(),
                        ) {
                            return Err(
                                Error::Release("maintenance Cell descriptor differs").into()
                            );
                        }
                        let target = CellTarget::new(
                            self.identity.tenant(),
                            self.identity.application(),
                            entry.namespace(),
                            entry.partition(),
                        )?;
                        let directory = local.path().join(hex::encode(entry.cell().as_bytes()));
                        tokio::fs::create_dir_all(&directory).await?;
                        let handle = acquire_provisioned_sql_cell(
                            &node,
                            &self.layout,
                            &self.nodes,
                            SqlCellSpec {
                                target: &target,
                                module,
                                schema,
                                max_database_bytes,
                                destination: directory.join("cell.sqlite"),
                            },
                            session,
                            &endpoint,
                            proof.clone(),
                        )
                        .await?;
                        handle.drain().await?;
                        // Only this worker's verified, closed restore scratch is
                        // removed. Runtime publication remains the durable authority.
                        tokio::fs::remove_dir_all(directory).await?;
                    }
                }
            }
            Ok::<_, ServerError>(())
        }
        .await;
        let drained = node.shutdown().await;
        if drained.is_ok() {
            local.confirm_drained();
        }
        stop.cancel();
        drained?;
        self.nodes
            .withdraw(&*observed.lock().await, unix_now_ms()?)
            .await?;
        result
    }
}
