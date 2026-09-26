//! One leased Canopy node serving one Repository Cell.

use std::{
    path::PathBuf,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use cellule_app::CellApplication;
use cellule_host::{CellNode, CellNodeBuilder};
use cellule_ltx::{CellReplica, DiskBudget, Host, Limits, LtxError};
use cellule_runtime::{
    ApplicationId, CatalogEntry, CatalogRole, CellAuthority, CellCatalog, CellClient, CellModule,
    CellStorageLayout, CellTarget, ControlState, Digest, Error, IncarnationId, NodeAdvertisement,
    NodeCapacity, NodeDirectory, NodeFailureDomain, NodeId, NodeLeaseGuard, Owner,
    RecoveryManifestStore, SessionId, SqlWorkerPool, TenantId, VersionedNodeAdvertisement,
};
use cellule_store::{StorageError, Store, probe_storage};
use ed25519_dalek::SigningKey;
use object_store::{ObjectStore, path::Path as StorePath, prefix::PrefixStore};
use tokio::{net::TcpListener, sync::Mutex, task::JoinHandle};
use tokio_util::sync::CancellationToken;

use crate::{
    CanopyApplication, REPOSITORY_DATABASE_LIMIT_BYTES, RepositoryCell, RepositoryModule,
    build_descriptor, git_gateway::GitGateway, http::GitHttpApi, repository_target,
};

const LEASE_MS: i64 = 10_000;
const RENEW_INTERVAL: Duration = Duration::from_secs(3);

#[derive(Debug, thiserror::Error)]
pub enum ServerError {
    #[error("Cellule runtime failed")]
    Runtime(#[from] Error),
    #[error("Cellule LTX failed")]
    Ltx(#[from] LtxError),
    #[error("object storage failed")]
    Storage(#[from] StorageError),
    #[error("server I/O failed")]
    Io(#[from] std::io::Error),
    #[error("server task failed")]
    Task(#[from] tokio::task::JoinError),
    #[error("system time cannot be represented")]
    Clock,
    #[error("repository startup failed: {0}")]
    Repository(&'static str),
    #[error("HTTP configuration is invalid: {0}")]
    Http(&'static str),
}

/// Required ownership and storage settings for a single-repository node.
pub struct SingleRepositoryConfig {
    pub tenant: TenantId,
    pub application: ApplicationId,
    pub repository: [u8; 16],
    pub node: NodeId,
    pub fleet: Digest,
    pub image: Digest,
    pub signing_key: SigningKey,
    pub owner: String,
    pub token: String,
    pub public_url: String,
    pub peer_endpoint: String,
    pub listen: std::net::SocketAddr,
    pub data_dir: PathBuf,
    pub store_prefix: StorePath,
    pub local_disk_limit_bytes: u64,
}

struct AdvertisementIdentity {
    node: NodeId,
    session: SessionId,
    endpoint: String,
    fleet: Digest,
    certificate: Digest,
    image: Digest,
    release: Digest,
    module_digests: Vec<Digest>,
    signing_key: SigningKey,
}

impl AdvertisementIdentity {
    fn sign(&self, progress: u64, now_ms: i64) -> Result<NodeAdvertisement, ServerError> {
        Ok(NodeAdvertisement::sign(
            self.node,
            self.session,
            self.endpoint.clone(),
            self.fleet,
            self.certificate,
            self.image,
            self.release,
            &self.signing_key,
            progress,
            now_ms,
            now_ms.checked_add(LEASE_MS).ok_or(ServerError::Clock)?,
            self.module_digests.clone(),
            vec![1],
            NodeFailureDomain::default(),
            NodeCapacity::default(),
        )?)
    }
}

/// Owns the HTTP listener, leased Cell node, and its shutdown order.
pub struct SingleRepositoryServer {
    address: std::net::SocketAddr,
    node: Arc<CellNode>,
    directory: NodeDirectory,
    advertisement: Arc<Mutex<VersionedNodeAdvertisement>>,
    stop: CancellationToken,
    ingress_stop: CancellationToken,
    serving: JoinHandle<std::io::Result<()>>,
    _local: tempfile::TempDir,
}

impl SingleRepositoryServer {
    /// Starts a node only after storage fencing, authority and Git ingress are ready.
    pub async fn start(
        config: SingleRepositoryConfig,
        raw_store: Arc<dyn ObjectStore>,
    ) -> Result<Self, ServerError> {
        repository_target(config.tenant, config.application, config.repository)?;
        std::fs::create_dir_all(&config.data_dir)?;
        let local = tempfile::TempDir::new_in(&config.data_dir)?;
        let store = Store::new(Arc::clone(&raw_store));
        let now_ms = unix_now_ms()?;
        let probe = probe_storage(
            &store,
            &config.store_prefix.clone().join("canopy-probe"),
            now_ms,
        )
        .await?;
        if !probe.passed() {
            tracing::error!(failed = ?probe.failed_checks(), "storage capability probe failed");
            return Err(ServerError::Repository(
                "storage does not support Cell fencing",
            ));
        }
        let application = Arc::new(CanopyApplication::compile(build_descriptor(
            include_bytes!("../Cargo.lock"),
            env!("CARGO_PKG_VERSION"),
        ))?);
        let registry = application.registry();
        let layout = CellStorageLayout::new(
            store,
            config.store_prefix.clone(),
            *config.application.as_bytes(),
        );
        let session = SessionId::from_bytes(uuid::Uuid::new_v4().into_bytes());
        let identity = AdvertisementIdentity {
            node: config.node,
            session,
            endpoint: config.peer_endpoint.clone(),
            fleet: config.fleet,
            certificate: Digest::from_bytes(
                *blake3::hash(config.signing_key.verifying_key().as_bytes()).as_bytes(),
            ),
            image: config.image,
            release: registry.release_digest(),
            module_digests: registry.module_digests(),
            signing_key: config.signing_key,
        };
        let directory = NodeDirectory::new(
            layout.clone(),
            identity.fleet,
            identity.image,
            identity.release,
        );
        let listener = TcpListener::bind(config.listen).await?;
        let address = listener.local_addr()?;
        let node = Arc::new(
            CellNodeBuilder::new(Arc::clone(&application))
                .with_runtime(SqlWorkerPool::new(1, 4)?, 64 * 1024 * 1024)
                .with_replica_host(
                    Host::default()
                        .with_local_disk_budget(DiskBudget::new(config.local_disk_limit_bytes)),
                )
                .with_session(session)
                .build()?,
        );
        let stop = CancellationToken::new();
        let tasks = node.install_task_group(stop.clone(), stop.clone())?;
        node.require_storage_capabilities(&probe)?;
        let observed = directory.create(identity.sign(1, now_ms)?, now_ms).await?;
        let advertisement = Arc::new(Mutex::new(observed));
        let startup = async {
            let guard = NodeLeaseGuard::new(now_ms, now_ms + LEASE_MS)?;
            node.install_node_lease_for_startup(guard.clone())?;
            let renewal_directory = directory.clone();
            let renewal_observed = Arc::clone(&advertisement);
            let renewal_stop = stop.clone();
            let renewal_guard = guard.clone();
            tasks.spawn(async move {
                renew_lease(
                    renewal_directory,
                    renewal_observed,
                    identity,
                    renewal_guard,
                    renewal_stop,
                )
                .await
            })?;
            let target = repository_target(config.tenant, config.application, config.repository)?;
            let handle = acquire_repository(
                &node,
                &layout,
                &directory,
                &target,
                session,
                &config.peer_endpoint,
                local.path().join("repository.sqlite"),
            )
            .await?;
            let application_handle = node.application_handle::<CanopyApplication>(
                CellClient::local(registry, handle),
                config.tenant,
                config.application,
            );
            let repository = Arc::new(RepositoryCell::new(&application_handle, target)?);
            // Both Cell state and external bodies share the configured storage prefix.
            let external_store: Arc<dyn ObjectStore> =
                Arc::new(PrefixStore::new(raw_store, config.store_prefix));
            let gateway = Arc::new(GitGateway::new(
                repository,
                local.path().to_path_buf(),
                external_store,
            ));
            let ready_node = Arc::clone(&node);
            let ready = Arc::new(move || ready_node.is_ready() && guard.check().is_ok());
            let api = Arc::new(
                GitHttpApi::new(
                    gateway,
                    config.owner,
                    &config.token,
                    &config.public_url,
                    ready,
                )
                .map_err(ServerError::Http)?,
            );
            node.start()?;
            Ok::<_, ServerError>(api)
        }
        .await;
        let api = match startup {
            Ok(api) => api,
            Err(error) => {
                let _ = node.shutdown().await;
                let observed = advertisement.lock().await;
                let _ = directory.withdraw(&observed, unix_now_ms()?).await;
                return Err(error);
            }
        };
        let ingress_stop = CancellationToken::new();
        let serving_stop = ingress_stop.clone();
        let serving = tokio::spawn(async move {
            axum::serve(listener, api.router())
                .with_graceful_shutdown(serving_stop.cancelled_owned())
                .await
        });
        Ok(Self {
            address,
            node,
            directory,
            advertisement,
            stop,
            ingress_stop,
            serving,
            _local: local,
        })
    }

    #[must_use]
    pub const fn local_addr(&self) -> std::net::SocketAddr {
        self.address
    }

    /// Stops ingress, drains the Cell node, then withdraws its directory lease.
    pub async fn shutdown(self) -> Result<(), ServerError> {
        self.ingress_stop.cancel();
        let serving = self.serving.await;
        let drained = self.node.shutdown().await;
        self.stop.cancel();
        let observed = self.advertisement.lock().await;
        let withdrawn = match unix_now_ms() {
            Ok(now_ms) => self.directory.withdraw(&observed, now_ms).await,
            Err(error) => return Err(error),
        };
        serving??;
        drained?;
        withdrawn?;
        Ok(())
    }
}

async fn renew_lease(
    directory: NodeDirectory,
    observed: Arc<Mutex<VersionedNodeAdvertisement>>,
    identity: AdvertisementIdentity,
    guard: NodeLeaseGuard,
    stop: CancellationToken,
) -> Result<(), ServerError> {
    let mut progress = 1_u64;
    loop {
        tokio::select! {
            () = stop.cancelled() => return Ok(()),
            () = tokio::time::sleep(RENEW_INTERVAL) => {}
        }
        guard.check()?;
        progress = progress.checked_add(1).ok_or(ServerError::Clock)?;
        let now_ms = unix_now_ms()?;
        let next = identity.sign(progress, now_ms)?;
        let mut current = observed.lock().await;
        match directory.refresh(&current, next, now_ms).await {
            Ok(refreshed) => {
                *current = refreshed;
                guard.renew(now_ms, now_ms + LEASE_MS)?;
            }
            Err(error) => tracing::warn!(error = %error, "node lease renewal failed"),
        }
    }
}

async fn acquire_repository(
    node: &CellNode,
    layout: &CellStorageLayout,
    directory: &NodeDirectory,
    target: &CellTarget,
    session: SessionId,
    endpoint: &str,
    destination: PathBuf,
) -> Result<cellule_runtime::CellHandle, ServerError> {
    let registry = node.application().registry();
    let code = registry
        .module_code(RepositoryModule::NAME)
        .ok_or(ServerError::Repository("repository module is absent"))?;
    let proof = CellCatalog::new(layout.clone(), target.tenant())
        .provision(CatalogEntry::new(target, CatalogRole::Sql, code, 1)?)
        .await?;
    let authority = CellAuthority::new(layout.clone());
    let owner = Owner {
        session,
        endpoint: endpoint.to_owned(),
    };
    let observed = match authority.load(target.cell_id()).await? {
        Some(observed) => observed,
        None => {
            authority
                .create_initial(
                    &proof,
                    IncarnationId::from_bytes(uuid::Uuid::new_v4().into_bytes()),
                    owner.clone(),
                )
                .await?
        }
    };
    let limits = Limits {
        max_database_bytes: REPOSITORY_DATABASE_LIMIT_BYTES,
        ..Limits::default()
    };
    let replica = CellReplica::new(
        layout.clone(),
        *target.cell_id().as_bytes(),
        *observed.value().incarnation.as_bytes(),
        limits,
    )?;
    let runtime = node.runtime();
    match (observed.value().state, observed.value().root.is_some()) {
        (ControlState::Recovering, false) if observed.value().owner.as_ref() == Some(&owner) => {
            Ok(runtime
                .bootstrap(
                    proof,
                    replica,
                    authority,
                    observed,
                    destination,
                    |transaction| {
                        transaction.execute_batch(include_str!("schema.sql"))?;
                        Ok(())
                    },
                )
                .await?)
        }
        (ControlState::Idle, true) => Ok(runtime
            .acquire_idle_restored(proof, replica, authority, observed, destination, owner)
            .await?),
        (ControlState::Recovering | ControlState::Serving, _) => {
            let previous = observed
                .value()
                .owner
                .as_ref()
                .ok_or(ServerError::Repository("published Cell has no owner"))?
                .session;
            let takeover = directory
                .claim_expired_for_takeover(previous, session, unix_now_ms()?)
                .await?;
            if observed.value().root.is_some() {
                Ok(runtime
                    .takeover_restored(
                        proof,
                        replica,
                        authority,
                        observed,
                        takeover,
                        RecoveryManifestStore::new(layout.clone(), limits),
                        destination,
                        owner,
                    )
                    .await?)
            } else {
                Ok(runtime
                    .takeover_unpublished(
                        proof,
                        replica,
                        authority,
                        observed,
                        takeover,
                        destination,
                        owner,
                        |transaction| {
                            transaction.execute_batch(include_str!("schema.sql"))?;
                            Ok(())
                        },
                    )
                    .await?)
            }
        }
        _ => Err(ServerError::Repository(
            "repository Cell cannot be acquired",
        )),
    }
}

fn unix_now_ms() -> Result<i64, ServerError> {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| ServerError::Clock)?
            .as_millis(),
    )
    .map_err(|_| ServerError::Clock)
}
