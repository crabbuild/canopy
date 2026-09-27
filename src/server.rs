//! One leased Canopy node serving Repository Cells on demand.

use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{Arc, Weak},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use crab_cell_app::CellApplication;
use crab_cell_host::{CellNode, CellNodeBuilder};
use crab_cell_runtime::{
    ApplicationId, CellClient, CellModule, CellTarget, Digest, Error, InvocationError,
    MutationIdentity, NodeLeaseGuard, SessionId, TenantId, cell::application::ApplicationIdentity,
    cell::catalog::CatalogEntry, cell::catalog::CatalogRole, cell::catalog::CellCatalog,
    cell::worker::SqlWorkerPool, control::ControlState, control::Owner,
    control::authority::CellAuthority, identity::IncarnationId, identity::NodeId,
    identity::RequestId, ltx::CellStorageLayout, node::NodeAdvertisement, node::NodeCapacity,
    node::NodeDirectory, node::NodeFailureDomain, node::VersionedNodeAdvertisement,
    primitives::sql::SqlResultSet, recovery::manifest::RecoveryManifestStore,
    recovery::release::ReleaseStore,
};
use crab_ltx::{CellReplica, CrabError, DiskBudget, Host, Limits};
use crab_storage::{StorageError, Store};
use ed25519_dalek::SigningKey;
use object_store::{ObjectStore, path::Path as StorePath, prefix::PrefixStore};
use sha2::{Digest as _, Sha256};
use tokio::{
    net::TcpListener,
    sync::{Mutex, Semaphore, oneshot},
    task::JoinHandle,
};
use tokio_util::{
    sync::CancellationToken,
    task::{AbortOnDropHandle, TaskTracker},
};

use crate::{
    CanopyApplication, ReadIdentity, build_descriptor,
    deployment::Deployment,
    directory::{
        self, CreateAccountOutcome, DirectoryCell, DirectoryModule, Principal, RenameOutcome,
        RepositoryEntry, RepositoryState, TokenScope,
    },
    http,
    repository_http::RepositoryHttp,
};

mod discovery;
mod lifecycle;
pub(crate) mod peer;
mod request_trace;
mod residency;
pub(crate) mod storage;
mod tokens;
pub(crate) mod workspace;

use crate::admission::AccountAdmission;
use residency::LoadedRepository;
pub(crate) use residency::RepositoryRoute;

const MAX_PENDING_REPOSITORIES: usize = 32;
pub(crate) const LEASE_MS: i64 = 10_000;
pub(crate) const RENEW_INTERVAL: Duration = Duration::from_secs(3);

#[derive(Debug, thiserror::Error)]
pub enum ServerError {
    #[error("invalid SSH host key")]
    SshKey(#[source] Box<directory::SshKeyError>),
    #[error("Crab runtime failed")]
    Runtime(#[from] Error),
    #[error("Crab LTX failed")]
    Ltx(#[from] CrabError),
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
    #[error("repository directory operation failed")]
    Directory(#[from] InvocationError<Vec<SqlResultSet>>),
}

/// Required ownership and storage settings for a Canopy node.
pub struct ServerConfig {
    pub tenant: TenantId,
    pub application: ApplicationId,
    pub node: NodeId,
    pub fleet: Digest,
    pub image: Digest,
    pub signing_key: SigningKey,
    pub owner: String,
    pub token: String,
    pub public_url: String,
    pub peer_endpoint: String,
    pub peer_ca_pem: Option<Vec<u8>>,
    pub listen: std::net::SocketAddr,
    pub ssh: Option<crate::ssh::SshConfig>,
    pub data_dir: PathBuf,
    pub store_prefix: StorePath,
    pub local_disk_limit_bytes: u64,
    pub max_active_repositories: usize,
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

/// Controls a supervised node; dropping the handle requests graceful shutdown.
pub struct CanopyServer {
    address: std::net::SocketAddr,
    ssh_address: Option<std::net::SocketAddr>,
    shutdown: oneshot::Sender<()>,
    finished: JoinHandle<Result<(), ServerError>>,
}

struct RunningServer {
    address: std::net::SocketAddr,
    ssh_address: Option<std::net::SocketAddr>,
    node: Arc<CellNode>,
    directory: NodeDirectory,
    advertisement: Arc<Mutex<VersionedNodeAdvertisement>>,
    stop: CancellationToken,
    renewal: AbortOnDropHandle<Result<(), ServerError>>,
    ingress_stop: CancellationToken,
    release_stop: CancellationToken,
    serving: JoinHandle<std::io::Result<()>>,
    ssh_serving: Option<JoinHandle<std::io::Result<()>>>,
    tasks: TaskTracker,
    local: Arc<workspace::Workspace>,
}

pub(crate) struct RepositoryManager {
    directory: DirectoryCell,
    peer: peer::NodePeer,
    node: Arc<CellNode>,
    layout: CellStorageLayout,
    node_directory: NodeDirectory,
    tenant: TenantId,
    application: ApplicationId,
    session: SessionId,
    endpoint: String,
    local: Arc<workspace::Workspace>,
    external_store: Arc<dyn ObjectStore>,
    disk_budget: DiskBudget,
    pub(crate) owner: String,
    pub(crate) public_url: String,
    pub(crate) ready: Arc<dyn Fn() -> bool + Send + Sync>,
    loaded: Mutex<HashMap<[u8; 16], LoadedRepository>>,
    residency_transitions: Mutex<HashMap<[u8; 16], Weak<Mutex<()>>>>,
    residency_slots: Arc<Semaphore>,
    residency_admission: AccountAdmission,
    transfers: AccountAdmission,
    tasks: TaskTracker,
}

pub(crate) enum MembershipOutcome {
    Updated,
    RepositoryMissing,
    AccountMissing,
    Forbidden,
}

impl RepositoryManager {
    pub(crate) async fn ssh_identity(
        &self,
        key: &crate::directory::SshKey,
    ) -> Result<Option<crate::directory::SshIdentity>, ServerError> {
        Ok(self.directory.ssh_identity(key, None).await?.output)
    }

    pub(crate) async fn transfer_permit(
        &self,
        actor: ReadIdentity<'_>,
    ) -> Result<Arc<crate::AdmissionPermit>, Error> {
        if let Ok(permit) = self.transfers.acquire(actor).await {
            return Ok(Arc::new(permit));
        }
        // Both transports share bounded waits and the same account/node slots.
        let permit = tokio::time::timeout(Duration::from_secs(1), self.transfers.wait(actor))
            .await
            .map_err(|_| Error::Capacity("node transfers"))??;
        Ok(Arc::new(permit))
    }

    pub(crate) async fn authenticate(
        &self,
        token_digest: [u8; 32],
    ) -> Result<Option<Principal>, ServerError> {
        let started = Instant::now();
        let result = self.directory.authenticate(token_digest, None).await;
        tracing::debug!(
            stage = "directory_authentication",
            elapsed_seconds = started.elapsed().as_secs_f64(),
            succeeded = result.is_ok(),
            "repository request stage completed"
        );
        Ok(result?.output)
    }

    pub(crate) async fn create_account(
        &self,
        actor_digest: [u8; 32],
        name: &str,
        token_digest: [u8; 32],
        scope: TokenScope,
    ) -> Result<CreateAccountOutcome, ServerError> {
        Ok(self
            .directory
            .create_account_authorized(
                mutation_identity()?,
                directory::TokenAuthority {
                    actor_digest,
                    site_owner: &self.owner,
                    account: name,
                },
                token_digest,
                scope,
            )
            .await?
            .output)
    }

    pub(crate) async fn disable_account(
        &self,
        actor_digest: [u8; 32],
        account: &str,
    ) -> Result<directory::DisableAccountOutcome, ServerError> {
        Ok(self
            .directory
            .disable_account(
                mutation_identity()?,
                directory::TokenAuthority {
                    actor_digest,
                    site_owner: &self.owner,
                    account,
                },
            )
            .await?
            .output)
    }

    pub(crate) async fn create(
        self: &Arc<Self>,
        name: &str,
    ) -> Result<RepositoryEntry, ServerError> {
        let reserved = self
            .directory
            .reserve(
                mutation_identity()?,
                &self.owner,
                name,
                uuid::Uuid::new_v4().into_bytes(),
            )
            .await?
            .output;
        let _route = self.load((&self.owner).into(), reserved.clone()).await?;
        if reserved.state == RepositoryState::Ready {
            return Ok(reserved);
        }
        Ok(self
            .directory
            .activate(mutation_identity()?, &reserved)
            .await?
            .output)
    }

    pub(crate) async fn resolve(
        self: &Arc<Self>,
        actor: ReadIdentity<'_>,
        owner: &str,
        name: &str,
    ) -> Result<Option<RepositoryRoute>, ServerError> {
        if owner != self.owner {
            return Ok(None);
        }
        let Some(entry) = self.directory.lookup(owner, name, None).await?.output else {
            return Ok(None);
        };
        if entry.state != RepositoryState::Ready {
            return Ok(None);
        }
        Ok(Some(self.load(actor, entry).await?))
    }

    pub(crate) async fn update_member(
        self: &Arc<Self>,
        name: &str,
        actor: &str,
        account: &str,
        role: Option<TokenScope>,
    ) -> Result<MembershipOutcome, ServerError> {
        if role.is_some() && !self.directory.account_exists(account).await?.output {
            return Ok(MembershipOutcome::AccountMissing);
        }
        let Some(route) = self.resolve(actor.into(), &self.owner, name).await? else {
            return Ok(MembershipOutcome::RepositoryMissing);
        };
        // Publish the candidate before granting access. Retaining it on revoke
        // avoids a concurrent grant losing its index entry to delayed cleanup.
        if role.is_some()
            && !self
                .directory
                .remember_access(
                    mutation_identity()?,
                    actor,
                    account,
                    route.repository.repository_id(),
                )
                .await?
                .output
        {
            return Ok(MembershipOutcome::Forbidden);
        }
        let authorized = match role {
            Some(role) => {
                route
                    .repository
                    .grant_member(mutation_identity()?, actor, account, role)
                    .await?
                    .output
            }
            None => {
                route
                    .repository
                    .revoke_member(mutation_identity()?, actor, account)
                    .await?
                    .output
            }
        };
        Ok(if authorized {
            MembershipOutcome::Updated
        } else {
            MembershipOutcome::Forbidden
        })
    }

    pub(crate) async fn rename(
        &self,
        old_name: &str,
        new_name: &str,
        repository_id: [u8; 16],
    ) -> Result<RenameOutcome, ServerError> {
        Ok(self
            .directory
            .rename(
                mutation_identity()?,
                &self.owner,
                old_name,
                new_name,
                repository_id,
            )
            .await?
            .output)
    }
}

impl RunningServer {
    async fn start(
        config: ServerConfig,
        raw_store: Arc<dyn ObjectStore>,
    ) -> Result<Self, ServerError> {
        // Crab permits 10,000 active Cells; reserve one for Directory takeover.
        if !(1..10_000).contains(&config.max_active_repositories) {
            return Err(ServerError::Http(
                "max_active_repositories must be between 1 and 9999",
            ));
        }
        directory::validate_component(&config.owner)
            .map_err(|_| ServerError::Http("invalid repository owner"))?;
        if config.token.is_empty() {
            return Err(ServerError::Http("Git access token is required"));
        }
        http::validate_public_url(&config.public_url).map_err(ServerError::Http)?;
        let data_dir = config.data_dir.clone();
        let local = Arc::new(
            tokio::task::spawn_blocking(move || workspace::Workspace::open(&data_dir)).await??,
        );
        let store = Store::new(Arc::clone(&raw_store));
        storage::probe(&store, &config.store_prefix.clone().join("canopy-probe")).await?;
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
        let deployment = Deployment::new(
            layout.store().clone(),
            config.store_prefix.clone(),
            ApplicationIdentity::new(config.tenant, config.application),
            config.fleet,
            config.image,
            registry.clone(),
        )?;
        deployment.initialize().await?;
        let release_stop = CancellationToken::new();
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
            signing_key: config.signing_key.clone(),
        };
        let directory = NodeDirectory::new(
            layout.clone(),
            identity.fleet,
            identity.image,
            identity.release,
        );
        let listener = TcpListener::bind(config.listen).await?;
        let mut config = config;
        let address = listener.local_addr()?;
        let ssh_listener = if let Some(ssh) = &config.ssh {
            if ssh.host_key.is_encrypted() {
                return Err(ServerError::Http("SSH host key must be decrypted"));
            }
            let public = ssh.host_key.public_key().to_openssh().map_err(|error| {
                ServerError::SshKey(Box::new(directory::SshKeyError::Encoding(error)))
            })?;
            directory::SshKey::parse(&public)
                .map_err(|error| ServerError::SshKey(Box::new(error)))?;
            Some(TcpListener::bind(ssh.listen).await?)
        } else {
            None
        };
        let ssh_address = ssh_listener
            .as_ref()
            .map(TcpListener::local_addr)
            .transpose()?;
        let ssh_config = config.ssh.take();
        let disk_budget = DiskBudget::new(config.local_disk_limit_bytes);
        let node = Arc::new(
            CellNodeBuilder::new(Arc::clone(&application))
                .with_runtime(
                    SqlWorkerPool::for_system(config.max_active_repositories + 1)?,
                    64 * 1024 * 1024,
                )
                .with_replica_host(Host::default().with_local_disk_budget(disk_budget.clone()))
                .with_session(session)
                .build()?,
        );
        let stop = CancellationToken::new();
        node.install_task_group(CancellationToken::new(), release_stop.clone())?;
        // Preflight may outlast a lease. Start its lifetime only when enrollment
        // begins, so slow probing cannot publish an already-expired advertisement.
        let now_ms = unix_now_ms()?;
        let guard = NodeLeaseGuard::new(now_ms, now_ms + LEASE_MS)?;
        node.install_node_lease_for_startup(guard.clone())?;
        let observed = directory.create(identity.sign(1, now_ms)?, now_ms).await?;
        let advertisement = Arc::new(Mutex::new(observed));
        let tasks = TaskTracker::new();
        let renewal_directory = directory.clone();
        let renewal_observed = Arc::clone(&advertisement);
        let renewal_stop = stop.clone();
        let renewal_guard = guard.clone();
        let renewal_deployment = deployment.clone();
        let renewal_release_stop = release_stop.clone();
        // CellNode cancels its task group before SQL drain. The process supervisor
        // owns renewal so a long drain cannot expire the authority it is releasing.
        let renewal = AbortOnDropHandle::new(tokio::spawn(async move {
            let _stop_node = renewal_release_stop.clone().drop_guard();
            renew_lease(
                renewal_directory,
                renewal_observed,
                identity,
                renewal_guard,
                renewal_stop,
                renewal_deployment,
                renewal_release_stop,
            )
            .await
        }));
        let startup = async {
            deployment.require_ready().await?;
            let directory_target = directory::directory_target(config.tenant, config.application)?;
            local.require_drain();
            let peer = peer::NodePeer::new(
                &config,
                Arc::clone(&node),
                layout.clone(),
                directory.clone(),
                Arc::clone(&local),
                tasks.clone(),
                session,
            )?;
            peer.ensure_directory().await?;
            let directory_application = node.application_handle::<CanopyApplication>(
                peer.client(),
                config.tenant,
                config.application,
            )?;
            let directory_cell = DirectoryCell::new(&directory_application, directory_target)?;
            let external_store: Arc<dyn ObjectStore> =
                Arc::new(PrefixStore::new(raw_store, config.store_prefix));
            let ready_node = Arc::clone(&node);
            let ready_release = release_stop.clone();
            let ready: Arc<dyn Fn() -> bool + Send + Sync> = Arc::new(move || {
                !ready_release.is_cancelled() && ready_node.is_ready() && guard.check().is_ok()
            });
            let token_digest = Sha256::digest(config.token.as_bytes()).into();
            node.start()?;
            if !matches!(
                directory_cell
                    .create_account(
                        mutation_identity()?,
                        &config.owner,
                        token_digest,
                        TokenScope::Admin,
                    )
                    .await?
                    .output,
                CreateAccountOutcome::Created(_)
            ) {
                return Err(ServerError::Repository(
                    "bootstrap account token differs from persisted identity",
                ));
            }
            let manager = Arc::new(RepositoryManager {
                directory: directory_cell,
                peer: peer.clone(),
                node: Arc::clone(&node),
                layout: layout.clone(),
                node_directory: directory.clone(),
                tenant: config.tenant,
                application: config.application,
                session,
                endpoint: config.peer_endpoint,
                local: Arc::clone(&local),
                external_store,
                disk_budget,
                owner: config.owner,
                public_url: config.public_url,
                ready,
                loaded: Mutex::new(HashMap::new()),
                residency_transitions: Mutex::new(HashMap::new()),
                residency_slots: Arc::new(Semaphore::new(config.max_active_repositories)),
                transfers: AccountAdmission::new(8, "node transfers", "account transfers"),
                residency_admission: AccountAdmission::new(
                    MAX_PENDING_REPOSITORIES,
                    "pending repository activations",
                    "account repository activations",
                ),
                tasks: tasks.clone(),
            });
            let api = Arc::new(RepositoryHttp::new(Arc::clone(&manager), tasks.clone()));
            deployment.require_ready().await?;
            Ok::<_, ServerError>((api, peer, manager))
        }
        .await;
        let (api, peer, manager) = match startup {
            Ok(api) => api,
            Err(error) => {
                match node.shutdown().await {
                    Ok(()) => local.confirm_drained(),
                    Err(cleanup) => tracing::error!(error = %cleanup, "startup drain failed"),
                }
                stop.cancel();
                if let Err(cleanup) = renewal
                    .await
                    .map_err(ServerError::from)
                    .and_then(|result| result)
                {
                    tracing::error!(error = %cleanup, "startup lease renewal failed");
                }
                let observed = advertisement.lock().await;
                let _ = directory.withdraw(&observed, unix_now_ms()?).await;
                return Err(error);
            }
        };
        let ingress_stop = CancellationToken::new();
        let ssh_serving = match (ssh_config, ssh_listener) {
            (Some(config), Some(listener)) => {
                let stop = ingress_stop.clone();
                let tasks = tasks.clone();
                let release = release_stop.clone();
                Some(tokio::spawn(async move {
                    let _release = release.drop_guard();
                    crate::ssh::serve(config, listener, manager, tasks, stop).await
                }))
            }
            _ => None,
        };
        let serving_stop = ingress_stop.clone();
        let serving = tokio::spawn(async move {
            let peer_routes = axum::Router::new()
                .route(peer::PATH, axum::routing::post(peer::serve))
                .with_state(peer);
            let routes = api
                .router()
                .merge(peer_routes)
                .layer(axum::middleware::from_fn(request_trace::trace));
            axum::serve(listener, routes)
                .with_graceful_shutdown(serving_stop.cancelled_owned())
                .await
        });
        Ok(Self {
            address,
            ssh_address,
            ssh_serving,
            node,
            directory,
            advertisement,
            stop,
            renewal,
            ingress_stop,
            release_stop,
            serving,
            tasks,
            local,
        })
    }
}

async fn renew_lease(
    directory: NodeDirectory,
    observed: Arc<Mutex<VersionedNodeAdvertisement>>,
    identity: AdvertisementIdentity,
    guard: NodeLeaseGuard,
    stop: CancellationToken,
    deployment: Deployment,
    release_stop: CancellationToken,
) -> Result<(), ServerError> {
    let mut progress = 1_u64;
    loop {
        tokio::select! {
            () = stop.cancelled() => return Ok(()),
            () = tokio::time::sleep(RENEW_INTERVAL) => {}
        }
        if !release_stop.is_cancelled()
            && let Err(error) = deployment.require_ready().await
        {
            tracing::warn!(error = %error, "deployment admission closed; draining node");
            release_stop.cancel();
        }
        // Continue renewing while accepted work drains. Withdrawal follows SQL
        // close, so maintenance never mistakes heartbeat expiry for closed writers.
        guard.check()?;
        progress = progress.checked_add(1).ok_or(ServerError::Clock)?;
        let now_ms = unix_now_ms()?;
        let next = identity.sign(progress, now_ms)?;
        let mut current = observed.lock().await;
        match directory.refresh(&current, next, now_ms).await {
            Ok(refreshed) => {
                *current = refreshed;
                renew_node_lease(&guard, &current)?;
            }
            Err(error) => tracing::warn!(error = %error, "node lease renewal failed"),
        }
    }
}

pub(crate) fn renew_node_lease(
    guard: &NodeLeaseGuard,
    observed: &VersionedNodeAdvertisement,
) -> Result<(), ServerError> {
    // Storage latency consumes the signed lease. Using its issuance timestamp
    // here would add that latency back to the runtime's monotonic deadline.
    guard.renew(unix_now_ms()?, observed.advertisement().expires_at_ms())?;
    Ok(())
}

pub(crate) struct SqlCellSpec<'a> {
    pub(crate) target: &'a CellTarget,
    pub(crate) module: &'static str,
    pub(crate) schema: &'static str,
    pub(crate) destination: PathBuf,
}

async fn acquire_sql_cell(
    node: &CellNode,
    layout: &CellStorageLayout,
    directory: &NodeDirectory,
    spec: SqlCellSpec<'_>,
    session: SessionId,
    endpoint: &str,
) -> Result<crab_cell_runtime::cell::actor::CellHandle, ServerError> {
    let target = spec.target;
    let registry = node.application().registry();
    let code = registry
        .module_code(spec.module)
        .ok_or(ServerError::Repository("SQL module is absent"))?;
    let catalog = CellCatalog::new(layout.clone(), target.tenant());
    let releases = ReleaseStore::new(
        layout.clone(),
        ApplicationIdentity::new(target.tenant(), target.application()),
    )?;
    let proof = releases
        .provision(
            &catalog,
            &registry,
            CatalogEntry::new(target, CatalogRole::Sql, code, 1)?,
        )
        .await?;
    acquire_provisioned_sql_cell(node, layout, directory, spec, session, endpoint, proof).await
}

pub(crate) async fn acquire_provisioned_sql_cell(
    node: &CellNode,
    layout: &CellStorageLayout,
    directory: &NodeDirectory,
    spec: SqlCellSpec<'_>,
    session: SessionId,
    endpoint: &str,
    proof: crab_cell_runtime::cell::catalog::CatalogProof,
) -> Result<crab_cell_runtime::cell::actor::CellHandle, ServerError> {
    let SqlCellSpec {
        target,
        module: _,
        schema,
        destination,
    } = spec;
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
    let cell_type = node
        .application()
        .cell_types()
        .iter()
        .find(|cell_type| cell_type.namespace() == target.namespace())
        .ok_or(Error::Control(
            "Cell namespace is not declared by application",
        ))?;
    // Acquisition and takeover must use the exact limits validated by the host.
    // Reading the declaration avoids separate serving/recovery limit policies.
    let limits = Limits {
        max_database_bytes: cell_type.database_limit_bytes(),
        max_capture_bytes: cell_type.capture_limit_bytes(),
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
                        transaction.execute_batch(schema)?;
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
                            transaction.execute_batch(schema)?;
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

pub(crate) fn unix_now_ms() -> Result<i64, ServerError> {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| ServerError::Clock)?
            .as_millis(),
    )
    .map_err(|_| ServerError::Clock)
}

pub(crate) fn mutation_identity() -> Result<MutationIdentity, ServerError> {
    let now_ms = unix_now_ms()?;
    Ok(MutationIdentity {
        request_id: RequestId::from_bytes(uuid::Uuid::new_v4().into_bytes()),
        issued_at_ms: now_ms,
        expires_at_ms: now_ms.checked_add(60_000).ok_or(ServerError::Clock)?,
    })
}
