//! Bounded repository residency with request pins and confirmed Cell release.

use std::{
    collections::{HashMap, HashSet},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Instant,
};

use axum::{
    Router,
    body::{Body, Bytes, HttpBody},
    http::{Request, Response},
};
use cellule_runtime::{
    CellClient, CellId, CellModule, CellTarget, Error, cell::catalog::CatalogRole,
};
use http_body::{Frame, SizeHint};
use tokio::sync::{Mutex, OwnedSemaphorePermit};
use tower::ServiceExt;

use super::{RepositoryManager, ServerError, SqlCellSpec, acquire_sql_cell, mutation_identity};
use crate::{
    CanopyApplication, ReadIdentity, RepositoryCell, RepositoryModule,
    directory::{RepositoryEntry, RepositoryState},
    git_gateway::GitGateway,
    http::GitHttpApi,
    repository_target,
};

pub(super) struct LoadedRepository {
    repository: Arc<RepositoryCell>,
    gateway: Arc<GitGateway>,
    name: String,
    router: Router,
    pin: Arc<()>,
    last_used: Instant,
    initialized: bool,
    local: bool,
    state: ResidencyState,
    slot: Arc<OwnedSemaphorePermit>,
}

enum EvictionAction {
    DropRemote,
    Cleanup,
    Release { cell: CellId, generation: u64 },
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ResidencyState {
    Serving,
    Releasing,
    RefreshHandle,
    Released,
}

#[cfg(test)]
mod tests;

pub(crate) struct RepositoryRoute {
    pub(crate) repository: Arc<RepositoryCell>,
    pub(crate) gateway: Arc<GitGateway>,
    router: Router,
    pin: Arc<()>,
}

impl RepositoryRoute {
    pub(crate) async fn dispatch(self, request: Request<Body>) -> Response<Body> {
        let response = match self.router.oneshot(request).await {
            Ok(response) => response,
            Err(error) => match error {},
        };
        // A streamed reply outlives the handler. Keep its Cell resident until the
        // body finishes or is dropped; cache generations have their own worker pins.
        response.map(|body| {
            Body::new(PinnedBody {
                body,
                pin: Some(self.pin),
            })
        })
    }
}

struct PinnedBody {
    body: Body,
    pin: Option<Arc<()>>,
}

impl HttpBody for PinnedBody {
    type Data = Bytes;
    type Error = axum::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
        let result = Pin::new(&mut self.body).poll_frame(cx);
        if matches!(&result, Poll::Ready(None | Some(Err(_)))) || self.body.is_end_stream() {
            self.pin = None;
        }
        result
    }

    fn is_end_stream(&self) -> bool {
        self.body.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.body.size_hint()
    }
}

impl RepositoryManager {
    pub(super) async fn load(
        self: &Arc<Self>,
        actor: ReadIdentity<'_>,
        entry: RepositoryEntry,
    ) -> Result<RepositoryRoute, ServerError> {
        if let Some(route) = self.local_route(&entry).await? {
            return Ok(route);
        }
        let admission = self.residency_admission.acquire(actor).await?;
        let queued = Instant::now();
        let manager = Arc::clone(self);
        // Client cancellation must not abandon a release or acquisition halfway
        // through. Shutdown waits for these tracked tasks before draining.
        self.tasks
            .spawn(async move {
                // Retain queue capacity through cancellation and transition cleanup.
                // Releasing it with the HTTP waiter would allow unbounded detached work.
                let _admission = admission;
                let transition = manager.transition_lock(entry.repository_id).await;
                let _change = transition.lock().await;
                let queue_seconds = queued.elapsed().as_secs_f64();
                let started = Instant::now();
                let result = manager.load_repository(&entry).await;
                tracing::debug!(
                    repository = %hex::encode(entry.repository_id),
                    queue_seconds,
                    transition_seconds = started.elapsed().as_secs_f64(),
                    succeeded = result.is_ok(),
                    "repository transition completed"
                );
                result
            })
            .await?
    }

    async fn local_route(
        &self,
        entry: &RepositoryEntry,
    ) -> Result<Option<RepositoryRoute>, ServerError> {
        let mut loaded = self.loaded.lock().await;
        let Some(repository) = loaded.get_mut(&entry.repository_id).filter(|repository| {
            repository.local
                && repository.initialized
                && repository.state == ResidencyState::Serving
        }) else {
            return Ok(None);
        };
        self.route(entry, repository).map(Some)
    }

    async fn transition_lock(&self, id: [u8; 16]) -> Arc<Mutex<()>> {
        let mut transitions = self.residency_transitions.lock().await;
        // Only admitted tasks retain locks. Weak entries avoid accumulating one
        // mutex per historical repository; live entries are bounded by admission.
        transitions.retain(|_, lock| lock.strong_count() != 0);
        if let Some(lock) = transitions.get(&id).and_then(std::sync::Weak::upgrade) {
            return lock;
        }
        let lock = Arc::new(Mutex::new(()));
        transitions.insert(id, Arc::downgrade(&lock));
        lock
    }

    // The repository guard serializes its own ownership changes. Slots are
    // reserved before storage I/O, including while an activation is in flight.
    async fn load_repository(
        &self,
        entry: &RepositoryEntry,
    ) -> Result<RepositoryRoute, ServerError> {
        if let Some(route) = self.local_route(entry).await? {
            return Ok(route);
        }
        let target = repository_target(self.tenant, self.application, entry.repository_id)?;
        let mut reclaimed = None;
        let remote_route = self
            .loaded
            .lock()
            .await
            .get(&entry.repository_id)
            .is_some_and(|repository| !repository.local);
        if remote_route && !self.peer.remote_owner(&target).await? {
            // Remote cache ownership is disposable. Reacquire idle/expired Cell
            // authority locally before binding a new route after owner loss.
            let removed = self.loaded.lock().await.remove(&entry.repository_id);
            reclaimed = removed.map(|repository| repository.slot);
        }
        let state = self
            .loaded
            .lock()
            .await
            .get(&entry.repository_id)
            .map(|repository| repository.state);
        match state {
            Some(ResidencyState::Released) => {
                reclaimed = Some(self.cleanup_released(entry.repository_id).await?);
            }
            Some(ResidencyState::RefreshHandle) => {
                let target = repository_target(self.tenant, self.application, entry.repository_id)?;
                // Transfer preflight can close the old capability even when release
                // fails. Only the runtime may provide a fresh serving capability.
                let handle = self
                    .node
                    .runtime()
                    .resident_handle(&target, CatalogRole::Sql)
                    .await?
                    .ok_or(ServerError::Repository(
                        "Cell release failed; restart the node to recover",
                    ))?;
                let mut loaded = self.loaded.lock().await;
                let slot = Arc::clone(
                    &loaded
                        .get(&entry.repository_id)
                        .ok_or(ServerError::Repository("loaded repository is absent"))?
                        .slot,
                );
                let previous = loaded.insert(
                    entry.repository_id,
                    self.bind_repository(
                        entry,
                        target,
                        CellClient::local(self.node.application().registry(), handle),
                        true,
                        slot,
                    )?,
                );
                drop(loaded);
                drop(previous);
            }
            _ => {}
        }
        let present = self.loaded.lock().await.contains_key(&entry.repository_id);
        if !present {
            let slot = match reclaimed {
                Some(slot) => slot,
                None => match Arc::clone(&self.residency_slots).try_acquire_owned() {
                    Ok(slot) => Arc::new(slot),
                    Err(_) => self.evict_repository().await?,
                },
            };
            let mut remote = self.peer.remote_owner(&target).await?;
            let client = if remote {
                self.peer.client()
            } else {
                let directory = self.local.path().join(hex::encode(entry.repository_id));
                tokio::fs::create_dir_all(&directory).await?;
                let started = Instant::now();
                let acquired = acquire_sql_cell(
                    &self.node,
                    &self.layout,
                    &self.node_directory,
                    SqlCellSpec {
                        target: &target,
                        module: RepositoryModule::NAME,
                        schema: include_str!("../../schema.sql"),
                        destination: directory.join("repository.sqlite"),
                    },
                    self.session,
                    &self.endpoint,
                )
                .await;
                match acquired {
                    Ok(handle) => {
                        tracing::debug!(repository = %hex::encode(entry.repository_id), elapsed_seconds = started.elapsed().as_secs_f64(), "acquired repository Cell");
                        CellClient::local(self.node.application().registry(), handle)
                    }
                    Err(error) => {
                        // Another gateway can win after our initial owner read.
                        // No repository command has been sent by this request.
                        // Follow only freshly validated, live remote authority;
                        // never retry the claim or replay an accepted mutation.
                        if !matches!(self.peer.remote_owner(&target).await, Ok(true)) {
                            return Err(error);
                        }
                        remote = true;
                        self.peer.client()
                    }
                }
            };
            self.loaded.lock().await.insert(
                entry.repository_id,
                self.bind_repository(entry, target, client, !remote, slot)?,
            );
        }
        let initialize = self
            .loaded
            .lock()
            .await
            .get(&entry.repository_id)
            .filter(|repository| !repository.initialized)
            .map(|repository| Arc::clone(&repository.repository));
        if let Some(repository) = initialize {
            // Keep the acquired Cell through an uncertain initialization result.
            // A later request retries setup before any fast-path route is exposed.
            if entry.state == RepositoryState::Pending {
                repository
                    .ensure_owner(mutation_identity()?, &entry.owner)
                    .await?;
            } else if !repository
                .identity_matches(&entry.owner, None)
                .await?
                .output
            {
                // Ready Cells must verify immutable identity without publishing a
                // write on every restore or recreating missing ownership state.
                return Err(ServerError::Repository(
                    "repository owner differs from directory",
                ));
            }
        }
        let mut loaded = self.loaded.lock().await;
        let existing = loaded
            .get_mut(&entry.repository_id)
            .ok_or(ServerError::Repository("loaded repository is absent"))?;
        existing.initialized = true;
        self.route(entry, existing)
    }

    fn route(
        &self,
        entry: &RepositoryEntry,
        existing: &mut LoadedRepository,
    ) -> Result<RepositoryRoute, ServerError> {
        if existing.name != entry.name {
            existing.router = self.router_for(entry, Arc::clone(&existing.gateway))?;
            existing.name.clone_from(&entry.name);
        }
        existing.last_used = Instant::now();
        Ok(RepositoryRoute {
            repository: Arc::clone(&existing.repository),
            gateway: Arc::clone(&existing.gateway),
            router: existing.router.clone(),
            pin: Arc::clone(&existing.pin),
        })
    }

    async fn evict_repository(&self) -> Result<Arc<OwnedSemaphorePermit>, ServerError> {
        // A candidate can start renewal or another lifecycle transition after
        // inventory. Try a different idle Cell within this same admitted cold
        // request instead of turning one transient preflight race into HTTP 503.
        // Bound rescans so a whole busy working set still backpressures callers.
        let mut rejected = HashSet::new();
        let settle_deadline = Instant::now() + std::time::Duration::from_millis(200);
        let mut settle_rescans = 0;
        loop {
            let candidates: HashMap<_, _> = self
                .node
                .idle_transfer_candidates()
                .await?
                .into_iter()
                .map(|(cell, generation, _, _)| (cell, generation))
                .collect();
            let (chosen, may_settle) = {
                let mut loaded = self.loaded.lock().await;
                let mut eligible = Vec::new();
                for (id, repository) in loaded.iter() {
                    if rejected.contains(id) || Arc::strong_count(&repository.pin) != 1 {
                        continue;
                    }
                    let (priority, action) = match (repository.state, repository.local) {
                        (ResidencyState::Released, _) => (0, EvictionAction::Cleanup),
                        (ResidencyState::Serving, false) => (1, EvictionAction::DropRemote),
                        (ResidencyState::Serving | ResidencyState::RefreshHandle, true) => {
                            let target = repository_target(self.tenant, self.application, *id)?;
                            let Some(generation) = candidates.get(&target.cell_id()) else {
                                continue;
                            };
                            (
                                2,
                                EvictionAction::Release {
                                    cell: target.cell_id(),
                                    generation: *generation,
                                },
                            )
                        }
                        _ => continue,
                    };
                    eligible.push((priority, repository.last_used, *id, action));
                }
                eligible.sort_unstable_by_key(|candidate| (candidate.0, candidate.1, candidate.2));
                let mut chosen = None;
                for (_, _, id, action) in eligible {
                    // Never wait for another repository while holding our own guard.
                    // This excludes in-flight initialization and competing evictions.
                    let Ok(transition) = self.transition_lock(id).await.try_lock_owned() else {
                        continue;
                    };
                    if matches!(action, EvictionAction::Release { .. }) {
                        loaded
                            .get_mut(&id)
                            .ok_or(ServerError::Repository("eviction candidate is absent"))?
                            .state = ResidencyState::Releasing;
                    }
                    chosen = Some((id, action, transition));
                    break;
                }
                let may_settle = loaded.iter().any(|(id, repository)| {
                    !rejected.contains(id)
                        && repository.local
                        && Arc::strong_count(&repository.pin) == 1
                        && matches!(
                            repository.state,
                            ResidencyState::Serving | ResidencyState::RefreshHandle
                        )
                });
                (chosen, may_settle)
            };
            let Some((id, action, _transition)) = chosen else {
                // Publication and renewal may still be settling just after an
                // acknowledged request. Reobserve inventory only: never replay
                // a mutation or release a Cell the runtime considers busy.
                // Keep every request/residency permit charged while waiting,
                // and do not wait at all when every resident is request-pinned.
                let remaining = settle_deadline.saturating_duration_since(Instant::now());
                if may_settle && settle_rescans < 8 && !remaining.is_zero() {
                    settle_rescans += 1;
                    tokio::time::sleep(remaining.min(std::time::Duration::from_millis(25))).await;
                    continue;
                }
                return Err(if rejected.is_empty() {
                    Error::Capacity("repository residency")
                } else {
                    Error::CellDraining
                }
                .into());
            };
            let (cell, generation) = match action {
                EvictionAction::DropRemote => {
                    let removed = self.loaded.lock().await.remove(&id);
                    return removed
                        .map(|repository| repository.slot)
                        .ok_or(ServerError::Repository("eviction candidate is absent"));
                }
                EvictionAction::Cleanup => return self.cleanup_released(id).await,
                EvictionAction::Release { cell, generation } => (cell, generation),
            };
            let mut result = self
                .node
                .release_idle_cell(cell, self.session, generation)
                .await;
            // Cellule rejects this exact capacity error before transfer preflight.
            // Wait one rate window; the retry rechecks generation and settled work.
            // Other failures can follow release and must retain the recovery path.
            if matches!(&result, Err(Error::Capacity("movement budget"))) {
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                result = self
                    .node
                    .release_idle_cell(cell, self.session, generation)
                    .await;
            }
            {
                let mut loaded = self.loaded.lock().await;
                let repository = loaded
                    .get_mut(&id)
                    .ok_or(ServerError::Repository("eviction candidate is absent"))?;
                repository.state = if result.is_ok() {
                    ResidencyState::Released
                } else {
                    ResidencyState::RefreshHandle
                };
            }
            if matches!(&result, Err(Error::CellDraining)) && rejected.len() < 8 {
                rejected.insert(id);
                continue;
            }
            result?;
            // Only a confirmed release permits dropping handles and deleting local
            // SQLite artifacts. Failed or ambiguous releases retain the local state.
            let slot = self.cleanup_released(id).await?;
            tracing::debug!(repository = %hex::encode(id), "released idle repository Cell");
            return Ok(slot);
        }
    }

    async fn cleanup_released(
        &self,
        id: [u8; 16],
    ) -> Result<Arc<OwnedSemaphorePermit>, ServerError> {
        // Keep the released entry until deletion completes. A failed cleanup must
        // be retried before restore, whose destination must not already exist.
        match tokio::fs::remove_dir_all(self.local.path().join(hex::encode(id))).await {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        // Transfer the still-reserved slot directly to the new activation.
        // Another cold request cannot steal it between release and restore.
        let removed = self.loaded.lock().await.remove(&id);
        // Dropping a gateway may delete its Git cache. The registry lock must
        // already be released before that filesystem cleanup runs.
        removed
            .map(|repository| repository.slot)
            .ok_or(ServerError::Repository("released repository is absent"))
    }

    fn bind_repository(
        &self,
        entry: &RepositoryEntry,
        target: CellTarget,
        client: CellClient,
        local: bool,
        slot: Arc<OwnedSemaphorePermit>,
    ) -> Result<LoadedRepository, ServerError> {
        let application = self.node.application_handle::<CanopyApplication>(
            client,
            self.tenant,
            self.application,
        )?;
        let repository = Arc::new(RepositoryCell::new(
            &application,
            target,
            entry.repository_id,
            entry.object_format,
        )?);
        let gateway = Arc::new(
            GitGateway::new(
                Arc::clone(&repository),
                self.local.path().to_path_buf(),
                Arc::clone(&self.external_store),
                self.disk_budget.clone(),
            )
            .with_signer_directory(Arc::clone(&self.directory)),
        );
        let router = self.router_for(entry, Arc::clone(&gateway))?;
        Ok(LoadedRepository {
            repository,
            gateway,
            name: entry.name.clone(),
            router,
            pin: Arc::new(()),
            last_used: Instant::now(),
            initialized: false,
            local,
            state: ResidencyState::Serving,
            slot,
        })
    }

    fn router_for(
        &self,
        entry: &RepositoryEntry,
        gateway: Arc<GitGateway>,
    ) -> Result<Router, ServerError> {
        Ok(Arc::new(
            GitHttpApi::new(
                gateway,
                self.owner.clone(),
                &entry.name,
                &self.public_url,
                Arc::clone(&self.ready),
            )
            .map_err(ServerError::Http)?,
        )
        .router())
    }
}
