//! Bounded repository residency with request pins and confirmed Cell release.

use std::{
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
use cellule_runtime::{CatalogRole, CellClient, CellModule, CellTarget, Error};
use http_body::{Frame, SizeHint};
use tower::ServiceExt;

use super::{
    RESIDENT_REPOSITORIES, RepositoryManager, ServerError, SqlCellSpec, acquire_sql_cell,
    mutation_identity,
};
use crate::{
    CanopyApplication, REPOSITORY_DATABASE_LIMIT_BYTES, RepositoryCell, RepositoryModule,
    directory::RepositoryEntry, git_gateway::GitGateway, http::GitHttpApi, repository_target,
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
        entry: RepositoryEntry,
    ) -> Result<RepositoryRoute, ServerError> {
        if let Some(route) = self.local_route(&entry).await? {
            return Ok(route);
        }
        let admission = Arc::clone(&self.residency_admission)
            .try_acquire_owned()
            .map_err(|_| Error::Capacity("pending repository activations"))?;
        let queued = Instant::now();
        let manager = Arc::clone(self);
        // Client cancellation must not abandon a release or acquisition halfway
        // through. Shutdown waits for these tracked tasks before draining.
        self.tasks
            .spawn(async move {
                // Retain queue capacity through cancellation and transition cleanup.
                // Releasing it with the HTTP waiter would allow unbounded detached work.
                let _admission = admission;
                let _change = manager.residency_change.lock().await;
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

    // The transition guard serializes slot admission and ownership changes.
    // The registry lock only protects memory and request pins, never storage I/O.
    async fn load_repository(
        &self,
        entry: &RepositoryEntry,
    ) -> Result<RepositoryRoute, ServerError> {
        if let Some(route) = self.local_route(entry).await? {
            return Ok(route);
        }
        let target = repository_target(self.tenant, self.application, entry.repository_id)?;
        let remote_route = self
            .loaded
            .lock()
            .await
            .get(&entry.repository_id)
            .is_some_and(|repository| !repository.local);
        if remote_route && !self.peer.remote_owner(&target).await? {
            // Remote cache ownership is disposable. Reacquire idle/expired Cell
            // authority locally before binding a new route after owner loss.
            self.loaded.lock().await.remove(&entry.repository_id);
        }
        let state = self
            .loaded
            .lock()
            .await
            .get(&entry.repository_id)
            .map(|repository| repository.state);
        match state {
            Some(ResidencyState::Released) => {
                self.cleanup_released(entry.repository_id).await?;
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
                self.loaded.lock().await.insert(
                    entry.repository_id,
                    self.bind_repository(
                        entry,
                        target,
                        CellClient::local(self.node.application().registry(), handle),
                        true,
                    )?,
                );
            }
            _ => {}
        }
        let (present, full) = {
            let loaded = self.loaded.lock().await;
            (
                loaded.contains_key(&entry.repository_id),
                loaded.len() >= RESIDENT_REPOSITORIES,
            )
        };
        if !present {
            if full {
                self.evict_repository().await?;
            }
            let remote = self.peer.remote_owner(&target).await?;
            let client = if remote {
                self.peer.client()
            } else {
                let directory = self.local.path().join(hex::encode(entry.repository_id));
                tokio::fs::create_dir_all(&directory).await?;
                let started = Instant::now();
                let handle = acquire_sql_cell(
                    &self.node,
                    &self.layout,
                    &self.node_directory,
                    SqlCellSpec {
                        target: &target,
                        module: RepositoryModule::NAME,
                        schema: include_str!("../schema.sql"),
                        max_database_bytes: REPOSITORY_DATABASE_LIMIT_BYTES,
                        destination: directory.join("repository.sqlite"),
                    },
                    self.session,
                    &self.endpoint,
                )
                .await?;
                tracing::debug!(repository = %hex::encode(entry.repository_id), elapsed_seconds = started.elapsed().as_secs_f64(), "acquired repository Cell");
                CellClient::local(self.node.application().registry(), handle)
            };
            self.loaded.lock().await.insert(
                entry.repository_id,
                self.bind_repository(entry, target, client, !remote)?,
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
            repository
                .ensure_owner(mutation_identity()?, &entry.owner)
                .await?;
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

    async fn evict_repository(&self) -> Result<(), ServerError> {
        let released = self
            .loaded
            .lock()
            .await
            .iter()
            .find_map(|(id, repository)| {
                (repository.state == ResidencyState::Released).then_some(*id)
            });
        if let Some(id) = released {
            return self.cleanup_released(id).await;
        }
        {
            let mut loaded = self.loaded.lock().await;
            if let Some(id) = loaded.iter().find_map(|(id, repository)| {
                (!repository.local && Arc::strong_count(&repository.pin) == 1).then_some(*id)
            }) {
                loaded.remove(&id);
                return Ok(());
            }
        }
        let candidates = self.node.idle_transfer_candidates().await?;
        let (id, cell, generation) = {
            let mut loaded = self.loaded.lock().await;
            let mut eligible = Vec::new();
            for (id, repository) in loaded.iter() {
                if Arc::strong_count(&repository.pin) != 1 {
                    continue;
                }
                let target = repository_target(self.tenant, self.application, *id)?;
                if let Some((cell, generation, _, _)) = candidates
                    .iter()
                    .find(|(cell, ..)| *cell == target.cell_id())
                {
                    eligible.push((repository.last_used, *id, *cell, *generation));
                }
            }
            eligible.sort_unstable_by_key(|candidate| (candidate.0, candidate.1));
            let Some((_, id, cell, generation)) = eligible.first().copied() else {
                return Err(ServerError::Runtime(Error::Capacity(
                    "repository residency",
                )));
            };
            // Claim the candidate under the same lock used to pin warm requests.
            // No new request may acquire this handle once release can start.
            loaded
                .get_mut(&id)
                .ok_or(ServerError::Repository("eviction candidate is absent"))?
                .state = ResidencyState::Releasing;
            (id, cell, generation)
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
        result?;
        // Only a confirmed release permits dropping handles and deleting local
        // SQLite artifacts. Failed or ambiguous releases retain the local state.
        self.cleanup_released(id).await?;
        tracing::debug!(repository = %hex::encode(id), "released idle repository Cell");
        Ok(())
    }

    async fn cleanup_released(&self, id: [u8; 16]) -> Result<(), ServerError> {
        // Keep the released entry until deletion completes. A failed cleanup must
        // be retried before restore, whose destination must not already exist.
        match tokio::fs::remove_dir_all(self.local.path().join(hex::encode(id))).await {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        self.loaded.lock().await.remove(&id);
        Ok(())
    }

    fn bind_repository(
        &self,
        entry: &RepositoryEntry,
        target: CellTarget,
        client: CellClient,
        local: bool,
    ) -> Result<LoadedRepository, ServerError> {
        let application = self.node.application_handle::<CanopyApplication>(
            client,
            self.tenant,
            self.application,
        );
        let repository = Arc::new(RepositoryCell::new(&application, target)?);
        let gateway = Arc::new(GitGateway::new(
            Arc::clone(&repository),
            self.local.path().to_path_buf(),
            Arc::clone(&self.external_store),
            self.disk_budget.clone(),
        ));
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
