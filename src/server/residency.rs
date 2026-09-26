//! Bounded repository residency with request pins and confirmed Cell release.

use std::{
    collections::HashMap,
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
use cellule_runtime::{CatalogRole, CellClient, CellHandle, CellModule, CellTarget, Error};
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
    state: ResidencyState,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ResidencyState {
    Serving,
    RefreshHandle,
    Released,
}

#[cfg(test)]
mod tests;

pub(crate) struct RepositoryRoute {
    pub(crate) repository: Arc<RepositoryCell>,
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
        let manager = Arc::clone(self);
        // Client cancellation must not abandon a release or acquisition halfway
        // through. Shutdown waits for these tracked tasks before draining.
        self.tasks
            .spawn(async move {
                let mut loaded = manager.loaded.lock().await;
                manager.load_locked(&entry, &mut loaded).await
            })
            .await?
    }

    async fn load_locked(
        &self,
        entry: &RepositoryEntry,
        loaded: &mut HashMap<[u8; 16], LoadedRepository>,
    ) -> Result<RepositoryRoute, ServerError> {
        match loaded
            .get(&entry.repository_id)
            .map(|repository| repository.state)
        {
            Some(ResidencyState::Released) => {
                self.cleanup_released(entry.repository_id, loaded).await?;
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
                loaded.insert(
                    entry.repository_id,
                    self.bind_repository(entry, target, handle)?,
                );
            }
            _ => {}
        }
        if !loaded.contains_key(&entry.repository_id) {
            if loaded.len() >= RESIDENT_REPOSITORIES {
                self.evict_repository(loaded).await?;
            }
            let target = repository_target(self.tenant, self.application, entry.repository_id)?;
            let directory = self.local_root.join(hex::encode(entry.repository_id));
            tokio::fs::create_dir_all(&directory).await?;
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
            loaded.insert(
                entry.repository_id,
                self.bind_repository(entry, target, handle)?,
            );
        }
        let existing = loaded
            .get_mut(&entry.repository_id)
            .ok_or(ServerError::Repository("loaded repository is absent"))?;
        if !existing.initialized {
            // Retain the acquired Cell on an uncertain owner-initialization result.
            // A later request retries the idempotent initialization on this owner.
            existing
                .repository
                .ensure_owner(mutation_identity()?, &entry.owner)
                .await?;
            existing.initialized = true;
        }
        if existing.name != entry.name {
            existing.router = self.router_for(entry, Arc::clone(&existing.gateway))?;
            existing.name.clone_from(&entry.name);
        }
        existing.last_used = Instant::now();
        Ok(RepositoryRoute {
            repository: Arc::clone(&existing.repository),
            router: existing.router.clone(),
            pin: Arc::clone(&existing.pin),
        })
    }

    async fn evict_repository(
        &self,
        loaded: &mut HashMap<[u8; 16], LoadedRepository>,
    ) -> Result<(), ServerError> {
        if let Some(id) = loaded.iter().find_map(|(id, repository)| {
            (repository.state == ResidencyState::Released).then_some(*id)
        }) {
            return self.cleanup_released(id, loaded).await;
        }
        let candidates = self.node.idle_transfer_candidates().await?;
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
        let result = self
            .node
            .release_idle_cell(cell, self.session, generation)
            .await;
        let repository = loaded
            .get_mut(&id)
            .ok_or(ServerError::Repository("eviction candidate is absent"))?;
        repository.state = if result.is_ok() {
            ResidencyState::Released
        } else {
            ResidencyState::RefreshHandle
        };
        result?;
        // Only a confirmed release permits dropping handles and deleting local
        // SQLite artifacts. Failed or ambiguous releases retain the local state.
        self.cleanup_released(id, loaded).await?;
        tracing::debug!(repository = %hex::encode(id), "released idle repository Cell");
        Ok(())
    }

    async fn cleanup_released(
        &self,
        id: [u8; 16],
        loaded: &mut HashMap<[u8; 16], LoadedRepository>,
    ) -> Result<(), ServerError> {
        // Keep the released entry until deletion completes. A failed cleanup must
        // be retried before restore, whose destination must not already exist.
        match tokio::fs::remove_dir_all(self.local_root.join(hex::encode(id))).await {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        loaded.remove(&id);
        Ok(())
    }

    fn bind_repository(
        &self,
        entry: &RepositoryEntry,
        target: CellTarget,
        handle: CellHandle,
    ) -> Result<LoadedRepository, ServerError> {
        let application = self.node.application_handle::<CanopyApplication>(
            CellClient::local(self.node.application().registry(), handle),
            self.tenant,
            self.application,
        );
        let repository = Arc::new(RepositoryCell::new(&application, target)?);
        let gateway = Arc::new(GitGateway::new(
            Arc::clone(&repository),
            self.local_root.clone(),
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
