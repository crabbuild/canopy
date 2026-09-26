//! Durable bridge from Git smart HTTP to one repository's SQLite Cell.

use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error as StdError,
    path::{Path, PathBuf},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use axum::body::Body;
use cellule_ltx::DiskBudget;
use cellule_runtime::{MutationIdentity, RequestId};
use object_store::ObjectStore;
use tokio::{
    process::Command,
    sync::{Mutex, OwnedSemaphorePermit},
};

use crate::{
    INLINE_OBJECT_LIMIT, ObjectBatch, ObjectKind, ObjectStorage, PushPlan, RefExpectation,
    RefUpdate, RepositoryCell, StoredObject,
    directory::TokenScope,
    git_cache::CacheError,
    git_http::{GitHttpBackend, GitHttpError, GitHttpRequest, GitHttpResponse},
    git_input::{GitInput, InputError, MAX_FETCH_REQUEST_BYTES, MAX_PUSH_BYTES},
    git_objects::GitObjects,
    large_blob::{LargeBlobError, LargeBlobReference, LargeBlobStore},
    lfs::LfsService,
    object_batch::MAX_OBJECTS,
    push::{PushCompletion, PushError},
    refs::{REF_PAGE_SIZE, RefReadError},
};

pub use crate::git_objects::ObjectReadError;

type CellError = Box<dyn StdError + Send + Sync>;

#[derive(Debug, thiserror::Error)]
pub enum GatewayError {
    #[error("Git cache failed")]
    Cache(#[from] CacheError),
    #[error("Git HTTP backend failed")]
    Http(#[from] GitHttpError),
    #[error("Git request input failed")]
    Input(#[from] InputError),
    #[error("repository Cell operation failed")]
    Cell(#[source] CellError),
    #[error("large Git blob store failed")]
    Blob(#[from] LargeBlobError),
    #[error("cache I/O failed")]
    Io(#[from] std::io::Error),
    #[error("Git cache command failed: {0}")]
    Git(String),
    #[error("Git cache contains malformed data")]
    MalformedCache,
    #[error("Git object ingestion failed")]
    Objects(#[from] ObjectReadError),
    #[error("ref publication conflicted with current Cell state")]
    RefConflict,
    #[error("repository refs kept changing during snapshot acquisition")]
    RefSnapshotBusy,
    #[error("authentication is required")]
    Unauthorized,
    #[error("durable push response failed")]
    Push(#[from] PushError),
    #[error("cache task failed")]
    Task(#[from] tokio::task::JoinError),
}

struct CachedRepository {
    backend: GitHttpBackend,
    snapshot: RefSnapshot,
}

#[derive(PartialEq, Eq)]
struct RefSnapshot {
    refs: BTreeMap<String, RefExpectation>,
    head: String,
    generation: i64,
}

/// Serves Git requests from a warm, disposable cache of durable Cell state.
pub struct GitGateway {
    repository: Arc<RepositoryCell>,
    large_blobs: LargeBlobStore,
    lfs: LfsService,
    scratch_root: PathBuf,
    disk_budget: DiskBudget,
    cache: Mutex<Option<Arc<CachedRepository>>>,
    push: Mutex<()>,
}

impl GitGateway {
    pub fn new(
        repository: Arc<RepositoryCell>,
        scratch_root: PathBuf,
        blob_store: Arc<dyn ObjectStore>,
        disk_budget: DiskBudget,
    ) -> Self {
        let large_blobs = LargeBlobStore::new(Arc::clone(&blob_store), repository.repository_id());
        let lfs = LfsService::new(Arc::clone(&repository), blob_store);
        Self {
            repository,
            large_blobs,
            lfs,
            scratch_root,
            disk_budget,
            cache: Mutex::new(None),
            push: Mutex::new(()),
        }
    }

    pub fn lfs(&self) -> &LfsService {
        &self.lfs
    }

    pub async fn access_level(&self, account: &str) -> Result<Option<TokenScope>, GatewayError> {
        Ok(self
            .repository
            .access_level(account, None)
            .await
            .map_err(|error| GatewayError::Cell(Box::new(error)))?
            .output)
    }

    /// Waits for Cell publication before returning any successful receive-pack body.
    pub async fn handle(
        &self,
        request: GitHttpRequest<Body>,
        actor: &str,
        push_id: Option<[u8; 16]>,
        admission: Option<Arc<OwnedSemaphorePermit>>,
    ) -> Result<GitHttpResponse<Body>, GatewayError> {
        if !request.authenticated {
            return Err(GatewayError::Unauthorized);
        }
        let is_push = request.method == "POST" && request.path_info == "/repo.git/git-receive-pack";
        if is_push {
            let _push = self.push.lock().await;
            let request = self.receive(request, MAX_PUSH_BYTES, admission).await?;
            let id = push_id.unwrap_or_else(|| uuid::Uuid::new_v4().into_bytes());
            let digest = request_digest(&request).await?;
            if let Some(response) = self.repository.begin_push(id, actor, digest).await? {
                return Ok(http_body(with_push_id(
                    self.repository.push_response(response).await?,
                    id,
                )));
            }
            let request = self.decode(request, MAX_PUSH_BYTES).await?;
            self.cache.lock().await.take();
            let cache = self.build_cache(self.cell_refs().await?).await?;
            return self
                .handle_push(&cache, request, actor, id, digest)
                .await
                .map(http_body);
        }
        let request = self
            .receive(request, MAX_FETCH_REQUEST_BYTES, admission)
            .await?;
        let request = self.decode(request, MAX_FETCH_REQUEST_BYTES).await?;
        let live_refs = self.cell_refs().await?;
        let cached = {
            let mut cache = self.cache.lock().await;
            if cache
                .as_ref()
                .is_none_or(|cached| cached.snapshot != live_refs)
            {
                *cache = None;
                *cache = Some(Arc::new(self.build_cache(live_refs).await?));
            }
            Arc::clone(cache.as_ref().ok_or(GatewayError::MalformedCache)?)
        };
        let response = cached.backend.stream(request, Arc::clone(&cached)).await?;
        Ok(GitHttpResponse {
            status: response.status,
            headers: response.headers,
            body: Body::from_stream(response.body),
        })
    }

    async fn receive(
        &self,
        request: GitHttpRequest<Body>,
        limit: u64,
        admission: Option<Arc<OwnedSemaphorePermit>>,
    ) -> Result<GitHttpRequest, GatewayError> {
        let GitHttpRequest {
            method,
            path_info,
            query,
            content_type,
            gzip,
            protocol_v2,
            body,
            authenticated,
        } = request;
        let body = GitInput::receive(
            body,
            &self.scratch_root,
            &self.disk_budget,
            limit,
            admission,
        )
        .await?;
        Ok(GitHttpRequest {
            method,
            path_info,
            query,
            content_type,
            gzip,
            protocol_v2,
            body,
            authenticated,
        })
    }

    async fn decode(
        &self,
        mut request: GitHttpRequest,
        limit: u64,
    ) -> Result<GitHttpRequest, GatewayError> {
        if request.gzip {
            request.body = request
                .body
                .decode_gzip(&self.scratch_root, &self.disk_budget, limit)
                .await?;
            request.gzip = false;
        }
        Ok(request)
    }

    async fn handle_push(
        &self,
        cached: &CachedRepository,
        request: GitHttpRequest,
        actor: &str,
        id: [u8; 16],
        digest: [u8; 32],
    ) -> Result<GitHttpResponse, GatewayError> {
        let before = cached.snapshot.refs.clone();
        let response = cached.backend.run(request).await?;
        // Git may accept some refs and reject others unless atomic was requested.
        // Publish its actual changes before forwarding the unmodified per-ref report.
        let plan = if response.status == 200 {
            let after = git_refs(&cached.backend.git_dir()).await?;
            let plan = diff_refs(&before, &after, actor);
            if plan.updates.is_empty() {
                None
            } else {
                self.persist_objects(&cached.backend, &before, &plan)
                    .await?;
                Some(plan)
            }
        } else {
            None
        };
        let response_id = self.repository.stage_push_response(id, &response).await?;
        let result = self
            .repository
            .complete_push(PushCompletion {
                id,
                actor: actor.into(),
                digest,
                response_id,
                plan,
            })
            .await
            .map_err(|error| match error {
                cellule_runtime::InvocationError::Rejected(_) => GatewayError::RefConflict,
                other => GatewayError::Cell(Box::new(other)),
            })?;
        if !result.output {
            return Err(GatewayError::RefConflict);
        }
        Ok(with_push_id(
            self.repository.completed_response(id).await?,
            id,
        ))
    }

    async fn build_cache(&self, snapshot: RefSnapshot) -> Result<CachedRepository, GatewayError> {
        let backend = GitHttpBackend::initialize(
            self.scratch_root.clone(),
            self.disk_budget.clone(),
            &snapshot.head,
        )
        .await?;
        self.hydrate(&backend, &snapshot.refs).await?;
        Ok(CachedRepository { backend, snapshot })
    }

    async fn hydrate(
        &self,
        backend: &GitHttpBackend,
        refs: &BTreeMap<String, RefExpectation>,
    ) -> Result<(), GatewayError> {
        let mut after = None;
        loop {
            let next = self
                .repository
                .next_object(after)
                .await
                .map_err(|error| GatewayError::Cell(Box::new(error)))?
                .output;
            let Some(object) = next else {
                break;
            };
            after = Some(object.oid);
            let body = match object.storage {
                ObjectStorage::Inline(body) => body,
                ObjectStorage::Chunked {
                    upload,
                    size,
                    blake3,
                } => self
                    .repository
                    .chunked_body(object.oid, object.kind, upload, size, blake3)
                    .await
                    .map_err(|error| GatewayError::Cell(Box::new(error)))?,
                ObjectStorage::External {
                    size,
                    blake3,
                    sha256,
                } => {
                    self.large_blobs
                        .get(&LargeBlobReference {
                            oid: object.oid,
                            size,
                            blake3,
                            sha256,
                        })
                        .await?
                }
            };
            backend
                .cache
                .store_object(object.oid, object.kind, body)
                .await?;
        }
        backend.cache.store_refs(refs).await?;
        Ok(())
    }

    async fn cell_refs(&self) -> Result<RefSnapshot, GatewayError> {
        for _ in 0..3 {
            let mut refs = BTreeMap::new();
            let mut after = String::new();
            let mut generation = None;
            loop {
                let page = match self.repository.refs_page(&after, generation).await {
                    Ok(page) => page.output,
                    Err(RefReadError::Changed) => break,
                    Err(RefReadError::Cell(error)) => {
                        return Err(GatewayError::Cell(Box::new(error)));
                    }
                };
                generation = Some(page.generation);
                let complete = page.refs.len() < REF_PAGE_SIZE;
                for (name, state) in page.refs {
                    after = name.clone();
                    refs.insert(name, state);
                }
                if complete {
                    return Ok(RefSnapshot {
                        refs,
                        head: page.default_branch,
                        generation: page.generation,
                    });
                }
            }
        }
        Err(GatewayError::RefSnapshotBusy)
    }

    async fn persist_objects(
        &self,
        backend: &GitHttpBackend,
        before: &BTreeMap<String, RefExpectation>,
        plan: &PushPlan,
    ) -> Result<(), GatewayError> {
        let included: Vec<_> = plan
            .updates
            .iter()
            .filter_map(|update| update.new_oid)
            .collect();
        if included.is_empty() {
            return Ok(());
        }
        // Published refs already have durable graph closure. Excluding them avoids
        // re-reading old history; the final Cell transaction still verifies every new tip.
        let excluded = before.values().filter_map(|state| state.oid).collect();
        let mut objects = GitObjects::start(&backend.git_dir(), included, excluded)?;
        let mut batch = ObjectBatch::default();
        loop {
            let mut candidates = Vec::with_capacity(MAX_OBJECTS);
            for _ in 0..MAX_OBJECTS {
                let Some(oid) = objects.next().await? else {
                    break;
                };
                candidates.push(oid);
            }
            if candidates.is_empty() {
                break;
            }
            let present = self
                .repository
                .existing_objects(&candidates)
                .await
                .map_err(|error| GatewayError::Cell(Box::new(error)))?
                .output;
            for oid in candidates.into_iter().filter(|oid| !present.contains(oid)) {
                let (kind, body) = objects.read(oid).await?;
                let object = if kind != ObjectKind::Blob && body.len() > INLINE_OBJECT_LIMIT {
                    self.repository
                        .stage_object(new_identity()?, kind, &body)
                        .await
                        .map_err(|error| GatewayError::Cell(Box::new(error)))?
                } else {
                    let storage = if kind == ObjectKind::Blob && body.len() > INLINE_OBJECT_LIMIT {
                        let uploaded = self.large_blobs.put(&body).await?;
                        if uploaded.oid != oid {
                            return Err(GatewayError::MalformedCache);
                        }
                        ObjectStorage::External {
                            size: uploaded.size,
                            blake3: uploaded.blake3,
                            sha256: uploaded.sha256,
                        }
                    } else {
                        ObjectStorage::Inline(body)
                    };
                    StoredObject { oid, kind, storage }
                };
                if let Err(object) = batch.try_push(object) {
                    self.repository
                        .put_objects(new_identity()?, std::mem::take(&mut batch))
                        .await
                        .map_err(|error| GatewayError::Cell(Box::new(error)))?;
                    batch
                        .try_push(object)
                        .map_err(|_| GatewayError::MalformedCache)?;
                }
            }
        }
        objects.finish().await?;
        if !batch.is_empty() {
            self.repository
                .put_objects(new_identity()?, batch)
                .await
                .map_err(|error| GatewayError::Cell(Box::new(error)))?;
        }
        Ok(())
    }
}

fn http_body(response: GitHttpResponse) -> GitHttpResponse<Body> {
    GitHttpResponse {
        status: response.status,
        headers: response.headers,
        body: Body::from(response.body),
    }
}

async fn request_digest(request: &GitHttpRequest) -> Result<[u8; 32], InputError> {
    let mut hash = blake3::Hasher::new();
    hash.update(b"canopy-git-push-v2");
    hash.update(&[
        u8::from(request.protocol_v2),
        u8::from(request.content_type.is_some()),
        u8::from(request.gzip),
    ]);
    for field in [
        request.method.as_bytes(),
        request.path_info.as_bytes(),
        request.query.as_bytes(),
        request
            .content_type
            .as_deref()
            .unwrap_or_default()
            .as_bytes(),
    ] {
        hash.update(&(field.len() as u64).to_le_bytes());
        hash.update(field);
    }
    request.body.digest(hash).await
}

fn with_push_id(mut response: GitHttpResponse, id: [u8; 16]) -> GitHttpResponse {
    response.headers.push((
        "X-Canopy-Push-Id".into(),
        uuid::Uuid::from_bytes(id).to_string(),
    ));
    response
}

async fn git_output(git_dir: &Path, args: &[&str]) -> Result<Vec<u8>, GatewayError> {
    let output = Command::new("git")
        .arg("--git-dir")
        .arg(git_dir)
        .args(args)
        .output()
        .await?;
    if !output.status.success() {
        return Err(GatewayError::Git(
            String::from_utf8_lossy(&output.stderr).into_owned(),
        ));
    }
    Ok(output.stdout)
}

async fn git_refs(git_dir: &Path) -> Result<BTreeMap<String, [u8; 20]>, GatewayError> {
    let listing = git_output(
        git_dir,
        &["for-each-ref", "--format=%(refname)%00%(objectname)"],
    )
    .await?;
    let mut refs = BTreeMap::new();
    for line in listing
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
    {
        let Some(separator) = line.iter().position(|byte| *byte == 0) else {
            return Err(GatewayError::MalformedCache);
        };
        let name =
            std::str::from_utf8(&line[..separator]).map_err(|_| GatewayError::MalformedCache)?;
        let oid = std::str::from_utf8(&line[separator + 1..])
            .map_err(|_| GatewayError::MalformedCache)?;
        refs.insert(name.to_owned(), parse_oid(oid)?);
    }
    Ok(refs)
}

fn diff_refs(
    before: &BTreeMap<String, RefExpectation>,
    after: &BTreeMap<String, [u8; 20]>,
    actor: &str,
) -> PushPlan {
    let names: BTreeSet<_> = before.keys().chain(after.keys()).cloned().collect();
    let updates = names
        .into_iter()
        .filter_map(|name| {
            let expected = before.get(&name).cloned();
            let new_oid = after.get(&name).copied();
            if expected.as_ref().and_then(|state| state.oid) == new_oid {
                None
            } else {
                Some(RefUpdate {
                    name,
                    expected,
                    new_oid,
                })
            }
        })
        .collect();
    PushPlan {
        actor: actor.into(),
        updates,
    }
}

fn parse_oid(oid: &str) -> Result<[u8; 20], GatewayError> {
    if oid.len() != 40 {
        return Err(GatewayError::MalformedCache);
    }
    hex::decode(oid)
        .map_err(|_| GatewayError::MalformedCache)?
        .try_into()
        .map_err(|_| GatewayError::MalformedCache)
}

fn new_identity() -> Result<MutationIdentity, GatewayError> {
    let now_ms = i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| GatewayError::MalformedCache)?
            .as_millis(),
    )
    .map_err(|_| GatewayError::MalformedCache)?;
    Ok(MutationIdentity {
        request_id: RequestId::from_bytes(uuid::Uuid::new_v4().into_bytes()),
        issued_at_ms: now_ms,
        expires_at_ms: now_ms + 60_000,
    })
}
