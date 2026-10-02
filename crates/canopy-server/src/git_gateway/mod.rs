//! Durable bridge from Git smart HTTP to one repository's SQLite Cell.

use crate::ReadIdentity;

use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    error::Error as StdError,
    path::{Path, PathBuf},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use crate::AdmissionPermit;
use axum::body::Body;
use cellule_ltx::DiskBudget;
use cellule_runtime::{MutationIdentity, identity::RequestId};
use object_store::ObjectStore;
use tokio::sync::{Mutex, OnceCell};

use crate::{
    INLINE_OBJECT_LIMIT, ObjectBatch, ObjectKind, ObjectStorage, PushPlan, RefExpectation,
    RefUpdate, RepositoryCell, StoredObject,
    blob::{LargeBlobError, LargeBlobStore},
    directory::TokenScope,
    git_cache::{CacheError, GitCache},
    git_http::{GitHttpBackend, GitHttpError, GitHttpRequest, GitHttpResponse},
    git_input::{GitInput, InputError, MAX_FETCH_REQUEST_BYTES},
    git_objects::GitObjects,
    lfs::LfsService,
    object_batch::MAX_OBJECTS,
    push::{PushCompletion, PushError},
    refs::RefReadError,
};

mod branch_policy;
mod candidates;
mod discovery;
mod fetch;
mod hydration;
mod maintenance;
mod push;
mod ssh;

use hydration::Hydration;

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
    #[error("{0}")]
    Certificate(&'static str),
    #[error("Git object ingestion failed")]
    Objects(#[from] ObjectReadError),
    #[error("repository refs kept changing during snapshot acquisition")]
    RefSnapshotBusy,
    #[error("requested Git object is not reachable from a current repository ref")]
    UnreachableWant,
    #[error("authentication is required")]
    Unauthorized,
    #[error("durable push response failed")]
    Push(#[from] PushError),
    #[error("cache task failed")]
    Task(#[from] tokio::task::JoinError),
}

struct CachedObjects {
    cache: Arc<GitCache>,
    through: i64,
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
    signer_directory: Option<Arc<crate::directory::DirectoryCell>>,
    certificate_seed: OnceCell<[u8; 32]>,
    large_blobs: LargeBlobStore,
    pack_reader: Arc<crate::pack_store::PackReader>,
    lfs: LfsService,
    scratch_root: PathBuf,
    disk_budget: DiskBudget,
    cache: Mutex<Option<Arc<CachedRepository>>>,
    objects: Mutex<Option<CachedObjects>>,
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
        let pack_reader = Arc::clone(repository.pack_reader.get_or_init(|| {
            Arc::new(crate::pack_store::PackReader::new(
                Arc::clone(&blob_store),
                repository.repository_id(),
                scratch_root.clone(),
                disk_budget.clone(),
                repository.object_format(),
            ))
        }));
        let lfs = LfsService::new(Arc::clone(&repository), blob_store);
        Self {
            repository,
            signer_directory: None,
            certificate_seed: OnceCell::new(),
            large_blobs,
            pack_reader,
            lfs,
            scratch_root,
            disk_budget,
            cache: Mutex::new(None),
            objects: Mutex::new(None),
            push: Mutex::new(()),
        }
    }

    pub(crate) fn with_signer_directory(
        mut self,
        directory: Arc<crate::directory::DirectoryCell>,
    ) -> Self {
        self.signer_directory = Some(directory);
        self
    }

    async fn certificate_nonce(&self) -> Result<Option<[u8; 32]>, GatewayError> {
        if self.signer_directory.is_none() {
            return Ok(None);
        }
        Ok(Some(
            *self
                .certificate_seed
                .get_or_try_init(|| async { self.repository.push_certificate_seed().await })
                .await?,
        ))
    }

    pub fn lfs(&self) -> &LfsService {
        &self.lfs
    }

    pub async fn access_level<'a>(
        &self,
        account: impl Into<ReadIdentity<'a>>,
    ) -> Result<Option<TokenScope>, GatewayError> {
        Ok(self
            .repository
            .access_level(account, None)
            .await
            .map_err(|error| GatewayError::Cell(Box::new(error)))?
            .output)
    }

    /// Waits for Cell publication before returning any successful receive-pack body.
    pub async fn handle<'a>(
        &self,
        request: GitHttpRequest<Body>,
        actor: impl Into<ReadIdentity<'a>>,
        push_id: Option<[u8; 16]>,
        admission: Option<Arc<AdmissionPermit>>,
    ) -> Result<GitHttpResponse<Body>, GatewayError> {
        let actor = actor.into();
        if self.access_level(actor).await?.is_none() {
            return Err(GatewayError::Unauthorized);
        }
        let is_push = request.method == "POST" && request.path_info == "/repo.git/git-receive-pack";
        if is_push {
            let ReadIdentity::Account(actor) = actor else {
                return Err(GatewayError::Unauthorized);
            };
            if !request.authenticated {
                return Err(GatewayError::Unauthorized);
            }
            let request = self.receive(request, None, admission).await?;
            let id = push_id.unwrap_or_else(|| uuid::Uuid::new_v4().into_bytes());
            let digest = request_digest(&request).await?;
            // Upload spooling uses a private, budgeted scratch file. Serialize
            // the push-ID check, decode, native Git work and publication, but
            // do not let one slow client block another client's upload.
            let _push = self.push.lock().await;
            if self.repository.begin_push(id, actor, digest).await? {
                return Ok(http_body(with_push_id(
                    self.repository.completed_response(id).await?,
                    id,
                )));
            }
            let request = self.decode(request, None).await?;
            return self
                .handle_push(request, actor, id, digest)
                .await
                .map(http_body);
        }
        let request = self
            .receive(request, Some(MAX_FETCH_REQUEST_BYTES), admission)
            .await?;
        let request = self.decode(request, Some(MAX_FETCH_REQUEST_BYTES)).await?;
        let capabilities = request.protocol_v2
            && request.method == "GET"
            && request.path_info == "/repo.git/info/refs"
            && url::form_urlencoded::parse(request.query.as_bytes())
                .eq([("service".into(), "git-upload-pack".into())]);
        let response = if capabilities {
            // Git v2 discovery advertises capabilities, not refs or objects.
            // Native Git still owns the wire response and capability policy.
            let head = self
                .repository
                .default_branch(None)
                .await
                .map_err(|error| GatewayError::Cell(Box::new(error)))?;
            let backend = GitHttpBackend::initialize(
                self.scratch_root.clone(),
                self.disk_budget.clone(),
                &head.output.reference,
                self.repository.object_format(),
            )
            .await?
            .with_nonce(self.certificate_nonce().await?);
            backend.stream(request, ()).await?
        } else if discovery::is_ref_discovery(&request).await? {
            if let Some(cached) = self.current_cache().await? {
                cached.backend.stream(request, Arc::clone(&cached)).await?
            } else {
                let backend = self.discovery_cache(self.cell_refs().await?).await?;
                backend.stream(request, ()).await?
            }
        } else {
            let fetch = fetch::FetchRequest::read(&request).await?;
            let cached = self.fetch_cache(&fetch.wants).await?;
            self.prepare_fetch(&cached, fetch).await?;
            cached.backend.stream(request, Arc::clone(&cached)).await?
        };
        Ok(GitHttpResponse {
            status: response.status,
            headers: response.headers,
            body: Body::from_stream(response.body),
        })
    }

    async fn current_cache(&self) -> Result<Option<Arc<CachedRepository>>, GatewayError> {
        // Hydration holds this mutex across storage I/O. Discovery must stay
        // independent, so inspect only a ready snapshot and release before SQL.
        let cached = self.cache.try_lock().ok().and_then(|cache| cache.clone());
        let Some(cached) = cached else {
            return Ok(None);
        };
        // Ref mutations, deletion/recreation and HEAD changes advance the same
        // generation transactionally. Matching it avoids scanning every ref page.
        let head = self
            .repository
            .default_branch(None)
            .await
            .map_err(|error| GatewayError::Cell(Box::new(error)))?
            .output;
        let current =
            cached.snapshot.generation == head.generation && cached.snapshot.head == head.reference;
        if current {
            tracing::debug!(
                repository = %hex::encode(self.repository.repository_id()),
                generation = head.generation,
                "reused Git ref snapshot"
            );
        }
        Ok(current.then_some(cached))
    }

    async fn receive(
        &self,
        request: GitHttpRequest<Body>,
        limit: Option<u64>,
        admission: Option<Arc<AdmissionPermit>>,
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
        limit: Option<u64>,
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

    async fn build_cache(
        &self,
        snapshot: RefSnapshot,
        include_blobs: bool,
    ) -> Result<CachedRepository, GatewayError> {
        // Only hydration writes the shared cache, and only from durable Cell
        // records. Native pushes/merges write into their private generation.
        let mut objects = self.objects.lock().await;
        if objects.is_none() {
            *objects = Some(CachedObjects {
                cache: self.pack_reader.cache().await?,
                through: 0,
            });
        }
        let shared = objects.as_mut().ok_or(GatewayError::MalformedCache)?;
        self.restore_packs(shared).await?;
        if include_blobs {
            self.hydrate(shared).await?;
        } else {
            // Native ref advertisement only needs tips and peeled tags. Fetch
            // hydrates the requested structural graph after validating wants.
            self.hydrate_selected(
                &shared.cache,
                snapshot
                    .refs
                    .values()
                    .filter_map(|state| state.oid)
                    .collect(),
            )
            .await?;
        }
        let backend = GitHttpBackend {
            cache: GitCache::create_with_objects(
                self.scratch_root.clone(),
                self.disk_budget.clone(),
                &snapshot.head,
                self.repository.object_format(),
                Some(Arc::clone(&shared.cache)),
            )
            .await?,
            nonce_seed: self.certificate_nonce().await?,
            signers: None,
        };
        backend.cache.store_refs(&snapshot.refs).await?;
        Ok(CachedRepository { backend, snapshot })
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
                let complete = !page.has_more;
                for (name, state) in page.refs {
                    after = name.clone();
                    refs.insert(name, state);
                }
                if complete {
                    tracing::debug!(
                        repository = %hex::encode(self.repository.repository_id()),
                        generation = page.generation,
                        refs = refs.len(),
                        "read Git ref snapshot"
                    );
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
        let started = std::time::Instant::now();
        let initial_high_water = self
            .repository
            .object_high_water()
            .await
            .map_err(|error| GatewayError::Cell(Box::new(error)))?
            .output;
        let mut sources = backend.cache.pack_sources().await?;
        let mut archive = None;
        let mut packed_ids = None;
        if sources.len() == 1 {
            let (hash, pack, index, ids) = sources.pop().ok_or(GatewayError::MalformedCache)?;
            let record = crate::pack_store::PackRecord {
                hash,
                pack: self.pack_reader.upload(pack).await?,
                index: self.pack_reader.upload(index).await?,
                approved: false,
                covered_through: 0,
            };
            self.repository
                .register_pack(new_identity()?, &record)
                .await
                .map_err(|error| GatewayError::Cell(Box::new(error)))?;
            archive = Some(record);
            packed_ids = Some(ids);
        }
        let mut objects = if let Some(ids) = packed_ids {
            GitObjects::packed(&backend.git_dir(), ids)?
        } else {
            GitObjects::start(&backend.git_dir(), included, excluded)?
        };

        let mut batch = ObjectBatch::default();
        let mut verified = HashSet::new();
        let mut logged = std::time::Instant::now();
        loop {
            let mut candidates = Vec::with_capacity(crate::object_batch::MAX_BATCH_OBJECTS);
            for _ in 0..crate::object_batch::MAX_BATCH_OBJECTS {
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
                .canonical_headers(&candidates)
                .await
                .map_err(|error| GatewayError::Cell(Box::new(error)))?;
            for oid in candidates {
                if let Some((kind, size, digest)) = present.get(&oid) {
                    if archive.is_some() {
                        objects
                            .read(oid)
                            .await?
                            .verify(*kind, *size, *digest)
                            .await?;
                        verified.insert(oid);
                    }
                    continue;
                }
                let mut input = objects.read(oid).await?;
                let object = if input.kind == ObjectKind::Blob && archive.is_some() {
                    let size = input.size;
                    let digest = input.fingerprint().await?;
                    StoredObject {
                        oid,
                        kind: ObjectKind::Blob,
                        storage: ObjectStorage::Packed {
                            size,
                            blake3: digest,
                            pack: archive
                                .as_ref()
                                .ok_or(GatewayError::MalformedCache)?
                                .pack
                                .sha256,
                        },
                    }
                } else if input.kind == ObjectKind::Blob && input.size > INLINE_OBJECT_LIMIT as u64
                {
                    let uploaded = self
                        .large_blobs
                        .put(oid, input.size, &mut input.reader)
                        .await?;
                    input.finish().await?;
                    StoredObject {
                        oid,
                        kind: ObjectKind::Blob,
                        storage: ObjectStorage::External {
                            size: uploaded.size,
                            blake3: uploaded.blake3,
                            sha256: uploaded.sha256,
                        },
                    }
                } else {
                    let (kind, body) = input.body().await?;
                    if body.len() > INLINE_OBJECT_LIMIT {
                        self.repository
                            .stage_object(new_identity()?, kind, &body)
                            .await
                            .map_err(|error| GatewayError::Cell(Box::new(error)))?
                    } else if let Some(record) =
                        archive.as_ref().filter(|_| kind == ObjectKind::Blob)
                    {
                        StoredObject {
                            oid,
                            kind,
                            storage: ObjectStorage::Packed {
                                size: body.len() as u64,
                                blake3: *blake3::hash(&body).as_bytes(),
                                pack: record.pack.sha256,
                            },
                        }
                    } else {
                        StoredObject {
                            oid,
                            kind,
                            storage: ObjectStorage::Inline(body),
                        }
                    }
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
                verified.insert(oid);
            }
            if logged.elapsed().as_secs() >= 10 {
                tracing::info!(
                    objects = verified.len(),
                    elapsed_seconds = started.elapsed().as_secs_f64(),
                    "persisting Git objects"
                );
                logged = std::time::Instant::now();
            }
        }
        objects.finish().await?;
        if !batch.is_empty() {
            self.repository
                .put_objects(new_identity()?, batch)
                .await
                .map_err(|error| GatewayError::Cell(Box::new(error)))?;
        }
        if let Some(record) = &archive {
            self.repository
                .approve_pack(new_identity()?, record.pack.sha256, verified.len())
                .await
                .map_err(|error| GatewayError::Cell(Box::new(error)))?;
        }
        let mut shared = self.objects.lock().await;
        if let Some(shared) = shared.as_mut() {
            let count = verified.len();
            match shared
                .cache
                .retain_verified_packs(Arc::clone(&backend.cache), verified)
                .await
            {
                Ok(retained) if retained == count && shared.through == initial_high_water => {
                    if let Some(record) = &archive {
                        shared.cache.mark_durable_pack(record.pack.sha256);
                    }
                    shared.through = self
                        .repository
                        .object_high_water()
                        .await
                        .map_err(|error| GatewayError::Cell(Box::new(error)))?
                        .output;
                    tracing::info!(
                        objects = retained,
                        through = shared.through,
                        "retained verified receive pack for immediate fetch"
                    );
                }
                Ok(retained) => {
                    if retained == count
                        && let Some(record) = &archive
                    {
                        shared.cache.mark_durable_pack(record.pack.sha256);
                    }
                }
                Err(error) => {
                    tracing::warn!(error = ?error, "receive pack cache reuse skipped; durable hydration remains available")
                }
            }
        }
        tracing::info!(
            elapsed_seconds = started.elapsed().as_secs_f64(),
            "persisted Git objects"
        );
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
    let output = crate::native_git::command(git_dir)?
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

async fn git_refs(git_dir: &Path) -> Result<BTreeMap<String, crate::ObjectId>, GatewayError> {
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
    after: &BTreeMap<String, crate::ObjectId>,
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

fn parse_oid(oid: &str) -> Result<crate::ObjectId, GatewayError> {
    if !matches!(oid.len(), 40 | 64) {
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
