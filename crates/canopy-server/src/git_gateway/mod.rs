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
    git_cache::CacheError,
    git_http::{GitHttpBackend, GitHttpError, GitHttpRequest, GitHttpResponse},
    git_input::{GitInput, InputError, MAX_FETCH_REQUEST_BYTES},
    git_objects::GitObjects,
    lfs::LfsService,
    push::PushError,
};

mod branch_policy;
mod candidates;
mod discovery;
mod fetch;
mod merge;
pub mod preflight;
mod push;
mod ssh;

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

struct CachedRepository {
    backend: GitHttpBackend,
    refs: BTreeMap<String, RefExpectation>,
}

/// Serves Git requests from a warm, disposable cache of durable Cell state.
#[derive(Clone)]
pub struct GitGateway {
    repository: Arc<RepositoryCell>,
    signer_directory: Option<Arc<crate::directory::DirectoryCell>>,
    certificate_seed: Arc<OnceCell<[u8; 32]>>,
    large_blobs: Arc<LargeBlobStore>,
    pack_reader: Arc<crate::pack_store::PackReader>,
    lfs: Arc<LfsService>,
    artifacts: Arc<canopy_object_storage::artifact::ArtifactStore>,
    scratch_root: PathBuf,
    disk_budget: DiskBudget,
    native: crate::native_resources::NativeScope,
    push: Arc<Mutex<()>>,
}

impl GitGateway {
    pub fn new(
        repository: Arc<RepositoryCell>,
        scratch_root: PathBuf,
        blob_store: Arc<dyn ObjectStore>,
        disk_budget: DiskBudget,
        native: crate::native_resources::NativeResources,
    ) -> Self {
        let native = native.scope(crate::native_resources::NativeClass::Foreground);
        let large_blobs = Arc::new(LargeBlobStore::new(
            Arc::clone(&blob_store),
            repository.repository_id(),
        ));
        let artifacts = Arc::new(canopy_object_storage::artifact::ArtifactStore::new(
            Arc::clone(&blob_store),
            repository.repository_id(),
        ));
        // A reader belongs to this gateway's workspace and disk admission.
        // Another gateway may use a different root/budget for the same Cell.
        let pack_reader = Arc::new(crate::pack_store::PackReader::new(
            Arc::clone(&blob_store),
            repository.repository_id(),
            scratch_root.clone(),
            disk_budget.clone(),
            repository.object_format(),
            native.clone(),
        ));
        {
            let mut readers = repository
                .pack_readers
                .lock()
                .expect("packed reader registry poisoned");
            readers.retain(|reader| reader.strong_count() > 0);
            readers.push(Arc::downgrade(&pack_reader));
        }
        let lfs = Arc::new(LfsService::new(Arc::clone(&repository), blob_store));
        Self {
            repository,
            signer_directory: None,
            certificate_seed: Arc::new(OnceCell::new()),
            large_blobs,
            pack_reader,
            lfs,
            artifacts,
            scratch_root,
            disk_budget,
            native,
            push: Arc::new(Mutex::new(())),
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
            let encoded = preflight::EncodedPush::new(
                request,
                &self.repository.target,
                self.repository.repository_id(),
                self.repository.object_format(),
                actor,
                id,
            )
            .await?;
            return self.handle_native_push(encoded).await;
        }
        let request = self
            .receive(request, Some(MAX_FETCH_REQUEST_BYTES), admission)
            .await?;
        let request = self.decode(request, Some(MAX_FETCH_REQUEST_BYTES)).await?;
        let snapshot = self
            .repository
            .serving_snapshot(actor)
            .await
            .map_err(|e| GatewayError::Cell(Box::new(e)))?;
        let capabilities = request.protocol_v2
            && request.method == "GET"
            && request.path_info == "/repo.git/info/refs"
            && url::form_urlencoded::parse(request.query.as_bytes())
                .eq([("service".into(), "git-upload-pack".into())]);
        let response = if capabilities {
            let head = snapshot
                .resolve_ref(None)
                .await
                .map_err(|e| GatewayError::Cell(Box::new(e)))?;
            let backend = GitHttpBackend::initialize(
                self.scratch_root.clone(),
                self.disk_budget.clone(),
                &head.reference,
                self.repository.object_format(),
                self.native.clone(),
            )
            .await?
            .with_nonce(self.certificate_nonce().await?);
            backend.stream(request, snapshot).await?
        } else {
            let discovery = discovery::is_ref_discovery(&request).await?;
            let fetch = fetch::FetchRequest::read(&request).await?;
            let workspace = snapshot
                .ref_workspace(crate::packs::publication::WorkspaceLimits::default())
                .await
                .map_err(|e| GatewayError::Cell(Box::new(e)))?;
            Self::validate_wants(&workspace, &fetch.wants).await?;
            tracing::debug!(discovery,filter=?fetch.filter,generation=workspace.fact().generation,
                "prepared certified Git transport");
            let backend = workspace.backend(self.certificate_nonce().await?);
            backend.stream(request, workspace.read_owner()).await?
        };
        Ok(GitHttpResponse {
            status: response.status,
            headers: response.headers,
            body: Body::from_stream(response.body),
        })
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
        actor: &str,
        names: &[String],
    ) -> Result<CachedRepository, GatewayError> {
        if !valid_ref_names(names) {
            return Err(GatewayError::MalformedCache);
        }
        let snapshot = self
            .repository
            .serving_snapshot(ReadIdentity::Account(actor))
            .await
            .map_err(|error| GatewayError::Cell(Box::new(error)))?;
        let mut refs = BTreeMap::new();
        for page in ref_pages(names, 128, 256 << 10) {
            for resolved in snapshot
                .resolve_refs(page)
                .await
                .map_err(|error| GatewayError::Cell(Box::new(error)))?
            {
                if let Some(state) = resolved.state {
                    refs.insert(resolved.reference, state);
                }
            }
        }
        let backend = snapshot
            .native_base()
            .await
            .map_err(|error| GatewayError::Cell(Box::new(error)))?
            .with_nonce(self.certificate_nonce().await?);
        Ok(CachedRepository { backend, refs })
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
            };
            self.repository
                .register_pack(new_identity()?, &record)
                .await
                .map_err(|error| GatewayError::Cell(Box::new(error)))?;
            archive = Some(record);
            packed_ids = Some(ids);
        }
        let mut objects = if let Some(ids) = packed_ids {
            GitObjects::packed(&backend.git_dir(), ids, &backend.cache.native)?
        } else {
            GitObjects::start(
                &backend.git_dir(),
                included,
                excluded,
                &backend.cache.native,
            )?
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
        tracing::info!(
            elapsed_seconds = started.elapsed().as_secs_f64(),
            "persisted Git objects"
        );
        Ok(())
    }
}

fn with_push_id<B>(mut response: GitHttpResponse<B>, id: [u8; 16]) -> GitHttpResponse<B> {
    response.headers.push((
        "X-Canopy-Push-Id".into(),
        uuid::Uuid::from_bytes(id).to_string(),
    ));
    response
}

fn valid_ref_names(names: &[String]) -> bool {
    names.len() <= crate::refs::MAX_UPDATES
        && names.windows(2).all(|p| p[0] < p[1])
        && names.iter().all(|name| {
            name.len() <= crate::packs::ref_state::MAX_NAME_BYTES
                && crate::refs::valid_ref_name(name)
        })
}

fn ref_pages(names: &[String], count: usize, bytes: usize) -> impl Iterator<Item = &[String]> {
    let mut at = 0;
    std::iter::from_fn(move || {
        if at == names.len() {
            return None;
        }
        let start = at;
        let mut used = 0;
        while at < names.len() && at - start < count {
            let charge = names[at].len() + 1;
            if charge > bytes - used {
                break;
            }
            used += charge;
            at += 1;
        }
        // Callers validate names against MAX_NAME_BYTES, so one always fits.
        Some(&names[start..at])
    })
}

// Exact requested names only. for-each-ref patterns can scan entire subtrees
// when an absent requested name prefixes existing refs; cat-file resolves each
// validated literal ref independently and reports missing names in order.
async fn git_refs(
    backend: &GitHttpBackend,
    names: &[String],
) -> Result<BTreeMap<String, crate::ObjectId>, GatewayError> {
    if !valid_ref_names(names) {
        return Err(GatewayError::MalformedCache);
    }
    let mut refs = BTreeMap::new();
    for page in ref_pages(names, 32, 64 << 10) {
        let mut input = page.join("\n").into_bytes();
        input.push(b'\n');
        let output = candidates::run(
            backend,
            &["cat-file", "--batch-check=%(objectname)"],
            &input,
            &[],
        )
        .await?;
        if !output.status.success() {
            return Err(output.error());
        }
        let text = std::str::from_utf8(&output.stdout).map_err(|_| GatewayError::MalformedCache)?;
        let lines = text
            .strip_suffix('\n')
            .ok_or(GatewayError::MalformedCache)?
            .split('\n')
            .collect::<Vec<_>>();
        if lines.len() != page.len() {
            return Err(GatewayError::MalformedCache);
        }
        for (name, line) in page.iter().zip(lines) {
            if line == format!("{name} missing") {
                continue;
            }
            let id = crate::ObjectId::from_hex(line.as_bytes())
                .map_err(|_| GatewayError::MalformedCache)?;
            if id.is_zero() || id.format() != backend.cache.object_format {
                return Err(GatewayError::MalformedCache);
            }
            refs.insert(name.clone(), id);
        }
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

#[cfg(test)]
mod native_refs_tests {
    use super::*;
    #[tokio::test]
    async fn native_ref_reads_resolve_only_exact_requested_names_in_both_formats()
    -> Result<(), Box<dyn std::error::Error>> {
        for format in [crate::ObjectFormat::Sha1, crate::ObjectFormat::Sha256] {
            let root = tempfile::TempDir::new()?;
            let backend = GitHttpBackend::initialize(
                root.path().to_owned(),
                DiskBudget::new(8 << 20),
                "refs/heads/main",
                format,
                crate::native_resources::NativeResources::default()
                    .scope(crate::native_resources::NativeClass::Foreground),
            )
            .await?;
            let body = b"native ref lookup";
            let id = crate::object_id(format, ObjectKind::Blob, body);
            backend
                .cache
                .store_object(id, ObjectKind::Blob, body.to_vec())
                .await?;
            backend
                .cache
                .store_refs(&BTreeMap::from([
                    (
                        "refs/heads/main".into(),
                        RefExpectation {
                            oid: Some(id),
                            version: 1,
                        },
                    ),
                    (
                        "refs/heads/absent/child".into(),
                        RefExpectation {
                            oid: Some(id),
                            version: 1,
                        },
                    ),
                    (
                        "refs/tags/tag".into(),
                        RefExpectation {
                            oid: Some(id),
                            version: 1,
                        },
                    ),
                ]))
                .await?;
            let names = vec![
                "refs/heads/absent".into(),
                "refs/heads/main".into(),
                "refs/tags/missing".into(),
                "refs/tags/tag".into(),
            ];
            assert_eq!(
                git_refs(&backend, &names).await?,
                BTreeMap::from([("refs/heads/main".into(), id), ("refs/tags/tag".into(), id)])
            );
            assert!(matches!(
                git_refs(&backend, &["HEAD".into()]).await,
                Err(GatewayError::MalformedCache)
            ));
        }
        Ok(())
    }
    #[test]
    fn ref_pages_bound_names_and_bytes_and_reject_non_literal_input() {
        let long = format!(
            "refs/heads/{}",
            "x".repeat(crate::packs::ref_state::MAX_NAME_BYTES - 11)
        );
        let names = vec![long, "refs/tags/a".into(), "refs/tags/b".into()];
        assert!(valid_ref_names(&names));
        let pages: Vec<_> = ref_pages(&names, 32, 64 << 10).collect();
        assert_eq!(pages.iter().map(|p| p.len()).collect::<Vec<_>>(), [1, 2]);
        assert_eq!(pages.concat(), names);
        for name in [
            "HEAD",
            "refs/heads/a^",
            "refs/heads/a\n",
            "refs/heads/a:foo",
        ] {
            assert!(!valid_ref_names(&[name.into()]));
        }
        assert!(!valid_ref_names(&[format!(
            "refs/heads/{}",
            "x".repeat(65_536)
        )]));
    }
}
