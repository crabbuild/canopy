//! Durable bridge from Git smart HTTP to one repository's SQLite Cell.

use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error as StdError,
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    process::Stdio,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use cellule_runtime::{MutationIdentity, RequestId};
use flate2::{Compression, write::ZlibEncoder};
use object_store::ObjectStore;
use tokio::{io::AsyncWriteExt, process::Command, sync::Mutex};

use crate::{
    INLINE_OBJECT_LIMIT, ObjectKind, ObjectStorage, PushPlan, RefExpectation, RefUpdate,
    RepositoryCell,
    git_http::{GitHttpBackend, GitHttpError, GitHttpRequest, GitHttpResponse},
    large_blob::{LargeBlobError, LargeBlobReference, LargeBlobStore, MAX_EXTERNAL_BLOB_BYTES},
    lfs::LfsService,
    object_id,
};

type CellError = Box<dyn StdError + Send + Sync>;

#[derive(Debug, thiserror::Error)]
pub enum GatewayError {
    #[error("Git HTTP backend failed")]
    Http(#[from] GitHttpError),
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
    #[error("Git object is too large for the current Cell ingest path")]
    ObjectTooLarge,
    #[error("ref publication conflicted with current Cell state")]
    RefConflict,
    #[error("authentication is required")]
    Unauthorized,
    #[error("cache task failed")]
    Task(#[from] tokio::task::JoinError),
}

struct CachedRepository {
    _scratch: tempfile::TempDir,
    backend: GitHttpBackend,
    refs: BTreeMap<String, RefExpectation>,
}

/// Serves Git requests from a warm, disposable cache of durable Cell state.
pub struct GitGateway {
    repository: Arc<RepositoryCell>,
    large_blobs: LargeBlobStore,
    lfs: LfsService,
    scratch_root: PathBuf,
    cache: Mutex<Option<CachedRepository>>,
}

impl GitGateway {
    pub fn new(
        repository: Arc<RepositoryCell>,
        scratch_root: PathBuf,
        blob_store: Arc<dyn ObjectStore>,
    ) -> Self {
        let large_blobs = LargeBlobStore::new(Arc::clone(&blob_store), repository.repository_id());
        let lfs = LfsService::new(Arc::clone(&repository), blob_store);
        Self {
            repository,
            large_blobs,
            lfs,
            scratch_root,
            cache: Mutex::new(None),
        }
    }

    pub fn lfs(&self) -> &LfsService {
        &self.lfs
    }

    /// Waits for Cell publication before returning any successful receive-pack body.
    pub async fn handle(&self, request: GitHttpRequest) -> Result<GitHttpResponse, GatewayError> {
        if !request.authenticated {
            return Err(GatewayError::Unauthorized);
        }
        let is_push = request.method == "POST" && request.path_info == "/repo.git/git-receive-pack";
        let mut cache = self.cache.lock().await;
        let live_refs = self.cell_refs().await?;
        if cache.as_ref().is_none_or(|cached| cached.refs != live_refs) {
            *cache = Some(self.build_cache(live_refs).await?);
        }
        let Some(cached) = cache.as_mut() else {
            return Err(GatewayError::MalformedCache);
        };
        let outcome = self.handle_cached(cached, request, is_push).await;
        if outcome.is_err() {
            // Any failed push may have moved only disposable Git refs.
            *cache = None;
        }
        outcome
    }

    async fn handle_cached(
        &self,
        cached: &mut CachedRepository,
        request: GitHttpRequest,
        is_push: bool,
    ) -> Result<GitHttpResponse, GatewayError> {
        let before = cached.refs.clone();
        let response = cached.backend.run(request).await?;
        if is_push && has_rejected_ref(&response.body) {
            return Err(GatewayError::RefConflict);
        }
        if is_push && response.status != 200 {
            return Err(GatewayError::RefConflict);
        }
        if !is_push {
            return Ok(response);
        }
        let after = git_refs(&cached.backend.git_dir()).await?;
        let plan = diff_refs(&before, &after);
        if plan.updates.is_empty() {
            return Ok(response);
        }
        self.persist_objects(&cached.backend).await?;
        let result = self
            .repository
            .finalize_push(new_identity()?, plan)
            .await
            .map_err(|error| match error {
                cellule_runtime::InvocationError::Rejected(_) => GatewayError::RefConflict,
                other => GatewayError::Cell(Box::new(other)),
            })?;
        if !result.output {
            return Err(GatewayError::RefConflict);
        }
        cached.refs = self.cell_refs().await?;
        Ok(response)
    }

    async fn build_cache(
        &self,
        refs: BTreeMap<String, RefExpectation>,
    ) -> Result<CachedRepository, GatewayError> {
        let scratch = tempfile::TempDir::new_in(&self.scratch_root)?;
        let backend = GitHttpBackend::initialize(scratch.path().to_path_buf()).await?;
        self.hydrate(&backend, &refs).await?;
        Ok(CachedRepository {
            _scratch: scratch,
            backend,
            refs,
        })
    }

    async fn hydrate(
        &self,
        backend: &GitHttpBackend,
        refs: &BTreeMap<String, RefExpectation>,
    ) -> Result<(), GatewayError> {
        let git_dir = backend.git_dir();
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
            let git_dir = git_dir.clone();
            tokio::task::spawn_blocking(move || {
                write_loose_object(&git_dir, object.oid, object.kind, &body)
            })
            .await??;
        }
        if refs.is_empty() {
            return Ok(());
        }
        let mut input = Vec::new();
        input.extend_from_slice(b"start\n");
        for (name, state) in refs {
            input.extend_from_slice(
                format!("update {name} {}\n", hex::encode(state.oid)).as_bytes(),
            );
        }
        input.extend_from_slice(b"prepare\ncommit\n");
        git_with_stdin(&git_dir, &["update-ref", "--stdin"], input).await?;
        Ok(())
    }

    async fn cell_refs(&self) -> Result<BTreeMap<String, RefExpectation>, GatewayError> {
        let mut refs = BTreeMap::new();
        let mut after = String::new();
        loop {
            let page = self
                .repository
                .refs_page(&after)
                .await
                .map_err(|error| GatewayError::Cell(Box::new(error)))?
                .output;
            if page.is_empty() {
                break;
            }
            for (name, state) in page {
                after = name.clone();
                refs.insert(name, state);
            }
        }
        Ok(refs)
    }

    async fn persist_objects(&self, backend: &GitHttpBackend) -> Result<(), GatewayError> {
        let git_dir = backend.git_dir();
        let listing = git_output(
            &git_dir,
            &[
                "cat-file",
                "--batch-all-objects",
                "--batch-check=%(objectname) %(objecttype) %(objectsize)",
            ],
        )
        .await?;
        for line in listing
            .split(|byte| *byte == b'\n')
            .filter(|line| !line.is_empty())
        {
            let line = std::str::from_utf8(line).map_err(|_| GatewayError::MalformedCache)?;
            let mut fields = line.split_whitespace();
            let (Some(oid), Some(kind), Some(size), None) =
                (fields.next(), fields.next(), fields.next(), fields.next())
            else {
                return Err(GatewayError::MalformedCache);
            };
            let oid = parse_oid(oid)?;
            let kind = parse_kind(kind)?;
            let size: usize = size.parse().map_err(|_| GatewayError::MalformedCache)?;
            if self
                .repository
                .object_exists(oid)
                .await
                .map_err(|error| GatewayError::Cell(Box::new(error)))?
                .output
            {
                continue;
            }
            if size > MAX_EXTERNAL_BLOB_BYTES
                || (size > INLINE_OBJECT_LIMIT && kind != ObjectKind::Blob)
            {
                return Err(GatewayError::ObjectTooLarge);
            }
            let body =
                git_output(&git_dir, &["cat-file", kind.git_name(), &hex::encode(oid)]).await?;
            if body.len() != size || object_id(kind, &body) != oid {
                return Err(GatewayError::MalformedCache);
            }
            if kind == ObjectKind::Blob && body.len() > INLINE_OBJECT_LIMIT {
                let uploaded = self.large_blobs.put(&body).await?;
                if uploaded.oid != oid {
                    return Err(GatewayError::MalformedCache);
                }
                self.repository
                    .put_external_blob(
                        new_identity()?,
                        oid,
                        uploaded.size,
                        uploaded.blake3,
                        uploaded.sha256,
                    )
                    .await
                    .map_err(|error| GatewayError::Cell(Box::new(error)))?;
            } else {
                let stored = self
                    .repository
                    .put_inline_object(new_identity()?, kind, &body)
                    .await
                    .map_err(|error| GatewayError::Cell(Box::new(error)))?;
                if stored.output != oid {
                    return Err(GatewayError::MalformedCache);
                }
            }
        }
        Ok(())
    }
}

fn write_loose_object(
    git_dir: &Path,
    oid: [u8; 20],
    kind: ObjectKind,
    body: &[u8],
) -> Result<(), std::io::Error> {
    if object_id(kind, body) != oid {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Git OID mismatch",
        ));
    }
    let hex = hex::encode(oid);
    let directory = git_dir.join("objects").join(&hex[..2]);
    fs::create_dir_all(&directory)?;
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(directory.join(&hex[2..]))?;
    let mut encoder = ZlibEncoder::new(file, Compression::default());
    encoder.write_all(format!("{} {}\0", kind.git_name(), body.len()).as_bytes())?;
    encoder.write_all(body)?;
    encoder.finish()?;
    Ok(())
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

async fn git_with_stdin(git_dir: &Path, args: &[&str], input: Vec<u8>) -> Result<(), GatewayError> {
    let mut child = Command::new("git")
        .arg("--git-dir")
        .arg(git_dir)
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()?;
    let Some(mut stdin) = child.stdin.take() else {
        return Err(GatewayError::MalformedCache);
    };
    stdin.write_all(&input).await?;
    drop(stdin);
    let output = child.wait_with_output().await?;
    if !output.status.success() {
        return Err(GatewayError::Git(
            String::from_utf8_lossy(&output.stderr).into_owned(),
        ));
    }
    Ok(())
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
) -> PushPlan {
    let names: BTreeSet<_> = before.keys().chain(after.keys()).cloned().collect();
    let updates = names
        .into_iter()
        .filter_map(|name| {
            let expected = before.get(&name).cloned();
            let new_oid = after.get(&name).copied();
            if expected.as_ref().map(|state| state.oid) == new_oid {
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
    PushPlan { updates }
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

fn parse_kind(kind: &str) -> Result<ObjectKind, GatewayError> {
    match kind {
        "blob" => Ok(ObjectKind::Blob),
        "tree" => Ok(ObjectKind::Tree),
        "commit" => Ok(ObjectKind::Commit),
        "tag" => Ok(ObjectKind::Tag),
        _ => Err(GatewayError::MalformedCache),
    }
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

fn has_rejected_ref(body: &[u8]) -> bool {
    body.windows(3).any(|window| window == b"ng ")
        || body.windows(7).any(|window| window == b"unpack ")
            && !body.windows(9).any(|window| window == b"unpack ok")
}
