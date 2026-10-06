//! Complete certified forward graph in admitted disk, with owned native inputs.
//! The cache may physically contain extra pack objects. Only the retained
//! membership spool authorizes wants; file presence never grants reachability.
use super::*;
use crate::packs::{catalog::graph_spool::GraphSpool, metadata::MetadataError};
use crate::{ObjectId, git_cache::GitCache, git_objects::ReadOwner};

#[derive(Clone, Copy, Debug)]
pub struct WorkspaceLimits {
    pub max_spool_bytes: u64,
    pub cache_kib: u32,
}
impl Default for WorkspaceLimits {
    fn default() -> Self {
        Self {
            max_spool_bytes: 1 << 30,
            cache_kib: 256,
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WorkspaceStats {
    pub objects: u64,
    pub packs: u64,
    pub input_bytes: u64,
}
#[derive(Clone)]
pub struct NativeWorkspace {
    core: Arc<Core>,
}
struct Core {
    cache: Arc<GitCache>,
    spool: Arc<Mutex<GraphSpool>>,
    pin: ServingPin,
    actor: Option<String>,
    stats: WorkspaceStats,
    complete_packs: bool,
}
impl NativeWorkspace {
    pub(crate) fn backend(&self, nonce_seed: Option<[u8; 32]>) -> crate::git_http::GitHttpBackend {
        crate::git_http::GitHttpBackend {
            cache: self.core.cache.clone(),
            nonce_seed,
            signers: None,
        }
    }
    pub fn object_format(&self) -> crate::ObjectFormat {
        self.core.pin.inner.lease.format
    }
    pub fn fact(&self) -> GenerationFact {
        self.core.pin.fact()
    }
    pub fn stats(&self) -> WorkspaceStats {
        self.core.stats
    }
    // Raw paths/owners stay crate-private. A decoded DTO cannot mint a native
    // read capability; producers must authorize requests and use contains.
    #[cfg(test)]
    pub(crate) fn git_dir(&self) -> std::path::PathBuf {
        self.core.cache.git_dir()
    }
    pub(crate) fn read_owner(&self) -> ReadOwner {
        self.core.clone()
    }
    /// Snapshot-specific forward membership, in caller order, including blobs
    /// and trees. Cache presence, future refs and unrelated histories are ignored.
    pub async fn contains(&self, ids: &[ObjectId]) -> Result<Vec<bool>, ServingReadError> {
        if ids.len() > PAGE_OBJECTS
            || ids
                .iter()
                .any(|id| id.is_zero() || id.format() != self.core.pin.inner.lease.format)
        {
            return Err(ServingReadError::Context);
        }
        let ids = ids.to_vec();
        let core = self.core.clone();
        self.core
            .pin
            .read_owned(
                self.core.actor.clone(),
                move |inner, _, permit| async move {
                    let owner = (core.clone(), inner.child(), permit);
                    tokio::task::spawn_blocking(move || {
                        let _owner = owner;
                        core.spool
                            .lock()
                            .map_err(|_| MetadataError::Integrity)?
                            .contains(&ids)
                    })
                    .await?
                    .map_err(|error| crate::packs::directory::index::IndexError::from(error).into())
                },
            )
            .await
    }
    /// Read only objects in the completed forward closure, even when downloaded
    /// packs contain other certified objects. Verify every returned native body.
    pub async fn body(
        &self,
        oid: ObjectId,
        limit: usize,
    ) -> Result<Option<Vec<u8>>, ServingReadError> {
        if oid.is_zero() || oid.format() != self.core.pin.inner.lease.format {
            return Err(ServingReadError::Context);
        }
        if limit == 0 || limit > super::body::MAX_BODY_BYTES {
            return Err(ServingReadError::TooLarge);
        }
        let core = self.core.clone();
        let workspace = self.clone();
        self.core
            .pin
            .read_owned(
                self.core.actor.clone(),
                move |inner, _, permit| async move {
                    let owner: ReadOwner =
                        Arc::new((inner.child(), permit, workspace.read_owner()));
                    if !job(&core.spool, owner.clone(), move |s| s.contains(&[oid])).await?[0] {
                        return Ok(None);
                    }
                    let reader = inner.catalog().await?;
                    let object = reader
                        .lookup(oid, &*inner.context.files, &*inner.context.files)
                        .await?
                        .ok_or(ServingReadError::Context)?;
                    let expected = object.entry.header.object;
                    if expected.size > limit as u64 {
                        return Err(ServingReadError::TooLarge);
                    }
                    if core.complete_packs {
                        let mut objects = crate::git_objects::GitObjects::batch_owned(
                            &core.cache.git_dir(),
                            &core.cache.native,
                            owner,
                        )
                        .map_err(crate::packs::catalog::NativeReadError::from)?;
                        let body = objects
                            .read_verified(expected, limit)
                            .await
                            .map_err(crate::packs::catalog::NativeReadError::from)?;
                        objects
                            .finish()
                            .await
                            .map_err(crate::packs::catalog::NativeReadError::from)?;
                        Ok(Some(body))
                    } else {
                        Ok(Some(inner.context.files.body(object, limit, owner).await?))
                    }
                },
            )
            .await
    }
}
impl ServingPin {
    pub(in crate::packs::publication::serving) async fn workspace(
        &self,
        actor: Option<String>,
        roots: Option<&[ObjectId]>,
        limits: WorkspaceLimits,
        snapshot: ServingSnapshot,
    ) -> Result<NativeWorkspace, ServingReadError> {
        if roots.is_some_and(|roots| {
            roots.is_empty()
                || roots.len() > MAX_EDGE_PARENTS
                || roots.windows(2).any(|p| p[0] >= p[1])
                || roots
                    .iter()
                    .any(|id| id.is_zero() || id.format() != self.inner.lease.format)
        }) {
            return Err(ServingReadError::Context);
        }
        if limits.max_spool_bytes < 16 << 10
            || limits.max_spool_bytes > canopy_object_storage::external::MAX_ARTIFACT_BYTES
            || !limits.max_spool_bytes.is_multiple_of(4096)
            || limits.cache_kib == 0
            || limits.cache_kib > 256
        {
            return Err(ServingReadError::Context);
        }
        let roots = roots.map(<[ObjectId]>::to_vec);
        let materialize = roots.is_some();
        let pin = self.clone();
        self.read_session(
            actor.clone(),
            move |inner, deadline, permit| async move {
                let mut observation = Observation {
                    deadline,
                    next: Instant::now(),
                };
                // The borrow sustains producer renewal during long construction and
                // through returned native workers, descendants and cache cleanup.
                let refs = if roots.is_none() {
                    Some(inner.ref_snapshot().await?.clone())
                } else {
                    None
                };
                let head = refs
                    .as_ref()
                    .map_or("refs/heads/main", |refs| refs.default_branch.as_str())
                    .to_owned();
                let cleanup: ReadOwner = Arc::new((inner.child(), snapshot));
                let owner: ReadOwner = Arc::new((cleanup.clone(), permit));
                let cache = inner
                    .context
                    .files
                    .workspace(owner.clone(), cleanup.clone(), head)
                    .await?;
                let spool = inner
                    .context
                    .files
                    .graph_spool(
                        limits.max_spool_bytes,
                        limits.cache_kib,
                        owner.clone(),
                        cleanup,
                    )
                    .await
                    .map_err(crate::packs::directory::index::IndexError::from)?;
                if let Some(roots) = roots {
                    job(&spool, owner.clone(), move |s| {
                        s.add(&roots.into_iter().map(|id| (id, None)).collect::<Vec<_>>())
                    })
                    .await?;
                } else {
                    let refs = refs.ok_or(ServingReadError::Context)?;
                    let mut cursor =
                        inner
                            .context
                            .indexes
                            .refs()
                            .cursor(refs.root.clone(), None, true)?;
                    let mut writer = cache
                        .serving_refs(owner.clone())
                        .await
                        .map_err(crate::packs::catalog::NativeReadError::from)?;
                    loop {
                        observation.refresh(&inner, &actor).await?;
                        let mut page = Vec::with_capacity(crate::refs::REF_PAGE_SIZE);
                        for _ in 0..crate::refs::REF_PAGE_SIZE {
                            let Some(record) = cursor.next().await? else {
                                break;
                            };
                            page.push((record.name().to_owned(), record.state().clone()));
                        }
                        if page.is_empty() {
                            break;
                        }
                        let roots = page
                            .iter()
                            .map(|(_, state)| state.oid.map(|id| (id, None)))
                            .collect::<Option<Vec<_>>>()
                            .ok_or(ServingReadError::Context)?;
                        job(&spool, owner.clone(), move |s| s.add(&roots)).await?;
                        writer = writer
                            .append(page)
                            .await
                            .map_err(crate::packs::catalog::NativeReadError::from)?;
                    }
                    writer
                        .finish()
                        .await
                        .map_err(crate::packs::catalog::NativeReadError::from)?;
                }
                let reader = inner.catalog().await?;
                let mut stats = WorkspaceStats {
                    objects: 0,
                    packs: 0,
                    input_bytes: 0,
                };
                loop {
                    observation.refresh(&inner, &actor).await?;
                    let pending = job(&spool, owner.clone(), |s| s.pending()).await?;
                    if pending.is_empty() {
                        break;
                    }
                    for (id, expected) in &pending {
                        observation.refresh(&inner, &actor).await?;
                        let object = reader
                            .lookup(*id, &*inner.context.files, &*inner.context.files)
                            .await?
                            .ok_or(ServingReadError::Context)?;
                        let kind = object.entry.header.object.kind;
                        if expected.is_some_and(|expected| expected != kind) {
                            return Err(ServingReadError::Context);
                        }
                        let id = *id;
                        job(&spool, owner.clone(), move |s| s.add(&[(id, Some(kind))])).await?;
                        let source = object.source.record.native();
                        source.validate(inner.context.repository(), inner.lease.format)?;
                        if !job(&spool, owner.clone(), move |s| s.pack_seen(source)).await? {
                            // Producer workspaces need a complete native
                            // baseline. Install each certified pair once, rather
                            // than expanding every object into loose copies.
                            if materialize {
                                inner
                                    .context
                                    .files
                                    .install_pack_workspace(cache.clone(), source, owner.clone())
                                    .await?;
                                observation.refresh(&inner, &actor).await?;
                            }
                            observation.refresh(&inner, &actor).await?;
                            job(&spool, owner.clone(), move |s| s.imported(source)).await?;
                            stats.packs = stats
                                .packs
                                .checked_add(1)
                                .ok_or(ServingReadError::TooLarge)?;
                            stats.input_bytes = stats
                                .input_bytes
                                .checked_add(source.pack.size)
                                .and_then(|n| n.checked_add(source.index.size))
                                .ok_or(ServingReadError::TooLarge)?;
                        }
                        let metadata = object.source.metadata;
                        let mut cursor = None;
                        loop {
                            observation.refresh(&inner, &actor).await?;
                            let metadata = metadata.clone();
                            let keep = owner.clone();
                            let edges = tokio::task::spawn_blocking(move || {
                                let _owner = keep;
                                metadata.edges_after(id, cursor)
                            })
                            .await?
                            .map_err(crate::packs::directory::index::IndexError::from)?;
                            if edges.is_empty() {
                                break;
                            }
                            cursor = edges.last().map(|edge| edge.child);
                            let count = edges.len();
                            job(&spool, owner.clone(), move |s| {
                                s.add(
                                    &edges
                                        .into_iter()
                                        .map(|edge| (edge.child, Some(edge.expected_kind)))
                                        .collect::<Vec<_>>(),
                                )
                            })
                            .await?;
                            if count < PAGE_OBJECTS {
                                break;
                            }
                        }
                    }
                    let count = pending.len() as u64;
                    job(&spool, owner.clone(), move |s| s.done(&pending)).await?;
                    stats.objects = stats
                        .objects
                        .checked_add(count)
                        .ok_or(ServingReadError::TooLarge)?;
                }
                inner.observe(actor.clone()).await?;
                Ok(NativeWorkspace {
                    core: Arc::new(Core {
                        cache,
                        spool,
                        pin,
                        actor,
                        stats,
                        complete_packs: materialize,
                    }),
                })
            },
            true,
        )
        .await
    }
}
pub(super) async fn job<T: Send + 'static>(
    spool: &Arc<Mutex<GraphSpool>>,
    owner: ReadOwner,
    body: impl FnOnce(&mut GraphSpool) -> Result<T, MetadataError> + Send + 'static,
) -> Result<T, ServingReadError> {
    let spool = spool.clone();
    tokio::task::spawn_blocking(move || {
        let _owner = owner;
        let mut spool = spool.lock().map_err(|_| MetadataError::Integrity)?;
        body(&mut spool)
    })
    .await?
    .map_err(|error| crate::packs::directory::index::IndexError::from(error).into())
}

/// Amortize authority queries across bounded graph steps, rather than issuing
/// repository SQL per object. Long provider suspensions still force a fresh check
/// before the next step, and construction always rechecks before returning.
pub(super) struct Observation {
    pub(super) deadline: Instant,
    pub(super) next: Instant,
}
impl Observation {
    pub(super) async fn refresh(
        &mut self,
        inner: &Inner,
        actor: &Option<String>,
    ) -> Result<(), ServingReadError> {
        let now = Instant::now();
        if now >= self.next || now >= self.deadline {
            self.deadline = inner.observe(actor.clone()).await?.1;
            self.next = (Instant::now() + std::time::Duration::from_millis(250)).min(self.deadline);
        }
        Ok(())
    }
}

mod prepare;
