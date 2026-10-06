//! Requested structural closure, then native Git's exact blob filter selection.
use super::*;
use crate::{ObjectKind, git_objects::GitObjectWalk};

const SELECTION_PAGE: usize = 128;

// Native size inspection consumes bodies even when the final response omits
// them. Retain only the candidates admitted by the other combined filters.
fn size_candidates(filter: &str, depth: usize) -> Result<Option<String>, ServingReadError> {
    if depth > 128 {
        return Err(ServingReadError::TooLarge);
    }
    if filter.starts_with("blob:limit=") {
        return Ok(None);
    }
    let Some(parts) = filter.strip_prefix("combine:") else {
        return Ok(Some(filter.into()));
    };
    let mut selected = Vec::new();
    for part in parts.split('+') {
        let decoded = percent_encoding::percent_decode_str(part)
            .decode_utf8()
            .map_err(|_| ServingReadError::Context)?;
        if let Some(value) = size_candidates(&decoded, depth + 1)? {
            selected.push(value);
        }
    }
    match selected.len() {
        0 => Ok(None),
        1 => Ok(selected.pop()),
        _ => Ok(Some(format!(
            "combine:{}",
            selected
                .iter()
                .map(|value| percent_encoding::utf8_percent_encode(
                    value,
                    percent_encoding::NON_ALPHANUMERIC
                )
                .to_string())
                .collect::<Vec<_>>()
                .join("+")
        ))),
    }
}

impl NativeWorkspace {
    pub(crate) async fn prepare_advertisement(&self) -> Result<(), ServingReadError> {
        let core = self.core.clone();
        let actor = core.actor.clone();
        self.core
            .pin
            .read_owned(actor.clone(), move |inner, deadline, permit| async move {
                let owner: ReadOwner = Arc::new((inner.child(), permit, core.clone()));
                let mut observation = Observation {
                    deadline,
                    next: Instant::now(),
                };
                let refs = inner.ref_snapshot().await?.clone();
                let mut cursor =
                    inner
                        .context
                        .indexes
                        .refs()
                        .cursor(refs.root.clone(), None, true)?;
                while let Some(record) = cursor.next().await? {
                    observation.refresh(&inner, &actor).await?;
                    let mut id = record.state().oid.ok_or(ServingReadError::Context)?;
                    let mut finished = false;
                    // Peeling requires only the advertised object and nested tags,
                    // never an unrelated commit's ancestors or tree history.
                    for _ in 0..128 {
                        let reader = inner.catalog().await?;
                        let object = reader
                            .lookup(id, &*inner.context.files, &*inner.context.files)
                            .await?
                            .ok_or(ServingReadError::Context)?;
                        let kind = object.entry.header.object.kind;
                        inner
                            .context
                            .files
                            .install_workspace(core.cache.clone(), &object, owner.clone())
                            .await?;
                        observation.refresh(&inner, &actor).await?;
                        if kind != ObjectKind::Tag {
                            finished = true;
                            break;
                        }
                        let metadata = object.source.metadata;
                        let held = owner.clone();
                        let edges = tokio::task::spawn_blocking(move || {
                            let _owner = held;
                            metadata.edges_after(id, None)
                        })
                        .await?
                        .map_err(crate::packs::directory::index::IndexError::from)?;
                        if edges.len() != 1 {
                            return Err(ServingReadError::Context);
                        }
                        id = edges[0].child;
                    }
                    if !finished {
                        return Err(ServingReadError::TooLarge);
                    }
                }
                inner.observe(actor).await?;
                Ok(())
            })
            .await
    }

    pub(crate) async fn prepare_fetch(
        &self,
        roots: Vec<ObjectId>,
        filter: Option<String>,
        needs_blob_sizes: bool,
    ) -> Result<(), ServingReadError> {
        if roots.is_empty() {
            return Ok(());
        }
        if roots.len() > crate::git_input::MAX_FETCH_REQUEST_BYTES as usize / 40
            || roots
                .iter()
                .any(|id| id.is_zero() || id.format() != self.object_format())
        {
            return Err(ServingReadError::Context);
        }
        let core = self.core.clone();
        let actor = core.actor.clone();
        self.core
            .pin
            .read_owned(actor.clone(), move |inner, deadline, permit| async move {
                let owner: ReadOwner = Arc::new((inner.child(), permit, core.clone()));
                let limits = WorkspaceLimits::default();
                let spool = inner
                    .context
                    .files
                    .graph_spool(
                        limits.max_spool_bytes,
                        limits.cache_kib,
                        owner.clone(),
                        owner.clone(),
                    )
                    .await
                    .map_err(crate::packs::directory::index::IndexError::from)?;
                let mut observation = Observation {
                    deadline,
                    next: Instant::now(),
                };
                let reader = inner.catalog().await?;
                for ids in roots.chunks(PAGE_OBJECTS) {
                    let ids = ids.to_vec();
                    if !job(&core.spool, owner.clone(), {
                        let ids = ids.clone();
                        move |s| s.contains(&ids)
                    })
                    .await?
                    .into_iter()
                    .all(|present| present)
                    {
                        return Err(ServingReadError::Context);
                    }
                    job(&spool, owner.clone(), {
                        let ids = ids.clone();
                        move |s| s.add(&ids.into_iter().map(|id| (id, None)).collect::<Vec<_>>())
                    })
                    .await?;
                    // Explicit blob wants override a filter. They must exist before
                    // rev-list, which cannot start from a missing root object.
                    for id in ids {
                        observation.refresh(&inner, &actor).await?;
                        let object = reader
                            .lookup(id, &*inner.context.files, &*inner.context.files)
                            .await?
                            .ok_or(ServingReadError::Context)?;
                        if object.entry.header.object.kind == ObjectKind::Blob {
                            inner
                                .context
                                .files
                                .install_workspace(core.cache.clone(), &object, owner.clone())
                                .await?;
                        }
                    }
                }
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
                        if kind != ObjectKind::Blob {
                            inner
                                .context
                                .files
                                .install_workspace(core.cache.clone(), &object, owner.clone())
                                .await?;
                        } else if needs_blob_sizes {
                            inner
                                .context
                                .files
                                .install_transient_workspace(
                                    core.cache.clone(),
                                    &object,
                                    owner.clone(),
                                )
                                .await?;
                        }
                        let metadata = object.source.metadata;
                        let mut cursor = None;
                        loop {
                            observation.refresh(&inner, &actor).await?;
                            let metadata = metadata.clone();
                            let held = owner.clone();
                            let edges = tokio::task::spawn_blocking(move || {
                                let _owner = held;
                                metadata.edges_after(id, cursor)
                            })
                            .await?
                            .map_err(crate::packs::directory::index::IndexError::from)?;
                            if edges.is_empty() {
                                break;
                            }
                            cursor = edges.last().map(|edge| edge.child);
                            let count = edges.len();
                            // An explicit tag naming a blob must remain peelable.
                            // Only tag roots can introduce tags in this closure.
                            if kind == ObjectKind::Tag {
                                for edge in &edges {
                                    if edge.expected_kind == ObjectKind::Blob {
                                        let object = reader
                                            .lookup(
                                                edge.child,
                                                &*inner.context.files,
                                                &*inner.context.files,
                                            )
                                            .await?
                                            .ok_or(ServingReadError::Context)?;
                                        if object.entry.header.object.kind != ObjectKind::Blob {
                                            return Err(ServingReadError::Context);
                                        }
                                        inner
                                            .context
                                            .files
                                            .install_workspace(
                                                core.cache.clone(),
                                                &object,
                                                owner.clone(),
                                            )
                                            .await?;
                                    }
                                }
                            }
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
                    job(&spool, owner.clone(), move |s| s.done(&pending)).await?;
                }
                // Git sees all requested structural objects and decides tree/type/
                // combine filters exactly. Size filters require all requested blobs
                // first, since native Git cannot inspect an absent blob's size.
                let selection_filter = if needs_blob_sizes {
                    filter
                        .as_deref()
                        .map(|value| size_candidates(value, 0))
                        .transpose()?
                        .flatten()
                } else {
                    filter.clone()
                };
                let walk = if needs_blob_sizes {
                    GitObjectWalk::selected_owned
                } else {
                    GitObjectWalk::missing_owned
                };
                let mut missing = walk(
                    &core.cache.git_dir(),
                    roots,
                    selection_filter.as_deref(),
                    &core.cache.native,
                    owner.clone(),
                )
                .map_err(crate::packs::catalog::NativeReadError::from)?;
                let mut ids = Vec::with_capacity(SELECTION_PAGE);
                while let Some(id) = missing
                    .next()
                    .await
                    .map_err(crate::packs::catalog::NativeReadError::from)?
                {
                    observation.refresh(&inner, &actor).await?;
                    if needs_blob_sizes {
                        let selected = reader
                            .lookup(id, &*inner.context.files, &*inner.context.files)
                            .await?
                            .ok_or(ServingReadError::Context)?;
                        if selected.entry.header.object.kind != ObjectKind::Blob {
                            continue;
                        }
                    }
                    ids.push(id);
                    if ids.len() == SELECTION_PAGE {
                        let page = std::mem::replace(&mut ids, Vec::with_capacity(SELECTION_PAGE));
                        job(&spool, owner.clone(), move |s| s.retry(&page)).await?;
                    }
                }
                missing
                    .finish()
                    .await
                    .map_err(crate::packs::catalog::NativeReadError::from)?;
                if !ids.is_empty() {
                    job(&spool, owner.clone(), move |s| s.retry(&ids)).await?;
                }
                // Release rev-list's native admission before starting extraction;
                // a one-slot read budget must not require a nested native child.
                loop {
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
                        if expected != &Some(ObjectKind::Blob)
                            || object.entry.header.object.kind != ObjectKind::Blob
                        {
                            return Err(ServingReadError::Context);
                        }
                        inner
                            .context
                            .files
                            .install_workspace(core.cache.clone(), &object, owner.clone())
                            .await?;
                    }
                    job(&spool, owner.clone(), move |s| s.done(&pending)).await?;
                }
                inner.observe(actor).await?;
                Ok(())
            })
            .await
    }
}
