//! Disposable native base for write preparation. Catalog presence supplies
//! inputs, never fetch reachability or authority to publish mutations.
use super::workspace::{Observation, job};
use super::*;
use crate::git_objects::ReadOwner;

impl ServingPin {
    pub(in crate::packs::publication::serving) async fn native_base(
        &self,
        actor: Option<String>,
        snapshot: ServingSnapshot,
    ) -> Result<crate::git_http::GitHttpBackend, ServingReadError> {
        self.read_session(
            actor.clone(),
            move |inner, deadline, permit| async move {
                let mut observation = Observation {
                    deadline,
                    next: Instant::now(),
                };
                let refs = inner.ref_snapshot().await?.clone();
                let cleanup: ReadOwner = Arc::new((inner.child(), snapshot));
                let owner: ReadOwner = Arc::new((cleanup.clone(), permit));
                let cache = inner
                    .context
                    .files
                    .workspace(owner.clone(), cleanup.clone(), refs.default_branch.clone())
                    .await?;
                let limits = WorkspaceLimits::default();
                let spool = inner
                    .context
                    .files
                    .graph_spool(
                        limits.max_spool_bytes,
                        limits.cache_kib,
                        owner.clone(),
                        cleanup.clone(),
                    )
                    .await
                    .map_err(crate::packs::directory::index::IndexError::from)?;
                let reader = inner.catalog().await?;
                let mut sources = reader.source_changes(None, None)?;
                loop {
                    observation.refresh(&inner, &actor).await?;
                    let page = sources.page(128, 64 << 10).await?;
                    if page.is_empty() {
                        break;
                    }
                    for record in page {
                        observation.refresh(&inner, &actor).await?;
                        let native = record.native();
                        native.validate(inner.context.repository(), inner.lease.format)?;
                        if !job(&spool, owner.clone(), move |s| s.pack_seen(native)).await? {
                            inner
                                .context
                                .files
                                .install_pack_workspace(cache.clone(), native, owner.clone())
                                .await?;
                            observation.refresh(&inner, &actor).await?;
                            job(&spool, owner.clone(), move |s| s.imported(native)).await?;
                        }
                    }
                }
                // Writable native results have their own pack directory. Baseline
                // catalog inputs remain immutable alternates, never incoming packs.
                let cache = inner
                    .context
                    .files
                    .write_workspace(owner.clone(), cleanup, refs.default_branch.clone(), cache)
                    .await?;
                let mut names = inner.context.indexes.refs().cursor(refs.root, None, true)?;
                let mut writer = cache
                    .serving_refs(owner.clone(), false)
                    .await
                    .map_err(crate::packs::catalog::NativeReadError::from)?;
                loop {
                    observation.refresh(&inner, &actor).await?;
                    let mut page = Vec::with_capacity(crate::refs::REF_PAGE_SIZE);
                    for _ in 0..crate::refs::REF_PAGE_SIZE {
                        let Some(record) = names.next().await? else {
                            break;
                        };
                        page.push((record.name().to_owned(), record.state().clone()));
                    }
                    if page.is_empty() {
                        break;
                    }
                    writer = writer
                        .append(page)
                        .await
                        .map_err(crate::packs::catalog::NativeReadError::from)?;
                }
                writer
                    .finish()
                    .await
                    .map_err(crate::packs::catalog::NativeReadError::from)?;
                inner.observe(actor).await?;
                Ok(crate::git_http::GitHttpBackend {
                    cache,
                    nonce_seed: None,
                    signers: None,
                })
            },
            true,
        )
        .await
    }
}
