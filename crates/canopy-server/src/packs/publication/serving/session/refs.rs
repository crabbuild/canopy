//! Ref facts come only from the accepted joint generation's immutable root.
use super::*;
use crate::packs::ref_state::{RefNameKey, RefStateSnapshot};
use crate::refs::{REF_PAGE_SIZE, RefExpectation, RefPage, valid_ref_name};

const PAGE_BYTES: usize = 512 * 1024;

pub struct ResolvedServingRef {
    pub generation: i64,
    pub reference: String,
    /// None means the name never existed. A retained deletion has a version.
    pub state: Option<RefExpectation>,
}

impl Inner {
    pub(super) async fn ref_snapshot(&self) -> Result<&RefStateSnapshot, ServingReadError> {
        self.refs
            .get_or_try_init(|| async {
                let fact = self.lease.fact;
                let snapshot = fact
                    .refs
                    .ok_or(ServingReadError::Context)?
                    .read(&self.context.indexes.store())
                    .await?;
                if snapshot.repository != self.lease.token.repository
                    || snapshot.format != self.lease.format
                    || snapshot.generation > fact.generation
                {
                    return Err(ServingReadError::Context);
                }
                Ok(snapshot)
            })
            .await
    }
}

impl ServingPin {
    pub(in crate::packs::publication::serving) async fn resolve_refs(
        &self,
        actor: Option<String>,
        names: &[String],
    ) -> Result<Vec<ResolvedServingRef>, ServingReadError> {
        if names.len() > 128
            || names.iter().any(|name| !valid_ref_name(name))
            || names.iter().map(String::len).sum::<usize>() > PAGE_BYTES
            || names.windows(2).any(|p| p[0] >= p[1])
        {
            return Err(ServingReadError::Context);
        }
        let names = names
            .iter()
            .map(|name| RefNameKey::new(name))
            .collect::<Result<Vec<_>, _>>()?;
        self.read_owned(actor, move |inner, deadline, _permit| async move {
            let snapshot = inner.ref_snapshot().await?;
            let mut result = Vec::with_capacity(names.len());
            for name in names {
                if Instant::now() >= deadline {
                    return Err(ServingReadError::Inactive);
                }
                result.push(ResolvedServingRef {
                    generation: snapshot.generation as i64,
                    state: inner
                        .context
                        .indexes
                        .refs()
                        .read(snapshot.root.clone(), name.as_str())
                        .await?,
                    reference: name.as_str().to_owned(),
                });
            }
            Ok(result)
        })
        .await
    }
    pub async fn resolve_ref(
        &self,
        actor: Option<String>,
        reference: Option<&str>,
    ) -> Result<ResolvedServingRef, ServingReadError> {
        if reference.is_some_and(|name| !valid_ref_name(name)) {
            return Err(ServingReadError::Context);
        }
        let reference = reference.map(RefNameKey::new).transpose()?;
        self.read_owned(actor, move |inner, deadline, _permit| async move {
            let snapshot = inner.ref_snapshot().await?;
            if Instant::now() >= deadline {
                return Err(ServingReadError::Inactive);
            }
            let reference = reference
                .as_ref()
                .map_or(snapshot.default_branch.as_str(), RefNameKey::as_str);
            let state = inner
                .context
                .indexes
                .refs()
                .read(snapshot.root.clone(), reference)
                .await?;
            Ok(ResolvedServingRef {
                generation: snapshot.generation as i64,
                reference: reference.to_owned(),
                state,
            })
        })
        .await
    }

    /// Count/byte-bounded page. Live cursors skip whole deleted subtrees; other
    /// consumers can retain tombstone versions without a second representation.
    pub async fn refs_page(
        &self,
        actor: Option<String>,
        after: &str,
        generation: Option<i64>,
        live_only: bool,
    ) -> Result<RefPage, ServingReadError> {
        if (!after.is_empty() && (!valid_ref_name(after) || generation.is_none()))
            || generation.is_some_and(|value| value < 0)
        {
            return Err(ServingReadError::Context);
        }
        let after = (!after.is_empty())
            .then(|| RefNameKey::new(after))
            .transpose()?;
        self.read_owned(actor, move |inner, deadline, _permit| async move {
            let snapshot = inner.ref_snapshot().await?;
            if Instant::now() >= deadline {
                return Err(ServingReadError::Inactive);
            }
            if generation.is_some_and(|value| value != snapshot.generation as i64) {
                return Err(ServingReadError::Changed);
            }
            let mut cursor =
                inner
                    .context
                    .indexes
                    .refs()
                    .cursor(snapshot.root.clone(), after, live_only)?;
            let mut refs = Vec::with_capacity(REF_PAGE_SIZE);
            let mut bytes = 0;
            let mut has_more = false;
            while let Some(record) = cursor.next().await? {
                let charge = record.name().len() + 64;
                if refs.len() == REF_PAGE_SIZE || charge > PAGE_BYTES - bytes {
                    has_more = true;
                    break;
                }
                bytes += charge;
                refs.push((record.name().to_owned(), record.state().clone()));
            }
            Ok(RefPage {
                generation: snapshot.generation as i64,
                default_branch: snapshot.default_branch.clone(),
                refs,
                has_more,
            })
        })
        .await
    }
}
