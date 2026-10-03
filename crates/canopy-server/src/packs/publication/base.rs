//! Authoritative preparation pin -> conditional certified base resolution.
//! The caller supplies the trusted application CellClient capability. Raw root
//! descriptors or decoded lease DTOs cannot construct this resolver.
use super::*;
use crate::packs::{
    catalog::{CatalogFiles, CatalogIndexes, CatalogReader},
    closure::{BaseBatch, BaseObject, BaseResolver, ClosureBase, ClosureContext, ClosureError},
    directory::index::IndexError,
    metadata::PAGE_OBJECTS,
};
use cellule_runtime::{CellClient, CellTarget, InvocationError, MutationIdentity, Receipt};
use std::{sync::Arc, time::Duration};
use tokio::time::{Instant, timeout_at};

#[derive(Debug, thiserror::Error)]
pub enum PreparationBaseError {
    #[error("authoritative preparation query failed")]
    Query(#[source] Box<InvocationError<Option<PreparationLease>>>),
    #[error("authoritative preparation frontier query failed")]
    Frontier(#[source] Box<InvocationError<Option<PreparationFrontier>>>),
    #[error("preparation renewal failed")]
    Command(#[source] Box<InvocationError<PreparationReply>>),
    #[error("preparation catalog loading failed")]
    Catalog(#[from] IndexError),
    #[error("preparation has no active matching lease")]
    Inactive,
    #[error("preparation lease context is inconsistent")]
    Context,
}
pub struct PreparationBaseResolver {
    pub(super) session: PreparationSession,
    selected: GenerationFact,
    reader: Option<Arc<CatalogReader>>,
    files: Arc<CatalogFiles>,
    indexes: Arc<CatalogIndexes>,
}
impl PreparationBaseResolver {
    pub(super) fn capability(&self) -> (&CellClient, &CellTarget, &LeaseCheck) {
        self.session.capability()
    }
    pub async fn open(
        client: CellClient,
        target: CellTarget,
        check: LeaseCheck,
        indexes: Arc<CatalogIndexes>,
        files: Arc<CatalogFiles>,
        minimum: Option<Receipt>,
    ) -> Result<Self, PreparationBaseError> {
        let session = PreparationSession::open(client, target, check, minimum).await?;
        Self::from_session(session, indexes, files).await
    }
    pub(super) async fn from_session(
        session: PreparationSession,
        indexes: Arc<CatalogIndexes>,
        files: Arc<CatalogFiles>,
    ) -> Result<Self, PreparationBaseError> {
        let (lease, deadline) = session.live_lease()?;
        if indexes.store().repository() != lease.token.repository
            || indexes.sources().format() != lease.format
        {
            return Err(PreparationBaseError::Context);
        }
        let reader = if let Some(catalog) = lease.base.catalog {
            Some(Arc::new(
                timeout_at(deadline, CatalogReader::open(Arc::clone(&indexes), catalog))
                    .await
                    .map_err(|_| PreparationBaseError::Inactive)??,
            ))
        } else {
            None
        };
        session.live_lease()?;
        Ok(Self {
            session,
            selected: lease.base,
            reader,
            files,
            indexes,
        })
    }
    pub(super) fn live_lease(&self) -> Result<(PreparationLease, Instant), PreparationBaseError> {
        self.session.live_lease()
    }
    pub(super) fn indexes(&self) -> Arc<CatalogIndexes> {
        Arc::clone(&self.indexes)
    }
    pub(super) fn files(&self) -> Arc<CatalogFiles> {
        Arc::clone(&self.files)
    }
    pub(super) fn catalog_parts(
        &self,
    ) -> (
        super::super::directory::snapshot::DirectorySnapshot,
        Option<super::super::sources::SourceRoot>,
    ) {
        match &self.reader {
            Some(reader) => (reader.directory(), reader.source_root()),
            None => (
                super::super::directory::snapshot::DirectorySnapshot::empty(
                    self.session.lease.token.repository,
                    self.session.lease.format,
                ),
                None,
            ),
        }
    }
    pub fn context(&self) -> ClosureContext {
        ClosureContext {
            repository: self.session.lease.token.repository,
            operation: self.session.lease.token.artifact_operation,
            format: self.session.lease.format,
            base: self.selected.catalog.map(|catalog| ClosureBase {
                catalog,
                generation: self.selected.generation,
            }),
        }
    }
    pub(super) fn context_token(&self) -> PreparationToken {
        self.session.lease.token
    }
    pub(super) fn generation_fact(&self) -> GenerationFact {
        self.selected
    }
    pub(super) fn retention_floor(&self) -> GenerationFact {
        self.session.lease.base
    }
    /// Select only facts read through the exact active attempt. The original
    /// floor, namespace, deadline and renewal fence are shared by all selections.
    pub(super) async fn select_current(&self) -> Result<Self, PreparationBaseError> {
        let (_, deadline) = self.live_lease()?;
        timeout_at(deadline, async {
            let started = Instant::now();
            let frontier = self
                .session
                .client
                .query::<CheckPreparationFrontier>(
                    &self.session.target,
                    None,
                    self.session.check.clone(),
                )
                .await
                .map_err(|error| PreparationBaseError::Frontier(Box::new(error)))?
                .output
                .ok_or(PreparationBaseError::Inactive)?;
            if frontier.lease.token != self.session.lease.token
                || frontier.lease.base != self.session.lease.base
                || frontier.lease.format != self.session.lease.format
                || frontier.current.generation < self.selected.generation
            {
                return Err(PreparationBaseError::Context);
            }
            let remaining = (frontier.lease.expires_at_ms - frontier.lease.observed_at_ms) as u64;
            let observed_deadline = started
                .checked_add(Duration::from_millis(remaining.min(MAX_LEASE_MS)))
                .ok_or(PreparationBaseError::Context)?;
            let deadline = {
                let mut shared = self
                    .session
                    .deadline
                    .lock()
                    .map_err(|_| PreparationBaseError::Context)?;
                *shared = (*shared).min(observed_deadline);
                deadline.min(*shared)
            };
            let reader = if frontier.current == self.selected {
                self.reader.clone()
            } else {
                match frontier.current.catalog {
                    Some(catalog) => Some(Arc::new(
                        timeout_at(
                            deadline,
                            CatalogReader::open(Arc::clone(&self.indexes), catalog),
                        )
                        .await
                        .map_err(|_| PreparationBaseError::Inactive)??,
                    )),
                    None => None,
                }
            };
            self.live_lease()?;
            if Instant::now() >= deadline {
                return Err(PreparationBaseError::Inactive);
            }
            Ok(Self {
                session: self.session.clone(),
                selected: frontier.current,
                reader,
                files: Arc::clone(&self.files),
                indexes: Arc::clone(&self.indexes),
            })
        })
        .await
        .map_err(|_| PreparationBaseError::Inactive)?
    }
    pub async fn renew(
        &self,
        identity: MutationIdentity,
        lease_ms: u64,
    ) -> Result<(), PreparationBaseError> {
        self.session.renew(identity, lease_ms).await
    }
}
impl BaseResolver for PreparationBaseResolver {
    async fn resolve(
        &self,
        base: ClosureBase,
        ids: &[crate::ObjectId],
    ) -> Result<BaseBatch, ClosureError> {
        if ids.len() > PAGE_OBJECTS
            || self.context().base != Some(base)
            || ids
                .iter()
                .any(|oid| oid.format() != self.session.lease.format || oid.is_zero())
        {
            return Err(ClosureError::Integrity);
        }
        let (_, deadline) = self
            .session
            .live_lease()
            .map_err(|_| ClosureError::LeaseExpired)?;
        let reader = self.reader.as_ref().ok_or(ClosureError::Integrity)?;
        let headers = timeout_at(deadline, reader.headers(ids, &*self.files, &*self.files))
            .await
            .map_err(|_| ClosureError::LeaseExpired)??;
        self.session
            .live_lease()
            .map_err(|_| ClosureError::LeaseExpired)?;
        if Instant::now() >= deadline {
            return Err(ClosureError::LeaseExpired);
        }
        Ok(BaseBatch {
            base,
            objects: headers
                .into_iter()
                .map(|header| {
                    header.map(|header| BaseObject {
                        header,
                        certified: true,
                    })
                })
                .collect(),
        })
    }
}
