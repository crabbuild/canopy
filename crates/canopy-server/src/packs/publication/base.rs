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
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
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
    client: CellClient,
    target: CellTarget,
    check: LeaseCheck,
    lease: PreparationLease,
    selected: GenerationFact,
    deadline: Arc<Mutex<Instant>>,
    reader: Option<Arc<CatalogReader>>,
    files: Arc<CatalogFiles>,
    indexes: Arc<CatalogIndexes>,
    fenced: Arc<AtomicBool>,
}
impl PreparationBaseResolver {
    pub(super) fn capability(&self) -> (&CellClient, &CellTarget, &LeaseCheck) {
        (&self.client, &self.target, &self.check)
    }
    pub async fn open(
        client: CellClient,
        target: CellTarget,
        check: LeaseCheck,
        indexes: Arc<CatalogIndexes>,
        files: Arc<CatalogFiles>,
        minimum: Option<Receipt>,
    ) -> Result<Self, PreparationBaseError> {
        if crate::repository_target(
            target.tenant(),
            target.application(),
            check.token.repository,
        )
        .map_err(|_| PreparationBaseError::Context)?
            != target
        {
            return Err(PreparationBaseError::Context);
        }
        let (lease, deadline) = probe(&client, &target, &check, minimum).await?;
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
        if Instant::now() >= deadline {
            return Err(PreparationBaseError::Inactive);
        }
        Ok(Self {
            client,
            target,
            check,
            lease,
            selected: lease.base,
            deadline: Arc::new(Mutex::new(deadline)),
            reader,
            files,
            indexes,
            fenced: Arc::new(AtomicBool::new(false)),
        })
    }
    pub(super) fn live_lease(&self) -> Result<(PreparationLease, Instant), PreparationBaseError> {
        let deadline = *self
            .deadline
            .lock()
            .map_err(|_| PreparationBaseError::Context)?;
        if self.fenced.load(Ordering::Acquire) || Instant::now() >= deadline {
            return Err(PreparationBaseError::Inactive);
        }
        Ok((self.lease, deadline))
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
                    self.lease.token.repository,
                    self.lease.format,
                ),
                None,
            ),
        }
    }
    pub fn context(&self) -> ClosureContext {
        ClosureContext {
            repository: self.lease.token.repository,
            operation: self.lease.token.artifact_operation,
            format: self.lease.format,
            base: self.selected.catalog.map(|catalog| ClosureBase {
                catalog,
                generation: self.selected.generation,
            }),
        }
    }
    pub(super) fn context_token(&self) -> PreparationToken {
        self.lease.token
    }
    pub(super) fn generation_fact(&self) -> GenerationFact {
        self.selected
    }
    pub(super) fn retention_floor(&self) -> GenerationFact {
        self.lease.base
    }
    /// Select only facts read through the exact active attempt. The original
    /// floor, namespace, deadline and renewal fence are shared by all selections.
    pub(super) async fn select_current(&self) -> Result<Self, PreparationBaseError> {
        let (_, deadline) = self.live_lease()?;
        timeout_at(deadline, async {
            let started = Instant::now();
            let frontier = self
                .client
                .query::<CheckPreparationFrontier>(&self.target, None, self.check.clone())
                .await
                .map_err(|error| PreparationBaseError::Frontier(Box::new(error)))?
                .output
                .ok_or(PreparationBaseError::Inactive)?;
            if frontier.lease.token != self.lease.token
                || frontier.lease.base != self.lease.base
                || frontier.lease.format != self.lease.format
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
                client: self.client.clone(),
                target: self.target.clone(),
                check: self.check.clone(),
                lease: self.lease,
                selected: frontier.current,
                deadline: Arc::clone(&self.deadline),
                reader,
                files: Arc::clone(&self.files),
                indexes: Arc::clone(&self.indexes),
                fenced: Arc::clone(&self.fenced),
            })
        })
        .await
        .map_err(|_| PreparationBaseError::Inactive)?
    }
    /// A recorded renewal result is never a fresh clock observation. Query
    /// after the durability gate even when the command is exact-outcome replay.
    pub async fn renew(
        &self,
        identity: MutationIdentity,
        lease_ms: u64,
    ) -> Result<(), PreparationBaseError> {
        let result = self.renew_inner(identity, lease_ms).await;
        if result.is_err() {
            self.fenced.store(true, Ordering::Release);
        }
        result
    }
    async fn renew_inner(
        &self,
        identity: MutationIdentity,
        lease_ms: u64,
    ) -> Result<(), PreparationBaseError> {
        if self.fenced.load(Ordering::Acquire) {
            return Err(PreparationBaseError::Inactive);
        }
        let committed = self
            .client
            .command::<RenewPreparation>(
                &self.target,
                identity,
                LeaseRequest {
                    check: self.check.clone(),
                    lease_ms,
                },
            )
            .await
            .map_err(|error| PreparationBaseError::Command(Box::new(error)))?;
        let (lease, deadline) = probe(
            &self.client,
            &self.target,
            &self.check,
            Some(committed.receipt),
        )
        .await?;
        if lease.token != self.lease.token
            || lease.base != self.lease.base
            || lease.format != self.lease.format
        {
            return Err(PreparationBaseError::Context);
        }
        *self
            .deadline
            .lock()
            .map_err(|_| PreparationBaseError::Context)? = deadline;
        Ok(())
    }
}
async fn probe(
    client: &CellClient,
    target: &CellTarget,
    check: &LeaseCheck,
    minimum: Option<Receipt>,
) -> Result<(PreparationLease, Instant), PreparationBaseError> {
    // Start before the query, not after its reply, so transport/queue time can
    // only shorten the usable lease. Queries do not replay stored commands.
    let started = Instant::now();
    let lease = client
        .query::<CheckPreparation>(target, minimum, check.clone())
        .await
        .map_err(|error| PreparationBaseError::Query(Box::new(error)))?
        .output
        .ok_or(PreparationBaseError::Inactive)?;
    if lease.token != check.token
        || lease.observed_at_ms < 0
        || lease.expires_at_ms <= lease.observed_at_ms
    {
        return Err(PreparationBaseError::Context);
    }
    let remaining = (lease.expires_at_ms - lease.observed_at_ms) as u64;
    let deadline = started
        .checked_add(Duration::from_millis(remaining.min(MAX_LEASE_MS)))
        .ok_or(PreparationBaseError::Context)?;
    if Instant::now() >= deadline {
        return Err(PreparationBaseError::Inactive);
    }
    Ok((lease, deadline))
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
                .any(|oid| oid.format() != self.lease.format || oid.is_zero())
        {
            return Err(ClosureError::Integrity);
        }
        let deadline = *self.deadline.lock().map_err(|_| ClosureError::Integrity)?;
        if self.fenced.load(Ordering::Acquire) || Instant::now() >= deadline {
            return Err(ClosureError::LeaseExpired);
        }
        let reader = self.reader.as_ref().ok_or(ClosureError::Integrity)?;
        let headers = timeout_at(deadline, reader.headers(ids, &*self.files, &*self.files))
            .await
            .map_err(|_| ClosureError::LeaseExpired)??;
        if self.fenced.load(Ordering::Acquire) || Instant::now() >= deadline {
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
