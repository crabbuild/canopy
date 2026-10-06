//! One workflow controller per admitted operation. Physical work still uses
//! the existing staged/bound worker slots; controllers must not block Bind.
use super::*;
use futures_util::FutureExt;
use std::fmt::Write;
pub(super) type DriverJoin =
    futures_util::future::Shared<futures_util::future::BoxFuture<'static, ()>>;

/// Retain diagnostics without retaining rejected preparation values or their
/// physical credits. Both message bytes and source traversal are bounded.
fn failure(error: &StagingError) -> StagingError {
    // A known stopped attempt remains distinguishable from ambiguous command
    // evidence. Wire callers may report failure only for this terminal state.
    if matches!(error, StagingError::Inactive) {
        return StagingError::Inactive;
    }
    struct Message(String);
    impl Write for Message {
        fn write_str(&mut self, value: &str) -> std::fmt::Result {
            let remaining = 4096 - self.0.len();
            if value.len() <= remaining {
                self.0.push_str(value);
                return Ok(());
            }
            let mut end = remaining;
            while !value.is_char_boundary(end) {
                end -= 1;
            }
            self.0.push_str(&value[..end]);
            Err(std::fmt::Error)
        }
    }
    let mut message = Message(String::new());
    let mut source: Option<&(dyn std::error::Error + 'static)> = Some(error);
    for _ in 0..8 {
        let Some(error) = source else { break };
        if !message.0.is_empty() && message.write_str(": ").is_err() {
            break;
        }
        if write!(&mut message, "{error}").is_err() {
            break;
        }
        source = error.source();
    }
    StagingError::DriverFailure(message.0.into_boxed_str())
}

impl StagingCoordinator {
    /// Called only after RepositoryCell selected this root from the completed
    /// row under current audit authorization. No client-provided root enters.
    pub(crate) async fn completed_options(
        &self,
        bytes: &[u8],
    ) -> Result<Vec<String>, StagingError> {
        let (_, _, store) = self.inner.resident.as_ref().ok_or(StagingError::Inactive)?;
        let mut decoder =
            BoundedDecoder::new(bytes, 128).map_err(|e| StagingError::Input(Box::new(e)))?;
        let root = NativeOutcomeRoot::decode(&mut decoder)
            .map_err(|e| StagingError::Input(Box::new(e)))?;
        decoder
            .finish()
            .map_err(|e| StagingError::Input(Box::new(e)))?;
        root_completion::read::selected_options(root, store)
            .await
            .map_err(StagingError::Input)
    }

    /// Select a completed response with current authorization before decoding or
    /// admitting another attempt. The resident holds the actual Cell capability.
    pub async fn replay_request(
        &self,
        request: BeginRequest,
        store: &canopy_object_storage::artifact::ArtifactStore,
    ) -> Result<
        Option<crate::git_http::GitHttpResponse<canopy_object_storage::artifact::ArtifactRead>>,
        RootPushReplayError,
    > {
        let (client, _, _) = self
            .inner
            .resident
            .as_ref()
            .ok_or(RootPushReplayError::Context)?;
        replay_root_push_response(client, &self.inner.target, request, None, store).await
    }

    pub(crate) fn new_resident(
        client: CellClient,
        target: CellTarget,
        limits: StagingLimits,
        authority: PreparationAuthority,
        budget: StagingBudget,
        publication: PublicationCoordinator,
        store: Arc<canopy_object_storage::artifact::ArtifactStore>,
    ) -> Result<Self, StagingError> {
        if !publication.matches_target(&target)
            || crate::repository_target(target.tenant(), target.application(), store.repository())
                .map_err(|_| StagingError::Foreign)?
                != target
        {
            return Err(StagingError::Foreign);
        }
        let mut coordinator = Self::new_with_budget(target, limits, authority, budget)?;
        Arc::get_mut(&mut coordinator.inner)
            .expect("new staging owner")
            .resident = Some((client, publication, store));
        Ok(coordinator)
    }

    /// Prepare a request with the resident's actual Cell capability. The
    /// existing registered custody factory performs the authoritative checks.
    pub async fn ready_request(
        &self,
        request: BeginRequest,
        identity: MutationIdentity,
    ) -> Result<ReadyStaging, StagingError> {
        {
            let admission = self.inner.admission.lock().expect("staging admission");
            if admission.closed || admission.paused {
                return Err(StagingError::Closed);
            }
        }
        let (client, _, _) = self.inner.resident.as_ref().ok_or(StagingError::Inactive)?;
        ReadyStaging::new(client.clone(), self.inner.target.clone(), request, identity).await
    }

    /// Candidate observers join by the digest of the existing frozen editorial
    /// intent. Search only the bounded admitted jobs, without a second UUID
    /// cache. Uncertain work keeps its original ticket/commands; once a known
    /// attempt fully drains, a new operation can retry the same pending intent.
    pub(crate) fn join_generated_candidate(
        &self,
        request: &BeginRequest,
    ) -> Result<Option<StagingTicket>, StagingError> {
        if crate::repository_target(
            self.inner.target.tenant(),
            self.inner.target.application(),
            request.repository,
        )
        .map_err(|_| StagingError::Context)?
            != self.inner.target
        {
            return Err(StagingError::Foreign);
        }
        let admitted = self.inner.admission.lock().expect("staging admission");
        Ok(admitted
            .jobs
            .values()
            .find(|job| job.actor == request.actor && job.request_digest == request.request_digest)
            .map(|job| StagingTicket {
                inner: self.inner.clone(),
                job: job.clone(),
            }))
    }

    /// Joining an operation ID requires the original authenticated context,
    /// including while Begin has not yet produced a token.
    pub fn join_request(
        &self,
        request: &BeginRequest,
    ) -> Result<Option<StagingTicket>, StagingError> {
        if crate::repository_target(
            self.inner.target.tenant(),
            self.inner.target.application(),
            request.repository,
        )
        .map_err(|_| StagingError::Context)?
            != self.inner.target
        {
            return Err(StagingError::Foreign);
        }
        let Some(ticket) = self.pending(request.operation) else {
            return Ok(None);
        };
        if ticket.job.actor != request.actor || ticket.job.request_digest != request.request_digest
        {
            return Err(StagingError::Context);
        }
        Ok(Some(ticket))
    }
}

impl StagingTicket {
    /// Transfer the entire workflow before the request's next await. An HTTP
    /// observer owns neither this task nor its exact command recovery. Only
    /// the production resident's publication dispatcher can be supplied here.
    /// Run physical work through `spawn`/`spawn_bound`; use this task only to
    /// retrieve their outputs and order checkpoint, Bind and publication steps.
    pub fn drive<F, Fut>(&self, producer: F) -> Result<(), StagingError>
    where
        F: FnOnce(StagingTicket, PublicationCoordinator) -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), StagingError>> + Send + 'static,
    {
        self.drive_with_drain(producer, false)
    }

    /// An authenticated receive-pack keeps its controller during the bounded
    /// node shutdown grace. Forced close still cancels it and joins physical
    /// workers and exact recovery before releasing the repository.
    pub(crate) fn drive_receive<F, Fut>(&self, producer: F) -> Result<(), StagingError>
    where
        F: FnOnce(StagingTicket, PublicationCoordinator) -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), StagingError>> + Send + 'static,
    {
        self.drive_with_drain(producer, true)
    }

    fn drive_with_drain<F, Fut>(&self, producer: F, graceful: bool) -> Result<(), StagingError>
    where
        F: FnOnce(StagingTicket, PublicationCoordinator) -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), StagingError>> + Send + 'static,
    {
        let publication = self
            .inner
            .resident
            .as_ref()
            .ok_or(StagingError::Inactive)?
            .1
            .clone();
        let admission = self.inner.admission.lock().expect("staging admission");
        let mut local = self.job.local.lock().expect("staging local");
        if admission.closed
            || admission.paused
            || local.stop
            || local.fenced
            || !admission
                .jobs
                .get(&self.job.operation)
                .is_some_and(|job| Arc::ptr_eq(job, &self.job))
        {
            return Err(StagingError::Inactive);
        }
        if local.driver_started {
            return Err(StagingError::Duplicate);
        }
        local.driver_started = true;
        local.driver_graceful = graceful;
        let ticket = self.clone();
        let owner = self.job.clone();
        let inner = self.inner.clone();
        let task = tokio::spawn(async move {
            let mut task = tokio::spawn(async move { producer(ticket, publication).await });
            let result = tokio::select! {
                result = &mut task => result.unwrap_or(Err(StagingError::Worker)),
                _ = owner.driver_stop.cancelled() => {
                    task.abort();
                    let _ = task.await;
                    Err(StagingError::Inactive)
                }
            };
            if let Err(error) = result {
                tracing::warn!(operation = %hex::encode(owner.operation), ?error, "owned push workflow stopped");
                // The lifecycle still owns every admitted exact command and
                // physical worker. Never replace an uncertain result with ng.
                let diagnostic = Arc::new(failure(&error));
                {
                    let mut local = owner.local.lock().expect("staging local");
                    local.driver_failure = Some(diagnostic);
                    local.stop = true;
                }
                // Stop is visible before returning any physical credit. Drop
                // outside the lock: an Activity destructor acquires it too.
                drop(error);
                owner.changed.notify_one();
            } else {
                let mut local = owner.local.lock().expect("staging local");
                if !local.finishing && !matches!(*owner.status.borrow(), StagingState::Published(_))
                {
                    local.stop = true;
                }
            }
            // Wake drain even when an uncertain command has no more workers.
            // Its exact owner may be awaiting explicit recovery independently.
            owner.changed.notify_one();
            inner.drained.notify_waiters();
        });
        *self.job.driver.lock().expect("staging driver") = Some(
            async move {
                let _ = task.await;
            }
            .boxed()
            .shared(),
        );
        Ok(())
    }
}

pub(super) async fn drain(job: &Job) {
    // Concurrent node/service drains must join the same actual task. Taking a
    // JoinHandle would let the second caller mistake its absence for completion.
    let task = job.driver.lock().expect("staging driver").clone();
    if let Some(task) = task {
        task.await;
    }
}
