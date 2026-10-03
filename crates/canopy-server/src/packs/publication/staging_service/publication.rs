//! The lifecycle retains only a small ticket. The fair coordinator owns and
//! charges the original private proof, command and exact transport recovery.
use super::*;

pub struct StagedPublicationFailure {
    pub reason: StagingError,
    pub ready: ReadyPublication,
}
impl std::fmt::Debug for StagedPublicationFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StagedPublicationFailure")
            .field("reason", &self.reason)
            .finish_non_exhaustive()
    }
}
impl std::fmt::Display for StagedPublicationFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.reason.fmt(f)
    }
}
impl std::error::Error for StagedPublicationFailure {}

/// Observation cannot activate/discard the lifecycle's held final command.
#[derive(Clone)]
#[must_use]
pub struct StagedPublicationTicket {
    ticket: PublicationTicket,
}
impl StagedPublicationTicket {
    pub fn state(&self) -> PublicationState {
        self.ticket.state()
    }
    pub async fn wait(&self) -> PublicationState {
        self.ticket.wait().await
    }
    pub async fn response(
        &self,
    ) -> Result<crate::git_http::GitHttpResponse, CatalogPushResponseError> {
        self.ticket.response().await
    }
    pub async fn root_response(
        &self,
        store: &canopy_object_storage::artifact::ArtifactStore,
    ) -> Result<
        crate::git_http::GitHttpResponse<canopy_object_storage::artifact::ArtifactRead>,
        RootPushReplayError,
    > {
        self.ticket.root_response(store).await
    }
}
impl StagingTicket {
    /// Seal the bound phase and synchronously transfer the exact final command
    /// into fair dispatch admission. Existing workers/results, due renewal and
    /// a queued checkpoint drain before activation. Returns the original ready
    /// value on refusal. Call after retrieving the producer's typed result;
    /// awaiting publication inside an owned producer would block its own drain.
    pub fn publish(
        &self,
        coordinator: &PublicationCoordinator,
        ready: impl Into<ReadyPublication>,
    ) -> Result<StagedPublicationTicket, Box<StagedPublicationFailure>> {
        let ready = ready.into();
        let mut local = self.job.local.lock().expect("staging local");
        let reason =
            if local.fenced || local.stop || Instant::now() >= local.deadline.min(local.lifetime) {
                Some(StagingError::Inactive)
            } else if local.finishing {
                Some(StagingError::Duplicate)
            } else if !matches!(
                self.state(),
                StagingState::Bound(_) | StagingState::RegisteringInputs
            ) {
                Some(StagingError::NotReady)
            } else {
                match local.bound.as_ref() {
                    Some(session) if session.live_lease().is_err() => Some(StagingError::Inactive),
                    Some(session) if ready.belongs_to(session) => None,
                    _ => Some(StagingError::Context),
                }
            };
        if let Some(reason) = reason {
            return Err(Box::new(StagedPublicationFailure { reason, ready }));
        }
        let ticket = coordinator.try_reserve(ready).map_err(|failure| {
            Box::new(StagedPublicationFailure {
                reason: StagingError::PublicationAdmission(failure.reason),
                ready: failure.ready,
            })
        })?;
        *self.job.publication.lock().expect("staging publication") = Some(ticket.clone());
        local.finishing = true;
        self.job.status.send_replace(StagingState::Finishing);
        self.job.changed.notify_one();
        Ok(StagedPublicationTicket { ticket })
    }
    /// Retrieve an observer after caller cancellation, including after a known
    /// final result removes the operation from lifecycle admission.
    pub fn pending_publication(&self) -> Option<StagedPublicationTicket> {
        self.job
            .publication
            .lock()
            .expect("staging publication")
            .clone()
            .map(|ticket| StagedPublicationTicket { ticket })
    }
}

pub(super) async fn observe(inner: &Inner, job: &Job, ticket: &PublicationTicket) {
    loop {
        job.status.send_replace(StagingState::Publishing);
        match ticket.wait().await {
            PublicationState::Uncertain(error) => {
                job.status.send_replace(StagingState::Uncertain(Arc::new(
                    StagingError::Publication(error),
                )));
                inner.drained.notify_waiters();
                tokio::select! {
                    _ = await_recovery(job) => {},
                    _ = ticket.wait_recovered() => {},
                }
                job.local.lock().expect("staging local").recovery = false;
                // Another service-internal observer may already have scheduled
                // exact recovery. Never replace or reconstruct that command.
                if matches!(ticket.state(), PublicationState::Uncertain(_)) {
                    let _ = ticket.recover().await;
                }
            }
            PublicationState::Finished(outcome) => {
                {
                    let mut local = job.local.lock().expect("staging local");
                    local.fenced = true;
                    if let Some(session) = &local.bound {
                        session.fence();
                    }
                }
                drain_work(job).await;
                job.status.send_replace(StagingState::Published(outcome));
                remove(inner, job);
                return;
            }
            PublicationState::Discarded => {
                // Only service-internal premature discard can reach this path;
                // there is no durable final result to acknowledge.
                {
                    let mut local = job.local.lock().expect("staging local");
                    local.fenced = true;
                    if let Some(session) = &local.bound {
                        session.fence();
                    }
                }
                drain_work(job).await;
                job.status
                    .send_replace(StagingState::Fenced(Arc::new(StagingError::Inactive)));
                remove(inner, job);
                return;
            }
            _ => unreachable!("publication wait observes an outcome"),
        }
    }
}
