//! Service-owned restart discovery over the existing independent attempt pins.
//! No local outbox, new identity or caller-supplied actor is used. The original
//! receiver still decides custody; discovery never adopts an old owner fence.
use super::*;
use std::{sync::Arc, time::Duration};
use tokio::{sync::watch, task::JoinHandle};

const MAX_PAGE: u16 = 128;
const SEEK: &str = "SELECT incarnation,admission_sequence FROM catalog_leases INDEXED BY catalog_leases_recovery_scan WHERE recovery IS NOT NULL AND (incarnation,admission_sequence)>(?1,?2) ORDER BY incarnation,admission_sequence LIMIT ?3";

#[derive(Clone, Copy, Debug)]
pub struct RecoveryScanLimits {
    /// Keys only; each bundle is authenticated and read separately (8 KiB).
    pub page: u16,
    /// Delay between pages, including retries and empty passes.
    pub interval: Duration,
}
impl Default for RecoveryScanLimits {
    fn default() -> Self {
        Self {
            page: MAX_PAGE,
            interval: Duration::from_secs(1),
        }
    }
}
impl RecoveryScanLimits {
    fn validate(self) -> Result<(), RootRecoveryError> {
        if self.page == 0
            || self.page > MAX_PAGE
            || self.interval < Duration::from_millis(10)
            || self.interval > Duration::from_secs(60)
        {
            return Err(RootRecoveryError::InvalidScanLimits);
        }
        Ok(())
    }
}

/// Cumulative bounded diagnostics, not a product response or read capability.
/// Only the latest failure is retained. Failed pins remain durable and are
/// revisited on the next pass; a broken artifact cannot strand later accounts.
#[derive(Clone, Debug, Default)]
pub struct RecoveryScanStats {
    pub passes: u64,
    pub scanned: u64,
    pub submitted: u64,
    pub recovered: u64,
    pub settled: u64,
    /// Retirement admissions and original-command retries (cumulative visits).
    pub release_submitted: u64,
    pub release_recovered: u64,
    /// Settled intermediate pages/denied roots require producer continuation.
    pub continuation: u64,
    pub deferred: u64,
    pub failures: u64,
    pub last_error: Option<Arc<RootRecoveryError>>,
}
impl RecoveryScanStats {
    fn failed(&mut self, error: RootRecoveryError) {
        self.failures = self.failures.saturating_add(1);
        self.last_error = Some(Arc::new(error));
    }
}

/// Start once for each service-owned repository coordinator. Dropping this
/// owner requests stop between scans; it never cancels an admitted command.
/// Shutdown joins the current scan. Close/drain the coordinator separately to
/// retain its unresolved tickets and their original admission reservations.
#[must_use]
pub struct RecoverySupervisor {
    stop: watch::Sender<bool>,
    stats: watch::Receiver<RecoveryScanStats>,
    task: Option<JoinHandle<RecoveryScanStats>>,
}
impl RecoverySupervisor {
    pub fn start(
        client: CellClient,
        target: CellTarget,
        store: ArtifactStore,
        coordinator: PublicationCoordinator,
        limits: RecoveryScanLimits,
    ) -> Result<Self, RootRecoveryError> {
        Self::start_inner(client, target, store, coordinator, limits, None)
    }
    /// The service supplies current repository administration and actual owner
    /// custody. Closed attempts release through the same fair maintenance queue;
    /// original uncertainty stays charged even after its pin disappears.
    pub fn start_retiring(
        client: CellClient,
        target: CellTarget,
        store: ArtifactStore,
        coordinator: PublicationCoordinator,
        limits: RecoveryScanLimits,
        maintenance: MaintenanceRequest,
    ) -> Result<Self, RootRecoveryError> {
        if maintenance.repository != store.repository() {
            return Err(RootRecoveryError::Context);
        }
        maintenance.encode(&mut BoundedEncoder::new(4096)?)?;
        Self::start_inner(
            client,
            target,
            store,
            coordinator,
            limits,
            Some(maintenance),
        )
    }
    fn start_inner(
        client: CellClient,
        target: CellTarget,
        store: ArtifactStore,
        coordinator: PublicationCoordinator,
        limits: RecoveryScanLimits,
        maintenance: Option<MaintenanceRequest>,
    ) -> Result<Self, RootRecoveryError> {
        limits.validate()?;
        if !coordinator.matches_target(&target)
            || crate::repository_target(target.tenant(), target.application(), store.repository())?
                != target
        {
            return Err(RootRecoveryError::Context);
        }
        let sql = SqlCell::<RepositoryModule>::new(client.clone(), target.clone())?;
        let (stop, stopping) = watch::channel(false);
        let (updates, stats) = watch::channel(RecoveryScanStats::default());
        let task = tokio::spawn(run(
            Scan {
                client,
                target,
                store,
                coordinator,
                maintenance,
            },
            sql,
            limits,
            stopping,
            updates,
        ));
        Ok(Self {
            stop,
            stats,
            task: Some(task),
        })
    }
    pub fn stats(&self) -> RecoveryScanStats {
        self.stats.borrow().clone()
    }
    pub async fn shutdown(mut self) -> Result<RecoveryScanStats, tokio::task::JoinError> {
        self.stop.send_replace(true);
        self.task.take().expect("owned restart scanner").await
    }
}
impl Drop for RecoverySupervisor {
    fn drop(&mut self) {
        self.stop.send_replace(true);
    }
}

#[derive(Clone, Copy, Default)]
struct Cursor {
    incarnation: [u8; 16],
    attempt: u64,
}
async fn page(
    sql: &SqlCell<RepositoryModule>,
    after: Cursor,
    limit: u16,
) -> Result<Vec<Cursor>, RootRecoveryError> {
    let result = sql
        .query(
            None,
            statement(
                SEEK,
                vec![
                    blob(after.incarnation),
                    number(after.attempt)?,
                    number(u64::from(limit))?,
                ],
            ),
        )
        .await
        .map_err(|error| RootRecoveryError::Query(Box::new(error)))?;
    let rows = rows(&result.output)?;
    if rows.len() > usize::from(limit) {
        return Err(RootRecoveryError::Context);
    }
    let mut keys = Vec::with_capacity(rows.len());
    let mut previous = after;
    for row in rows {
        let [incarnation, attempt] = row.as_slice() else {
            return Err(RootRecoveryError::Context);
        };
        let key = Cursor {
            incarnation: fixed(incarnation)?,
            attempt: unsigned(attempt)?,
        };
        if key.attempt == 0
            || (key.incarnation, key.attempt) <= (previous.incarnation, previous.attempt)
        {
            return Err(RootRecoveryError::Context);
        }
        keys.push(key);
        previous = key;
    }
    Ok(keys)
}

struct Scan {
    client: CellClient,
    target: CellTarget,
    store: ArtifactStore,
    coordinator: PublicationCoordinator,
    maintenance: Option<MaintenanceRequest>,
}
impl Scan {
    async fn visit(
        &self,
        key: Cursor,
        stats: &mut RecoveryScanStats,
    ) -> Result<(), RootRecoveryError> {
        let Some(registered) = RegisteredRootRecovery::load_pin(
            &self.client,
            &self.target,
            &self.store,
            IncarnationId::from_bytes(key.incarnation),
            key.attempt,
            None,
        )
        .await?
        else {
            return Ok(());
        };
        if let Some(ticket) = self.coordinator.pending(registered.token().operation).await {
            if ticket
                .recover_discovered(&registered)
                .await
                .map_err(|error| Error::Facility {
                    name: "restart recovery queue",
                    source: Box::new(error),
                })?
            {
                stats.recovered = stats.recovered.saturating_add(1);
            } else {
                stats.deferred = stats.deferred.saturating_add(1);
            }
            return Ok(());
        }
        // Settled heads must not consume new command slots every scan. Known
        // intermediate results remain pinned for the retained-input producer;
        // this worker cannot invent its next page/root or claim new custody.
        // A racing head advance is retried on the next pass. Discovery itself
        // never walks an arbitrary predecessor history; only an already owned
        // exact command needs historical receipt resolution in the fair queue.
        let journal = registered.current_journal(&self.client, None).await?;
        if journal.primary.is_some()
            && (!journal.refused(&registered.record)? || journal.refusal.is_some())
        {
            stats.settled = stats.settled.saturating_add(1);
            if journal.terminal(&registered.record)?.is_some()
                && let Some(maintenance) = &self.maintenance
            {
                if !registered.attempt_closed(&self.client).await? {
                    stats.deferred = stats.deferred.saturating_add(1);
                    return Ok(());
                }
                let identity =
                    crate::server::mutation_identity().map_err(|source| Error::Facility {
                        name: "terminal retirement identity",
                        source: Box::new(source),
                    })?;
                let ready = registered
                    .ready_terminal_release(
                        self.client.clone(),
                        &self.store,
                        maintenance.clone(),
                        identity,
                    )
                    .await?;
                match self.coordinator.submit(ready).await {
                    Ok(_) => stats.release_submitted = stats.release_submitted.saturating_add(1),
                    Err(failure) => match failure.reason {
                        PublicationScheduleError::Capacity
                        | PublicationScheduleError::Duplicate
                        | PublicationScheduleError::Closed => {
                            stats.deferred = stats.deferred.saturating_add(1)
                        }
                        reason => {
                            return Err(Error::Facility {
                                name: "terminal retirement admission",
                                source: Box::new(reason),
                            }
                            .into());
                        }
                    },
                }
            }
            if journal.may_advance(&registered.record)? {
                stats.continuation = stats.continuation.saturating_add(1);
            }
            return Ok(());
        }
        match self
            .coordinator
            .submit(registered.ready(self.client.clone(), self.store.clone())?)
            .await
        {
            Ok(_) => stats.submitted = stats.submitted.saturating_add(1),
            Err(failure) => match failure.reason {
                PublicationScheduleError::Capacity
                | PublicationScheduleError::Duplicate
                | PublicationScheduleError::Closed => {
                    stats.deferred = stats.deferred.saturating_add(1)
                }
                reason => {
                    return Err(Error::Facility {
                        name: "restart recovery admission",
                        source: Box::new(reason),
                    }
                    .into());
                }
            },
        }
        Ok(())
    }
}

async fn run(
    scan: Scan,
    sql: SqlCell<RepositoryModule>,
    limits: RecoveryScanLimits,
    mut stopping: watch::Receiver<bool>,
    updates: watch::Sender<RecoveryScanStats>,
) -> RecoveryScanStats {
    let mut stats = RecoveryScanStats::default();
    let mut after = Cursor::default();
    loop {
        if *stopping.borrow() {
            return stats;
        }
        if scan.maintenance.is_some() {
            match scan.coordinator.recover_terminal_releases().await {
                Ok(recovered) => {
                    stats.release_recovered = stats.release_recovered.saturating_add(recovered)
                }
                Err(source) => stats.failed(
                    Error::Facility {
                        name: "terminal retirement recovery",
                        source: Box::new(source),
                    }
                    .into(),
                ),
            }
        }
        match page(&sql, after, limits.page).await {
            Ok(keys) => {
                if keys.is_empty() {
                    after = Cursor::default();
                    stats.passes = stats.passes.saturating_add(1);
                }
                for key in keys {
                    if *stopping.borrow() {
                        break;
                    }
                    stats.scanned = stats.scanned.saturating_add(1);
                    if let Err(error) = scan.visit(key, &mut stats).await {
                        stats.failed(error);
                    }
                    after = key;
                }
            }
            Err(error) => stats.failed(error),
        }
        updates.send_replace(stats.clone());
        tokio::select! {
            _ = tokio::time::sleep(limits.interval) => {},
            changed = stopping.changed() => {
                if changed.is_err() || *stopping.borrow() { return stats; }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn scan_limits_bound_keys_and_retry_frequency() {
        assert!(RecoveryScanLimits::default().validate().is_ok());
        for page in [0, MAX_PAGE + 1] {
            assert!(
                RecoveryScanLimits {
                    page,
                    ..Default::default()
                }
                .validate()
                .is_err()
            );
        }
        for interval in [
            Duration::ZERO,
            Duration::from_millis(9),
            Duration::from_secs(61),
        ] {
            assert!(
                RecoveryScanLimits {
                    interval,
                    ..Default::default()
                }
                .validate()
                .is_err()
            );
        }
    }
}
