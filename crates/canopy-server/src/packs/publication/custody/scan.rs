//! Bounded keyset discovery; existing maintenance admission owns exact stops.
use super::*;
use std::sync::Arc;
use tokio::{sync::watch, task::JoinHandle};

const SEEK: &str = "SELECT operation FROM catalog_custody_commands INDEXED BY catalog_custody_pending WHERE phase IS NULL AND stopped IS NULL AND operation>?1 ORDER BY operation LIMIT ?2";
#[derive(Clone, Debug, Default)]
pub struct CustodyScanStats {
    pub passes: u64,
    pub scanned: u64,
    pub submitted: u64,
    pub recovered: u64,
    pub deferred: u64,
    pub failures: u64,
    pub last_error: Option<Arc<CustodyError>>,
}
impl CustodyScanStats {
    fn failed(&mut self, error: CustodyError) {
        self.failures = self.failures.saturating_add(1);
        self.last_error = Some(Arc::new(error));
    }
}
#[must_use]
pub struct CustodySupervisor {
    stop: watch::Sender<bool>,
    stats: watch::Receiver<CustodyScanStats>,
    task: Option<JoinHandle<CustodyScanStats>>,
}
impl CustodySupervisor {
    pub fn start(
        client: CellClient,
        target: CellTarget,
        coordinator: PublicationCoordinator,
        limits: RecoveryScanLimits,
        authority: PreparationAuthority,
    ) -> Result<Self, CustodyError> {
        if limits.validate().is_err() {
            return Err(CustodyError::InvalidScanLimits);
        }
        if !authority.matches(&target) || coordinator.target() != &target {
            return Err(CustodyError::Context);
        }
        let sql = SqlCell::<RepositoryModule>::new(client.clone(), target.clone())?;
        let (stop, stopping) = watch::channel(false);
        let (updates, stats) = watch::channel(CustodyScanStats::default());
        let scan = Scan {
            client,
            target,
            coordinator,
            authority,
        };
        let task = tokio::spawn(run(scan, sql, limits, stopping, updates));
        Ok(Self {
            stop,
            stats,
            task: Some(task),
        })
    }
    pub fn stats(&self) -> CustodyScanStats {
        self.stats.borrow().clone()
    }
    pub async fn shutdown(mut self) -> Result<CustodyScanStats, tokio::task::JoinError> {
        self.stop.send_replace(true);
        self.task.take().expect("custody scan owner").await
    }
}
impl Drop for CustodySupervisor {
    fn drop(&mut self) {
        self.stop.send_replace(true);
    }
}
struct Scan {
    client: CellClient,
    target: CellTarget,
    coordinator: PublicationCoordinator,
    authority: PreparationAuthority,
}
impl Scan {
    async fn visit(
        &self,
        operation: [u8; 16],
        stats: &mut CustodyScanStats,
    ) -> Result<(), CustodyError> {
        // The coordinator owns accepted uncertainty even if its SQL key vanished.
        // Do not create a second retirement identity for an admitted operation.
        if self
            .coordinator
            .pending_custody_stop(operation)
            .await
            .is_some()
        {
            stats.deferred = stats.deferred.saturating_add(1);
            return Ok(());
        }
        let Some(saved) = load(&self.client, &self.target, operation, None).await? else {
            return Ok(());
        };
        if saved.closed() || saved.evidence().identity().expires_at_ms > now(0)? {
            stats.deferred = stats.deferred.saturating_add(1);
            return Ok(());
        }
        let identity = crate::server::mutation_identity()
            .map_err(|source| CustodyError::Clock(Box::new(source)))?;
        let ready = saved
            .ready_stop(self.client.clone(), identity, &self.authority)
            .await?;
        match self.coordinator.submit(ready).await {
            Ok(_) => stats.submitted = stats.submitted.saturating_add(1),
            Err(failure) => match failure.reason {
                PublicationScheduleError::Capacity
                | PublicationScheduleError::Duplicate
                | PublicationScheduleError::Closed => {
                    stats.deferred = stats.deferred.saturating_add(1);
                }
                _ => return Err(CustodyError::Context),
            },
        }
        Ok(())
    }
}
async fn page(
    sql: &SqlCell<RepositoryModule>,
    after: [u8; 16],
    count: u16,
) -> Result<Vec<[u8; 16]>, CustodyError> {
    let result = sql
        .query(
            None,
            statement(SEEK, vec![blob(after), number(u64::from(count))?]),
        )
        .await
        .map_err(|error| CustodyError::Query(Box::new(error)))?;
    let rows = rows(&result.output)?;
    if rows.len() > count as usize {
        return Err(CustodyError::Context);
    }
    let mut keys = Vec::with_capacity(rows.len());
    let mut previous = after;
    for row in rows {
        let [key] = row.as_slice() else {
            return Err(CustodyError::Context);
        };
        let key = fixed::<16>(key)?;
        if key <= previous {
            return Err(CustodyError::Context);
        }
        keys.push(key);
        previous = key;
    }
    Ok(keys)
}
async fn run(
    scan: Scan,
    sql: SqlCell<RepositoryModule>,
    limits: RecoveryScanLimits,
    mut stopping: watch::Receiver<bool>,
    updates: watch::Sender<CustodyScanStats>,
) -> CustodyScanStats {
    let mut stats = CustodyScanStats::default();
    let mut after = [0; 16];
    loop {
        if *stopping.borrow() {
            return stats;
        }
        match scan.coordinator.recover_custody_stops().await {
            Ok(recovered) => stats.recovered = stats.recovered.saturating_add(recovered),
            Err(_) => stats.failed(CustodyError::Context),
        }
        match page(&sql, after, limits.page).await {
            Ok(keys) => {
                if keys.is_empty() {
                    after = [0; 16];
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
                    // Advance even for corrupt heads; revisit on the next pass.
                    after = key;
                }
            }
            Err(error) => stats.failed(error),
        }
        updates.send_replace(stats.clone());
        tokio::select! {
            _ = tokio::time::sleep(limits.interval) => {},
            changed = stopping.changed() => { if changed.is_err() || *stopping.borrow() { return stats; } }
        }
    }
}
