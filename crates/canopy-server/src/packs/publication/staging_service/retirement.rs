//! One read-only sweeper per service, over existing bounded admitted jobs.
use super::*;
use std::collections::BinaryHeap;

pub(super) fn start(inner: Arc<Inner>) {
    let mut admission = inner.admission.lock().expect("staging admission");
    if admission.retirement_probe {
        return;
    }
    admission.retirement_probe = true;
    drop(admission);
    tokio::spawn(async move {
        loop {
            if tokio::spawn(run(Arc::clone(&inner))).await.is_ok() {
                return;
            }
            {
                let mut admission = inner.admission.lock().expect("staging admission");
                admission.retirement_restarts = admission.retirement_restarts.saturating_add(1);
            }
            tracing::error!("staging retirement probe failed; retaining exact jobs and restarting");
            tokio::time::sleep(RecoveryScanLimits::default().interval).await;
        }
    });
}
fn eligible(job: &Job) -> bool {
    matches!(*job.status.borrow(), StagingState::Uncertain(_))
        && job
            .exact
            .lock()
            .expect("staging exact")
            .as_ref()
            .is_some_and(|exact| exact.custody_original().is_some())
}
fn page(inner: &Inner, after: &mut Option<[u8; 16]>, limit: usize) -> Option<Vec<Arc<Job>>> {
    let mut admission = inner.admission.lock().expect("staging admission");
    let mut keys = BinaryHeap::with_capacity(limit + 1);
    for pass in 0..2 {
        for (operation, job) in &admission.jobs {
            if after.is_none_or(|cursor| *operation > cursor) && eligible(job) {
                keys.push(*operation);
                if keys.len() > limit {
                    keys.pop();
                }
            }
        }
        if !keys.is_empty() {
            break;
        }
        if pass == 0 {
            *after = None;
        }
    }
    if keys.is_empty() {
        // The same lock covers start/exit, so a newly uncertain job cannot lose
        // its wakeup between observing an empty page and relinquishing ownership.
        admission.retirement_probe = false;
        return None;
    }
    Some(
        keys.into_sorted_vec()
            .into_iter()
            .map(|key| Arc::clone(&admission.jobs[&key]))
            .collect(),
    )
}
async fn visit(inner: &Arc<Inner>, job: &Arc<Job>) {
    let probe = {
        let exact = job.exact.lock().expect("staging exact");
        exact
            .as_ref()
            .and_then(Exact::custody_original)
            .map(OwnedCustody::stop_probe)
    };
    let Some(probe) = probe else {
        return;
    };
    let probe = match probe {
        Ok(probe) => probe,
        Err(error) => {
            failed(inner, &error);
            return;
        }
    };
    // No ready command/body is cloned across this await. This independent read
    // uses SDK query admission, and never performs registration or execution.
    let stopped = probe.observed(&job.client).await;
    {
        let mut admission = inner.admission.lock().expect("staging admission");
        admission.retirement_probes = admission.retirement_probes.saturating_add(1);
    }
    match stopped {
        Ok(true) => {}
        Ok(false) => return,
        Err(error) => {
            failed(inner, &error);
            return;
        }
    }
    let same_original = job
        .exact
        .lock()
        .expect("staging exact")
        .as_ref()
        .and_then(Exact::custody_original)
        .is_some_and(|current| current.evidence() == probe.evidence());
    if !same_original {
        return;
    }
    let ticket = StagingTicket {
        inner: Arc::clone(inner),
        job: Arc::clone(job),
    };
    // Exact recovery reauthenticates closure, fences the shared session and
    // drains resources before returning admission. Closure never becomes a reply.
    if (StagingCoordinator {
        inner: Arc::clone(inner),
    })
    .recover(&ticket)
    .is_ok()
    {
        let mut admission = inner.admission.lock().expect("staging admission");
        admission.retirement_recoveries = admission.retirement_recoveries.saturating_add(1);
    }
}
fn failed(inner: &Inner, error: &CustodyError) {
    let mut admission = inner.admission.lock().expect("staging admission");
    admission.retirement_failures = admission.retirement_failures.saturating_add(1);
    tracing::debug!(%error, "staging retirement probe unavailable; retaining original");
}
async fn run(inner: Arc<Inner>) {
    let limits = RecoveryScanLimits::default();
    let mut after = None;
    loop {
        let Some(jobs) = page(&inner, &mut after, usize::from(limits.page)) else {
            return;
        };
        let deadline = Instant::now() + limits.interval;
        for job in jobs {
            after = Some(job.operation);
            visit(&inner, &job).await;
            // A slow failed head advances the cursor without consuming the rest
            // of this round. Keep the read owner until its query finishes.
            if Instant::now() >= deadline {
                break;
            }
        }
        tokio::time::sleep(limits.interval).await;
    }
}
