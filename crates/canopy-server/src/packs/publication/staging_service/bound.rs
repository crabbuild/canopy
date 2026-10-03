//! Bound handoff reuses the parent lifecycle job, admission and worker slots.
use super::*;

pub(super) async fn accept_bound(
    inner: &Inner,
    job: &Job,
    value: Committed<PreparationReply>,
    claimed: bool,
) -> bool {
    let PreparationReply::Granted(lease) = value.output else {
        fence_and_drain(inner, job, StagingError::Context).await;
        return false;
    };
    let original = Arc::new(StagingBound {
        lease: *lease,
        receipt: value.receipt,
    });
    let (matched, ceiling) = {
        let mut local = job.local.lock().expect("staging local");
        // A known result is retained before fresh probes, even after revocation.
        local.bound_result = Some(original.clone());
        let matched = if claimed {
            local.bound_source.as_ref().is_some_and(|source| {
                source.token.repository == lease.token.repository
                    && source.token.operation == lease.token.operation
                    && source.token.request_digest == lease.token.request_digest
                    && source.token != lease.token
                    && source.actor == job.actor
            })
        } else {
            local.lease.is_some_and(|staged| {
                staged.token == lease.token
                    && staged.format == lease.format
                    && lease.expires_at_ms >= staged.expires_at_ms
            })
        };
        let started = local.bound_started.expect("bound command admission time");
        let ceiling =
            (started + Duration::from_millis(inner.limits.bound_lifetime_ms)).min(local.lifetime);
        local.lifetime = ceiling;
        (
            matched && !local.fenced && Instant::now() < ceiling,
            ceiling,
        )
    };
    if !matched {
        fence_and_drain(inner, job, StagingError::Context).await;
        return false;
    }
    let session = PreparationSession::open(
        job.client.clone(),
        job.target.clone(),
        LeaseCheck {
            token: lease.token,
            actor: job.actor.clone(),
        },
        Some(value.receipt),
    )
    .await;
    let mut session = match session {
        Ok(session) => session,
        Err(error) => {
            fence_and_drain(inner, job, StagingError::Base(error)).await;
            return false;
        }
    };
    if session.lease.base != lease.base || session.lease.format != lease.format {
        fence_and_drain(inner, job, StagingError::Context).await;
        return false;
    }
    session.ceiling = Some(ceiling);
    let _ = match session.live_lease() {
        Ok((_, deadline)) => deadline,
        Err(error) => {
            fence_and_drain(inner, job, StagingError::Base(error)).await;
            return false;
        }
    };
    let deadline = *session.deadline.lock().expect("bound deadline");
    let mut local = job.local.lock().expect("staging local");
    local.bound = Some(Arc::new(session));
    local.deadline = deadline;
    job.status.send_replace(StagingState::Bound(original));
    true
}
