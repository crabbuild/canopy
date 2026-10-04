//! Reconstruct only an authenticated registered original, never a new retry.
use super::*;

impl ReadyStaging {
    /// Load the exact latest registered custody head after process loss.
    /// No recorded clock grants work; the service resolves original knowledge
    /// before independently acquiring current lease/owner custody.
    pub async fn restore(
        client: CellClient,
        target: CellTarget,
        operation: [u8; 16],
    ) -> Result<Self, StagingError> {
        let command = OwnedCustody::restore(&client, &target, operation).await?;
        let request = match command.action().map_err(|_| StagingError::Context)? {
            CustodyAction::BeginPreparation(request) | CustodyAction::BeginStaging(request) => {
                request
            }
            CustodyAction::ClaimPreparation(request)
            | CustodyAction::RenewPreparation(request)
            | CustodyAction::ClaimStaging(request)
            | CustodyAction::RenewStaging(request) => context(request.check, request.lease_ms),
            // Bind has no requested renewal duration. This synthetic value is
            // admission metadata only, never execution bytes or a lease clock.
            CustodyAction::BindStaging(check) => context(check, DEFAULT_LEASE_MS),
        };
        if request.operation != operation
            || crate::repository_target(target.tenant(), target.application(), request.repository)
                .map_err(|_| StagingError::Context)?
                != target
        {
            return Err(StagingError::Context);
        }
        request
            .encode(&mut BoundedEncoder::new(COMMAND_BYTES).map_err(|_| StagingError::Context)?)
            .map_err(|_| StagingError::Context)?;
        Ok(Self {
            inner: Box::new(StagingRequest {
                client,
                target,
                request,
                command: Exact::Restored(command),
                bound_source: None,
            }),
        })
    }
}
fn context(check: LeaseCheck, lease_ms: u64) -> BeginRequest {
    BeginRequest {
        repository: check.token.repository,
        operation: check.token.operation,
        request_digest: check.token.request_digest,
        actor: check.actor,
        lease_ms,
    }
}

pub(super) async fn dispatch(
    command: OwnedCustody,
    client: &CellClient,
    job: &Job,
    recover: bool,
    fault: u8,
) -> Result<Outcome, StagingError> {
    // Only frozen command execution occurs here, not new native work. Known
    // phases precede the local guard. On proven SDK absence the original
    // receiver checks live custody and actual ownership atomically; successful
    // execution still cannot grant a worker before the fresh post-result probe.
    let result = command
        .invoke(client, recover, fault, || custody_guard(job))
        .await
        .map_err(|source| StagingError::Custody {
            evidence: Box::new(command.evidence().clone()),
            source: Box::new(source),
        })?;
    let value = match result {
        Ok(value) => value,
        Err(InvocationError::Rejected(value)) => *value,
        Err(error) => return Err(StagingError::Restoration(Box::new(error))),
    };
    Ok(Outcome::Restored(Box::new(value)))
}

pub(super) async fn accept(inner: &Inner, job: &Job, value: Committed<CustodyReply>) -> bool {
    // Preserve positive AND negative knowledge before any current authority,
    // lease query, scope ceiling or newly configured duration can refuse work.
    let value = Arc::new(value);
    job.local.lock().expect("staging local").restored_outcome = Some(value.clone());
    match &value.output {
        CustodyReply::Staging(StagingReply::Granted(recorded)) => {
            job.local.lock().expect("staging local").lease = Some(**recorded);
            match probe(job, value.receipt).await {
                Ok((lease, deadline))
                    if lease.token == recorded.token && lease.format == recorded.format =>
                {
                    let active = {
                        let mut local = job.local.lock().expect("staging local");
                        if local.fenced || Instant::now() >= deadline.min(local.lifetime) {
                            false
                        } else {
                            local.lease = Some(lease);
                            local.deadline = deadline;
                            job.status.send_replace(if local.stop {
                                StagingState::Draining(lease)
                            } else {
                                StagingState::Active(lease)
                            });
                            true
                        }
                    };
                    if !active {
                        fence_and_drain(inner, job, StagingError::Inactive).await;
                    }
                    active
                }
                Ok(_) => {
                    fence_and_drain(inner, job, StagingError::Context).await;
                    false
                }
                Err(error) => {
                    fence_and_drain(inner, job, error).await;
                    false
                }
            }
        }
        CustodyReply::Preparation(PreparationReply::Granted(lease)) => {
            let original = Arc::new(StagingBound {
                lease: **lease,
                receipt: value.receipt,
            });
            let ceiling = {
                let mut local = job.local.lock().expect("staging local");
                local.bound_result = Some(original.clone());
                let started = local
                    .bound_started
                    .expect("restored custody admission time");
                let ceiling = (started + Duration::from_millis(inner.limits.bound_lifetime_ms))
                    .min(local.lifetime);
                local.lifetime = ceiling;
                (!local.fenced && Instant::now() < ceiling).then_some(ceiling)
            };
            match ceiling {
                Some(ceiling) => bound::open_bound(inner, job, original, ceiling).await,
                None => {
                    fence_and_drain(inner, job, StagingError::Inactive).await;
                    false
                }
            }
        }
        CustodyReply::Staging(StagingReply::Denied(_))
        | CustodyReply::Preparation(PreparationReply::Denied(_)) => {
            fence_and_drain(inner, job, StagingError::Context).await;
            false
        }
    }
}
