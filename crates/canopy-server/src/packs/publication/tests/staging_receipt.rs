//! Actual original receipt, SQL atomicity, cold owner and custody separation.
use super::publishing::edit;
use super::*;
use cellule_runtime::Resolution;
use tokio::time::{Duration, timeout};

async fn expire(expires_at_ms: i64) -> Result {
    let now = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis())?;
    if now <= expires_at_ms {
        tokio::time::sleep(Duration::from_millis(u64::try_from(
            expires_at_ms - now + 1,
        )?))
        .await;
    }
    Ok(())
}
fn denied_stage(
    result: std::result::Result<
        cellule_runtime::Committed<StagingReply>,
        InvocationError<StagingReply>,
    >,
    reason: PreparationDenial,
) {
    match result {
        Err(InvocationError::Rejected(value)) => {
            assert_eq!(value.output, StagingReply::Denied(reason))
        }
        other => panic!("expected domain denial {reason:?}, observed {other:?}"),
    }
}
#[tokio::test]
async fn initial_staging_receipt_late_sql_failure_rolls_back_namespace_pin_and_sdk_acceptance()
-> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let f = Fixture::new(format).await?;
        let request = f.begin([216; 16]);
        let command = f
            .client()
            .prepare_command::<BeginStaging>(&f.target, identity()?, request.clone())
            .await?;
        let evidence = command.evidence().clone();
        edit(&f, "CREATE TRIGGER initial_receipt_fault BEFORE INSERT ON pushes BEGIN SELECT RAISE(ABORT,'late initial receipt failure'); END").await?;
        assert!(command.clone().execute().await.is_err());
        assert_eq!(f.counts().await?, (0, 0));
        assert!(matches!(
            f.client().resolve(&evidence).await?,
            Resolution::Absent
        ));
        assert!(
            StagingAdmission::load(&f.client(), &f.target, request.operation)
                .await?
                .is_none()
        );
        edit(&f, "DROP TRIGGER initial_receipt_fault").await?;
        let accepted = command.execute().await?;
        let StagingReply::Granted(lease) = accepted.output else {
            return Err("missing admission".into());
        };
        assert_eq!(lease.token.artifact_operation, artifact_number(1));
        let saved = StagingAdmission::load(&f.client(), &f.target, request.operation)
            .await?
            .ok_or("receipt missing")?;
        assert_eq!(saved.receipt(), accepted.receipt);
        assert_eq!(saved.lease(), *lease);
        assert_eq!(f.counts().await?, (1, 1));
        for statement in [
            "UPDATE pushes SET initial_staging=NULL",
            "UPDATE pushes SET initial_staging=x'01'",
            "DELETE FROM pushes",
        ] {
            assert!(edit(&f, statement).await.is_err());
        }
        f.runtime.shutdown().await?;
    }
    Ok(())
}
#[tokio::test]
async fn initial_staging_receipt_cold_restore_requires_actual_claim_and_keeps_original_receipt()
-> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let f = Fixture::new(format).await?;
        let input = f.begin([217; 16]);
        let mut mutation = identity()?;
        mutation.expires_at_ms = mutation.issued_at_ms + 1_000;
        let command = PreparedCustody::prepare(
            &f.client(),
            &f.target,
            CustodyAction::BeginStaging(input.clone()),
            mutation,
        )
        .await?;
        let evidence = command.evidence().clone();
        let original = command
            .register(&f.client(), identity()?)
            .await?
            .recover_staging(&f.client())
            .await?;
        let StagingReply::Granted(old) = original.output else {
            return Err("missing admission".into());
        };
        let (runtime, handle, client) =
            super::durable_recovery::restore_owner(&f, &check(old.token)).await?;
        expire(mutation.expires_at_ms).await?;
        assert!(matches!(
            client.resolve(&evidence).await?,
            Resolution::Expired
        ));
        let saved = StagingAdmission::load(&client, &f.target, input.operation)
            .await?
            .ok_or("cold receipt missing")?;
        assert_eq!(saved.receipt(), original.receipt);
        assert_eq!(saved.lease(), *old);
        assert!(matches!(
            ReadyStaging::new(client.clone(), f.target.clone(), input.clone(), identity()?).await,
            Err(StagingError::Duplicate)
        ));
        denied_stage(
            client
                .command::<RenewStaging>(&f.target, identity()?, request(old.token))
                .await,
            PreparationDenial::Stale,
        );
        let coordinator = StagingCoordinator::new(f.target.clone(), StagingLimits::default())?;
        let ticket = coordinator
            .submit(
                saved
                    .ready_claim(client.clone(), DEFAULT_LEASE_MS, identity()?)
                    .await?,
            )
            .map_err(|(e, _)| e)?;
        let StagingState::Active(next) = timeout(Duration::from_secs(10), ticket.wait()).await?
        else {
            return Err("claim did not acquire custody".into());
        };
        assert_eq!(next.token.owner, handle.owner_fence());
        assert_ne!(next.token.owner, old.token.owner);
        assert_ne!(next.token.artifact_operation, old.token.artifact_operation);
        assert!(next.token.attempt > old.token.attempt);
        assert_eq!(counts(&handle).await?, (1, 2));
        let still_original = StagingAdmission::load(&client, &f.target, input.operation)
            .await?
            .ok_or("claim replaced original receipt")?;
        assert_eq!(still_original.receipt(), original.receipt);
        assert_eq!(still_original.lease(), *old);
        assert!(coordinator.close_and_drain().await.is_empty());
        runtime.shutdown().await?;
    }
    Ok(())
}
#[tokio::test]
async fn initial_staging_receipt_survives_reaping_but_does_not_restore_expired_custody() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let f = Fixture::new(format).await?;
        let mut input = f.begin([218; 16]);
        input.lease_ms = 1_000;
        let original = PreparedCustody::prepare(
            &f.client(),
            &f.target,
            CustodyAction::BeginStaging(input.clone()),
            identity()?,
        )
        .await?
        .register(&f.client(), identity()?)
        .await?
        .recover_staging(&f.client())
        .await?;
        let StagingReply::Granted(lease) = original.output else {
            return Err("missing admission".into());
        };
        expire(lease.expires_at_ms).await?;
        f.client()
            .command::<ReapPreparation>(
                &f.target,
                identity()?,
                MaintenanceRequest {
                    repository: f.repository,
                    actor: "owner".into(),
                    owner: f.handle.owner_fence(),
                },
            )
            .await?;
        assert_eq!(f.counts().await?, (0, 0));
        let saved = StagingAdmission::load(&f.client(), &f.target, input.operation)
            .await?
            .ok_or("reaped receipt missing")?;
        assert_eq!(saved.receipt(), original.receipt);
        assert_eq!(saved.lease(), *lease);
        let coordinator = StagingCoordinator::new(f.target.clone(), StagingLimits::default())?;
        let ticket = coordinator
            .submit(
                saved
                    .ready_claim(f.client(), DEFAULT_LEASE_MS, identity()?)
                    .await?,
            )
            .map_err(|(e, _)| e)?;
        let StagingState::Active(next) = timeout(Duration::from_secs(10), ticket.wait()).await?
        else {
            return Err("explicit restart Claim failed".into());
        };
        assert_ne!(
            next.token.artifact_operation,
            lease.token.artifact_operation
        );
        assert!(next.token.attempt > lease.token.attempt);
        assert_eq!(next.token.owner, f.handle.owner_fence());
        assert!(
            f.client()
                .query::<CheckStaging>(&f.target, None, check(lease.token))
                .await?
                .output
                .is_none()
        );
        assert_eq!(f.counts().await?, (1, 1));
        let original_still_known = StagingAdmission::load(&f.client(), &f.target, input.operation)
            .await?
            .ok_or("restart overwrote original receipt")?;
        assert_eq!(original_still_known.receipt(), original.receipt);
        assert_eq!(original_still_known.lease(), *lease);
        assert!(coordinator.close_and_drain().await.is_empty());
        assert_eq!(f.counts().await?, (1, 1));
        f.runtime.shutdown().await?;
    }
    Ok(())
}
#[tokio::test]
async fn initial_staging_receipt_is_internal_knowledge_after_write_revocation() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let f = Fixture::new(format).await?;
        let input = f.begin([215; 16]);
        let coordinator = StagingCoordinator::new(f.target.clone(), StagingLimits::default())?;
        coordinator.fault_for_test(2);
        let ticket = coordinator
            .submit(
                ReadyStaging::new(f.client(), f.target.clone(), input.clone(), identity()?).await?,
            )
            .map_err(|(e, _)| e)?;
        assert!(matches!(
            timeout(Duration::from_secs(10), ticket.wait_terminal()).await?,
            StagingState::Uncertain(_)
        ));
        let original = StagingAdmission::load(&f.client(), &f.target, input.operation)
            .await?
            .ok_or("receipt missing")?;
        edit(
            &f,
            "UPDATE repository_identity SET owner='revoked' WHERE singleton=1",
        )
        .await?;
        coordinator.recover(&ticket)?;
        assert!(matches!(
            timeout(Duration::from_secs(10), ticket.wait_terminal()).await?,
            StagingState::Fenced(_)
        ));
        assert!(matches!(
            ticket.spawn(|_| async { Ok(()) }),
            Err(StagingError::Inactive)
        ));
        assert!(coordinator.close_and_drain().await.is_empty());
        let known = StagingAdmission::load(&f.client(), &f.target, input.operation)
            .await?
            .ok_or("revocation hid original knowledge")?;
        assert_eq!(known.receipt(), original.receipt());
        assert_eq!(known.lease(), original.lease());
        f.runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn staging_custody_intent_corruption_keeps_original_evidence_and_reservation() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let f = Fixture::new(format).await?;
        let input = f.begin([214; 16]);
        let coordinator = StagingCoordinator::new(f.target.clone(), StagingLimits::default())?;
        coordinator.fault_for_test(2);
        let ticket = coordinator
            .submit(
                ReadyStaging::new(f.client(), f.target.clone(), input.clone(), identity()?).await?,
            )
            .map_err(|(e, _)| e)?;
        let StagingState::Uncertain(error) =
            timeout(Duration::from_secs(10), ticket.wait_terminal()).await?
        else {
            return Err("lost receipt not retained".into());
        };
        let StagingError::Begin(error) = error.as_ref() else {
            return Err("not original Begin".into());
        };
        let InvocationError::Pending(evidence) = error.as_ref() else {
            return Err("missing evidence".into());
        };
        let evidence = (**evidence).clone();
        let saved = RegisteredCustody::load_latest(&f.client(), &f.target, input.operation)
            .await?
            .ok_or("intent missing")?;
        assert_eq!(saved.evidence(), &evidence);
        let original = saved.recover_staging(&f.client()).await?;
        let body = f
            .handle
            .query(0, 4096, |db| {
                Ok(db.query_row("SELECT intent FROM catalog_custody_commands WHERE operation = x'd6d6d6d6d6d6d6d6d6d6d6d6d6d6d6d6'", [], |r| {
                    r.get::<_, Vec<u8>>(0)
                })?)
            })
            .await?;
        let mut corrupt = body.clone();
        let end = corrupt.last_mut().ok_or("empty receipt")?;
        *end ^= 1;
        edit(&f, "DROP TRIGGER catalog_custody_identity_immutable").await?;
        edit(
            &f,
            &format!(
                "UPDATE catalog_custody_commands SET intent=x'{}' WHERE step=0",
                hex::encode(corrupt)
            ),
        )
        .await?;
        coordinator.recover(&ticket)?;
        let StagingState::Uncertain(error) =
            timeout(Duration::from_secs(10), ticket.wait_terminal()).await?
        else {
            return Err("corruption lost uncertainty".into());
        };
        let StagingError::Custody {
            evidence: retained, ..
        } = error.as_ref()
        else {
            return Err("missing receipt recovery error".into());
        };
        assert_eq!(retained.as_ref(), &evidence);
        assert_eq!(
            coordinator.stats().command_bytes,
            super::super::custody::RESERVATION
        );
        assert!(matches!(
            ticket.spawn(|_| async { Ok(()) }),
            Err(StagingError::Inactive)
        ));
        assert_eq!(f.counts().await?, (1, 1));
        edit(
            &f,
            &format!(
                "UPDATE catalog_custody_commands SET intent=x'{}' WHERE step=0",
                hex::encode(body)
            ),
        )
        .await?;
        coordinator.recover(&ticket)?;
        let StagingState::Active(active) = timeout(Duration::from_secs(10), ticket.wait()).await?
        else {
            return Err("restored original did not recover".into());
        };
        let StagingReply::Granted(original_lease) = original.output else {
            return Err("original was not granted".into());
        };
        assert_eq!(active.token, original_lease.token);
        let restored = StagingAdmission::load(&f.client(), &f.target, [214; 16])
            .await?
            .ok_or("restored receipt missing")?;
        assert_eq!(restored.receipt(), original.receipt);
        assert!(coordinator.close_and_drain().await.is_empty());
        f.runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn initial_staging_receipt_restart_claim_refuses_forgery_and_rolls_back_late_writes() -> Result
{
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let f = Fixture::new(format).await?;
        let input = f.begin([213; 16]);
        let admitted = f
            .client()
            .command::<BeginStaging>(&f.target, identity()?, input.clone())
            .await?;
        let StagingReply::Granted(old) = admitted.output else {
            return Err("missing admission".into());
        };
        f.client()
            .command::<AbortPreparation>(&f.target, identity()?, check(old.token))
            .await?;
        assert_eq!(f.counts().await?, (0, 1));
        let mut malformed = request(old.token);
        malformed.check.token.artifact_operation = [71; 16];
        assert!(matches!(
            f.client()
                .command::<ClaimStaging>(&f.target, identity()?, malformed)
                .await,
            Err(InvocationError::NotStarted(_))
        ));
        assert_eq!(f.counts().await?, (0, 1));
        for variant in 0..4 {
            let mut forged = request(old.token);
            match variant {
                0 => forged.check.token.owner.epoch += 1,
                1 => forged.check.token.attempt += 1,
                2 => forged.check.token.artifact_operation = artifact_number(71),
                _ => forged.check.token.request_digest = [72; 32],
            }
            denied_stage(
                f.client()
                    .command::<ClaimStaging>(&f.target, identity()?, forged)
                    .await,
                PreparationDenial::Missing,
            );
            assert_eq!(f.counts().await?, (0, 1));
        }
        let command = f
            .client()
            .prepare_command::<ClaimStaging>(&f.target, identity()?, request(old.token))
            .await?;
        let evidence = command.evidence().clone();
        edit(&f, "CREATE TRIGGER restart_claim_fault BEFORE INSERT ON catalog_operations BEGIN SELECT RAISE(ABORT,'late restart Claim failure'); END").await?;
        assert!(command.clone().execute().await.is_err());
        assert!(matches!(
            f.client().resolve(&evidence).await?,
            Resolution::Absent
        ));
        assert_eq!(f.counts().await?, (0, 1));
        edit(&f, "DROP TRIGGER restart_claim_fault").await?;
        let claimed = command.execute().await?;
        let StagingReply::Granted(next) = claimed.output else {
            return Err("exact Claim retry failed".into());
        };
        assert_eq!(next.token.artifact_operation, artifact_number(2));
        assert_eq!(next.token.owner, f.handle.owner_fence());
        assert_eq!(f.counts().await?, (1, 2));
        let known = StagingAdmission::load(&f.client(), &f.target, input.operation)
            .await?
            .ok_or("Claim lost initial receipt")?;
        assert_eq!(known.receipt(), admitted.receipt);
        assert_eq!(known.lease(), *old);
        f.client()
            .command::<AbortPreparation>(&f.target, identity()?, check(next.token))
            .await?;
        edit(
            &f,
            "UPDATE pushes SET response_id=zeroblob(16),completion_digest=zeroblob(32),rejected=1",
        )
        .await?;
        denied_stage(
            f.client()
                .command::<ClaimStaging>(&f.target, identity()?, request(old.token))
                .await,
            PreparationDenial::Conflict,
        );
        assert_eq!(f.counts().await?, (0, 2));
        f.runtime.shutdown().await?;
    }
    Ok(())
}
