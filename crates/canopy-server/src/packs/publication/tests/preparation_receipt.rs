//! First preparation admission survives transport loss independently of custody.
use super::publishing::edit;
use super::*;
use cellule_runtime::Resolution;
use tokio::time::{Duration, timeout};

fn denied(
    result: std::result::Result<
        cellule_runtime::Committed<PreparationReply>,
        InvocationError<PreparationReply>,
    >,
    reason: PreparationDenial,
) {
    match result {
        Err(InvocationError::Rejected(value)) => {
            assert_eq!(value.output, PreparationReply::Denied(reason))
        }
        other => panic!("expected preparation denial {reason:?}, observed {other:?}"),
    }
}

async fn expire(expires_at_ms: i64) -> Result {
    let now = sql::now(0)?;
    if now <= expires_at_ms {
        tokio::time::sleep(Duration::from_millis(u64::try_from(
            expires_at_ms - now + 1,
        )?))
        .await;
    }
    Ok(())
}

#[tokio::test]
async fn initial_preparation_receipt_is_saved_with_the_actual_admission() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let f = Fixture::new(format).await?;
        let original = f
            .client()
            .command::<BeginPreparation>(&f.target, identity()?, f.begin([218; 16]))
            .await?;
        let accepted = lease(original.output)?;
        let operation = accepted.token.operation;
        let saved = f
            .handle
            .query(0, 2048, move |db| {
                Ok(db.query_row(
                    "SELECT initial_preparation FROM pushes WHERE id=?1",
                    [operation.as_slice()],
                    |row| row.get::<_, Vec<u8>>(0),
                )?)
            })
            .await?;
        assert!(!saved.is_empty());
        assert!(saved.len() <= CERTIFICATE_BYTES as usize);
        assert_eq!(accepted.token.attempt, original.receipt.commit_sequence);
        assert_eq!(f.counts().await?, (1, 1));
        let known = PreparationAdmission::load(&f.client(), &f.target, operation)
            .await?
            .ok_or("initial receipt absent")?;
        assert_eq!(known.lease(), accepted);
        assert_eq!(known.receipt(), original.receipt);
        assert_eq!(known.request(), &f.begin(operation));
        f.runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn initial_preparation_receipt_late_write_rolls_back_and_first_result_is_immutable() -> Result
{
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        for bound in [false, true] {
            let f = Fixture::new(format).await?;
            let input = f.begin([219; 16]);
            let prior = if bound {
                let staged = f
                    .client()
                    .command::<BeginStaging>(&f.target, identity()?, input.clone())
                    .await?;
                let StagingReply::Granted(staged) = staged.output else {
                    return Err("staging grant absent".into());
                };
                f.client()
                    .command::<BindStaging>(&f.target, identity()?, check(staged.token))
                    .await?;
                Some(staged.token)
            } else {
                None
            };
            let command = f
                .client()
                .prepare_command::<BeginPreparation>(&f.target, identity()?, input.clone())
                .await?;
            let evidence = command.evidence().clone();
            let event = if bound {
                "UPDATE OF initial_preparation"
            } else {
                "INSERT"
            };
            edit(&f, &format!("CREATE TRIGGER initial_preparation_fault BEFORE {event} ON pushes BEGIN SELECT RAISE(ABORT,'late initial preparation receipt failure'); END")).await?;
            assert!(command.clone().execute().await.is_err());
            assert!(matches!(
                f.client().resolve(&evidence).await?,
                Resolution::Absent
            ));
            assert_eq!(f.counts().await?, if bound { (1, 1) } else { (0, 0) });
            assert!(
                PreparationAdmission::load(&f.client(), &f.target, input.operation)
                    .await?
                    .is_none()
            );
            edit(&f, "DROP TRIGGER initial_preparation_fault").await?;
            let original = command.execute().await?;
            let accepted = lease(original.output.clone())?;
            assert_eq!(accepted.token.artifact_operation, artifact_number(1));
            if let Some(prior) = prior {
                assert_eq!(accepted.token, prior);
                assert!(original.receipt.commit_sequence > prior.attempt);
                assert_eq!(
                    StagingAdmission::load(&f.client(), &f.target, input.operation)
                        .await?
                        .ok_or("staging knowledge lost")?
                        .lease()
                        .token,
                    prior
                );
            }
            let saved = PreparationAdmission::load(&f.client(), &f.target, input.operation)
                .await?
                .ok_or("initial receipt absent")?;
            assert_eq!(
                saved.original(&evidence)?.ok_or("original stamp absent")?,
                original
            );
            let observer = f
                .client()
                .prepare_command::<BeginPreparation>(&f.target, identity()?, input.clone())
                .await?;
            assert!(saved.original(observer.evidence())?.is_none());
            let later = observer.execute().await?;
            assert_eq!(lease(later.output)?.token, accepted.token);
            assert!(later.receipt.commit_sequence > original.receipt.commit_sequence);
            assert_eq!(
                PreparationAdmission::load(&f.client(), &f.target, input.operation)
                    .await?
                    .ok_or("original replaced")?
                    .receipt(),
                original.receipt
            );
            for statement in [
                "UPDATE pushes SET initial_preparation=NULL",
                "UPDATE pushes SET initial_preparation=x'01'",
                "DELETE FROM pushes",
                "INSERT OR REPLACE INTO pushes(id,actor,request_digest) SELECT id,actor,request_digest FROM pushes",
            ] {
                assert!(edit(&f, statement).await.is_err());
            }
            f.runtime.shutdown().await?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn initial_preparation_receipt_cold_owner_and_sdk_expiry_preserve_actual_receipt() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let f = Fixture::new(format).await?;
        let input = f.begin([220; 16]);
        let mut mutation = identity()?;
        mutation.expires_at_ms = mutation.issued_at_ms + 1000;
        let command = PreparedCustody::prepare(
            &f.client(),
            &f.target,
            CustodyAction::BeginPreparation(input.clone()),
            mutation,
        )
        .await?;
        let evidence = command.evidence().clone();
        // Discard every factory after acceptance; cold Claim starts from the
        // mandatory registered predecessor, never a raw command adapter.
        let original = command
            .register(&f.client(), identity()?)
            .await?
            .recover_preparation(&f.client())
            .await?;
        let old = lease(original.output.clone())?;
        let (runtime, handle, client) =
            super::durable_recovery::restore_owner(&f, &check(old.token)).await?;
        expire(mutation.expires_at_ms).await?;
        assert!(matches!(
            client.resolve(&evidence).await?,
            Resolution::Expired
        ));
        let known = PreparationAdmission::load(&client, &f.target, input.operation)
            .await?
            .ok_or("cold receipt absent")?;
        assert_eq!(
            known.original(&evidence)?.ok_or("cold original absent")?,
            original
        );
        assert_eq!(known.lease(), old);
        denied(
            client
                .command::<RenewPreparation>(&f.target, identity()?, request(old.token))
                .await,
            PreparationDenial::Stale,
        );
        let coordinator =
            PublicationCoordinator::new(f.target.clone(), PublicationLimits::default())?;
        let ticket = coordinator
            .submit(
                known
                    .ready_claim(client.clone(), DEFAULT_LEASE_MS, identity()?)
                    .await?,
            )
            .await?;
        let PublicationState::Finished(Ok(PublicationOutcome::Preparation(next))) =
            timeout(Duration::from_secs(10), ticket.wait()).await?
        else {
            return Err("cold Claim did not finish".into());
        };
        let next = next.session.map_err(|error| error.to_string())?;
        assert_eq!(next.lease.token.owner, handle.owner_fence());
        assert_ne!(next.lease.token.owner, old.token.owner);
        assert_ne!(
            next.lease.token.artifact_operation,
            old.token.artifact_operation
        );
        assert_eq!(counts(&handle).await?, (1, 2));
        let preserved = PreparationAdmission::load(&client, &f.target, input.operation)
            .await?
            .ok_or("Claim lost initial receipt")?;
        assert_eq!(
            preserved
                .original(&evidence)?
                .ok_or("initial identity lost")?,
            original
        );
        assert!(coordinator.close_and_drain().await.is_empty());
        runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn initial_preparation_receipt_reaped_restart_claim_checks_original_and_rolls_back() -> Result
{
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let f = Fixture::new(format).await?;
        let input = f.begin([221; 16]);
        let original = f
            .client()
            .command::<BeginPreparation>(&f.target, identity()?, input.clone())
            .await?;
        let old = lease(original.output.clone())?;
        edit(&f, "UPDATE catalog_operations SET expires_at_ms=0; UPDATE catalog_leases SET expires_at_ms=0").await?;
        assert_eq!(
            f.client()
                .command::<ReapPreparation>(
                    &f.target,
                    identity()?,
                    MaintenanceRequest {
                        repository: f.repository,
                        actor: "owner".into(),
                        owner: f.handle.owner_fence()
                    }
                )
                .await?
                .output,
            2
        );
        assert_eq!(f.counts().await?, (0, 0));
        let known = PreparationAdmission::load(&f.client(), &f.target, input.operation)
            .await?
            .ok_or("reaping lost receipt")?;
        assert_eq!(known.receipt(), original.receipt);
        assert!(
            PreparationSession::open(
                f.client(),
                f.target.clone(),
                check(old.token),
                Some(original.receipt)
            )
            .await
            .is_err()
        );
        for variant in 0..4 {
            let mut forged = request(old.token);
            match variant {
                0 => forged.check.token.owner.epoch += 1,
                1 => forged.check.token.attempt += 1,
                2 => forged.check.token.artifact_operation = artifact_number(71),
                _ => forged.check.token.request_digest = [72; 32],
            }
            denied(
                f.client()
                    .command::<ClaimPreparation>(&f.target, identity()?, forged)
                    .await,
                PreparationDenial::Missing,
            );
            assert_eq!(f.counts().await?, (0, 0));
        }
        let command = f
            .client()
            .prepare_command::<ClaimPreparation>(&f.target, identity()?, request(old.token))
            .await?;
        let evidence = command.evidence().clone();
        edit(&f, "CREATE TRIGGER preparation_restart_fault BEFORE INSERT ON catalog_operations BEGIN SELECT RAISE(ABORT,'late preparation restart failure'); END").await?;
        assert!(command.clone().execute().await.is_err());
        assert!(matches!(
            f.client().resolve(&evidence).await?,
            Resolution::Absent
        ));
        assert_eq!(f.counts().await?, (0, 0));
        edit(&f, "DROP TRIGGER preparation_restart_fault").await?;
        let next = lease(command.execute().await?.output)?;
        assert_eq!(next.token.artifact_operation, artifact_number(2));
        assert_eq!(next.token.owner, f.handle.owner_fence());
        assert_eq!(f.counts().await?, (1, 1));
        // The original grant cannot displace a known active successor.
        denied(
            f.client()
                .command::<ClaimPreparation>(&f.target, identity()?, request(old.token))
                .await,
            PreparationDenial::Stale,
        );
        assert_eq!(
            PreparationAdmission::load(&f.client(), &f.target, input.operation)
                .await?
                .ok_or("restart replaced receipt")?
                .lease(),
            old
        );
        f.client()
            .command::<AbortPreparation>(&f.target, identity()?, check(next.token))
            .await?;
        edit(
            &f,
            "UPDATE pushes SET response_id=zeroblob(16),completion_digest=zeroblob(32),rejected=1",
        )
        .await?;
        denied(
            f.client()
                .command::<ClaimPreparation>(&f.target, identity()?, request(old.token))
                .await,
            PreparationDenial::Conflict,
        );
        assert_eq!(f.counts().await?, (0, 1));
        f.runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn initial_preparation_receipt_knowledge_does_not_restore_revoked_or_expired_custody()
-> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        for revoked in [false, true] {
            let f = Fixture::new(format).await?;
            let input = f.begin([222; 16]);
            let command = f
                .client()
                .prepare_command::<BeginPreparation>(&f.target, identity()?, input.clone())
                .await?;
            let evidence = command.evidence().clone();
            let original = command.execute().await?;
            let old = lease(original.output.clone())?;
            if revoked {
                edit(
                    &f,
                    "UPDATE repository_identity SET owner='other' WHERE singleton=1",
                )
                .await?;
            } else {
                edit(&f, "UPDATE catalog_operations SET expires_at_ms=0; UPDATE catalog_leases SET expires_at_ms=0").await?;
            }
            let known = PreparationAdmission::load(&f.client(), &f.target, input.operation)
                .await?
                .ok_or("custody hid knowledge")?;
            assert_eq!(
                known.original(&evidence)?.ok_or("original hidden")?,
                original
            );
            assert!(
                PreparationSession::open(
                    f.client(),
                    f.target.clone(),
                    check(old.token),
                    Some(original.receipt)
                )
                .await
                .is_err()
            );
            denied(
                f.client()
                    .command::<RenewPreparation>(&f.target, identity()?, request(old.token))
                    .await,
                if revoked {
                    PreparationDenial::Unauthorized
                } else {
                    PreparationDenial::Expired
                },
            );
            if revoked {
                denied(
                    f.client()
                        .command::<ClaimPreparation>(&f.target, identity()?, request(old.token))
                        .await,
                    PreparationDenial::Unauthorized,
                );
            }
            assert_eq!(f.counts().await?, (1, 1));
            f.runtime.shutdown().await?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn initial_preparation_receipt_rejects_corruption_and_cross_purpose_metadata() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let f = Fixture::new(format).await?;
        let input = f.begin([223; 16]);
        let staged = f
            .client()
            .command::<BeginStaging>(&f.target, identity()?, input.clone())
            .await?;
        let StagingReply::Granted(staged) = staged.output else {
            return Err("staging grant absent".into());
        };
        f.client()
            .command::<BindStaging>(&f.target, identity()?, check(staged.token))
            .await?;
        let original = f
            .client()
            .command::<BeginPreparation>(&f.target, identity()?, input.clone())
            .await?;
        let body = f
            .handle
            .query(0, 2048, |db| {
                Ok(
                    db.query_row("SELECT initial_preparation FROM pushes", [], |row| {
                        row.get::<_, Vec<u8>>(0)
                    })?,
                )
            })
            .await?;
        let mut corrupt = body.clone();
        *corrupt.last_mut().ok_or("empty receipt")? ^= 1;
        edit(&f, "DROP TRIGGER push_initial_preparation_immutable").await?;
        edit(
            &f,
            &format!(
                "UPDATE pushes SET initial_preparation=x'{}'",
                hex::encode(corrupt)
            ),
        )
        .await?;
        assert!(
            PreparationAdmission::load(&f.client(), &f.target, input.operation)
                .await
                .is_err()
        );
        edit(
            &f,
            &format!(
                "UPDATE pushes SET initial_preparation=x'{}'",
                hex::encode(body)
            ),
        )
        .await?;
        assert_eq!(
            PreparationAdmission::load(&f.client(), &f.target, input.operation)
                .await?
                .ok_or("restored receipt absent")?
                .receipt(),
            original.receipt
        );
        // Keep scope, actor, operation, digest and MAC valid. Only the purpose
        // differs, so a context mismatch cannot satisfy this assertion.
        edit(&f, "UPDATE pushes SET initial_preparation=initial_staging").await?;
        assert!(matches!(
            PreparationAdmission::load(&f.client(), &f.target, input.operation).await,
            Err(PreparationReceiptError::Codec(CodecError::Invalid(
                "initial admission receipt purpose"
            )))
        ));
        f.runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn initial_preparation_receipt_does_not_invent_knowledge_for_denied_or_unexecuted_begin()
-> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let f = Fixture::new(format).await?;
        let mut input = f.begin([225; 16]);
        input.actor = "outsider".into();
        denied(
            f.client()
                .command::<BeginPreparation>(&f.target, identity()?, input.clone())
                .await,
            PreparationDenial::Unauthorized,
        );
        assert!(
            PreparationAdmission::load(&f.client(), &f.target, input.operation)
                .await?
                .is_none()
        );
        let mut mutation = identity()?;
        mutation.expires_at_ms = mutation.issued_at_ms + 1000;
        let command = f
            .client()
            .prepare_command::<BeginPreparation>(&f.target, mutation, f.begin([226; 16]))
            .await?;
        let evidence = command.evidence().clone();
        assert!(matches!(
            f.client().resolve(&evidence).await?,
            Resolution::Absent
        ));
        drop(command);
        expire(mutation.expires_at_ms).await?;
        assert!(matches!(
            f.client().resolve(&evidence).await?,
            Resolution::Expired
        ));
        // No domain record does not convert Expired into authoritative absence.
        assert!(
            PreparationAdmission::load(&f.client(), &f.target, [226; 16])
                .await?
                .is_none()
        );
        assert_eq!(f.counts().await?, (0, 0));
        f.runtime.shutdown().await?;
    }
    Ok(())
}
