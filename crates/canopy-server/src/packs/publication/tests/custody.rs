//! Real Cell receipts, pre-admission recovery and the original command's atomic result.
use super::{publishing::edit, *};
use cellule_runtime::{Committed, Resolution};
use tokio::time::Duration;

async fn prepare(f: &Fixture, action: CustodyAction) -> Result<PreparedCustody> {
    Ok(PreparedCustody::prepare(&f.client(), &f.target, action, identity()?).await?)
}
async fn execute(f: &Fixture, action: CustodyAction) -> Result<Committed<CustodyReply>> {
    Ok(prepare(f, action)
        .await?
        .register(&f.client(), identity()?)
        .await?
        .recover(&f.client())
        .await?)
}
fn token(output: &CustodyReply) -> Result<PreparationToken> {
    match output {
        CustodyReply::Preparation(PreparationReply::Granted(lease)) => Ok(lease.token),
        CustodyReply::Staging(StagingReply::Granted(lease)) => Ok(lease.token),
        other => Err(format!("expected custody grant: {other:?}").into()),
    }
}
async fn expire(identity: MutationIdentity) -> Result {
    let now = sql::now(0)?;
    if now <= identity.expires_at_ms {
        tokio::time::sleep(Duration::from_millis(u64::try_from(
            identity.expires_at_ms - now + 1,
        )?))
        .await;
    }
    Ok(())
}

#[tokio::test]
async fn owned_registrar_loss_preserves_both_originals_and_recovers_without_new_namespace() -> Result
{
    use crate::packs::publication::custody::OwnedCustody;
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        for fault in [4, 5, 6] {
            let f = Fixture::new(format).await?;
            let operation = [fault + 180; 16];
            let owned = OwnedCustody::prepare(
                &f.client(),
                &f.target,
                CustodyAction::BeginPreparation(f.begin(operation)),
                identity()?,
            )
            .await?;
            let original = owned.evidence().clone();
            let registrar = owned
                .registration_evidence()
                .ok_or("registrar missing")?
                .clone();
            assert_ne!(original, registrar);
            let dispatch = owned.clone();
            let client = f.client();
            let result =
                tokio::spawn(
                    async move { dispatch.invoke(&client, false, fault, || Ok(())).await },
                )
                .await;
            if fault == 6 {
                assert!(result.is_err_and(|error| error.is_panic()));
            } else {
                let Err(CustodyError::Registration(error)) = result? else {
                    return Err("registrar fault did not preserve uncertainty".into());
                };
                let InvocationError::Pending(evidence) = *error else {
                    return Err("registrar fault returned terminal evidence".into());
                };
                assert_eq!(*evidence, registrar);
            }
            assert_eq!(owned.evidence(), &original);
            assert_eq!(owned.registration_evidence(), Some(&registrar));
            assert_eq!(f.counts().await?, (0, 0));
            assert!(matches!(
                f.client().resolve(&original).await?,
                Resolution::Absent
            ));
            let registered =
                RegisteredCustody::load_latest(&f.client(), &f.target, operation).await?;
            assert_eq!(registered.is_some(), fault != 4);
            if let Some(registered) = registered {
                assert_eq!(registered.evidence(), &original);
            }
            let committed = owned.invoke(&f.client(), true, 0, || Ok(())).await??;
            assert_eq!(
                token(&committed.output)?.artifact_operation,
                artifact_number(1)
            );
            assert_eq!(f.counts().await?, (1, 1));
            assert!(matches!(
                f.client().resolve(&registrar).await?,
                Resolution::Committed(_)
            ));
            drop(owned);
            let restored = OwnedCustody::restore(&f.client(), &f.target, operation).await?;
            assert_eq!(restored.evidence(), &original);
            assert!(restored.registration_evidence().is_none());
            // Recorded knowledge precedes a fresh execution guard; recovery
            // must not execute another Begin or allocate another namespace.
            let replay = restored
                .invoke(&f.client(), true, 0, || {
                    Err(Error::Command("fresh custody denied"))
                })
                .await??;
            assert_eq!(replay, committed);
            assert_eq!(f.counts().await?, (1, 1));
            f.runtime.shutdown().await?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn intent_precedes_namespace_and_refuses_unregistered_or_losing_execution() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let f = Fixture::new(format).await?;
        let operation = [231; 16];
        let action = CustodyAction::BeginPreparation(f.begin(operation));
        let original = prepare(&f, action.clone()).await?;
        let command = original.command_for_test(&f.client())?;
        assert!(matches!(
            command.clone().execute().await,
            Err(InvocationError::NotStarted(_))
        ));
        assert!(matches!(
            f.client().resolve(command.evidence()).await?,
            Resolution::Absent
        ));
        let loser = prepare(&f, action.clone()).await?;
        let registered = original.register(&f.client(), identity()?).await?;
        assert_eq!(f.counts().await?, (0, 0));
        assert!(!registered.settled());
        assert!(
            matches!(prepare(&f, action).await, Err(error) if error.downcast_ref::<CustodyError>().is_some_and(|e| matches!(e, CustodyError::Unsettled(_))))
        );
        assert!(loser.register(&f.client(), identity()?).await.is_err());
        let losing = loser.command_for_test(&f.client())?;
        assert!(matches!(
            losing.clone().execute().await,
            Err(InvocationError::NotStarted(_))
        ));
        assert!(matches!(
            f.client().resolve(losing.evidence()).await?,
            Resolution::Absent
        ));
        // Discard all capabilities: discovery preserves the first SDK identity.
        drop(registered);
        drop(original);
        let discovered = RegisteredCustody::load_latest(&f.client(), &f.target, operation)
            .await?
            .ok_or("intent missing")?;
        assert_eq!(discovered.evidence(), command.evidence());
        let accepted = discovered.recover(&f.client()).await?;
        let granted = token(&accepted.output)?;
        assert_eq!(granted.attempt, accepted.receipt.commit_sequence);
        assert_eq!(granted.owner, f.handle.owner_fence());
        assert_eq!(granted.artifact_operation, artifact_number(1));
        assert_eq!(f.counts().await?, (1, 1));
        assert_eq!(discovered.recover(&f.client()).await?, accepted);
        f.runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn registration_and_late_phase_failure_keep_sdk_absent_and_exact_retry() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let f = Fixture::new(format).await?;
        let original = prepare(&f, CustodyAction::BeginStaging(f.begin([232; 16]))).await?;
        let registration = f
            .client()
            .prepare_command::<RegisterCustodyIntent>(
                &f.target,
                identity()?,
                original.intent_for_test(),
            )
            .await?;
        edit(&f, "CREATE TRIGGER custody_registration_fault BEFORE INSERT ON catalog_custody_commands BEGIN SELECT RAISE(ABORT,'late custody registration failure'); END").await?;
        assert!(
            matches!(registration.clone().execute().await, Err(InvocationError::NotStarted(error)) if format!("{error:?}").contains("late custody registration failure"))
        );
        assert!(matches!(
            f.client().resolve(registration.evidence()).await?,
            Resolution::Absent
        ));
        assert!(
            RegisteredCustody::load_latest(&f.client(), &f.target, [232; 16])
                .await?
                .is_none()
        );
        assert_eq!(f.counts().await?, (0, 0));
        edit(&f, "DROP TRIGGER custody_registration_fault").await?;
        registration.execute().await?;
        // Losing the registration acknowledgement is recovered by its durable
        // pointer, without preparing another original custody command.
        let registered = RegisteredCustody::load_latest(&f.client(), &f.target, [232; 16])
            .await?
            .ok_or("registration absent")?;
        assert_eq!(registered.evidence(), original.evidence());
        edit(&f, "CREATE TRIGGER custody_phase_fault BEFORE UPDATE OF phase ON catalog_custody_commands BEGIN SELECT RAISE(ABORT,'late custody phase failure'); END").await?;
        let command = original.command_for_test(&f.client())?;
        assert!(
            matches!(command.clone().execute().await, Err(InvocationError::NotStarted(error)) if format!("{error:?}").contains("late custody phase failure"))
        );
        assert!(matches!(
            f.client().resolve(command.evidence()).await?,
            Resolution::Absent
        ));
        assert_eq!(f.counts().await?, (0, 0));
        assert!(
            StagingAdmission::load(&f.client(), &f.target, [232; 16])
                .await?
                .is_none()
        );
        edit(&f, "DROP TRIGGER custody_phase_fault").await?;
        let committed = registered.recover(&f.client()).await?;
        assert_eq!(
            token(&committed.output)?.artifact_operation,
            artifact_number(1)
        );
        assert_eq!(f.counts().await?, (1, 1));
        for sql in [
            "UPDATE catalog_custody_commands SET intent=x'01'",
            "UPDATE catalog_custody_commands SET phase=NULL",
            "DELETE FROM catalog_custody_commands",
            "INSERT OR REPLACE INTO catalog_custody_commands SELECT * FROM catalog_custody_commands",
        ] {
            assert!(edit(&f, sql).await.is_err());
        }
        f.runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn every_custody_transition_retains_its_original_result_across_successors() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let f = Fixture::new(format).await?;
        let operation = [233; 16];
        let mut known = Vec::new();
        let mut next = CustodyAction::BeginStaging(f.begin(operation));
        for step in 0..7 {
            let original = prepare(&f, next).await?;
            let registered = original.register(&f.client(), identity()?).await?;
            let committed = registered.recover(&f.client()).await?;
            let current = token(&committed.output)?;
            known.push((registered, committed));
            next = match step {
                0 => CustodyAction::RenewStaging(request(current)),
                1 => CustodyAction::ClaimStaging(request(current)),
                2 => CustodyAction::BindStaging(check(current)),
                3 => CustodyAction::RenewPreparation(request(current)),
                4 => CustodyAction::ClaimPreparation(request(current)),
                _ => CustodyAction::BeginPreparation(f.begin(operation)),
            };
        }
        for (registered, committed) in known {
            assert_eq!(registered.recover(&f.client()).await?, committed);
        }
        let operation_count = f
            .handle
            .query(0, 1024, |db| {
                Ok(
                    db.query_row("SELECT count(*) FROM catalog_custody_commands", [], |r| {
                        r.get::<_, u64>(0).map(|value| value.to_be_bytes().to_vec())
                    })?,
                )
            })
            .await?;
        assert_eq!(u64::from_be_bytes(operation_count.try_into().unwrap()), 7);
        assert_eq!(f.counts().await?, (1, 3));
        f.runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn denied_begin_is_original_knowledge_after_sdk_expiry_and_authority_changes() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let f = Fixture::new(format).await?;
        let operation = [234; 16];
        let mut mutation = identity()?;
        mutation.expires_at_ms = mutation.issued_at_ms + 1_000;
        let original = PreparedCustody::prepare(
            &f.client(),
            &f.target,
            CustodyAction::BeginPreparation(f.begin(operation)),
            mutation,
        )
        .await?;
        let registered = original.register(&f.client(), identity()?).await?;
        edit(&f, "UPDATE repository_identity SET owner='other'").await?;
        let committed = match registered.recover(&f.client()).await {
            Err(InvocationError::Rejected(committed)) => *committed,
            other => return Err(format!("expected original unauthorized denial: {other:?}").into()),
        };
        assert_eq!(
            committed.output,
            CustodyReply::Preparation(PreparationReply::Denied(PreparationDenial::Unauthorized))
        );
        assert_eq!(f.counts().await?, (0, 0));
        expire(mutation).await?;
        assert!(matches!(
            f.client().resolve(original.evidence()).await?,
            Resolution::Expired
        ));
        edit(&f, "UPDATE repository_identity SET owner='owner'").await?;
        let next = execute(&f, CustodyAction::BeginPreparation(f.begin(operation))).await?;
        assert_eq!(token(&next.output)?.artifact_operation, artifact_number(1));
        assert!(
            matches!(registered.recover(&f.client()).await, Err(InvocationError::Rejected(value)) if *value == committed)
        );
        f.runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn cold_owner_restoration_recovers_claim_and_renew_receipts_without_reviving_custody()
-> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        for claim in [false, true] {
            let f = Fixture::new(format).await?;
            let operation = [235; 16];
            let started = execute(&f, CustodyAction::BeginPreparation(f.begin(operation))).await?;
            let old = token(&started.output)?;
            let mut mutation = identity()?;
            mutation.expires_at_ms = mutation.issued_at_ms + 1_000;
            let action = if claim {
                CustodyAction::ClaimPreparation(request(old))
            } else {
                CustodyAction::RenewPreparation(request(old))
            };
            let original =
                PreparedCustody::prepare(&f.client(), &f.target, action, mutation).await?;
            let registered = original.register(&f.client(), identity()?).await?;
            let accepted = registered.recover(&f.client()).await?;
            let token = token(&accepted.output)?;
            let (runtime, handle, client) =
                super::durable_recovery::restore_owner(&f, &check(token)).await?;
            expire(mutation).await?;
            assert!(matches!(
                client.resolve(original.evidence()).await?,
                Resolution::Expired
            ));
            edit_restored(&handle, "UPDATE repository_identity SET owner='other'").await?;
            let recovered = RegisteredCustody::load_latest(&client, &f.target, operation)
                .await?
                .ok_or("cold custody receipt absent")?;
            assert_eq!(recovered.evidence(), original.evidence());
            assert_eq!(recovered.recover(&client).await?, accepted);
            assert_ne!(token.owner, handle.owner_fence());
            assert!(
                client
                    .query::<CheckPreparation>(&f.target, Some(accepted.receipt), check(token))
                    .await?
                    .output
                    .is_none()
            );
            runtime.shutdown().await?;
        }
    }
    Ok(())
}
async fn edit_restored(handle: &CellHandle, sql: &'static str) -> Result {
    super::publishing::edit_handle(handle, sql).await
}

#[tokio::test]
async fn expired_unsettled_identity_cannot_be_replaced_or_reported_as_absent() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let f = Fixture::new(format).await?;
        let operation = [236; 16];
        let action = CustodyAction::BeginStaging(f.begin(operation));
        let mut mutation = identity()?;
        mutation.expires_at_ms = mutation.issued_at_ms + 1_000;
        let original =
            PreparedCustody::prepare(&f.client(), &f.target, action.clone(), mutation).await?;
        let registered = original.register(&f.client(), identity()?).await?;
        expire(mutation).await?;
        assert!(
            matches!(registered.recover(&f.client()).await, Err(InvocationError::Pending(evidence)) if *evidence == *original.evidence())
        );
        assert!(
            matches!(PreparedCustody::prepare(&f.client(), &f.target, action, identity()?).await, Err(CustodyError::Unsettled(evidence)) if *evidence == *original.evidence())
        );
        assert_eq!(f.counts().await?, (0, 0));
        f.runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn denied_renewal_preserves_knowledge_and_exact_successor_claim_survives_reaping() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        for staging in [false, true] {
            let f = Fixture::new(format).await?;
            let input = f.begin([237; 16]);
            let begin = if staging {
                CustodyAction::BeginStaging(input)
            } else {
                CustodyAction::BeginPreparation(input)
            };
            let first = execute(&f, begin).await?;
            let original = token(&first.output)?;
            let claim = if staging {
                CustodyAction::ClaimStaging(request(original))
            } else {
                CustodyAction::ClaimPreparation(request(original))
            };
            let second = execute(&f, claim).await?;
            let prior = token(&second.output)?;
            assert_ne!(prior, original);
            edit(&f, "UPDATE catalog_operations SET expires_at_ms=0; UPDATE catalog_leases SET expires_at_ms=0").await?;
            let mut mutation = identity()?;
            mutation.expires_at_ms = mutation.issued_at_ms + 1_000;
            let action = if staging {
                CustodyAction::RenewStaging(request(prior))
            } else {
                CustodyAction::RenewPreparation(request(prior))
            };
            let denied = PreparedCustody::prepare(&f.client(), &f.target, action, mutation)
                .await?
                .register(&f.client(), identity()?)
                .await?;
            let original_denial = match denied.recover(&f.client()).await {
                Err(InvocationError::Rejected(value)) => *value,
                other => return Err(format!("expected original expired denial: {other:?}").into()),
            };
            let expected = if staging {
                CustodyReply::Staging(StagingReply::Denied(PreparationDenial::Expired))
            } else {
                CustodyReply::Preparation(PreparationReply::Denied(PreparationDenial::Expired))
            };
            assert_eq!(original_denial.output, expected);
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
            // A forged token cannot borrow the authenticated previous grant.
            let mut forged = prior;
            forged.request_digest[0] ^= 1;
            assert!(
                PreparedCustody::prepare(
                    &f.client(),
                    &f.target,
                    if staging {
                        CustodyAction::ClaimStaging(request(forged))
                    } else {
                        CustodyAction::ClaimPreparation(request(forged))
                    },
                    identity()?
                )
                .await
                .is_err()
            );
            let claimed = execute(
                &f,
                if staging {
                    CustodyAction::ClaimStaging(request(prior))
                } else {
                    CustodyAction::ClaimPreparation(request(prior))
                },
            )
            .await?;
            let next = token(&claimed.output)?;
            assert_ne!(next, prior);
            assert_eq!(next.owner, f.handle.owner_fence());
            assert_eq!(next.attempt, claimed.receipt.commit_sequence);
            assert_eq!(next.artifact_operation, artifact_number(3));
            assert_eq!(f.counts().await?, (1, 1));
            expire(mutation).await?;
            assert!(matches!(
                f.client().resolve(denied.evidence()).await?,
                Resolution::Expired
            ));
            assert!(
                matches!(denied.recover(&f.client()).await, Err(InvocationError::Rejected(value)) if *value == original_denial)
            );
            f.runtime.shutdown().await?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn corrupted_metadata_blocks_sdk_fallback_and_journal_queries_are_indexed() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let f = Fixture::new(format).await?;
        let operation = [238; 16];
        let registered = prepare(&f, CustodyAction::BeginPreparation(f.begin(operation)))
            .await?
            .register(&f.client(), identity()?)
            .await?;
        registered.recover(&f.client()).await?;
        let plans = f.handle.query(0, 4096, |db| {
            let mut all = String::new();
            for sql in [
                "EXPLAIN QUERY PLAN SELECT intent,phase FROM catalog_custody_commands WHERE operation=zeroblob(16) ORDER BY step DESC LIMIT 1",
                "EXPLAIN QUERY PLAN SELECT operation FROM catalog_custody_commands WHERE phase IS NULL AND stopped IS NULL LIMIT 1024",
                "EXPLAIN QUERY PLAN SELECT intent,phase FROM catalog_custody_commands INDEXED BY catalog_custody_grants WHERE operation=zeroblob(16) AND granted_incarnation=zeroblob(16) AND granted_attempt=1 ORDER BY step DESC LIMIT 1",
            ] {
                let mut statement = db.prepare(sql)?;
                let mut rows = statement.query([])?;
                while let Some(row) = rows.next()? { all.push_str(&row.get::<_,String>(3)?); all.push('\n'); }
            }
            Ok(all.into_bytes())
        }).await?;
        let plans = String::from_utf8(plans)?;
        assert!(plans.contains("PRIMARY KEY"), "{plans}");
        assert!(plans.contains("catalog_custody_pending"), "{plans}");
        assert!(plans.contains("catalog_custody_grants"), "{plans}");
        assert!(matches!(
            f.client().resolve(registered.evidence()).await?,
            Resolution::Committed(_)
        ));
        edit(&f, "DROP TRIGGER catalog_custody_identity_immutable").await?;
        f.handle
            .execute(
                identity()?,
                Digest::from_bytes([239; 32]),
                sql::now(0)?,
                4096,
                0,
                move |tx| {
                    let mut bytes: Vec<u8> = tx.query_row(
                        "SELECT intent FROM catalog_custody_commands WHERE operation=?1",
                        [operation.as_slice()],
                        |row| row.get(0),
                    )?;
                    *bytes
                        .last_mut()
                        .ok_or(cellule_runtime::Error::Command("custody body absent"))? ^= 1;
                    tx.execute(
                        "UPDATE catalog_custody_commands SET intent=?1 WHERE operation=?2",
                        rusqlite::params![bytes, operation.as_slice()],
                    )?;
                    Ok(cellule_runtime::cell::executor::HandlerOutcome::Success(
                        Vec::new(),
                    ))
                },
            )
            .await?;
        assert!(
            RegisteredCustody::load_latest(&f.client(), &f.target, operation)
                .await
                .is_err()
        );
        assert!(
            matches!(registered.recover(&f.client()).await, Err(InvocationError::Pending(evidence)) if *evidence == *registered.evidence())
        );
        assert_eq!(f.counts().await?, (1, 1));
        f.runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn initialized_generation_grants_fit_the_journal_and_preserve_joint_roots() -> Result {
    use canopy_object_storage::artifact::ArtifactStore;
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let f = Fixture::new(format).await?;
        let store = Arc::new(ArtifactStore::new(Arc::new(InMemory::new()), f.repository));
        let (prepared, _root, _budget) = super::initialization::empty(&f, [240; 16], store).await?;
        let proof = prepared.empty_ref_initialization().await?;
        let (initialization, _) =
            super::initialization::registered(&f, &prepared, proof, identity()?).await?;
        let initialized = initialization.execute().await?;
        let InitializationReply::Initialized(fact) = initialized.output else {
            return Err("initialization failed".into());
        };
        assert!(fact.catalog.is_some() && fact.refs.is_some());
        let original = prepare(&f, CustodyAction::BeginPreparation(f.begin([241; 16])))
            .await?
            .register(&f.client(), identity()?)
            .await?;
        let committed = original.recover_preparation(&f.client()).await?;
        let PreparationReply::Granted(lease) = committed.output else {
            return Err("grant absent".into());
        };
        assert_eq!(lease.base, *fact);
        let renewed = execute(&f, CustodyAction::RenewPreparation(request(lease.token))).await?;
        let CustodyReply::Preparation(PreparationReply::Granted(renewed)) = renewed.output else {
            return Err("renewal absent".into());
        };
        assert_eq!(renewed.base, *fact);
        f.runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn ignored_registration_or_phase_write_cannot_commit_without_domain_knowledge() -> Result {
    for phase in [false, true] {
        let f = Fixture::new(ObjectFormat::Sha256).await?;
        let operation = [242; 16];
        let original = prepare(&f, CustodyAction::BeginPreparation(f.begin(operation))).await?;
        if phase {
            original.register(&f.client(), identity()?).await?;
        }
        let event = if phase { "UPDATE OF phase" } else { "INSERT" };
        edit(&f, &format!("CREATE TRIGGER custody_ignore_fault BEFORE {event} ON catalog_custody_commands BEGIN SELECT RAISE(IGNORE); END")).await?;
        if phase {
            let command = original.command_for_test(&f.client())?;
            assert!(
                matches!(command.clone().execute().await, Err(InvocationError::NotStarted(error)) if format!("{error:?}").contains("publication changed unexpected rows"))
            );
            assert!(matches!(
                f.client().resolve(command.evidence()).await?,
                Resolution::Absent
            ));
        } else {
            let command = f
                .client()
                .prepare_command::<RegisterCustodyIntent>(
                    &f.target,
                    identity()?,
                    original.intent_for_test(),
                )
                .await?;
            assert!(
                matches!(command.clone().execute().await, Err(InvocationError::NotStarted(error)) if format!("{error:?}").contains("publication changed unexpected rows"))
            );
            assert!(matches!(
                f.client().resolve(command.evidence()).await?,
                Resolution::Absent
            ));
            assert!(
                RegisteredCustody::load_latest(&f.client(), &f.target, operation)
                    .await?
                    .is_none()
            );
        }
        assert_eq!(f.counts().await?, (0, 0));
        edit(&f, "DROP TRIGGER custody_ignore_fault").await?;
        original
            .register(&f.client(), identity()?)
            .await?
            .recover(&f.client())
            .await?;
        assert_eq!(f.counts().await?, (1, 1));
        f.runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn registered_unexecuted_begin_survives_deleted_sqlite_and_cold_owner_restore() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        for staging in [false, true] {
            let f = Fixture::new(format).await?;
            let operation = [243; 16];
            let action = if staging {
                CustodyAction::BeginStaging(f.begin(operation))
            } else {
                CustodyAction::BeginPreparation(f.begin(operation))
            };
            let prepared = prepare(&f, action.clone()).await?;
            let original = prepared.evidence().clone();
            prepared.register(&f.client(), identity()?).await?;
            assert_eq!(f.counts().await?, (0, 0));
            assert!(matches!(
                f.client().resolve(&original).await?,
                Resolution::Absent
            ));
            drop(prepared);
            let (runtime, handle, client) =
                super::durable_recovery::restore_owner_fence(&f, f.handle.owner_fence()).await?;
            assert_eq!(super::counts(&handle).await?, (0, 0));
            let recovered = RegisteredCustody::load_latest(&client, &f.target, operation)
                .await?
                .ok_or("pre-namespace intent missing")?;
            assert_eq!(recovered.evidence(), &original);
            assert_eq!(recovered.action()?, action);
            assert!(matches!(
                client.resolve(&original).await?,
                Resolution::Absent
            ));
            let committed = recovered.recover(&client).await?;
            let admitted = token(&committed.output)?;
            assert_eq!(admitted.owner, handle.owner_fence());
            assert_eq!(admitted.attempt, committed.receipt.commit_sequence);
            assert_eq!(admitted.artifact_operation, artifact_number(1));
            assert_eq!(super::counts(&handle).await?, (1, 1));
            assert_eq!(recovered.recover(&client).await?, committed);
            runtime.shutdown().await?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn denied_claim_keeps_its_original_receipt_after_sdk_expiry_and_cold_restore() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        for staging in [false, true] {
            let f = Fixture::new(format).await?;
            let operation = [244; 16];
            let begun = execute(
                &f,
                if staging {
                    CustodyAction::BeginStaging(f.begin(operation))
                } else {
                    CustodyAction::BeginPreparation(f.begin(operation))
                },
            )
            .await?;
            let old = token(&begun.output)?;
            let accepted = execute(
                &f,
                if staging {
                    CustodyAction::ClaimStaging(request(old))
                } else {
                    CustodyAction::ClaimPreparation(request(old))
                },
            )
            .await?;
            let successor = token(&accepted.output)?;
            let mut mutation = identity()?;
            mutation.expires_at_ms = mutation.issued_at_ms + 1_000;
            let action = if staging {
                CustodyAction::ClaimStaging(request(old))
            } else {
                CustodyAction::ClaimPreparation(request(old))
            };
            let denied = PreparedCustody::prepare(&f.client(), &f.target, action, mutation)
                .await?
                .register(&f.client(), identity()?)
                .await?;
            let original = match denied.recover(&f.client()).await {
                Err(InvocationError::Rejected(value)) => *value,
                other => return Err(format!("expected original stale Claim: {other:?}").into()),
            };
            let expected = if staging {
                CustodyReply::Staging(StagingReply::Denied(PreparationDenial::Stale))
            } else {
                CustodyReply::Preparation(PreparationReply::Denied(PreparationDenial::Stale))
            };
            assert_eq!(original.output, expected);
            assert_eq!(f.counts().await?, (1, 2));
            let evidence = denied.evidence().clone();
            let (runtime, handle, client) =
                super::durable_recovery::restore_owner_fence(&f, successor.owner).await?;
            expire(mutation).await?;
            assert!(matches!(
                client.resolve(&evidence).await?,
                Resolution::Expired
            ));
            let restored = RegisteredCustody::load_latest(&client, &f.target, operation)
                .await?
                .ok_or("cold Claim denial missing")?;
            assert_eq!(restored.evidence(), &evidence);
            assert!(
                matches!(restored.recover(&client).await, Err(InvocationError::Rejected(value)) if *value == original)
            );
            assert_eq!(super::counts(&handle).await?, (1, 2));
            runtime.shutdown().await?;
        }
    }
    Ok(())
}
