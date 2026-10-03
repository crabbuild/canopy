use super::publishing::{edit, state};
use super::root_dispatch::Context;
use super::*;
use crate::packs::metadata::tests::limits;
use cellule_runtime::Resolution;
use tokio::time::{Duration, timeout};

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Loss {
    Live,
    LatePolicy,
    LateWrite,
    Policy,
    Write,
    Expiry,
}
async fn settled(ticket: &StagingTicket, uncertain: bool) -> Result<StagingState> {
    Ok(timeout(Duration::from_secs(10), async {
        loop {
            let value = ticket.state();
            if matches!(value, StagingState::Published(_) | StagingState::Fenced(_))
                || uncertain && matches!(value, StagingState::Uncertain(_))
            {
                return value;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?)
}
pub(super) async fn qualify(context: Context<'_>, fault: u8, loss: Loss) -> Result {
    if matches!(loss, Loss::Live | Loss::LatePolicy | Loss::LateWrite) {
        return qualify_live(context, loss).await;
    }
    let Context {
        fixture: f,
        prepared,
        store,
        staging,
        ticket,
        root,
        budget,
        request,
    } = context;
    let expected =
        crate::push::report::rejected_report(&request.response, crate::push::report::REJECTED)?;
    let session = Arc::new(ticket.bound_session()?);
    let operation = session.check.token.operation;
    assert!(
        session
            .root_outcome_completion(store, root, budget.clone(), None)
            .await
            .is_err()
    );
    let input = session
        .root_refusal_completion(store, root, budget.clone(), None)
        .await?;
    assert!(input.refusal);
    assert_eq!(input.outcomes.ref_generation, 0);
    let mut encoded = BoundedEncoder::new(ROOT_COMPLETION_BYTES)?;
    input.encode(&mut encoded)?;
    let bytes = encoded.finish();
    assert!(bytes.len() < 2048);
    let mut decoder = BoundedDecoder::new(&bytes, ROOT_COMPLETION_BYTES)?;
    assert_eq!(RootOutcomeCompletion::decode(&mut decoder)?, input);
    decoder.finish()?;
    let mut tampered = input.clone();
    tampered.refusal = false;
    assert!(
        tampered
            .encode(&mut BoundedEncoder::new(ROOT_COMPLETION_BYTES)?)
            .is_err()
    );
    let before = state(&f.handle).await?;
    // Both possible late writes must roll back; no terminal negative result
    // can exist without consuming custody in the same Cell transaction.
    if fault == 0 && loss == Loss::Policy {
        for trigger in [
            "BEFORE INSERT ON pushes",
            "BEFORE DELETE ON catalog_operations",
        ] {
            edit(f, &format!("CREATE TRIGGER policy_refusal_fault {trigger} BEGIN SELECT RAISE(ABORT,'policy refusal late fault'); END;")).await?;
            assert!(
                f.client()
                    .command::<CompleteRootOutcome>(&f.target, identity()?, input.clone())
                    .await
                    .is_err()
            );
            assert_eq!(state(&f.handle).await?, before);
            edit(f, "DROP TRIGGER policy_refusal_fault").await?;
        }
    }
    let pending = Arc::new(
        prepared
            .ref_policy_preparation(
                request.plan.ok_or("native plan")?,
                root,
                budget.clone(),
                limits(),
            )
            .await?,
    );
    let page = pending.ready_page(&prepared, identity()?, 0).await?;
    let page_evidence = page.evidence_for_test();
    let refusal = session
        .ready_root_refusal(identity()?, store, root, budget, None)
        .await?;
    let refusal_evidence = refusal.evidence_for_test();
    let mut page = page.with_refusal(refusal)?;
    page.refusal_fault_for_test(fault);
    let p = PublicationCoordinator::new(f.target.clone(), PublicationLimits::default())?;
    p.fault_for_test(1);
    drop(ticket.register_policy_page(&p, page)?);
    let StagingState::Uncertain(error) = settled(ticket, true).await? else {
        return Err("page uncertainty lost".into());
    };
    assert!(matches!(&*error, StagingError::Publication(error)
        if matches!(&**error, PublicationError::PolicyPage(InvocationError::Pending(actual)) if actual.as_ref()==&page_evidence)));
    assert!(matches!(
        f.client().resolve(&page_evidence).await?,
        Resolution::Absent
    ));
    assert_eq!(p.stats().await.command_bytes, 528 << 10);
    if loss == Loss::Write {
        edit(
            f,
            "UPDATE repository_identity SET owner='replacement' WHERE singleton=1",
        )
        .await?;
    } else {
        edit(
            f,
            "INSERT INTO branch_rules VALUES('refs/heads/main',1,1,0,0,0,0)",
        )
        .await?;
    }
    let recovery_before = state(&f.handle).await?;
    drop(pending);
    drop(prepared);
    drop(session);
    p.pending(operation)
        .await
        .ok_or("armed page lost")?
        .recover()
        .await?;
    let original = if fault != 0 {
        let error = timeout(Duration::from_secs(10), async {
            loop {
                if let StagingState::Uncertain(error) = ticket.state()
                    && matches!(&*error, StagingError::Publication(error)
                        if matches!(&**error, PublicationError::RootPush(InvocationError::Pending(actual)) if actual.as_ref()==&refusal_evidence)) {
                    break error;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }).await?;
        assert!(matches!(&*error, StagingError::Publication(error)
            if matches!(&**error, PublicationError::RootPush(InvocationError::Pending(actual)) if actual.as_ref()==&refusal_evidence)));
        assert_eq!(p.stats().await.command_bytes, 528 << 10);
        let original = match f.client().resolve(&refusal_evidence).await? {
            Resolution::Absent => None,
            Resolution::Committed(value) => {
                Some((value.result().to_vec(), value.commit_sequence()))
            }
            other => return Err(format!("unexpected refusal resolution {other:?}").into()),
        };
        assert_eq!(original.is_some(), fault != 1);
        if loss == Loss::Expiry {
            edit(f, "UPDATE catalog_operations SET expires_at_ms=0; UPDATE catalog_leases SET expires_at_ms=0;").await?;
        }
        assert_eq!(staging.close_and_drain().await.len(), 1);
        assert_eq!(p.close_and_drain().await.len(), 1);
        let before = state(&f.handle).await?;
        p.pending(operation)
            .await
            .ok_or("refusal phase lost")?
            .recover()
            .await?;
        assert!(matches!(
            settled(ticket, false).await?,
            StagingState::Published(_)
        ));
        if original.is_some() || loss == Loss::Expiry {
            assert_eq!(state(&f.handle).await?, before);
        }
        original
    } else {
        None
    };
    let observer = ticket
        .pending_publication()
        .ok_or("refusal observer lost")?;
    let outcome = observer.wait().await;
    if loss == Loss::Expiry && fault == 1 {
        assert!(matches!(outcome, PublicationState::Finished(Err(error))
            if matches!(&*error, PublicationError::RootPush(InvocationError::Rejected(value)) if value.output==RootCompletionReply::Denied(PreparationDenial::Expired))));
        assert!(observer.root_response(store).await.is_err());
        assert_eq!(f.counts().await?, (1, 2));
    } else {
        let PublicationState::Finished(Ok(PublicationOutcome::RootPush(committed))) = outcome
        else {
            return Err("terminal refusal not saved".into());
        };
        let RootCompletionReply::Completed(value) = &committed.output else {
            return Err("terminal refusal denied".into());
        };
        assert!(value.completion.rejected);
        assert!(value.completion.publication.is_none());
        if let Some((bytes, sequence)) = original {
            let mut e = BoundedEncoder::new(512)?;
            committed.output.encode(&mut e)?;
            assert_eq!(bytes, e.finish());
            assert_eq!(sequence, committed.receipt.commit_sequence);
        }
        if loss == Loss::Write {
            assert!(matches!(
                observer.root_response(store).await,
                Err(RootPushReplayError::Denied(PreparationDenial::Unauthorized))
            ));
            edit(
                f,
                "UPDATE repository_identity SET owner='owner' WHERE singleton=1",
            )
            .await?;
        }
        let mut actual = observer.root_response(store).await?;
        let mut body = Vec::new();
        while let Some(part) = actual.body.next().await? {
            body.extend_from_slice(&part);
        }
        assert_eq!(actual.status, expected.status);
        assert_eq!(actual.headers, expected.headers);
        assert_eq!(body, expected.body);
        assert_eq!(body.windows(3).filter(|p| *p == b"ng ").count(), 257);
        assert!(!body.windows(3).any(|p| p == b"ok "));
        assert_eq!(f.counts().await?, (0, 2));
    }
    let page = f.client().resolve(&page_evidence).await?;
    assert!(matches!(
        page,
        Resolution::Committed(cellule_runtime::cell::executor::StoredOutcome::Rejected { .. })
    ));
    let before: Vec<serde_json::Value> = serde_json::from_slice(&recovery_before)?;
    let after: Vec<serde_json::Value> = serde_json::from_slice(&state(&f.handle).await?)?;
    assert_eq!(before[..8], after[..8]);
    f.handle
        .query(0, 1024, move |db| {
            for table in [
                "refs",
                "push_responses",
                "push_response_chunks",
                "push_certificate_chunks",
            ] {
                assert_eq!(
                    db.query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r
                        .get::<_, u64>(0))?,
                    0
                );
            }
            assert_eq!(
                db.query_row(
                    "SELECT generation FROM catalog_state WHERE singleton=1",
                    [],
                    |r| r.get::<_, u64>(0)
                )?,
                1
            );
            assert_eq!(
                db.query_row("SELECT count(*) FROM pushes", [], |r| r.get::<_, u64>(0))?,
                u64::from(!(loss == Loss::Expiry && fault == 1))
            );
            Ok(Vec::new())
        })
        .await?;
    assert!(staging.close_and_drain().await.is_empty());
    assert!(p.close_and_drain().await.is_empty());
    assert_eq!(staging.stats().admitted, 0);
    assert_eq!(p.reservations_for_test().await, (0, 0, 0));
    Ok(())
}

async fn qualify_live(context: Context<'_>, loss: Loss) -> Result {
    let Context {
        fixture: f,
        prepared,
        store,
        staging,
        ticket,
        root,
        budget,
        request,
    } = context;
    let pending = Arc::new(
        prepared
            .ref_policy_preparation(
                request.plan.ok_or("native plan")?,
                root,
                budget.clone(),
                limits(),
            )
            .await?,
    );
    let session = Arc::new(ticket.bound_session()?);
    let refusal = Arc::new(
        session
            .ready_root_refusal(identity()?, store, root, budget.clone(), None)
            .await?,
    );
    let evidence = refusal.evidence_for_test();
    let p = PublicationCoordinator::new(f.target.clone(), PublicationLimits::default())?;
    for offset in [0, 128, 256] {
        let page = pending
            .ready_page(&prepared, identity()?, offset)
            .await?
            .with_refusal(refusal.clone())?;
        let observer = ticket.register_policy_page(&p, page)?;
        let PublicationState::Finished(Ok(PublicationOutcome::PolicyPage(value))) =
            observer.wait().await
        else {
            return Err("armed live page did not register".into());
        };
        assert!(
            matches!(value.output, RefPolicyReply::Registered(progress) if progress.valid && progress.next == (offset+128).min(257) as u64)
        );
        timeout(Duration::from_secs(10), async {
            while !matches!(ticket.state(), StagingState::Bound(_)) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await?;
        assert!(matches!(
            f.client().resolve(&evidence).await?,
            Resolution::Absent
        ));
        assert_eq!(p.reservations_for_test().await, (0, 0, 0));
        assert_eq!(Arc::strong_count(&refusal), 1);
    }
    let guard = Arc::new(pending.ready(&prepared).await?);
    let positive = owned_positive(ticket, &prepared, &guard, root, budget.clone()).await?;
    let positive_evidence = positive.evidence_for_test();
    let page = pending.ready_page(&prepared, identity()?, 0).await?;
    let page_evidence = page.evidence_for_test();
    let failure = page
        .with_refusal(positive)
        .err()
        .ok_or("positive command accepted as refusal")?;
    assert_eq!(failure.page.evidence_for_test(), page_evidence);
    assert_eq!(failure.refusal.evidence_for_test(), positive_evidence);
    drop(failure);
    let before = state(&f.handle).await?;
    let observer = if loss == Loss::Live {
        let positive = owned_positive(ticket, &prepared, &guard, root, budget).await?;
        ticket.publish(&p, positive)?
    } else {
        let changed = if loss == Loss::LateWrite {
            "UPDATE repository_identity SET owner='replacement' WHERE singleton=1"
        } else {
            "INSERT INTO branch_rules VALUES('refs/heads/main',1,1,0,0,0,0)"
        };
        edit(f, changed).await?;
        assert!(pending.ready(&prepared).await.is_err());
        ticket.renew_for_test();
        ticket.publish(&p, refusal.clone())?
    };
    let PublicationState::Finished(Ok(PublicationOutcome::RootPush(value))) = observer.wait().await
    else {
        return Err("armed completed-page pipeline did not complete".into());
    };
    let RootCompletionReply::Completed(completed) = &value.output else {
        return Err("completed-page result denied".into());
    };
    assert_eq!(completed.completion.rejected, loss != Loss::Live);
    assert_eq!(
        completed.completion.publication.is_some(),
        loss == Loss::Live
    );
    assert!(matches!(
        settled(ticket, false).await?,
        StagingState::Published(Ok(_))
    ));
    if loss == Loss::LateWrite {
        assert!(matches!(
            observer.root_response(store).await,
            Err(RootPushReplayError::Denied(PreparationDenial::Unauthorized))
        ));
        edit(
            f,
            "UPDATE repository_identity SET owner='owner' WHERE singleton=1",
        )
        .await?;
    }
    let mut actual = observer.root_response(store).await?;
    let mut body = Vec::new();
    while let Some(part) = actual.body.next().await? {
        body.extend_from_slice(&part);
    }
    if loss == Loss::Live {
        assert_eq!(body, request.response.body);
        assert!(matches!(
            f.client().resolve(&evidence).await?,
            Resolution::Absent
        ));
    } else {
        let expected =
            crate::push::report::rejected_report(&request.response, crate::push::report::REJECTED)?;
        assert_eq!(body, expected.body);
        assert_eq!(actual.status, expected.status);
        assert_eq!(actual.headers, expected.headers);
        assert_eq!(body.windows(3).filter(|p| *p == b"ng ").count(), 257);
        assert!(!body.windows(3).any(|p| p == b"ok "));
        assert!(matches!(
            f.client().resolve(&evidence).await?,
            Resolution::Committed(_)
        ));
        let before: Vec<serde_json::Value> = serde_json::from_slice(&before)?;
        let after: Vec<serde_json::Value> = serde_json::from_slice(&state(&f.handle).await?)?;
        assert_eq!(before[..6], after[..6]);
    }
    assert_eq!(f.counts().await?, (0, 2));
    assert!(staging.close_and_drain().await.is_empty());
    assert!(p.close_and_drain().await.is_empty());
    assert_eq!(p.reservations_for_test().await, (0, 0, 0));
    Ok(())
}

// Match the existing production ownership boundary instead of composing the
// native fixture and expensive root-preparation poll frames on one stack.
async fn owned_positive(
    ticket: &StagingTicket,
    prepared: &Arc<PreparedCatalog>,
    guard: &Arc<PreparedRefPolicyGuard>,
    root: &std::path::Path,
    budget: cellule_ltx::DiskBudget,
) -> Result<ReadyRootPush> {
    let owner = prepared.clone();
    let guard = guard.clone();
    let directory = root.to_path_buf();
    let mutation = identity()?;
    Ok(ticket
        .spawn_bound(move |_| async move {
            owner
                .ready_root_push(mutation, &guard, &directory, budget, limits(), None)
                .await
                .map_err(|error| StagingError::Input(Box::new(error)))
        })?
        .wait()
        .await
        .map_err(|error| format!("owned positive preparation: {error}"))?)
}
