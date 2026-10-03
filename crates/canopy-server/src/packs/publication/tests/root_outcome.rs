use super::publishing::{edit, state};
use super::*;
use crate::git_http::GitHttpResponse;
use canopy_object_storage::artifact::{ArtifactRead, ArtifactStore};
use cellule_ltx::DiskBudget;
use cellule_runtime::Resolution;
use std::path::Path;
use tokio::time::{Duration, timeout};
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Kind {
    HookRefusal,
    Empty,
}
pub(super) struct Context<'a> {
    pub fixture: &'a Fixture,
    pub store: &'a ArtifactStore,
    pub staging: &'a StagingCoordinator,
    pub ticket: &'a StagingTicket,
    pub root: &'a Path,
    pub budget: DiskBudget,
    pub request: PushCompletionRequest,
}
async fn response(mut value: GitHttpResponse<ArtifactRead>) -> Result<GitHttpResponse> {
    let mut body = Vec::new();
    while let Some(part) = value.body.next().await? {
        body.extend_from_slice(&part);
    }
    Ok(GitHttpResponse {
        status: value.status,
        headers: value.headers,
        body,
    })
}
async fn terminal(ticket: &StagingTicket, uncertain: bool) -> Result<StagingState> {
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
fn roots_unchanged(before: &[u8], after: &[u8]) -> Result {
    let before: Vec<serde_json::Value> = serde_json::from_slice(before)?;
    let after: Vec<serde_json::Value> = serde_json::from_slice(after)?;
    assert_eq!(before.len(), 9);
    assert_eq!(after.len(), 9);
    assert_eq!(before[..8], after[..8]);
    assert_ne!(before[8], after[8]);
    Ok(())
}
pub(super) async fn qualify(context: Context<'_>, kind: Kind, fault: u8, revoked: bool) -> Result {
    let Context {
        fixture: f,
        store,
        staging,
        ticket,
        root,
        budget,
        request,
    } = context;
    assert!(request.plan.is_none());
    let expected = request.response;
    let session = Arc::new(ticket.bound_session()?);
    let operation = session.check.token.operation;
    let lookup = BeginRequest {
        repository: f.repository,
        operation,
        request_digest: session.check.token.request_digest,
        actor: session.check.actor.clone(),
        lease_ms: DEFAULT_LEASE_MS,
    };
    assert!(session.lease.base.catalog.is_none());
    let before = state(&f.handle).await?;
    if fault == 0 {
        let input = session
            .root_outcome_completion(store, root, budget.clone(), None)
            .await?;
        assert_eq!(input.outcomes.ref_generation, 0);
        let mut e = BoundedEncoder::new(ROOT_COMPLETION_BYTES)?;
        input.encode(&mut e)?;
        let bytes = e.finish();
        assert!(bytes.len() < 2048);
        let mut d = BoundedDecoder::new(&bytes, ROOT_COMPLETION_BYTES)?;
        assert_eq!(RootOutcomeCompletion::decode(&mut d)?, input);
        d.finish()?;
        assert!(
            RootPushCompletion::decode(&mut BoundedDecoder::new(&bytes, ROOT_COMPLETION_BYTES)?)
                .is_err()
        );
        let mut changed = input.clone();
        changed.input_checkpoint_digest[0] ^= 1;
        assert!(
            changed
                .encode(&mut BoundedEncoder::new(ROOT_COMPLETION_BYTES)?)
                .is_err()
        );
        let mut changed = input.clone();
        changed.refusal = true;
        assert!(
            changed
                .encode(&mut BoundedEncoder::new(ROOT_COMPLETION_BYTES)?)
                .is_err()
        );
        let mut changed = input.clone();
        changed.outcomes.ref_generation = 1;
        assert!(
            changed
                .encode(&mut BoundedEncoder::new(ROOT_COMPLETION_BYTES)?)
                .is_err()
        );
        let mut e = BoundedEncoder::new(CERTIFICATE_BYTES)?;
        input.proof.encode(&mut e)?;
        let mut bytes = e.finish();
        *bytes.last_mut().ok_or("certificate MAC")? ^= 1;
        let mut changed = input.clone();
        let mut d = BoundedDecoder::new(&bytes, CERTIFICATE_BYTES)?;
        changed.proof = OutcomeCertificate::decode(&mut d)?;
        d.finish()?;
        let error = f
            .client()
            .command::<CompleteRootOutcome>(&f.target, identity()?, changed)
            .await
            .unwrap_err();
        assert!(
            matches!(error, InvocationError::Rejected(ref r) if r.output == RootCompletionReply::Denied(PreparationDenial::Unauthorized))
        );
        assert_eq!(state(&f.handle).await?, before);
        let inline = CatalogPushCompletion {
            proof: CompletionCatalogProof::OutcomeOnly(input.proof.clone()),
            response_id: input.outcomes.response_id,
            response: expected.clone(),
            options: Vec::new(),
            signed: None,
        };
        let error = f
            .client()
            .command::<CompleteCatalogPush>(&f.target, identity()?, inline)
            .await
            .unwrap_err();
        assert!(
            matches!(error, InvocationError::Rejected(ref r) if r.output == CatalogCompletionReply::Denied(PreparationDenial::Unauthorized))
        );
        assert_eq!(state(&f.handle).await?, before);
        for trigger in [
            "BEFORE UPDATE OF response_root ON pushes",
            "BEFORE DELETE ON catalog_operations",
        ] {
            edit(f, &format!("CREATE TRIGGER root_outcome_fault {trigger} BEGIN SELECT RAISE(ABORT,'root outcome late fault'); END;")).await?;
            assert!(
                f.client()
                    .command::<CompleteRootOutcome>(&f.target, identity()?, input.clone())
                    .await
                    .is_err()
            );
            assert_eq!(state(&f.handle).await?, before);
            edit(f, "DROP TRIGGER root_outcome_fault").await?;
        }
    }
    let ready = session
        .ready_root_outcome(identity()?, store, root, budget, None)
        .await?;
    let evidence = ready.evidence_for_test();
    let p = PublicationCoordinator::new(f.target.clone(), PublicationLimits::default())?;
    p.fault_for_test(fault);
    let observer = ticket.publish(&p, ready)?;
    drop(observer);
    let original = if fault != 0 {
        let StagingState::Uncertain(error) = terminal(ticket, true).await? else {
            return Err("ref-free root uncertainty lost".into());
        };
        let StagingError::Publication(error) = &*error else {
            return Err("ref-free uncertainty kind".into());
        };
        let PublicationError::RootPush(InvocationError::Pending(actual)) = &**error else {
            return Err("ref-free exact command evidence lost".into());
        };
        assert_eq!(actual.as_ref(), &evidence);
        assert_eq!(p.stats().await.command_bytes, 16 << 10);
        let known = match f.client().resolve(&evidence).await? {
            Resolution::Absent => None,
            Resolution::Committed(value) => {
                Some((value.result().to_vec(), value.commit_sequence()))
            }
            other => return Err(format!("unexpected ref-free resolution {other:?}").into()),
        };
        assert_eq!(known.is_some(), fault != 1);
        if revoked {
            edit(
                f,
                "UPDATE repository_identity SET owner='replacement' WHERE singleton=1",
            )
            .await?;
        }
        assert_eq!(staging.close_and_drain().await.len(), 1);
        assert_eq!(p.close_and_drain().await.len(), 1);
        let before = state(&f.handle).await?;
        p.pending(operation)
            .await
            .ok_or("retained root outcome lost")?
            .recover()
            .await?;
        assert!(matches!(
            terminal(ticket, false).await?,
            StagingState::Published(_)
        ));
        if known.is_some() {
            assert_eq!(state(&f.handle).await?, before);
        }
        known
    } else {
        None
    };
    let observer = ticket
        .pending_publication()
        .ok_or("canceled root outcome observer lost")?;
    let PublicationState::Finished(Ok(PublicationOutcome::RootPush(committed))) =
        observer.wait().await
    else {
        return Err("ref-free root outcome not completed".into());
    };
    if let Some((bytes, sequence)) = original {
        let mut e = BoundedEncoder::new(512)?;
        committed.output.encode(&mut e)?;
        assert_eq!(e.finish(), bytes);
        assert_eq!(committed.receipt.commit_sequence, sequence);
    }
    let RootCompletionReply::Completed(value) = &committed.output else {
        return Err("ref-free result denied".into());
    };
    let rejected = fault == 1 && revoked;
    assert_eq!(value.completion.rejected, rejected);
    assert!(value.completion.publication.is_none());
    if revoked {
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
    let actual = response(observer.root_response(store).await?).await?;
    if rejected {
        assert_eq!(actual.status, 409);
        assert_eq!(
            actual.body,
            format!("{}\n", crate::push::report::REJECTED).as_bytes()
        );
    } else {
        assert_eq!(actual, expected);
    }
    if kind == Kind::Empty && !rejected {
        assert!(!actual.body.windows(3).any(|p| p == b"ok "));
    }
    roots_unchanged(&before, &state(&f.handle).await?)?;
    assert_eq!(f.counts().await?, (0, 1));
    assert_eq!(
        f.client()
            .query::<CheckCompletedRootPush>(&f.target, Some(committed.receipt), lookup)
            .await?
            .output,
        Some(committed.output)
    );
    f.handle
        .query(0, 1024, |db| {
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
                db.query_row("SELECT count(*) FROM pushes WHERE options!='[]'", [], |r| r
                    .get::<_, u64>(0))?,
                0
            );
            assert_eq!(
                db.query_row(
                    "SELECT generation FROM catalog_state WHERE singleton=1",
                    [],
                    |r| r.get::<_, u64>(0)
                )?,
                0
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
