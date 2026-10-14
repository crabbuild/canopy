//! Indexed current-root observation and exact eviction admission, with real receipts.
use super::*;
use cellule_runtime::Resolution;

async fn selection(f: &Fixture, actor: Option<&str>) -> Result<Option<GenerationFact>> {
    Ok(f.client()
        .query::<SelectServingGeneration>(
            &f.target,
            None,
            ServingSelection {
                repository: f.repository,
                actor: actor.map(str::to_owned),
            },
        )
        .await?
        .output)
}
fn queue(f: &Fixture) -> Result<PublicationCoordinator> {
    Ok(PublicationCoordinator::new(
        f.target.clone(),
        PublicationLimits::default(),
        f.publication_budget.clone(),
    )?)
}
async fn pin(
    f: &Fixture,
    store: Arc<ArtifactStore>,
    root: &tempfile::TempDir,
    tasks: TaskTracker,
    reader: u8,
) -> Result<ServingPin> {
    let (lease, _, _) = acquire(f, Some("owner"), reader, DEFAULT_LEASE_MS).await?;
    Ok(ServingPin::open(
        context(f, store, root, tasks)?,
        lease.token,
        Some("owner".into()),
    )
    .await?)
}
async fn acquisition(f: &Fixture, operation: u8) -> Result<ReadyServingCommand> {
    Ok(ReadyServingCommand::acquire(
        f.client(),
        f.target.clone(),
        f.begin([operation; 16]),
        identity()?,
        f.authority(),
    )
    .await?)
}
async fn close_drained(gate: &ServingDrainAdmission) -> Result {
    timeout(Duration::from_secs(5), async {
        while !gate.close_if_drained().await {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    Ok(())
}
async fn release_result(ticket: &PublicationTicket) -> Result<Committed<ServingReleaseReply>> {
    match timeout(Duration::from_secs(5), ticket.wait()).await? {
        PublicationState::Finished(Ok(PublicationOutcome::ServingRelease(value))) => Ok(value),
        state => Err(format!("release state {state:?}").into()),
    }
}

#[tokio::test]
async fn selection_requires_current_read_joint_initialization_and_exact_repository_identity()
-> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let f = Fixture::new(format).await?;
        assert!(selection(&f, Some("owner")).await?.is_none());
        let store = Arc::new(ArtifactStore::new(Arc::new(InMemory::new()), f.repository));
        let fact = initialize(&f, store).await?;
        edit(
            &f,
            "INSERT INTO repository_members(account,role) VALUES('viewer','read')",
        )
        .await?;
        let before = f.counts().await?;
        assert_eq!(selection(&f, Some("viewer")).await?, Some(fact));
        assert_eq!(selection(&f, Some("owner")).await?, Some(fact));
        assert!(selection(&f, Some("other")).await?.is_none());
        assert!(selection(&f, None).await?.is_none());
        let wrong = ServingSelection {
            repository: *uuid::Uuid::new_v4().as_bytes(),
            actor: Some("owner".into()),
        };
        assert!(
            f.client()
                .query::<SelectServingGeneration>(&f.target, None, wrong)
                .await?
                .output
                .is_none()
        );
        edit(&f, "UPDATE ref_generation SET visibility='public'").await?;
        assert_eq!(selection(&f, None).await?, Some(fact));
        edit(&f, "UPDATE ref_generation SET visibility='private'; DELETE FROM repository_members WHERE account='viewer'").await?;
        assert!(selection(&f, None).await?.is_none());
        assert!(selection(&f, Some("viewer")).await?.is_none());
        assert_eq!(f.counts().await?, before);
        assert_eq!(pin_count(&f).await?, 0);
        f.runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn selection_follows_current_joint_head_while_old_pin_remains_immutable() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let f = Fixture::new(format).await?;
        let store = Arc::new(ArtifactStore::new(Arc::new(InMemory::new()), f.repository));
        let initial = initialize(&f, store.clone()).await?;
        let root = tempfile::TempDir::new()?;
        let tasks = TaskTracker::new();
        let old = pin(&f, store, &root, tasks.clone(), 201).await?;
        // Trusted copied roots qualify head selection, not native publication.
        edit(&f, "INSERT INTO catalog_generations(generation,catalog,certificate,refs) SELECT 2,catalog,certificate,refs FROM catalog_generations WHERE generation=1; UPDATE catalog_state SET generation=2 WHERE singleton=1").await?;
        let current = selection(&f, Some("owner"))
            .await?
            .ok_or("current root missing")?;
        assert_eq!(current.generation, 2);
        assert_eq!(
            (current.catalog, current.refs, current.certificate),
            (initial.catalog, initial.refs, initial.certificate)
        );
        assert_eq!(old.fact(), initial);
        assert_eq!(
            f.client()
                .query::<CheckServingPin>(
                    &f.target,
                    None,
                    ServingCheck {
                        token: old.token(),
                        actor: Some("owner".into()),
                    }
                )
                .await?
                .output
                .ok_or("old retention lost")?
                .fact,
            initial
        );
        f.handle.query(0,4096,|db| {
            let mut query=db.prepare("EXPLAIN QUERY PLAN SELECT g.generation,g.catalog,g.certificate,g.refs FROM catalog_state s JOIN catalog_generations g ON g.generation=s.generation WHERE s.singleton=1")?;
            let plan:Vec<String>=query.query_map([],|r|r.get(3))?.collect::<std::result::Result<_,_>>()?;
            assert!(plan.iter().all(|line| !line.contains("SCAN") && !line.contains("TEMP B-TREE")),"{plan:?}");
            Ok(Vec::new())
        }).await?;
        release(&f, &old).await?;
        tasks.close();
        tasks.wait().await;
        f.runtime.shutdown().await?;
    }
    Ok(())
}

#[test]
fn selection_codec_is_bounded_and_does_not_carry_a_pin() -> Result {
    let repository = *uuid::Uuid::new_v4().as_bytes();
    for actor in [None, Some("viewer".into())] {
        let input = ServingSelection { repository, actor };
        let mut encoder = BoundedEncoder::new(1024)?;
        input.encode(&mut encoder)?;
        let bytes = encoder.finish();
        let mut decoder = BoundedDecoder::new(&bytes, 1024)?;
        assert_eq!(ServingSelection::decode(&mut decoder)?, input);
        decoder.finish()?;
        for end in 0..bytes.len() {
            assert!(
                (|| -> std::result::Result<(), CodecError> {
                    let mut decoder = BoundedDecoder::new(&bytes[..end], 1024)?;
                    ServingSelection::decode(&mut decoder)?;
                    decoder.finish()
                })()
                .is_err()
            );
        }
        let mut trailing = bytes;
        trailing.push(0);
        let mut decoder = BoundedDecoder::new(&trailing, 1024)?;
        ServingSelection::decode(&mut decoder)?;
        assert!(decoder.finish().is_err());
    }
    assert!(
        ServingSelection {
            repository: [0; 16],
            actor: None
        }
        .encode(&mut BoundedEncoder::new(1024)?)
        .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn eviction_admits_only_exact_selected_releases_and_closes_after_all_real_successes() -> Result
{
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let f = Fixture::new(format).await?;
        let store = Arc::new(ArtifactStore::new(Arc::new(InMemory::new()), f.repository));
        initialize(&f, store.clone()).await?;
        let root = tempfile::TempDir::new()?;
        let tasks = TaskTracker::new();
        let first = pin(&f, store.clone(), &root, tasks.clone(), 202).await?;
        let second = pin(&f, store.clone(), &root, tasks.clone(), 203).await?;
        let foreign = pin(&f, store, &root, tasks.clone(), 204).await?;
        let q = queue(&f)?;
        let gate = q
            .reserve_serving_drain(&[first.token(), second.token()])
            .await?
            .ok_or("idle drain refused")?;
        assert!(!gate.close_if_drained().await);
        assert!(!q.close_if_idle().await);
        assert!(q.reserve_serving_drain(&[]).await?.is_none());
        let before = f.counts().await?;
        let denied = q
            .try_reserve(acquisition(&f, 202).await?)
            .err()
            .ok_or("acquisition admitted during drain")?;
        assert_eq!(denied.reason, PublicationScheduleError::Closed);
        assert_eq!(f.counts().await?, before);
        let denied_release = q
            .try_reserve(foreign.ready_release(identity()?).await?)
            .err()
            .ok_or("foreign release admitted during drain")?;
        assert_eq!(denied_release.reason, PublicationScheduleError::Closed);
        assert_eq!(pin_count(&f).await?, 3);
        let held = q.try_reserve(first.ready_release(identity()?).await?)?;
        assert!(!gate.close_if_drained().await);
        held.activate().await?;
        assert_eq!(
            release_result(&held).await?.output,
            ServingReleaseReply::Released
        );
        assert!(!gate.close_if_drained().await);
        let held = q.try_reserve(second.ready_release(identity()?).await?)?;
        held.activate().await?;
        assert_eq!(
            release_result(&held).await?.output,
            ServingReleaseReply::Released
        );
        close_drained(&gate).await?;
        drop(gate);
        assert!(q.stats().await.closed);
        assert_eq!(
            q.try_reserve(denied.ready)
                .err()
                .ok_or("closed admission reopened")?
                .reason,
            PublicationScheduleError::Closed
        );
        assert!(q.close_and_drain().await.is_empty());
        release(&f, &foreign).await?;
        assert_eq!(pin_count(&f).await?, 0);
        tasks.close();
        tasks.wait().await;
        f.runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn busy_or_invalid_drain_never_changes_existing_admission_or_exact_evidence() -> Result {
    let f = Fixture::new(ObjectFormat::Sha256).await?;
    let q = queue(&f)?;
    let held = q.try_reserve(acquisition(&f, 205).await?)?;
    assert!(q.reserve_serving_drain(&[]).await?.is_none());
    assert!(!q.stats().await.closed);
    held.discard_held().await?;
    let store = Arc::new(ArtifactStore::new(Arc::new(InMemory::new()), f.repository));
    initialize(&f, store.clone()).await?;
    let root = tempfile::TempDir::new()?;
    let tasks = TaskTracker::new();
    let retained = pin(&f, store.clone(), &root, tasks.clone(), 206).await?;
    for tokens in [vec![retained.token(); 2], vec![retained.token(); 17]] {
        assert!(matches!(
            q.reserve_serving_drain(&tokens).await,
            Err(PublicationScheduleError::InvalidLimits)
        ));
    }
    let mut malformed = retained.token();
    malformed.admission_sequence = 0;
    assert!(matches!(
        q.reserve_serving_drain(&[malformed]).await,
        Err(PublicationScheduleError::InvalidLimits)
    ));
    let mut foreign = retained.token();
    foreign.repository = *uuid::Uuid::new_v4().as_bytes();
    assert!(matches!(
        q.reserve_serving_drain(&[foreign]).await,
        Err(PublicationScheduleError::Foreign)
    ));
    let ordinary = acquisition(&f, 207).await?;
    let original = ordinary.evidence().clone();
    let gate = q
        .reserve_serving_drain(&[])
        .await?
        .ok_or("idle admission")?;
    let failure = q
        .try_reserve(ordinary)
        .err()
        .ok_or("acquisition admitted during drain")?;
    assert_eq!(failure.reason, PublicationScheduleError::Closed);
    assert!(matches!(
        f.client().resolve(&original).await?,
        Resolution::Absent
    ));
    drop(gate);
    let ticket = q.try_reserve(failure.ready)?;
    ticket.activate().await?;
    let state = timeout(Duration::from_secs(5), ticket.wait()).await?;
    let PublicationState::Finished(Ok(PublicationOutcome::ServingCommand(value))) = state else {
        return Err(format!("acquisition state {state:?}").into());
    };
    let lease = granted(value.output)?;
    let resumed = ServingPin::open(
        context(&f, store, &root, tasks.clone())?,
        lease.token,
        Some("owner".into()),
    )
    .await?;
    assert!(matches!(
        f.client().resolve(&original).await?,
        Resolution::Committed(_)
    ));
    assert!(!q.stats().await.closed);
    release(&f, &retained).await?;
    release(&f, &resumed).await?;
    assert!(q.close_and_drain().await.is_empty());
    tasks.close();
    tasks.wait().await;
    f.runtime.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn release_uncertainty_and_observer_loss_cannot_complete_eviction_early() -> Result {
    for fault in 1..=3 {
        let f = Fixture::new(ObjectFormat::Sha1).await?;
        let store = Arc::new(ArtifactStore::new(Arc::new(InMemory::new()), f.repository));
        initialize(&f, store.clone()).await?;
        let root = tempfile::TempDir::new()?;
        let tasks = TaskTracker::new();
        let retained = pin(&f, store, &root, tasks.clone(), 208).await?;
        let q = queue(&f)?;
        let gate = q
            .reserve_serving_drain(&[retained.token()])
            .await?
            .ok_or("idle drain")?;
        let original = retained.ready_release(identity()?).await?;
        let evidence = original.evidence().clone();
        q.fault_for_test(fault);
        let ticket = q.submit(original).await?;
        assert!(matches!(
            timeout(Duration::from_secs(5), ticket.wait()).await?,
            PublicationState::Uncertain(_)
        ));
        assert!(!gate.close_if_drained().await);
        assert_eq!(q.stats().await.command_bytes, 8 << 10);
        drop(ticket);
        let ticket = q
            .pending_serving_release([208; 16])
            .await
            .ok_or("release owner lost")?;
        ticket.recover().await?;
        assert_eq!(
            release_result(&ticket).await?.output,
            ServingReleaseReply::Released
        );
        assert!(matches!(
            f.client().resolve(&evidence).await?,
            Resolution::Committed(_)
        ));
        close_drained(&gate).await?;
        assert_eq!(q.stats().await.command_bytes, 0);
        drop(gate);
        assert!(q.close_and_drain().await.is_empty());
        tasks.close();
        tasks.wait().await;
        f.runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn a_denied_release_keeps_its_sql_root_and_cannot_satisfy_the_drain_guard() -> Result {
    let f = Fixture::new(ObjectFormat::Sha256).await?;
    let store = Arc::new(ArtifactStore::new(Arc::new(InMemory::new()), f.repository));
    initialize(&f, store.clone()).await?;
    let root = tempfile::TempDir::new()?;
    let tasks = TaskTracker::new();
    let retained = pin(&f, store, &root, tasks.clone(), 209).await?;
    let q = queue(&f)?;
    let gate = q
        .reserve_serving_drain(&[retained.token()])
        .await?
        .ok_or("idle drain")?;
    let ready = retained.ready_release(identity()?).await?;
    edit(&f, "UPDATE repository_identity SET owner='other'").await?;
    let state = timeout(Duration::from_secs(5), q.submit(ready).await?.wait()).await?;
    assert!(
        matches!(state,PublicationState::Finished(Err(ref error)) if matches!(&**error,
        PublicationError::ServingRelease(InvocationError::Rejected(value)) if value.output==ServingReleaseReply::Denied(ServingDenial::Unauthorized)))
    );
    assert_eq!(pin_count(&f).await?, 1);
    assert!(!gate.close_if_drained().await);
    assert!(!q.close_if_idle().await);
    drop(gate);
    edit(&f, "UPDATE repository_identity SET owner='owner'").await?;
    release(&f, &retained).await?;
    assert!(q.close_and_drain().await.is_empty());
    tasks.close();
    tasks.wait().await;
    f.runtime.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn global_close_waits_for_drain_owner_and_cancellation_never_reopens_closed_admission()
-> Result {
    let f = Fixture::new(ObjectFormat::Sha256).await?;
    let q = queue(&f)?;
    let gate = q.reserve_serving_drain(&[]).await?.ok_or("idle drain")?;
    let closing = q.clone();
    let waiter = tokio::spawn(async move { closing.close_and_drain().await });
    tokio::task::yield_now().await;
    assert!(!waiter.is_finished());
    waiter.abort();
    assert!(
        waiter
            .await
            .err()
            .ok_or("global close completed before drain")?
            .is_cancelled()
    );
    assert!(!q.stats().await.closed);
    assert!(gate.close_if_drained().await);
    drop(gate);
    assert!(q.stats().await.closed);
    assert!(q.close_and_drain().await.is_empty());
    assert!(q.reserve_serving_drain(&[]).await?.is_none());

    let q = queue(&f)?;
    let gate = q
        .reserve_serving_drain(&[])
        .await?
        .ok_or("new idle drain")?;
    let closing = q.clone();
    let waiter = tokio::spawn(async move { closing.close_and_drain().await });
    tokio::task::yield_now().await;
    assert!(!waiter.is_finished());
    drop(gate);
    assert!(timeout(Duration::from_secs(5), waiter).await??.is_empty());
    assert!(q.stats().await.closed);
    f.runtime.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn reused_reader_id_cannot_release_another_exact_drain_token() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let f = Fixture::new(format).await?;
        let store = Arc::new(ArtifactStore::new(Arc::new(InMemory::new()), f.repository));
        initialize(&f, store.clone()).await?;
        let root = tempfile::TempDir::new()?;
        let tasks = TaskTracker::new();
        let old = pin(&f, store.clone(), &root, tasks.clone(), 210).await?;
        let old_token = old.token();
        release(&f, &old).await?;
        let current = pin(&f, store, &root, tasks.clone(), 210).await?;
        assert_eq!(old_token.reader, current.token().reader);
        assert_ne!(
            old_token.admission_sequence,
            current.token().admission_sequence
        );
        let q = queue(&f)?;
        let gate = q
            .reserve_serving_drain(&[old_token])
            .await?
            .ok_or("idle drain")?;
        let ready = current.ready_release(identity()?).await?;
        let original = ready.evidence().clone();
        let refused = q
            .try_reserve(ready)
            .err()
            .ok_or("different token admitted")?;
        assert_eq!(refused.reason, PublicationScheduleError::Closed);
        assert!(matches!(
            f.client().resolve(&original).await?,
            Resolution::Absent
        ));
        assert_eq!(pin_count(&f).await?, 1);
        assert!(!gate.close_if_drained().await);
        assert_eq!(q.stats().await.command_bytes, 0);
        drop(gate);
        let ticket = q.submit(refused.ready).await?;
        assert_eq!(
            release_result(&ticket).await?.output,
            ServingReleaseReply::Released
        );
        assert!(matches!(
            f.client().resolve(&original).await?,
            Resolution::Committed(_)
        ));
        assert!(q.close_and_drain().await.is_empty());
        tasks.close();
        tasks.wait().await;
        f.runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn global_close_preserves_release_admission_through_real_dispatch_and_uncertainty() -> Result
{
    for fault in 1..=3 {
        let f = Fixture::new(ObjectFormat::Sha256).await?;
        let store = Arc::new(ArtifactStore::new(Arc::new(InMemory::new()), f.repository));
        initialize(&f, store.clone()).await?;
        let root = tempfile::TempDir::new()?;
        let tasks = TaskTracker::new();
        let retained = pin(&f, store, &root, tasks.clone(), 211).await?;
        let q = queue(&f)?;
        let gate = q
            .reserve_serving_drain(&[retained.token()])
            .await?
            .ok_or("idle drain")?;
        let closing = q.clone();
        let waiter = tokio::spawn(async move { closing.close_and_drain().await });
        tokio::task::yield_now().await;
        assert!(!waiter.is_finished());
        assert!(!q.stats().await.closed);
        let (dispatch, entered) = q.pause_for_test().await;
        q.fault_for_test(fault);
        let ticket = q.submit(retained.ready_release(identity()?).await?).await?;
        timeout(Duration::from_secs(5), entered).await??;
        assert!(!waiter.is_finished());
        assert!(!gate.close_if_drained().await);
        assert_eq!(pin_count(&f).await?, 1);
        dispatch
            .send(())
            .map_err(|_| "release worker disappeared")?;
        assert!(matches!(
            timeout(Duration::from_secs(5), ticket.wait()).await?,
            PublicationState::Uncertain(_)
        ));
        assert!(!waiter.is_finished());
        assert!(!gate.close_if_drained().await);
        ticket.recover().await?;
        assert_eq!(
            release_result(&ticket).await?.output,
            ServingReleaseReply::Released
        );
        assert!(!waiter.is_finished());
        close_drained(&gate).await?;
        assert!(timeout(Duration::from_secs(5), waiter).await??.is_empty());
        drop(gate);
        assert!(q.stats().await.closed);
        assert_eq!(pin_count(&f).await?, 0);
        tasks.close();
        tasks.wait().await;
        f.runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn dropping_admission_guard_preserves_dispatched_original_and_its_recovery_credits() -> Result
{
    let f = Fixture::new(ObjectFormat::Sha1).await?;
    let store = Arc::new(ArtifactStore::new(Arc::new(InMemory::new()), f.repository));
    initialize(&f, store.clone()).await?;
    let root = tempfile::TempDir::new()?;
    let tasks = TaskTracker::new();
    let retained = pin(&f, store, &root, tasks.clone(), 212).await?;
    let q = queue(&f)?;
    let gate = q
        .reserve_serving_drain(&[retained.token()])
        .await?
        .ok_or("idle drain")?;
    let original = retained.ready_release(identity()?).await?;
    let evidence = original.evidence().clone();
    let (dispatch, entered) = q.pause_for_test().await;
    q.fault_for_test(3);
    let ticket = q.submit(original).await?;
    timeout(Duration::from_secs(5), entered).await??;
    drop(gate);
    drop(ticket);
    assert_eq!(q.stats().await.command_bytes, 8 << 10);
    assert_eq!(pin_count(&f).await?, 1);
    assert!(!q.stats().await.closed);
    // Guard cancellation resumes admission; it cannot cancel admitted work.
    let held = q.try_reserve(acquisition(&f, 213).await?)?;
    assert_eq!(q.stats().await.command_bytes, (8 + 28) << 10);
    dispatch
        .send(())
        .map_err(|_| "release worker disappeared")?;
    let ticket = q
        .pending_serving_release([212; 16])
        .await
        .ok_or("original lost")?;
    assert!(matches!(
        timeout(Duration::from_secs(5), ticket.wait()).await?,
        PublicationState::Uncertain(_)
    ));
    held.discard_held().await?;
    assert_eq!(q.stats().await.command_bytes, 8 << 10);
    ticket.recover().await?;
    assert_eq!(
        release_result(&ticket).await?.output,
        ServingReleaseReply::Released
    );
    assert!(matches!(
        f.client().resolve(&evidence).await?,
        Resolution::Committed(_)
    ));
    assert_eq!(pin_count(&f).await?, 0);
    assert_eq!(q.stats().await.command_bytes, 0);
    assert!(q.close_and_drain().await.is_empty());
    tasks.close();
    tasks.wait().await;
    f.runtime.shutdown().await?;
    Ok(())
}
