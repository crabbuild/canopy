use super::*;
use tokio::time::{Duration, timeout};

async fn opened(fixture: &Fixture, operation: [u8; 16]) -> Result<Arc<PreparationSession>> {
    let started = fixture
        .client()
        .command::<BeginPreparation>(&fixture.target, identity()?, fixture.begin(operation))
        .await?;
    let token = lease(started.output)?.token;
    Ok(Arc::new(
        PreparationSession::open(
            fixture.client(),
            fixture.target.clone(),
            check(token),
            Some(started.receipt),
        )
        .await?,
    ))
}
fn native(status: u16, signed: bool, session: &PreparationSession) -> PushCompletionRequest {
    let mut body = Vec::new();
    packet(&mut body, b"unpack ok\n");
    packet(&mut body, b"ng refs/heads/main hook declined\n");
    body.extend_from_slice(b"0000");
    PushCompletionRequest {
        plan: None,
        response: GitHttpResponse {
            status,
            headers: vec![("X-Native-Trace".into(), "exact".into())],
            body,
        },
        options: vec!["canopy.note=refused".into()],
        certificate: signed.then(|| VerifiedPushCertificate {
            target: session.target.clone(),
            request_digest: session.check.token.request_digest,
            signer: session.check.actor.clone(),
            key: "native-verified".into(),
            body: b"verified native signature bytes".to_vec(),
        }),
    }
}

#[tokio::test]
async fn refusals_and_noop_outcomes_need_no_catalog_artifacts_or_generation_increment() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let fixture = Fixture::new(format).await?;
        for (i, status) in [200, 503, 204].into_iter().enumerate() {
            let operation = [130 + i as u8; 16];
            let session = opened(&fixture, operation).await?;
            let mut request = native(status, i == 0, &session);
            if status == 204 {
                request.response.body.clear();
                request.options.clear();
            }
            let expected = request.response.clone();
            let before = state(&fixture.handle).await?;
            let input = session.push_outcome(request).await?;
            assert_eq!(state(&fixture.handle).await?, before);
            let mut encoder = BoundedEncoder::new(4 << 20)?;
            input.encode(&mut encoder)?;
            let wire = encoder.finish();
            let mut decoder = BoundedDecoder::new(&wire, 4 << 20)?;
            let input = CatalogPushCompletion::decode(&mut decoder)?;
            decoder.finish()?;
            let CompletionCatalogProof::OutcomeOnly(ref proof) = input.proof else {
                return Err("wrong purpose".into());
            };
            let mut bounded = BoundedEncoder::new(CERTIFICATE_BYTES)?;
            proof.encode(&mut bounded)?;
            let mutation = identity()?;
            let first = fixture
                .client()
                .command::<CompleteCatalogPush>(&fixture.target, mutation, input.clone())
                .await?;
            let result = completed(first.output)?;
            assert_eq!(result.publication, None);
            assert!(!result.rejected);
            assert_eq!(session.completed_push_response(&first).await?, expected);
            assert_eq!(state(&fixture.handle).await?, before);
            let exact = fixture
                .client()
                .command::<CompleteCatalogPush>(&fixture.target, mutation, input.clone())
                .await?;
            assert_eq!(exact.output, first.output);
            assert_eq!(exact.receipt, first.receipt);
            let logical = fixture
                .client()
                .command::<CompleteCatalogPush>(&fixture.target, identity()?, input)
                .await?;
            assert_eq!(logical.output, first.output);
            assert_eq!(
                replay_push_response(
                    &fixture.client(),
                    &fixture.target,
                    fixture.begin(operation),
                    None
                )
                .await?,
                Some(expected)
            );
        }
        assert_eq!(fixture.counts().await?, (0, 3));
        fixture.runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn outcome_on_unavailable_old_catalog_survives_moving_frontier_without_loading_it() -> Result
{
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let fixture = Fixture::new(format).await?;
        // The trusted fixture publishes a descriptor whose temporary artifact
        // store is already gone. The session API has no loader/provider input.
        fixture.install_empty_root(1).await?;
        let session = opened(&fixture, [134; 16]).await?;
        assert_eq!(session.lease.base.generation, 1);
        let request = native(200, false, &session);
        let expected = request.response.clone();
        let input = session.push_outcome(request).await?;
        fixture.install_empty_root(2).await?;
        let before = state(&fixture.handle).await?;
        let committed = fixture
            .client()
            .command::<CompleteCatalogPush>(&fixture.target, identity()?, input)
            .await?;
        assert_eq!(completed(committed.output)?.publication, None);
        assert_eq!(state(&fixture.handle).await?, before);
        assert_eq!(session.completed_push_response(&committed).await?, expected);
        fixture.runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn outcome_cannot_claim_refs_cross_purpose_or_edit_authenticated_native_bytes() -> Result {
    let fixture = Fixture::new(ObjectFormat::Sha256).await?;
    let session = opened(&fixture, [135; 16]).await?;
    let input = session.push_outcome(native(200, true, &session)).await?;
    for variant in 0..8 {
        let mut bad = input.clone();
        match variant {
            0 => bad.response_id[0] ^= 1,
            1 => bad.response.status = 503,
            2 => bad
                .response
                .headers
                .push(("X-Changed".into(), "yes".into())),
            3 => bad.response.body.push(b'x'),
            4 => bad.options.push("canopy.note=changed".into()),
            5 => bad.signed.as_mut().unwrap().key.push('x'),
            6 => bad.signed.as_mut().unwrap().body[0] ^= 1,
            _ => bad.signed = None,
        }
        reject(&fixture, bad, PreparationDenial::Unauthorized).await?;
    }
    let mut request = native(200, false, &session);
    request.plan = Some(plan(Vec::new()));
    assert!(session.push_outcome(request).await.is_err());
    let mut request = native(200, false, &session);
    let mut success = Vec::new();
    packet(&mut success, b"unpack ok\n");
    packet(&mut success, b"ok refs/heads/main\n");
    success.extend_from_slice(b"0000");
    request.response.body = success;
    assert!(session.push_outcome(request).await.is_err());
    let CompletionCatalogProof::OutcomeOnly(outcome) = input.proof else {
        return Err("purpose".into());
    };
    let graph = assembled(&fixture, [136; 16], 0).await?;
    let real = graph.prepared.certificate().await?;
    let before = state(&fixture.handle).await?;
    // Reusing the same envelope does not make the two purposes interchangeable.
    let fake = CatalogCertificate(outcome.0.clone());
    assert!(
        fake.encode(&mut BoundedEncoder::new(CERTIFICATE_BYTES)?)
            .is_err()
    );
    let fake = OutcomeCertificate(real.0);
    assert!(
        fake.encode(&mut BoundedEncoder::new(CERTIFICATE_BYTES)?)
            .is_err()
    );
    assert_eq!(state(&fixture.handle).await?, before);
    drop(graph.prepared);
    cleaned(graph.root.path(), &graph.budget).await?;
    fixture.runtime.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn outcome_checks_current_write_authority_expiry_and_reconnect_read_authority() -> Result {
    for expired in [false, true] {
        let fixture = Fixture::new(ObjectFormat::Sha1).await?;
        let session = opened(&fixture, [137; 16]).await?;
        let input = session.push_outcome(native(200, false, &session)).await?;
        if expired {
            edit(&fixture, "UPDATE catalog_operations SET expires_at_ms=0; UPDATE catalog_leases SET expires_at_ms=0;").await?;
            reject(&fixture, input, PreparationDenial::Expired).await?;
        } else {
            edit(
                &fixture,
                "UPDATE repository_identity SET owner='successor';",
            )
            .await?;
            reject(&fixture, input.clone(), PreparationDenial::Unauthorized).await?;
            assert!(
                session
                    .push_outcome(native(200, false, &session))
                    .await
                    .is_err()
            );
            edit(
                &fixture,
                "INSERT INTO repository_members VALUES('owner','write');",
            )
            .await?;
            let committed = fixture
                .client()
                .command::<CompleteCatalogPush>(&fixture.target, identity()?, input)
                .await?;
            assert_eq!(completed(committed.output)?.publication, None);
            edit(
                &fixture,
                "DELETE FROM repository_members WHERE account='owner';",
            )
            .await?;
            assert!(matches!(
                replay_push_response(
                    &fixture.client(),
                    &fixture.target,
                    fixture.begin([137; 16]),
                    None
                )
                .await,
                Err(CatalogPushResponseError::Denied(
                    PreparationDenial::Unauthorized
                ))
            ));
        }
        fixture.runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn outcome_dispatch_retains_session_and_exact_command_after_observer_cancel_or_lost_reply()
-> Result {
    for fault in [0, 1, 2, 3] {
        let fixture = Fixture::new(ObjectFormat::Sha256).await?;
        let session = opened(&fixture, [140 + fault; 16]).await?;
        let weak = Arc::downgrade(&session);
        let expected = native(200, false, &session).response;
        let ready = session
            .ready_outcome(identity()?, native(200, false, &session))
            .await?;
        let coordinator =
            PublicationCoordinator::new(fixture.target.clone(), PublicationLimits::default())?;
        let (release, entered) = coordinator.pause_for_test().await;
        coordinator.fault_for_test(fault);
        let ticket = coordinator.submit(ready).await?;
        timeout(Duration::from_secs(5), entered).await??;
        let lookup = fixture.begin(session.check.token.operation);
        drop(session);
        assert!(weak.upgrade().is_some());
        drop(ticket);
        release.send(()).map_err(|_| "dispatch disappeared")?;
        let pending = timeout(Duration::from_secs(10), coordinator.close_and_drain()).await?;
        if fault == 0 {
            assert!(pending.is_empty());
        } else {
            assert_eq!(pending.len(), 1);
            assert!(weak.upgrade().is_some());
            coordinator.recover(&pending[0]).await?;
            assert!(matches!(
                timeout(Duration::from_secs(10), pending[0].wait()).await?,
                PublicationState::Finished(Ok(PublicationOutcome::Push(_)))
            ));
        }
        assert!(weak.upgrade().is_none());
        assert_eq!(coordinator.reservations_for_test().await, (0, 0, 0));
        assert_eq!(
            replay_push_response(&fixture.client(), &fixture.target, lookup, None).await?,
            Some(expected)
        );
        fixture.runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn restored_owner_rejects_unfinished_outcome_and_replays_original_completed_receipt() -> Result
{
    let fixture = Fixture::new(ObjectFormat::Sha256).await?;
    let completed_session = opened(&fixture, [145; 16]).await?;
    let pending_session = opened(&fixture, [146; 16]).await?;
    let expected = native(200, true, &completed_session).response;
    let input = completed_session
        .push_outcome(native(200, true, &completed_session))
        .await?;
    let pending = pending_session
        .push_outcome(native(503, false, &pending_session))
        .await?;
    let mutation = identity()?;
    let first = fixture
        .client()
        .command::<CompleteCatalogPush>(&fixture.target, mutation, input.clone())
        .await?;
    let before = state(&fixture.handle).await?;
    let outcomes = counts(&fixture.handle).await?;
    fixture.handle.drain().await?;
    fixture.runtime.shutdown().await?;
    let session = SessionId::from_bytes([147; 16]);
    let runtime = CellRuntime::new(SqlWorkerPool::new(1, 4)?, 64 << 20, session)?;
    let authority = CellAuthority::new(fixture.layout.clone());
    let idle = authority
        .load(fixture.target.cell_id())
        .await?
        .ok_or("idle")?;
    let provision = CellCatalog::new(fixture.layout.clone(), fixture.target.tenant())
        .lookup(fixture.target.cell_id())
        .await?
        .ok_or("provision")?;
    let handle = runtime
        .acquire_idle_restored(
            provision,
            fixture.replica.clone(),
            authority,
            idle,
            fixture.root.path().join("outcome-b.sqlite"),
            Owner {
                session,
                endpoint: "https://outcome-b.invalid".into(),
            },
        )
        .await?;
    let client = CellClient::local(Arc::clone(&fixture.registry), handle.clone());
    let replay = client
        .command::<CompleteCatalogPush>(&fixture.target, mutation, input.clone())
        .await?;
    assert_eq!(replay.output, first.output);
    assert_eq!(replay.receipt, first.receipt);
    let logical = client
        .command::<CompleteCatalogPush>(&fixture.target, identity()?, input)
        .await?;
    assert_eq!(logical.output, first.output);
    assert_eq!(
        replay_push_response(&client, &fixture.target, fixture.begin([145; 16]), None).await?,
        Some(expected)
    );
    let failed = client
        .command::<CompleteCatalogPush>(&fixture.target, identity()?, pending)
        .await;
    assert!(
        matches!(failed,Err(InvocationError::Rejected(ref value)) if value.output==CatalogCompletionReply::Denied(PreparationDenial::Stale))
    );
    assert_eq!(state(&handle).await?, before);
    assert_eq!(counts(&handle).await?, outcomes);
    runtime.shutdown().await?;
    Ok(())
}
