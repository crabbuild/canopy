use super::super::root_completion::tests::{
    audit_native, change_namespace, reseal_fixture, response,
};
use super::publishing::{edit, state};
use super::*;
use crate::git_http::GitHttpResponse;
use crate::packs::metadata::tests::limits;
use canopy_object_storage::artifact::ArtifactStore;
use cellule_ltx::DiskBudget;
use cellule_runtime::Committed;
use std::path::Path;
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum CompletionMode {
    RefFree {
        kind: super::root_outcome::Kind,
        fault: u8,
        revoked: bool,
    },
    Dispatch {
        fault: u8,
        loss: super::root_dispatch::Loss,
    },
    Success,
    PolicyRefusal,
    CheckRefusal,
    SignedAccepted,
    SignedReplay,
    WriteRevoked,
}
impl CompletionMode {
    fn rejected(self) -> bool {
        matches!(
            self,
            Self::PolicyRefusal | Self::CheckRefusal | Self::SignedReplay | Self::WriteRevoked
        )
    }
}
pub(super) struct ReplayFixture {
    input: RootPushCompletion,
    committed: Committed<RootCompletionReply>,
    mutation: MutationIdentity,
    request: BeginRequest,
    expected: GitHttpResponse,
}

pub(super) async fn qualify(
    fixture: &Fixture,
    prepared: &PreparedCatalog,
    store: &Arc<ArtifactStore>,
    request: PushCompletionRequest,
    directory: &Path,
    budget: DiskBudget,
    mode: CompletionMode,
) -> Result<ReplayFixture> {
    let rejected = mode.rejected();
    let plan = request.plan.unwrap();
    assert!(prepared.base().refs.is_some(), "prepared rooted base");
    let (checkpoint, _, _, _) = prepared.base.session.push_checkpoint().await?;
    let mut encoded = BoundedEncoder::new(CERTIFICATE_BYTES)?;
    checkpoint.encode(&mut encoded)?;
    assert_eq!(
        prepared.input_checkpoint_digest,
        Some(*blake3::hash(&encoded.finish()).as_bytes()),
        "prepared/checkpoint custody digest"
    );
    if mode == CompletionMode::CheckRefusal {
        let oid = plan.updates[0].new_oid.ok_or("check target")?;
        edit(fixture, &format!("INSERT INTO check_contexts VALUES('ci','owner',1,1); INSERT INTO branch_rules VALUES('refs/heads/main',1,1,0,0,0,0); INSERT INTO branch_required_checks VALUES('refs/heads/main','ci'); INSERT INTO check_runs(id,oid,context,context_version,reporter,state,version,summary,created_ms,updated_ms) VALUES(X'{}',X'{}','ci',1,'owner','success',1,'',0,0)",hex::encode(uuid::Uuid::new_v4().as_bytes()),hex::encode(oid))).await?;
    }
    let pending = prepared
        .ref_policy_preparation(plan.clone(), directory, budget.clone(), limits())
        .await?;
    let mut start = 0;
    while start < pending.plan().updates.len() {
        let page = pending.page(prepared, start).await?;
        start += page.proof.plan.updates.len();
        fixture
            .client()
            .command::<RegisterRefPolicyPage>(&fixture.target, identity()?, page)
            .await?;
    }
    let guard = pending.ready(prepared).await?;
    let before = state(&fixture.handle).await?;
    let mut completion = prepared
        .root_push_completion(&guard, directory, budget.clone(), limits(), None)
        .await
        .map_err(|error| format!("root completion preparation: {error:?}"))?;
    assert_eq!(completion.outcomes.ref_generation, 1);
    assert_eq!(
        response(completion.outcomes.native, store).await?,
        request.response
    );
    assert_eq!(
        response(completion.outcomes.rejected, store).await?,
        crate::push::report::rejected_report(&request.response, crate::push::report::REJECTED)?
    );
    assert_eq!(
        response(completion.outcomes.replayed, store).await?,
        crate::push::report::rejected_report(
            &request.response,
            "Canopy signed push certificate was already used"
        )?
    );
    for root in [
        completion.outcomes.native,
        completion.outcomes.rejected,
        completion.outcomes.replayed,
    ] {
        assert_eq!(root.operation(), prepared.token().artifact_operation);
        assert!(root.artifact().size < 1024);
        let native = audit_native(root, store).await?;
        let (proof, _, _, _) = prepared.base.session.push_checkpoint().await?;
        assert_eq!(Some(native), proof.native_result()?);
    }
    let mut e = BoundedEncoder::new(ROOT_COMPLETION_BYTES)?;
    completion.encode(&mut e)?;
    let bytes = e.finish();
    assert!(bytes.len() < 2048);
    let mut d = BoundedDecoder::new(&bytes, ROOT_COMPLETION_BYTES)?;
    assert_eq!(RootPushCompletion::decode(&mut d)?, completion);
    d.finish()?;
    // Isolate the command's generation predicate from payload/MAC mismatch:
    // even a correctly resealed joint proof cannot carry a ref-free bundle.
    let mut ref_free = completion.clone();
    ref_free.outcomes.ref_generation = 0;
    reseal_fixture(&mut ref_free)?;
    assert!(
        ref_free
            .encode(&mut BoundedEncoder::new(ROOT_COMPLETION_BYTES)?)
            .is_err()
    );
    assert_eq!(state(&fixture.handle).await?, before);
    for choice in 0..8 {
        let mut changed = completion.clone();
        match choice {
            0 => changed.outcomes.response_id = *uuid::Uuid::new_v4().as_bytes(),
            1 => changed.outcomes.ref_generation += 1,
            2 => std::mem::swap(&mut changed.outcomes.native, &mut changed.outcomes.rejected),
            3 => std::mem::swap(
                &mut changed.outcomes.rejected,
                &mut changed.outcomes.replayed,
            ),
            4 => {
                changed.outcomes.signed = Some(RootSignedPushFact {
                    digest: [1; 32],
                    key: "key".into(),
                    size: 1,
                })
            }
            5 => changed.proof.guard.plan_digest[0] ^= 1,
            6 => changed.proof.snapshot = prepared.base().refs.unwrap(),
            _ => changed.outcomes.native = change_namespace(changed.outcomes.native),
        }
        assert!(
            changed
                .encode(&mut BoundedEncoder::new(ROOT_COMPLETION_BYTES)?)
                .is_err(),
            "choice {choice}"
        );
    }
    // The existing inline publisher must not accept a root-completion purpose.
    let ancestry = vec![0; plan.updates.len().div_ceil(8)];
    let denied = fixture
        .client()
        .command::<PublishCatalogRefs>(
            &fixture.target,
            identity()?,
            RefPublicationProof {
                certificate: completion.proof.certificate.clone(),
                plan,
                ancestry,
            },
        )
        .await;
    assert!(matches!(denied, Err(InvocationError::Rejected(value))
        if value.output == PublicationReply::Denied(PreparationDenial::Unauthorized)));
    assert_eq!(state(&fixture.handle).await?, before);
    fixture
        .install_generation(2, prepared.catalog(), prepared.base().refs)
        .await?;
    if !rejected {
        let before = state(&fixture.handle).await?;
        let stale = fixture
            .client()
            .command::<CompleteRootPush>(&fixture.target, identity()?, completion.clone())
            .await;
        assert!(
            matches!(stale,Err(InvocationError::Rejected(value)) if value.output==RootCompletionReply::Denied(PreparationDenial::Conflict))
        );
        assert_eq!(state(&fixture.handle).await?, before);
        let rebased = Box::pin(prepared.reconcile()).await?;
        completion = rebased
            .root_push_completion(&guard, directory, budget.clone(), limits(), None)
            .await?;
        assert_eq!(completion.proof.certificate.data()?.base.generation, 2);
    }
    if mode == CompletionMode::PolicyRefusal {
        edit(
            fixture,
            "INSERT INTO branch_rules VALUES('refs/heads/main',1,1,0,1,0,0)",
        )
        .await?;
        assert!(
            prepared
                .root_push_completion(&guard, directory, budget.clone(), limits(), None)
                .await
                .is_err()
        );
    }
    if mode == CompletionMode::CheckRefusal {
        edit(fixture, "UPDATE check_runs SET state='failure',version=2").await?;
        let epoch = fixture
            .handle
            .query(0, 32, |db| {
                let epoch: u64 =
                    db.query_row("SELECT version FROM ref_policy_epoch", [], |row| row.get(0))?;
                Ok(epoch.to_be_bytes().to_vec())
            })
            .await?;
        assert_eq!(epoch, completion.proof.guard.epoch.to_be_bytes());
        assert!(
            prepared
                .root_push_completion(&guard, directory, budget.clone(), limits(), None)
                .await
                .is_err()
        );
    }
    // Exercise the final transaction's ownership branches with a trusted
    // attestor fixture. This does not qualify native signed-CGI verification.
    if matches!(
        mode,
        CompletionMode::SignedAccepted | CompletionMode::SignedReplay
    ) {
        completion.outcomes.signed = Some(RootSignedPushFact {
            digest: [17; 32],
            key: "fixture signing key".into(),
            size: 1234,
        });
        reseal_fixture(&mut completion)?;
        if mode == CompletionMode::SignedReplay {
            edit(fixture, "INSERT INTO pushes(id,actor,request_digest) VALUES(zeroblob(16),'original',zeroblob(32)); INSERT INTO push_certificates VALUES(X'1111111111111111111111111111111111111111111111111111111111111111',zeroblob(16),'original','original','original key',1234,0)").await?;
        }
    }
    if mode == CompletionMode::WriteRevoked {
        edit(fixture, "UPDATE repository_identity SET owner='other' WHERE singleton=1; INSERT INTO repository_members VALUES('owner','read')").await?;
    }
    let request = BeginRequest {
        repository: fixture.repository,
        operation: prepared.token().operation,
        request_digest: prepared.token().request_digest,
        actor: "owner".into(),
        lease_ms: DEFAULT_LEASE_MS,
    };
    assert!(
        replay_root_push_response(
            &fixture.client(),
            &fixture.target,
            request.clone(),
            None,
            store
        )
        .await?
        .is_none()
    );
    // Every last-write fault must restore all earlier effects, including the
    // saved root, consumed operation, joint generation and attestation pin.
    let mut faults = vec![
        "CREATE TRIGGER fixture_root_fault BEFORE INSERT ON pushes BEGIN SELECT RAISE(ABORT,'late root outcome fault'); END",
        "CREATE TRIGGER fixture_root_fault BEFORE DELETE ON catalog_operations BEGIN SELECT RAISE(ABORT,'late operation consumption fault'); END",
    ];
    if mode == CompletionMode::SignedAccepted {
        faults.push("CREATE TRIGGER fixture_root_fault BEFORE INSERT ON push_certificates BEGIN SELECT RAISE(ABORT,'late signed ownership fault'); END");
    }
    for sql in faults {
        edit(fixture, sql).await?;
        let before = state(&fixture.handle).await?;
        assert!(
            fixture
                .client()
                .command::<CompleteRootPush>(&fixture.target, identity()?, completion.clone())
                .await
                .is_err()
        );
        assert_eq!(state(&fixture.handle).await?, before);
        edit(fixture, "DROP TRIGGER fixture_root_fault").await?;
    }
    let mutation = identity()?;
    let committed = fixture
        .client()
        .command::<CompleteRootPush>(&fixture.target, mutation, completion.clone())
        .await?;
    let RootCompletionReply::Completed(ref output) = committed.output else {
        return Err("root publication denied".into());
    };
    assert_eq!(output.completion.rejected, rejected);
    assert_eq!(
        output.root,
        if mode == CompletionMode::SignedReplay {
            completion.outcomes.replayed
        } else if rejected {
            completion.outcomes.rejected
        } else {
            completion.outcomes.native
        }
    );
    assert_eq!(output.completion.publication.is_some(), !rejected);
    if let Some(value) = output.completion.publication {
        assert_eq!(value.generation, 3);
        assert_eq!(value.ref_generation, 1);
    }
    let roots = fixture.handle.query(0, 1024, |db| {
        let value: (u64, Vec<u8>, Vec<u8>) = db.query_row("SELECT generation,catalog,refs FROM catalog_generations WHERE generation=(SELECT generation FROM catalog_state)", [], |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?)))?;
        Ok(serde_json::to_vec(&value).unwrap())
    }).await?;
    let (generation, catalog, refs): (u64, Vec<u8>, Vec<u8>) = serde_json::from_slice(&roots)?;
    assert_eq!(generation, if rejected { 2 } else { 3 });
    let mut encoded = BoundedEncoder::new(256)?;
    completion
        .proof
        .certificate
        .data()?
        .catalog
        .encode(&mut encoded)?;
    assert_eq!(catalog, encoded.finish());
    let mut encoded = BoundedEncoder::new(128)?;
    if rejected {
        prepared.base().refs.unwrap()
    } else {
        completion.proof.snapshot
    }
    .encode(&mut encoded)?;
    assert_eq!(refs, encoded.finish());
    let expected = response(output.root, store).await?;
    if matches!(
        mode,
        CompletionMode::SignedAccepted | CompletionMode::SignedReplay
    ) {
        let ownership = fixture.handle.query(0, 1024, |db| {
            let value: (Vec<u8>, String, String, String, u64, i64) = db.query_row("SELECT push_id,actor,signer,key,size,(SELECT count(*) FROM push_certificate_chunks) FROM push_certificates", [], |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?,row.get(4)?,row.get(5)?)))?;
            Ok(serde_json::to_vec(&value).unwrap())
        }).await?;
        let original = mode == CompletionMode::SignedReplay;
        assert_eq!(
            serde_json::from_slice::<(Vec<u8>, String, String, String, u64, i64)>(&ownership)?,
            (
                if original {
                    vec![0; 16]
                } else {
                    request.operation.to_vec()
                },
                if original { "original" } else { "owner" }.into(),
                if original { "original" } else { "owner" }.into(),
                if original {
                    "original key"
                } else {
                    "fixture signing key"
                }
                .into(),
                1234,
                0
            )
        );
        let before = state(&fixture.handle).await?;
        assert!(
            edit(fixture, "DELETE FROM push_certificates")
                .await
                .is_err()
        );
        assert_eq!(state(&fixture.handle).await?, before);
        assert!(edit(fixture, "INSERT OR REPLACE INTO push_certificates SELECT X'2222222222222222222222222222222222222222222222222222222222222222',push_id,actor,signer,key,size,recorded_at_ms FROM push_certificates").await.is_err());
        assert_eq!(state(&fixture.handle).await?, before);
    }
    let mut foreign = request.clone();
    foreign.request_digest[0] ^= 1;
    assert!(matches!(
        replay_root_push_response(&fixture.client(), &fixture.target, foreign, None, store).await,
        Err(RootPushReplayError::Denied(PreparationDenial::Conflict))
    ));
    let mut foreign = request.clone();
    foreign.repository = *uuid::Uuid::new_v4().as_bytes();
    assert!(matches!(
        replay_root_push_response(&fixture.client(), &fixture.target, foreign, None, store).await,
        Err(RootPushReplayError::Context)
    ));
    for sql in [
        "DELETE FROM pushes WHERE response_root IS NOT NULL",
        "UPDATE pushes SET response_root=X'00' WHERE response_root IS NOT NULL",
        "UPDATE pushes SET publication=NULL,publication_plan_digest=NULL WHERE response_root IS NOT NULL AND publication IS NOT NULL",
    ] {
        if sql.contains("publication=NULL") && rejected {
            continue;
        }
        let before = state(&fixture.handle).await?;
        assert!(edit(fixture, sql).await.is_err());
        assert_eq!(state(&fixture.handle).await?, before);
    }
    let mut replay = replay_root_push_response(
        &fixture.client(),
        &fixture.target,
        request.clone(),
        Some(committed.receipt),
        store,
    )
    .await?
    .ok_or("missing durable root response")?;
    let mut body = Vec::new();
    while let Some(part) = replay.body.next().await? {
        body.extend_from_slice(&part);
    }
    assert_eq!(
        GitHttpResponse {
            status: replay.status,
            headers: replay.headers,
            body
        },
        expected
    );
    let facts = fixture.handle.query(0, 128, |db| {
        let counts: (i64,i64,i64,i64,i64) = db.query_row("SELECT (SELECT count(*) FROM refs),(SELECT count(*) FROM push_responses),(SELECT count(*) FROM push_response_chunks),(SELECT count(*) FROM catalog_operations),(SELECT count(*) FROM catalog_leases)",[],|row|Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?,row.get(4)?)))?;
        Ok(serde_json::to_vec(&counts).unwrap())
    }).await?;
    assert_eq!(
        serde_json::from_slice::<(i64, i64, i64, i64, i64)>(&facts)?,
        (0, 0, 0, 0, 2)
    );
    if mode == CompletionMode::WriteRevoked {
        edit(
            fixture,
            "DELETE FROM repository_members WHERE account='owner'",
        )
        .await?;
    }
    let before = state(&fixture.handle).await?;
    edit(
        fixture,
        "UPDATE repository_identity SET owner='other' WHERE singleton=1",
    )
    .await?;
    let after_acl = state(&fixture.handle).await?;
    assert_eq!(
        fixture
            .client()
            .command::<CompleteRootPush>(&fixture.target, identity()?, completion.clone())
            .await?
            .output,
        committed.output
    );
    assert_eq!(
        fixture
            .client()
            .command::<CompleteRootPush>(&fixture.target, mutation, completion.clone())
            .await?
            .receipt,
        committed.receipt
    );
    assert_eq!(state(&fixture.handle).await?, after_acl);
    assert_eq!(after_acl, before);
    assert!(matches!(
        replay_root_push_response(
            &fixture.client(),
            &fixture.target,
            request.clone(),
            None,
            store
        )
        .await,
        Err(RootPushReplayError::Denied(PreparationDenial::Unauthorized))
    ));
    edit(
        fixture,
        "UPDATE repository_identity SET owner='owner' WHERE singleton=1",
    )
    .await?;
    Ok(ReplayFixture {
        input: completion,
        committed,
        mutation,
        request,
        expected,
    })
}
pub(super) async fn restored(
    fixture: &Fixture,
    store: &ArtifactStore,
    replay: ReplayFixture,
) -> Result {
    fixture.handle.drain().await?;
    fixture.runtime.shutdown().await?;
    let session = SessionId::from_bytes([251; 16]);
    let runtime = CellRuntime::new(SqlWorkerPool::new(1, 4)?, 64 << 20, session)?;
    let authority = CellAuthority::new(fixture.layout.clone());
    let idle = authority
        .load(fixture.target.cell_id())
        .await?
        .ok_or("idle root owner")?;
    let provision = CellCatalog::new(fixture.layout.clone(), fixture.target.tenant())
        .lookup(fixture.target.cell_id())
        .await?
        .ok_or("root provision")?;
    let handle = runtime
        .acquire_idle_restored(
            provision,
            fixture.replica.clone(),
            authority,
            idle,
            fixture.root.path().join("root-restored.sqlite"),
            Owner {
                session,
                endpoint: "https://root-restored.invalid".into(),
            },
        )
        .await?;
    assert!(handle.owner_fence().epoch > replay.input.proof.certificate.data()?.token.owner.epoch);
    let client = CellClient::local(fixture.registry.clone(), handle.clone());
    let before = state(&handle).await?;
    let exact = client
        .command::<CompleteRootPush>(&fixture.target, replay.mutation, replay.input.clone())
        .await?;
    assert_eq!(
        (exact.output, exact.receipt),
        (replay.committed.output.clone(), replay.committed.receipt)
    );
    assert_eq!(
        client
            .command::<CompleteRootPush>(&fixture.target, identity()?, replay.input.clone())
            .await?
            .output,
        replay.committed.output
    );
    assert_eq!(state(&handle).await?, before);
    // Trusted fixture resealing tests the actual fence independently of the
    // missing-operation branch; product callers cannot mint this certificate.
    let mut stale = replay.input;
    let mut data = stale.proof.certificate.data()?;
    data.token.operation = *uuid::Uuid::new_v4().as_bytes();
    let seed = handle
        .query(0, 32, |db| {
            Ok(db.query_row(
                "SELECT push_cert_seed FROM repository_identity",
                [],
                |row| row.get::<_, Vec<u8>>(0),
            )?)
        })
        .await?;
    let seed: [u8; 32] = seed.try_into().map_err(|_| "seed shape")?;
    stale.proof.certificate = CatalogCertificate::seal(&data, &seed)?;
    let denied = client
        .command::<CompleteRootPush>(&fixture.target, identity()?, stale)
        .await;
    assert!(
        matches!(denied,Err(InvocationError::Rejected(value)) if value.output==RootCompletionReply::Denied(PreparationDenial::Stale))
    );
    assert_eq!(state(&handle).await?, before);
    let mut response = replay_root_push_response(
        &client,
        &fixture.target,
        replay.request,
        Some(replay.committed.receipt),
        store,
    )
    .await?
    .ok_or("restored response")?;
    let mut body = Vec::new();
    while let Some(part) = response.body.next().await? {
        body.extend_from_slice(&part);
    }
    assert_eq!(
        GitHttpResponse {
            status: response.status,
            headers: response.headers,
            body
        },
        replay.expected
    );
    runtime.shutdown().await?;
    Ok(())
}
