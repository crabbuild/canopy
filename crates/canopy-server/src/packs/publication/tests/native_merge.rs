//! Trusted initial root fixtures isolate the native transaction and exact SDK
//! recovery. Pack/catalog bytes come from verified stock-Git witnesses; this
//! does not qualify the public merge adapter or generated candidate producer.
use super::*;
use super::{
    prepare::{cleaned, opened},
    publishing::{Graph, assembled, edit, plan, state, update},
};
use crate::{
    packs::{
        metadata::tests::limits,
        ref_state::{RefStateIndex, RefStateSnapshot},
    },
    pulls::PullRevision,
    pulls::merge::{MergeOutcome, MergeRecord, MergeRequest, MergeStrategy, command::MergeInput},
};
use cellule_ltx::DiskBudget;
use cellule_runtime::{PreparedCommand, Resolution};

async fn initial(format: ObjectFormat, unrelated: bool) -> Result<(Fixture, Graph, MergeRequest)> {
    let f = Fixture::new(format).await?;
    let graph = assembled(&f, [245; 16], 16).await?;
    let source = if unrelated { graph.other } else { graph.tip };
    let store = graph.store.clone();
    let namespace = graph.prepared.token().artifact_operation;
    let index = RefStateIndex::new(store.clone(), format);
    let refs = index
        .prepare(
            None,
            namespace,
            &plan(vec![
                update("refs/heads/main", None, Some(graph.initial)),
                update("refs/heads/feature", None, Some(source)),
            ]),
        )
        .await?;
    let refs = RefStateSnapshotRoot::upload(
        &store,
        namespace,
        RefStateSnapshot {
            repository: f.repository,
            format,
            generation: 1,
            default_branch: "refs/heads/main".into(),
            root: Some(refs.root()),
        },
    )
    .await?;
    let mut catalog = BoundedEncoder::new(256)?;
    graph.prepared.catalog().encode(&mut catalog)?;
    let mut encoded_refs = BoundedEncoder::new(128)?;
    refs.encode(&mut encoded_refs)?;
    let catalog = catalog.finish();
    let refs = encoded_refs.finish();
    let base = graph.initial.to_vec();
    let source_bytes = source.to_vec();
    f.handle.execute(identity()?, Digest::from_bytes([145;32]), sql::now(0)?, 4096, 0, move |tx| {
        tx.execute("INSERT INTO catalog_generations(generation,catalog,certificate,refs) VALUES(1,?1,?2,?3)", rusqlite::params![catalog,[4u8;32].as_slice(),refs])?;
        tx.execute("UPDATE catalog_state SET generation=1 WHERE singleton=1", [])?;
        tx.execute("INSERT INTO pull_requests(number,id,creation_digest,author,title,body,state,draft,version,source_ref,base_ref,initial_source_oid,initial_base_oid,created_ms,updated_ms) VALUES(1,?1,?2,'writer','Merge','', 'open',0,1,'refs/heads/feature','refs/heads/main',?3,?4,0,0)",rusqlite::params![[25u8;16].as_slice(),[26u8;32].as_slice(),source_bytes,base])?;
        Ok(cellule_runtime::cell::executor::HandlerOutcome::Success(Vec::new()))
    }).await?;
    let request = MergeRequest {
        id: uuid::Uuid::new_v4().to_string(),
        strategy: MergeStrategy::FastForward,
        candidate_id: None,
        revision: PullRevision {
            pull_version: 1,
            source_oid: hex::encode(source),
            source_version: 1,
            base_oid: hex::encode(graph.initial),
            base_version: 1,
        },
    };
    Ok((f, graph, request))
}
async fn preparation(
    f: &Fixture,
    graph: &Graph,
) -> Result<(Arc<PreparedCatalog>, tempfile::TempDir, DiskBudget)> {
    let (base, _, _) = opened(f, *uuid::Uuid::new_v4().as_bytes(), graph.store.clone()).await?;
    let root = tempfile::TempDir::new()?;
    let budget = DiskBudget::new(256 << 20);
    let prepared = Arc::new(
        CatalogPreparation::new(root.path(), budget.clone(), base, limits())
            .await?
            .finish()
            .await?,
    );
    assert_eq!(prepared.input_count(), 0);
    Ok((prepared, root, budget))
}
async fn prepared_command(
    f: &Fixture,
    prepared: &PreparedCatalog,
    request: MergeRequest,
    root: &std::path::Path,
    budget: DiskBudget,
) -> Result<(
    PreparedCommand<PublishReviewedMerge>,
    RegisteredRootRecovery,
)> {
    let mutation = identity()?;
    let proof = prepared
        .native_merge_proof(
            MergeInput {
                actor: "owner".into(),
                number: 1,
                request,
                issued_at_ms: mutation.issued_at_ms,
            },
            root,
            budget,
            limits(),
        )
        .await?;
    let command = f
        .client()
        .prepare_command::<PublishReviewedMerge>(&f.target, mutation, proof)
        .await?;
    let registered = super::super::recovery::persist(
        &prepared.base.session,
        &command,
        super::super::recovery::Kind::Merge,
        &prepared.base.indexes().store(),
        identity()?,
        0,
    )
    .await?;
    Ok((command, registered))
}
async fn domain_state(f: &Fixture) -> Result<Vec<u8>> {
    let roots = state(&f.handle).await?;
    let editorial = f
        .handle
        .query(0, 4096, |db| {
            let pull = db.query_row(
                "SELECT state,version,updated_ms FROM pull_requests WHERE number=1",
                [],
                |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, i64>(1)?,
                        r.get::<_, i64>(2)?,
                    ))
                },
            )?;
            let merges = db.query_row("SELECT count(*) FROM pull_merges", [], |r| {
                r.get::<_, u64>(0)
            })?;
            serde_json::to_vec(&(pull, merges)).map_err(|_| Error::Command("merge fixture state"))
        })
        .await?;
    Ok([roots, editorial].concat())
}
async fn merged_roots(f: &Fixture, graph: &Graph) -> Result {
    let bytes=f.handle.query(0,4096,|db| {
        let result=db.query_row("SELECT s.generation,g.refs,(SELECT count(*) FROM refs),(SELECT generation FROM ref_generation),(SELECT state FROM pull_requests WHERE number=1),(SELECT count(*) FROM pull_merges) FROM catalog_state s JOIN catalog_generations g ON g.generation=s.generation",[],|r|Ok((r.get::<_,u64>(0)?,r.get::<_,Vec<u8>>(1)?,r.get::<_,u64>(2)?,r.get::<_,u64>(3)?,r.get::<_,String>(4)?,r.get::<_,u64>(5)?)))?;
        serde_json::to_vec(&result).map_err(|_|Error::Command("merge fixture roots"))
    }).await?;
    let (generation, refs, legacy, legacy_generation, pull, merges): (
        u64,
        Vec<u8>,
        u64,
        u64,
        String,
        u64,
    ) = serde_json::from_slice(&bytes)?;
    assert_eq!(
        (generation, legacy, legacy_generation, pull.as_str(), merges),
        (2, 0, 0, "merged", 1)
    );
    let mut d = BoundedDecoder::new(&refs, 128)?;
    let root = RefStateSnapshotRoot::decode(&mut d)?;
    d.finish()?;
    let snapshot = root.read(&graph.store).await?;
    assert_eq!(snapshot.generation, 2);
    let index = RefStateIndex::new(graph.store.clone(), f.format);
    let base = index
        .read(snapshot.root.clone(), "refs/heads/main")
        .await?
        .ok_or("merged base")?;
    let source = index
        .read(snapshot.root, "refs/heads/feature")
        .await?
        .ok_or("source")?;
    assert_eq!(
        (base.oid, base.version, source.oid, source.version),
        (Some(graph.tip), 2, Some(graph.tip), 1)
    );
    Ok(())
}
#[tokio::test]
async fn native_merge_commits_joint_roots_pull_uuid_and_original_receipt_without_sql_ref_authority()
-> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let (f, graph, request) = initial(format, false).await?;
        let (prepared, root, budget) = preparation(&f, &graph).await?;
        let (command, registered) =
            prepared_command(&f, &prepared, request.clone(), root.path(), budget.clone()).await?;
        let result = command.clone().execute().await?;
        let MergeOutcome::Applied { ref merge } = result.output else {
            return Err("merge denied".into());
        };
        assert_eq!(
            (merge.id.as_str(), merge.oid.as_str()),
            (request.id.as_str(), request.revision.source_oid.as_str())
        );
        merged_roots(&f, &graph).await?;
        let recovered = registered
            .dispatch_any(
                &f.client(),
                &graph.store,
                &f.authority(),
                &std::sync::atomic::AtomicBool::new(false),
            )
            .await?;
        let PublicationOutcome::Merge(recovered) = recovered else {
            return Err("wrong recovered purpose".into());
        };
        assert_eq!(recovered.receipt, result.receipt);
        assert_eq!(recovered.output, result.output);
        // A fresh SDK identity and privately owned attempt replay the original
        // application UUID, even though the pull and base revision have advanced.
        let (retry, retry_root, retry_budget) = preparation(&f, &graph).await?;
        let (retry_command, _) =
            prepared_command(&f, &retry, request, retry_root.path(), retry_budget.clone()).await?;
        assert_eq!(retry_command.execute().await?.output, result.output);
        merged_roots(&f, &graph).await?;
        drop(retry);
        cleaned(retry_root.path(), &retry_budget).await?;
        drop(prepared);
        cleaned(root.path(), &budget).await?;
        drop(graph.prepared);
        cleaned(graph.root.path(), &graph.budget).await?;
        f.runtime.shutdown().await?;
    }
    Ok(())
}
#[tokio::test]
async fn native_merge_unprotected_unrelated_history_records_refusal_without_publishing() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let (f, graph, request) = initial(format, true).await?;
        let (prepared, root, budget) = preparation(&f, &graph).await?;
        let (command, registered) =
            prepared_command(&f, &prepared, request, root.path(), budget.clone()).await?;
        let before = domain_state(&f).await?;
        let result = command.execute().await?;
        assert_eq!(result.output, MergeOutcome::NotFastForward);
        assert_eq!(domain_state(&f).await?, before);
        let recovered = registered
            .dispatch_any(
                &f.client(),
                &graph.store,
                &f.authority(),
                &std::sync::atomic::AtomicBool::new(false),
            )
            .await;
        assert!(
            matches!(recovered,Err(PublicationError::Merge(InvocationError::Rejected(value))) if value.receipt==result.receipt && value.output==MergeOutcome::NotFastForward)
        );
        drop(prepared);
        cleaned(root.path(), &budget).await?;
        drop(graph.prepared);
        cleaned(graph.root.path(), &graph.budget).await?;
        f.runtime.shutdown().await?;
    }
    Ok(())
}
#[tokio::test]
async fn native_merge_late_review_requirement_is_current_and_does_not_bind_a_rejected_application_uuid()
-> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let (f, graph, request) = initial(format, false).await?;
        let (prepared, root, budget) = preparation(&f, &graph).await?;
        let (first, registered) =
            prepared_command(&f, &prepared, request.clone(), root.path(), budget.clone()).await?;
        edit(
            &f,
            "INSERT INTO branch_rules VALUES('refs/heads/main',1,1,1,1,1,1)",
        )
        .await?;
        let before = domain_state(&f).await?;
        let result = first.execute().await?;
        assert_eq!(result.output, MergeOutcome::ReviewsRequired);
        assert_eq!(domain_state(&f).await?, before);
        // Remove only the review requirement, retain require-PR, ancestry and
        // deletion policy. Only the reviewed command may satisfy this PR gate.
        edit(&f,"UPDATE branch_rules SET required_approvals=0,version=2 WHERE reference='refs/heads/main'").await?;
        let (retry, retry_root, retry_budget) = preparation(&f, &graph).await?;
        let (retry_command, _) =
            prepared_command(&f, &retry, request, retry_root.path(), retry_budget.clone()).await?;
        assert!(matches!(
            retry_command.execute().await?.output,
            MergeOutcome::Applied { .. }
        ));
        merged_roots(&f, &graph).await?;
        assert!(
            matches!(registered.dispatch_any(&f.client(),&graph.store,&f.authority(),&std::sync::atomic::AtomicBool::new(false)).await,
            Err(PublicationError::Merge(InvocationError::Rejected(value))) if value.receipt==result.receipt && value.output==MergeOutcome::ReviewsRequired)
        );
        drop(retry);
        cleaned(retry_root.path(), &retry_budget).await?;
        drop(prepared);
        cleaned(root.path(), &budget).await?;
        drop(graph.prepared);
        cleaned(graph.root.path(), &graph.budget).await?;
        f.runtime.shutdown().await?;
    }
    Ok(())
}
#[tokio::test]
async fn native_merge_late_sql_abort_rolls_back_roots_pull_uuid_checkpoint_and_recovery_phase()
-> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let (f, graph, request) = initial(format, false).await?;
        let (prepared, root, budget) = preparation(&f, &graph).await?;
        let (command, _registered) =
            prepared_command(&f, &prepared, request, root.path(), budget.clone()).await?;
        edit(&f,"CREATE TRIGGER abort_merge BEFORE INSERT ON pull_merges BEGIN SELECT RAISE(ABORT,'late merge failure'); END;").await?;
        let phase_before = phase_state(&f).await?;
        let before = domain_state(&f).await?;
        assert!(command.clone().execute().await.is_err());
        assert_eq!(domain_state(&f).await?, before);
        assert!(matches!(
            f.client().resolve(command.evidence()).await?,
            Resolution::Absent
        ));
        assert_eq!(phase_state(&f).await?, phase_before);
        edit(&f, "DROP TRIGGER abort_merge").await?;
        assert!(matches!(
            command.execute().await?.output,
            MergeOutcome::Applied { .. }
        ));
        merged_roots(&f, &graph).await?;
        drop(prepared);
        cleaned(root.path(), &budget).await?;
        drop(graph.prepared);
        cleaned(graph.root.path(), &graph.budget).await?;
        f.runtime.shutdown().await?;
    }
    Ok(())
}
#[test]
fn maximum_sha256_merge_result_fits_existing_512_byte_recovery_contract() -> Result {
    let max = i64::MAX;
    let result = MergeOutcome::Applied {
        merge: MergeRecord {
            id: "12345678-1234-1234-1234-123456789abc".into(),
            number: max,
            oid: "c".repeat(64),
            merged_at_ms: max,
            revision: PullRevision {
                pull_version: max - 1,
                source_oid: "a".repeat(64),
                source_version: max - 1,
                base_oid: "b".repeat(64),
                base_version: max - 1,
            },
        },
    };
    let mut e = BoundedEncoder::new(512)?;
    result.encode(&mut e)?;
    let bytes = e.finish();
    assert_eq!(bytes.len(), 493);
    let mut d = BoundedDecoder::new(&bytes, 512)?;
    assert_eq!(MergeOutcome::decode(&mut d)?, result);
    d.finish()?;
    Ok(())
}

async fn phase_state(f: &Fixture) -> Result<Vec<u8>> {
    Ok(f.handle.query(0,65536,|db| {
        let mut statement=db.prepare("SELECT recovery_phase,recovery_phase_revision FROM catalog_leases ORDER BY admission_sequence")?;
        let rows=statement.query_map([],|r|Ok((r.get::<_,Option<Vec<u8>>>(0)?,r.get::<_,u64>(1)?)))?.collect::<rusqlite::Result<Vec<_>>>()?;
        serde_json::to_vec(&rows).map_err(|_|Error::Command("merge fixture phase state"))
    }).await?)
}

#[tokio::test]
async fn native_merge_changed_request_and_later_joint_generation_cannot_publish() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        for altered_request in [true, false] {
            let (f, graph, request) = initial(format, false).await?;
            let (prepared, root, budget) = preparation(&f, &graph).await?;
            let mutation = identity()?;
            let proof = prepared
                .native_merge_proof(
                    MergeInput {
                        actor: "owner".into(),
                        number: 1,
                        request: request.clone(),
                        issued_at_ms: mutation.issued_at_ms,
                    },
                    root.path(),
                    budget.clone(),
                    limits(),
                )
                .await?;
            let mut e = BoundedEncoder::new(NATIVE_MERGE_BYTES)?;
            proof.encode(&mut e)?;
            let mut bytes = e.finish();
            if altered_request {
                let locations: Vec<_> = bytes
                    .windows(request.id.len())
                    .enumerate()
                    .filter(|(_, b)| *b == request.id.as_bytes())
                    .map(|(i, _)| i)
                    .collect();
                assert_eq!(locations.len(), 1);
                let last = locations[0] + request.id.len() - 1;
                bytes[last] = if bytes[last] == b'a' { b'b' } else { b'a' };
            }
            let mut d = BoundedDecoder::new(&bytes, NATIVE_MERGE_BYTES)?;
            let proof = NativeMergeProof::decode(&mut d)?;
            d.finish()?;
            let command = f
                .client()
                .prepare_command::<PublishReviewedMerge>(&f.target, mutation, proof)
                .await?;
            super::super::recovery::persist(
                &prepared.base.session,
                &command,
                super::super::recovery::Kind::Merge,
                &graph.store,
                identity()?,
                0,
            )
            .await?;
            if !altered_request {
                // Model an authoritative unrelated catalog publication with
                // identical ref facts. The joint generation alone must fence it.
                edit(&f, "INSERT INTO catalog_generations SELECT 2,catalog,certificate,refs FROM catalog_generations WHERE generation=1; UPDATE catalog_state SET generation=2 WHERE singleton=1;").await?;
            }
            let before = domain_state(&f).await?;
            assert_eq!(command.execute().await?.output, MergeOutcome::Conflict);
            assert_eq!(domain_state(&f).await?, before);
            drop(prepared);
            cleaned(root.path(), &budget).await?;
            drop(graph.prepared);
            cleaned(graph.root.path(), &graph.budget).await?;
            f.runtime.shutdown().await?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn native_merge_requires_registered_original_command_before_its_first_domain_write() -> Result
{
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let (f, graph, request) = initial(format, false).await?;
        let (prepared, root, budget) = preparation(&f, &graph).await?;
        let mutation = identity()?;
        let proof = prepared
            .native_merge_proof(
                MergeInput {
                    actor: "owner".into(),
                    number: 1,
                    request,
                    issued_at_ms: mutation.issued_at_ms,
                },
                root.path(),
                budget.clone(),
                limits(),
            )
            .await?;
        let command = f
            .client()
            .prepare_command::<PublishReviewedMerge>(&f.target, mutation, proof)
            .await?;
        let before = domain_state(&f).await?;
        assert!(command.clone().execute().await.is_err());
        assert_eq!(domain_state(&f).await?, before);
        assert!(matches!(
            f.client().resolve(command.evidence()).await?,
            Resolution::Absent
        ));
        super::super::recovery::persist(
            &prepared.base.session,
            &command,
            super::super::recovery::Kind::Merge,
            &graph.store,
            identity()?,
            0,
        )
        .await?;
        assert!(matches!(
            command.execute().await?.output,
            MergeOutcome::Applied { .. }
        ));
        merged_roots(&f, &graph).await?;
        drop(prepared);
        cleaned(root.path(), &budget).await?;
        drop(graph.prepared);
        cleaned(graph.root.path(), &graph.budget).await?;
        f.runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn native_merge_original_result_survives_sqlite_loss_and_actual_owner_restoration() -> Result
{
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let (f, graph, request) = initial(format, false).await?;
        let (prepared, root, budget) = preparation(&f, &graph).await?;
        let check = prepared.base.capability().2.clone();
        let (command, registered) =
            prepared_command(&f, &prepared, request, root.path(), budget.clone()).await?;
        let result = command.execute().await?;
        assert!(matches!(result.output, MergeOutcome::Applied { .. }));
        let evidence = registered.evidence().clone();
        drop(registered);
        drop(prepared);
        cleaned(root.path(), &budget).await?;
        let (runtime, handle, client) = super::durable_recovery::restore_owner(&f, &check).await?;
        let loaded = RegisteredRootRecovery::load(&client, &f.target, &graph.store, &check)
            .await?
            .ok_or("native merge recovery pin missing")?;
        assert_eq!(loaded.evidence(), &evidence);
        let recovered = loaded
            .dispatch_any(
                &client,
                &graph.store,
                &f.authority(),
                &std::sync::atomic::AtomicBool::new(false),
            )
            .await?;
        let PublicationOutcome::Merge(recovered) = recovered else {
            return Err("wrong restored result purpose".into());
        };
        assert_eq!(recovered.receipt, result.receipt);
        assert_eq!(recovered.output, result.output);
        assert!(handle.owner_fence().epoch > check.token.owner.epoch);
        let rows = handle.query(0, 128, |db| {
            let result: (u64,u64) = db.query_row("SELECT (SELECT generation FROM catalog_state),(SELECT count(*) FROM pull_merges)", [], |r| Ok((r.get(0)?,r.get(1)?)))?;
            serde_json::to_vec(&result).map_err(|_| Error::Command("restored merge fixture"))
        }).await?;
        assert_eq!(serde_json::from_slice::<(u64, u64)>(&rows)?, (2, 1));
        drop(graph.prepared);
        cleaned(graph.root.path(), &graph.budget).await?;
        runtime.shutdown().await?;
    }
    Ok(())
}
