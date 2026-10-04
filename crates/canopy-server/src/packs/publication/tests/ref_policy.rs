use super::*;
use super::{
    prepare::{cleaned, opened},
    publishing::{Graph as CatalogGraph, edit, plan, state, update},
};
use crate::packs::metadata::tests::limits;
use canopy_object_storage::artifact::ArtifactStore;
use cellule_ltx::DiskBudget;
use cellule_runtime::{PreparedCommand, Resolution};
use tokio::sync::Mutex;
use tokio::time::{Duration, timeout};
mod fixture;

mod cleanup;
mod freshness;
mod pages;

struct Graph {
    catalog: CatalogGraph,
    staging: StagingCoordinator,
    ticket: StagingTicket,
    refusal: Arc<ReadyRootPush>,
    head: Mutex<Option<RegisteredRootRecovery>>,
    provider: Arc<dyn object_store::ObjectStore>,
}
impl std::ops::Deref for Graph {
    type Target = CatalogGraph;
    fn deref(&self) -> &Self::Target {
        &self.catalog
    }
}
async fn rooted(format: ObjectFormat) -> Result<(Fixture, Graph)> {
    let fixture = Fixture::new(format).await?;
    let provider: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let store = Arc::new(ArtifactStore::new(provider.clone(), fixture.repository));
    let (base, _, _) = opened(&fixture, [230; 16], store.clone()).await?;
    let root = tempfile::TempDir::new()?;
    let budget = DiskBudget::new(64 << 20);
    let empty = CatalogPreparation::new(root.path(), budget.clone(), base, limits())
        .await?
        .finish()
        .await?;
    let proof = empty.empty_ref_initialization().await?;
    let (command, registered) =
        super::initialization::registered(&fixture, &empty, proof, identity()?).await?;
    command.execute().await?;
    // Match production startup: the completed initial fact keeps its metadata
    // and original receipt, while its generation-zero pin is retired.
    registered
        .ready_terminal_release(
            fixture.client(),
            &store,
            super::terminal_retention::maintenance(&fixture.handle, fixture.repository).await?,
            identity()?,
        )
        .await?
        .complete()
        .await?;
    drop(empty);
    cleaned(root.path(), &budget).await?;
    let graph = fixture::attempt(&fixture, provider, store).await?;
    Ok((fixture, graph))
}
async fn fresh(f: &Fixture, graph: &Graph) -> Result<Graph> {
    let fresh = fixture::attempt(f, graph.provider.clone(), graph.store.clone()).await?;
    assert_eq!((fresh.tip, fresh.blob), (graph.tip, graph.blob));
    assert_ne!(
        fresh.prepared.token().artifact_operation,
        graph.prepared.token().artifact_operation
    );
    Ok(fresh)
}
async fn finish_graph(graph: Graph) -> Result {
    graph.ticket.stop();
    assert!(graph.staging.close_and_drain().await.is_empty());
    let Graph { catalog, .. } = graph;
    drop(catalog.prepared);
    cleaned(catalog.root.path(), &catalog.budget).await?;
    Ok(())
}
async fn close(fixture: Fixture, graph: Graph) -> Result {
    finish_graph(graph).await?;
    fixture.runtime.shutdown().await?;
    Ok(())
}
async fn arm(
    f: &Fixture,
    graph: &Graph,
    page: RefPolicyPage,
) -> Result<PreparedCommand<RegisterRefPolicyPage>> {
    let command = f
        .client()
        .prepare_command::<RegisterRefPolicyPage>(&f.target, identity()?, page)
        .await?;
    let mut head = graph.head.lock().await;
    let registered = Box::pin(super::super::recovery::persist_full(
        &graph.prepared.base.session,
        &command,
        super::super::recovery::Kind::Policy,
        Some(
            graph
                .refusal
                .refusal_command()
                .ok_or("frozen policy fixture refusal")?,
        ),
        head.as_ref(),
        &graph.store,
        identity()?,
        0,
    ))
    .await?;
    *head = Some(registered);
    Ok(command)
}
async fn denied(
    f: &Fixture,
    command: &PreparedCommand<RegisterRefPolicyPage>,
    reason: PreparationDenial,
) -> Result {
    let before = state(&f.handle).await?;
    let phase_before = super::mandatory_registration::registration_state(f).await?;
    let value = Box::pin(command.clone().execute()).await?;
    assert_eq!(value.output, RefPolicyReply::Denied(reason));
    assert_eq!(state(&f.handle).await?, before);
    let phase_after = super::mandatory_registration::registration_state(f).await?;
    assert_ne!(phase_after, phase_before);
    let Resolution::Committed(original) = f.client().resolve(command.evidence()).await? else {
        return Err("registered negative page receipt missing".into());
    };
    assert!(matches!(
        original,
        cellule_runtime::cell::executor::StoredOutcome::Success { .. }
    ));
    let mut decoder = BoundedDecoder::new(original.result(), 512)?;
    assert_eq!(RefPolicyReply::decode(&mut decoder)?, value.output);
    decoder.finish()?;
    assert_eq!(original.commit_sequence(), value.receipt.commit_sequence);
    let replay = Box::pin(command.clone().execute()).await?;
    assert_eq!(
        (replay.output, replay.receipt),
        (value.output, value.receipt)
    );
    assert_eq!(state(&f.handle).await?, before);
    assert_eq!(
        super::mandatory_registration::registration_state(f).await?,
        phase_after
    );
    Ok(())
}
fn run_sql(oid: crate::ObjectId, context: &str, version: u64, state: &str) -> String {
    format!(
        "INSERT INTO check_runs(id,oid,context,context_version,reporter,state,version,summary,created_ms,updated_ms) VALUES(X'{}',X'{}','{context}',{version},'owner','{state}',1,'',0,0)",
        hex::encode(uuid::Uuid::new_v4().as_bytes()),
        hex::encode(oid)
    )
}
async fn protect(fixture: &Fixture, graph: &Graph) -> Result {
    edit(fixture,&format!("INSERT INTO check_contexts VALUES('ci','owner',1,1); INSERT INTO branch_rules VALUES('refs/heads/main',1,1,0,1,0,0); INSERT INTO branch_required_checks VALUES('refs/heads/main','ci'); {}; {}",run_sql(graph.tip,"ci",1,"queued"),run_sql(graph.tip,"ci",1,"success"))).await
}
async fn registered(
    fixture: &Fixture,
    graph: &Graph,
    changes: crate::PushPlan,
) -> Result<(
    RefPolicyPreparation,
    RefPolicyPage,
    PreparedCommand<RegisterRefPolicyPage>,
)> {
    fn send<T: Send>(value: T) -> T {
        value
    }
    let pending = send(graph.prepared.ref_policy_preparation(
        changes,
        graph.root.path(),
        graph.budget.clone(),
        limits(),
    ))
    .await?;
    let page = send(pending.page(&graph.prepared, 0)).await?;
    let command = arm(fixture, graph, page.clone()).await?;
    let reply = Box::pin(command.clone().execute()).await?;
    assert!(matches!(reply.output,RefPolicyReply::Registered(progress) if progress.ready()));
    Ok((pending, page, command))
}
#[tokio::test]
async fn guarded_root_binds_catalog_conditional_refs_and_original_intent_and_survives_unrelated_rebase()
-> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let (fixture, graph) = rooted(format).await?;
        protect(&fixture, &graph).await?;
        let changes = plan(vec![
            update("refs/heads/main", None, Some(graph.tip)),
            update("refs/tags/example", None, Some(graph.initial)),
        ]);
        let (pending, page, command) = registered(&fixture, &graph, changes.clone()).await?;
        let receipt = Box::pin(command.clone().execute()).await?;
        let replay = Box::pin(command.clone().execute()).await?;
        assert_eq!(
            (receipt.output, receipt.receipt),
            (replay.output, replay.receipt)
        );
        let guard = pending.ready(&graph.prepared).await?;
        let old = graph.prepared.base().refs;
        fixture
            .install_generation(2, graph.prepared.catalog(), old)
            .await?;
        let rebased = graph.prepared.reconcile().await?;
        assert!(pending.ready(&rebased).await.is_ok());
        fn send<T: Send>(value: T) -> T {
            value
        }
        let proof = send(rebased.guarded_ref_snapshot(
            &guard,
            changes.clone(),
            graph.root.path(),
            graph.budget.clone(),
            limits(),
        ))
        .await?;
        assert_eq!(proof.certificate.data()?.base.generation, 2);
        assert_eq!(
            proof.guard.plan_digest,
            super::super::ref_proof::plan_digest(&changes)?
        );
        assert_eq!(proof.snapshot.read(&graph.store).await?.generation, 1);
        let mut e = BoundedEncoder::new(2048)?;
        proof.encode(&mut e)?;
        let bytes = e.finish();
        assert!(bytes.len() < 2048);
        let mut d = BoundedDecoder::new(&bytes, 2048)?;
        assert_eq!(RefRootPublicationProof::decode(&mut d)?, proof);
        d.finish()?;
        for cut in 0..bytes.len() {
            assert!(
                RefRootPublicationProof::decode(&mut BoundedDecoder::new(&bytes[..cut], 2048)?)
                    .is_err()
            );
        }
        let before = state(&fixture.handle).await?;
        let legacy = RefPublicationProof {
            plan: changes,
            ancestry: page.proof.ancestry,
            certificate: proof.certificate,
        };
        let result = fixture
            .client()
            .command::<PublishCatalogRefs>(&fixture.target, identity()?, legacy)
            .await;
        assert!(
            matches!(result,Err(InvocationError::Rejected(value)) if value.output==PublicationReply::Denied(PreparationDenial::Unauthorized))
        );
        assert_eq!(before, state(&fixture.handle).await?);
        drop(rebased);
        close(fixture, graph).await?;
    }
    Ok(())
}

#[tokio::test]
async fn exact_check_dependencies_ignore_unrelated_and_older_reports_and_refuse_newer_or_replaced_attempts()
-> Result {
    let (fixture, graph) = rooted(ObjectFormat::Sha256).await?;
    protect(&fixture, &graph).await?;
    let changes = plan(vec![update("refs/heads/main", None, Some(graph.tip))]);
    let (pending, _, _) = registered(&fixture, &graph, changes.clone()).await?;
    assert_ne!(graph.blob, graph.tip);
    edit(&fixture,&format!("{}; {}; UPDATE check_runs SET state='failure',version=2 WHERE number=1; DELETE FROM check_runs WHERE number=1",run_sql(graph.blob,"ci",1,"success"),run_sql(graph.tip,"ci",2,"failure"))).await?;
    assert!(pending.ready(&graph.prepared).await.is_ok());
    edit(&fixture, &run_sql(graph.tip, "ci", 1, "queued")).await?;
    assert!(matches!(
        pending.ready(&graph.prepared).await,
        Err(RefPolicyPreparationError::Context)
    ));
    edit(&fixture,"UPDATE check_runs SET state='success',version=2 WHERE number=(SELECT max(number) FROM check_runs WHERE context_version=1)").await?;
    let (pending, _, _) = registered(&fixture, &graph, changes.clone()).await?;
    edit(&fixture,&format!("INSERT OR REPLACE INTO check_runs SELECT number,id,X'{}',context,context_version,reporter,state,version,summary,created_ms,updated_ms FROM check_runs WHERE oid=X'{}' AND context_version=1 ORDER BY number DESC LIMIT 1",hex::encode(graph.blob),hex::encode(graph.tip))).await?;
    assert!(matches!(
        pending.ready(&graph.prepared).await,
        Err(RefPolicyPreparationError::Context)
    ));
    edit(&fixture, &run_sql(graph.tip, "ci", 1, "success")).await?;
    let (pending, _, _) = registered(&fixture, &graph, changes.clone()).await?;
    edit(&fixture,&format!("INSERT OR REPLACE INTO check_runs(id,oid,context,context_version,reporter,state,version,summary,created_ms,updated_ms) SELECT id,X'{}',context,context_version,reporter,state,version,summary,created_ms,updated_ms FROM check_runs WHERE oid=X'{}' AND context_version=1 ORDER BY number DESC LIMIT 1",hex::encode(graph.blob),hex::encode(graph.tip))).await?;
    assert!(matches!(
        pending.ready(&graph.prepared).await,
        Err(RefPolicyPreparationError::Context)
    ));
    edit(&fixture, &run_sql(graph.tip, "ci", 1, "success")).await?;
    let (pending, _, _) = registered(&fixture, &graph, changes).await?;
    edit(
        &fixture,
        &format!(
            "DELETE FROM check_runs WHERE oid=X'{}' AND context_version=1",
            hex::encode(graph.tip)
        ),
    )
    .await?;
    assert!(matches!(
        pending.ready(&graph.prepared).await,
        Err(RefPolicyPreparationError::Context)
    ));
    close(fixture, graph).await
}
