use super::*;
use super::{
    prepare::{cleaned, opened},
    publishing::{Graph, edit, plan, state, update},
    reconcile::graph,
};
use crate::packs::metadata::tests::limits;
use canopy_object_storage::artifact::ArtifactStore;
use cellule_ltx::DiskBudget;

mod cleanup;
mod freshness;
mod pages;

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
    fixture
        .client()
        .command::<InitializeCatalogRefs>(&fixture.target, identity()?, proof)
        .await?;
    drop(empty);
    cleaned(root.path(), &budget).await?;
    let graph = graph(&fixture, provider, store, [231; 16], 4).await?;
    Ok((fixture, graph))
}
async fn close(fixture: Fixture, graph: Graph) -> Result {
    drop(graph.prepared);
    cleaned(graph.root.path(), &graph.budget).await?;
    fixture.runtime.shutdown().await?;
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
) -> Result<(RefPolicyPreparation, RefPolicyPage)> {
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
    let reply = fixture
        .client()
        .command::<RegisterRefPolicyPage>(&fixture.target, identity()?, page.clone())
        .await?;
    assert!(matches!(reply.output,RefPolicyReply::Registered(progress) if progress.ready()));
    Ok((pending, page))
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
        let (pending, page) = registered(&fixture, &graph, changes.clone()).await?;
        let receipt = fixture
            .client()
            .command::<RegisterRefPolicyPage>(&fixture.target, identity()?, page.clone())
            .await?;
        let replay = fixture
            .client()
            .command::<RegisterRefPolicyPage>(&fixture.target, identity()?, page.clone())
            .await?;
        assert_eq!(receipt.output, replay.output);
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
    let (pending, _) = registered(&fixture, &graph, changes.clone()).await?;
    assert_ne!(graph.blob, graph.tip);
    edit(&fixture,&format!("{}; {}; UPDATE check_runs SET state='failure',version=2 WHERE number=1; DELETE FROM check_runs WHERE number=1",run_sql(graph.blob,"ci",1,"success"),run_sql(graph.tip,"ci",2,"failure"))).await?;
    assert!(pending.ready(&graph.prepared).await.is_ok());
    edit(&fixture, &run_sql(graph.tip, "ci", 1, "queued")).await?;
    assert!(matches!(
        pending.ready(&graph.prepared).await,
        Err(RefPolicyPreparationError::Context)
    ));
    edit(&fixture,"UPDATE check_runs SET state='success',version=2 WHERE number=(SELECT max(number) FROM check_runs WHERE context_version=1)").await?;
    let (pending, _) = registered(&fixture, &graph, changes.clone()).await?;
    edit(&fixture,&format!("INSERT OR REPLACE INTO check_runs SELECT number,id,X'{}',context,context_version,reporter,state,version,summary,created_ms,updated_ms FROM check_runs WHERE oid=X'{}' AND context_version=1 ORDER BY number DESC LIMIT 1",hex::encode(graph.blob),hex::encode(graph.tip))).await?;
    assert!(matches!(
        pending.ready(&graph.prepared).await,
        Err(RefPolicyPreparationError::Context)
    ));
    edit(&fixture, &run_sql(graph.tip, "ci", 1, "success")).await?;
    let (pending, _) = registered(&fixture, &graph, changes.clone()).await?;
    edit(&fixture,&format!("INSERT OR REPLACE INTO check_runs(id,oid,context,context_version,reporter,state,version,summary,created_ms,updated_ms) SELECT id,X'{}',context,context_version,reporter,state,version,summary,created_ms,updated_ms FROM check_runs WHERE oid=X'{}' AND context_version=1 ORDER BY number DESC LIMIT 1",hex::encode(graph.blob),hex::encode(graph.tip))).await?;
    assert!(matches!(
        pending.ready(&graph.prepared).await,
        Err(RefPolicyPreparationError::Context)
    ));
    edit(&fixture, &run_sql(graph.tip, "ci", 1, "success")).await?;
    let (pending, _) = registered(&fixture, &graph, changes).await?;
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
