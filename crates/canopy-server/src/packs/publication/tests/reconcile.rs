use super::*;
use super::{
    prepare::{cleaned, opened, physical},
    publishing::{Graph, edit, plan, update},
};
use crate::{
    ObjectKind,
    git_http::GitHttpResponse,
    packs::{
        catalog::{CatalogReader, CatalogSnapshot},
        metadata::tests::limits,
        verification::{
            PhysicalVerifier,
            physical::tests::{
                independence::{git_input, upload_pair_for_operation},
                physical_limits, prepared_for_store,
            },
        },
    },
};
use canopy_object_storage::artifact::ArtifactStore;
use cellule_ltx::DiskBudget;

pub(super) async fn graph(
    fixture: &Fixture,
    provider: Arc<dyn object_store::ObjectStore>,
    store: Arc<ArtifactStore>,
    operation: [u8; 16],
    blobs: usize,
) -> Result<Graph> {
    graph_with_run_limits(fixture, provider, store, operation, blobs, limits()).await
}
pub(super) async fn graph_with_run_limits(
    fixture: &Fixture,
    provider: Arc<dyn object_store::ObjectStore>,
    store: Arc<ArtifactStore>,
    operation: [u8; 16],
    blobs: usize,
    run_limits: crate::packs::metadata::MetadataLimits,
) -> Result<Graph> {
    let (base, _, _) = opened(fixture, operation, Arc::clone(&store)).await?;
    let native = prepared_for_store(
        fixture.format,
        blobs,
        base.context().operation,
        provider,
        Arc::clone(&store),
    )
    .await?;
    let initial = native
        .fixture
        .objects
        .values()
        .find(|(object, _)| object.kind == ObjectKind::Commit)
        .ok_or("commit")?
        .0
        .oid;
    let blob = native
        .fixture
        .objects
        .values()
        .find(|(object, _)| object.kind == ObjectKind::Blob)
        .ok_or("blob")?
        .0
        .oid;
    let root = tempfile::TempDir::new()?;
    let budget = DiskBudget::new(256 << 20);
    let mut builder = CatalogPreparation::new_with_run_limits(
        root.path(),
        budget.clone(),
        base,
        limits(),
        run_limits,
    )
    .await?;
    let (witness, segments) = physical(&native, root.path(), budget.clone()).await?;
    builder.begin_pack(witness)?;
    for segment in segments {
        builder.add_segment(segment).await?;
    }
    builder.finish_pack().await?;
    Ok(Graph {
        prepared: builder.finish().await?,
        root,
        budget,
        initial,
        tip: initial,
        other: initial,
        blob,
        store,
    })
}
async fn publish(
    fixture: &Fixture,
    prepared: &PreparedCatalog,
    root: &std::path::Path,
    budget: DiskBudget,
    name: &str,
    tip: crate::ObjectId,
) -> Result<PublishedRefs> {
    let proof = Box::pin(prepared.ref_proof(
        plan(vec![update(name, None, Some(tip))]),
        root,
        budget,
        limits(),
    ))
    .await?;
    assert!(proof.certificate.bytes()?.len() <= CERTIFICATE_BYTES as usize);
    match fixture
        .client()
        .command::<PublishCatalogRefs>(&fixture.target, identity()?, proof)
        .await?
        .output
    {
        PublicationReply::Published(value) => Ok(value),
        value => Err(format!("unexpected publication {value:?}").into()),
    }
}

#[tokio::test]
async fn concurrent_native_inputs_reconcile_without_claim_and_publish_exact_network_outcome()
-> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let fixture = Fixture::new(format).await?;
        let provider: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let store = Arc::new(ArtifactStore::new(
            Arc::clone(&provider),
            fixture.repository,
        ));
        let first = graph(
            &fixture,
            Arc::clone(&provider),
            Arc::clone(&store),
            [80; 16],
            4,
        )
        .await?;
        let second = graph(
            &fixture,
            Arc::clone(&provider),
            Arc::clone(&store),
            [81; 16],
            5,
        )
        .await?;
        let third = graph(&fixture, provider, Arc::clone(&store), [82; 16], 6).await?;
        let original = second.prepared.token();
        second.prepared.attest(identity()?).await?;
        let checkpoint = fixture
            .handle
            .query(0, 4096, move |connection| {
                Ok(connection.query_row(
                    "SELECT attestation FROM catalog_leases WHERE artifact_operation=?1",
                    [original.artifact_operation.as_slice()],
                    |row| row.get::<_, Vec<u8>>(0),
                )?)
            })
            .await?;
        assert_eq!(
            publish(
                &fixture,
                &first.prepared,
                first.root.path(),
                first.budget.clone(),
                "refs/heads/a",
                first.initial
            )
            .await?
            .generation,
            1
        );
        let stale = second
            .prepared
            .ref_proof(
                plan(vec![update("refs/heads/b", None, Some(second.initial))]),
                second.root.path(),
                second.budget.clone(),
                limits(),
            )
            .await?;
        assert!(
            matches!(fixture.client().command::<PublishCatalogRefs>(&fixture.target,identity()?,stale).await,Err(InvocationError::Rejected(value)) if value.output==PublicationReply::Denied(PreparationDenial::Conflict))
        );
        let before = fixture.counts().await?;
        let reconciled = second.prepared.reconcile().await?;
        assert_eq!(fixture.counts().await?, before);
        assert_eq!(reconciled.token(), original);
        assert_eq!(reconciled.base().generation, 1);
        assert_eq!(reconciled.base.retention_floor().generation, 0);
        assert_eq!(reconciled.inputs_digest(), second.prepared.inputs_digest());
        assert_eq!(
            reconciled.inventory_digest(),
            second.prepared.inventory_digest()
        );
        assert_eq!(reconciled.object_count(), second.prepared.object_count());
        // A trusted fault fixture MAC cannot substitute the selected generation
        // for the operation's immutable original retention floor.
        let mut wrong = reconciled
            .ref_proof(
                plan(vec![update("refs/heads/b", None, Some(second.initial))]),
                second.root.path(),
                second.budget.clone(),
                limits(),
            )
            .await?;
        let mut wrong_data = wrong.certificate.data()?;
        wrong_data.retention_floor = wrong_data.base.generation;
        wrong_data.retention_certificate = wrong_data.base.certificate;
        wrong.certificate = CatalogCertificate::seal(&wrong_data, &[16; 32])?;
        let unchanged = super::publishing::state(&fixture.handle).await?;
        assert!(
            matches!(fixture.client().command::<PublishCatalogRefs>(&fixture.target,identity()?,wrong).await,Err(InvocationError::Rejected(value)) if value.output==PublicationReply::Denied(PreparationDenial::Conflict))
        );
        assert_eq!(super::publishing::state(&fixture.handle).await?, unchanged);
        assert_eq!(
            publish(
                &fixture,
                &reconciled,
                second.root.path(),
                second.budget.clone(),
                "refs/heads/b",
                second.initial
            )
            .await?
            .generation,
            2
        );
        let preserved = fixture
            .handle
            .query(0, 4096, move |connection| {
                Ok(connection.query_row(
                    "SELECT attestation FROM catalog_leases WHERE artifact_operation=?1",
                    [original.artifact_operation.as_slice()],
                    |row| row.get::<_, Vec<u8>>(0),
                )?)
            })
            .await?;
        assert_eq!(preserved, checkpoint); // immutable optional input checkpoint
        let final_catalog = third.prepared.reconcile().await?;
        assert_eq!(final_catalog.base().generation, 2);
        let mut body = Vec::new();
        for line in ["unpack ok\n", "ok refs/heads/c\n"] {
            body.extend_from_slice(format!("{:04x}{line}", line.len() + 4).as_bytes());
        }
        body.extend_from_slice(b"0000");
        let response = GitHttpResponse {
            status: 200,
            headers: vec![],
            body,
        };
        let completed = final_catalog
            .complete_push(
                identity()?,
                PushCompletionRequest {
                    plan: Some(plan(vec![update(
                        "refs/heads/c",
                        None,
                        Some(third.initial),
                    )])),
                    response: response.clone(),
                    options: vec![],
                    certificate: None,
                },
                third.root.path(),
                third.budget.clone(),
                limits(),
            )
            .await?;
        assert!(
            matches!(completed.output,CatalogCompletionReply::Completed(ref value) if value.publication.is_some_and(|result|result.generation==3)&&!value.rejected)
        );
        assert_eq!(
            final_catalog.completed_push_response(&completed).await?,
            response
        );
        let indexes = final_catalog.base.indexes();
        let files = final_catalog.base.files();
        let reader = CatalogReader::open(Arc::clone(&indexes), final_catalog.catalog()).await?;
        let tips = [first.initial, second.initial, third.initial];
        assert!(
            reader
                .headers(&tips, &*files, &*files)
                .await?
                .iter()
                .all(Option::is_some)
        );
        let snapshot = CatalogSnapshot::download(&store, final_catalog.catalog()).await?;
        let sources = indexes.sources();
        let mut cursor = sources.cursor(snapshot.sources, None)?;
        let mut namespaces = std::collections::BTreeSet::new();
        while let Some(record) = cursor.next().await? {
            namespaces.insert(record.native().operation);
        }
        assert_eq!(
            namespaces,
            [
                first.prepared.token().artifact_operation,
                second.prepared.token().artifact_operation,
                third.prepared.token().artifact_operation
            ]
            .into_iter()
            .collect()
        );
        drop(reconciled);
        drop(final_catalog);
        for graph in [first, second, third] {
            drop(graph.prepared);
            cleaned(graph.root.path(), &graph.budget).await?;
        }
        fixture.runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn reconciliation_preserves_external_dependencies_and_rejects_their_removal() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let fixture = Fixture::new(format).await?;
        let provider: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let store = Arc::new(ArtifactStore::new(
            Arc::clone(&provider),
            fixture.repository,
        ));
        let first = graph(
            &fixture,
            Arc::clone(&provider),
            Arc::clone(&store),
            [83; 16],
            4,
        )
        .await?;
        publish(
            &fixture,
            &first.prepared,
            first.root.path(),
            first.budget.clone(),
            "refs/heads/a",
            first.initial,
        )
        .await?;
        let (base, _, _) = opened(&fixture, [84; 16], Arc::clone(&store)).await?;
        let native = prepared_for_store(
            format,
            4,
            base.context().operation,
            provider,
            Arc::clone(&store),
        )
        .await?;
        let commit = native
            .fixture
            .objects
            .values()
            .find(|(object, _)| object.kind == ObjectKind::Commit)
            .ok_or("commit")?
            .0;
        let bytes = git_input(
            native.fixture.root.path(),
            &["pack-objects", "--stdout", "--no-reuse-delta"],
            format!("{}\n", hex::encode(commit.oid)).as_bytes(),
        )
        .await?;
        let descriptor =
            upload_pair_for_operation(&native, commit.oid, &bytes, base.context().operation)
                .await?;
        let root = tempfile::TempDir::new()?;
        let budget = DiskBudget::new(256 << 20);
        let mut physical = PhysicalVerifier::download(
            root.path(),
            budget.clone(),
            &store,
            descriptor,
            physical_limits(),
            crate::native_resources::NativeResources::default()
                .scope(crate::native_resources::NativeClass::Foreground),
        )
        .await?;
        let segment = physical.inspect_next_shard(1).await?;
        let mut builder =
            CatalogPreparation::new(root.path(), budget.clone(), base, limits()).await?;
        builder.begin_pack(physical.finish().await?)?;
        builder.add_segment(segment).await?;
        builder.finish_pack().await?;
        let prepared = builder.finish().await?;
        // A fixture-only new certified root changes the generation but preserves
        // canonical dependencies. Physical work and original floor are reused.
        fixture.install_catalog(2, first.prepared.catalog()).await?;
        let reconciled = prepared.reconcile().await?;
        assert_eq!(reconciled.base().generation, 2);
        assert_eq!(reconciled.base.retention_floor().generation, 1);
        assert_eq!(reconciled.object_count(), 1);
        let mut data = reconciled.certificate().await?.data()?;
        assert_eq!(data.retention_floor, 1);
        assert_eq!(
            data.retention_certificate,
            prepared.base.retention_floor().certificate
        );
        data.actor = "a".repeat(64);
        data.refs_digest = Some([1; 32]);
        data.completion_digest = Some([2; 32]);
        let maximum = CatalogCertificate::seal(&data, &[16; 32])?;
        assert!(maximum.bytes()?.len() <= CERTIFICATE_BYTES as usize);
        // Fault fixture: current certification loses the external tree. The
        // old floor still retains its bytes, but the proposed new root must not
        // silently refer to that absent dependency.
        let directory =
            crate::packs::directory::snapshot::DirectorySnapshot::empty(fixture.repository, format)
                .upload(&store, [85; 16])
                .await?;
        let empty = CatalogSnapshot {
            directory,
            sources: None,
        }
        .upload(&store, [85; 16])
        .await?;
        fixture.install_catalog(3, empty).await?;
        assert!(matches!(
            prepared.reconcile().await,
            Err(CatalogPreparationError::Closure(
                crate::packs::closure::ClosureError::Missing(_)
            ))
        ));
        assert!(prepared.ensure_live().is_ok());
        drop(reconciled);
        drop(prepared);
        cleaned(root.path(), &budget).await?;
        drop(first.prepared);
        cleaned(first.root.path(), &first.budget).await?;
        fixture.runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn reconciliation_rechecks_revocation_expiry_and_claim_without_releasing_inputs() -> Result {
    let fixture = Fixture::new(ObjectFormat::Sha1).await?;
    let provider: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let store = Arc::new(ArtifactStore::new(
        Arc::clone(&provider),
        fixture.repository,
    ));
    let graph = graph(&fixture, provider, store, [86; 16], 4).await?;
    let original = graph.prepared.token();
    let held = graph.budget.used();
    assert!(held > 0);
    edit(
        &fixture,
        "UPDATE repository_identity SET owner='replacement'",
    )
    .await?;
    assert!(graph.prepared.reconcile().await.is_err());
    assert_eq!(graph.budget.used(), held);
    edit(&fixture, "UPDATE repository_identity SET owner='owner'").await?;
    let same = graph.prepared.reconcile().await?;
    assert_eq!(same.catalog(), graph.prepared.catalog());
    fixture
        .client()
        .command::<ClaimPreparation>(&fixture.target, identity()?, request(original))
        .await?;
    assert!(graph.prepared.reconcile().await.is_err());
    drop(same);
    let fresh = super::publishing::next_graph(&fixture, &graph, [89; 16]).await?;
    let fresh_held = fresh.budget.used();
    edit(
        &fixture,
        "UPDATE catalog_operations SET expires_at_ms=0; UPDATE catalog_leases SET expires_at_ms=0;",
    )
    .await?;
    assert!(fresh.prepared.reconcile().await.is_err());
    assert_eq!(fresh.budget.used(), fresh_held);
    drop(fresh.prepared);
    cleaned(fresh.root.path(), &fresh.budget).await?;
    drop(graph.prepared);
    cleaned(graph.root.path(), &graph.budget).await?;
    fixture.runtime.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn reconciliation_rejects_canonical_body_and_graph_conflicts_in_the_new_base() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        for graph_conflict in [false, true] {
            let fixture = Fixture::new(format).await?;
            let provider: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
            let store = Arc::new(ArtifactStore::new(
                Arc::clone(&provider),
                fixture.repository,
            ));
            let incoming = graph(
                &fixture,
                Arc::clone(&provider),
                Arc::clone(&store),
                [87; 16],
                4,
            )
            .await?;
            let (base, _, _) = opened(&fixture, [88; 16], Arc::clone(&store)).await?;
            let mut fake = prepared_for_store(
                format,
                4,
                base.context().operation,
                provider,
                Arc::clone(&store),
            )
            .await?;
            let altered = fake
                .fixture
                .objects
                .values_mut()
                .find(|(object, _)| {
                    object.kind
                        == if graph_conflict {
                            ObjectKind::Tree
                        } else {
                            ObjectKind::Blob
                        }
                })
                .ok_or("altered object")?;
            if graph_conflict {
                assert!(!altered.1.is_empty());
                altered.1.clear();
            } else {
                altered.0.digest[0] ^= 1;
            }
            let root = tempfile::TempDir::new()?;
            let budget = DiskBudget::new(128 << 20);
            let mut metadata = crate::packs::metadata::MetadataBuilder::new(
                root.path(),
                budget.clone(),
                fake.fixture.identity,
                limits(),
            )?;
            crate::packs::metadata::tests::fill(
                &mut metadata,
                &fake.fixture.objects.values().cloned().collect::<Vec<_>>(),
            )?;
            let segment = Arc::new(metadata.seal(&fake.fixture.index)?);
            let mut directory = crate::packs::directory::DirectoryBuilder::new(
                root.path(),
                budget.clone(),
                fixture.repository,
                base.context().operation,
                format,
                limits(),
            )?;
            directory.add_segment(&segment)?;
            let directory = Arc::new(directory.seal()?);
            let run = Arc::clone(&directory).upload(&store).await?;
            let sources = base.indexes().sources();
            let source = sources
                .insert(
                    None,
                    base.context().operation,
                    crate::packs::sources::SourceRecord {
                        metadata: Arc::clone(&segment).upload(&store).await?,
                        pack: fake.descriptor.pack,
                        index: fake.descriptor.index,
                        pack_object_count: fake.descriptor.object_count,
                    },
                )
                .await?;
            let mut snapshot = crate::packs::directory::snapshot::DirectorySnapshot::empty(
                fixture.repository,
                format,
            );
            let indexes = base.indexes();
            let run_root = indexes
                .ranges()
                .insert(None, base.context().operation, run)
                .await?;
            snapshot.append(indexes.ranges(), run_root).await?;
            let catalog = CatalogSnapshot {
                directory: snapshot.upload(&store, base.context().operation).await?,
                sources: Some(source),
            }
            .upload(&store, base.context().operation)
            .await?;
            // Trusted fault injection, not a route for public certification:
            // source and directory agree but the canonical identity is false.
            fixture.install_catalog(1, catalog).await?;
            let held = incoming.budget.used();
            assert!(matches!(
                incoming.prepared.reconcile().await,
                Err(CatalogPreparationError::Closure(
                    crate::packs::closure::ClosureError::Metadata(
                        crate::packs::metadata::MetadataError::IdentityConflict
                    )
                ))
            ));
            assert_eq!(incoming.budget.used(), held);
            assert_eq!(incoming.prepared.base().generation, 0);
            drop(segment);
            drop(directory);
            cleaned(root.path(), &budget).await?;
            drop(incoming.prepared);
            cleaned(incoming.root.path(), &incoming.budget).await?;
            fixture.runtime.shutdown().await?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn reconciled_publication_replays_after_owner_restore_and_pending_old_proofs_are_fenced()
-> Result {
    let fixture = Fixture::new(ObjectFormat::Sha256).await?;
    let provider: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let store = Arc::new(ArtifactStore::new(
        Arc::clone(&provider),
        fixture.repository,
    ));
    let first = graph(
        &fixture,
        Arc::clone(&provider),
        Arc::clone(&store),
        [90; 16],
        4,
    )
    .await?;
    let second = graph(
        &fixture,
        Arc::clone(&provider),
        Arc::clone(&store),
        [91; 16],
        5,
    )
    .await?;
    let pending = graph(&fixture, provider, store, [92; 16], 6).await?;
    publish(
        &fixture,
        &first.prepared,
        first.root.path(),
        first.budget.clone(),
        "refs/heads/a",
        first.initial,
    )
    .await?;
    let second_ready = second.prepared.reconcile().await?;
    let proof = second_ready
        .ref_proof(
            plan(vec![update("refs/heads/b", None, Some(second.initial))]),
            second.root.path(),
            second.budget.clone(),
            limits(),
        )
        .await?;
    let mutation = identity()?;
    let committed = fixture
        .client()
        .command::<PublishCatalogRefs>(&fixture.target, mutation, proof.clone())
        .await?;
    let pending_ready = pending.prepared.reconcile().await?;
    let stale = pending_ready
        .ref_proof(
            plan(vec![update("refs/heads/c", None, Some(pending.initial))]),
            pending.root.path(),
            pending.budget.clone(),
            limits(),
        )
        .await?;
    assert_eq!(pending_ready.base().generation, 2);
    fixture.handle.drain().await?;
    fixture.runtime.shutdown().await?;
    let session = SessionId::from_bytes([93; 16]);
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
            fixture.root.path().join("reconciled-restore.sqlite"),
            Owner {
                session,
                endpoint: "https://reconciled-owner.invalid".into(),
            },
        )
        .await?;
    let client = CellClient::local(Arc::clone(&fixture.registry), handle.clone());
    let exact = client
        .command::<PublishCatalogRefs>(&fixture.target, mutation, proof.clone())
        .await?;
    assert_eq!(exact.receipt, committed.receipt);
    assert_eq!(exact.output, committed.output);
    let logical = client
        .command::<PublishCatalogRefs>(&fixture.target, identity()?, proof)
        .await?;
    assert_eq!(logical.output, committed.output);
    let before = super::publishing::state(&handle).await?;
    assert!(
        matches!(client.command::<PublishCatalogRefs>(&fixture.target,identity()?,stale).await,Err(InvocationError::Rejected(value)) if value.output==PublicationReply::Denied(PreparationDenial::Stale))
    );
    assert_eq!(super::publishing::state(&handle).await?, before);
    drop(second_ready);
    drop(pending_ready);
    for graph in [first, second, pending] {
        drop(graph.prepared);
        cleaned(graph.root.path(), &graph.budget).await?;
    }
    runtime.shutdown().await?;
    Ok(())
}
