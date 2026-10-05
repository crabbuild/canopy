use super::prepare::{cleaned, opened, opened_native, physical};
use super::*;
use crate::packs::{
    catalog::CatalogReader,
    metadata::{MetadataError, tests::limits},
    verification::physical::tests::{Prepared, independence::git_input},
};
use crate::{ObjectId, ObjectKind, PushPlan, RefExpectation, RefUpdate};
use canopy_object_storage::artifact::{ArtifactKey, ArtifactKind};
use cellule_ltx::DiskBudget;

pub(super) struct Graph {
    pub(super) prepared: PreparedCatalog,
    pub(super) root: tempfile::TempDir,
    pub(super) budget: DiskBudget,
    pub(super) initial: ObjectId,
    pub(super) tip: ObjectId,
    pub(super) other: ObjectId,
    pub(super) blob: ObjectId,
    pub(super) store: Arc<canopy_object_storage::artifact::ArtifactStore>,
    pub(super) provider: Arc<dyn object_store::ObjectStore>,
}
pub(super) fn update(name: &str, old: Option<(ObjectId, i64)>, new: Option<ObjectId>) -> RefUpdate {
    RefUpdate {
        name: name.into(),
        expected: old.map(|(oid, version)| RefExpectation {
            oid: Some(oid),
            version,
        }),
        new_oid: new,
    }
}
pub(super) fn plan(updates: Vec<RefUpdate>) -> PushPlan {
    PushPlan {
        actor: "owner".into(),
        updates,
    }
}
pub(super) async fn assembled(
    fixture: &Fixture,
    operation: [u8; 16],
    depth: usize,
) -> Result<Graph> {
    let (mut native, base, _, _) = opened_native(fixture, operation, 4).await?;
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
    super::prepare::renewing(fixture, &base, async {
        let (tip, other) = if depth == 0 {
            (initial, initial)
        } else {
            history(&mut native, initial, depth).await?
        };
        let root = tempfile::TempDir::new()?;
        let budget = DiskBudget::new(256 << 20);
        let mut builder =
            CatalogPreparation::new(root.path(), budget.clone(), base.clone(), limits()).await?;
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
            tip,
            other,
            blob,
            store: native.store,
            provider: native.provider,
        })
    })
    .await
}
async fn history(
    native: &mut Prepared,
    initial: ObjectId,
    count: usize,
) -> Result<(ObjectId, ObjectId)> {
    let mut input = Vec::new();
    for n in 1..=count {
        let parent = if n == 1 {
            hex::encode(initial)
        } else {
            format!(":{}", n - 1)
        };
        input.extend_from_slice(format!("commit refs/heads/chain\nmark :{n}\ncommitter Test <test@example.invalid> {} +0000\ndata 1\nx\nfrom {parent}\n\n",n+1).as_bytes());
    }
    input.extend_from_slice(
        b"commit refs/heads/other\ncommitter Test <test@example.invalid> 1 +0000\ndata 1\ny\n\n",
    );
    git_input(
        native.fixture.root.path(),
        &["fast-import", "--quiet"],
        &input,
    )
    .await?;
    let parse = |bytes: Vec<u8>| -> Result<ObjectId> {
        Ok(ObjectId::try_from(
            hex::decode(String::from_utf8(bytes)?.trim())?.as_slice(),
        )?)
    };
    let tip = parse(
        git_input(
            native.fixture.root.path(),
            &["rev-parse", "refs/heads/chain"],
            b"",
        )
        .await?,
    )?;
    let other = parse(
        git_input(
            native.fixture.root.path(),
            &["rev-parse", "refs/heads/other"],
            b"",
        )
        .await?,
    )?;
    git_input(native.fixture.root.path(), &["repack", "-ad"], b"").await?;
    let path = std::fs::read_dir(native.fixture.root.path().join("objects/pack"))?
        .find_map(|entry| {
            entry
                .ok()
                .map(|entry| entry.path())
                .filter(|path| path.extension().is_some_and(|ext| ext == "idx"))
        })
        .ok_or("index")?;
    let index = crate::git_format::pack_index::PackIndex::open(&path, native.descriptor.format)?;
    let pack = std::fs::read(path.with_extension("pack"))?;
    let digest = *blake3::hash(&pack).as_bytes();
    let key = |kind| ArtifactKey {
        operation: native.descriptor.operation,
        binding_digest: digest,
        kind,
    };
    let pack = native
        .store
        .put(
            key(ArtifactKind::Pack),
            pack.len() as u64,
            digest,
            &mut &pack[..],
        )
        .await?;
    let index_bytes = std::fs::read(path)?;
    let artifact_index = native
        .store
        .put(
            key(ArtifactKind::Index),
            index_bytes.len() as u64,
            *blake3::hash(&index_bytes).as_bytes(),
            &mut &index_bytes[..],
        )
        .await?;
    native.descriptor.pack = pack;
    native.descriptor.index = artifact_index;
    native.descriptor.git_checksum = index.pack_checksum();
    native.descriptor.object_count = index.len();
    Ok((tip, other))
}
async fn proof(graph: &Graph, updates: Vec<RefUpdate>) -> Result<RefPublicationProof> {
    Ok(Box::pin(graph.prepared.ref_proof(
        plan(updates),
        graph.root.path(),
        graph.budget.clone(),
        limits(),
    ))
    .await?)
}
pub(super) async fn state(handle: &CellHandle) -> Result<Vec<u8>> {
    state_except_operation(handle, None).await
}
// Terminal merge semantics intentionally close only one exact operation.
// All other operation, root, ref, checkpoint and policy facts remain compared.
pub(super) async fn state_except_operation(
    handle: &CellHandle,
    exclude: Option<[u8; 16]>,
) -> Result<Vec<u8>> {
    Ok(handle.query(0, 64 << 10, move |connection| {
        let mut refs = connection.prepare("SELECT name,oid,version FROM refs ORDER BY name")?;
        let refs = refs.query_map([], |row| Ok((row.get::<_,String>(0)?,row.get::<_,Option<Vec<u8>>>(1)?,row.get::<_,i64>(2)?)))?.collect::<rusqlite::Result<Vec<_>>>()?;
        let mut catalog = connection.prepare("SELECT generation,catalog,certificate,refs FROM catalog_generations ORDER BY generation")?;
        let catalog = catalog.query_map([], |row| Ok((row.get::<_,u64>(0)?,row.get::<_,Option<Vec<u8>>>(1)?,row.get::<_,Option<Vec<u8>>>(2)?,row.get::<_,Option<Vec<u8>>>(3)?)))?.collect::<rusqlite::Result<Vec<_>>>()?;
        let generations = connection.query_row("SELECT (SELECT generation FROM catalog_state),(SELECT generation FROM ref_generation)", [], |row| Ok((row.get::<_,u64>(0)?,row.get::<_,u64>(1)?)))?;
        let pushes = connection.query_row("SELECT count(*) FROM pushes WHERE publication IS NOT NULL", [], |row|row.get::<_,u64>(0))?;
        let mut checkpoints=connection.prepare("SELECT artifact_operation,attestation,attestation_digest FROM catalog_leases ORDER BY artifact_operation")?;
        let checkpoints=checkpoints.query_map([],|row|Ok((row.get::<_,Vec<u8>>(0)?,row.get::<_,Option<Vec<u8>>>(1)?,row.get::<_,Option<Vec<u8>>>(2)?)))?.collect::<rusqlite::Result<Vec<_>>>()?;
        let mut initial=connection.prepare("SELECT id,actor,request_digest,verification_digest,result FROM catalog_initialization")?;
        let initial=initial.query_map([],|row|Ok((row.get::<_,Vec<u8>>(0)?,row.get::<_,String>(1)?,row.get::<_,Vec<u8>>(2)?,row.get::<_,Vec<u8>>(3)?,row.get::<_,Vec<u8>>(4)?)))?.collect::<rusqlite::Result<Vec<_>>>()?;
        let policy=connection.query_row("SELECT (SELECT version FROM ref_policy_epoch),(SELECT watches FROM ref_policy_budget)",[],|row|Ok((row.get::<_,u64>(0)?,row.get::<_,u64>(1)?)))?;
        let mut hash=blake3::Hasher::new();hash.update(b"fixture.ref-policy-state.v1\0");
        let mut guards=connection.prepare("SELECT id,scope,token,policy_epoch,total,next,valid FROM ref_policy_guards ORDER BY id")?;
        let mut rows=guards.query([])?;
        while let Some(row)=rows.next()? {
            let record=(row.get::<_,Vec<u8>>(0)?,row.get::<_,Vec<u8>>(1)?,row.get::<_,Vec<u8>>(2)?,row.get::<_,u64>(3)?,row.get::<_,u64>(4)?,row.get::<_,u64>(5)?,row.get::<_,u8>(6)?);
            hash.update(&serde_json::to_vec(&record).map_err(|_|Error::Command("fixture guard hash"))?);
        }
        hash.update(b"\0watches\0");
        let mut watches=connection.prepare("SELECT guard,oid,context,context_version,run_number FROM ref_policy_watches ORDER BY guard,oid,context,context_version,run_number")?;
        let mut rows=watches.query([])?;
        while let Some(row)=rows.next()? {
            let record=(row.get::<_,Vec<u8>>(0)?,row.get::<_,Vec<u8>>(1)?,row.get::<_,String>(2)?,row.get::<_,u64>(3)?,row.get::<_,u64>(4)?);
            hash.update(&serde_json::to_vec(&record).map_err(|_|Error::Command("fixture watch hash"))?);
        }
        let policy_hash=*hash.finalize().as_bytes();
        hash.update(b"\0completed-root-and-operation-state\0");
        let mut outcomes=connection.prepare("SELECT id,actor,request_digest,response_id,completion_digest,rejected,publication,publication_plan_digest,response_root,initial_staging,initial_preparation FROM pushes ORDER BY id")?;
        let mut rows=outcomes.query([])?;
        while let Some(row)=rows.next()? {
            let record=(row.get::<_,Vec<u8>>(0)?,row.get::<_,String>(1)?,row.get::<_,Vec<u8>>(2)?,row.get::<_,Option<Vec<u8>>>(3)?,row.get::<_,Option<Vec<u8>>>(4)?,row.get::<_,Option<i64>>(5)?,row.get::<_,Option<Vec<u8>>>(6)?,row.get::<_,Option<Vec<u8>>>(7)?,row.get::<_,Option<Vec<u8>>>(8)?,row.get::<_,Option<Vec<u8>>>(9)?,row.get::<_,Option<Vec<u8>>>(10)?);
            hash.update(&serde_json::to_vec(&record).map_err(|_|Error::Command("fixture root outcome hash"))?);
        }
        let mut operations=connection.prepare("SELECT id,actor,request_digest,artifact_operation,generation,attestation,attestation_digest FROM catalog_operations WHERE ?1 IS NULL OR id!=?1 ORDER BY id")?;
        let mut rows=operations.query([exclude.map(|id|id.to_vec())])?;
        while let Some(row)=rows.next()? {
            let record=(row.get::<_,Vec<u8>>(0)?,row.get::<_,String>(1)?,row.get::<_,Vec<u8>>(2)?,row.get::<_,Vec<u8>>(3)?,row.get::<_,Option<u64>>(4)?,row.get::<_,Option<Vec<u8>>>(5)?,row.get::<_,Option<Vec<u8>>>(6)?);
            hash.update(&serde_json::to_vec(&record).map_err(|_|Error::Command("fixture root operation hash"))?);
        }
        let mut certificates=connection.prepare("SELECT digest,push_id,actor,signer,key,size,recorded_at_ms FROM push_certificates ORDER BY digest")?;
        let mut rows=certificates.query([])?;
        while let Some(row)=rows.next()? {
            let record=(row.get::<_,Vec<u8>>(0)?,row.get::<_,Vec<u8>>(1)?,row.get::<_,String>(2)?,row.get::<_,String>(3)?,row.get::<_,String>(4)?,row.get::<_,u64>(5)?,row.get::<_,i64>(6)?);
            hash.update(&serde_json::to_vec(&record).map_err(|_|Error::Command("fixture root ownership hash"))?);
        }
        let root_hash=*hash.finalize().as_bytes();
        serde_json::to_vec(&(refs,catalog,generations,pushes,checkpoints,initial,policy,policy_hash,root_hash)).map_err(|_| Error::Command("fixture publication state"))
    }).await?)
}
fn published(reply: PublicationReply) -> Result<PublishedRefs> {
    match reply {
        PublicationReply::Published(value) => Ok(value),
        PublicationReply::Denied(reason) => Err(format!("denied {reason:?}").into()),
    }
}
async fn reject(
    fixture: &Fixture,
    input: RefPublicationProof,
    reason: PreparationDenial,
) -> Result {
    let before = state(&fixture.handle).await?;
    let result = Box::pin(fixture.client().command::<PublishCatalogRefs>(
        &fixture.target,
        identity()?,
        input,
    ))
    .await;
    assert!(
        matches!(result,Err(InvocationError::Rejected(ref value)) if value.output==PublicationReply::Denied(reason)),
        "{result:?}"
    );
    assert_eq!(state(&fixture.handle).await?, before);
    Ok(())
}
pub(super) async fn edit(fixture: &Fixture, sql: &str) -> Result {
    edit_handle(&fixture.handle, sql).await
}
pub(super) async fn edit_handle(handle: &CellHandle, sql: &str) -> Result {
    let sql = sql.to_owned();
    handle
        .execute(
            identity()?,
            Digest::from_bytes(*blake3::hash(sql.as_bytes()).as_bytes()),
            sql::now(0)?,
            sql.len(),
            0,
            move |tx| {
                tx.execute_batch(&sql)?;
                Ok(cellule_runtime::cell::executor::HandlerOutcome::Success(
                    Vec::new(),
                ))
            },
        )
        .await?;
    Ok(())
}
pub(super) async fn next_graph(
    fixture: &Fixture,
    old: &Graph,
    operation: [u8; 16],
) -> Result<Graph> {
    let (base, _, _) = opened(fixture, operation, Arc::clone(&old.store)).await?;
    let root = tempfile::TempDir::new()?;
    let budget = DiskBudget::new(256 << 20);
    let prepared = CatalogPreparation::new(root.path(), budget.clone(), base, limits())
        .await?
        .finish()
        .await?;
    Ok(Graph {
        prepared,
        root,
        budget,
        initial: old.initial,
        tip: old.tip,
        other: old.other,
        blob: old.blob,
        store: Arc::clone(&old.store),
        provider: Arc::clone(&old.provider),
    })
}

#[tokio::test]
async fn catalog_refs_publish_atomically_for_both_formats_and_replay_exact_outcomes() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let fixture = Fixture::new(format).await?;
        let graph = assembled(&fixture, [70; 16], 0).await?;
        let before = state(&fixture.handle).await?;
        let input = proof(
            &graph,
            vec![
                update("refs/heads/main", None, Some(graph.initial)),
                update("refs/tags/blob", None, Some(graph.blob)),
            ],
        )
        .await?;
        let changed_plan = proof(
            &graph,
            vec![update("refs/heads/other", None, Some(graph.initial))],
        )
        .await?;
        assert_eq!(state(&fixture.handle).await?, before);
        let mut e = BoundedEncoder::new(4 << 20)?;
        input.encode(&mut e)?;
        let bytes = e.finish();
        let mut d = BoundedDecoder::new(&bytes, 4 << 20)?;
        assert_eq!(RefPublicationProof::decode(&mut d)?, input);
        d.finish()?;
        let mutation = identity()?;
        let first = Box::pin(fixture.client().command::<PublishCatalogRefs>(
            &fixture.target,
            mutation,
            input.clone(),
        ))
        .await?;
        let result = published(first.output)?;
        assert_eq!((result.generation, result.ref_generation), (1, 1));
        assert_eq!(
            result.certificate_digest,
            *blake3::hash(&input.certificate.bytes()?).as_bytes()
        );
        assert_eq!(fixture.counts().await?, (0, 1));
        let after = state(&fixture.handle).await?;
        let replay = Box::pin(fixture.client().command::<PublishCatalogRefs>(
            &fixture.target,
            mutation,
            input.clone(),
        ))
        .await?;
        assert_eq!(replay.output, first.output);
        assert_eq!(replay.receipt, first.receipt);
        let logical = Box::pin(fixture.client().command::<PublishCatalogRefs>(
            &fixture.target,
            identity()?,
            input,
        ))
        .await?;
        assert_eq!(logical.output, first.output);
        assert_eq!(state(&fixture.handle).await?, after);
        reject(&fixture, changed_plan, PreparationDenial::Conflict).await?;
        let (_, files, indexes) = opened(&fixture, [71; 16], Arc::clone(&graph.store)).await?;
        let reader = CatalogReader::open(indexes, graph.prepared.catalog()).await?;
        let headers = reader
            .headers(&[graph.initial, graph.blob], &*files, &*files)
            .await?;
        assert!(headers.iter().all(Option::is_some));
        drop(graph.prepared);
        cleaned(graph.root.path(), &graph.budget).await?;
        fixture.runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn catalog_ref_membership_kind_and_tampered_bindings_cannot_publish() -> Result {
    let fixture = Fixture::new(ObjectFormat::Sha1).await?;
    let graph = assembled(&fixture, [72; 16], 0).await?;
    for updates in [
        vec![update("refs/heads/main", None, Some(graph.blob))],
        vec![update(
            "refs/heads/main",
            None,
            Some(ObjectId::Sha1([99; 20])),
        )],
        vec![update("refs/canopy/forbidden", None, Some(graph.initial))],
        vec![
            update("refs/heads/main", None, Some(graph.initial)),
            update("refs/heads/main", None, Some(graph.initial)),
        ],
    ] {
        assert!(matches!(
            graph
                .prepared
                .ref_proof(
                    plan(updates),
                    graph.root.path(),
                    graph.budget.clone(),
                    limits()
                )
                .await,
            Err(super::super::RefProofError::Invalid)
        ));
    }
    let input = proof(
        &graph,
        vec![update("refs/heads/main", None, Some(graph.initial))],
    )
    .await?;
    let mut edited = input.clone();
    edited.plan.updates[0].name = "refs/heads/edited".into();
    reject(&fixture, edited, PreparationDenial::Unauthorized).await?;
    let mut edited = input.clone();
    edited.ancestry[0] = 0;
    reject(&fixture, edited, PreparationDenial::Unauthorized).await?;
    let mut edited = input.clone();
    // Change a signed digest byte while preserving both optional-field flags.
    let digest_byte = edited.certificate.0.body.len() - 2;
    edited.certificate.0.body[digest_byte] ^= 1;
    reject(&fixture, edited, PreparationDenial::Unauthorized).await?;
    let mut edited = input.clone();
    edited.certificate = graph.prepared.certificate().await?;
    reject(&fixture, edited, PreparationDenial::Unauthorized).await?;
    let mut invalid = input.clone();
    invalid.ancestry[0] |= 0x80;
    let mut e = BoundedEncoder::new(4 << 20)?;
    assert!(invalid.encode(&mut e).is_err());
    drop(graph.prepared);
    cleaned(graph.root.path(), &graph.budget).await?;
    fixture.runtime.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn final_current_policies_use_certified_ancestry_and_current_check_versions() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let fixture = Fixture::new(format).await?;
        let graph = assembled(&fixture, [73; 16], 1200).await?;
        let initial = proof(
            &graph,
            vec![
                update("refs/heads/main", None, Some(graph.initial)),
                update("refs/heads/other", None, Some(graph.other)),
            ],
        )
        .await?;
        fixture
            .client()
            .command::<PublishCatalogRefs>(&fixture.target, identity()?, initial)
            .await?;
        edit(&fixture,"INSERT INTO branch_rules VALUES('refs/heads/main',1,1,1,1,0,0); INSERT INTO branch_rules VALUES('refs/heads/other',1,1,1,1,0,0);").await?;
        let next = next_graph(&fixture, &graph, [74; 16]).await?;
        let combined = proof(
            &next,
            vec![
                update("refs/heads/main", Some((graph.initial, 1)), Some(graph.tip)),
                update("refs/heads/other", Some((graph.other, 1)), Some(graph.tip)),
            ],
        )
        .await?;
        assert!(super::super::ref_proof::proven(&combined.ancestry, 0));
        assert!(!super::super::ref_proof::proven(&combined.ancestry, 1));
        reject(&fixture, combined, PreparationDenial::Conflict).await?;
        let valid = proof(
            &next,
            vec![update(
                "refs/heads/main",
                Some((graph.initial, 1)),
                Some(graph.tip),
            )],
        )
        .await?;
        edit(&fixture,"INSERT INTO check_contexts VALUES('test','ci',1,1); INSERT INTO branch_required_checks VALUES('refs/heads/main','test');").await?;
        reject(&fixture, valid.clone(), PreparationDenial::Conflict).await?;
        let oid = hex::encode(graph.tip);
        edit(&fixture,&format!("INSERT INTO check_runs(id,oid,context,context_version,reporter,state,version,summary,created_ms,updated_ms) VALUES(zeroblob(16),x'{oid}','test',1,'ci','success',1,'',0,0);")).await?;
        // A current reporter/context change invalidates an earlier successful run.
        edit(
            &fixture,
            "UPDATE check_contexts SET version=2,reporter='ci-new' WHERE name='test';",
        )
        .await?;
        reject(&fixture, valid.clone(), PreparationDenial::Conflict).await?;
        edit(&fixture,&format!("INSERT INTO check_runs(id,oid,context,context_version,reporter,state,version,summary,created_ms,updated_ms) VALUES(x'01010101010101010101010101010101',x'{oid}','test',2,'ci-new','success',1,'',0,0);")).await?;
        let result = published(
            fixture
                .client()
                .command::<PublishCatalogRefs>(&fixture.target, identity()?, valid)
                .await?
                .output,
        )?;
        assert_eq!((result.generation, result.ref_generation), (2, 2));
        drop(graph.prepared);
        cleaned(graph.root.path(), &graph.budget).await?;
        drop(next.prepared);
        cleaned(next.root.path(), &next.budget).await?;
        fixture.runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn ancestry_growth_reuses_pairs_only_in_one_exact_native_catalog() -> Result {
    use super::super::ref_proof::{RefProofError, ancestry::Walker};
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let fixture = Fixture::new(format).await?;
        let graph = assembled(&fixture, [151; 16], 1200).await?;
        let reader =
            CatalogReader::open(graph.prepared.base.indexes(), graph.prepared.catalog()).await?;
        let files = graph.prepared.base.files();
        let root = tempfile::TempDir::new()?;
        let budget = DiskBudget::new(4 << 20);
        let mut walk = Walker::new(root.path(), budget.clone(), limits()).await?;
        assert_eq!(
            budget.used(),
            crate::packs::metadata::growth::INITIAL_BYTES * 3
        );
        assert!(
            walk.is_ancestor(
                &reader,
                &files,
                graph.initial,
                graph.tip,
                &graph.prepared.base
            )
            .await?
        );
        assert!(
            !walk
                .is_ancestor(
                    &reader,
                    &files,
                    graph.other,
                    graph.tip,
                    &graph.prepared.base
                )
                .await?
        );
        assert!(budget.used() > crate::packs::metadata::growth::INITIAL_BYTES * 3);
        // Both cached answers retain their meaning after another queue was used.
        assert!(
            walk.is_ancestor(
                &reader,
                &files,
                graph.initial,
                graph.tip,
                &graph.prepared.base
            )
            .await?
        );
        assert!(
            !walk
                .is_ancestor(
                    &reader,
                    &files,
                    graph.other,
                    graph.tip,
                    &graph.prepared.base
                )
                .await?
        );
        let other = assembled(&fixture, [152; 16], 4).await?;
        let foreign =
            CatalogReader::open(other.prepared.base.indexes(), other.prepared.catalog()).await?;
        assert!(matches!(
            walk.is_ancestor(
                &foreign,
                &other.prepared.base.files(),
                other.initial,
                other.tip,
                &other.prepared.base
            )
            .await,
            Err(super::super::RefProofError::Invalid)
        ));
        assert!(matches!(
            walk.is_ancestor(
                &reader,
                &files,
                graph.initial,
                graph.tip,
                &graph.prepared.base
            )
            .await,
            Err(RefProofError::Canceled)
        ));
        drop(walk);
        cleaned(root.path(), &budget).await?;
        drop((reader, foreign, files, graph.prepared, other.prepared));
        cleaned(graph.root.path(), &graph.budget).await?;
        cleaned(other.root.path(), &other.budget).await?;
        fixture.runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn denied_native_ancestry_growth_cannot_become_a_negative_or_reused_answer() -> Result {
    use super::super::ref_proof::{RefProofError, ancestry::Walker};
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let fixture = Fixture::new(format).await?;
        let graph = assembled(&fixture, [153; 16], 1200).await?;
        let reader =
            CatalogReader::open(graph.prepared.base.indexes(), graph.prepared.catalog()).await?;
        let files = graph.prepared.base.files();
        let root = tempfile::TempDir::new()?;
        let budget = DiskBudget::new(crate::packs::metadata::growth::INITIAL_BYTES * 3);
        let mut walk = Walker::new(root.path(), budget.clone(), limits()).await?;
        assert!(matches!(
            walk.is_ancestor(
                &reader,
                &files,
                graph.other,
                graph.tip,
                &graph.prepared.base
            )
            .await,
            Err(RefProofError::Metadata(MetadataError::Budget(_)))
        ));
        assert_eq!(
            budget.used(),
            crate::packs::metadata::growth::INITIAL_BYTES * 3
        );
        assert!(matches!(
            walk.is_ancestor(
                &reader,
                &files,
                graph.initial,
                graph.tip,
                &graph.prepared.base
            )
            .await,
            Err(RefProofError::Canceled)
        ));
        drop(walk);
        cleaned(root.path(), &budget).await?;
        drop((reader, files, graph.prepared));
        cleaned(graph.root.path(), &graph.budget).await?;
        fixture.runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn canceled_native_ancestry_fences_reuse_and_retains_queued_worker_credit() -> Result {
    use super::super::ref_proof::{RefProofError, ancestry::Walker};
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let fixture = Fixture::new(format).await?;
        let graph = assembled(&fixture, [154; 16], 4).await?;
        let reader =
            CatalogReader::open(graph.prepared.base.indexes(), graph.prepared.catalog()).await?;
        let files = graph.prepared.base.files();
        // Warm membership so cancellation occurs with the walk awaiting scratch.
        reader
            .headers(&[graph.initial, graph.tip], &*files, &*files)
            .await?;
        let root = tempfile::TempDir::new()?;
        let budget = DiskBudget::new(4 << 20);
        let mut walk = Walker::new(root.path(), budget.clone(), limits()).await?;
        let (entered, started) = tokio::sync::oneshot::channel();
        let (release, gate) = std::sync::mpsc::channel();
        let worker = walk.test_blocker(entered, gate);
        started.await?;
        let held = budget.used();
        let mut pending = Box::pin(walk.is_ancestor(
            &reader,
            &files,
            graph.initial,
            graph.tip,
            &graph.prepared.base,
        ));
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(25), &mut pending)
                .await
                .is_err()
        );
        drop(pending);
        assert!(matches!(
            walk.is_ancestor(
                &reader,
                &files,
                graph.initial,
                graph.tip,
                &graph.prepared.base
            )
            .await,
            Err(RefProofError::Canceled)
        ));
        drop(walk);
        assert_eq!(budget.used(), held);
        assert_eq!(std::fs::read_dir(root.path())?.count(), 1);
        release.send(())?;
        worker.await??;
        cleaned(root.path(), &budget).await?;
        drop((reader, files, graph.prepared));
        cleaned(graph.root.path(), &graph.budget).await?;
        fixture.runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn later_policy_changes_and_late_transaction_failures_publish_nothing() -> Result {
    let fixture = Fixture::new(ObjectFormat::Sha1).await?;
    let graph = assembled(&fixture, [75; 16], 8).await?;
    let initial = proof(
        &graph,
        vec![update("refs/heads/main", None, Some(graph.initial))],
    )
    .await?;
    fixture
        .client()
        .command::<PublishCatalogRefs>(&fixture.target, identity()?, initial)
        .await?;
    let next = next_graph(&fixture, &graph, [76; 16]).await?;
    let no_ancestry = proof(
        &next,
        vec![update(
            "refs/heads/main",
            Some((graph.initial, 1)),
            Some(graph.tip),
        )],
    )
    .await?;
    assert!(!super::super::ref_proof::proven(&no_ancestry.ancestry, 0));
    edit(
        &fixture,
        "INSERT INTO branch_rules VALUES('refs/heads/main',1,1,1,1,0,0);",
    )
    .await?;
    reject(&fixture, no_ancestry, PreparationDenial::Conflict).await?;
    let valid = proof(
        &next,
        vec![update(
            "refs/heads/main",
            Some((graph.initial, 1)),
            Some(graph.tip),
        )],
    )
    .await?;
    edit(&fixture,"UPDATE branch_rules SET version=2,require_pull_request=1 WHERE reference='refs/heads/main';").await?;
    reject(&fixture, valid.clone(), PreparationDenial::Conflict).await?;
    edit(&fixture,"UPDATE branch_rules SET version=3,require_pull_request=0 WHERE reference='refs/heads/main'; CREATE TRIGGER forced_publication_failure BEFORE UPDATE OF publication ON pushes BEGIN SELECT RAISE(ABORT,'forced late publication failure'); END;").await?;
    let before = state(&fixture.handle).await?;
    let command = fixture
        .client()
        .prepare_command::<PublishCatalogRefs>(&fixture.target, identity()?, valid)
        .await?;
    let evidence = command.evidence().clone();
    let failed = command.clone().execute().await;
    assert!(
        matches!(&failed, Err(InvocationError::NotStarted(error)) if format!("{error:?}").contains("forced late publication failure")),
        "{failed:?}"
    );
    assert!(matches!(
        fixture.client().resolve(&evidence).await?,
        cellule_runtime::Resolution::Absent
    ));
    assert_eq!(state(&fixture.handle).await?, before);
    edit(&fixture, "DROP TRIGGER forced_publication_failure;").await?;
    command.execute().await?;
    drop(graph.prepared);
    cleaned(graph.root.path(), &graph.budget).await?;
    drop(next.prepared);
    cleaned(next.root.path(), &next.budget).await?;
    fixture.runtime.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn catalog_cas_and_authority_races_preserve_inputs_and_optional_checkpoints() -> Result {
    let fixture = Fixture::new(ObjectFormat::Sha1).await?;
    let first = assembled(&fixture, [77; 16], 0).await?;
    let second = assembled(&fixture, [78; 16], 0).await?;
    let input = proof(
        &first,
        vec![update("refs/heads/main", None, Some(first.initial))],
    )
    .await?;
    let stale = proof(
        &second,
        vec![update("refs/heads/second", None, Some(second.initial))],
    )
    .await?;
    // A catalog-only checkpoint remains immutable and compatible with the
    // stronger final ref proof; it adds no third command to the ordinary path.
    first.prepared.attest(identity()?).await?;
    let mutation = identity()?;
    let committed = fixture
        .client()
        .command::<PublishCatalogRefs>(&fixture.target, mutation, input.clone())
        .await?;
    reject(&fixture, stale, PreparationDenial::Conflict).await?;
    let next = next_graph(&fixture, &first, [79; 16]).await?;
    let pending = proof(
        &next,
        vec![update("refs/heads/pending", None, Some(first.initial))],
    )
    .await?;
    edit(
        &fixture,
        "UPDATE repository_identity SET owner='successor' WHERE singleton=1;",
    )
    .await?;
    reject(&fixture, pending, PreparationDenial::Unauthorized).await?;
    let after = state(&fixture.handle).await?;
    let replay = fixture
        .client()
        .command::<PublishCatalogRefs>(&fixture.target, mutation, input.clone())
        .await?;
    assert_eq!(replay.output, committed.output);
    assert_eq!(replay.receipt, committed.receipt);
    let logical = fixture
        .client()
        .command::<PublishCatalogRefs>(&fixture.target, identity()?, input)
        .await?;
    assert_eq!(logical.output, committed.output);
    assert_eq!(state(&fixture.handle).await?, after);
    drop(first.prepared);
    cleaned(first.root.path(), &first.budget).await?;
    drop(second.prepared);
    cleaned(second.root.path(), &second.budget).await?;
    drop(next.prepared);
    cleaned(next.root.path(), &next.budget).await?;
    fixture.runtime.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn publication_acknowledgements_survive_owner_restore_and_stale_attempts_cannot_write()
-> Result {
    let fixture = Fixture::new(ObjectFormat::Sha1).await?;
    let first = assembled(&fixture, [80; 16], 0).await?;
    let second = assembled(&fixture, [81; 16], 0).await?;
    let input = proof(
        &first,
        vec![update("refs/heads/main", None, Some(first.initial))],
    )
    .await?;
    let pending = proof(
        &second,
        vec![update("refs/heads/pending", None, Some(second.initial))],
    )
    .await?;
    let mutation = identity()?;
    let committed = fixture
        .client()
        .command::<PublishCatalogRefs>(&fixture.target, mutation, input.clone())
        .await?;
    let before = state(&fixture.handle).await?;
    fixture.handle.drain().await?;
    fixture.runtime.shutdown().await?;
    let session = SessionId::from_bytes([82; 16]);
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
            fixture.root.path().join("publication-b.sqlite"),
            Owner {
                session,
                endpoint: "https://publication-b.invalid".into(),
            },
        )
        .await?;
    assert!(handle.owner_fence().epoch > first.prepared.token().owner.epoch);
    let client = CellClient::local(Arc::clone(&fixture.registry), handle.clone());
    assert_eq!(state(&handle).await?, before);
    let replay = client
        .command::<PublishCatalogRefs>(&fixture.target, mutation, input.clone())
        .await?;
    assert_eq!(replay.output, committed.output);
    assert_eq!(replay.receipt, committed.receipt);
    let logical = client
        .command::<PublishCatalogRefs>(&fixture.target, identity()?, input)
        .await?;
    assert_eq!(logical.output, committed.output);
    let stale = client
        .command::<PublishCatalogRefs>(&fixture.target, identity()?, pending)
        .await;
    assert!(
        matches!(stale,Err(InvocationError::Rejected(ref value)) if value.output==PublicationReply::Denied(PreparationDenial::Stale)),
        "{stale:?}"
    );
    assert_eq!(state(&handle).await?, before);
    drop(first.prepared);
    cleaned(first.root.path(), &first.budget).await?;
    drop(second.prepared);
    cleaned(second.root.path(), &second.budget).await?;
    runtime.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn expired_and_claimed_proofs_and_mutable_publication_facts_fail_closed() -> Result {
    let fixture = Fixture::new(ObjectFormat::Sha1).await?;
    let graph = assembled(&fixture, [83; 16], 0).await?;
    let input = proof(
        &graph,
        vec![update("refs/heads/main", None, Some(graph.initial))],
    )
    .await?;
    edit(
        &fixture,
        "UPDATE catalog_operations SET expires_at_ms=0; UPDATE catalog_leases SET expires_at_ms=0;",
    )
    .await?;
    reject(&fixture, input.clone(), PreparationDenial::Expired).await?;
    fixture
        .client()
        .command::<ClaimPreparation>(
            &fixture.target,
            identity()?,
            request(graph.prepared.token()),
        )
        .await?;
    reject(&fixture, input, PreparationDenial::Stale).await?;
    let fresh = assembled(&fixture, [84; 16], 0).await?;
    let input = proof(
        &fresh,
        vec![update("refs/heads/main", None, Some(fresh.initial))],
    )
    .await?;
    fixture
        .client()
        .command::<PublishCatalogRefs>(&fixture.target, identity()?, input)
        .await?;
    let before = state(&fixture.handle).await?;
    for sql in [
        "UPDATE pushes SET publication=zeroblob(53)",
        "UPDATE pushes SET publication_plan_digest=zeroblob(32)",
        "UPDATE pushes SET actor='outsider'",
        "INSERT OR REPLACE INTO pushes SELECT * FROM pushes",
        "INSERT OR REPLACE INTO catalog_generations SELECT * FROM catalog_generations WHERE generation=1",
    ] {
        assert!(edit(&fixture, sql).await.is_err(), "{sql}");
        assert_eq!(state(&fixture.handle).await?, before);
    }
    drop(graph.prepared);
    cleaned(graph.root.path(), &graph.budget).await?;
    drop(fresh.prepared);
    cleaned(fresh.root.path(), &fresh.budget).await?;
    fixture.runtime.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn reviewed_merge_requires_native_ancestry_without_a_fast_forward_branch_rule() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let fixture = Fixture::new(format).await?;
        let graph = assembled(&fixture, [241; 16], 16).await?;
        let changes = plan(vec![
            update(
                "refs/heads/merge",
                Some((graph.initial, 1)),
                Some(graph.tip),
            ),
            update(
                "refs/heads/unrelated",
                Some((graph.initial, 1)),
                Some(graph.other),
            ),
            update(
                "refs/heads/backwards",
                Some((graph.tip, 1)),
                Some(graph.initial),
            ),
            update(
                "refs/heads/unchanged",
                Some((graph.tip, 1)),
                Some(graph.tip),
            ),
            update("refs/heads/created", None, Some(graph.tip)),
            update("refs/heads/deleted", Some((graph.initial, 1)), None),
        ]);
        let before = state(&fixture.handle).await?;
        let proof = graph
            .prepared
            .ref_proof_with_required_ancestry(
                changes.clone(),
                graph.root.path(),
                graph.budget.clone(),
                limits(),
            )
            .await?;
        assert_eq!(
            proof.ancestry,
            vec![0b0011_1001],
            "merges require native ancestry evidence even without a branch fast-forward rule"
        );
        assert_eq!(
            proof.certificate.data()?.refs_digest,
            Some(super::super::ref_proof::binding(&changes, &proof.ancestry)?)
        );
        assert_eq!(
            state(&fixture.handle).await?,
            before,
            "proof construction must not publish or populate SQL refs"
        );
        let selective = graph
            .prepared
            .ref_proof(
                changes.clone(),
                graph.root.path(),
                graph.budget.clone(),
                limits(),
            )
            .await?;
        assert_eq!(
            selective.ancestry,
            vec![0b0011_1000],
            "ordinary pushes must retain policy-driven ancestry work"
        );
        assert_ne!(
            proof.certificate.data()?.refs_digest,
            selective.certificate.data()?.refs_digest
        );
        let mut forged = proof.clone();
        forged.ancestry[0] |= 2;
        assert_ne!(
            forged.certificate.data()?.refs_digest,
            Some(super::super::ref_proof::binding(
                &forged.plan,
                &forged.ancestry
            )?),
            "an unrelated history cannot become proven by changing transport bits"
        );
        let invalid = plan(vec![update(
            "refs/heads/not-a-commit",
            Some((graph.initial, 1)),
            Some(graph.blob),
        )]);
        assert!(matches!(
            graph
                .prepared
                .ref_proof_with_required_ancestry(
                    invalid,
                    graph.root.path(),
                    graph.budget.clone(),
                    limits(),
                )
                .await,
            Err(super::super::RefProofError::Invalid)
        ));
        assert_eq!(state(&fixture.handle).await?, before);

        drop(graph.prepared);
        cleaned(graph.root.path(), &graph.budget).await?;
        fixture.runtime.shutdown().await?;
    }
    Ok(())
}
