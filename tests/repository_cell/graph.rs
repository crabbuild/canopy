use std::time::{SystemTime, UNIX_EPOCH};

use canopy_server::{ObjectKind, PushPlan, RefUpdate, RepositoryCell, RepositoryModule, object_id};
use crab_cell_runtime::{
    InvocationError, MutationIdentity, SqlCell, identity::RequestId, primitives::sql::SqlBatch,
    primitives::sql::SqlStatement, primitives::sql::SqlValue,
};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

fn identity() -> Result<MutationIdentity> {
    let now = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis())?;
    Ok(MutationIdentity {
        request_id: RequestId::from_bytes(uuid::Uuid::new_v4().into_bytes()),
        issued_at_ms: now,
        expires_at_ms: now + 60_000,
    })
}

fn plan(name: &str, oid: [u8; 20]) -> PushPlan {
    PushPlan {
        actor: "canopy".into(),
        updates: vec![RefUpdate {
            name: name.into(),
            expected: None,
            new_oid: Some(oid),
        }],
    }
}

async fn put(repository: &RepositoryCell, kind: ObjectKind, body: &[u8]) -> Result<[u8; 20]> {
    Ok(super::objects::put(repository, identity()?, kind, body)
        .await?
        .output)
}

fn commit(tree: [u8; 20], parent: Option<[u8; 20]>) -> Vec<u8> {
    let parent = parent.map_or_else(String::new, |oid| format!("parent {}\n", hex::encode(oid)));
    format!("tree {}\n{parent}author Canopy <test@example.invalid> 0 +0000\ncommitter Canopy <test@example.invalid> 0 +0000\n\nGraph test\n", hex::encode(tree)).into_bytes()
}

fn entry(mode: &str, name: &[u8], oid: [u8; 20]) -> Vec<u8> {
    let mut body = format!("{mode} ").into_bytes();
    body.extend_from_slice(name);
    body.push(0);
    body.extend_from_slice(&oid);
    body
}

async fn certificates(sql: &SqlCell<RepositoryModule>) -> Result<Vec<SqlValue>> {
    let result = sql
        .query(
            None,
            SqlBatch {
                statements: vec![SqlStatement {
                    sql: "SELECT oid FROM object_closure ORDER BY oid".into(),
                    parameters: vec![],
                }],
            },
        )
        .await?;
    Ok(result
        .output
        .into_iter()
        .flat_map(|set| set.rows)
        .flatten()
        .collect())
}

pub async fn verify(
    repository: &RepositoryCell,
    sql: &SqlCell<RepositoryModule>,
    application: &crab_cell_app::ApplicationHandle<canopy_server::CanopyApplication>,
    target: &crab_cell_runtime::CellTarget,
) -> Result<()> {
    let blob_body = b"graph closure leaf";
    let blob = object_id(ObjectKind::Blob, blob_body);
    let tree_body = entry("100644", b"leaf", blob);
    let tree = object_id(ObjectKind::Tree, &tree_body);
    let root = put(repository, ObjectKind::Commit, &commit(tree, None)).await?;

    let push = plan("refs/heads/graph", root);
    assert!(matches!(
        repository.finalize_push(identity()?, push.clone()).await,
        Err(InvocationError::Rejected(_))
    ));
    assert!(
        repository
            .ref_state("refs/heads/graph", None)
            .await?
            .output
            .is_none()
    );
    put(repository, ObjectKind::Tree, &tree_body).await?;
    assert!(matches!(
        repository.finalize_push(identity()?, push.clone()).await,
        Err(InvocationError::Rejected(_))
    ));
    assert!(
        !certificates(sql)
            .await?
            .contains(&SqlValue::Blob(root.to_vec()))
    );
    put(repository, ObjectKind::Blob, blob_body).await?;
    for candidates in [vec![root, tree, blob], vec![blob, root]] {
        assert!(matches!(
            application
                .command::<CertificateCommand>(target, identity()?, CertificateInput(candidates))
                .await,
            Err(InvocationError::Rejected(_))
        ));
        assert!(
            !certificates(sql)
                .await?
                .contains(&SqlValue::Blob(blob.to_vec()))
        );
    }
    assert!(
        application
            .command::<CertificateCommand>(target, identity()?, CertificateInput(vec![blob, tree]))
            .await?
            .output
    );
    // Calling the low-level command cannot bypass certificate preparation.
    assert!(matches!(
        application
            .command::<canopy_server::FinalizePush>(target, identity()?, push.clone())
            .await,
        Err(InvocationError::Rejected(_))
    ));
    repository.finalize_push(identity()?, push).await?;
    let after = certificates(sql).await?;
    for oid in [root, tree, blob] {
        assert!(after.contains(&SqlValue::Blob(oid.to_vec())));
    }
    repository
        .finalize_push(identity()?, plan("refs/heads/graph-again", root))
        .await?;
    assert_eq!(certificates(sql).await?, after);

    let wrong_tree = entry("40000", b"directory", blob);
    let malformed_tree = b"100644 truncated\0short";
    let wrong_parent = commit(tree, Some(blob));
    let missing_parent = commit(tree, Some([71; 20]));
    let wrong_tag = format!(
        "object {}\ntype commit\ntag wrong-kind\n\n",
        hex::encode(blob)
    );
    let missing_tag = format!(
        "object {}\ntype tag\ntag missing\n\n",
        hex::encode([72; 20])
    );
    for (name, kind, body) in [
        ("wrong-tree", ObjectKind::Tree, wrong_tree.as_slice()),
        ("short-tree", ObjectKind::Tree, malformed_tree.as_slice()),
        ("wrong-parent", ObjectKind::Commit, wrong_parent.as_slice()),
        (
            "missing-parent",
            ObjectKind::Commit,
            missing_parent.as_slice(),
        ),
        ("wrong-tag", ObjectKind::Tag, wrong_tag.as_bytes()),
        ("missing-tag", ObjectKind::Tag, missing_tag.as_bytes()),
        (
            "bad-commit",
            ObjectKind::Commit,
            b"tree not-an-object-id\n".as_slice(),
        ),
    ] {
        let oid = put(repository, kind, body).await?;
        let name = format!("refs/tags/{name}");
        assert!(matches!(
            application
                .command::<CertificateCommand>(target, identity()?, CertificateInput(vec![oid]))
                .await,
            Err(InvocationError::Rejected(_))
        ));
        assert!(
            matches!(
                repository
                    .finalize_push(identity()?, plan(&name, oid))
                    .await,
                Err(InvocationError::Rejected(_))
            ),
            "{name}"
        );
        assert!(repository.ref_state(&name, None).await?.output.is_none());
    }
    assert!(matches!(
        repository
            .finalize_push(identity()?, plan("refs/heads/blob", blob))
            .await,
        Err(InvocationError::Rejected(_))
    ));
    assert_eq!(certificates(sql).await?, after);

    // A missing root must reject every ref even when another root is valid.
    // Certificate preparation is independent of ref publication.
    let independent = put(repository, ObjectKind::Blob, b"independent graph leaf").await?;
    let mut mixed = plan("refs/tags/missing-root", [73; 20]);
    mixed
        .updates
        .extend(plan("refs/tags/independent", independent).updates);
    assert!(matches!(
        repository.finalize_push(identity()?, mixed).await,
        Err(InvocationError::Rejected(_))
    ));
    assert!(
        repository
            .ref_state("refs/tags/independent", None)
            .await?
            .output
            .is_none()
    );

    // Gitlinks point outside this repository. Symlinks and non-UTF8 names
    // still point to local blobs; repeated edges can share a certified leaf.
    let mut linked_tree = entry("120000", b"link", blob);
    linked_tree.extend(entry("160000", b"submodule", [74; 20]));
    linked_tree.extend(entry("100755", b"\xff", blob));
    let linked_tree = put(repository, ObjectKind::Tree, &linked_tree).await?;
    let linked_commit = put(
        repository,
        ObjectKind::Commit,
        &commit(linked_tree, Some(root)),
    )
    .await?;
    let tag = format!(
        "object {}\ntype commit\ntag v1\n\n",
        hex::encode(linked_commit)
    );
    let tag = put(repository, ObjectKind::Tag, tag.as_bytes()).await?;
    let nested_tag = format!("object {}\ntype tag\ntag nested\n\n", hex::encode(tag));
    let nested_tag = put(repository, ObjectKind::Tag, nested_tag.as_bytes()).await?;
    repository
        .finalize_push(identity()?, plan("refs/tags/nested", nested_tag))
        .await?;
    assert!(matches!(
        repository
            .finalize_push(identity()?, plan("refs/heads/tag", tag))
            .await,
        Err(InvocationError::Rejected(_))
    ));
    resumable(repository, sql).await?;
    Ok(())
}

async fn resumable(repository: &RepositoryCell, sql: &SqlCell<RepositoryModule>) -> Result<()> {
    use canopy_server::{ObjectBatch, ObjectStorage, StoredObject};
    let mut leaves: Vec<_> = (0..270)
        .map(|index| {
            let body = format!("resumable leaf {index}").into_bytes();
            (object_id(ObjectKind::Blob, &body), body)
        })
        .collect();
    leaves.sort_by_key(|(oid, _)| *oid);
    let mut tree = Vec::new();
    for (index, (oid, _)) in leaves.iter().enumerate() {
        tree.extend(entry("100644", format!("leaf-{index:03}").as_bytes(), *oid));
    }
    let (missing, body) = leaves.pop().ok_or("missing fixture leaf")?;
    for group in leaves.chunks(128) {
        let mut batch = ObjectBatch::default();
        for (oid, body) in group {
            assert!(
                batch
                    .try_push(StoredObject {
                        oid: *oid,
                        kind: ObjectKind::Blob,
                        storage: ObjectStorage::Inline(body.clone())
                    })
                    .is_ok()
            );
        }
        repository.put_objects(identity()?, batch).await?;
    }
    let root = put(repository, ObjectKind::Tree, &tree).await?;
    let generation = repository.refs_page("", None).await?.output.generation;
    let plan = plan("refs/tags/resumable-graph", root);
    assert!(matches!(
        repository.finalize_push(identity()?, plan.clone()).await,
        Err(InvocationError::Rejected(_))
    ));
    let persisted = certificates(sql).await?;
    assert_eq!(
        leaves
            .iter()
            .filter(|(oid, _)| persisted.contains(&SqlValue::Blob(oid.to_vec())))
            .count(),
        256
    );
    assert!(!persisted.contains(&SqlValue::Blob(root.to_vec())));
    assert_eq!(
        repository.refs_page("", None).await?.output.generation,
        generation
    );
    assert!(
        repository
            .ref_state("refs/tags/resumable-graph", None)
            .await?
            .output
            .is_none()
    );
    assert_eq!(put(repository, ObjectKind::Blob, &body).await?, missing);
    repository.finalize_push(identity()?, plan).await?;
    assert_eq!(
        repository.refs_page("", None).await?.output.generation,
        generation + 1
    );
    assert_eq!(
        repository
            .ref_state("refs/tags/resumable-graph", None)
            .await?
            .output
            .and_then(|state| state.oid),
        Some(root)
    );
    let persisted = certificates(sql).await?;
    assert!(persisted.contains(&SqlValue::Blob(root.to_vec())));
    for (oid, _) in leaves {
        assert!(persisted.contains(&SqlValue::Blob(oid.to_vec())));
    }
    Ok(())
}

// A separately encoded client invokes the registered server command. Its local
// handler cannot run, so these checks exercise the actual wire trust boundary.
struct CertificateInput(Vec<[u8; 20]>);
impl crab_cell_runtime::codec::WireValue for CertificateInput {
    fn encode(
        &self,
        encoder: &mut crab_cell_runtime::codec::BoundedEncoder,
    ) -> std::result::Result<(), crab_cell_runtime::codec::CodecError> {
        encoder.write_count(self.0.len())?;
        for oid in &self.0 {
            encoder.write_bytes(oid)?;
        }
        Ok(())
    }
    fn decode(
        _: &mut crab_cell_runtime::codec::BoundedDecoder<'_>,
    ) -> std::result::Result<Self, crab_cell_runtime::codec::CodecError> {
        Err(crab_cell_runtime::codec::CodecError::Invalid(
            "client fixture is encode-only",
        ))
    }
}
struct CertificateCommand;
impl crab_cell_runtime::Command for CertificateCommand {
    const MODULE: &'static str = "repository";
    const ID: u32 = 6;
    const CODEC_VERSION: u32 = 1;
    type Input = CertificateInput;
    type Output = bool;
    fn execute(
        _: &mut crab_cell_runtime::registry::CommandContext<'_, '_>,
        _: Self::Input,
    ) -> crab_cell_runtime::Result<crab_cell_runtime::registry::CommandResult<bool>> {
        Err(crab_cell_runtime::Error::Command(
            "client fixture handler must not execute",
        ))
    }
}
