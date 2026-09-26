use std::time::{SystemTime, UNIX_EPOCH};

use canopy_server::{ObjectKind, PushPlan, RefUpdate, RepositoryCell, RepositoryModule, object_id};
use cellule_runtime::{
    InvocationError, MutationIdentity, RequestId, SqlBatch, SqlCell, SqlStatement, SqlValue,
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

pub async fn verify(repository: &RepositoryCell, sql: &SqlCell<RepositoryModule>) -> Result<()> {
    let blob_body = b"graph closure leaf";
    let blob = object_id(ObjectKind::Blob, blob_body);
    let tree_body = entry("100644", b"leaf", blob);
    let tree = object_id(ObjectKind::Tree, &tree_body);
    let root = put(repository, ObjectKind::Commit, &commit(tree, None)).await?;
    let before = certificates(sql).await?;
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
    assert_eq!(certificates(sql).await?, before);
    put(repository, ObjectKind::Blob, blob_body).await?;
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

    // The valid root is traversed first. A later missing root must roll back
    // both its new certificates and every ref, not only the invalid update.
    let independent = put(repository, ObjectKind::Blob, b"independent graph leaf").await?;
    let mut mixed = plan("refs/tags/missing-root", [73; 20]);
    mixed
        .updates
        .extend(plan("refs/tags/independent", independent).updates);
    assert!(matches!(
        repository.finalize_push(identity()?, mixed).await,
        Err(InvocationError::Rejected(_))
    ));
    assert_eq!(certificates(sql).await?, after);
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
    Ok(())
}
