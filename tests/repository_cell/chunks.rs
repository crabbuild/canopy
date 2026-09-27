use super::*;
use canopy_server::{INLINE_OBJECT_LIMIT, ObjectBatch, ObjectStorage, StoredObject};
use crab_cell_runtime::{
    SqlCell, primitives::sql::SqlBatch, primitives::sql::SqlStatement, primitives::sql::SqlValue,
};

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

fn identity() -> Result<MutationIdentity> {
    let now = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis())?;
    Ok(MutationIdentity {
        request_id: RequestId::from_bytes(uuid::Uuid::new_v4().into_bytes()),
        issued_at_ms: now,
        expires_at_ms: now + 60_000,
    })
}

fn batch(objects: impl IntoIterator<Item = StoredObject>) -> ObjectBatch {
    let mut batch = ObjectBatch::default();
    for object in objects {
        assert!(batch.try_push(object).is_ok());
    }
    batch
}

pub async fn exercise(repository: &RepositoryCell, sql: &SqlCell<RepositoryModule>) -> Result {
    let body = vec![b't'; INLINE_OBJECT_LIMIT + 1];
    let staged_id = identity()?;
    let object = repository
        .stage_object(staged_id, ObjectKind::Tag, &body)
        .await?;
    let oid = object.oid;
    assert!(repository.object(oid, None).await?.output.is_none());
    assert!(repository.existing_objects(&[oid]).await?.output.is_empty());
    let committed = repository.put_objects(identity()?, batch([object])).await?;
    assert_eq!(
        repository
            .object(oid, Some(committed.receipt))
            .await?
            .output,
        Some((ObjectKind::Tag, body.clone()))
    );
    // Stage retries replay their chunk commands; a second upload of identical bytes converges.
    for request in [staged_id, identity()?] {
        let duplicate = repository
            .stage_object(request, ObjectKind::Tag, &body)
            .await?;
        repository
            .put_objects(identity()?, batch([duplicate]))
            .await?;
    }
    let layout = sql
        .query(
            None,
            SqlBatch {
                statements: vec![SqlStatement {
                    sql: "SELECT storage, body, external_sha256, size FROM objects WHERE oid = ?1"
                        .into(),
                    parameters: vec![SqlValue::Blob(oid.to_vec())],
                }],
            },
        )
        .await?;
    assert_eq!(
        layout.output[0].rows[0],
        vec![
            SqlValue::Text("chunked".into()),
            SqlValue::Null,
            SqlValue::Null,
            SqlValue::Integer(body.len() as i64)
        ]
    );

    for case in 0..7 {
        let bytes = vec![case + 10; INLINE_OBJECT_LIMIT + 32];
        let mut object = repository
            .stage_object(identity()?, ObjectKind::Commit, &bytes)
            .await?;
        let oid = object.oid;
        let ObjectStorage::Chunked {
            upload,
            size,
            blake3,
        } = &mut object.storage
        else {
            return Err("chunk reference missing".into());
        };
        let upload = *upload;
        let statement = match case {
            0 => Some(SqlStatement { sql: "DELETE FROM object_chunks WHERE upload_id = ?1 AND part = 1".into(), parameters: vec![SqlValue::Blob(upload.to_vec())] }),
            1 => Some(SqlStatement { sql: "UPDATE object_chunks SET body = zeroblob(length(body)) WHERE upload_id = ?1 AND part = 0".into(), parameters: vec![SqlValue::Blob(upload.to_vec())] }),
            2 => Some(SqlStatement { sql: "INSERT INTO object_chunks (upload_id, part, body) VALUES (?1, 2, ?2)".into(), parameters: vec![SqlValue::Blob(upload.to_vec()), SqlValue::Blob(vec![1])] }),
            3 => { blake3[0] ^= 1; None },
            4 => { object.oid = match object.oid { canopy_server::ObjectId::Sha1(mut bytes) => { bytes[0] ^= 1; canopy_server::ObjectId::Sha1(bytes) }, canopy_server::ObjectId::Sha256(mut bytes) => { bytes[0] ^= 1; canopy_server::ObjectId::Sha256(bytes) } }; None },
            5 => { object.kind = ObjectKind::Blob; None },
            _ => { *size -= 1; None },
        };
        if let Some(statement) = statement {
            sql.batch(
                identity()?,
                SqlBatch {
                    statements: vec![statement],
                },
            )
            .await?;
        }
        let first = format!("rolled back with invalid chunks {case}").into_bytes();
        let first_id = object_id(canopy_server::ObjectFormat::Sha1, ObjectKind::Blob, &first);
        let first = StoredObject {
            oid: first_id,
            kind: ObjectKind::Blob,
            storage: ObjectStorage::Inline(first),
        };
        assert!(
            matches!(
                repository
                    .put_objects(identity()?, batch([first, object]))
                    .await,
                Err(InvocationError::Rejected(_))
            ),
            "case {case}"
        );
        assert!(
            repository
                .existing_objects(&[first_id, oid])
                .await?
                .output
                .is_empty(),
            "case {case}"
        );
    }
    // Chunk storage must preserve typed graph validation, including missing edges.
    let mut tree = Vec::new();
    for index in 0..24_000 {
        tree.extend_from_slice(format!("100644 file-{index:05}\0").as_bytes());
        tree.extend_from_slice(&[91; 20]);
    }
    let mut commit = format!("tree {}\nauthor Canopy <test@example.invalid> 0 +0000\ncommitter Canopy <test@example.invalid> 0 +0000\n\n", hex::encode([92; 20])).into_bytes();
    commit.extend(vec![b'c'; INLINE_OBJECT_LIMIT]);
    let mut tag = format!(
        "object {}\ntype commit\ntag missing\n\n",
        hex::encode([93; 20])
    )
    .into_bytes();
    tag.extend(vec![b't'; INLINE_OBJECT_LIMIT]);
    for (kind, bytes) in [
        (ObjectKind::Tree, tree),
        (ObjectKind::Commit, commit),
        (ObjectKind::Tag, tag),
    ] {
        let object = repository.stage_object(identity()?, kind, &bytes).await?;
        let name = format!("refs/tags/chunked-{}", kind.git_name());
        let plan = PushPlan {
            actor: "canopy".into(),
            updates: vec![RefUpdate {
                name: name.clone(),
                expected: None,
                new_oid: Some(object.oid),
            }],
        };
        repository.put_objects(identity()?, batch([object])).await?;
        assert!(matches!(
            repository.finalize_push(identity()?, plan).await,
            Err(InvocationError::Rejected(_))
        ));
        assert!(repository.ref_state(&name, None).await?.output.is_none());
    }
    // Corruption after publication must also be detected during recovery reads.
    sql.batch(identity()?, SqlBatch { statements: vec![SqlStatement {
        sql: "DELETE FROM object_chunks WHERE upload_id = (SELECT chunk_id FROM objects WHERE oid = ?1) AND part = 1".into(),
        parameters: vec![SqlValue::Blob(oid.to_vec())],
    }] }).await?;
    assert!(repository.object(oid, None).await.is_err());
    assert!(
        repository
            .stage_object(identity()?, ObjectKind::Blob, &body)
            .await
            .is_err()
    );
    assert!(
        repository
            .stage_object(identity()?, ObjectKind::Tree, b"small")
            .await
            .is_err()
    );
    Ok(())
}
