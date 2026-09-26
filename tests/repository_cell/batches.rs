use std::time::{SystemTime, UNIX_EPOCH};

use canopy_server::{
    INLINE_OBJECT_LIMIT, ObjectBatch, ObjectKind, ObjectStorage, RepositoryCell, RepositoryModule,
    StoredObject, object_id,
};
use cellule_runtime::{
    InvocationError, MutationIdentity, RequestId, SqlBatch, SqlCell, SqlStatement, SqlValue,
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

fn inline(body: Vec<u8>) -> StoredObject {
    StoredObject {
        oid: object_id(ObjectKind::Blob, &body),
        kind: ObjectKind::Blob,
        storage: ObjectStorage::Inline(body),
    }
}

fn batch(objects: impl IntoIterator<Item = StoredObject>) -> ObjectBatch {
    let mut batch = ObjectBatch::default();
    for object in objects {
        assert!(batch.try_push(object).is_ok());
    }
    batch
}

pub async fn exercise(repository: &RepositoryCell, sql: &SqlCell<RepositoryModule>) -> Result {
    // Every inserted object becomes visible at the same publication receipt.
    let objects: Vec<_> = (0..128)
        .map(|n| inline(format!("batch-{n}").into_bytes()))
        .collect();
    let ids: Vec<_> = objects.iter().map(|object| object.oid).collect();
    let request_id = identity()?;
    let committed = repository.put_objects(request_id, batch(objects)).await?;
    for (n, oid) in ids.iter().enumerate() {
        assert_eq!(
            repository
                .object(*oid, Some(committed.receipt))
                .await?
                .output,
            Some((ObjectKind::Blob, format!("batch-{n}").into_bytes()))
        );
    }
    assert_eq!(
        repository.existing_objects(&ids).await?.output,
        ids.iter().copied().collect()
    );
    let replay = repository
        .put_objects(
            request_id,
            batch((0..128).map(|n| inline(format!("batch-{n}").into_bytes()))),
        )
        .await?;
    assert_eq!(replay.receipt, committed.receipt);

    // Two bodies exactly fill the shared payload budget without exceeding the wire limit.
    let first = vec![17; INLINE_OBJECT_LIMIT / 2];
    let second = vec![18; INLINE_OBJECT_LIMIT / 2];
    let first_id = object_id(ObjectKind::Blob, &first);
    let second_id = object_id(ObjectKind::Blob, &second);
    let stored = repository
        .put_objects(
            identity()?,
            batch([inline(first.clone()), inline(second.clone())]),
        )
        .await?;
    assert_eq!(
        repository
            .object(first_id, Some(stored.receipt))
            .await?
            .output,
        Some((ObjectKind::Blob, first))
    );
    assert_eq!(
        repository
            .object(second_id, Some(stored.receipt))
            .await?
            .output,
        Some((ObjectKind::Blob, second))
    );

    // A later invalid identity rolls back earlier valid records in the same command.
    let fresh = inline(b"must roll back with bad OID".to_vec());
    let fresh_id = fresh.oid;
    let mut wrong = inline(b"wrong object identity".to_vec());
    wrong.oid = [13; 20];
    assert!(matches!(
        repository
            .put_objects(identity()?, batch([fresh, wrong]))
            .await,
        Err(InvocationError::Rejected(_))
    ));
    assert!(
        repository
            .existing_objects(&[fresh_id])
            .await?
            .output
            .is_empty()
    );

    // Existing corrupt rows cannot be accepted just because their OID is present.
    sql.batch(
        identity()?,
        SqlBatch {
            statements: vec![SqlStatement {
                sql: "UPDATE objects SET digest = ?1 WHERE oid = ?2".into(),
                parameters: vec![
                    SqlValue::Blob(vec![91; 32]),
                    SqlValue::Blob(ids[0].to_vec()),
                ],
            }],
        },
    )
    .await?;
    let fresh = inline(b"must roll back with corrupt existing object".to_vec());
    let fresh_id = fresh.oid;
    assert!(matches!(
        repository
            .put_objects(identity()?, batch([fresh, inline(b"batch-0".to_vec())]))
            .await,
        Err(InvocationError::Rejected(_))
    ));
    assert!(
        repository
            .existing_objects(&[fresh_id])
            .await?
            .output
            .is_empty()
    );

    // External bytes are uploaded by the caller; their identity record is immutable here.
    let external = |size| StoredObject {
        oid: [27; 20],
        kind: ObjectKind::Blob,
        storage: ObjectStorage::External {
            size,
            blake3: [28; 32],
            sha256: [29; 32],
        },
    };
    repository
        .put_objects(identity()?, batch([external(1_000_000)]))
        .await?;
    let fresh = inline(b"must roll back with external identity conflict".to_vec());
    let fresh_id = fresh.oid;
    assert!(matches!(
        repository
            .put_objects(identity()?, batch([fresh, external(1_000_001)]))
            .await,
        Err(InvocationError::Rejected(_))
    ));
    assert!(
        repository
            .existing_objects(&[fresh_id])
            .await?
            .output
            .is_empty()
    );
    for object in [
        StoredObject {
            oid: [31; 20],
            kind: ObjectKind::Tree,
            storage: ObjectStorage::External {
                size: 1,
                blake3: [32; 32],
                sha256: [33; 32],
            },
        },
        external(u64::MAX),
    ] {
        assert!(matches!(
            repository.put_objects(identity()?, batch([object])).await,
            Err(InvocationError::Rejected(_))
        ));
    }
    assert!(repository.existing_objects(&[]).await.is_err());
    assert!(
        repository
            .existing_objects(&vec![[0; 20]; 129])
            .await
            .is_err()
    );
    Ok(())
}
