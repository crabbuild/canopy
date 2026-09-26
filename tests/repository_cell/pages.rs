use super::*;
use canopy_server::{INLINE_OBJECT_LIMIT, ObjectBatch, ObjectStorage, StoredObject};
use cellule_runtime::{SqlBatch, SqlCell, SqlStatement, SqlValue};
use sha2::{Digest as _, Sha256};
use std::collections::BTreeMap;

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

async fn put(
    repository: &RepositoryCell,
    objects: impl IntoIterator<Item = StoredObject>,
) -> Result {
    let mut batch = ObjectBatch::default();
    for object in objects {
        assert!(batch.try_push(object).is_ok());
    }
    repository.put_objects(identity()?, batch).await?;
    Ok(())
}

async fn clear(sql: &SqlCell<RepositoryModule>) -> Result {
    // This exercise runs first in a fresh fixture and owns all these records.
    sql.batch(
        identity()?,
        SqlBatch {
            statements: [
                "DELETE FROM objects",
                "DELETE FROM object_chunks",
                "DELETE FROM object_uploads",
            ]
            .into_iter()
            .map(|sql| SqlStatement {
                sql: sql.into(),
                parameters: vec![],
            })
            .collect(),
        },
    )
    .await?;
    Ok(())
}

async fn scan(repository: &RepositoryCell) -> Result<BTreeMap<[u8; 20], StoredObject>> {
    let mut after = None;
    let mut objects = BTreeMap::new();
    for _ in 0..10 {
        let page = repository.object_page(after).await?.output;
        if page.is_empty() {
            return Ok(objects);
        }
        assert!(page.len() <= 128);
        let bytes: usize = page
            .iter()
            .map(|object| match &object.storage {
                ObjectStorage::Inline(body) => body.len(),
                _ => 0,
            })
            .sum();
        assert!(bytes <= INLINE_OBJECT_LIMIT);
        for object in page {
            assert!(after.is_none_or(|old| object.oid > old));
            after = Some(object.oid);
            assert!(objects.insert(object.oid, object).is_none());
        }
    }
    Err("object pagination did not finish".into())
}

fn before(mut oid: [u8; 20]) -> Option<[u8; 20]> {
    for byte in oid.iter_mut().rev() {
        if *byte != 0 {
            *byte -= 1;
            return Some(oid);
        }
        *byte = u8::MAX;
    }
    None
}

pub async fn exercise(repository: &RepositoryCell, sql: &SqlCell<RepositoryModule>) -> Result {
    assert!(repository.object_page(None).await?.output.is_empty());
    let mut expected = BTreeMap::new();
    for start in [0, 128] {
        let batch = (start..start + 128)
            .map(|n| {
                let body = format!("object-page-{n}").into_bytes();
                let object = inline(body.clone());
                expected.insert(object.oid, body);
                object
            })
            .collect::<Vec<_>>();
        put(repository, batch).await?;
    }
    let first = repository.object_page(None).await?.output;
    assert_eq!(first.len(), 128);
    let second = repository
        .object_page(Some(first.last().ok_or("missing first page")?.oid))
        .await?
        .output;
    assert_eq!(second.len(), 128);
    assert!(
        repository
            .object_page(Some(second.last().ok_or("missing second page")?.oid))
            .await?
            .output
            .is_empty()
    );
    let actual = scan(repository).await?;
    assert_eq!(
        actual.keys().copied().collect::<Vec<_>>(),
        expected.keys().copied().collect::<Vec<_>>()
    );
    for (oid, object) in actual {
        let ObjectStorage::Inline(body) = object.storage else {
            return Err("inline body missing".into());
        };
        assert_eq!(body, expected[&oid]);
    }
    clear(sql).await?;
    expected.clear();

    let halves: Vec<_> = [11, 12]
        .map(|byte| inline(vec![byte; INLINE_OBJECT_LIMIT / 2]))
        .into();
    for object in &halves {
        let ObjectStorage::Inline(body) = &object.storage else {
            return Err("inline body missing".into());
        };
        expected.insert(object.oid, body.clone());
    }
    put(repository, halves).await?;
    let full = repository.object_page(None).await?.output;
    assert_eq!(full.len(), 2);
    assert_eq!(
        full.iter()
            .map(|object| match &object.storage {
                ObjectStorage::Inline(body) => body.len(),
                _ => 0,
            })
            .sum::<usize>(),
        INLINE_OBJECT_LIMIT
    );
    for body in [vec![13; INLINE_OBJECT_LIMIT], vec![14], Vec::new()] {
        let object = inline(body.clone());
        expected.insert(object.oid, body);
        put(repository, [object]).await?;
    }
    let chunked = vec![15; INLINE_OBJECT_LIMIT + 1];
    let staged = repository
        .stage_object(identity()?, ObjectKind::Tag, &chunked)
        .await?;
    let chunked_oid = staged.oid;
    let ObjectStorage::Chunked { upload, .. } = staged.storage else {
        return Err("missing chunk reference".into());
    };
    put(
        repository,
        [StoredObject {
            oid: chunked_oid,
            kind: ObjectKind::Tag,
            storage: ObjectStorage::Chunked {
                upload,
                size: chunked.len() as u64,
                blake3: *blake3::hash(&chunked).as_bytes(),
            },
        }],
    )
    .await?;
    let external = vec![16; INLINE_OBJECT_LIMIT + 1];
    let external_oid = object_id(ObjectKind::Blob, &external);
    let sha256: [u8; 32] = Sha256::digest(&external).into();
    put(
        repository,
        [StoredObject {
            oid: external_oid,
            kind: ObjectKind::Blob,
            storage: ObjectStorage::External {
                size: external.len() as u64,
                blake3: *blake3::hash(&external).as_bytes(),
                sha256,
            },
        }],
    )
    .await?;
    let mut actual = scan(repository).await?;
    assert_eq!(actual.len(), expected.len() + 2);
    for (oid, expected_body) in &expected {
        let object = actual.remove(oid).ok_or("missing inline object")?;
        let ObjectStorage::Inline(body) = object.storage else {
            return Err("inline body missing".into());
        };
        assert_eq!(&body, expected_body);
    }
    let descriptor = actual
        .remove(&chunked_oid)
        .ok_or("missing chunked object")?;
    assert!(
        matches!(descriptor.storage, ObjectStorage::Chunked { upload: stored, size, blake3: digest } if stored == upload && size == chunked.len() as u64 && digest == *blake3::hash(&chunked).as_bytes())
    );
    let descriptor = actual
        .remove(&external_oid)
        .ok_or("missing external object")?;
    assert!(
        matches!(descriptor.storage, ObjectStorage::External { size, blake3: digest, sha256: stored } if size == external.len() as u64 && digest == *blake3::hash(&external).as_bytes() && stored == sha256)
    );

    let damaged = object_id(ObjectKind::Blob, &[14]);
    for (statement, parameters) in [
        (
            "UPDATE objects SET digest = zeroblob(32) WHERE oid = ?1",
            vec![SqlValue::Blob(damaged.to_vec())],
        ),
        (
            "UPDATE objects SET body = ?2, digest = ?3 WHERE oid = ?1",
            vec![
                SqlValue::Blob(damaged.to_vec()),
                SqlValue::Blob(vec![17]),
                SqlValue::Blob(blake3::hash(&[17]).as_bytes().to_vec()),
            ],
        ),
        (
            "UPDATE objects SET body = zeroblob(?2), size = ?2 WHERE oid = ?1",
            vec![
                SqlValue::Blob(damaged.to_vec()),
                SqlValue::Integer(INLINE_OBJECT_LIMIT as i64 + 1),
            ],
        ),
    ] {
        sql.batch(
            identity()?,
            SqlBatch {
                statements: vec![SqlStatement {
                    sql: statement.into(),
                    parameters,
                }],
            },
        )
        .await?;
        assert!(repository.object_page(before(damaged)).await.is_err());
    }
    clear(sql).await?;
    assert!(repository.object_page(None).await?.output.is_empty());
    Ok(())
}
