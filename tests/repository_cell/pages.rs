use super::*;
use canopy_server::{ObjectBatch, ObjectStorage, StoredObject};
use cellule_runtime::{
    SqlCell,
    primitives::sql::{SqlBatch, SqlStatement, SqlValue},
};
use std::collections::BTreeMap;
type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

pub async fn exercise(repository: &RepositoryCell, sql: &SqlCell<RepositoryModule>) -> Result {
    let mut expected = BTreeMap::new();
    for group in 0..3 {
        let mut batch = ObjectBatch::default();
        for n in 0..512 {
            let body = format!("metadata-page-{group}-{n}").into_bytes();
            let oid = object_id(repository.object_format(), ObjectKind::Blob, &body);
            expected.insert(oid, body.clone());
            assert!(
                batch
                    .try_push(StoredObject {
                        oid,
                        kind: ObjectKind::Blob,
                        storage: ObjectStorage::Input(body)
                    })
                    .is_ok()
            );
        }
        repository
            .put_objects(super::batches::identity()?, batch)
            .await?;
    }
    let mut after = None;
    let mut actual = BTreeMap::new();
    let mut pages = 0;
    loop {
        let page = repository.object_page(after).await?.output;
        if page.is_empty() {
            break;
        }
        assert!(page.len() <= 512);
        pages += 1;
        for object in page {
            assert!(after.is_none_or(|prior| object.oid > prior));
            assert!(matches!(object.storage, ObjectStorage::Packed(_)));
            let (_, body) = repository
                .object(object.oid, None)
                .await?
                .output
                .ok_or("missing packed body")?;
            after = Some(object.oid);
            actual.insert(object.oid, body);
        }
    }
    assert_eq!(pages, 3);
    assert_eq!(actual, expected);
    let packs = sql
        .query(
            None,
            SqlBatch {
                statements: vec![SqlStatement {
                    sql: "SELECT COUNT(*) FROM git_packs".into(),
                    parameters: vec![],
                }],
            },
        )
        .await?;
    assert_eq!(packs.output[0].rows, vec![vec![SqlValue::Integer(3)]]);
    sql.batch(
        super::batches::identity()?,
        SqlBatch {
            statements: [
                "DELETE FROM object_dependencies",
                "DELETE FROM object_closure",
                "DELETE FROM objects",
                "DELETE FROM git_packs",
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
