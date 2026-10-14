use super::*;
use canopy_server::{INLINE_OBJECT_LIMIT, ObjectBatch, ObjectStorage};
use cellule_runtime::{
    SqlCell,
    primitives::sql::{SqlBatch, SqlStatement, SqlValue},
};
type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

pub async fn exercise(repository: &RepositoryCell, sql: &SqlCell<RepositoryModule>) -> Result {
    for kind in [
        ObjectKind::Blob,
        ObjectKind::Tree,
        ObjectKind::Commit,
        ObjectKind::Tag,
    ] {
        let body = vec![kind.git_name().as_bytes()[0]; INLINE_OBJECT_LIMIT + 1];
        let object = repository.stage_object(kind, &body).await?;
        let oid = object.oid;
        assert!(repository.object(oid, None).await?.output.is_none());
        let ObjectStorage::Packed(reference) = &object.storage else {
            return Err("stage must return native pack".into());
        };
        assert_eq!(reference.size(), body.len() as u64);
        assert!(reference.pack().pack_bytes < body.len() as u64 / 100);
        let mut batch = ObjectBatch::default();
        assert!(batch.try_push(object).is_ok());
        let publication = repository
            .put_objects(super::batches::identity()?, batch)
            .await?;
        assert_eq!(
            repository
                .object(oid, Some(publication.receipt))
                .await?
                .output,
            Some((kind, body.clone()))
        );
        let object = repository.stage_object(kind, &body).await?;
        let mut batch = ObjectBatch::default();
        assert!(batch.try_push(object).is_ok());
        repository
            .put_objects(super::batches::identity()?, batch)
            .await?;
        // Metadata corruption cannot silently change content read from the pack.
        sql.batch(
            super::batches::identity()?,
            SqlBatch {
                statements: vec![SqlStatement {
                    sql: "UPDATE objects SET digest = zeroblob(32) WHERE oid = ?1".into(),
                    parameters: vec![SqlValue::Blob(oid.to_vec())],
                }],
            },
        )
        .await?;
        assert!(repository.object(oid, None).await.is_err());
        sql.batch(
            super::batches::identity()?,
            SqlBatch {
                statements: vec![SqlStatement {
                    sql: "UPDATE objects SET digest = ?2 WHERE oid = ?1".into(),
                    parameters: vec![
                        SqlValue::Blob(oid.to_vec()),
                        SqlValue::Blob(blake3::hash(&body).as_bytes().to_vec()),
                    ],
                }],
            },
        )
        .await?;
    }
    // Cross the dependency-write boundary with distinct child IDs. These
    // unreferenced objects may stage before their children are available.
    let mut tree = Vec::new();
    for index in 0..8_193_u32 {
        let child = object_id(
            repository.object_format(),
            ObjectKind::Blob,
            &index.to_le_bytes(),
        );
        tree.extend_from_slice(format!("100644 file-{index:05}\0").as_bytes());
        tree.extend_from_slice(&child);
    }
    let object = repository.stage_object(ObjectKind::Tree, &tree).await?;
    let oid = object.oid;
    let mut batch = ObjectBatch::default();
    assert!(batch.try_push(object).is_ok());
    repository
        .put_objects(super::batches::identity()?, batch)
        .await?;
    let rows = sql
        .query(
            None,
            SqlBatch {
                statements: vec![SqlStatement {
                    sql: "SELECT count(*) FROM object_dependencies WHERE parent = ?".into(),
                    parameters: vec![SqlValue::Blob(oid.to_vec())],
                }],
            },
        )
        .await?
        .output;
    assert_eq!(rows[0].rows[0], vec![SqlValue::Integer(8_193)]);
    assert_eq!(
        repository.object(oid, None).await?.output,
        Some((ObjectKind::Tree, tree))
    );
    Ok(())
}
