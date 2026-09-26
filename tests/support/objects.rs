use canopy_server::{
    ObjectBatch, ObjectKind, ObjectStorage, RepositoryCell, StoredObject, object_id,
};
use cellule_runtime::{Committed, MutationIdentity};

pub async fn put(
    repository: &RepositoryCell,
    identity: MutationIdentity,
    kind: ObjectKind,
    body: &[u8],
) -> Result<Committed<[u8; 20]>, Box<dyn std::error::Error>> {
    let oid = object_id(kind, body);
    let mut batch = ObjectBatch::default();
    batch
        .try_push(StoredObject {
            oid,
            kind,
            storage: ObjectStorage::Inline(body.to_vec()),
        })
        .map_err(|_| "fixture object exceeds batch bounds")?;
    let result = repository.put_objects(identity, batch).await?;
    Ok(Committed {
        output: oid,
        receipt: result.receipt,
    })
}
