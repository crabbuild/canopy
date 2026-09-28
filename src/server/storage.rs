use super::ServerError;
use bytes::Bytes;
use cellule_store::{StorageError, Store};
use object_store::path::Path;

pub(crate) async fn probe(store: &Store, prefix: &Path) -> Result<(), ServerError> {
    let key = prefix.clone().join(uuid::Uuid::new_v4().to_string());
    // Ignored conditional headers can admit two writers. Verify the provider
    // before creating authority records; the unique probe key is never Cell data.
    let result = async {
        let first = Bytes::from_static(b"canopy-storage-probe-1");
        let second = Bytes::from_static(b"canopy-storage-probe-2");
        if !store.put_if_absent(&key, first.clone()).await?
            || store.put_if_absent(&key, first).await?
        {
            return Err(ServerError::Repository("storage conditional create failed"));
        }
        let (_, initial) = store.get_with_etag(&key).await?;
        let updated = store.update(&key, second.clone(), initial.clone()).await?;
        if updated == initial {
            return Err(ServerError::Repository("storage ETag did not advance"));
        }
        match store
            .update(&key, Bytes::from_static(b"stale"), initial)
            .await
        {
            Err(StorageError::StateConflict { .. }) => {}
            Err(error) => return Err(error.into()),
            Ok(_) => return Err(ServerError::Repository("storage accepted stale ETag")),
        }
        if store.get_with_etag(&key).await?.0 != second
            || store.range_get(&key, 7..14).await?.as_ref() != b"storage"
        {
            return Err(ServerError::Repository("storage read verification failed"));
        }
        Ok(())
    }
    .await;
    let cleanup = store.delete(&key).await;
    result?;
    cleanup?;
    Ok(())
}
