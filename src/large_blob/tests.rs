use super::*;
use crate::{ObjectKind, object_id};
use object_store::{ObjectStoreExt, memory::InMemory};
use std::future::poll_fn;

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

#[tokio::test]
async fn streamed_objects_verify_all_hashes_without_consuming_the_next_frame() -> Result {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let blobs = LargeBlobStore::new(store.clone(), [1; 16]);
    for (format, size) in [crate::ObjectFormat::Sha1, crate::ObjectFormat::Sha256]
        .into_iter()
        .flat_map(|format| [0, 1, CHUNK_BYTES, CHUNK_BYTES + 1].map(move |size| (format, size)))
    {
        let body = vec![21; size];
        let mut input = body.clone();
        input.extend_from_slice(b"\nnext frame");
        let mut input = input.as_slice();
        let oid = object_id(format, ObjectKind::Blob, &body);
        let reference = blobs.put(oid, size as u64, &mut input).await?;
        assert_eq!(input, b"\nnext frame");
        let replay = blobs.put(oid, size as u64, &mut body.as_slice()).await?;
        assert_eq!(reference.sha256, replay.sha256);
        let mut reader = blobs.read(&reference).await?;
        let mut bytes_read = 0;
        while let Some(bytes) = reader.next().await? {
            assert!(bytes.len() <= CHUNK_BYTES);
            assert_eq!(bytes.as_ref(), &body[bytes_read..bytes_read + bytes.len()]);
            bytes_read += bytes.len();
        }
        assert_eq!(bytes_read, size);
    }
    let mut objects = store.list(None);
    while let Some(object) = poll_fn(|cx| objects.as_mut().poll_next(cx)).await {
        assert!(!object?.location.as_ref().contains("staging"));
    }
    Ok(())
}

#[tokio::test]
async fn bad_input_does_not_publish_and_existing_corruption_is_not_overwritten() -> Result {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let blobs = LargeBlobStore::new(store.clone(), [2; 16]);
    let body = b"immutable blob";
    let oid = object_id(crate::ObjectFormat::Sha1, ObjectKind::Blob, body);
    assert!(matches!(
        blobs
            .put(
                crate::ObjectId::Sha1([0; 20]),
                body.len() as u64,
                &mut body.as_slice()
            )
            .await,
        Err(LargeBlobError::Corrupt)
    ));
    assert!(
        blobs
            .put(oid, body.len() as u64 + 1, &mut body.as_slice())
            .await
            .is_err()
    );
    let listing = store.list_with_delimiter(None).await?;
    assert!(listing.objects.is_empty() && listing.common_prefixes.is_empty());
    let reference = blobs
        .put(oid, body.len() as u64, &mut body.as_slice())
        .await?;
    let path = crate::external::part(&blob_path([2; 16], &reference.sha256), 0);
    store
        .put(&path, Bytes::from(vec![0; body.len()]).into())
        .await?;
    let mut reader = blobs.read(&reference).await?;
    assert!(matches!(reader.next().await, Err(LargeBlobError::Corrupt)));
    assert!(matches!(reader.next().await, Err(LargeBlobError::Corrupt)));
    assert!(matches!(
        blobs
            .put(oid, body.len() as u64, &mut body.as_slice())
            .await,
        Err(LargeBlobError::Corrupt)
    ));
    assert_eq!(
        store.get(&path).await?.bytes().await?.as_ref(),
        vec![0; body.len()]
    );
    Ok(())
}

#[tokio::test]
async fn metadata_hash_corruption_withholds_the_final_range() -> Result {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let blobs = LargeBlobStore::new(store.clone(), [3; 16]);
    let body = vec![42; CHUNK_BYTES + 17];
    let reference = blobs
        .put(
            object_id(crate::ObjectFormat::Sha1, ObjectKind::Blob, &body),
            body.len() as u64,
            &mut body.as_slice(),
        )
        .await?;
    crate::external::copy_parts(
        store.as_ref(),
        &blob_path([3; 16], &reference.sha256),
        &blob_path([3; 16], &[0; 32]),
        reference.size,
    )
    .await?;
    store
        .copy(
            &blob_path([3; 16], &reference.sha256),
            &blob_path([3; 16], &[0; 32]),
        )
        .await?;
    for bad in [
        LargeBlobReference {
            sha256: [0; 32],
            ..reference
        },
        LargeBlobReference {
            oid: crate::ObjectId::Sha1([0; 20]),
            ..reference
        },
        LargeBlobReference {
            blake3: [0; 32],
            ..reference
        },
    ] {
        let mut reader = blobs.read(&bad).await?;
        assert_eq!(
            reader.next().await?.ok_or("first range")?.len(),
            CHUNK_BYTES
        );
        assert!(matches!(reader.next().await, Err(LargeBlobError::Corrupt)));
    }
    Ok(())
}

#[tokio::test]
async fn replacement_between_ranges_invalidates_the_reader() -> Result {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let blobs = LargeBlobStore::new(store.clone(), [4; 16]);
    let body = vec![42; CHUNK_BYTES + 17];
    let reference = blobs
        .put(
            object_id(crate::ObjectFormat::Sha1, ObjectKind::Blob, &body),
            body.len() as u64,
            &mut body.as_slice(),
        )
        .await?;
    let mut reader = blobs.read(&reference).await?;
    reader.next().await?.ok_or("first range")?;
    store
        .put(
            &blob_path([4; 16], &reference.sha256),
            Bytes::from(vec![21; body.len()]).into(),
        )
        .await?;
    assert!(matches!(
        reader.next().await,
        Err(LargeBlobError::Store(
            object_store::Error::Precondition { .. }
        ))
    ));
    assert!(matches!(reader.next().await, Err(LargeBlobError::Corrupt)));
    Ok(())
}
