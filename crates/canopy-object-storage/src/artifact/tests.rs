use super::*;
use object_store::{ObjectStoreExt, memory::InMemory};

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

fn key(body: &[u8]) -> ArtifactKey {
    ArtifactKey {
        operation: [2; 16],
        binding_digest: *blake3::hash(body).as_bytes(),
        kind: ArtifactKind::Pack,
    }
}

#[tokio::test]
async fn catalog_artifacts_bind_their_own_digest_and_isolate_retired_incarnations() -> Result {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let artifacts = ArtifactStore::new(Arc::clone(&store), [1; 16]);
    let body = b"immutable catalog fixture";
    let digest = *blake3::hash(body).as_bytes();
    for kind in [ArtifactKind::DirectoryRun, ArtifactKind::CatalogNode] {
        let old = ArtifactKey {
            operation: [2; 16],
            binding_digest: digest,
            kind,
        };
        let new = ArtifactKey {
            operation: [3; 16],
            ..old
        };
        let mut wrong = old;
        wrong.binding_digest[0] ^= 1;
        assert!(matches!(
            artifacts.path(wrong, digest),
            Err(ArtifactError::Corrupt)
        ));
        let descriptor = artifacts
            .put(old, body.len() as u64, digest, &mut body.as_slice())
            .await?;
        let next = artifacts
            .put(new, body.len() as u64, digest, &mut body.as_slice())
            .await?;
        let path = artifacts.path(old, digest)?;
        assert!(path.as_ref().contains("/git-catalogs/"));
        store.delete(&external::part(&path, 0)).await?;
        store.delete(&path).await?;
        assert!(artifacts.read(old, descriptor).await.is_err());
        let mut read = artifacts.read(new, next).await?;
        assert_eq!(read.next().await?.ok_or("part")?.as_ref(), body);
        assert!(read.next().await?.is_none());
    }
    Ok(())
}

#[tokio::test]
async fn roundtrip_preserves_frames_and_replays_create_only_artifacts() -> Result {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let artifacts = ArtifactStore::new(Arc::clone(&store), [1; 16]);
    for size in [0, 1, PART_BYTES, PART_BYTES + 1] {
        let body = vec![27; size];
        let key = key(&body);
        let mut framed = body.clone();
        framed.extend_from_slice(b"next frame");
        let mut input = framed.as_slice();
        let descriptor = artifacts
            .put(key, size as u64, key.binding_digest, &mut input)
            .await?;
        assert_eq!(input, b"next frame");
        assert_eq!(
            descriptor,
            artifacts
                .put(key, size as u64, key.binding_digest, &mut body.as_slice())
                .await?
        );
        let mut reader = artifacts.read(key, descriptor).await?;
        assert_eq!(reader.descriptor(), descriptor);
        let mut offset = 0;
        while let Some(bytes) = reader.next().await? {
            assert!(bytes.len() <= PART_BYTES);
            assert_eq!(bytes.as_ref(), &body[offset..offset + bytes.len()]);
            offset += bytes.len();
        }
        assert_eq!(offset, size);
    }
    let objects = store.list_with_delimiter(None).await?;
    assert_eq!(objects.common_prefixes.len(), 1);
    // Recursive listing confirms every staging artifact was reclaimed.
    let mut listing = store.list(None);
    while let Some(object) = std::future::poll_fn(|cx| listing.as_mut().poll_next(cx)).await {
        assert!(!object?.location.as_ref().contains("/staging/"));
    }
    Ok(())
}

#[tokio::test]
async fn invalid_input_does_not_publish_and_size_limits_precede_reads() -> Result {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let artifacts = ArtifactStore::new(Arc::clone(&store), [1; 16]);
    let body = b"pack bytes";
    let key = key(body);
    assert!(matches!(
        artifacts
            .put(
                key,
                MAX_ARTIFACT_BYTES + 1,
                key.binding_digest,
                &mut body.as_slice()
            )
            .await,
        Err(ArtifactError::TooLarge)
    ));
    assert!(matches!(
        artifacts
            .put(key, body.len() as u64, [0; 32], &mut body.as_slice())
            .await,
        Err(ArtifactError::Corrupt)
    ));
    let wrong_input = b"wrong pack";
    assert!(matches!(
        artifacts
            .put(
                key,
                wrong_input.len() as u64,
                key.binding_digest,
                &mut wrong_input.as_slice()
            )
            .await,
        Err(ArtifactError::Corrupt)
    ));
    assert!(matches!(
        artifacts
            .put(
                key,
                body.len() as u64 + 1,
                key.binding_digest,
                &mut body.as_slice()
            )
            .await,
        Err(ArtifactError::Io(_))
    ));
    let listing = store.list_with_delimiter(None).await?;
    assert!(listing.objects.is_empty() && listing.common_prefixes.is_empty());
    Ok(())
}

#[tokio::test]
async fn corrupt_part_fails_before_yield_and_poisoned_reader_cannot_resume() -> Result {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let artifacts = ArtifactStore::new(Arc::clone(&store), [1; 16]);
    let body = vec![28; PART_BYTES + 1];
    let key = key(&body);
    let descriptor = artifacts
        .put(
            key,
            body.len() as u64,
            key.binding_digest,
            &mut body.as_slice(),
        )
        .await?;
    let path = artifacts.path(key, descriptor.digest)?;
    store
        .put(
            &external::part(&path, 0),
            Bytes::from(vec![0; PART_BYTES]).into(),
        )
        .await?;
    let mut reader = artifacts.read(key, descriptor).await?;
    assert!(matches!(reader.next().await, Err(ArtifactError::Corrupt)));
    assert!(matches!(reader.next().await, Err(ArtifactError::Corrupt)));
    // A retry must reject an existing wrong part rather than trusting AlreadyExists.
    assert!(
        artifacts
            .put(
                key,
                body.len() as u64,
                key.binding_digest,
                &mut body.as_slice()
            )
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn collision_is_checked_before_manifest_publication() -> Result {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let artifacts = ArtifactStore::new(Arc::clone(&store), [1; 16]);
    let body = b"native pack";
    let key = key(body);
    let path = artifacts.path(key, key.binding_digest)?;
    store
        .put(
            &external::part(&path, 0),
            Bytes::from(vec![0; body.len()]).into(),
        )
        .await?;
    assert!(
        artifacts
            .put(
                key,
                body.len() as u64,
                key.binding_digest,
                &mut body.as_slice()
            )
            .await
            .is_err()
    );
    assert!(matches!(
        store.head(&path).await,
        Err(object_store::Error::NotFound { .. })
    ));
    Ok(())
}

#[tokio::test]
async fn existing_manifest_corruption_is_not_overwritten() -> Result {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let artifacts = ArtifactStore::new(Arc::clone(&store), [1; 16]);
    let body = b"native index";
    let mut key = key(b"parent pack");
    key.kind = ArtifactKind::Index;
    let digest = *blake3::hash(body).as_bytes();
    let descriptor = artifacts
        .put(key, body.len() as u64, digest, &mut body.as_slice())
        .await?;
    let path = artifacts.path(key, digest)?;
    let corrupt = Bytes::from(vec![0; 48]);
    store.put(&path, corrupt.clone().into()).await?;
    assert!(artifacts.read(key, descriptor).await.is_err());
    assert!(
        artifacts
            .put(key, body.len() as u64, digest, &mut body.as_slice())
            .await
            .is_err()
    );
    assert_eq!(store.get(&path).await?.bytes().await?, corrupt);
    Ok(())
}

#[tokio::test]
async fn operation_and_repository_namespaces_prevent_delete_aba() -> Result {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let artifacts = ArtifactStore::new(Arc::clone(&store), [1; 16]);
    let other_repository = ArtifactStore::new(Arc::clone(&store), [3; 16]);
    let body = b"same immutable bytes";
    let old = key(body);
    let mut new = old;
    new.operation = [4; 16];
    let descriptor = artifacts
        .put(
            old,
            body.len() as u64,
            old.binding_digest,
            &mut body.as_slice(),
        )
        .await?;
    let next = artifacts
        .put(
            new,
            body.len() as u64,
            new.binding_digest,
            &mut body.as_slice(),
        )
        .await?;
    assert_eq!(descriptor, next);
    assert_ne!(
        artifacts.path(old, descriptor.digest)?,
        artifacts.path(new, next.digest)?
    );
    assert!(other_repository.read(new, next).await.is_err());
    // An old collector's delayed deletes address only the old incarnation.
    let old_path = artifacts.path(old, descriptor.digest)?;
    store.delete(&old_path).await?;
    store.delete(&external::part(&old_path, 0)).await?;
    let mut reader = artifacts.read(new, next).await?;
    assert_eq!(reader.next().await?.unwrap().as_ref(), body);
    assert!(reader.next().await?.is_none());
    Ok(())
}

#[tokio::test]
async fn whole_digest_and_empty_part_are_authenticated() -> Result {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let artifacts = ArtifactStore::new(Arc::clone(&store), [1; 16]);
    let body = b"index bytes";
    let mut key = key(b"parent pack");
    key.kind = ArtifactKind::Index;
    let digest = *blake3::hash(body).as_bytes();
    let mut descriptor = artifacts
        .put(key, body.len() as u64, digest, &mut body.as_slice())
        .await?;
    descriptor.manifest_digest = [0; 32];
    assert!(artifacts.read(key, descriptor).await.is_err());
    let empty = key_for_empty();
    let descriptor = artifacts
        .put(empty, 0, empty.binding_digest, &mut b"".as_slice())
        .await?;
    let path = artifacts.path(empty, descriptor.digest)?;
    store.delete(&external::part(&path, 0)).await?;
    assert!(artifacts.read(empty, descriptor).await.is_err());
    Ok(())
}

#[tokio::test]
async fn final_part_is_withheld_when_whole_digest_disagrees() -> Result {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let artifacts = ArtifactStore::new(Arc::clone(&store), [1; 16]);
    let body = b"index bytes";
    let mut key = key(b"parent pack");
    key.kind = ArtifactKind::Index;
    let mut descriptor = artifacts
        .put(
            key,
            body.len() as u64,
            *blake3::hash(body).as_bytes(),
            &mut body.as_slice(),
        )
        .await?;
    let original = artifacts.path(key, descriptor.digest)?;
    descriptor.digest = [0; 32];
    let wrong = artifacts.path(key, descriptor.digest)?;
    store.copy(&original, &wrong).await?;
    store
        .copy(&external::part(&original, 0), &external::part(&wrong, 0))
        .await?;
    let mut reader = artifacts.read(key, descriptor).await?;
    assert!(matches!(reader.next().await, Err(ArtifactError::Corrupt)));
    assert!(matches!(reader.next().await, Err(ArtifactError::Corrupt)));
    Ok(())
}

#[tokio::test]
async fn empty_manifest_must_certify_the_empty_part() -> Result {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let artifacts = ArtifactStore::new(Arc::clone(&store), [1; 16]);
    let key = key(b"");
    let mut descriptor = artifacts
        .put(key, 0, key.binding_digest, &mut b"".as_slice())
        .await?;
    let path = artifacts.path(key, descriptor.digest)?;
    let mut manifest = b"CANOPY02".to_vec();
    manifest.extend_from_slice(&0_u64.to_le_bytes());
    manifest.extend_from_slice(&[0; 32]);
    descriptor.manifest_digest = *blake3::hash(&manifest).as_bytes();
    store.put(&path, manifest.into()).await?;
    assert!(matches!(
        artifacts.read(key, descriptor).await,
        Err(ArtifactError::Corrupt)
    ));
    Ok(())
}

fn key_for_empty() -> ArtifactKey {
    key(b"")
}
