use super::*;
use canopy_object_storage::artifact::{ArtifactKey, ArtifactKind, ArtifactStore};
use object_store::{ObjectStore, ObjectStoreExt};

#[test]
fn namespace_watermark_and_pin_identity_cannot_be_reset_or_rebound() -> Result {
    let mut connection = rusqlite::Connection::open_in_memory()?;
    connection.execute_batch("PRAGMA foreign_keys=ON")?;
    connection.execute_batch(SCHEMA)?;
    connection.execute("INSERT INTO repository_identity(singleton,repository_id,object_format,owner,push_cert_seed,artifact_sequence) VALUES(1,?1,'sha256','owner',zeroblob(32),1)", [uuid::Uuid::new_v4().as_bytes().as_slice()])?;
    for sql in [
        "UPDATE repository_identity SET artifact_sequence=0",
        "UPDATE repository_identity SET artifact_sequence=1",
        "UPDATE repository_identity SET artifact_sequence=3",
        "DELETE FROM repository_identity",
        "INSERT OR REPLACE INTO repository_identity(singleton,repository_id,object_format,owner,push_cert_seed,artifact_sequence) VALUES(1,zeroblob(16),'sha256','owner',zeroblob(32),0)",
    ] {
        assert!(connection.execute(sql, []).is_err());
    }
    connection.execute("UPDATE repository_identity SET artifact_sequence=2", [])?;
    // A valid alternate generation ensures the mutation fails because the
    // pin binding is immutable, rather than because its target is absent.
    connection.execute(
        "INSERT INTO catalog_generations(generation,catalog,certificate) VALUES(1,x'01',zeroblob(32))",
        [],
    )?;
    connection.execute("INSERT INTO catalog_leases(incarnation,admission_sequence,operation,owner_epoch,artifact_operation,generation,expires_at_ms) VALUES(zeroblob(16),1,zeroblob(16),x'0000000000000001',?1,0,100)", [artifact_number(1).as_slice()])?;
    assert!(connection.execute("INSERT OR REPLACE INTO catalog_leases(incarnation,admission_sequence,operation,owner_epoch,artifact_operation,generation,expires_at_ms) VALUES(zeroblob(16),1,zeroblob(16),x'0000000000000001',?1,0,100)", [artifact_number(2).as_slice()]).is_err());
    assert!(connection.execute("INSERT OR REPLACE INTO catalog_leases(incarnation,admission_sequence,operation,owner_epoch,artifact_operation,generation,expires_at_ms) VALUES(zeroblob(16),2,zeroblob(16),x'0000000000000001',?1,0,100)", [artifact_number(1).as_slice()]).is_err());
    for sql in [
        "UPDATE catalog_leases SET incarnation=randomblob(16)",
        "UPDATE catalog_leases SET admission_sequence=2",
        "UPDATE catalog_leases SET operation=randomblob(16)",
        "UPDATE catalog_leases SET owner_epoch=x'0000000000000002'",
        "UPDATE catalog_leases SET artifact_operation=randomblob(16)",
        "UPDATE catalog_leases SET generation=1",
    ] {
        assert!(connection.execute(sql, []).is_err());
    }
    // Each deferred binding differs from the existing pin by one field.
    for (logical, epoch, namespace) in [
        ([1u8; 16], 1u64, artifact_number(1)),
        ([0u8; 16], 2, artifact_number(1)),
        ([0u8; 16], 1, artifact_number(2)),
    ] {
        let tx = connection.transaction()?;
        tx.execute("INSERT INTO catalog_operations(id,actor,request_digest,incarnation,owner_epoch,admission_sequence,artifact_operation,generation,expires_at_ms) VALUES(?1,'owner',zeroblob(32),zeroblob(16),?2,1,?3,0,100)", rusqlite::params![logical.as_slice(),epoch.to_be_bytes().as_slice(),namespace.as_slice()])?;
        assert!(tx.commit().is_err());
    }
    connection.execute(
        "UPDATE catalog_leases SET attestation=x'01',attestation_digest=zeroblob(32)",
        [],
    )?;
    for sql in [
        "UPDATE catalog_leases SET attestation=NULL,attestation_digest=NULL",
        "UPDATE catalog_leases SET attestation=x'02'",
        "UPDATE catalog_leases SET attestation_digest=randomblob(32)",
    ] {
        assert!(connection.execute(sql, []).is_err());
    }
    connection.execute(
        "UPDATE catalog_leases SET attestation=x'01',attestation_digest=zeroblob(32)",
        [],
    )?;
    Ok(())
}

#[tokio::test]
async fn delayed_old_attempt_deletes_cannot_remove_recreated_logical_request_bytes() -> Result {
    let fixture = Fixture::new(ObjectFormat::Sha256).await?;
    let client = fixture.client();
    let input = fixture.begin([58; 16]);
    let mutation = identity()?;
    let first = client
        .command::<BeginPreparation>(&fixture.target, mutation, input.clone())
        .await?;
    let old = lease(first.output.clone())?;
    let provider: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let artifacts = ArtifactStore::new(Arc::clone(&provider), fixture.repository);
    let body = b"identical immutable artifact bytes";
    let digest = *blake3::hash(body).as_bytes();
    let mut pending = Vec::new();
    for kind in [
        ArtifactKind::Pack,
        ArtifactKind::Index,
        ArtifactKind::Metadata,
        ArtifactKind::DirectoryRun,
        ArtifactKind::CatalogNode,
    ] {
        let key = ArtifactKey {
            operation: old.token.artifact_operation,
            binding_digest: digest,
            kind,
        };
        artifacts
            .put(key, body.len() as u64, digest, &mut body.as_slice())
            .await?;
        let path = artifacts.path(key, digest)?;
        pending.push(canopy_object_storage::external::part(&path, 0));
        pending.push(path);
    }
    let claimed = lease(
        client
            .command::<ClaimPreparation>(&fixture.target, identity()?, request(old.token))
            .await?
            .output,
    )?;
    assert_eq!(claimed.token.operation, old.token.operation);
    assert_ne!(
        claimed.token.artifact_operation,
        old.token.artifact_operation
    );
    client
        .command::<AbortPreparation>(&fixture.target, identity()?, check(claimed.token))
        .await?;
    let next = lease(
        client
            .command::<BeginPreparation>(&fixture.target, identity()?, input.clone())
            .await?
            .output,
    )?;
    assert_eq!(next.token.operation, old.token.operation);
    assert_eq!(next.token.artifact_operation, artifact_number(3));
    assert_eq!(fixture.counts().await?, (1, 3));
    let replay = client
        .command::<BeginPreparation>(&fixture.target, mutation, input)
        .await?;
    assert_eq!(replay.output, first.output);
    assert_eq!(replay.receipt, first.receipt);
    let mut successors = Vec::new();
    for kind in [
        ArtifactKind::Pack,
        ArtifactKind::Index,
        ArtifactKind::Metadata,
        ArtifactKind::DirectoryRun,
        ArtifactKind::CatalogNode,
    ] {
        let key = ArtifactKey {
            operation: next.token.artifact_operation,
            binding_digest: digest,
            kind,
        };
        let stored = artifacts
            .put(key, body.len() as u64, digest, &mut body.as_slice())
            .await?;
        successors.push((key, stored));
    }
    // Fault injection only: the production collector still needs retained-root
    // and reader-drain proofs before scheduling any delete.
    for path in pending {
        provider.delete(&path).await?;
    }
    for (key, stored) in successors {
        let mut read = artifacts.read(key, stored).await?;
        assert_eq!(read.next().await?.ok_or("part")?.as_ref(), body);
        assert!(read.next().await?.is_none());
    }
    fixture.runtime.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn exhausted_namespace_allocator_preserves_live_attempts_and_exact_replay() -> Result {
    let fixture = Fixture::with_artifact_sequence(ObjectFormat::Sha1, i64::MAX - 1).await?;
    let client = fixture.client();
    let input = fixture.begin([59; 16]);
    let mutation = identity()?;
    let first = client
        .command::<BeginPreparation>(&fixture.target, mutation, input.clone())
        .await?;
    let old = lease(first.output.clone())?;
    assert_eq!(
        old.token.artifact_operation,
        artifact_number(i64::MAX as u64)
    );
    let duplicate = client
        .command::<BeginPreparation>(&fixture.target, identity()?, input.clone())
        .await?;
    assert_eq!(lease(duplicate.output)?.token, old.token);
    let replay = client
        .command::<BeginPreparation>(&fixture.target, mutation, input)
        .await?;
    assert_eq!(replay.output, first.output);
    assert_eq!(replay.receipt, first.receipt);
    assert!(
        client
            .command::<BeginPreparation>(&fixture.target, identity()?, fixture.begin([60; 16]))
            .await
            .is_err()
    );
    assert!(
        client
            .command::<ClaimPreparation>(&fixture.target, identity()?, request(old.token))
            .await
            .is_err()
    );
    assert_eq!(fixture.counts().await?, (1, 1));
    let active = client
        .query::<CheckPreparation>(&fixture.target, None, check(old.token))
        .await?
        .output
        .ok_or("live attempt")?;
    assert_eq!(active.token, old.token);
    fixture.runtime.shutdown().await?;
    Ok(())
}
