use super::*;

async fn refresh(
    fixture: &Fixture,
    token: PreparationToken,
) -> Result<Option<PreparationFrontier>> {
    Ok(fixture
        .client()
        .query::<CheckPreparationFrontier>(&fixture.target, None, check(token))
        .await?
        .output)
}
pub(super) async fn reap(fixture: &Fixture) -> Result<u64> {
    Ok(fixture
        .client()
        .command::<ReapPreparation>(
            &fixture.target,
            identity()?,
            MaintenanceRequest {
                repository: fixture.repository,
                actor: "owner".into(),
                owner: fixture.handle.owner_fence(),
            },
        )
        .await?
        .output)
}
pub(super) async fn facts(fixture: &Fixture) -> Result<Vec<u8>> {
    Ok(fixture
        .handle
        .query(0, 4096, |connection| {
            let mut query = connection
                .prepare("SELECT generation FROM catalog_generations ORDER BY generation")?;
            let values = query
                .query_map([], |row| row.get::<_, u64>(0))?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok(values.into_iter().flat_map(u64::to_be_bytes).collect())
        })
        .await?)
}
pub(super) fn expected(values: &[u64]) -> Vec<u8> {
    values
        .iter()
        .flat_map(|value| value.to_be_bytes())
        .collect()
}

#[tokio::test]
async fn frontier_reads_latest_root_without_claim_namespace_or_lease_extension() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let fixture = Fixture::new(format).await?;
        let started = fixture
            .client()
            .command::<BeginPreparation>(&fixture.target, identity()?, fixture.begin([70; 16]))
            .await?;
        let original = lease(started.output)?;
        assert_eq!(
            refresh(&fixture, original.token)
                .await?
                .ok_or("frontier")?
                .current,
            original.base
        );
        let first = fixture.install_empty_root(1).await?;
        let one = refresh(&fixture, original.token)
            .await?
            .ok_or("first frontier")?;
        assert_eq!(one.current.catalog, Some(first));
        let second = fixture.install_empty_root(2).await?;
        let before = fixture
            .client()
            .query::<CheckPreparationFrontier>(&fixture.target, None, check(original.token))
            .await?;
        let two = before.output.ok_or("second frontier")?;
        assert_eq!(two.current.generation, 2);
        assert_eq!(two.current.catalog, Some(second));
        assert_eq!(two.lease.base, original.base);
        assert_eq!(two.lease.token, original.token);
        assert_eq!(two.lease.expires_at_ms, original.expires_at_ms);
        assert!(two.lease.observed_at_ms >= one.lease.observed_at_ms);
        let after = fixture
            .client()
            .query::<CheckPreparationFrontier>(
                &fixture.target,
                Some(before.receipt),
                check(original.token),
            )
            .await?;
        assert_eq!(after.receipt, before.receipt);
        assert_eq!(fixture.counts().await?, (1, 1));
        assert_eq!(reap(&fixture).await?, 0);
        assert_eq!(facts(&fixture).await?, expected(&[0, 1, 2]));
        let mut encoder = BoundedEncoder::new(1024)?;
        two.encode(&mut encoder)?;
        let bytes = encoder.finish();
        let mut decoder = BoundedDecoder::new(&bytes, 1024)?;
        assert_eq!(PreparationFrontier::decode(&mut decoder)?, two);
        decoder.finish()?;
        // A genuinely new operation gets the next namespace, not one consumed
        // by any of the frontier queries above.
        let next = lease(
            fixture
                .client()
                .command::<BeginPreparation>(&fixture.target, identity()?, fixture.begin([71; 16]))
                .await?
                .output,
        )?;
        assert_eq!(next.token.artifact_operation, artifact_number(2));
        fixture.runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn detached_attempt_floor_retains_intervening_roots_until_reaped() -> Result {
    let fixture = Fixture::new(ObjectFormat::Sha256).await?;
    for n in 1..=3 {
        fixture.install_empty_root(n).await?;
    }
    let first = lease(
        fixture
            .client()
            .command::<BeginPreparation>(&fixture.target, identity()?, fixture.begin([72; 16]))
            .await?
            .output,
    )?;
    fixture.install_empty_root(4).await?;
    fixture.install_empty_root(5).await?;
    let next = lease(
        fixture
            .client()
            .command::<ClaimPreparation>(&fixture.target, identity()?, request(first.token))
            .await?
            .output,
    )?;
    fixture.install_empty_root(6).await?;
    assert!(refresh(&fixture, first.token).await?.is_none());
    assert_eq!(
        refresh(&fixture, next.token)
            .await?
            .ok_or("claimed frontier")?
            .current
            .generation,
        6
    );
    // The superseded pin protects 3,4,5,6; the new pin alone would protect 5,6.
    assert_eq!(reap(&fixture).await?, 2);
    assert_eq!(facts(&fixture).await?, expected(&[0, 3, 4, 5, 6]));
    fixture
        .client()
        .command::<AbortPreparation>(&fixture.target, identity()?, check(next.token))
        .await?;
    assert!(refresh(&fixture, next.token).await?.is_none());
    assert_eq!(reap(&fixture).await?, 0);
    fixture
        .handle
        .execute(
            identity()?,
            Digest::from_bytes([72; 32]),
            sql::now(0)?,
            1,
            0,
            |tx| {
                // Expiration alone does not authorize fact deletion. Removing the
                // expired independent pins is the reaper's preceding transaction step.
                tx.execute("UPDATE catalog_leases SET expires_at_ms=0", [])?;
                assert!(
                    tx.execute("DELETE FROM catalog_generations WHERE generation=4", [])
                        .is_err()
                );
                Ok(cellule_runtime::cell::executor::HandlerOutcome::Success(
                    Vec::new(),
                ))
            },
        )
        .await?;
    assert_eq!(reap(&fixture).await?, 5); // two pins and three obsolete facts
    assert_eq!(facts(&fixture).await?, expected(&[0, 6]));
    fixture.runtime.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn frontier_rechecks_actor_identity_expiry_and_active_attempt() -> Result {
    let fixture = Fixture::new(ObjectFormat::Sha1).await?;
    fixture
        .handle
        .execute(
            identity()?,
            Digest::from_bytes([73; 32]),
            sql::now(0)?,
            1,
            0,
            |tx| {
                tx.execute(
                    "INSERT INTO repository_members VALUES('writer','write')",
                    [],
                )?;
                Ok(cellule_runtime::cell::executor::HandlerOutcome::Success(
                    Vec::new(),
                ))
            },
        )
        .await?;
    let mut input = fixture.begin([73; 16]);
    input.actor = "writer".into();
    let original = lease(
        fixture
            .client()
            .command::<BeginPreparation>(&fixture.target, identity()?, input)
            .await?
            .output,
    )?;
    let correct = LeaseCheck {
        token: original.token,
        actor: "writer".into(),
    };
    for mut wrong in [
        correct.clone(),
        correct.clone(),
        correct.clone(),
        correct.clone(),
    ]
    .into_iter()
    .enumerate()
    {
        match wrong.0 {
            0 => wrong.1.actor = "owner".into(),
            1 => wrong.1.token.repository = *uuid::Uuid::new_v4().as_bytes(),
            2 => wrong.1.token.request_digest[0] ^= 1,
            _ => wrong.1.token.artifact_operation = artifact_number(2),
        }
        assert!(
            fixture
                .client()
                .query::<CheckPreparationFrontier>(&fixture.target, None, wrong.1)
                .await?
                .output
                .is_none()
        );
    }
    assert!(
        fixture
            .client()
            .query::<CheckPreparationFrontier>(&fixture.target, None, correct.clone())
            .await?
            .output
            .is_some()
    );
    fixture
        .handle
        .execute(
            identity()?,
            Digest::from_bytes([74; 32]),
            sql::now(0)?,
            1,
            0,
            |tx| {
                tx.execute(
                    "UPDATE repository_members SET role='read' WHERE account='writer'",
                    [],
                )?;
                Ok(cellule_runtime::cell::executor::HandlerOutcome::Success(
                    Vec::new(),
                ))
            },
        )
        .await?;
    assert!(
        fixture
            .client()
            .query::<CheckPreparationFrontier>(&fixture.target, None, correct.clone())
            .await?
            .output
            .is_none()
    );
    fixture
        .handle
        .execute(
            identity()?,
            Digest::from_bytes([75; 32]),
            sql::now(0)?,
            1,
            0,
            |tx| {
                tx.execute(
                    "UPDATE repository_members SET role='write' WHERE account='writer'",
                    [],
                )?;
                tx.execute("UPDATE catalog_operations SET expires_at_ms=0", [])?;
                tx.execute("UPDATE catalog_leases SET expires_at_ms=0", [])?;
                Ok(cellule_runtime::cell::executor::HandlerOutcome::Success(
                    Vec::new(),
                ))
            },
        )
        .await?;
    assert!(
        fixture
            .client()
            .query::<CheckPreparationFrontier>(&fixture.target, None, correct)
            .await?
            .output
            .is_none()
    );
    assert_eq!(fixture.counts().await?, (1, 1));
    fixture.runtime.shutdown().await?;
    Ok(())
}

#[test]
fn floor_reaping_is_indexed_bounded_and_cannot_remove_intervening_roots() -> Result {
    let mut connection = rusqlite::Connection::open_in_memory()?;
    connection.execute_batch("PRAGMA foreign_keys=ON")?;
    connection.execute_batch(SCHEMA)?;
    let tx = connection.transaction()?;
    for generation in 1..=1200 {
        tx.execute(
            "INSERT INTO catalog_generations(generation,catalog,certificate) VALUES(?1,x'01',zeroblob(32))",
            [generation],
        )?;
    }
    tx.execute("UPDATE catalog_state SET generation=1200", [])?;
    tx.execute("INSERT INTO catalog_leases(incarnation,admission_sequence,operation,owner_epoch,artifact_operation,generation,expires_at_ms) VALUES(zeroblob(16),1,zeroblob(16),x'0000000000000001',?1,900,0)", [artifact_number(1).as_slice()])?;
    tx.commit()?;
    for generation in [0, 900, 901, 1199] {
        assert!(
            connection
                .execute(
                    "DELETE FROM catalog_generations WHERE generation=?1",
                    [generation]
                )
                .is_err()
        );
    }
    let mut explain = connection.prepare(&format!(
        "EXPLAIN QUERY PLAN {}",
        commands::REAP_GENERATIONS
    ))?;
    let plan = explain
        .query_map([REAP_ROWS], |row| row.get::<_, String>(3))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    assert!(
        plan.iter()
            .any(|line| line.contains("SEARCH g USING PRIMARY KEY")),
        "{plan:?}"
    );
    assert!(
        plan.iter()
            .any(|line| line.contains("USING COVERING INDEX catalog_leases_by_generation")),
        "{plan:?}"
    );
    assert!(!plan.iter().any(|line| line.contains("SCAN g")), "{plan:?}");
    drop(explain);
    assert_eq!(
        connection.execute(commands::REAP_GENERATIONS, [REAP_ROWS])?,
        REAP_ROWS as usize
    );
    assert_eq!(
        connection.execute(commands::REAP_GENERATIONS, [REAP_ROWS])?,
        899 - REAP_ROWS as usize
    );
    assert_eq!(
        connection.execute(commands::REAP_GENERATIONS, [REAP_ROWS])?,
        0
    );
    let retained: u64 = connection.query_row(
        "SELECT count(*) FROM catalog_generations WHERE generation BETWEEN 900 AND 1200",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(retained, 301);
    connection.execute("DELETE FROM catalog_leases", [])?;
    assert_eq!(
        connection.execute(commands::REAP_GENERATIONS, [REAP_ROWS])?,
        300
    );
    assert_eq!(
        connection.query_row("SELECT count(*) FROM catalog_generations", [], |row| row
            .get::<_, u64>(0))?,
        2
    );
    Ok(())
}

#[test]
fn frontier_codec_rejects_rollback_foreign_catalog_and_changed_same_generation() -> Result {
    let token = PreparationToken {
        repository: *uuid::Uuid::new_v4().as_bytes(),
        operation: [76; 16],
        artifact_operation: artifact_number(1),
        request_digest: [77; 32],
        owner: OwnerFence {
            incarnation: IncarnationId::from_bytes([78; 16]),
            epoch: 1,
        },
        attempt: 1,
    };
    let empty = GenerationFact {
        generation: 0,
        catalog: None,
        refs: None,
        certificate: None,
    };
    let mut value = PreparationFrontier {
        lease: PreparationLease {
            token,
            base: empty,
            format: ObjectFormat::Sha1,
            observed_at_ms: 10,
            expires_at_ms: 11,
        },
        current: empty,
    };
    value.validate()?;
    value.lease.observed_at_ms = 11;
    assert!(value.validate().is_err());
    value.lease.observed_at_ms = 10;
    // Reuse a valid descriptor from the directory/catalog codec fixture shape.
    let artifact = canopy_object_storage::artifact::ArtifactDescriptor {
        size: 1,
        digest: [0; 32],
        manifest_digest: [0; 32],
    };
    let catalog = StoredCatalog {
        repository: token.repository,
        operation: artifact_number(1),
        format: ObjectFormat::Sha1,
        artifact,
    };
    value.lease.base = GenerationFact {
        generation: 1,
        catalog: Some(catalog),
        refs: None,
        certificate: Some([1; 32]),
    };
    assert!(value.validate().is_err()); // current zero is below the floor
    value.current = value.lease.base;
    value.validate()?;
    value.current.certificate = Some([2; 32]);
    assert!(value.validate().is_err());
    value.current = GenerationFact {
        generation: 2,
        catalog: Some(StoredCatalog {
            repository: *uuid::Uuid::new_v4().as_bytes(),
            ..catalog
        }),
        refs: None,
        certificate: Some([3; 32]),
    };
    assert!(value.validate().is_err());
    Ok(())
}
