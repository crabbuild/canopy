use super::prepare::{cleaned, opened_native, physical};
use super::*;
use crate::packs::metadata::tests::limits;
use cellule_ltx::DiskBudget;

async fn prepared(
    fixture: &Fixture,
    operation: [u8; 16],
) -> Result<(PreparedCatalog, tempfile::TempDir, DiskBudget)> {
    let (native, base, _, _) = opened_native(fixture, operation, 32).await?;
    let root = tempfile::TempDir::new()?;
    let budget = DiskBudget::new(256 << 20);
    let mut assembler =
        CatalogPreparation::new(root.path(), budget.clone(), base, limits()).await?;
    let (witness, segments) = physical(&native, root.path(), budget.clone()).await?;
    assembler.begin_pack(witness)?;
    for segment in segments {
        assembler.add_segment(segment).await?;
    }
    assembler.finish_pack().await?;
    Ok((assembler.finish().await?, root, budget))
}
async fn saved(handle: &CellHandle, operation: [u8; 16]) -> Result<Option<CatalogCertificate>> {
    let bytes = handle
        .query(0, CERTIFICATE_BYTES as usize, move |connection| {
            use rusqlite::OptionalExtension;
            let body: Option<Option<Vec<u8>>> = connection
                .query_row(
                    "SELECT attestation FROM catalog_operations WHERE id=?1",
                    [operation.as_slice()],
                    |row| row.get(0),
                )
                .optional()?;
            Ok(body.flatten().unwrap_or_default())
        })
        .await?;
    if bytes.is_empty() {
        return Ok(None);
    }
    let mut decoder = BoundedDecoder::new(&bytes, CERTIFICATE_BYTES)?;
    let certificate = CatalogCertificate::decode(&mut decoder)?;
    decoder.finish()?;
    Ok(Some(certificate))
}
async fn retained(
    handle: &CellHandle,
    token: PreparationToken,
) -> Result<Option<CatalogCertificate>> {
    let bytes = handle.query(0, CERTIFICATE_BYTES as usize, move |connection| {
        use rusqlite::OptionalExtension;
        let bytes = connection.query_row(
            "SELECT operation,owner_epoch,artifact_operation,attestation,attestation_digest FROM catalog_leases WHERE incarnation=?1 AND admission_sequence=?2",
            rusqlite::params![token.owner.incarnation.as_bytes().as_slice(),token.attempt as i64],
            |row| {
                let operation: Vec<u8> = row.get(0)?;
                let epoch: Vec<u8> = row.get(1)?;
                let artifact: Vec<u8> = row.get(2)?;
                let body: Option<Vec<u8>> = row.get(3)?;
                let digest: Option<Vec<u8>> = row.get(4)?;
                assert_eq!(operation, token.operation);
                assert_eq!(epoch, token.owner.epoch.to_be_bytes());
                assert_eq!(artifact, token.artifact_operation);
                assert_eq!(digest, body.as_ref().map(|b| blake3::hash(b).as_bytes().to_vec()));
                Ok(body.unwrap_or_default())
            },
        ).optional()?;
        Ok(bytes.unwrap_or_default())
    }).await?;
    if bytes.is_empty() {
        return Ok(None);
    }
    let mut decoder = BoundedDecoder::new(&bytes, CERTIFICATE_BYTES)?;
    let certificate = CatalogCertificate::decode(&mut decoder)?;
    decoder.finish()?;
    Ok(Some(certificate))
}
fn registered(outcome: AttestationOutcome) -> Result<RegisteredCatalog> {
    match outcome {
        AttestationOutcome::Registered(value) => Ok(value),
        AttestationOutcome::Denied(reason) => Err(format!("denied: {reason:?}").into()),
    }
}
fn denied(
    result: std::result::Result<
        cellule_runtime::Committed<AttestationOutcome>,
        InvocationError<AttestationOutcome>,
    >,
    reason: PreparationDenial,
) {
    assert!(
        matches!(result,Err(InvocationError::Rejected(ref value)) if value.output==AttestationOutcome::Denied(reason))
    );
}
async fn catalog_state(handle: &CellHandle) -> Result<Vec<u8>> {
    Ok(handle
        .query(0, 32, |connection| {
            let generation: u64 = connection.query_row(
                "SELECT generation FROM catalog_state WHERE singleton=1",
                [],
                |row| row.get(0),
            )?;
            let refs: u64 =
                connection.query_row("SELECT count(*) FROM refs", [], |row| row.get(0))?;
            let generations: u64 =
                connection.query_row("SELECT count(*) FROM catalog_generations", [], |row| {
                    row.get(0)
                })?;
            Ok([
                generation.to_be_bytes(),
                refs.to_be_bytes(),
                generations.to_be_bytes(),
            ]
            .concat())
        })
        .await?)
}

#[tokio::test]
async fn native_catalog_attestation_is_bounded_durable_idempotent_and_not_publication() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let fixture = Fixture::new(format).await?;
        let (proof, root, budget) = prepared(&fixture, [52; 16]).await?;
        let before = catalog_state(&fixture.handle).await?;
        let inline = proof.certificate().await?;
        assert!(
            saved(&fixture.handle, proof.token().operation)
                .await?
                .is_none()
        );
        assert_eq!(catalog_state(&fixture.handle).await?, before);
        let mutation = identity()?;
        let first = proof.attest(mutation).await?;
        let binding = registered(first.output)?;
        assert_eq!(binding.token, proof.token());
        let certificate = saved(&fixture.handle, proof.token().operation)
            .await?
            .ok_or("certificate")?;
        let bytes = certificate.bytes()?;
        assert_eq!(
            retained(&fixture.handle, proof.token()).await?,
            Some(certificate.clone())
        );
        assert_eq!(inline, certificate);
        assert!(bytes.len() <= CERTIFICATE_BYTES as usize);
        assert_eq!(binding.certificate_digest, *blake3::hash(&bytes).as_bytes());
        let facts = certificate.data()?;
        assert_eq!(facts.base, proof.base());
        assert_eq!(facts.catalog, proof.catalog());
        assert_eq!(facts.object_count, proof.object_count());
        assert_eq!(facts.inputs_digest, proof.inputs_digest());
        assert_eq!(facts.inventory_digest, proof.inventory_digest());
        let replay = proof.attest(mutation).await?;
        assert_eq!(replay.output, first.output);
        assert_eq!(replay.receipt, first.receipt);
        assert_eq!(proof.attest(identity()?).await?.output, first.output);
        assert_eq!(catalog_state(&fixture.handle).await?, before);
        assert_eq!(fixture.counts().await?, (1, 1));
        // No extra renewal or staged proof record is needed to retain it.
        fixture
            .client()
            .command::<RenewPreparation>(&fixture.target, identity()?, request(proof.token()))
            .await?;
        assert_eq!(
            saved(&fixture.handle, proof.token().operation).await?,
            Some(certificate.clone())
        );
        for length in 0..bytes.len() {
            let mut decoder = BoundedDecoder::new(&bytes[..length], CERTIFICATE_BYTES)?;
            assert!(CatalogCertificate::decode(&mut decoder).is_err());
        }
        let mut extra = bytes.clone();
        extra.push(0);
        let mut decoder = BoundedDecoder::new(&extra, CERTIFICATE_BYTES)?;
        CatalogCertificate::decode(&mut decoder)?;
        assert!(decoder.finish().is_err());
        drop(proof);
        cleaned(root.path(), &budget).await?;
        fixture.runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn edited_facts_wrong_scope_and_conflicting_attestations_do_not_replace_the_record() -> Result
{
    let fixture = Fixture::new(ObjectFormat::Sha256).await?;
    let (proof, root, budget) = prepared(&fixture, [53; 16]).await?;
    proof.attest(identity()?).await?;
    let original = saved(&fixture.handle, proof.token().operation)
        .await?
        .ok_or("certificate")?;
    for scope in [false, true] {
        let mut certificate = original.clone();
        let mut facts = certificate.data()?;
        if scope {
            facts.application[0] ^= 1;
        } else {
            facts.inventory_digest[0] ^= 1;
        }
        let mut encoder = BoundedEncoder::new(960)?;
        facts.encode(&mut encoder)?;
        certificate.0.body = encoder.finish();
        denied(
            fixture
                .client()
                .command::<RegisterCatalogAttestation>(&fixture.target, identity()?, certificate)
                .await,
            if scope {
                PreparationDenial::Stale
            } else {
                PreparationDenial::Unauthorized
            },
        );
    }
    // A privileged test signer cannot overwrite a prior valid registration
    // with a different valid certificate for the same attempt either.
    let mut conflicting = original.data()?;
    conflicting.inputs_digest[0] ^= 1;
    let conflicting = CatalogCertificate::seal(&conflicting, &[16; 32])?;
    denied(
        fixture
            .client()
            .command::<RegisterCatalogAttestation>(&fixture.target, identity()?, conflicting)
            .await,
        PreparationDenial::Conflict,
    );
    assert_eq!(
        saved(&fixture.handle, proof.token().operation).await?,
        Some(original)
    );
    assert_eq!(
        catalog_state(&fixture.handle).await?,
        [0u64.to_be_bytes(), 0u64.to_be_bytes(), 1u64.to_be_bytes()].concat()
    );
    drop(proof);
    cleaned(root.path(), &budget).await?;
    fixture.runtime.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn revoked_actor_cannot_issue_or_register_but_recorded_outcome_remains_exact() -> Result {
    let fixture = Fixture::new(ObjectFormat::Sha256).await?;
    let (proof, root, budget) = prepared(&fixture, [57; 16]).await?;
    let certificate = proof.certificate().await?;
    let mutation = identity()?;
    let first = fixture
        .client()
        .command::<RegisterCatalogAttestation>(&fixture.target, mutation, certificate.clone())
        .await?;
    let before = catalog_state(&fixture.handle).await?;
    fixture
        .handle
        .execute(
            identity()?,
            Digest::from_bytes([64; 32]),
            sql::now(0)?,
            1,
            0,
            |tx| {
                tx.execute(
                    "UPDATE repository_identity SET owner='successor' WHERE singleton=1",
                    [],
                )?;
                Ok(cellule_runtime::cell::executor::HandlerOutcome::Success(
                    Vec::new(),
                ))
            },
        )
        .await?;
    assert!(proof.certificate().await.is_err());
    assert!(proof.attest(identity()?).await.is_err());
    denied(
        fixture
            .client()
            .command::<RegisterCatalogAttestation>(
                &fixture.target,
                identity()?,
                certificate.clone(),
            )
            .await,
        PreparationDenial::Unauthorized,
    );
    let replay = fixture
        .client()
        .command::<RegisterCatalogAttestation>(&fixture.target, mutation, certificate.clone())
        .await?;
    assert_eq!(replay.output, first.output);
    assert_eq!(replay.receipt, first.receipt);
    assert_eq!(
        saved(&fixture.handle, proof.token().operation).await?,
        Some(certificate)
    );
    assert_eq!(catalog_state(&fixture.handle).await?, before);
    drop(proof);
    cleaned(root.path(), &budget).await?;
    fixture.runtime.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn claim_clears_the_attestation_and_recorded_replay_cannot_recreate_it() -> Result {
    let fixture = Fixture::new(ObjectFormat::Sha1).await?;
    let (proof, root, budget) = prepared(&fixture, [54; 16]).await?;
    let mutation = identity()?;
    let first = proof.attest(mutation).await?;
    let certificate = saved(&fixture.handle, proof.token().operation)
        .await?
        .ok_or("certificate")?;
    let claimed = lease(
        fixture
            .client()
            .command::<ClaimPreparation>(&fixture.target, identity()?, request(proof.token()))
            .await?
            .output,
    )?;
    assert_ne!(claimed.token.attempt, proof.token().attempt);
    assert_ne!(
        claimed.token.artifact_operation,
        proof.token().artifact_operation
    );
    assert_eq!(
        retained(&fixture.handle, proof.token()).await?,
        Some(certificate.clone())
    );
    assert!(retained(&fixture.handle, claimed.token).await?.is_none());
    assert!(
        saved(&fixture.handle, proof.token().operation)
            .await?
            .is_none()
    );
    denied(
        fixture
            .client()
            .command::<RegisterCatalogAttestation>(
                &fixture.target,
                identity()?,
                certificate.clone(),
            )
            .await,
        PreparationDenial::Stale,
    );
    let replay = fixture
        .client()
        .command::<RegisterCatalogAttestation>(&fixture.target, mutation, certificate)
        .await?;
    assert_eq!(replay.output, first.output);
    assert_eq!(replay.receipt, first.receipt);
    assert!(
        saved(&fixture.handle, proof.token().operation)
            .await?
            .is_none()
    );
    assert!(proof.attest(identity()?).await.is_err());
    assert_eq!(fixture.counts().await?, (1, 2));
    drop(proof);
    cleaned(root.path(), &budget).await?;
    fixture.runtime.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn expired_attestations_are_not_reissued_and_reaping_removes_the_bounded_record() -> Result {
    let fixture = Fixture::new(ObjectFormat::Sha256).await?;
    let (proof, root, budget) = prepared(&fixture, [55; 16]).await?;
    let mutation = identity()?;
    let first = proof.attest(mutation).await?;
    let certificate = saved(&fixture.handle, proof.token().operation)
        .await?
        .ok_or("certificate")?;
    fixture
        .handle
        .execute(
            identity()?,
            Digest::from_bytes([69; 32]),
            sql::now(0)?,
            128,
            0,
            |tx| {
                tx.execute("UPDATE catalog_operations SET expires_at_ms=0", [])?;
                tx.execute("UPDATE catalog_leases SET expires_at_ms=0", [])?;
                Ok(cellule_runtime::cell::executor::HandlerOutcome::Success(
                    Vec::new(),
                ))
            },
        )
        .await?;
    assert!(proof.attest(identity()?).await.is_err());
    denied(
        fixture
            .client()
            .command::<RegisterCatalogAttestation>(
                &fixture.target,
                identity()?,
                certificate.clone(),
            )
            .await,
        PreparationDenial::Expired,
    );
    let replay = fixture
        .client()
        .command::<RegisterCatalogAttestation>(&fixture.target, mutation, certificate.clone())
        .await?;
    assert_eq!(replay.output, first.output);
    assert_eq!(replay.receipt, first.receipt);
    assert_eq!(
        fixture
            .client()
            .command::<ReapPreparation>(
                &fixture.target,
                identity()?,
                MaintenanceRequest {
                    repository: fixture.repository,
                    actor: "owner".into(),
                    owner: fixture.handle.owner_fence()
                }
            )
            .await?
            .output,
        2
    );
    assert!(
        saved(&fixture.handle, proof.token().operation)
            .await?
            .is_none()
    );
    denied(
        fixture
            .client()
            .command::<RegisterCatalogAttestation>(
                &fixture.target,
                identity()?,
                certificate.clone(),
            )
            .await,
        PreparationDenial::Missing,
    );
    let fresh = lease(
        fixture
            .client()
            .command::<BeginPreparation>(
                &fixture.target,
                identity()?,
                fixture.begin(proof.token().operation),
            )
            .await?
            .output,
    )?;
    assert!(fresh.token.attempt > proof.token().attempt);
    assert_eq!(fresh.token.artifact_operation, artifact_number(2));
    assert!(retained(&fixture.handle, proof.token()).await?.is_none());
    denied(
        fixture
            .client()
            .command::<RegisterCatalogAttestation>(&fixture.target, identity()?, certificate)
            .await,
        PreparationDenial::Stale,
    );
    drop(proof);
    cleaned(root.path(), &budget).await?;
    fixture.runtime.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn successor_owner_cannot_register_old_proofs_but_preserves_exact_outcome_replay() -> Result {
    let fixture = Fixture::new(ObjectFormat::Sha1).await?;
    let (proof, root, budget) = prepared(&fixture, [56; 16]).await?;
    let mutation = identity()?;
    let first = proof.attest(mutation).await?;
    let certificate = saved(&fixture.handle, proof.token().operation)
        .await?
        .ok_or("certificate")?;
    fixture.handle.drain().await?;
    fixture.runtime.shutdown().await?;
    let session = SessionId::from_bytes([57; 16]);
    let runtime = CellRuntime::new(SqlWorkerPool::new(1, 4)?, 64 << 20, session)?;
    let authority = CellAuthority::new(fixture.layout.clone());
    let idle = authority
        .load(fixture.target.cell_id())
        .await?
        .ok_or("idle")?;
    let provision = CellCatalog::new(fixture.layout.clone(), fixture.target.tenant())
        .lookup(fixture.target.cell_id())
        .await?
        .ok_or("provision")?;
    let handle = runtime
        .acquire_idle_restored(
            provision,
            fixture.replica.clone(),
            authority,
            idle,
            fixture.root.path().join("attestation-b.sqlite"),
            Owner {
                session,
                endpoint: "https://attestation-b.invalid".into(),
            },
        )
        .await?;
    let client = CellClient::local(Arc::clone(&fixture.registry), handle.clone());
    assert!(handle.owner_fence().epoch > proof.token().owner.epoch);
    denied(
        client
            .command::<RegisterCatalogAttestation>(
                &fixture.target,
                identity()?,
                certificate.clone(),
            )
            .await,
        PreparationDenial::Stale,
    );
    let replay = client
        .command::<RegisterCatalogAttestation>(&fixture.target, mutation, certificate.clone())
        .await?;
    assert_eq!(replay.output, first.output);
    assert_eq!(replay.receipt, first.receipt);
    assert_eq!(
        saved(&handle, proof.token().operation).await?,
        Some(certificate.clone())
    );
    assert_eq!(
        retained(&handle, proof.token()).await?,
        Some(certificate.clone())
    );
    let claimed = lease(
        client
            .command::<ClaimPreparation>(&fixture.target, identity()?, request(proof.token()))
            .await?
            .output,
    )?;
    assert_eq!(claimed.token.artifact_operation, artifact_number(2));
    assert_eq!(retained(&handle, proof.token()).await?, Some(certificate));
    assert!(retained(&handle, claimed.token).await?.is_none());
    assert!(saved(&handle, proof.token().operation).await?.is_none());
    assert_eq!(
        catalog_state(&handle).await?,
        [0u64.to_be_bytes(), 0u64.to_be_bytes(), 1u64.to_be_bytes()].concat()
    );
    drop(proof);
    cleaned(root.path(), &budget).await?;
    runtime.shutdown().await?;
    Ok(())
}
