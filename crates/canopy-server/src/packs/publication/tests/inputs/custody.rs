use super::super::{
    prepare::{cleaned, physical},
    publishing::{plan, update},
};
use super::*;
use crate::packs::{
    catalog::{CatalogFileLimits, CatalogFiles, CatalogIndexes, CatalogReader},
    metadata::{MetadataSegment, tests::limits},
    verification::{
        PhysicalPackWitness, PhysicalVerifier,
        physical::tests::{independence::git_input, physical_limits, prepared_for_store},
    },
};
use crate::{ObjectId, ObjectKind};
use cellule_ltx::DiskBudget;

struct Recovered {
    fixture: Fixture,
    coordinator: StagingCoordinator,
    ticket: StagingTicket,
    store: Arc<ArtifactStore>,
    provider: Arc<dyn object_store::ObjectStore>,
    prior: NativeInputCertificate,
    adopted: NativeInputCertificate,
    native: NativePackDescriptor,
}
impl Recovered {
    async fn new(format: ObjectFormat) -> Result<Self> {
        Self::new_with_inventory(format, false).await
    }
    async fn new_with_inventory(format: ObjectFormat, alter_index: bool) -> Result<Self> {
        let fixture = Fixture::new(format).await?;
        let (old_coordinator, old_ticket) = active(&fixture, [184; 16]).await?;
        let provider: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let store = Arc::new(ArtifactStore::new(provider.clone(), fixture.repository));
        let captured = store.clone();
        let task = old_ticket.spawn(move |context| async move {
            capture_real(context, captured)
                .await
                .map_err(StagingError::Input)
        })?;
        let mut prior = task.wait().await.map_err(|e| e.to_string())?;
        let index = NativeInputIndex::new(store.clone(), format);
        let mut cursor = index.cursor(prior.root()?, None)?;
        let native = cursor.next().await?.ok_or("native input")?;
        assert!(cursor.next().await?.is_none());
        if alter_index {
            let mut different = native;
            different.index.manifest_digest[0] ^= 1;
            let provider = store.clone();
            let task = old_ticket.spawn(move |context| async move {
                context
                    .seal_native_inputs(provider, [different])
                    .await
                    .map_err(|e| StagingError::Input(Box::new(e)))
            })?;
            prior = task.wait().await.map_err(|e| e.to_string())?;
        }
        let checkpoint = old_ticket
            .register_inputs(prior.clone(), identity()?)
            .map_err(|(e, _)| e)?;
        checkpoint.wait().await.map_err(|e| e.to_string())?;
        old_ticket.stop();
        assert!(old_coordinator.close_and_drain().await.is_empty());
        let coordinator =
            StagingCoordinator::new(fixture.target.clone(), StagingLimits::default())?;
        let ready = ReadyStaging::claim(
            fixture.client(),
            fixture.target.clone(),
            LeaseRequest {
                check: LeaseCheck {
                    token: prior.token()?,
                    actor: "owner".into(),
                },
                lease_ms: DEFAULT_LEASE_MS,
            },
            identity()?,
        )
        .await?;
        let ticket = coordinator.submit(ready).map_err(|(e, _)| e)?;
        assert!(matches!(
            timeout(Duration::from_secs(10), ticket.wait()).await?,
            StagingState::Active(_)
        ));
        let old = prior.clone();
        let adopt_store = store.clone();
        let task = ticket.spawn(move |context| async move {
            context
                .adopt_native_inputs(adopt_store, &old)
                .await
                .map_err(|e| StagingError::Input(Box::new(e)))
        })?;
        let adopted = task.wait().await.map_err(|e| e.to_string())?;

        Ok(Self {
            fixture,
            coordinator,
            ticket,
            store,
            provider,
            prior,
            adopted,
            native,
        })
    }
    async fn register(&self) -> Result {
        let observer = self
            .ticket
            .register_inputs(self.adopted.clone(), identity()?)
            .map_err(|(e, _)| e)?;
        observer.wait().await.map_err(|e| e.to_string())?;
        assert!(matches!(
            timeout(Duration::from_secs(10), self.ticket.wait()).await?,
            StagingState::Active(_)
        ));
        Ok(())
    }
    async fn base(&self) -> Result<(Arc<PreparationBaseResolver>, Arc<CatalogIndexes>)> {
        self.ticket.seal()?;
        assert!(matches!(
            timeout(Duration::from_secs(10), self.ticket.wait_terminal()).await?,
            StagingState::Bound(_)
        ));
        let indexes = Arc::new(CatalogIndexes::new(self.store.clone(), self.fixture.format));
        let files = Arc::new(CatalogFiles::new(
            self.fixture.root.path(),
            DiskBudget::new(64 << 20),
            self.store.clone(),
            self.fixture.format,
            CatalogFileLimits::default(),
        )?);
        let base = Arc::new(self.ticket.open_base(indexes.clone(), files).await?);
        Ok((base, indexes))
    }
    async fn physical(
        &self,
        root: &std::path::Path,
        budget: DiskBudget,
    ) -> Result<(PhysicalPackWitness, Arc<MetadataSegment>, ObjectId)> {
        let mut verifier = PhysicalVerifier::download(
            root,
            budget,
            &self.store,
            self.native,
            physical_limits(),
            crate::native_resources::NativeResources::default()
                .scope(crate::native_resources::NativeClass::Foreground),
        )
        .await?;
        let segment = verifier
            .inspect_next_shard(self.native.object_count)
            .await?;
        let tip = segment
            .headers_after(None)?
            .into_iter()
            .find(|h| h.object.kind == ObjectKind::Commit)
            .ok_or("tip")?
            .object
            .oid;
        Ok((verifier.finish().await?, segment, tip))
    }
    async fn publish_background(&self, operation: [u8; 16], name: &str, blobs: usize) -> Result {
        let (base, _, _) =
            super::super::prepare::opened(&self.fixture, operation, self.store.clone()).await?;
        let native = prepared_for_store(
            self.fixture.format,
            blobs,
            base.context().operation,
            self.provider.clone(),
            self.store.clone(),
        )
        .await?;
        let tip = native
            .fixture
            .objects
            .values()
            .find(|(o, _)| o.kind == ObjectKind::Commit)
            .ok_or("background tip")?
            .0
            .oid;
        let root = tempfile::TempDir::new()?;
        let budget = DiskBudget::new(64 << 20);
        let mut builder =
            CatalogPreparation::new(root.path(), budget.clone(), base, limits()).await?;
        let (witness, segments) = physical(&native, root.path(), budget.clone()).await?;
        builder.begin_pack(witness)?;
        for segment in segments {
            builder.add_segment(segment).await?;
        }
        builder.finish_pack().await?;
        let prepared = builder.finish().await?;
        let proof = prepared
            .ref_proof(
                plan(vec![update(name, None, Some(tip))]),
                root.path(),
                budget.clone(),
                limits(),
            )
            .await?;
        assert!(matches!(
            self.fixture
                .client()
                .command::<PublishCatalogRefs>(&self.fixture.target, identity()?, proof)
                .await?
                .output,
            PublicationReply::Published(_)
        ));
        drop((prepared, native));
        cleaned(root.path(), &budget).await?;
        Ok(())
    }
    async fn close(self) -> Result {
        self.ticket.stop();
        assert!(self.coordinator.close_and_drain().await.is_empty());
        self.fixture.runtime.shutdown().await?;
        Ok(())
    }
}
fn digest(proof: &NativeInputCertificate) -> Result<[u8; 32]> {
    let mut e = BoundedEncoder::new(CERTIFICATE_BYTES)?;
    proof.encode(&mut e)?;
    Ok(*blake3::hash(&e.finish()).as_bytes())
}

#[tokio::test]
async fn retained_input_custody_reconciles_over_moving_nonempty_base_and_cold_clones() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let recovered = Recovered::new(format).await?;
        recovered.register().await?;
        mutate(
            &recovered.fixture.handle,
            format!(
                "UPDATE catalog_leases SET expires_at_ms=0 WHERE admission_sequence={}",
                recovered.prior.token()?.attempt
            ),
        )
        .await?;
        recovered
            .publish_background([185; 16], "refs/heads/base", 2)
            .await?;
        let (base, indexes) = recovered.base().await?;
        assert_eq!(base.generation_fact().generation, 1);
        let root = tempfile::TempDir::new()?;
        let budget = DiskBudget::new(64 << 20);
        let mut builder =
            CatalogPreparation::new(root.path(), budget.clone(), base, limits()).await?;
        let (witness, segment, tip) = recovered.physical(root.path(), budget.clone()).await?;
        builder.begin_retained_pack(witness).await?;
        assert!(indexes.input_stats().loaded_nodes > 0);
        builder.add_segment(segment).await?;
        builder.finish_pack().await?;
        let prepared = builder.finish().await?;
        assert_eq!(
            prepared
                .certificate()
                .await?
                .data()?
                .input_checkpoint_digest,
            Some(digest(&recovered.adopted)?)
        );
        recovered
            .publish_background([186; 16], "refs/heads/advance", 3)
            .await?;
        let next = prepared.reconcile().await?;
        assert_eq!(next.base().generation, 2);
        let proof = next
            .ref_proof(
                plan(vec![update("refs/heads/main", None, Some(tip))]),
                root.path(),
                budget.clone(),
                limits(),
            )
            .await?;
        assert_eq!(
            proof.certificate.data()?.input_checkpoint_digest,
            Some(digest(&recovered.adopted)?)
        );
        assert!(proof.certificate.bytes()?.len() <= CERTIFICATE_BYTES as usize);
        let mut worst = proof.certificate.data()?;
        worst.actor = "z".repeat(64);
        worst.completion_digest = Some([93; 32]);
        assert!(
            CatalogCertificate::seal(&worst, &[16; 32])?.bytes()?.len()
                <= CERTIFICATE_BYTES as usize
        );
        let mutation = identity()?;
        let committed = recovered
            .fixture
            .client()
            .command::<PublishCatalogRefs>(&recovered.fixture.target, mutation, proof.clone())
            .await?;
        let replay = recovered
            .fixture
            .client()
            .command::<PublishCatalogRefs>(&recovered.fixture.target, mutation, proof)
            .await?;
        assert_eq!(committed.receipt, replay.receipt);
        assert!(matches!(
            committed.output,
            PublicationReply::Published(PublishedRefs { generation: 3, .. })
        ));
        let catalog = next.catalog();
        drop((next, prepared));
        cleaned(root.path(), &budget).await?;
        let cold_root = tempfile::TempDir::new()?;
        let cold_budget = DiskBudget::new(64 << 20);
        let reader = CatalogReader::open(
            Arc::new(CatalogIndexes::new(recovered.store.clone(), format)),
            catalog,
        )
        .await?;
        let files = CatalogFiles::new(
            cold_root.path(),
            cold_budget.clone(),
            recovered.store.clone(),
            format,
            CatalogFileLimits::default(),
        )?;
        let selected = reader
            .lookup(tip, &files, &files)
            .await?
            .ok_or("retained tip")?;
        assert_eq!(selected.source.record.native(), recovered.native);
        let backend = crate::git_http::GitHttpBackend::initialize(
            cold_root.path().into(),
            cold_budget,
            "refs/heads/main",
            format,
            crate::native_resources::NativeResources::default()
                .scope(crate::native_resources::NativeClass::Foreground),
        )
        .await?;
        backend
            .cache
            .download_native(&recovered.store, selected.source.record.native())
            .await?;
        backend
            .cache
            .store_refs(&std::collections::BTreeMap::from([(
                "refs/heads/main".into(),
                crate::RefExpectation {
                    oid: Some(tip),
                    version: 1,
                },
            )]))
            .await?;
        let client = tempfile::TempDir::new()?;
        let clone = client.path().join("clone");
        git_input(
            client.path(),
            &[
                "-c",
                "protocol.file.allow=always",
                "clone",
                "--no-local",
                backend.git_dir().to_str().ok_or("cache path")?,
                clone.to_str().ok_or("clone path")?,
            ],
            &[],
        )
        .await?;
        git_input(&clone, &["fsck", "--full"], &[]).await?;
        recovered.close().await?;
    }
    Ok(())
}

#[tokio::test]
async fn retained_input_custody_requires_current_successor_checkpoint_and_keeps_raw_namespace_guard()
-> Result {
    let recovered = Recovered::new(ObjectFormat::Sha256).await?;
    let (base, _) = recovered.base().await?;
    for raw in [true, false] {
        let root = tempfile::TempDir::new()?;
        let budget = DiskBudget::new(64 << 20);
        let mut builder =
            CatalogPreparation::new(root.path(), budget.clone(), base.clone(), limits()).await?;
        let (witness, segment, _) = recovered.physical(root.path(), budget.clone()).await?;
        let result = if raw {
            builder.begin_pack(witness)
        } else {
            builder.begin_retained_pack(witness).await
        };
        assert!(result.is_err());
        assert!(builder.finish().await.is_err());
        drop(segment);
        cleaned(root.path(), &budget).await?;
    }
    recovered.close().await?;
    Ok(())
}

#[tokio::test]
async fn retained_input_custody_rejects_native_pair_missing_from_authenticated_inventory() -> Result
{
    let recovered = Recovered::new(ObjectFormat::Sha256).await?;
    // A fresh checkpoint in the successor namespace cannot authorize the old
    // physical pair simply because both belong to the same logical request.
    let wrong = seal(
        &recovered.fixture,
        &recovered.ticket,
        recovered.store.clone(),
        1,
    )
    .await?;
    let observer = recovered
        .ticket
        .register_inputs(wrong, identity()?)
        .map_err(|(e, _)| e)?;
    observer.wait().await.map_err(|e| e.to_string())?;
    let (base, _) = recovered.base().await?;
    let root = tempfile::TempDir::new()?;
    let budget = DiskBudget::new(64 << 20);
    let mut builder = CatalogPreparation::new(root.path(), budget.clone(), base, limits()).await?;
    let (witness, segment, _) = recovered.physical(root.path(), budget.clone()).await?;
    assert!(builder.begin_retained_pack(witness).await.is_err());
    assert!(builder.finish().await.is_err());
    drop(segment);
    cleaned(root.path(), &budget).await?;
    recovered.close().await?;
    Ok(())
}

#[tokio::test]
async fn retained_input_custody_final_certificate_digest_is_checked_and_old_domain_rejected()
-> Result {
    let recovered = Recovered::new(ObjectFormat::Sha256).await?;
    recovered.register().await?;
    let (base, _) = recovered.base().await?;
    let root = tempfile::TempDir::new()?;
    let budget = DiskBudget::new(64 << 20);
    let mut builder = CatalogPreparation::new(root.path(), budget.clone(), base, limits()).await?;
    let (witness, segment, tip) = recovered.physical(root.path(), budget.clone()).await?;
    builder.begin_retained_pack(witness).await?;
    builder.add_segment(segment).await?;
    builder.finish_pack().await?;
    let prepared = builder.finish().await?;
    let proof = prepared
        .ref_proof(
            plan(vec![update("refs/heads/main", None, Some(tip))]),
            root.path(),
            budget.clone(),
            limits(),
        )
        .await?;
    let mut data = proof.certificate.data()?;
    data.input_checkpoint_digest = Some([92; 32]);
    let wrong = CatalogCertificate::seal(&data, &[16; 32])?;
    assert!(
        matches!(recovered.fixture.client().command::<RegisterCatalogAttestation>(&recovered.fixture.target,identity()?,wrong.clone()).await,Err(InvocationError::Rejected(outcome)) if outcome.output==AttestationOutcome::Denied(PreparationDenial::Conflict))
    );
    let mut wrong_proof = proof.clone();
    wrong_proof.certificate = wrong;
    assert!(
        matches!(recovered.fixture.client().command::<PublishCatalogRefs>(&recovered.fixture.target,identity()?,wrong_proof).await,Err(InvocationError::Rejected(outcome)) if outcome.output==PublicationReply::Denied(PreparationDenial::Conflict))
    );
    let mut old = proof.certificate.clone();
    let domain = b"canopy.catalog-attestation.v3\0";
    let at = old
        .0
        .body
        .windows(domain.len())
        .position(|b| b == domain)
        .ok_or("catalog domain")?;
    old.0.body[at + domain.len() - 2] = b'2';
    assert!(old.bytes().is_err());
    let mut e = BoundedEncoder::new(CERTIFICATE_BYTES)?;
    old.0.encode(&mut e)?;
    let bytes = e.finish();
    let mut d = BoundedDecoder::new(&bytes, CERTIFICATE_BYTES)?;
    assert!(CatalogCertificate::decode(&mut d).is_err());
    assert!(matches!(
        recovered
            .fixture
            .client()
            .command::<PublishCatalogRefs>(&recovered.fixture.target, identity()?, proof)
            .await?
            .output,
        PublicationReply::Published(_)
    ));
    drop(prepared);
    cleaned(root.path(), &budget).await?;
    recovered.close().await?;
    Ok(())
}

#[tokio::test]
async fn retained_input_custody_matches_exact_index_incarnation_not_just_pack_key() -> Result {
    let recovered = Recovered::new_with_inventory(ObjectFormat::Sha256, true).await?;
    recovered.register().await?;
    let (base, indexes) = recovered.base().await?;
    let key = crate::packs::directory::SegmentKey {
        operation: recovered.native.operation,
        digest: recovered.native.pack.digest,
    };
    let selected = indexes
        .inputs()
        .find(recovered.adopted.root()?, key)
        .await?
        .ok_or("indexed pair")?;
    assert_eq!(selected.pack, recovered.native.pack);
    assert_ne!(
        selected.index.manifest_digest,
        recovered.native.index.manifest_digest
    );
    let root = tempfile::TempDir::new()?;
    let budget = DiskBudget::new(64 << 20);
    let mut builder = CatalogPreparation::new(root.path(), budget.clone(), base, limits()).await?;
    let (witness, segment, _) = recovered.physical(root.path(), budget.clone()).await?;
    assert!(builder.begin_retained_pack(witness).await.is_err());
    assert!(builder.finish().await.is_err());
    drop(segment);
    cleaned(root.path(), &budget).await?;
    recovered.close().await?;
    Ok(())
}

#[tokio::test]
async fn retained_input_custody_issuer_and_final_command_recheck_revocation_expiry_and_claim()
-> Result {
    for loss in [0, 1, 2] {
        let recovered = Recovered::new(ObjectFormat::Sha256).await?;
        recovered.register().await?;
        let (base, _) = recovered.base().await?;
        let root = tempfile::TempDir::new()?;
        let budget = DiskBudget::new(64 << 20);
        let mut builder =
            CatalogPreparation::new(root.path(), budget.clone(), base, limits()).await?;
        let (witness, segment, tip) = recovered.physical(root.path(), budget.clone()).await?;
        builder.begin_retained_pack(witness).await?;
        builder.add_segment(segment).await?;
        builder.finish_pack().await?;
        let prepared = builder.finish().await?;
        let proof = prepared
            .ref_proof(
                plan(vec![update("refs/heads/main", None, Some(tip))]),
                root.path(),
                budget.clone(),
                limits(),
            )
            .await?;
        let denial = match loss {
            0 => {
                mutate(
                    &recovered.fixture.handle,
                    "UPDATE repository_identity SET owner='other' WHERE singleton=1".into(),
                )
                .await?;
                PreparationDenial::Unauthorized
            }
            1 => {
                mutate(&recovered.fixture.handle,format!("UPDATE catalog_operations SET expires_at_ms=0; UPDATE catalog_leases SET expires_at_ms=0 WHERE admission_sequence={}",prepared.token().attempt)).await?;
                PreparationDenial::Expired
            }
            _ => {
                recovered
                    .fixture
                    .client()
                    .command::<ClaimPreparation>(
                        &recovered.fixture.target,
                        identity()?,
                        LeaseRequest {
                            check: LeaseCheck {
                                token: prepared.token(),
                                actor: "owner".into(),
                            },
                            lease_ms: DEFAULT_LEASE_MS,
                        },
                    )
                    .await?;
                PreparationDenial::Stale
            }
        };
        assert!(prepared.certificate().await.is_err());
        assert!(
            matches!(recovered.fixture.client().command::<PublishCatalogRefs>(&recovered.fixture.target,identity()?,proof).await,Err(InvocationError::Rejected(outcome)) if outcome.output==PublicationReply::Denied(denial))
        );
        let state = recovered
            .fixture
            .handle
            .query(0, 32, |c| {
                let generation: u64 = c.query_row(
                    "SELECT generation FROM catalog_state WHERE singleton=1",
                    [],
                    |r| r.get(0),
                )?;
                let refs: u64 = c.query_row("SELECT count(*) FROM refs", [], |r| r.get(0))?;
                Ok([generation.to_be_bytes(), refs.to_be_bytes()].concat())
            })
            .await?;
        assert_eq!(state, vec![0; 16]);
        drop(prepared);
        cleaned(root.path(), &budget).await?;
        recovered.close().await?;
    }
    Ok(())
}
