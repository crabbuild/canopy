use super::publishing::{assembled, plan, state, update};
use super::*;
use crate::RefExpectation;
use crate::packs::ref_state::{RefStateIndex, RefStateSnapshot, RefStateSnapshotRoot};

#[tokio::test]
async fn query_derived_ref_preparation_retains_exact_base_and_canonical_plan() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let fixture = Fixture::new(format).await?;
        let graph = assembled(&fixture, [201; 16], 3).await?;
        assert!(matches!(
            graph
                .prepared
                .prepare_ref_snapshot(&plan(vec![update(
                    "refs/heads/main",
                    None,
                    Some(graph.initial)
                )]))
                .await,
            Err(RefSnapshotPreparationError::Unavailable)
        ));
        let index = RefStateIndex::new(Arc::clone(&graph.store), format);
        let initial = index
            .prepare(
                None,
                graph.prepared.token().artifact_operation,
                &plan(vec![
                    update("refs/heads/main", None, Some(graph.initial)),
                    update("refs/tags/old", None, Some(graph.initial)),
                ]),
            )
            .await?;
        let old = RefStateSnapshotRoot::upload(
            &graph.store,
            graph.prepared.token().artifact_operation,
            RefStateSnapshot {
                repository: fixture.repository,
                format,
                generation: 1,
                default_branch: "refs/heads/main".into(),
                root: Some(initial.root()),
            },
        )
        .await?;
        fixture
            .install_generation(1, graph.prepared.catalog(), Some(old))
            .await?;
        let prepared = graph.prepared.reconcile().await?;
        assert_eq!(prepared.base().refs, Some(old));
        let changes = plan(vec![
            update("refs/tags/old", Some((graph.initial, 1)), None),
            update("refs/heads/main", Some((graph.initial, 1)), Some(graph.tip)),
            update("refs/heads/new", None, Some(graph.other)),
        ]);
        fn send<T: Send>(value: T) -> T {
            value
        }
        let transition = send(prepared.prepare_ref_snapshot(&changes)).await?;
        assert_eq!(transition.base(), prepared.base());
        assert_eq!(
            transition.plan_digest(),
            super::super::ref_proof::plan_digest(&changes)?
        );
        let saved = transition.snapshot().read(&graph.store).await?;
        assert_eq!(
            (saved.generation, saved.format, saved.repository),
            (2, format, fixture.repository)
        );
        assert_eq!(saved.default_branch, "refs/heads/main");
        assert_eq!(
            index.read(saved.root.clone(), "refs/heads/main").await?,
            Some(RefExpectation {
                oid: Some(graph.tip),
                version: 2
            })
        );
        assert_eq!(
            index.read(saved.root.clone(), "refs/tags/old").await?,
            Some(RefExpectation {
                oid: None,
                version: 2
            })
        );
        assert_eq!(
            index
                .read(old.read(&graph.store).await?.root, "refs/heads/main")
                .await?,
            Some(RefExpectation {
                oid: Some(graph.initial),
                version: 1
            })
        );
        let retry = prepared.prepare_ref_snapshot(&changes).await?;
        assert_eq!(transition.snapshot(), retry.snapshot());
        let before = state(&fixture.handle).await?;
        let inline = Box::pin(prepared.ref_proof(
            plan(vec![update("refs/heads/untracked", None, Some(graph.tip))]),
            graph.root.path(),
            graph.budget.clone(),
            crate::packs::metadata::tests::limits(),
        ))
        .await?;
        assert_eq!(inline.certificate.data()?.base.refs, Some(old));
        let reply = fixture
            .client()
            .command::<PublishCatalogRefs>(&fixture.target, identity()?, inline)
            .await;
        assert!(
            matches!(reply, Err(InvocationError::Rejected(ref value)) if value.output==PublicationReply::Denied(PreparationDenial::Conflict))
        );
        assert_eq!(state(&fixture.handle).await?, before);

        fixture
            .install_generation(2, prepared.catalog(), Some(transition.snapshot()))
            .await?;
        let current = prepared.reconcile().await?;
        assert_eq!(current.base().refs, Some(transition.snapshot()));
        assert!(matches!(
            current.prepare_ref_snapshot(&changes).await,
            Err(RefSnapshotPreparationError::State(
                crate::packs::ref_state::RefStateError::Changed
            ))
        ));
        let mut denied = plan(vec![update("refs/heads/actor", None, Some(graph.tip))]);
        denied.actor = "outsider".into();
        assert!(matches!(
            current.prepare_ref_snapshot(&denied).await,
            Err(RefSnapshotPreparationError::Context)
        ));
    }
    Ok(())
}

#[tokio::test]
async fn selected_ref_metadata_rejects_cross_format_future_generation_and_absent_artifacts()
-> Result {
    let fixture = Fixture::new(ObjectFormat::Sha1).await?;
    let graph = assembled(&fixture, [202; 16], 0).await?;
    for (n, format, ref_generation, missing) in [
        (1, ObjectFormat::Sha256, 1, false),
        (2, ObjectFormat::Sha1, 3, false),
        (3, ObjectFormat::Sha1, 1, true),
    ] {
        let root = RefStateSnapshotRoot::upload(
            &graph.store,
            graph.prepared.token().artifact_operation,
            RefStateSnapshot {
                repository: fixture.repository,
                format,
                generation: ref_generation,
                default_branch: "refs/heads/main".into(),
                root: None,
            },
        )
        .await?;
        let root = if missing {
            // A syntactically valid public descriptor is still not proof that
            // its selected manifest/body exists in the trusted store.
            let mut e = BoundedEncoder::new(128)?;
            root.encode(&mut e)?;
            let mut bytes = e.finish();
            let last = bytes.len() - 1;
            bytes[last] ^= 1;
            let mut d = BoundedDecoder::new(&bytes, 128)?;
            let root = RefStateSnapshotRoot::decode(&mut d)?;
            d.finish()?;
            root
        } else {
            root
        };
        fixture
            .install_generation(n, graph.prepared.catalog(), Some(root))
            .await?;
        let prepared = graph.prepared.reconcile().await?;
        let result = prepared
            .prepare_ref_snapshot(&plan(vec![update(
                "refs/heads/main",
                None,
                Some(graph.initial),
            )]))
            .await;
        if missing {
            assert!(matches!(
                result,
                Err(RefSnapshotPreparationError::Snapshot(_))
            ));
        } else {
            assert!(matches!(result, Err(RefSnapshotPreparationError::Context)));
        }
    }
    Ok(())
}

#[tokio::test]
async fn ref_root_is_in_joint_fact_codec_certificate_and_retained_generation() -> Result {
    let fixture = Fixture::new(ObjectFormat::Sha256).await?;
    let graph = assembled(&fixture, [203; 16], 0).await?;
    let root = RefStateSnapshotRoot::upload(
        &graph.store,
        graph.prepared.token().artifact_operation,
        RefStateSnapshot {
            repository: fixture.repository,
            format: fixture.format,
            generation: 0,
            default_branch: "refs/heads/main".into(),
            root: None,
        },
    )
    .await?;
    fixture
        .install_generation(1, graph.prepared.catalog(), Some(root))
        .await?;
    let prepared = graph.prepared.reconcile().await?;
    let base = prepared.base();
    let mut e = BoundedEncoder::new(512)?;
    base.encode(&mut e)?;
    let bytes = e.finish();
    let mut d = BoundedDecoder::new(&bytes, 512)?;
    assert_eq!(GenerationFact::decode(&mut d)?, base);
    d.finish()?;
    for at in 0..bytes.len() {
        let mut d = BoundedDecoder::new(&bytes[..at], 512)?;
        assert!(GenerationFact::decode(&mut d).is_err());
    }
    let certificate = prepared.issue_certificate(None, None).await?;
    assert!(certificate.bytes()?.len() <= CERTIFICATE_BYTES as usize);
    assert_eq!(certificate.data()?.base.refs, Some(root));
    let mut maximal = certificate.data()?;
    maximal.actor = "a".repeat(64);
    maximal.retention_floor = maximal.base.generation;
    maximal.retention_certificate = maximal.base.certificate;
    maximal.object_count = i64::MAX as u64;
    maximal.edge_count = i64::MAX as u64;
    maximal.input_count = i64::MAX as u64;
    maximal.input_checkpoint_digest = Some([51; 32]);
    maximal.refs_digest = Some([52; 32]);
    maximal.completion_digest = Some([53; 32]);
    maximal.token.owner.epoch = u64::MAX;
    maximal.token.attempt = i64::MAX as u64;
    assert!(
        CatalogCertificate::seal(&maximal, &[16; 32])?
            .bytes()?
            .len()
            <= CERTIFICATE_BYTES as usize
    );
    let mut altered = certificate.clone();
    let mut data = certificate.data()?;
    data.base.refs = None;
    let mut e = BoundedEncoder::new(960)?;
    data.encode(&mut e)?;
    altered.0.body = e.finish();
    assert!(!altered.authenticated(&[16; 32]));
    fixture
        .install_generation(2, prepared.catalog(), Some(root))
        .await?;
    let reply = fixture
        .client()
        .command::<ReapPreparation>(
            &fixture.target,
            identity()?,
            MaintenanceRequest {
                repository: fixture.repository,
                actor: "owner".into(),
                owner: graph.prepared.token().owner,
            },
        )
        .await?;
    assert_eq!(reply.output, 0); // original floor zero retains both joint roots
    let mut invalid = base;
    invalid.generation = 0;
    invalid.catalog = None;
    invalid.certificate = None;
    assert!(invalid.validate().is_err()); // empty genesis cannot carry a root
    Ok(())
}
