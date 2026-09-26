use super::*;
use crate::{CanopyApplication, RepositoryModule, build_descriptor, repository_target};
use cellule_app::CellApplication;
use cellule_runtime::{ApplicationId, CatalogEntry, CatalogRole, CellModule, TenantId};
use object_store::{ObjectStoreExt, memory::InMemory};

type TestResult = std::result::Result<(), Box<dyn std::error::Error>>;

fn fixture() -> std::result::Result<Deployment, Box<dyn std::error::Error>> {
    let app = CanopyApplication::compile(build_descriptor(
        include_bytes!("../../Cargo.lock"),
        env!("CARGO_PKG_VERSION"),
    ))?;
    Ok(Deployment::new(
        Store::new(Arc::new(InMemory::new())),
        Path::from("deployment"),
        ApplicationIdentity::new(
            TenantId::from_bytes([1; 16]),
            ApplicationId::from_bytes([2; 16]),
        ),
        Digest::from_bytes([3; 32]),
        Digest::from_bytes([4; 32]),
        app.registry(),
    )?)
}

#[tokio::test]
async fn concurrent_initialization_selects_one_exact_release() -> TestResult {
    let deployment = fixture()?;
    let (left, right, third) = tokio::join!(
        deployment.initialize(),
        deployment.initialize(),
        deployment.initialize()
    );
    left?;
    right?;
    third?;
    let ready = deployment.record().await?;
    assert_eq!(ready.state(), ReleaseState::Ready);
    assert_eq!(ready.revision(), 3);
    deployment.initialize().await?;
    assert_eq!(deployment.record().await?, ready);
    Ok(())
}

#[tokio::test]
async fn interrupted_initial_activation_resumes_exact_phases() -> TestResult {
    for activate in [false, true] {
        let deployment = fixture()?;
        deployment
            .identities
            .initialize(deployment.identity)
            .await?;
        let digest = deployment.registry.release_digest();
        let mut operation = [0; 16];
        operation.copy_from_slice(&digest.as_bytes()[..16]);
        operation[0] |= 1;
        let operation = RequestId::from_bytes(operation);
        let prepared = deployment
            .releases
            .prepare(
                deployment.registry.release_bytes(),
                digest,
                0,
                &deployment.image,
                operation,
            )
            .await?;
        if activate {
            deployment
                .releases
                .start_activation(prepared.revision(), operation)
                .await?;
        }
        deployment.initialize().await?;
        deployment.require_ready().await?;
    }
    Ok(())
}

#[tokio::test]
async fn existing_catalog_without_release_is_not_silently_adopted() -> TestResult {
    let deployment = fixture()?;
    let target = repository_target(
        deployment.identity.tenant(),
        deployment.identity.application(),
        uuid::Uuid::new_v4().into_bytes(),
    )?;
    let code = deployment
        .registry
        .module_code(RepositoryModule::NAME)
        .ok_or("missing code")?;
    CellCatalog::new(deployment.layout.clone(), deployment.identity.tenant())
        .provision(CatalogEntry::new(&target, CatalogRole::Sql, code, 1)?)
        .await?;
    assert!(deployment.initialize().await.is_err());
    assert!(deployment.releases.load().await?.is_none());
    Ok(())
}

#[tokio::test]
async fn corrupted_selected_descriptor_closes_admission() -> TestResult {
    let deployment = fixture()?;
    deployment.initialize().await?;
    deployment
        .layout
        .store()
        .inner()
        .put(
            &deployment
                .layout
                .release_descriptor_path(deployment.registry.release_digest().as_bytes()),
            bytes::Bytes::from_static(b"corrupt").into(),
        )
        .await?;
    assert!(deployment.require_ready().await.is_err());
    Ok(())
}

#[tokio::test]
async fn expired_advertisement_is_not_drain_evidence() -> TestResult {
    use cellule_runtime::{NodeAdvertisement, NodeCapacity, NodeFailureDomain, NodeId, SessionId};
    let deployment = fixture()?;
    deployment.initialize().await?;
    let advertisement = NodeAdvertisement::sign(
        NodeId::from_bytes([5; 16]),
        SessionId::from_bytes([6; 16]),
        "https://fixture.example.invalid".into(),
        Digest::from_bytes([3; 32]),
        Digest::from_bytes([7; 32]),
        Digest::from_bytes([4; 32]),
        deployment.registry.release_digest(),
        &ed25519_dalek::SigningKey::from_bytes(&[8; 32]),
        1,
        1000,
        11_000,
        deployment.registry.module_digests(),
        vec![1],
        NodeFailureDomain::default(),
        NodeCapacity::default(),
    )?;
    deployment.nodes.create(advertisement, 1000).await?;
    let operation = RequestId::from_bytes(uuid::Uuid::new_v4().into_bytes());
    deployment.begin_maintenance(operation).await?;
    let status = deployment.status(1_000_000).await?;
    assert_eq!(status.advertised_sessions, 1);
    assert!(!status.drained);
    assert!(
        deployment
            .end_maintenance(operation, 1_000_000)
            .await
            .is_err()
    );
    Ok(())
}

fn recovery_config(data_dir: std::path::PathBuf) -> RecoveryConfig {
    RecoveryConfig {
        node: cellule_runtime::NodeId::from_bytes(uuid::Uuid::new_v4().into_bytes()),
        signing_key: ed25519_dalek::SigningKey::from_bytes(&[21; 32]),
        endpoint: "https://recovery.example.invalid".into(),
        data_dir,
        local_disk_limit_bytes: 256 * 1024 * 1024,
    }
}

async fn advertise_owner(deployment: &Deployment, expired: bool) -> TestResult {
    use cellule_runtime::{NodeAdvertisement, NodeCapacity, NodeFailureDomain, NodeId, SessionId};
    let now = crate::server::unix_now_ms()?;
    let issued = if expired { now - 20_000 } else { now };
    let advertisement = NodeAdvertisement::sign(
        NodeId::from_bytes([5; 16]),
        SessionId::from_bytes([6; 16]),
        "https://fixture.example.invalid".into(),
        deployment.nodes.fleet(),
        Digest::from_bytes([7; 32]),
        deployment.image_digest,
        deployment.registry.release_digest(),
        &ed25519_dalek::SigningKey::from_bytes(&[8; 32]),
        1,
        issued,
        issued + 10_000,
        deployment.registry.module_digests(),
        vec![1],
        NodeFailureDomain::default(),
        NodeCapacity::default(),
    )?;
    deployment.nodes.create(advertisement, issued).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn maintenance_recovery_initializes_abandoned_catalog_and_unpublished_owner() -> TestResult {
    use cellule_runtime::{IncarnationId, Owner, SessionId};
    let deployment = fixture()?;
    deployment.initialize().await?;
    advertise_owner(&deployment, true).await?;
    let files = tempfile::TempDir::new()?;
    let catalog = CellCatalog::new(deployment.layout.clone(), deployment.identity.tenant());
    let authority = CellAuthority::new(deployment.layout.clone());
    let code = deployment
        .registry
        .module_code(RepositoryModule::NAME)
        .ok_or("missing code")?;
    let mut cells = Vec::new();
    for unpublished in [false, true] {
        let target = repository_target(
            deployment.identity.tenant(),
            deployment.identity.application(),
            uuid::Uuid::new_v4().into_bytes(),
        )?;
        let proof = catalog
            .provision(CatalogEntry::new(&target, CatalogRole::Sql, code, 1)?)
            .await?;
        if unpublished {
            authority
                .create_initial(
                    &proof,
                    IncarnationId::from_bytes(uuid::Uuid::new_v4().into_bytes()),
                    Owner {
                        session: SessionId::from_bytes([6; 16]),
                        endpoint: "https://fixture.example.invalid".into(),
                    },
                )
                .await?;
        }
        cells.push(target.cell_id());
    }
    let operation = RequestId::from_bytes(uuid::Uuid::new_v4().into_bytes());
    assert!(
        deployment
            .recover_maintenance(operation, recovery_config(files.path().join("before")))
            .await
            .is_err()
    );
    deployment.begin_maintenance(operation).await?;
    deployment
        .recover_maintenance(operation, recovery_config(files.path().join("recover")))
        .await?;
    for cell in cells {
        let control = authority
            .load(cell)
            .await?
            .ok_or("missing recovered Cell")?;
        assert_eq!(control.value().state, ControlState::Idle);
        assert!(control.value().root.is_some());
        assert!(control.value().owner.is_none());
    }
    let now = crate::server::unix_now_ms()?;
    assert!(deployment.status(now).await?.drained);
    // A completed recovery is harmless to repeat while the same operation remains active.
    deployment
        .recover_maintenance(operation, recovery_config(files.path().join("repeat")))
        .await?;
    deployment
        .end_maintenance(operation, crate::server::unix_now_ms()?)
        .await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn maintenance_recovery_cannot_fence_a_live_owner_or_another_operation() -> TestResult {
    let deployment = fixture()?;
    deployment.initialize().await?;
    advertise_owner(&deployment, false).await?;
    let files = tempfile::TempDir::new()?;
    let operation = RequestId::from_bytes(uuid::Uuid::new_v4().into_bytes());
    deployment.begin_maintenance(operation).await?;
    let owner = cellule_runtime::SessionId::from_bytes([6; 16]);
    let before = deployment
        .nodes
        .load(owner, crate::server::unix_now_ms()?)
        .await?
        .ok_or("owner absent")?;
    assert!(
        deployment
            .recover_maintenance(
                RequestId::from_bytes([30; 16]),
                recovery_config(files.path().join("wrong"))
            )
            .await
            .is_err()
    );
    assert!(!files.path().join("wrong").exists());
    assert!(
        deployment
            .recover_maintenance(operation, recovery_config(files.path().join("live")))
            .await
            .is_err()
    );
    let now = crate::server::unix_now_ms()?;
    let after = deployment
        .nodes
        .load(owner, now)
        .await?
        .ok_or("live owner was fenced")?;
    assert_eq!(before.advertisement(), after.advertisement());
    let status = deployment.status(now).await?;
    assert_eq!(status.advertised_sessions, 1);
    assert!(!status.drained);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn maintenance_recovery_rejects_missing_published_root() -> TestResult {
    use cellule_runtime::{IncarnationId, Owner, RootRef, SessionId, Transition};
    let deployment = fixture()?;
    deployment.initialize().await?;
    advertise_owner(&deployment, true).await?;
    let files = tempfile::TempDir::new()?;
    let target = repository_target(
        deployment.identity.tenant(),
        deployment.identity.application(),
        uuid::Uuid::new_v4().into_bytes(),
    )?;
    let code = deployment
        .registry
        .module_code(RepositoryModule::NAME)
        .ok_or("missing code")?;
    let proof = CellCatalog::new(deployment.layout.clone(), deployment.identity.tenant())
        .provision(CatalogEntry::new(&target, CatalogRole::Sql, code, 1)?)
        .await?;
    let authority = CellAuthority::new(deployment.layout.clone());
    let initial = authority
        .create_initial(
            &proof,
            IncarnationId::from_bytes(uuid::Uuid::new_v4().into_bytes()),
            Owner {
                session: SessionId::from_bytes([6; 16]),
                endpoint: "https://fixture.example.invalid".into(),
            },
        )
        .await?;
    let mut published = initial.value().clone();
    published.revision += 1;
    published.progress += 1;
    published.state = ControlState::Serving;
    published.root = Some(RootRef {
        digest: Digest::from_bytes([99; 32]),
        txid: 1,
        checksum: (1 << 63) | 1,
        commit_sequence: 1,
    });
    authority
        .transition(&initial, published.clone(), Transition::Publish)
        .await?;
    let operation = RequestId::from_bytes(uuid::Uuid::new_v4().into_bytes());
    deployment.begin_maintenance(operation).await?;
    assert!(
        deployment
            .recover_maintenance(operation, recovery_config(files.path().join("corrupt")))
            .await
            .is_err()
    );
    let after = authority
        .load(target.cell_id())
        .await?
        .ok_or("lost authority")?;
    assert_eq!(after.value().root, published.root);
    assert_eq!(
        deployment.record().await?.state(),
        ReleaseState::Maintenance
    );
    Ok(())
}
