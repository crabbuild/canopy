use super::*;
use crate::{CanopyApplication, RepositoryModule, build_descriptor, repository_target};
use cellule_app::CellApplication;
use cellule_runtime::{ApplicationId, CatalogEntry, CatalogRole, CellModule, TenantId};
use object_store::{ObjectStoreExt, memory::InMemory};

type TestResult = std::result::Result<(), Box<dyn std::error::Error>>;

fn fixture() -> std::result::Result<Deployment, Box<dyn std::error::Error>> {
    let app = CanopyApplication::compile(build_descriptor(
        include_bytes!("../../Cargo.lock"),
        "deployment-test",
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
