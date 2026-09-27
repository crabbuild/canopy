use super::*;
use canopy_server::{CanopyApplication, build_descriptor, deployment::Deployment};
use crab_cell_app::CellApplication;
use crab_cell_runtime::{
    cell::application::ApplicationIdentity, identity::RequestId, recovery::release::ReleaseState,
};
use crab_storage::Store;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

fn now() -> Result<i64> {
    Ok(i64::try_from(
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis(),
    )?)
}

#[tokio::test(flavor = "multi_thread")]
async fn maintenance_drains_two_nodes_and_fences_startup_until_exact_resume() -> Result {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let files = tempfile::TempDir::new()?;
    let a = available_address().await?;
    let b = available_address().await?;
    let (ca, tls) = peers::tls_config()?;
    let (peer_a, _proxy_a) = peers::proxy(a, Arc::clone(&tls)).await?;
    let (peer_b, _proxy_b) = peers::proxy(b, tls).await?;
    let mut first_config = config(a, files.path().join("first"));
    first_config.peer_endpoint = peer_a;
    first_config.peer_ca_pem = Some(ca.clone());
    let application = CanopyApplication::compile(build_descriptor(
        include_bytes!("../../Cargo.lock"),
        env!("CARGO_PKG_VERSION"),
    ))?;
    let deployment = Deployment::new(
        Store::new(Arc::clone(&store)),
        first_config.store_prefix.clone(),
        ApplicationIdentity::new(first_config.tenant, first_config.application),
        first_config.fleet,
        first_config.image,
        application.registry(),
    )?;
    let first = CanopyServer::start(first_config, Arc::clone(&store)).await?;
    let url = create_repository(a, "retained").await?;
    let source = files.path().join("source");
    run_git(None, &["init", "-b", "main", path_str(&source)?]).await?;
    run_git(Some(&source), &["config", "user.name", "Canopy Test"]).await?;
    run_git(
        Some(&source),
        &["config", "user.email", "canopy@example.invalid"],
    )
    .await?;
    std::fs::write(source.join("README.md"), b"Survives fleet maintenance\n")?;
    run_git(Some(&source), &["add", "."]).await?;
    run_git(Some(&source), &["commit", "-m", "Before maintenance"]).await?;
    run_git(
        Some(&source),
        &[
            "-c",
            "http.extraHeader=Authorization: Bearer local-test-token",
            "push",
            &url,
            "main",
        ],
    )
    .await?;
    let mut second_config = config(b, files.path().join("second"));
    second_config.peer_endpoint = peer_b;
    second_config.peer_ca_pem = Some(ca);
    second_config.node = NodeId::from_bytes(uuid::Uuid::new_v4().into_bytes());
    let second = CanopyServer::start(second_config, Arc::clone(&store)).await?;
    create_repository(b, "other").await?;
    let initial = deployment.status(now()?).await?;
    assert_eq!(initial.advertised_sessions, 2);
    assert!(!initial.drained);
    let operation = RequestId::from_bytes(uuid::Uuid::new_v4().into_bytes());
    let started = deployment.begin_maintenance(operation).await?;
    assert_eq!(started.state(), ReleaseState::Maintenance);
    assert_eq!(deployment.begin_maintenance(operation).await?, started);
    assert!(deployment.require_ready().await.is_err());
    let other_operation = RequestId::from_bytes(uuid::Uuid::new_v4().into_bytes());
    assert!(deployment.begin_maintenance(other_operation).await.is_err());
    assert!(
        deployment
            .end_maintenance(other_operation, now()?)
            .await
            .is_err()
    );
    let denied_config = config(available_address().await?, files.path().join("denied"));
    assert!(
        CanopyServer::start(denied_config, Arc::clone(&store))
            .await
            .is_err()
    );
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            if deployment.status(now()?).await?.drained {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        Ok::<_, Box<dyn std::error::Error>>(())
    })
    .await??;
    first.shutdown().await?;
    second.shutdown().await?;
    let resumed = deployment.end_maintenance(operation, now()?).await?;
    assert_eq!(resumed.state(), ReleaseState::Ready);
    assert_eq!(
        deployment.end_maintenance(operation, now()?).await?,
        resumed
    );
    assert_eq!(deployment.begin_maintenance(operation).await?, resumed);
    let mut wrong_identity = config(
        available_address().await?,
        files.path().join("wrong-identity"),
    );
    wrong_identity.tenant = TenantId::from_bytes([255; 16]);
    assert!(
        CanopyServer::start(wrong_identity, Arc::clone(&store))
            .await
            .is_err()
    );
    let mut wrong_image = config(available_address().await?, files.path().join("wrong-image"));
    wrong_image.image = Digest::from_bytes([255; 32]);
    assert!(
        CanopyServer::start(wrong_image, Arc::clone(&store))
            .await
            .is_err()
    );
    let c = available_address().await?;
    let restored = CanopyServer::start(config(c, files.path().join("restored")), store).await?;
    let clone = files.path().join("clone");
    run_git(
        None,
        &[
            "-c",
            "http.extraHeader=Authorization: Bearer local-test-token",
            "clone",
            &format!("http://{c}/canopy/retained.git"),
            path_str(&clone)?,
        ],
    )
    .await?;
    assert_eq!(
        std::fs::read(clone.join("README.md"))?,
        b"Survives fleet maintenance\n"
    );
    run_git(Some(&clone), &["fsck", "--strict", "--full"]).await?;
    restored.shutdown().await?;
    Ok(())
}
