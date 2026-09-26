use std::{
    path::Path,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use canopy_server::{
    CanopyApplication, RepositoryCell, RepositoryModule, build_descriptor, git_gateway::GitGateway,
    http::GitHttpApi, repository_target,
};
use cellule_app::{CellApplication, CompiledApplication};
use cellule_host::{CellNode, CellNodeBuilder};
use cellule_ltx::{CellReplica, DiskBudget, Host, Limits};
use cellule_runtime::{
    ApplicationId, CatalogEntry, CatalogRole, CellAuthority, CellCatalog, CellClient, CellModule,
    CellStorageLayout, ControlState, Digest, Error, IncarnationId, NodeAdvertisement, NodeCapacity,
    NodeDirectory, NodeFailureDomain, NodeId, NodeLeaseGuard, Owner, SessionId, SqlWorkerPool,
    TenantId, VersionedNodeAdvertisement,
};
use cellule_store::Store;
use ed25519_dalek::SigningKey;
use object_store::{ObjectStore, memory::InMemory, path::Path as StorePath};
use tokio::{net::TcpListener, process::Command, sync::oneshot};
use tokio_util::sync::CancellationToken;

#[tokio::test(flavor = "multi_thread")]
async fn a_second_node_clones_from_the_published_root_after_local_disk_loss()
-> Result<(), Box<dyn std::error::Error>> {
    let application = Arc::new(CanopyApplication::compile(build_descriptor(
        include_bytes!("../Cargo.lock"),
        "owner-restart-test",
    ))?);
    let tenant = TenantId::from_bytes([31; 16]);
    let application_id = ApplicationId::from_bytes([32; 16]);
    let mut repository_id = [33; 16];
    repository_id[6] = 0x73;
    repository_id[8] = 0x83;
    let target = repository_target(tenant, application_id, repository_id)?;
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let layout = CellStorageLayout::new(
        Store::new(Arc::clone(&object_store)),
        StorePath::from("owner-restart-test"),
        *application_id.as_bytes(),
    );
    let registry = application.registry();
    let code = registry
        .module_code(RepositoryModule::NAME)
        .ok_or(Error::Registry("repository module missing"))?;
    let catalog = CellCatalog::new(layout.clone(), tenant);
    let proof = catalog
        .provision(CatalogEntry::new(&target, CatalogRole::Sql, code, 1)?)
        .await?;
    let authority = CellAuthority::new(layout.clone());
    let first_session = SessionId::from_bytes([34; 16]);
    let observed = authority
        .create_initial(
            &proof,
            IncarnationId::from_bytes([35; 16]),
            Owner {
                session: first_session,
                endpoint: "https://first.canopy.test".into(),
            },
        )
        .await?;
    let first_disk = tempfile::TempDir::new()?;
    let (first, directory, first_advertisement) =
        node(Arc::clone(&application), &layout, first_session).await?;
    let first_handle = first
        .runtime()
        .bootstrap(
            proof,
            replica(&layout, &target, *observed.value().incarnation.as_bytes())?,
            authority.clone(),
            observed,
            first_disk.path().join("repository.sqlite"),
            |transaction| {
                transaction.execute_batch(include_str!("../src/schema.sql"))?;
                Ok(())
            },
        )
        .await?;
    let app_handle = first.application_handle::<CanopyApplication>(
        CellClient::local(Arc::clone(&registry), first_handle),
        tenant,
        application_id,
    );
    let repository = Arc::new(RepositoryCell::new(&app_handle, target.clone())?);
    let first_gateway = Arc::new(GitGateway::new(
        Arc::clone(&repository),
        first_disk.path().to_path_buf(),
        Arc::clone(&object_store),
    ));
    let (address, stop, server) = serve(first_gateway).await?;
    let first_url = format!("http://{address}/canopy/example.git");
    let local = first_disk.path().join("local");
    run_git(None, &["init", "-b", "main", path_str(&local)?]).await?;
    run_git(Some(&local), &["config", "user.name", "Canopy Test"]).await?;
    run_git(
        Some(&local),
        &["config", "user.email", "canopy@example.invalid"],
    )
    .await?;
    tokio::fs::write(local.join("README.md"), b"published Cell root\n").await?;
    run_git(Some(&local), &["add", "README.md"]).await?;
    run_git(Some(&local), &["commit", "-m", "Initial commit"]).await?;
    run_git(
        Some(&local),
        &[
            "-c",
            "http.extraHeader=Authorization: Bearer local-test-token",
            "push",
            &first_url,
            "HEAD:refs/heads/main",
        ],
    )
    .await?;
    let original = run_git(Some(&local), &["rev-parse", "HEAD"]).await?;
    let _ = stop.send(());
    server.await??;
    drop(repository);
    drop(app_handle);
    first.shutdown().await?;
    directory
        .withdraw(&first_advertisement, unix_now_ms()?)
        .await?;
    drop(first_disk);

    let control = authority
        .load(target.cell_id())
        .await?
        .ok_or("repository control is missing")?;
    assert_eq!(control.value().state, ControlState::Idle);
    assert!(control.value().root.is_some());
    let second_session = SessionId::from_bytes([36; 16]);
    let second_disk = tempfile::TempDir::new()?;
    let (second, directory, second_advertisement) =
        node(Arc::clone(&application), &layout, second_session).await?;
    let proof = catalog
        .lookup(target.cell_id())
        .await?
        .ok_or("repository catalog entry is missing")?;
    let second_handle = second
        .runtime()
        .acquire_idle_restored(
            proof,
            replica(&layout, &target, *control.value().incarnation.as_bytes())?,
            authority,
            control,
            second_disk.path().join("repository.sqlite"),
            Owner {
                session: second_session,
                endpoint: "https://second.canopy.test".into(),
            },
        )
        .await?;
    let app_handle = second.application_handle::<CanopyApplication>(
        CellClient::local(registry, second_handle),
        tenant,
        application_id,
    );
    let repository = Arc::new(RepositoryCell::new(&app_handle, target)?);
    let second_gateway = Arc::new(GitGateway::new(
        repository,
        second_disk.path().to_path_buf(),
        object_store,
    ));
    let (address, stop, server) = serve(second_gateway).await?;
    let second_url = format!("http://{address}/canopy/example.git");
    let clone = second_disk.path().join("clone");
    run_git(
        None,
        &[
            "-c",
            "http.extraHeader=Authorization: Bearer local-test-token",
            "clone",
            &second_url,
            path_str(&clone)?,
        ],
    )
    .await?;
    assert_eq!(
        tokio::fs::read(clone.join("README.md")).await?,
        b"published Cell root\n"
    );
    assert_eq!(
        run_git(Some(&clone), &["rev-parse", "HEAD"]).await?,
        original
    );
    let _ = stop.send(());
    server.await??;
    second.shutdown().await?;
    directory
        .withdraw(&second_advertisement, unix_now_ms()?)
        .await?;
    Ok(())
}

async fn node(
    application: Arc<CompiledApplication>,
    layout: &CellStorageLayout,
    session: SessionId,
) -> Result<(CellNode, NodeDirectory, VersionedNodeAdvertisement), Box<dyn std::error::Error>> {
    let fleet = Digest::from_bytes([41; 32]);
    let image = Digest::from_bytes([42; 32]);
    let registry = application.registry();
    let directory = NodeDirectory::new(layout.clone(), fleet, image, registry.release_digest());
    let now_ms = unix_now_ms()?;
    let expires_at_ms = now_ms + 10_000;
    let advertisement = NodeAdvertisement::sign(
        NodeId::from_bytes(*session.as_bytes()),
        session,
        "https://canopy.test".into(),
        fleet,
        Digest::from_bytes([43; 32]),
        image,
        registry.release_digest(),
        &SigningKey::from_bytes(&[44; 32]),
        1,
        now_ms,
        expires_at_ms,
        registry.module_digests(),
        vec![1],
        NodeFailureDomain::default(),
        NodeCapacity::default(),
    )?;
    let observed = directory.create(advertisement, now_ms).await?;
    let node = CellNodeBuilder::new(application)
        .with_runtime(SqlWorkerPool::new(1, 4)?, 16 * 1024 * 1024)
        .with_replica_host(Host::default().with_local_disk_budget(DiskBudget::new(1 << 30)))
        .with_session(session)
        .build()?;
    node.install_task_group(CancellationToken::new(), CancellationToken::new())?;
    node.install_node_lease(NodeLeaseGuard::new(now_ms, expires_at_ms)?)?;
    Ok((node, directory, observed))
}

fn unix_now_ms() -> Result<i64, Box<dyn std::error::Error>> {
    Ok(i64::try_from(
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis(),
    )?)
}

fn replica(
    layout: &CellStorageLayout,
    target: &cellule_runtime::CellTarget,
    incarnation: [u8; 16],
) -> Result<CellReplica, Box<dyn std::error::Error>> {
    Ok(CellReplica::new(
        layout.clone(),
        *target.cell_id().as_bytes(),
        incarnation,
        Limits::default(),
    )?)
}

async fn serve(
    gateway: Arc<GitGateway>,
) -> Result<
    (
        std::net::SocketAddr,
        oneshot::Sender<()>,
        tokio::task::JoinHandle<Result<(), std::io::Error>>,
    ),
    Box<dyn std::error::Error>,
> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let api = Arc::new(GitHttpApi::new(
        gateway,
        "canopy".into(),
        "example",
        "local-test-token",
        &format!("http://{address}"),
        Arc::new(|| true),
    )?);
    let (stop, stopped) = oneshot::channel();
    let server = tokio::spawn(async move {
        axum::serve(listener, api.router())
            .with_graceful_shutdown(async move {
                let _ = stopped.await;
            })
            .await
    });
    Ok((address, stop, server))
}

fn path_str(path: &Path) -> Result<&str, &'static str> {
    path.to_str().ok_or("path is not UTF-8")
}

async fn run_git(cwd: Option<&Path>, args: &[&str]) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let mut command = Command::new("git");
    command.arg("-c").arg("credential.helper=");
    command.env("GIT_TERMINAL_PROMPT", "0");
    if let Some(cwd) = cwd {
        command.current_dir(cwd);
    }
    let output = command.args(args).output().await?;
    if !output.status.success() {
        return Err(format!(
            "git {} failed: {}",
            args.join(" "),
            String::from_utf8_lossy(&output.stderr)
        )
        .into());
    }
    Ok(output.stdout)
}
