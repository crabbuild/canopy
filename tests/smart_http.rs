use std::{path::Path, sync::Arc};

use canopy_server::{
    CanopyApplication, ObjectStorage, RepositoryCell, RepositoryModule, build_descriptor,
    git_gateway::GitGateway, http::GitHttpApi, lfs::LfsError, repository_target,
};
use cellule_app::{ApplicationHandle, CellApplication};
use cellule_ltx::{CellReplica, DiskBudget, Host, Limits};
use cellule_runtime::{
    ApplicationId, CatalogEntry, CatalogRole, CellAuthority, CellCatalog, CellClient, CellModule,
    CellRuntime, CellStorageLayout, Error, IncarnationId, Owner, SessionId, SqlWorkerPool,
    TenantId,
};
use cellule_store::Store;
use object_store::{ObjectStore, memory::InMemory, path::Path as StorePath};
use sha2::{Digest as _, Sha256};
use tokio::{net::TcpListener, process::Command, sync::oneshot};

#[tokio::test(flavor = "multi_thread")]
async fn stock_git_push_and_clone_are_backed_by_one_repository_cell()
-> Result<(), Box<dyn std::error::Error>> {
    let application = Arc::new(CanopyApplication::compile(build_descriptor(
        include_bytes!("../Cargo.lock"),
        "smart-http-test",
    ))?);
    let tenant = TenantId::from_bytes([21; 16]);
    let application_id = ApplicationId::from_bytes([22; 16]);
    let mut repository_id = [23; 16];
    repository_id[6] = 0x73;
    repository_id[8] = 0x83;
    let target = repository_target(tenant, application_id, repository_id)?;
    let layout = CellStorageLayout::new(
        Store::new(Arc::new(InMemory::new())),
        StorePath::from("smart-http-test"),
        *application_id.as_bytes(),
    );
    let registry = application.registry();
    let code = registry
        .module_code(RepositoryModule::NAME)
        .ok_or(Error::Registry("repository module missing"))?;
    let proof = CellCatalog::new(layout.clone(), tenant)
        .provision(CatalogEntry::new(&target, CatalogRole::Sql, code, 1)?)
        .await?;
    let authority = CellAuthority::new(layout.clone());
    let incarnation = IncarnationId::from_bytes([24; 16]);
    let session = SessionId::from_bytes([25; 16]);
    let observed = authority
        .create_initial(
            &proof,
            incarnation,
            Owner {
                session,
                endpoint: "https://canopy.test".into(),
            },
        )
        .await?;
    let scratch = tempfile::TempDir::new()?;
    let runtime = CellRuntime::new_with_replica_host(
        SqlWorkerPool::new(1, 4)?,
        16 * 1024 * 1024,
        session,
        Host::default().with_local_disk_budget(DiskBudget::new(1 << 30)),
    )?;
    let outcome: Result<(), Box<dyn std::error::Error>> = async {
        let handle = runtime
            .bootstrap(
                proof,
                CellReplica::new(
                    layout,
                    *target.cell_id().as_bytes(),
                    *incarnation.as_bytes(),
                    Limits::default(),
                )?,
                authority,
                observed,
                scratch.path().join("repository.sqlite"),
                |transaction| {
                    transaction.execute_batch(include_str!("../src/schema.sql"))?;
                    Ok(())
                },
            )
            .await?;
        let application_handle = ApplicationHandle::<CanopyApplication>::new(
            CellClient::local(registry, handle),
            application,
            tenant,
            application_id,
        );
        let repository = Arc::new(RepositoryCell::new(&application_handle, target)?);
        let blob_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let gateway = Arc::new(GitGateway::new(
            Arc::clone(&repository),
            scratch.path().to_path_buf(),
            Arc::clone(&blob_store),
        ));
        let invalid_oid = [0; 32];
        assert!(matches!(
            gateway.lfs().put(invalid_oid, b"wrong digest").await,
            Err(LfsError::Corrupt)
        ));
        assert!(repository.lfs_object(invalid_oid).await?.output.is_none());
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let api = Arc::new(GitHttpApi::new(
            gateway,
            "canopy".into(),
            "local-test-token",
            &format!("http://{address}"),
        )?);
        let (stop_tx, stop_rx) = oneshot::channel::<()>();
        let server = tokio::spawn(async move {
            axum::serve(listener, api.router())
                .with_graceful_shutdown(async move {
                    let _ = stop_rx.await;
                })
                .await
        });
        let url = format!("http://{address}/repo.git");
        let unauthenticated = Command::new("git")
            .args(["-c", "credential.helper=", "ls-remote", &url])
            .env("GIT_TERMINAL_PROMPT", "0")
            .output()
            .await?;
        assert!(!unauthenticated.status.success());

        let local = scratch.path().join("local");
        run_git(
            None,
            &["init", "-b", "main", local.to_str().ok_or("invalid path")?],
        )
        .await?;
        run_git(Some(&local), &["config", "user.name", "Canopy Test"]).await?;
        run_git(
            Some(&local),
            &["config", "user.email", "canopy@example.invalid"],
        )
        .await?;
        run_git(Some(&local), &["lfs", "install", "--local"]).await?;
        run_git(Some(&local), &["lfs", "track", "*.lfs"]).await?;
        tokio::fs::write(
            local.join("README.md"),
            b"served from the repository Cell\n",
        )
        .await?;
        let large_body = vec![0x5a; 1_200_000];
        tokio::fs::write(local.join("large.bin"), &large_body).await?;
        let lfs_body = vec![0xa5; 1_500_000];
        tokio::fs::write(local.join("tracked.lfs"), &lfs_body).await?;
        run_git(
            Some(&local),
            &[
                "add",
                ".gitattributes",
                "README.md",
                "large.bin",
                "tracked.lfs",
            ],
        )
        .await?;
        run_git(Some(&local), &["commit", "-m", "Initial commit"]).await?;
        let original = run_git(Some(&local), &["rev-parse", "HEAD"]).await?;
        let original = std::str::from_utf8(&original)?.trim();
        run_git(
            Some(&local),
            &[
                "-c",
                "http.extraHeader=Authorization: Bearer local-test-token",
                "push",
                &url,
                "HEAD:refs/heads/main",
            ],
        )
        .await?;
        let expected_oid: [u8; 20] = hex::decode(original)?
            .try_into()
            .map_err(|_| "invalid commit ID")?;
        assert_eq!(
            repository
                .ref_state("refs/heads/main", None)
                .await?
                .output
                .map(|state| state.oid),
            Some(expected_oid)
        );
        let mut cursor = None;
        let mut external_seen = false;
        while let Some(object) = repository.next_object(cursor).await?.output {
            cursor = Some(object.oid);
            external_seen |= matches!(object.storage, ObjectStorage::External { .. });
        }
        assert!(external_seen);
        let lfs_oid: [u8; 32] = Sha256::digest(&lfs_body).into();
        assert_eq!(
            repository
                .lfs_object(lfs_oid)
                .await?
                .output
                .map(|object| object.size),
            Some(lfs_body.len() as u64)
        );
        let _ = stop_tx.send(());
        server.await??;

        let gateway = Arc::new(GitGateway::new(
            Arc::clone(&repository),
            scratch.path().to_path_buf(),
            blob_store,
        ));
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let api = Arc::new(GitHttpApi::new(
            gateway,
            "canopy".into(),
            "local-test-token",
            &format!("http://{address}"),
        )?);
        let (stop_tx, stop_rx) = oneshot::channel::<()>();
        let server = tokio::spawn(async move {
            axum::serve(listener, api.router())
                .with_graceful_shutdown(async move {
                    let _ = stop_rx.await;
                })
                .await
        });
        let url = format!("http://{address}/repo.git");
        let clone = scratch.path().join("clone");
        run_git(
            None,
            &[
                "-c",
                "http.extraHeader=Authorization: Bearer local-test-token",
                "clone",
                &url,
                clone.to_str().ok_or("invalid path")?,
            ],
        )
        .await?;
        assert_eq!(
            tokio::fs::read(clone.join("README.md")).await?,
            b"served from the repository Cell\n"
        );
        assert_eq!(tokio::fs::read(clone.join("large.bin")).await?, large_body);
        run_git(Some(&clone), &["lfs", "install", "--local"]).await?;
        run_git(
            Some(&clone),
            &[
                "-c",
                "http.extraHeader=Authorization: Bearer local-test-token",
                "lfs",
                "pull",
            ],
        )
        .await?;
        assert!(tokio::fs::read(clone.join("tracked.lfs")).await? == lfs_body);
        let _ = stop_tx.send(());
        server.await??;
        Ok(())
    }
    .await;
    let shutdown = runtime.shutdown().await;
    outcome?;
    shutdown?;
    Ok(())
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
