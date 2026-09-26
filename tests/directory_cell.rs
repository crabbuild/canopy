use std::{
    path::Path,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use canopy_server::{
    CanopyApplication, ObjectKind, RepositoryCell, RepositoryModule, build_descriptor,
    directory::{self, DirectoryCell, DirectoryModule, RepositoryState},
    object_id, repository_target,
};
use cellule_app::{ApplicationHandle, CellApplication};
use cellule_ltx::{CellReplica, DiskBudget, Host, Limits};
use cellule_runtime::{
    ApplicationId, CatalogEntry, CatalogRole, CellAuthority, CellCatalog, CellClient, CellHandle,
    CellModule, CellRuntime, CellStorageLayout, IncarnationId, MutationIdentity, Owner, Registry,
    RequestId, SessionId, SqlWorkerPool, TenantId,
};
use cellule_store::Store;
use object_store::{memory::InMemory, path::Path as StorePath};

#[tokio::test(flavor = "multi_thread")]
async fn directory_reservations_recover_two_distinct_repository_cells()
-> Result<(), Box<dyn std::error::Error>> {
    let application = Arc::new(CanopyApplication::compile(build_descriptor(
        include_bytes!("../Cargo.lock"),
        "directory-test",
    ))?);
    let tenant = TenantId::from_bytes([71; 16]);
    let application_id = ApplicationId::from_bytes([72; 16]);
    let first_id = *uuid::Uuid::new_v4().as_bytes();
    let second_id = *uuid::Uuid::new_v4().as_bytes();
    let directory_target = directory::directory_target(tenant, application_id)?;
    let first_target = repository_target(tenant, application_id, first_id)?;
    let second_target = repository_target(tenant, application_id, second_id)?;
    assert_ne!(first_target.cell_id(), second_target.cell_id());
    let layout = CellStorageLayout::new(
        Store::new(Arc::new(InMemory::new())),
        StorePath::from("directory-test"),
        *application_id.as_bytes(),
    );
    let files = tempfile::TempDir::new()?;
    let first_session = SessionId::from_bytes([73; 16]);
    let first_runtime = runtime(first_session)?;
    let registry = application.registry();
    let directory_handle = bootstrap(
        &first_runtime,
        &registry,
        &layout,
        &directory_target,
        (DirectoryModule::NAME, directory::SCHEMA),
        first_session,
        &files.path().join("first-directory.sqlite"),
    )
    .await?;
    let directory = DirectoryCell::new(
        &app_handle(&application, tenant, application_id, directory_handle),
        directory_target.clone(),
    )?;
    let reserved = directory
        .reserve(identity(1)?, "alice", "alpha", first_id)
        .await?;
    assert_eq!(reserved.output.state, RepositoryState::Pending);
    let retried = directory
        .reserve(identity(2)?, "alice", "alpha", second_id)
        .await?;
    assert_eq!(retried.output.repository_id, first_id);
    let second = directory
        .reserve(identity(3)?, "alice", "beta", second_id)
        .await?
        .output;

    let first_repository = RepositoryCell::new(
        &app_handle(
            &application,
            tenant,
            application_id,
            bootstrap(
                &first_runtime,
                &registry,
                &layout,
                &first_target,
                (RepositoryModule::NAME, include_str!("../src/schema.sql")),
                first_session,
                &files.path().join("first-alpha.sqlite"),
            )
            .await?,
        ),
        first_target.clone(),
    )?;
    let second_repository = RepositoryCell::new(
        &app_handle(
            &application,
            tenant,
            application_id,
            bootstrap(
                &first_runtime,
                &registry,
                &layout,
                &second_target,
                (RepositoryModule::NAME, include_str!("../src/schema.sql")),
                first_session,
                &files.path().join("first-beta.sqlite"),
            )
            .await?,
        ),
        second_target.clone(),
    )?;
    let first = directory
        .activate(identity(4)?, &reserved.output)
        .await?
        .output;
    directory.activate(identity(5)?, &second).await?;
    assert_eq!(directory.list("alice", "").await?.output.len(), 2);
    assert_eq!(
        directory.lookup("alice", "alpha", None).await?.output,
        Some(first)
    );
    let body = b"stored only in alpha";
    let oid = first_repository
        .put_inline_object(identity(6)?, ObjectKind::Blob, body)
        .await?
        .output;
    assert_eq!(oid, object_id(ObjectKind::Blob, body));
    assert!(!second_repository.object_exists(oid).await?.output);
    first_runtime.shutdown().await?;

    let second_session = SessionId::from_bytes([74; 16]);
    let second_runtime = runtime(second_session)?;
    let restored_directory = restore(
        &second_runtime,
        &registry,
        &layout,
        &directory_target,
        DirectoryModule::NAME,
        second_session,
        &files.path().join("second-directory.sqlite"),
    )
    .await?;
    let directory = DirectoryCell::new(
        &app_handle(&application, tenant, application_id, restored_directory),
        directory_target,
    )?;
    let entries = directory.list("alice", "").await?.output;
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].repository_id, first_id);
    assert_eq!(entries[1].repository_id, second_id);
    assert!(
        entries
            .iter()
            .all(|entry| entry.state == RepositoryState::Ready)
    );
    let alpha = RepositoryCell::new(
        &app_handle(
            &application,
            tenant,
            application_id,
            restore(
                &second_runtime,
                &registry,
                &layout,
                &first_target,
                RepositoryModule::NAME,
                second_session,
                &files.path().join("second-alpha.sqlite"),
            )
            .await?,
        ),
        first_target,
    )?;
    let beta = RepositoryCell::new(
        &app_handle(
            &application,
            tenant,
            application_id,
            restore(
                &second_runtime,
                &registry,
                &layout,
                &second_target,
                RepositoryModule::NAME,
                second_session,
                &files.path().join("second-beta.sqlite"),
            )
            .await?,
        ),
        second_target,
    )?;
    assert_eq!(
        alpha.object(oid, None).await?.output,
        Some((ObjectKind::Blob, body.to_vec()))
    );
    assert!(!beta.object_exists(oid).await?.output);
    second_runtime.shutdown().await?;
    Ok(())
}

fn runtime(session: SessionId) -> cellule_runtime::Result<CellRuntime> {
    CellRuntime::new_with_replica_host(
        SqlWorkerPool::new(1, 4)?,
        64 * 1024 * 1024,
        session,
        Host::default().with_local_disk_budget(DiskBudget::new(1 << 30)),
    )
}

fn app_handle(
    application: &Arc<cellule_app::CompiledApplication>,
    tenant: TenantId,
    application_id: ApplicationId,
    handle: CellHandle,
) -> ApplicationHandle<CanopyApplication> {
    ApplicationHandle::new(
        CellClient::local(application.registry(), handle),
        Arc::clone(application),
        tenant,
        application_id,
    )
}

async fn bootstrap(
    runtime: &CellRuntime,
    registry: &Registry,
    layout: &CellStorageLayout,
    target: &cellule_runtime::CellTarget,
    module_schema: (&str, &'static str),
    session: SessionId,
    destination: &Path,
) -> Result<CellHandle, Box<dyn std::error::Error>> {
    let (module, schema) = module_schema;
    let code = registry.module_code(module).ok_or("module missing")?;
    let proof = CellCatalog::new(layout.clone(), target.tenant())
        .provision(CatalogEntry::new(target, CatalogRole::Sql, code, 1)?)
        .await?;
    let authority = CellAuthority::new(layout.clone());
    let observed = authority
        .create_initial(
            &proof,
            IncarnationId::from_bytes(*uuid::Uuid::new_v4().as_bytes()),
            Owner {
                session,
                endpoint: "https://canopy.test".into(),
            },
        )
        .await?;
    let replica = CellReplica::new(
        layout.clone(),
        *target.cell_id().as_bytes(),
        *observed.value().incarnation.as_bytes(),
        Limits::default(),
    )?;
    Ok(runtime
        .bootstrap(
            proof,
            replica,
            authority,
            observed,
            destination.to_path_buf(),
            |transaction| {
                transaction.execute_batch(schema)?;
                Ok(())
            },
        )
        .await?)
}

async fn restore(
    runtime: &CellRuntime,
    registry: &Registry,
    layout: &CellStorageLayout,
    target: &cellule_runtime::CellTarget,
    module: &str,
    session: SessionId,
    destination: &Path,
) -> Result<CellHandle, Box<dyn std::error::Error>> {
    let code = registry.module_code(module).ok_or("module missing")?;
    let proof = CellCatalog::new(layout.clone(), target.tenant())
        .provision(CatalogEntry::new(target, CatalogRole::Sql, code, 1)?)
        .await?;
    let authority = CellAuthority::new(layout.clone());
    let observed = authority
        .load(target.cell_id())
        .await?
        .ok_or("Cell authority missing")?;
    let replica = CellReplica::new(
        layout.clone(),
        *target.cell_id().as_bytes(),
        *observed.value().incarnation.as_bytes(),
        Limits::default(),
    )?;
    Ok(runtime
        .acquire_idle_restored(
            proof,
            replica,
            authority,
            observed,
            destination.to_path_buf(),
            Owner {
                session,
                endpoint: "https://canopy.test".into(),
            },
        )
        .await?)
}

fn identity(byte: u8) -> Result<MutationIdentity, Box<dyn std::error::Error>> {
    let now_ms = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis())?;
    Ok(MutationIdentity {
        request_id: RequestId::from_bytes([byte; 16]),
        issued_at_ms: now_ms,
        expires_at_ms: now_ms + 60_000,
    })
}
