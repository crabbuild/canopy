#[path = "support/objects.rs"]
mod objects;

use std::{
    path::Path,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use canopy_server::{
    CanopyApplication, ObjectKind, RepositoryCell, RepositoryModule, build_descriptor,
    directory::{
        self, CreateAccountOutcome, DirectoryCell, DirectoryModule, RenameOutcome, RepositoryState,
        TokenScope,
    },
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
use sha2::{Digest as _, Sha256};

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
    let admin_digest: [u8; 32] = Sha256::digest(b"admin-test-token").into();
    let reader_digest: [u8; 32] = Sha256::digest(b"reader-test-token").into();
    assert!(matches!(
        directory
            .create_account(identity(11)?, "alice", admin_digest, TokenScope::Admin)
            .await?
            .output,
        CreateAccountOutcome::Created(_)
    ));
    assert!(matches!(
        directory
            .create_account(identity(12)?, "bob", reader_digest, TokenScope::Read)
            .await?
            .output,
        CreateAccountOutcome::Created(_)
    ));
    assert_eq!(
        directory
            .create_account(identity(13)?, "bob", admin_digest, TokenScope::Admin)
            .await?
            .output,
        CreateAccountOutcome::NameTaken
    );
    assert_eq!(
        directory
            .authenticate(reader_digest, None)
            .await?
            .output
            .map(|principal| (principal.account, principal.scope)),
        Some(("bob".into(), TokenScope::Read))
    );
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
    assert!(
        directory
            .list_candidates("alice", None)
            .await?
            .output
            .is_empty()
    );
    assert!(
        !directory
            .remember_access(identity(14)?, "alice", "bob", first_id)
            .await?
            .output
    );

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
    for (actor, account) in [("bob", "bob"), ("alice", "alice"), ("alice", "missing")] {
        assert!(
            !directory
                .remember_access(random_identity()?, actor, account, first_id)
                .await?
                .output
        );
    }
    assert!(
        directory
            .list_candidates("bob", None)
            .await?
            .output
            .is_empty()
    );
    assert!(
        directory
            .lookup_candidate("bob", "alice", "alpha")
            .await?
            .output
            .is_none()
    );
    let remember = identity(15)?;
    for identity in [remember, remember, random_identity()?] {
        assert!(
            directory
                .remember_access(identity, "alice", "bob", first_id)
                .await?
                .output
        );
    }
    // Candidate publication alone grants no repository-local permission.
    assert_eq!(
        first_repository.access_level("bob", None).await?.output,
        None
    );
    let candidates = directory.list_candidates("bob", None).await?.output;
    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0].repository_id, first_id);
    assert_eq!(
        directory
            .lookup_candidate("bob", "alice", "alpha")
            .await?
            .output
            .map(|entry| entry.repository_id),
        Some(first_id)
    );
    assert_eq!(
        directory.list_candidates("alice", None).await?.output.len(),
        2
    );
    assert_eq!(
        directory.lookup("alice", "alpha", None).await?.output,
        Some(first)
    );
    assert_eq!(
        directory
            .rename(identity(7)?, "alice", "alpha", "beta", first_id)
            .await?
            .output,
        RenameOutcome::NameTaken
    );
    assert_eq!(
        directory
            .rename(identity(8)?, "alice", "alpha", "gamma", second_id)
            .await?
            .output,
        RenameOutcome::NotFound
    );
    let renamed = directory
        .rename(identity(9)?, "alice", "alpha", "gamma", first_id)
        .await?
        .output;
    assert!(
        matches!(renamed, RenameOutcome::Renamed(ref entry) if entry.repository_id == first_id && entry.name == "gamma")
    );
    assert_eq!(directory.lookup("alice", "alpha", None).await?.output, None);
    assert_eq!(
        directory
            .rename(identity(10)?, "alice", "alpha", "gamma", first_id)
            .await?
            .output,
        renamed
    );
    let body = b"stored only in alpha";
    let oid = objects::put(&first_repository, identity(6)?, ObjectKind::Blob, body)
        .await?
        .output;
    assert_eq!(oid, object_id(ObjectKind::Blob, body));
    assert!(
        second_repository
            .existing_objects(&[oid])
            .await?
            .output
            .is_empty()
    );
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
    assert_eq!(
        directory
            .authenticate(admin_digest, None)
            .await?
            .output
            .map(|principal| (principal.account, principal.scope)),
        Some(("alice".into(), TokenScope::Admin))
    );
    assert_eq!(directory.authenticate([0; 32], None).await?.output, None);
    let candidates = directory.list_candidates("bob", None).await?.output;
    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0].name, "gamma");
    let entries = directory.list_candidates("alice", None).await?.output;
    assert_eq!(entries.len(), 2);
    let mut expected = vec![(first_id, "gamma"), (second_id, "beta")];
    expected.sort_by_key(|(id, _)| *id);
    assert_eq!(
        entries
            .iter()
            .map(|entry| (entry.repository_id, entry.name.as_str()))
            .collect::<Vec<_>>(),
        expected
    );
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
    assert!(beta.existing_objects(&[oid]).await?.output.is_empty());
    assert_eq!(alpha.access_level("bob", None).await?.output, None);
    second_runtime.shutdown().await?;
    Ok(())
}

fn random_identity() -> Result<MutationIdentity, Box<dyn std::error::Error>> {
    let now = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis())?;
    Ok(MutationIdentity {
        request_id: RequestId::from_bytes(uuid::Uuid::new_v4().into_bytes()),
        issued_at_ms: now,
        expires_at_ms: now + 60_000,
    })
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
