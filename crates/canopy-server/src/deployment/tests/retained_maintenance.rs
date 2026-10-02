//! Owned fixtures exercise the real maintenance worker, not only registry admission.
//! They do not claim old-executable or retained RustFS corpus recovery.

use super::*;
use crate::directory::{self, DirectoryModule};
use cellule_ltx::{CellReplica, DiskBudget, Host};
use cellule_runtime::{
    CellRuntime, CellTarget, NamespaceId, NodeLeaseGuard, SessionId, cell::worker::SqlWorkerPool,
    control::Owner, identity::IncarnationId,
};

fn retained_code() -> std::result::Result<Digest, Box<dyn std::error::Error>> {
    let previous =
        include_bytes!("../../../tests/directory_cell/fixtures/c51-selected-release.json")
            .trim_ascii_end();
    assert_eq!(
        blake3::hash(previous).to_hex().as_str(),
        "e31bf1a951e2fa19d91e9f964b2ddeade1a81b05a20ad628362819a1487c16b1"
    );
    let descriptor: serde_json::Value = serde_json::from_slice(previous)?;
    let module = descriptor["modules"]
        .as_array()
        .ok_or("modules missing")?
        .iter()
        .find(|module| module["name"] == DirectoryModule::NAME)
        .ok_or("Directory missing")?;
    Ok(Digest::from_bytes(
        hex::decode(module["code"].as_str().ok_or("code missing")?)?
            .try_into()
            .map_err(|_| "invalid retained code")?,
    ))
}

fn runtime(session: SessionId) -> cellule_runtime::Result<CellRuntime> {
    CellRuntime::new_with_replica_host(
        SqlWorkerPool::new(1, 1)?,
        64 * 1024 * 1024,
        session,
        Host::default().with_local_disk_budget(DiskBudget::new(256 * 1024 * 1024)),
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn maintenance_recovers_unpublished_retained_directory() -> TestResult {
    let deployment = fixture()?;
    deployment.initialize().await?;
    advertise_owner(&deployment, true).await?;
    let files = tempfile::TempDir::new()?;
    let target = directory::directory_target(
        deployment.identity.tenant(),
        deployment.identity.application(),
    )?;
    let entry = CatalogEntry::new(&target, CatalogRole::Sql, retained_code()?, 1)?;
    let catalog = CellCatalog::new(deployment.layout.clone(), deployment.identity.tenant());
    let proof = catalog.provision(entry.clone()).await?;
    let authority = CellAuthority::new(deployment.layout.clone());
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
    let operation = RequestId::from_bytes(uuid::Uuid::new_v4().into_bytes());
    deployment.begin_maintenance(operation).await?;
    deployment
        .recover_maintenance(operation, recovery_config(files.path().join("recover")))
        .await?;
    let after = authority
        .load(target.cell_id())
        .await?
        .ok_or("Cell missing")?;
    assert_eq!(after.value().state, ControlState::Idle);
    assert!(after.value().owner.is_none());
    assert!(after.value().root.is_some());
    assert_eq!(after.value().code, entry.initial_code());
    assert_eq!(after.value().schema, 1);
    assert_eq!(
        catalog
            .lookup(target.cell_id())
            .await?
            .ok_or("catalog missing")?
            .entry(),
        &entry
    );
    deployment
        .end_maintenance(operation, crate::server::unix_now_ms()?)
        .await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn maintenance_recovers_published_retained_directory_without_changing_data() -> TestResult {
    let deployment = fixture()?;
    deployment.initialize().await?;
    let files = tempfile::TempDir::new()?;
    let target = directory::directory_target(
        deployment.identity.tenant(),
        deployment.identity.application(),
    )?;
    let code = retained_code()?;
    assert!(
        deployment
            .registry
            .supports_cell(directory::DIRECTORY, CatalogRole::Sql, code, 1)
    );
    assert!(
        !deployment
            .registry
            .is_current_cell(directory::DIRECTORY, CatalogRole::Sql, code, 1)
    );
    let entry = CatalogEntry::new(&target, CatalogRole::Sql, code, 1)?;
    let catalog = CellCatalog::new(deployment.layout.clone(), deployment.identity.tenant());
    let proof = catalog.provision(entry.clone()).await?;
    let authority = CellAuthority::new(deployment.layout.clone());
    let session = SessionId::from_bytes([6; 16]);
    let owner = Owner {
        session,
        endpoint: "https://fixture.example.invalid".into(),
    };
    let initial = authority
        .create_initial(
            &proof,
            IncarnationId::from_bytes(uuid::Uuid::new_v4().into_bytes()),
            owner,
        )
        .await?;
    let now = crate::server::unix_now_ms()?;
    let guard = NodeLeaseGuard::new(now, now + crate::server::LEASE_MS)?;
    let old_runtime = CellRuntime::new_with_replica_host_requiring_node_lease(
        SqlWorkerPool::new(1, 1)?,
        64 * 1024 * 1024,
        session,
        Host::default().with_local_disk_budget(DiskBudget::new(256 * 1024 * 1024)),
    )?;
    old_runtime.install_node_lease(guard.clone())?;
    let limits = crate::replica_limits(crate::REPOSITORY_DATABASE_LIMIT_BYTES, 16 * 1024 * 1024);
    old_runtime.bootstrap(
        proof.clone(),
        CellReplica::new(deployment.layout.clone(), *target.cell_id().as_bytes(), *initial.value().incarnation.as_bytes(), limits)?,
        authority.clone(), initial, files.path().join("old.sqlite"),
        |transaction| {
            transaction.execute_batch(directory::SCHEMA)?;
            transaction.execute_batch("CREATE TABLE maintenance_marker(value TEXT NOT NULL); INSERT INTO maintenance_marker VALUES ('retained bytes');")?;
            Ok(())
        },
    ).await?;
    let before = authority
        .load(target.cell_id())
        .await?
        .ok_or("published Cell missing")?;
    assert_eq!(before.value().state, ControlState::Serving);
    assert!(before.value().root.is_some());
    // Fence only this owned runtime, then close its SQL workers. No raw Control writes.
    guard.fence();
    assert!(old_runtime.shutdown().await.is_err());
    assert_eq!(
        authority
            .load(target.cell_id())
            .await?
            .ok_or("Cell lost")?
            .value(),
        before.value()
    );
    advertise_owner(&deployment, true).await?;
    let operation = RequestId::from_bytes(uuid::Uuid::new_v4().into_bytes());
    deployment.begin_maintenance(operation).await?;
    deployment
        .recover_maintenance(operation, recovery_config(files.path().join("recover")))
        .await?;
    let after = authority
        .load(target.cell_id())
        .await?
        .ok_or("recovered Cell missing")?;
    assert_eq!(after.value().state, ControlState::Idle);
    assert!(after.value().owner.is_none());
    assert_eq!(after.value().root, before.value().root);
    assert_eq!(after.value().code, code);
    assert_eq!(after.value().schema, 1);
    assert_eq!(
        catalog
            .lookup(target.cell_id())
            .await?
            .ok_or("catalog missing")?
            .entry(),
        &entry
    );
    assert!(
        deployment
            .status(crate::server::unix_now_ms()?)
            .await?
            .drained
    );
    deployment
        .end_maintenance(operation, crate::server::unix_now_ms()?)
        .await?;
    let reader = runtime(SessionId::from_bytes([31; 16]))?;
    let read = reader
        .acquire_idle_restored(
            proof,
            CellReplica::new(
                deployment.layout.clone(),
                *target.cell_id().as_bytes(),
                *after.value().incarnation.as_bytes(),
                limits,
            )?,
            authority,
            after,
            files.path().join("read.sqlite"),
            Owner {
                session: SessionId::from_bytes([31; 16]),
                endpoint: "https://reader.example.invalid".into(),
            },
        )
        .await?;
    let bytes = read
        .query(0, 64, |connection| {
            let value: String =
                connection
                    .query_row("SELECT value FROM maintenance_marker", [], |row| row.get(0))?;
            Ok(value.into_bytes())
        })
        .await;
    reader.shutdown().await?;
    assert_eq!(bytes?, b"retained bytes");
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn maintenance_rejects_unknown_catalog_contracts_without_initializing_control() -> TestResult
{
    let retained = retained_code()?;
    for (namespace, role, code, schema) in [
        (
            directory::DIRECTORY,
            CatalogRole::Sql,
            Digest::from_bytes([99; 32]),
            1,
        ),
        (directory::DIRECTORY, CatalogRole::Sql, retained, 2),
        (directory::DIRECTORY, CatalogRole::Kv, retained, 1),
        (
            NamespaceId::from_bytes([99; 16]),
            CatalogRole::Sql,
            retained,
            1,
        ),
    ] {
        let deployment = fixture()?;
        deployment.initialize().await?;
        let files = tempfile::TempDir::new()?;
        let target = CellTarget::new(
            deployment.identity.tenant(),
            deployment.identity.application(),
            namespace,
            &[0, 0, 0, 0],
        )?;
        let entry = CatalogEntry::new(&target, role, code, schema)?;
        let catalog = CellCatalog::new(deployment.layout.clone(), deployment.identity.tenant());
        catalog.provision(entry.clone()).await?;
        let authority = CellAuthority::new(deployment.layout.clone());
        let operation = RequestId::from_bytes(uuid::Uuid::new_v4().into_bytes());
        deployment.begin_maintenance(operation).await?;
        assert!(
            deployment
                .recover_maintenance(operation, recovery_config(files.path().join("reject")))
                .await
                .is_err()
        );
        assert!(authority.load(target.cell_id()).await?.is_none());
        assert_eq!(
            catalog
                .lookup(target.cell_id())
                .await?
                .ok_or("catalog missing")?
                .entry(),
            &entry
        );
        assert_eq!(
            deployment.record().await?.state(),
            ReleaseState::Maintenance
        );
        assert!(
            !deployment
                .status(crate::server::unix_now_ms()?)
                .await?
                .drained
        );
    }
    Ok(())
}
