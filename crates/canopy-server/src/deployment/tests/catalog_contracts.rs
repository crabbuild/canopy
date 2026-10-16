//! Maintenance refuses noncurrent catalog contracts before allocating Control.
use super::*;
use crate::directory;
use cellule_runtime::{CellTarget, NamespaceId};

fn deployment_code() -> std::result::Result<Digest, Box<dyn std::error::Error>> {
    fixture()?
        .registry
        .module_code(crate::directory::DirectoryModule::NAME)
        .ok_or_else(|| "directory code missing".into())
}

#[tokio::test(flavor = "multi_thread")]
async fn maintenance_rejects_unknown_catalog_contracts_without_initializing_control() -> TestResult
{
    let code = deployment_code()?;
    for (namespace, role, code, schema) in [
        (
            directory::DIRECTORY,
            CatalogRole::Sql,
            Digest::from_bytes([99; 32]),
            1,
        ),
        (directory::DIRECTORY, CatalogRole::Sql, code, 2),
        (directory::DIRECTORY, CatalogRole::Kv, code, 1),
        (NamespaceId::from_bytes([99; 16]), CatalogRole::Sql, code, 1),
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
