//! Unsupported persisted identities fail before Cell acquisition or rewriting.
use super::*;
use cellule_runtime::{control::authority::CellAuthority, ltx::CellStorageLayout};
use cellule_store::Store;

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

async fn unsupported_control_stays_unchanged(code: Option<Digest>, schema: Option<u32>) -> Result {
    let files = tempfile::TempDir::new()?;
    let address = available_address().await?;
    let data = files.path().join("server");
    let configuration = config(address, data.clone());
    let target = canopy_server::directory::directory_target(
        configuration.tenant,
        configuration.application,
    )?;
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let layout = CellStorageLayout::new(
        Store::new(Arc::clone(&store)),
        configuration.store_prefix.clone(),
        *configuration.application.as_bytes(),
    );
    let server = CanopyServer::start(configuration, Arc::clone(&store)).await?;
    server.shutdown().await?;
    let authority = CellAuthority::new(layout.clone());
    let observed = authority
        .load(target.cell_id())
        .await?
        .ok_or("control missing")?;
    let mut unsupported = observed.value().clone();
    if let Some(code) = code {
        unsupported.code = code;
    }
    if let Some(schema) = schema {
        unsupported.schema = schema;
    }
    unsupported.revision += 1;
    unsupported.progress += 1;
    let path = layout.control_path(target.cell_id().as_bytes());
    let (_, token) = layout.store().get_with_etag(&path).await?;
    let encoded = unsupported.encode()?;
    layout
        .store()
        .update(&path, encoded.clone().into(), token)
        .await?;
    assert!(
        CanopyServer::start(config(address, data), Arc::clone(&store))
            .await
            .is_err()
    );
    assert_eq!(
        authority
            .load(target.cell_id())
            .await?
            .ok_or("control disappeared")?
            .value()
            .encode()?,
        encoded
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn unknown_control_code_is_rejected_before_cell_acquisition() -> Result {
    unsupported_control_stays_unchanged(Some(Digest::from_bytes([1; 32])), None).await
}

#[tokio::test(flavor = "multi_thread")]
async fn unknown_control_schema_is_rejected_before_cell_acquisition() -> Result {
    unsupported_control_stays_unchanged(None, Some(2)).await
}
