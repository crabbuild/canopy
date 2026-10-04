use super::*;

#[tokio::test(flavor = "multi_thread")]
async fn old_deployment_format_is_rejected_before_workspace_or_identity_writes()
-> Result<(), Box<dyn std::error::Error>> {
    for bytes in [
        br#"{"kind":"service"}"#.as_slice(),
        br#"{"purpose":{"kind":"service"}}"#,
        br#"{"format":"future-format","purpose":{"kind":"service"}}"#,
        br#"{"format":"canopy-pack-v1","purpose":{"kind":"backup","source":"source","pin":"11111111-1111-4111-8111-111111111111","complete":true}}"#,
        br#"{"format":"canopy-pack-v1","purpose":{"kind":"restore","source":"source","pin":"11111111-1111-4111-8111-111111111111","complete":false}}"#,
    ] {
        let files = tempfile::TempDir::new()?;
        let data = files.path().join("node");
        let packed = data.join("canopy-pack-v1");
        std::fs::create_dir_all(&packed)?;
        std::fs::write(packed.join(".canopy-runtime"), b"canopy-pack-v1")?;
        std::fs::write(packed.join("important"), b"retain existing cache")?;
        let configuration = config(available_address().await?, data.clone());
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let storage = cellule_store::Store::new(Arc::clone(&store));
        let root = configuration
            .store_prefix
            .clone()
            .join("canopy-root-v1.json");
        storage
            .create_strict(&root, bytes::Bytes::copy_from_slice(bytes))
            .await?;
        let original = storage.get_with_etag(&root).await?;
        let identities = cellule_runtime::cell::application::ApplicationIdentityStore::new(
            storage.clone(),
            configuration.store_prefix.clone(),
        );
        if let Ok(server) = CanopyServer::start(configuration, store).await {
            server.shutdown().await?;
            return Err("unversioned deployment was admitted".into());
        }
        assert!(identities.load().await?.is_none());
        assert_eq!(storage.get_with_etag(&root).await?, original);
        assert_eq!(
            std::fs::read(packed.join("important"))?,
            b"retain existing cache"
        );
        assert!(!data.join(".canopy-owner.lock").exists());
        assert!(!data.join("runtime-v1").exists());
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn active_node_blocks_workspace_reuse_and_shutdown_allows_durable_restore()
-> Result<(), Box<dyn std::error::Error>> {
    let files = tempfile::TempDir::new()?;
    let data = files.path().join("node");
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let address = available_address().await?;
    let server = CanopyServer::start(config(address, data.clone()), Arc::clone(&store)).await?;
    create_repository(address, "persistent").await?;
    let client = reqwest::Client::new();
    let before: serde_json::Value = client
        .get(format!("http://{address}/api/repositories/persistent"))
        .bearer_auth("local-test-token")
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let occupied = CanopyServer::start(
        config(available_address().await?, data.clone()),
        Arc::clone(&store),
    )
    .await;
    assert!(
        matches!(occupied, Err(canopy_server::server::ServerError::Io(error)) if error.kind() == std::io::ErrorKind::WouldBlock)
    );
    server.shutdown().await?;
    let sentinel = data.join("canopy-pack-v1/abandoned");
    std::fs::write(&sentinel, b"reclaim before restoring")?;
    let address = available_address().await?;
    let server = CanopyServer::start(config(address, data), store).await?;
    assert!(!sentinel.exists());
    let after: serde_json::Value = client
        .get(format!("http://{address}/api/repositories/persistent"))
        .bearer_auth("local-test-token")
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(after["repository_id"], before["repository_id"]);
    server.shutdown().await?;
    Ok(())
}
