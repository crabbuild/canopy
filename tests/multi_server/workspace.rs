use super::*;

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
    let sentinel = data.join("runtime-v1/abandoned");
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
