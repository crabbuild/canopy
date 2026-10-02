use super::*;
use canopy_server::server::ServerError;

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

#[tokio::test(flavor = "multi_thread")]
async fn reserved_listener_survives_startup_and_serves_advertised_clone_url() -> Result {
    let files = tempfile::TempDir::new()?;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    assert_eq!(
        TcpListener::bind(address).await.unwrap_err().kind(),
        std::io::ErrorKind::AddrInUse
    );
    // A port chosen for an advertised clone URL must stay reserved until the
    // serving task owns it, rather than being released and rebound at startup.
    let server = CanopyServer::start_with_listener(
        config(address, files.path().join("node")),
        Arc::new(InMemory::new()),
        listener,
    )
    .await?;
    assert_eq!(server.local_addr(), address);
    let url = create_repository(address, "listener-handoff").await?;
    assert_eq!(url, format!("http://{address}/canopy/listener-handoff.git"));
    run_git(
        None,
        &[
            "-c",
            "http.extraHeader=Authorization: Bearer local-test-token",
            "ls-remote",
            &url,
        ],
    )
    .await?;
    server.shutdown().await?;
    let rebound = TcpListener::bind(address).await?;
    assert_eq!(rebound.local_addr()?, address);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn mismatched_listener_is_rejected_before_workspace_and_storage_writes() -> Result {
    let files = tempfile::TempDir::new()?;
    let store = Arc::new(InMemory::new());
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let other = TcpListener::bind("127.0.0.1:0").await?;
    let configured = other.local_addr()?;
    assert_ne!(address, configured);
    let data = files.path().join("node");
    let result = CanopyServer::start_with_listener(
        config(configured, data.clone()),
        store.clone(),
        listener,
    )
    .await;
    assert!(matches!(
        result,
        Err(ServerError::Http(
            "HTTP listener address differs from listen configuration"
        ))
    ));
    assert!(!data.exists(), "mismatch created a workspace");
    let remaining = store.list_with_delimiter(None).await?;
    assert!(remaining.objects.is_empty() && remaining.common_prefixes.is_empty());
    let rebound = TcpListener::bind(address).await?;
    assert_eq!(rebound.local_addr()?, address);
    assert_eq!(other.local_addr()?, configured);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn unpolled_startup_releases_reserved_listener() -> Result {
    let files = tempfile::TempDir::new()?;
    let store = Arc::new(InMemory::new());
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let data = files.path().join("node");
    let startup =
        CanopyServer::start_with_listener(config(address, data.clone()), store.clone(), listener);
    drop(startup);
    assert!(!data.exists());
    let remaining = store.list_with_delimiter(None).await?;
    assert!(remaining.objects.is_empty() && remaining.common_prefixes.is_empty());
    let rebound = TcpListener::bind(address).await?;
    assert_eq!(rebound.local_addr()?, address);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn released_port_can_be_taken_before_server_startup() -> Result {
    let files = tempfile::TempDir::new()?;
    let address = available_address().await?;
    // Deterministically insert the other binder into the test helper's gap.
    let competing = TcpListener::bind(address).await?;
    let result = CanopyServer::start(
        config(address, files.path().join("node")),
        Arc::new(InMemory::new()),
    )
    .await;
    assert!(matches!(
        result,
        Err(ServerError::Io(error)) if error.kind() == std::io::ErrorKind::AddrInUse
    ));
    assert_eq!(competing.local_addr()?, address);
    Ok(())
}
