use super::*;
use crate::native_resources::{NativeClass, NativeLimits, NativeWork};
use object_store::memory::InMemory;

#[tokio::test(flavor = "multi_thread")]
async fn failed_startup_waits_for_supervisor_completion_and_owned_listener_release()
-> Result<(), Box<dyn std::error::Error>> {
    let listener = listeners::ReservedListener::new(TcpListener::bind("127.0.0.1:0").await?)?;
    let address = listener.local_addr()?;
    let (ready, receive_ready) = oneshot::channel();
    let (entered, receive_entered) = oneshot::channel();
    let (release, receive_release) = oneshot::channel();
    let (drained, receive_drained) = oneshot::channel();
    let finished = tokio::spawn(async move {
        // The actual supervisor can publish its error before its task-owned
        // resources finish dropping. Hold a real reserved listener here.
        let _ = ready.send(Err(ServerError::Repository("injected startup failure")));
        let _ = entered.send(());
        let _ = receive_release.await;
        drop(listener);
        let _ = drained.send(());
        Ok(())
    });
    let mut startup = tokio::spawn(receive_startup(receive_ready, finished));
    tokio::time::timeout(Duration::from_secs(5), receive_entered).await??;
    let early = tokio::time::timeout(Duration::from_millis(50), &mut startup)
        .await
        .ok();
    let returned_before_drain = early.is_some();
    assert!(
        TcpListener::bind(address)
            .await
            .is_err_and(|e| e.kind() == std::io::ErrorKind::AddrInUse)
    );
    let _ = release.send(());
    tokio::time::timeout(Duration::from_secs(5), receive_drained).await??;
    let result = match early {
        Some(result) => result?,
        None => tokio::time::timeout(Duration::from_secs(5), startup).await??,
    };
    assert!(matches!(
        result,
        Err(ServerError::Repository("injected startup failure"))
    ));
    assert!(
        !returned_before_drain,
        "startup error returned while supervisor still owned the listener"
    );
    let rebound = TcpListener::bind(address).await?;
    assert_eq!(rebound.local_addr()?, address);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn native_drain_retains_cell_workspace_and_lease_past_one_lease()
-> Result<(), Box<dyn std::error::Error>> {
    let files = tempfile::TempDir::new()?;
    let data = files.path().join("node");
    let server = RunningServer::start(
        ServerConfig {
            tenant: TenantId::from_bytes([51; 16]),
            application: ApplicationId::from_bytes([52; 16]),
            node: NodeId::from_bytes([54; 16]),
            fleet: Digest::from_bytes([55; 32]),
            image: Digest::from_bytes([56; 32]),
            signing_key: SigningKey::from_bytes(&[57; 32]),
            owner: "canopy".into(),
            token: "local-test-token".into(),
            public_url: "http://127.0.0.1".into(),
            peer_endpoint: "https://canopy.test".into(),
            peer_ca_pem: None,
            listen: "127.0.0.1:0".parse()?,
            ssh: None,
            data_dir: data.clone(),
            store_prefix: StorePath::from("native-shutdown-test"),
            native_limits: NativeLimits::default(),
            local_disk_limit_bytes: 1 << 30,
            max_active_repositories: 3,
        },
        Arc::new(InMemory::new()),
        None,
    )
    .await?;
    let native = server.native.clone();
    let foreground = native
        .scope(NativeClass::Foreground)
        .try_admit(NativeWork::Read)?;
    let maintenance = native
        .scope(NativeClass::Maintenance)
        .try_admit(NativeWork::Pack)?;
    let node = Arc::clone(&server.node);
    let directory = server.directory.clone();
    let session = server.advertisement.lock().await.advertisement().session();
    let mut shutdown = tokio::spawn(server.shutdown());
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match native
                .scope(NativeClass::Foreground)
                .try_admit(NativeWork::Read)
            {
                Err(error) if crate::native_resources::is_exhausted(&error) => break,
                Err(error) => return Err(error),
                Ok(claim) => drop(claim),
            }
            tokio::task::yield_now().await;
        }
        Ok::<_, std::io::Error>(())
    })
    .await??;
    // A finite shutdown timeout would hide premature authority withdrawal.
    // The native owner must retain renewal past the original signed lease.
    tokio::time::sleep(Duration::from_millis(u64::try_from(LEASE_MS)? + 2000)).await;
    assert!(!shutdown.is_finished());
    assert!(
        !node.is_shutting_down(),
        "Cell shutdown preceded native drain"
    );
    assert!(directory.is_live(session, unix_now_ms()?).await?);
    assert!(
        workspace::Workspace::open(&data)
            .is_err_and(|error| error.kind() == std::io::ErrorKind::WouldBlock)
    );
    drop(foreground);
    // Canceling the wait must not stop the supervisor or its maintenance owner.
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut shutdown)
            .await
            .is_err()
    );
    assert!(!node.is_shutting_down());
    assert!(directory.is_live(session, unix_now_ms()?).await?);
    drop(maintenance);
    tokio::time::timeout(Duration::from_secs(10), shutdown).await???;
    assert!(node.is_shutting_down());
    assert!(!directory.is_live(session, unix_now_ms()?).await?);
    let _restored = workspace::Workspace::open(&data)?;
    Ok(())
}
