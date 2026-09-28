use super::*;

#[tokio::test(flavor = "multi_thread")]
async fn node_lease_remains_live_through_a_drain_longer_than_one_lease() -> Result {
    use canopy_server::{CanopyApplication, build_descriptor};
    use cellule_app::CellApplication;
    use cellule_runtime::{ltx::CellStorageLayout, node::NodeDirectory};

    let files = tempfile::TempDir::new()?;
    let store = Arc::new(PausedStore::default());
    let address = available_address().await?;
    let data = files.path().join("node");
    let settings = config(address, data.clone());
    let application = CanopyApplication::compile(build_descriptor(
        include_bytes!("../../../Cargo.lock"),
        env!("CARGO_PKG_VERSION"),
    ))?;
    let directory = NodeDirectory::new(
        CellStorageLayout::new(
            cellule_store::Store::new(store.clone()),
            settings.store_prefix.clone(),
            *settings.application.as_bytes(),
        ),
        settings.fleet,
        settings.image,
        application.registry().release_digest(),
    );
    let now = || -> Result<i64> {
        Ok(i64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_millis(),
        )?)
    };
    let server = CanopyServer::start(settings, store.clone()).await?;
    create_repository(address, "held").await?;
    let advertised = directory.live(now()?, 2).await?;
    let session = advertised
        .first()
        .ok_or("node advertisement missing")?
        .session();
    store.arm(ControlState::Idle);
    let shutdown = tokio::spawn(server.shutdown());
    store.wait().await?;
    // Runtime drain must retain heartbeat ownership beyond the node lease.
    tokio::time::sleep(Duration::from_secs(32)).await;
    let live = directory.is_live(session, now()?).await;
    store.proceed.notify_one();
    let drained = timeout(Duration::from_secs(10), shutdown).await??;
    assert!(live?, "node lease expired before Cell release completed");
    drained?;
    assert!(!directory.is_live(session, now()?).await?);
    wait_for_cleanup(&data).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn slow_deployment_read_does_not_block_node_lease_renewal() -> Result {
    use canopy_server::{CanopyApplication, build_descriptor};
    use cellule_app::CellApplication;
    use cellule_runtime::{ltx::CellStorageLayout, node::NodeDirectory};

    let files = tempfile::TempDir::new()?;
    let store = Arc::new(PausedStore::default());
    let address = available_address().await?;
    let settings = config(address, files.path().join("node"));
    let application = CanopyApplication::compile(build_descriptor(
        include_bytes!("../../../Cargo.lock"),
        env!("CARGO_PKG_VERSION"),
    ))?;
    let layout = CellStorageLayout::new(
        cellule_store::Store::new(store.clone()),
        settings.store_prefix.clone(),
        *settings.application.as_bytes(),
    );
    let directory = NodeDirectory::new(
        layout.clone(),
        settings.fleet,
        settings.image,
        application.registry().release_digest(),
    );
    let server = CanopyServer::start(settings, store.clone()).await?;
    let now = || -> Result<i64> {
        Ok(i64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_millis(),
        )?)
    };
    let session = directory
        .live(now()?, 2)
        .await?
        .first()
        .ok_or("node advertisement missing")?
        .session();
    *store.read.lock().unwrap() = Some((layout.release_path(), 1));
    store.wait().await?;
    tokio::time::sleep(Duration::from_secs(32)).await;
    let live = directory.is_live(session, now()?).await?;
    let readiness = reqwest::Client::new()
        .get(format!("http://{address}/readyz"))
        .send()
        .await?;
    store.proceed.notify_one();
    let drained = timeout(Duration::from_secs(10), server.shutdown()).await?;
    assert!(live, "deployment read stalled advertisement renewal");
    assert_eq!(readiness.status(), reqwest::StatusCode::OK);
    drained?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn startup_preflight_does_not_consume_the_node_lease() -> Result {
    let files = tempfile::TempDir::new()?;
    let store = Arc::new(PausedStore::default());
    let address = available_address().await?;
    let settings = config(address, files.path().join("node"));
    let layout = cellule_runtime::ltx::CellStorageLayout::new(
        cellule_store::Store::new(store.clone()),
        settings.store_prefix.clone(),
        *settings.application.as_bytes(),
    );
    *store.read.lock().unwrap() = Some((layout.release_path(), 1));
    let startup = tokio::spawn(CanopyServer::start(settings, store.clone()));
    store.wait().await?;
    // Deployment validation happens before this node owns any Cell. Its I/O
    // must not spend the node authority lease used by subsequent startup.
    tokio::time::sleep(Duration::from_secs(31)).await;
    let preflight_finished = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis(),
    )?;
    store.proceed.notify_one();
    let server = timeout(Duration::from_secs(10), startup).await???;
    let initial = store
        .initial_advertisement
        .lock()
        .unwrap()
        .clone()
        .ok_or("initial advertisement missing")?;
    let issued = initial["lease"]["issued_at_ms"]
        .as_str()
        .ok_or("advertisement issue time missing")?
        .parse::<i64>()?;
    create_repository(address, "after-slow-preflight").await?;
    server.shutdown().await?;
    assert!(
        issued >= preflight_finished,
        "preflight consumed the initial node lease"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn delayed_renewal_reply_does_not_extend_confirmed_authority() -> Result {
    let files = tempfile::TempDir::new()?;
    let store = Arc::new(PausedStore::default());
    let address = available_address().await?;
    let server =
        CanopyServer::start(config(address, files.path().join("node")), store.clone()).await?;
    store.delay_renewals.store(true, Ordering::SeqCst);
    store.wait().await?;
    let expires = store
        .renewal_expiry
        .lock()
        .unwrap()
        .ok_or("renewal expiry missing")?;
    // Delay the successful response while the initial node lease is live.
    // Its replacement still expires at the originally signed wall-clock time.
    tokio::time::sleep(Duration::from_secs(4)).await;
    store.proceed.notify_one();
    store.wait().await?;
    let now = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis(),
    )?;
    let remaining = u64::try_from(expires - now + 200)?;
    tokio::time::sleep(Duration::from_millis(remaining)).await;
    let readiness = reqwest::Client::new()
        .get(format!("http://{address}/readyz"))
        .timeout(Duration::from_secs(2))
        .send()
        .await;
    store.delay_renewals.store(false, Ordering::SeqCst);
    store.proceed.notify_one();
    // Fencing may report an error from shutdown; it must still settle before
    // the fixture releases its workspace. The HTTP observation is the verdict.
    let _ = timeout(Duration::from_secs(10), server.shutdown()).await?;
    assert!(
        match &readiness {
            Ok(response) => response.status() == reqwest::StatusCode::SERVICE_UNAVAILABLE,
            Err(error) => error.is_connect(),
        },
        "node served readiness beyond its confirmed advertisement expiry: {readiness:?}"
    );
    Ok(())
}
