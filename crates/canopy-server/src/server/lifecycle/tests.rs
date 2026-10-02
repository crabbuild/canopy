use super::*;
use crate::repository_target;

// Exercise the real HTTP cache after the runtime interrupts a started read and
// releases its Cell. A passing low-level fence test alone cannot cover this seam.
#[tokio::test(flavor = "multi_thread")]
async fn metadata_after_started_query_deadline_reacquires_released_cell()
-> Result<(), Box<dyn std::error::Error>> {
    use super::super::{RunningServer, ServerConfig};
    use cellule_runtime::{
        ApplicationId, Digest, TenantId, control::ControlState, control::authority::CellAuthority,
        identity::NodeId, ltx::CellStorageLayout,
    };
    use cellule_store::Store;
    use ed25519_dalek::SigningKey;
    use object_store::{ObjectStore, memory::InMemory, path::Path as StorePath};
    use std::time::Duration;

    let workspace = tempfile::TempDir::new()?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let tenant = TenantId::from_bytes([61; 16]);
    let application = ApplicationId::from_bytes([62; 16]);
    let prefix = StorePath::from("query-deadline-residency-test");
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let layout = CellStorageLayout::new(
        Store::new(Arc::clone(&store)),
        prefix.clone(),
        *application.as_bytes(),
    );
    let server = RunningServer::start(
        ServerConfig {
            tenant,
            application,
            node: NodeId::from_bytes([64; 16]),
            fleet: Digest::from_bytes([65; 32]),
            image: Digest::from_bytes([66; 32]),
            signing_key: SigningKey::from_bytes(&[67; 32]),
            owner: "canopy".into(),
            token: "local-test-token".into(),
            public_url: format!("http://{address}"),
            peer_endpoint: "https://canopy.test".into(),
            peer_ca_pem: None,
            listen: address,
            ssh: None,
            data_dir: workspace.path().join("server"),
            store_prefix: prefix,
            local_disk_limit_bytes: 1 << 30,
            max_active_repositories: 3,
        },
        store,
        Some(listener),
    )
    .await?;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()?;
    let outcome = async {
        let mut repositories = Vec::new();
        for name in ["timed-out-read", "healthy"] {
            let value = client
                .post(format!("http://{address}/api/repositories"))
                .bearer_auth("local-test-token")
                .json(&serde_json::json!({"name": name}))
                .send()
                .await?
                .error_for_status()?
                .json::<serde_json::Value>()
                .await?;
            repositories.push(uuid::Uuid::parse_str(
                value["repository_id"].as_str().ok_or("repository id missing")?,
            )?);
        }
        // Populate Canopy's normal metadata fast path before fencing its handle.
        client
            .get(format!("http://{address}/api/repositories/timed-out-read"))
            .bearer_auth("local-test-token")
            .send()
            .await?
            .error_for_status()?;
        let target = repository_target(tenant, application, repositories[0].into_bytes())?;
        let handle = server
            .node
            .runtime()
            .resident_handle(&target, cellule_runtime::cell::catalog::CatalogRole::Sql)
            .await?
            .ok_or("repository has no resident handle")?;
        let authority = CellAuthority::new(layout);
        let before = authority.load(target.cell_id()).await?.ok_or("control missing")?;
        let interrupted = tokio::time::timeout(
            Duration::from_secs(7),
            handle.query(64, 64, |connection| {
                let value = connection.query_row(
                    "WITH RECURSIVE counter(value) AS (VALUES(0) UNION ALL SELECT value + 1 FROM counter WHERE value < 1000000000) SELECT sum(value) FROM counter",
                    [],
                    |row| row.get::<_, i64>(0),
                )?;
                Ok(value.to_be_bytes().to_vec())
            }),
        )
        .await?;
        assert!(matches!(interrupted, Err(Error::Deadline)), "{interrupted:?}");
        assert!(matches!(
            handle.query(1, 1, |_| Ok(Vec::new())).await,
            Err(Error::Fenced)
        ));
        let released = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let control = authority.load(target.cell_id()).await?.ok_or("control missing")?;
                if control.value().state == ControlState::Idle && control.value().owner.is_none() {
                    break Ok::<_, Box<dyn std::error::Error>>(control);
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await??;
        assert_eq!(released.value().root, before.value().root);
        let healthy = client
            .get(format!("http://{address}/api/repositories/healthy"))
            .bearer_auth("local-test-token")
            .send()
            .await?
            .error_for_status()?
            .json::<serde_json::Value>()
            .await?;
        assert_eq!(healthy["repository_id"], repositories[1].to_string());
        let response = client
            .get(format!("http://{address}/api/repositories/timed-out-read"))
            .bearer_auth("local-test-token")
            .send()
            .await?;
        let status = response.status();
        let body = response.text().await?;
        // Drain the owned node before asserting the signal, even on the red run.
        Ok::<_, Box<dyn std::error::Error>>((status, body, repositories[0]))
    }
    .await;
    let drained = server.shutdown().await;
    let (status, body, repository) = outcome?;
    drained?;
    assert_eq!(
        status,
        reqwest::StatusCode::OK,
        "released Cell metadata: {body}"
    );
    let value: serde_json::Value = serde_json::from_str(&body)?;
    assert_eq!(value["repository_id"], repository.to_string());
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn deadline_recovery_keeps_pinned_files_and_acknowledged_data()
-> Result<(), Box<dyn std::error::Error>> {
    use cellule_runtime::{control::authority::CellAuthority, identity::NodeId};
    use object_store::memory::InMemory;

    let workspace = tempfile::TempDir::new()?;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let tenant = TenantId::from_bytes([71; 16]);
    let application = ApplicationId::from_bytes([72; 16]);
    let prefix = StorePath::from("pinned-query-deadline-test");
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let layout = CellStorageLayout::new(
        Store::new(Arc::clone(&store)),
        prefix.clone(),
        *application.as_bytes(),
    );
    let server = RunningServer::start(
        ServerConfig {
            tenant,
            application,
            node: NodeId::from_bytes([74; 16]),
            fleet: Digest::from_bytes([75; 32]),
            image: Digest::from_bytes([76; 32]),
            signing_key: SigningKey::from_bytes(&[77; 32]),
            owner: "canopy".into(),
            token: "local-test-token".into(),
            public_url: format!("http://{address}"),
            peer_endpoint: "https://canopy.test".into(),
            peer_ca_pem: None,
            listen: address,
            ssh: None,
            data_dir: workspace.path().join("server"),
            store_prefix: prefix,
            local_disk_limit_bytes: 1 << 30,
            max_active_repositories: 1,
        },
        store,
        Some(listener),
    )
    .await?;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()?;
    let outcome = async {
        let created = client.post(format!("http://{address}/api/repositories"))
            .bearer_auth("local-test-token").json(&serde_json::json!({"name": "pinned"}))
            .send().await?.error_for_status()?.json::<serde_json::Value>().await?;
        let repository = uuid::Uuid::parse_str(created["repository_id"].as_str().ok_or("repository id missing")?)?;
        let issue_id = uuid::Uuid::new_v4();
        client.post(format!("http://{address}/api/repositories/pinned/issues"))
            .bearer_auth("local-test-token").json(&serde_json::json!({
                "repository_id": repository.to_string(), "id": issue_id.to_string(),
                "title": "ack before fencing", "body": "must survive read interruption",
            })).send().await?.error_for_status()?;
        let entry = server.test_manager.directory.lookup("canopy", "pinned", None)
            .await?.output.ok_or("directory entry missing")?;
        // Hold the same route pin an admitted handler owns, not a mock counter.
        let route = server.test_manager.load(ReadIdentity::Account("canopy"), entry).await?;
        let target = repository_target(tenant, application, repository.into_bytes())?;
        let handle = server.node.runtime().resident_handle(&target, CatalogRole::Sql)
            .await?.ok_or("resident handle missing")?;
        let authority = CellAuthority::new(layout);
        let before = authority.load(target.cell_id()).await?.ok_or("control missing")?;
        let interrupted = tokio::time::timeout(Duration::from_secs(7), handle.query(64, 64, |connection| {
            let value = connection.query_row(
                "WITH RECURSIVE counter(value) AS (VALUES(0) UNION ALL SELECT value + 1 FROM counter WHERE value < 1000000000) SELECT sum(value) FROM counter",
                [], |row| row.get::<_, i64>(0))?;
            Ok(value.to_be_bytes().to_vec())
        })).await?;
        assert!(matches!(interrupted, Err(Error::Deadline)), "{interrupted:?}");
        let released = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let current = authority.load(target.cell_id()).await?.ok_or("control missing")?;
                if current.value().state == ControlState::Idle && current.value().owner.is_none() {
                    break Ok::<_, Box<dyn std::error::Error>>(current);
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }).await??;
        assert_eq!(released.value().root, before.value().root);
        let sqlite = server.local.path().join(hex::encode(repository.as_bytes())).join("repository.sqlite");
        let pinned_bytes = tokio::fs::read(&sqlite).await?;
        assert_eq!(server.test_manager.residency_slots.available_permits(), 0);
        let refused = client.get(format!("http://{address}/api/repositories/pinned"))
            .bearer_auth("local-test-token").send().await?;
        assert_eq!(refused.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(tokio::fs::read(&sqlite).await?, pinned_bytes);
        assert_eq!(authority.load(target.cell_id()).await?.ok_or("control missing")?.value(), released.value());
        assert_eq!(server.test_manager.residency_slots.available_permits(), 0);
        drop(route);

        // Four admitted callers contend for one transition; no client retry.
        let mut readers = tokio::task::JoinSet::new();
        for _ in 0..4 {
            let client = client.clone();
            readers.spawn(async move {
                client.get(format!("http://{address}/api/repositories/pinned"))
                    .bearer_auth("local-test-token").send().await?.error_for_status()?
                    .json::<serde_json::Value>().await
            });
        }
        while let Some(reader) = readers.join_next().await {
            assert_eq!(reader??["repository_id"], repository.to_string());
        }
        let issue = client.get(format!("http://{address}/api/repositories/pinned/issues/1"))
            .bearer_auth("local-test-token").send().await?.error_for_status()?
            .json::<serde_json::Value>().await?;
        assert_eq!(issue["issue"]["id"], issue_id.to_string());
        assert_eq!(issue["issue"]["title"], "ack before fencing");
        assert_eq!(issue["issue"]["body"], "must survive read interruption");
        let restored = authority.load(target.cell_id()).await?.ok_or("control missing")?;
        assert_eq!(restored.value().root, before.value().root);
        assert_eq!(restored.value().epoch, released.value().epoch + 1);
        assert_eq!(server.test_manager.residency_slots.available_permits(), 0);
        Ok::<_, Box<dyn std::error::Error>>(())
    }.await;
    let drained = server.shutdown().await;
    outcome?;
    drained?;
    Ok(())
}
