use super::*;
use canopy_server::repository_target;
use cellule_runtime::{
    control::ControlState, control::authority::CellAuthority, ltx::CellStorageLayout,
};
use cellule_store::Store;
use sha2::{Digest as _, Sha256};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    time::{Duration, timeout},
};

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

mod faults;

#[tokio::test(flavor = "multi_thread")]
async fn repositories_beyond_resident_capacity_restore_git_and_lfs_on_the_same_node() -> Result {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let workspace = tempfile::TempDir::new()?;
    let address = available_address().await?;
    let settings = config(address, workspace.path().join("server"));
    let tenant = settings.tenant;
    let application = settings.application;
    let layout = CellStorageLayout::new(
        Store::new(Arc::clone(&store)),
        settings.store_prefix.clone(),
        *application.as_bytes(),
    );
    let authority = CellAuthority::new(layout.clone());
    let server = CanopyServer::start(settings, store).await?;
    let local_root = workspace.path().join("server/canopy-pack-v1");
    let local = workspace.path().join("source");
    run_git(None, &["init", "-b", "main", path_str(&local)?]).await?;
    run_git(Some(&local), &["config", "user.name", "Canopy Test"]).await?;
    run_git(
        Some(&local),
        &["config", "user.email", "canopy@example.invalid"],
    )
    .await?;
    run_git(Some(&local), &["lfs", "install", "--local"]).await?;
    run_git(Some(&local), &["lfs", "track", "*.lfs"]).await?;
    let lfs_body = vec![0x6b; 1_300_000];
    tokio::fs::write(local.join("asset.lfs"), &lfs_body).await?;
    let client = reqwest::Client::new();
    let mut repositories = Vec::new();
    for index in 0..6 {
        let name = format!("repository-{index}");
        let (url, id) = create(&client, address, &name).await?;
        let body = format!("repository {index}\n");
        tokio::fs::write(local.join("README.md"), &body).await?;
        run_git(Some(&local), &["add", "."]).await?;
        run_git(Some(&local), &["commit", "-m", &name]).await?;
        run_git(
            Some(&local),
            &[
                "-c",
                "http.extraHeader=Authorization: Bearer local-test-token",
                "push",
                &url,
                "HEAD:refs/heads/main",
            ],
        )
        .await?;
        let oid = run_git(Some(&local), &["rev-parse", "HEAD"]).await?;
        repositories.push((url, id, body, oid));
        let mut serving = 0;
        for (_, id, _, _) in &repositories {
            let target = repository_target(tenant, application, *id)?;
            let control = authority
                .load(target.cell_id())
                .await?
                .ok_or("Cell authority missing")?;
            assert!(control.value().root.is_some());
            if control.value().state == ControlState::Serving {
                serving += 1;
            } else {
                assert_eq!(control.value().state, ControlState::Idle);
                assert!(control.value().owner.is_none());
                assert!(!local_root.join(hex::encode(id)).exists());
            }
        }
        assert_eq!(serving, repositories.len().min(3));
    }
    // Reading in creation order forces another eviction and exact-root restore
    // for each repository while the same node lease remains alive.
    for (index, (url, id, body, oid)) in repositories.iter().enumerate() {
        let target = repository_target(tenant, application, *id)?;
        let before = authority
            .load(target.cell_id())
            .await?
            .ok_or("Cell missing")?;
        let root = before.value().root.clone();
        let before_rows = durable_rows(
            &layout,
            &target,
            &workspace.path().join(format!("before-{index}.sqlite")),
        )
        .await?;
        let clone = workspace.path().join(format!("clone-{index}"));
        run_git(
            None,
            &[
                "-c",
                "http.extraHeader=Authorization: Bearer local-test-token",
                "clone",
                url,
                path_str(&clone)?,
            ],
        )
        .await?;
        assert_eq!(
            tokio::fs::read(clone.join("README.md")).await?,
            body.as_bytes()
        );
        run_git(Some(&clone), &["lfs", "install", "--local"]).await?;
        run_git(
            Some(&clone),
            &[
                "-c",
                "http.extraHeader=Authorization: Bearer local-test-token",
                "lfs",
                "pull",
            ],
        )
        .await?;
        assert!(tokio::fs::read(clone.join("asset.lfs")).await? == lfs_body);
        assert_eq!(&run_git(Some(&clone), &["rev-parse", "HEAD"]).await?, oid);
        run_git(Some(&clone), &["fsck", "--full"]).await?;
        let after = authority
            .load(target.cell_id())
            .await?
            .ok_or("Cell missing")?;
        let after_rows = durable_rows(
            &layout,
            &target,
            &workspace.path().join(format!("after-{index}.sqlite")),
        )
        .await?;
        assert_read_only_restore(&before_rows, &after_rows);
        assert!(
            after
                .value()
                .root
                .as_ref()
                .ok_or("restored root absent")?
                .commit_sequence
                >= root.as_ref().ok_or("original root absent")?.commit_sequence
        );
    }
    server.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn active_uploads_block_eviction_until_a_request_disconnects() -> Result {
    let workspace = tempfile::TempDir::new()?;
    let address = available_address().await?;
    let server = CanopyServer::start(
        config(address, workspace.path().join("server")),
        Arc::new(InMemory::new()),
    )
    .await?;
    let client = reqwest::Client::new();
    let body = b"pinned LFS upload";
    let oid = hex::encode(Sha256::digest(body));
    let mut uploads = Vec::new();
    for index in 0..3 {
        let name = format!("pinned-{index}");
        create(&client, address, &name).await?;
        let mut stream = TcpStream::connect(address).await?;
        let headers = format!(
            "PUT /canopy/{name}.git/info/lfs/objects/{oid} HTTP/1.1\r\nHost: {address}\r\nAuthorization: Bearer local-test-token\r\nContent-Length: {}\r\nExpect: 100-continue\r\nConnection: close\r\n\r\n",
            body.len()
        );
        stream.write_all(headers.as_bytes()).await?;
        let mut response = vec![0; b"HTTP/1.1 100 Continue\r\n\r\n".len()];
        timeout(Duration::from_secs(5), stream.read_exact(&mut response)).await??;
        assert_eq!(response, b"HTTP/1.1 100 Continue\r\n\r\n");
        uploads.push(stream);
    }
    let creation = || {
        client
            .post(format!("http://{address}/api/repositories"))
            .bearer_auth("local-test-token")
            .json(&serde_json::json!({"name":"fourth"}))
            .send()
    };
    assert_eq!(
        creation().await?.status(),
        reqwest::StatusCode::SERVICE_UNAVAILABLE
    );
    drop(uploads.remove(0));
    timeout(Duration::from_secs(5), async {
        loop {
            let response = creation().await?;
            if response.status().is_success() {
                break Ok::<_, reqwest::Error>(());
            }
            assert_eq!(response.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await??;
    for (index, mut stream) in uploads.into_iter().enumerate() {
        stream.write_all(body).await?;
        let mut response = Vec::new();
        timeout(Duration::from_secs(5), stream.read_to_end(&mut response)).await??;
        assert!(
            response.starts_with(b"HTTP/1.1 200 OK\r\n"),
            "{}",
            String::from_utf8_lossy(&response)
        );
        let fetched = client
            .get(format!(
                "http://{address}/canopy/pinned-{}.git/info/lfs/objects/{oid}",
                index + 1
            ))
            .bearer_auth("local-test-token")
            .send()
            .await?
            .error_for_status()?
            .bytes()
            .await?;
        assert_eq!(fetched.as_ref(), body);
    }
    server.shutdown().await?;
    Ok(())
}

async fn create(
    client: &reqwest::Client,
    address: std::net::SocketAddr,
    name: &str,
) -> Result<(String, [u8; 16])> {
    let response: serde_json::Value = client
        .post(format!("http://{address}/api/repositories"))
        .bearer_auth("local-test-token")
        .json(&serde_json::json!({"name":name}))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    Ok((
        response["clone_url"]
            .as_str()
            .ok_or("clone URL missing")?
            .into(),
        uuid::Uuid::parse_str(
            response["repository_id"]
                .as_str()
                .ok_or("repository UUID missing")?,
        )?
        .into_bytes(),
    ))
}

#[tokio::test(flavor = "multi_thread")]
async fn configured_residency_admits_a_larger_working_set_and_stays_bounded() -> Result {
    let workspace = tempfile::TempDir::new()?;
    let address = available_address().await?;
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut settings = config(address, workspace.path().join("server"));
    settings.max_active_repositories = 100;
    let tenant = settings.tenant;
    let application = settings.application;
    let authority = CellAuthority::new(CellStorageLayout::new(
        Store::new(Arc::clone(&store)),
        settings.store_prefix.clone(),
        *application.as_bytes(),
    ));
    let server = CanopyServer::start(settings, store).await?;
    let client = reqwest::Client::new();
    let mut targets = Vec::new();
    for index in 0..100 {
        let (_, id) = create(&client, address, &format!("working-set-{index}")).await?;
        targets.push(repository_target(tenant, application, id)?);
    }
    for target in &targets {
        let control = authority
            .load(target.cell_id())
            .await?
            .ok_or("Cell missing")?;
        assert_eq!(control.value().state, ControlState::Serving);
    }
    let (_, id) = create(&client, address, "overflow").await?;
    targets.push(repository_target(tenant, application, id)?);
    let mut serving = 0;
    for target in &targets {
        let control = authority
            .load(target.cell_id())
            .await?
            .ok_or("Cell missing")?;
        serving += usize::from(control.value().state == ControlState::Serving);
    }
    assert_eq!(serving, 100);
    client
        .get(format!("http://{address}/api/repositories/working-set-0"))
        .bearer_auth("local-test-token")
        .send()
        .await?
        .error_for_status()?;
    server.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn invalid_residency_limits_fail_before_creating_local_state() -> Result {
    let workspace = tempfile::TempDir::new()?;
    for limit in [0, 10_000, usize::MAX] {
        let path = workspace.path().join(format!("limit-{limit}"));
        let mut settings = config(available_address().await?, path.clone());
        settings.max_active_repositories = limit;
        let result = CanopyServer::start(settings, Arc::new(InMemory::new())).await;
        assert!(matches!(
            result,
            Err(canopy_server::server::ServerError::Http(_))
        ));
        assert!(!path.exists());
    }
    Ok(())
}

// Inspect authenticated roots through Cellule's VFS; restored files may contain
// sparse placeholders that ordinary SQLite cannot read independently.
type DurableRows =
    std::collections::BTreeMap<String, Vec<Vec<cellule_ltx::rusqlite::types::Value>>>;

fn assert_read_only_restore(before: &DurableRows, after: &DurableRows) {
    use cellule_ltx::rusqlite::types::Value;
    assert_eq!(
        before.keys().collect::<Vec<_>>(),
        after.keys().collect::<Vec<_>>(),
        "restoration changed the schema inventory"
    );
    for (table, rows) in before {
        let restored = &after[table];
        match table.as_str() {
            "catalog_custody_commands" => {
                // Serving purpose is 1. Existing staging intents (purpose 0)
                // remain exact; a read cannot admit publication work.
                let staging = |values: &Vec<Vec<Value>>| {
                    values
                        .iter()
                        .filter(|row| row[0] == Value::Integer(0))
                        .cloned()
                        .collect::<Vec<_>>()
                };
                assert_eq!(staging(rows), staging(restored));
                for original in rows {
                    assert!(
                        restored.iter().any(|row| row[..6] == original[..6]),
                        "restoration replaced a custody intent"
                    );
                }
            }
            "catalog_serving_pins" => {
                assert_eq!(restored.len(), 1, "one current generation must be pinned");
                let generation = &after["catalog_state"][0][1];
                for row in restored {
                    assert_eq!(
                        &row[4], generation,
                        "serving pin selected another generation"
                    );
                }
            }
            "sys_requests" => {
                for row in rows {
                    assert!(
                        restored.contains(row),
                        "restoration replaced a durable receipt"
                    );
                }
            }
            "sys_meta" => {
                let normalize = |values: &Vec<Vec<Value>>| {
                    let mut values = values.clone();
                    assert_eq!(values.len(), 1);
                    // Serving commands advance sequence and logical time.
                    values[0][3] = Value::Integer(0);
                    values[0][4] = Value::Integer(0);
                    values
                };
                assert_eq!(normalize(rows), normalize(restored));
            }
            _ => assert_eq!(rows, restored, "read-only restoration changed {table}"),
        }
    }
}

async fn durable_rows(
    layout: &CellStorageLayout,
    target: &cellule_runtime::CellTarget,
    path: &Path,
) -> Result<DurableRows> {
    let control = CellAuthority::new(layout.clone())
        .load(target.cell_id())
        .await?
        .ok_or("repository absent")?;
    let root = control.value().ltx_root().ok_or("root absent")?;
    let replica = cellule_ltx::CellReplica::new(
        layout.clone(),
        *target.cell_id().as_bytes(),
        *control.value().incarnation.as_bytes(),
        cellule_ltx::Limits::default(),
    )?;
    let root = replica.open_root(&root).await?.open_read_only(path)?;
    let c = root.connection()?;
    let mut names = c.prepare("SELECT name FROM sqlite_schema WHERE type='table' ORDER BY name")?;
    let names = names
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let mut result = std::collections::BTreeMap::new();
    for name in names {
        let quoted = name.replace('"', "\"\"");
        let query = c.prepare(&format!("SELECT * FROM \"{quoted}\""))?;
        let count = query.column_count();
        let order = (1..=count)
            .map(|i| i.to_string())
            .collect::<Vec<_>>()
            .join(",");
        drop(query);
        let mut query = c.prepare(&format!("SELECT * FROM \"{quoted}\" ORDER BY {order}"))?;
        let rows = query
            .query_map([], |row| {
                (0..count)
                    .map(|i| row.get(i))
                    .collect::<std::result::Result<Vec<cellule_ltx::rusqlite::types::Value>, _>>()
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        result.insert(name, rows);
    }
    Ok(result)
}
