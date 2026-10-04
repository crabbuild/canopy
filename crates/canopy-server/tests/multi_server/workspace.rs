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

#[tokio::test(flavor = "multi_thread")]
async fn new_repositories_bootstrap_the_production_packed_catalog_before_becoming_ready()
-> Result<(), Box<dyn std::error::Error>> {
    use canopy_server::packs::{
        catalog::{CatalogSnapshot, StoredCatalog},
        ref_state::RefStateSnapshotRoot,
    };
    use cellule_runtime::codec::{BoundedDecoder, WireValue};
    use object_store::ObjectStoreExt;
    for format in ["sha1", "sha256"] {
        let files = tempfile::TempDir::new()?;
        let data = files.path().join("node");
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let cfg = config(address, data.clone());
        let prefix = cfg.store_prefix.clone();
        let tenant = cfg.tenant;
        let application = cfg.application;
        let layout = cellule_runtime::ltx::CellStorageLayout::new(
            cellule_store::Store::new(Arc::clone(&store)),
            prefix.clone(),
            *application.as_bytes(),
        );
        let server = CanopyServer::start_with_listener(cfg, Arc::clone(&store), listener).await?;
        let client = reqwest::Client::new();
        let response: serde_json::Value = client
            .post(format!("http://{address}/api/repositories"))
            .bearer_auth("local-test-token")
            .json(&serde_json::json!({"name":"packed","object_format":format}))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        let repository = uuid::Uuid::parse_str(
            response["repository_id"]
                .as_str()
                .ok_or("repository UUID absent")?,
        )?;
        let target = canopy_server::repository_target(tenant, application, *repository.as_bytes())?;
        server.shutdown().await?;
        let root = repository_root(&layout, &target, &files.path().join("original.sqlite")).await?;
        let (catalog, refs, allocation) = {
            let connection = root.connection()?;
            for table in [
                "objects",
                "object_uploads",
                "object_chunks",
                "object_edges",
                "object_closure",
                "git_packs",
                "commit_parents",
                "commit_ancestry",
            ] {
                assert!(
                    !connection.query_row(
                        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name=?1)",
                        [table],
                        |row| row.get::<_, bool>(0)
                    )?,
                    "legacy table {table} is still selected"
                );
            }
            let (generation,catalog,refs,certificate): (i64,Vec<u8>,Vec<u8>,Vec<u8>)=connection.query_row(
            "SELECT g.generation,g.catalog,g.refs,g.certificate FROM catalog_state s JOIN catalog_generations g ON g.generation=s.generation WHERE s.singleton=1",[],|row| Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?)))?;
            assert_eq!(generation, 1);
            assert_eq!(certificate.len(), 32);
            assert_eq!(
                connection
                    .query_row("SELECT count(*) FROM catalog_initialization", [], |row| row
                        .get::<_, i64>(0))?,
                1
            );
            assert_eq!(
                connection
                    .query_row("SELECT count(*) FROM catalog_operations", [], |row| row
                        .get::<_, i64>(0))?,
                0
            );
            assert_eq!(connection.query_row("SELECT count(*) FROM catalog_leases WHERE generation=0 AND recovery IS NOT NULL AND recovery_phase IS NOT NULL AND recovery_phase_revision=1", [], |row| row.get::<_, i64>(0))?, 1);
            let allocation = connection.query_row(
                "SELECT artifact_sequence FROM repository_identity WHERE singleton=1",
                [],
                |row| row.get::<_, i64>(0),
            )?;
            assert_eq!(allocation, 1);
            (catalog, refs, allocation)
        };
        drop(root);
        let mut decoder = BoundedDecoder::new(&catalog, 256)?;
        let catalog = StoredCatalog::decode(&mut decoder)?;
        decoder.finish()?;
        let mut decoder = BoundedDecoder::new(&refs, 128)?;
        let refs = RefStateSnapshotRoot::decode(&mut decoder)?;
        decoder.finish()?;
        let provider: Arc<dyn ObjectStore> = Arc::new(object_store::prefix::PrefixStore::new(
            Arc::clone(&store),
            prefix.clone(),
        ));
        let artifacts = canopy_object_storage::artifact::ArtifactStore::new(
            Arc::clone(&provider),
            *repository.as_bytes(),
        );
        let snapshot = CatalogSnapshot::download(&artifacts, catalog).await?;
        assert!(snapshot.sources.is_none());
        let snapshot = refs.read(&artifacts).await?;
        assert_eq!(snapshot.generation, 0);
        assert_eq!(snapshot.default_branch, "refs/heads/main");
        assert!(snapshot.root.is_none());
        let restored_data = files.path().join("restored");
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let restored_address = listener.local_addr()?;
        let restored = CanopyServer::start_with_listener(
            config(restored_address, restored_data.clone()),
            Arc::clone(&store),
            listener,
        )
        .await?;
        let restored_response: serde_json::Value = client
            .get(format!("http://{restored_address}/api/repositories/packed"))
            .bearer_auth("local-test-token")
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        assert_eq!(
            restored_response["repository_id"],
            response["repository_id"]
        );
        restored.shutdown().await?;
        let root = repository_root(&layout, &target, &files.path().join("restored.sqlite")).await?;
        {
            let connection = root.connection()?;
            assert_eq!(
                connection.query_row(
                    "SELECT artifact_sequence FROM repository_identity WHERE singleton=1",
                    [],
                    |row| row.get::<_, i64>(0)
                )?,
                allocation
            );
            assert_eq!(
                connection.query_row(
                    "SELECT catalog FROM catalog_generations WHERE generation=1",
                    [],
                    |row| row.get::<_, Vec<u8>>(0)
                )?,
                {
                    let mut encoded = cellule_runtime::codec::BoundedEncoder::new(256)?;
                    catalog.encode(&mut encoded)?;
                    encoded.finish()
                }
            );
        }
        drop(root);

        // A Ready repository must fail closed when retained initialization
        // metadata is missing, rather than silently publishing another empty
        // catalog. The immutable Cell outcome still exists in this fixture.
        let catalog_path = artifacts.path(
            canopy_object_storage::artifact::ArtifactKey {
                operation: catalog.operation,
                binding_digest: catalog.artifact.digest,
                kind: canopy_object_storage::artifact::ArtifactKind::CatalogNode,
            },
            catalog.artifact.digest,
        )?;
        provider.head(&catalog_path).await?;
        provider.delete(&catalog_path).await?;
        assert!(matches!(
            provider.head(&catalog_path).await,
            Err(object_store::Error::NotFound { .. })
        ));
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let broken_address = listener.local_addr()?;
        let broken = CanopyServer::start_with_listener(
            config(broken_address, files.path().join("missing-initial-catalog")),
            Arc::clone(&store),
            listener,
        )
        .await?;
        let refused = client
            .get(format!("http://{broken_address}/api/repositories/packed"))
            .bearer_auth("local-test-token")
            .send()
            .await?;
        assert_eq!(refused.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(refused.text().await?, "Repository is unavailable");
        broken.shutdown().await?;
        let root = repository_root(&layout, &target, &files.path().join("refused.sqlite")).await?;
        {
            let connection = root.connection()?;
            assert_eq!(
                connection.query_row(
                    "SELECT artifact_sequence FROM repository_identity WHERE singleton=1",
                    [],
                    |row| row.get::<_, i64>(0)
                )?,
                allocation
            );
            assert_eq!(
                connection.query_row(
                    "SELECT generation FROM catalog_state WHERE singleton=1",
                    [],
                    |row| row.get::<_, i64>(0)
                )?,
                1
            );
        }
        drop(root);
    }
    Ok(())
}

// Restored workers use sparse placeholders. Ordinary SQLite cannot fetch their
// missing pages; inspect the authenticated published root through Cellule's VFS.
async fn repository_root(
    layout: &cellule_runtime::ltx::CellStorageLayout,
    target: &cellule_runtime::CellTarget,
    destination: &Path,
) -> Result<cellule_ltx::ReadOnlyRoot, Box<dyn std::error::Error>> {
    let control = cellule_runtime::control::authority::CellAuthority::new(layout.clone())
        .load(target.cell_id())
        .await?
        .ok_or("repository control absent")?;
    let root = control.value().ltx_root().ok_or("repository root absent")?;
    let replica = cellule_ltx::CellReplica::new(
        layout.clone(),
        *target.cell_id().as_bytes(),
        *control.value().incarnation.as_bytes(),
        cellule_ltx::Limits::default(),
    )?;
    Ok(replica
        .open_root(&root)
        .await?
        .open_read_only(destination)?)
}
