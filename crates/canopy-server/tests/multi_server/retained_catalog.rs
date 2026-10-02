//! Real startup seam for an immutable predecessor Directory catalog entry.
//! This owned in-memory fixture is not an upgrade of the live RustFS corpus
//! or proof that an old executable produced the stored bytes.

use super::*;
use bytes::Bytes;
use canopy_server::{
    CanopyApplication, build_descriptor,
    directory::{self, DirectoryCell, DirectoryModule, TokenScope},
};
use cellule_app::{ApplicationHandle, CellApplication};
use cellule_ltx::{CellReplica, DiskBudget, Host, Limits};
use cellule_runtime::{
    CellClient, CellModule, CellRuntime, MutationIdentity, SessionId,
    cell::application::{ApplicationIdentity, ApplicationIdentityStore},
    cell::catalog::{CatalogEntry, CatalogRole, CellCatalog},
    cell::worker::SqlWorkerPool,
    control::{ControlState, Owner, authority::CellAuthority},
    identity::{IncarnationId, RequestId},
    ltx::CellStorageLayout,
    node::NodeDirectory,
    recovery::release::{ReleaseState, ReleaseStore},
};
use cellule_store::Store;
use sha2::{Digest as _, Sha256};
use std::time::{SystemTime, UNIX_EPOCH};

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

fn now() -> Result<i64> {
    Ok(i64::try_from(
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis(),
    )?)
}

struct RetainedFixture {
    _files: tempfile::TempDir,
    configuration: ServerConfig,
    store: Arc<dyn ObjectStore>,
    layout: CellStorageLayout,
    catalog: CellCatalog,
    authority: CellAuthority,
    target: cellule_runtime::CellTarget,
    entry: CatalogEntry,
}

async fn retained_fixture() -> Result<RetainedFixture> {
    let files = tempfile::TempDir::new()?;
    let address = available_address().await?;
    let configuration = config(address, files.path().join("server"));
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let storage = Store::new(Arc::clone(&store));
    let identity = ApplicationIdentity::new(configuration.tenant, configuration.application);
    storage
        .create_strict(
            &configuration
                .store_prefix
                .clone()
                .join("canopy-root-v1.json"),
            Bytes::from_static(br#"{"kind":"service"}"#),
        )
        .await?;
    ApplicationIdentityStore::new(storage.clone(), configuration.store_prefix.clone())
        .initialize(identity)
        .await?;
    let layout = CellStorageLayout::new(
        storage,
        configuration.store_prefix.clone(),
        *configuration.application.as_bytes(),
    );
    let application = Arc::new(CanopyApplication::compile(build_descriptor(
        include_bytes!("../../../../Cargo.lock"),
        env!("CARGO_PKG_VERSION"),
    ))?);
    let registry = application.registry();
    let previous =
        include_bytes!("../directory_cell/fixtures/c51-selected-release.json").trim_ascii_end();
    assert_eq!(
        blake3::hash(previous).to_hex().as_str(),
        "e31bf1a951e2fa19d91e9f964b2ddeade1a81b05a20ad628362819a1487c16b1"
    );
    // This explicit fixture activation carries only a Directory Cell. The
    // repository pack schema separately rejects whole-release rolling upgrade.
    let descriptor: serde_json::Value = serde_json::from_slice(previous)?;
    let module = descriptor["modules"]
        .as_array()
        .ok_or("modules missing")?
        .iter()
        .find(|module| module["name"] == DirectoryModule::NAME)
        .ok_or("Directory predecessor missing")?;
    let old_code = Digest::from_bytes(
        hex::decode(module["code"].as_str().ok_or("code missing")?)?
            .try_into()
            .map_err(|_| "invalid predecessor code")?,
    );
    assert!(registry.supports_cell(directory::DIRECTORY, CatalogRole::Sql, old_code, 1));
    let releases = ReleaseStore::new(layout.clone(), identity)?;
    let image = format!("sha256:{}", hex::encode(configuration.image.as_bytes()));
    let old_operation = RequestId::from_bytes(uuid::Uuid::new_v4().into_bytes());
    let prepared = releases
        .prepare(
            previous,
            Digest::from_bytes(*blake3::hash(previous).as_bytes()),
            0,
            &image,
            old_operation,
        )
        .await?;
    // The fixture has no Cells or advertised writers at first activation.
    let activating = releases
        .start_activation(prepared.revision(), old_operation)
        .await?;
    let old_ready = releases
        .complete_activation(activating.revision(), old_operation)
        .await?;
    assert_eq!(old_ready.state(), ReleaseState::Ready);

    let target = directory::directory_target(configuration.tenant, configuration.application)?;
    let catalog = CellCatalog::new(layout.clone(), configuration.tenant);
    let entry = CatalogEntry::new(&target, CatalogRole::Sql, old_code, 1)?;
    let proof = catalog.provision(entry.clone()).await?;
    let authority = CellAuthority::new(layout.clone());
    let session = SessionId::from_bytes(uuid::Uuid::new_v4().into_bytes());
    let observed = authority
        .create_initial(
            &proof,
            IncarnationId::from_bytes(uuid::Uuid::new_v4().into_bytes()),
            Owner {
                session,
                endpoint: "https://canopy.test".into(),
            },
        )
        .await?;
    let runtime = CellRuntime::new_with_replica_host(
        SqlWorkerPool::new(1, 4)?,
        64 * 1024 * 1024,
        session,
        Host::default().with_local_disk_budget(DiskBudget::new(1 << 30)),
    )?;
    let handle = runtime
        .bootstrap(
            proof,
            CellReplica::new(
                layout.clone(),
                *target.cell_id().as_bytes(),
                *observed.value().incarnation.as_bytes(),
                Limits::default(),
            )?,
            authority.clone(),
            observed,
            files.path().join("predecessor.sqlite"),
            |transaction| {
                transaction.execute_batch(directory::SCHEMA)?;
                Ok(())
            },
        )
        .await?;
    let client = ApplicationHandle::<CanopyApplication>::new(
        CellClient::local(registry.clone(), handle),
        application,
        configuration.tenant,
        configuration.application,
    )?;
    let directory = DirectoryCell::new(&client, target.clone())?;
    let issued = now()?;
    directory
        .create_account(
            MutationIdentity {
                request_id: RequestId::from_bytes(uuid::Uuid::new_v4().into_bytes()),
                issued_at_ms: issued,
                expires_at_ms: issued + 60_000,
            },
            "canopy",
            Sha256::digest(configuration.token.as_bytes()).into(),
            TokenScope::Admin,
        )
        .await?;
    runtime.shutdown().await?;
    let old_control = authority
        .load(target.cell_id())
        .await?
        .ok_or("control missing")?;
    assert_eq!(old_control.value().code, old_code);
    assert_eq!(old_control.value().schema, 1);
    assert_eq!(old_control.value().state, ControlState::Idle);
    assert!(old_control.value().root.is_some());
    assert!(old_control.value().owner.is_none());

    // Explicit fixture-only admission, before completing the new release.
    // The low-level phase CAS does not itself prove compatibility or drain.
    let nodes = NodeDirectory::new(
        layout.clone(),
        configuration.fleet,
        configuration.image,
        registry.release_digest(),
    );
    assert!(nodes.advertised_sessions(now()?, 4096).await?.is_empty());
    let mut cells = 0;
    for shard in 0..=u8::MAX {
        let mut scan = catalog.scan_shard(shard).await?;
        while let Some(page) = scan.next_page().await? {
            for proof in page.entries() {
                assert_eq!(proof.entry(), &entry);
                assert!(registry.supports_cell(
                    proof.entry().namespace(),
                    proof.entry().role(),
                    proof.entry().initial_code(),
                    proof.entry().initial_schema(),
                ));
                cells += 1;
            }
        }
    }
    assert_eq!(cells, 1);
    let operation = RequestId::from_bytes(uuid::Uuid::new_v4().into_bytes());
    let prepared = releases
        .prepare(
            registry.release_bytes(),
            registry.release_digest(),
            old_ready.revision(),
            &image,
            operation,
        )
        .await?;
    let activating = releases
        .start_activation(prepared.revision(), operation)
        .await?;
    releases
        .complete_activation(activating.revision(), operation)
        .await?;

    Ok(RetainedFixture {
        _files: files,
        configuration,
        store,
        layout,
        catalog,
        authority,
        target,
        entry,
    })
}

#[tokio::test(flavor = "multi_thread")]
async fn server_reopens_a_retained_directory_catalog_after_explicit_fixture_activation() -> Result {
    let fixture = retained_fixture().await?;
    let address = fixture.configuration.listen;
    let RetainedFixture {
        _files,
        configuration,
        store,
        catalog,
        target,
        entry,
        ..
    } = fixture;

    // Exercise real Canopy startup, not a direct low-level client workaround.
    let server = CanopyServer::start(configuration, Arc::clone(&store)).await?;
    let url = create_repository(address, "after-activation").await?;
    let listing: serde_json::Value = reqwest::Client::new()
        .get(format!("http://{address}/api/repositories"))
        .bearer_auth("local-test-token")
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(
        listing["repositories"]
            .as_array()
            .ok_or("list missing")?
            .len(),
        1
    );
    assert_eq!(
        catalog
            .lookup(target.cell_id())
            .await?
            .ok_or("catalog missing")?
            .entry(),
        &entry
    );
    assert!(url.ends_with("/canopy/after-activation.git"));
    server.shutdown().await?;
    Ok(())
}

async fn unsupported_control_stays_unchanged(code: Option<Digest>, schema: Option<u32>) -> Result {
    let fixture = retained_fixture().await?;
    let observed = fixture
        .authority
        .load(fixture.target.cell_id())
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
    let path = fixture
        .layout
        .control_path(fixture.target.cell_id().as_bytes());
    let (_, token) = fixture.layout.store().get_with_etag(&path).await?;
    let encoded = unsupported.encode()?;
    // Inject unsupported persisted metadata only in this owned in-memory fixture.
    // Its catalog remains supported. A failed startup must reject the control
    // before acquisition, not claim it and rely on later SQL/client rejection.
    fixture
        .layout
        .store()
        .update(&path, Bytes::from(encoded.clone()), token)
        .await?;
    assert!(
        CanopyServer::start(fixture.configuration, Arc::clone(&fixture.store))
            .await
            .is_err()
    );
    let after = fixture
        .authority
        .load(fixture.target.cell_id())
        .await?
        .ok_or("control disappeared")?;
    assert_eq!(
        after.value().encode()?,
        encoded,
        "unsupported persisted control was acquired or rewritten"
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
