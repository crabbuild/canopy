use super::*;
use canopy_server::{
    CanopyApplication, build_descriptor,
    deployment::{Deployment, WorkerConfig},
};
use crab_cell_app::CellApplication;
use crab_cell_runtime::{cell::application::ApplicationIdentity, identity::RequestId};
use crab_storage::Store;
use object_store::ObjectStoreExt;
use serde_json::{Value, json};

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

#[tokio::test(flavor = "multi_thread")]
async fn backup_restores_git_lfs_and_collaboration_without_original_storage() -> Result {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let files = tempfile::TempDir::new()?;
    let address = available_address().await?;
    let settings = config(address, files.path().join("server"));
    let source_prefix = settings.store_prefix.clone();
    let application = CanopyApplication::compile(build_descriptor(
        include_bytes!("../../Cargo.lock"),
        env!("CARGO_PKG_VERSION"),
    ))?;
    let deployment = Deployment::new(
        Store::new(store.clone()),
        source_prefix.clone(),
        ApplicationIdentity::new(settings.tenant, settings.application),
        settings.fleet,
        settings.image,
        application.registry(),
    )?;
    let worker = || WorkerConfig {
        node: NodeId::from_bytes(uuid::Uuid::new_v4().into_bytes()),
        signing_key: SigningKey::from_bytes(&[42; 32]),
        endpoint: "https://backup.example.invalid".into(),
        data_dir: files.path().join("worker"),
        local_disk_limit_bytes: 256 * 1024 * 1024,
    };
    let server = CanopyServer::start(settings, store.clone()).await?;
    let url = create_repository(address, "retained").await?;
    let local = files.path().join("source");
    run_git(None, &["init", "-b", "main", path_str(&local)?]).await?;
    for arguments in [
        &["config", "user.name", "Backup Test"][..],
        &["config", "user.email", "backup@example.invalid"],
        &["lfs", "install", "--local"],
        &["lfs", "track", "*.lfs"],
    ] {
        run_git(Some(&local), arguments).await?;
    }
    let blob = vec![17_u8; 1024 * 1024];
    let lfs = vec![29_u8; 256 * 1024];
    std::fs::write(local.join("large.bin"), &blob)?;
    std::fs::write(local.join("data.lfs"), &lfs)?;
    run_git(Some(&local), &["add", "."]).await?;
    run_git(Some(&local), &["commit", "-m", "Back up all bytes"]).await?;
    run_git(
        Some(&local),
        &[
            "-c",
            "http.extraHeader=Authorization: Bearer local-test-token",
            "push",
            &url,
            "main",
        ],
    )
    .await?;
    let client = reqwest::Client::new();
    let api = format!("http://{address}/api/repositories/retained");
    let repository: Value = client
        .get(&api)
        .bearer_auth("local-test-token")
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let issue: Value = client.post(format!("{api}/issues")).bearer_auth("local-test-token").json(&json!({"repository_id": repository["repository_id"], "id": uuid::Uuid::new_v4().to_string(), "title": "Retained issue", "body": "Restored without source objects"})).send().await?.error_for_status()?.json().await?;
    server.shutdown().await?;
    let id = RequestId::from_bytes(uuid::Uuid::new_v4().into_bytes());
    let backup = StorePath::from("isolated-backup");
    let destination = StorePath::from("isolated-restore");
    assert!(
        deployment
            .create_backup(id, source_prefix.clone(), worker())
            .await
            .is_err()
    );
    let report = deployment
        .create_backup(id, backup.clone(), worker())
        .await?;
    assert_eq!(report.external_objects, 2);
    assert_eq!(report.cells, 2);
    deployment
        .create_backup(id, backup.clone(), worker())
        .await?;
    let mut forbidden = config(
        available_address().await?,
        files.path().join("backup-server"),
    );
    forbidden.store_prefix = backup.clone();
    assert!(CanopyServer::start(forbidden, store.clone()).await.is_err());
    // Delete only this fixture's original prefix; verification and restore must
    // depend on the completed backup's copies, including external bodies.
    for object in store
        .list_with_delimiter(Some(&source_prefix))
        .await?
        .objects
    {
        store.delete(&object.location).await?;
    }
    let mut pending = vec![source_prefix.clone()];
    while let Some(prefix) = pending.pop() {
        let listing = store.list_with_delimiter(Some(&prefix)).await?;
        pending.extend(listing.common_prefixes);
        for object in listing.objects {
            store.delete(&object.location).await?;
        }
    }
    let emptied = store.list_with_delimiter(Some(&source_prefix)).await?;
    assert!(emptied.objects.is_empty() && emptied.common_prefixes.is_empty());
    deployment
        .verify_backup(id, backup.clone(), worker())
        .await?;
    let mut constrained = worker();
    constrained.local_disk_limit_bytes = 1;
    assert!(
        deployment
            .verify_backup(id, backup.clone(), constrained)
            .await
            .is_err()
    );
    deployment
        .restore_backup(id, backup.clone(), destination.clone(), worker())
        .await?;
    deployment
        .restore_backup(id, backup.clone(), destination.clone(), worker())
        .await?;
    let mut restored_settings = config(available_address().await?, files.path().join("restored"));
    restored_settings.store_prefix = destination.clone();
    let restored_address = restored_settings.listen;
    let restored = CanopyServer::start(restored_settings, store.clone()).await?;
    let cloned = files.path().join("clone");
    run_git(
        None,
        &[
            "-c",
            "http.extraHeader=Authorization: Bearer local-test-token",
            "clone",
            &format!("http://{restored_address}/canopy/retained.git"),
            path_str(&cloned)?,
        ],
    )
    .await?;
    run_git(Some(&cloned), &["lfs", "install", "--local"]).await?;
    run_git(
        Some(&cloned),
        &[
            "-c",
            "http.extraHeader=Authorization: Bearer local-test-token",
            "lfs",
            "pull",
        ],
    )
    .await?;
    assert!(
        std::fs::read(cloned.join("large.bin"))? == blob,
        "restored Git blob differs"
    );
    assert!(
        std::fs::read(cloned.join("data.lfs"))? == lfs,
        "restored LFS body differs"
    );
    run_git(Some(&cloned), &["fsck", "--strict", "--full"]).await?;
    let restored_issue: Value = client
        .get(format!(
            "http://{restored_address}/api/repositories/retained/issues/1"
        ))
        .bearer_auth("local-test-token")
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(issue, json!({"number": 1}));
    assert_eq!(restored_issue["issue"]["title"], "Retained issue");
    assert_eq!(
        restored_issue["issue"]["body"],
        "Restored without source objects"
    );
    let restored_api = format!("http://{restored_address}/api/repositories/retained");
    client
        .post(format!("{restored_api}/issues"))
        .bearer_auth("local-test-token")
        .json(&json!({
            "repository_id": repository["repository_id"],
            "id": uuid::Uuid::new_v4().to_string(),
            "title": "Created after restore",
            "body": "Keep subsequent service writes"
        }))
        .send()
        .await?
        .error_for_status()?;
    deployment
        .restore_backup(id, backup.clone(), destination, worker())
        .await?;
    let later_issue: Value = client
        .get(format!("{restored_api}/issues/2"))
        .bearer_auth("local-test-token")
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(later_issue["issue"]["title"], "Created after restore");
    restored.shutdown().await?;
    use sha2::{Digest as _, Sha256};
    let repository_id = uuid::Uuid::parse_str(
        repository["repository_id"]
            .as_str()
            .ok_or("missing repository id")?,
    )?;
    let lfs_path = StorePath::from(format!(
        "{backup}/repos/{}/lfs/{}",
        hex::encode(repository_id.as_bytes()),
        hex::encode(Sha256::digest(&lfs))
    ));
    store.put(&lfs_path, vec![0; lfs.len()].into()).await?;
    assert!(
        deployment
            .verify_backup(id, backup.clone(), worker())
            .await
            .is_err()
    );
    let incomplete = StorePath::from("incomplete-restore");
    assert!(
        deployment
            .restore_backup(id, backup.clone(), incomplete.clone(), worker())
            .await
            .is_err()
    );
    let mut denied = config(available_address().await?, files.path().join("denied"));
    denied.store_prefix = incomplete.clone();
    assert!(CanopyServer::start(denied, store.clone()).await.is_err());
    store.put(&lfs_path, lfs.into()).await?;
    deployment
        .restore_backup(id, backup, incomplete, worker())
        .await?;
    Ok(())
}
