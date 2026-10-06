use super::*;
use canopy_server::{
    CanopyApplication, build_descriptor,
    deployment::{Deployment, WorkerConfig},
};
use cellule_app::CellApplication;
use cellule_runtime::{cell::application::ApplicationIdentity, identity::RequestId};
use cellule_store::Store;
use object_store::ObjectStoreExt;
use serde_json::{Value, json};

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

#[tokio::test(flavor = "multi_thread")]
async fn backup_restores_git_lfs_and_collaboration_without_original_storage() -> Result {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let files = tempfile::TempDir::new()?;
    let address = available_address().await?;
    let settings = config(address, files.path().join("server"));
    let source_prefix = settings.store_prefix.clone();
    let application = CanopyApplication::compile(build_descriptor(
        include_bytes!("../../../../Cargo.lock"),
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
    // Force a native receive pack with delta-compressed small blobs. Backup
    // must retain both immutable artifacts after original storage is deleted.
    for n in 0..200 {
        let mut body = vec![b'x'; 32 * 1024];
        body[..8].copy_from_slice(&(n as u64).to_le_bytes());
        std::fs::write(local.join(format!("packed-{n:03}")), body)?;
    }
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
    // Simulate an uploaded creating body whose attempt never registered a
    // root. Provider listings must not turn this orphan into a backup root.
    use canopy_object_storage::artifact::{ArtifactKey, ArtifactKind, ArtifactStore};
    let orphan_store = ArtifactStore::new(
        Arc::new(object_store::prefix::PrefixStore::new(
            store.clone(),
            source_prefix.clone(),
        )),
        *uuid::Uuid::parse_str(
            repository["repository_id"]
                .as_str()
                .ok_or("repository UUID missing")?,
        )?
        .as_bytes(),
    );
    let mut orphan = b"unregistered creating input".as_slice();
    let digest = *blake3::hash(orphan).as_bytes();
    orphan_store
        .put(
            ArtifactKey {
                operation: [99; 16],
                binding_digest: digest,
                kind: ArtifactKind::InputBody,
            },
            orphan.len() as u64,
            digest,
            &mut orphan,
        )
        .await?;
    let report = deployment
        .create_backup(id, backup.clone(), worker())
        .await?;
    // Artifact deduplication depends on native Git's packing and exact
    // response bytes. Verify the physical inventory below rather than a
    // platform-specific number of distinct content-addressed artifacts.
    assert_eq!(report.cells, 2);
    deployment
        .create_backup(id, backup.clone(), worker())
        .await?;
    // Retired input/command bodies and unselected native responses are not
    // permanent audit edges. Remove all this fixture's unretained native bytes
    // and prove the same pinned backup remains independently reproducible.
    use futures_util::TryStreamExt;
    let native_repository = format!(
        "repos/{}",
        hex::encode(
            uuid::Uuid::parse_str(
                repository["repository_id"]
                    .as_str()
                    .ok_or("missing repository id")?
            )?
            .as_bytes()
        )
    );
    let retained_prefix = StorePath::from(format!("{backup}/{native_repository}"));
    let retained = store
        .list(Some(&retained_prefix))
        .try_collect::<Vec<_>>()
        .await?
        .into_iter()
        .map(|entry| {
            entry
                .location
                .as_ref()
                .strip_prefix(backup.as_ref())
                .map(str::to_owned)
                .ok_or("backup namespace differs")
        })
        .collect::<std::result::Result<std::collections::HashSet<_>, _>>()?;
    let native_manifests = retained
        .iter()
        .filter(|path| !path.contains(".parts/"))
        .count() as u64;
    // Repository inventory includes its one retained LFS body. Count
    // artifacts, not provider parts, against the report.
    assert_eq!(report.external_objects, native_manifests);
    assert_eq!(
        retained
            .iter()
            .filter(|path| path.contains("/lfs/") && !path.contains(".parts/"))
            .count(),
        1
    );
    for family in ["git-packs", "git-catalogs", "git-inputs"] {
        assert!(
            retained.iter().any(|path| path.contains(family)),
            "missing {family}"
        );
    }
    assert!(
        !retained
            .iter()
            .any(|path| path.contains(&hex::encode(digest))),
        "unregistered creating input must not become a backup root"
    );
    let creating_prefix = StorePath::from(format!("{source_prefix}/{native_repository}"));
    let creating = store
        .list(Some(&creating_prefix))
        .try_collect::<Vec<_>>()
        .await?;
    let mut retired = 0;
    for entry in creating {
        let relative = entry
            .location
            .as_ref()
            .strip_prefix(source_prefix.as_ref())
            .ok_or("source namespace differs")?;
        if !retained.contains(relative) {
            store.delete(&entry.location).await?;
            retired += 1;
        }
    }
    assert!(
        retired > 0,
        "fixture must contain unretained native input artifacts"
    );
    assert_eq!(
        deployment
            .create_backup(id, backup.clone(), worker())
            .await?
            .external_objects,
        report.external_objects
    );
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
    let verified = deployment
        .verify_backup(id, backup.clone(), worker())
        .await?;
    assert_eq!(verified.external_objects, report.external_objects);
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
    assert_eq!(
        std::fs::read(cloned.join("packed-199"))?,
        std::fs::read(local.join("packed-199"))?
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
    // A complete backup must verify the native parts as well as LFS. Locate
    // this fixture's one pack solely to inject provider corruption; production
    // traversal derives its authority from the pinned typed graph.
    let native_prefix = StorePath::from(format!(
        "{backup}/repos/{}/git-packs",
        hex::encode(repository_id.as_bytes())
    ));
    let parts = store
        .list(Some(&native_prefix))
        .try_collect::<Vec<_>>()
        .await?;
    let native_part = parts
        .iter()
        .find(|entry| {
            entry
                .location
                .as_ref()
                .ends_with("/pack.parts/0000000000000000")
        })
        .ok_or("backup native pack part absent")?
        .location
        .clone();
    let original = store.get(&native_part).await?.bytes().await?;
    store
        .put(&native_part, vec![0; original.len()].into())
        .await?;
    assert!(
        deployment
            .verify_backup(id, backup.clone(), worker())
            .await
            .is_err()
    );
    store.put(&native_part, original.into()).await?;
    assert_eq!(
        deployment
            .verify_backup(id, backup.clone(), worker())
            .await?
            .external_objects,
        report.external_objects
    );
    let lfs_path = StorePath::from(format!(
        "{backup}/repos/{}/lfs/{}.parts/0000000000000000",
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
