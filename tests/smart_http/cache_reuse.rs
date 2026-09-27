use super::*;
use std::{collections::BTreeMap, path::PathBuf, time::SystemTime};

fn cached_objects(root: &Path) -> std::io::Result<BTreeMap<PathBuf, (u64, SystemTime)>> {
    let mut objects = BTreeMap::new();
    for entry in std::fs::read_dir(root)? {
        let entry = entry?;
        if !entry
            .file_name()
            .to_string_lossy()
            .starts_with("canopy-git-")
        {
            continue;
        }
        let git = entry.path().join("repo.git");
        if git.join("objects/info/alternates").exists() {
            continue;
        }
        for shard in std::fs::read_dir(git.join("objects"))? {
            let shard = shard?;
            let name = shard.file_name();
            if name.len() != 2 || !name.as_encoded_bytes().iter().all(u8::is_ascii_hexdigit) {
                continue;
            }
            for object in std::fs::read_dir(shard.path())? {
                let object = object?;
                if object.file_name().len() == 38 {
                    let metadata = object.metadata()?;
                    objects.insert(object.path(), (metadata.len(), metadata.modified()?));
                }
            }
        }
    }
    Ok(objects)
}

pub async fn verify(
    root: &Path,
    local: &Path,
    url: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let before = cached_objects(root)?;
    assert!(!before.is_empty());
    tokio::fs::write(local.join("cache-reuse.txt"), b"only this blob is new\n").await?;
    run_git(Some(local), &["add", "cache-reuse.txt"]).await?;
    run_git(Some(local), &["commit", "-m", "Incremental object cache"]).await?;
    let commit = run_git(Some(local), &["rev-parse", "HEAD"]).await?;
    run_git(
        Some(local),
        &["-c", AUTH, "push", url, "HEAD:refs/heads/cache-reuse"],
    )
    .await?;
    run_git(None, &["-c", AUTH, "ls-remote", url]).await?;
    let after = cached_objects(root)?;
    // A new blob, tree and commit are hydrated. Existing bodies keep their
    // actual files across receive-pack and the next published ref generation.
    assert_eq!(after.len(), before.len() + 3);
    for (path, metadata) in before {
        assert_eq!(after.get(&path), Some(&metadata));
    }
    let clone = root.join("cache-reuse-clone");
    run_git(
        None,
        &[
            "-c",
            AUTH,
            "clone",
            "--single-branch",
            "--branch",
            "cache-reuse",
            "--no-checkout",
            url,
            clone.to_str().ok_or("clone path")?,
        ],
    )
    .await?;
    assert_eq!(run_git(Some(&clone), &["rev-parse", "HEAD"]).await?, commit);
    assert_eq!(
        run_git(Some(&clone), &["show", "HEAD:cache-reuse.txt"]).await?,
        b"only this blob is new\n"
    );
    run_git(Some(&clone), &["fsck", "--strict", "--full"]).await?;
    Ok(())
}

const AUTH: &str = "http.extraHeader=Authorization: Bearer local-test-token";
