use super::*;

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

#[test]
fn live_owner_blocks_cleanup_and_restart_reclaims_only_managed_state() -> Result {
    let directory = tempfile::TempDir::new()?;
    let workspace = Workspace::open(directory.path())?;
    let data = workspace.path().join("directory.sqlite");
    fs::write(&data, b"disposable SQLite cache")?;
    fs::write(directory.path().join("operator-notes"), b"retain")?;
    assert!(
        Workspace::open(directory.path())
            .is_err_and(|error| error.kind() == io::ErrorKind::WouldBlock)
    );
    assert!(data.exists());
    drop(workspace);
    let restored = Workspace::open(directory.path())?;
    assert!(!data.exists());
    assert_eq!(
        fs::read(directory.path().join("operator-notes"))?,
        b"retain"
    );
    assert_eq!(fs::read_dir(restored.path())?.count(), 1);
    Ok(())
}

#[test]
fn unrecognized_runtime_is_never_reclaimed() -> Result {
    let directory = tempfile::TempDir::new()?;
    let root = directory.path().join("runtime-v1");
    fs::create_dir(&root)?;
    fs::write(root.join("important"), b"retain")?;
    assert!(Workspace::open(directory.path()).is_err());
    fs::write(root.join(MARKER), b"unknown version")?;
    assert!(
        Workspace::open(directory.path())
            .is_err_and(|error| error.kind() == io::ErrorKind::InvalidData)
    );
    assert_eq!(fs::read(root.join("important"))?, b"retain");
    Ok(())
}

#[cfg(unix)]
#[test]
fn runtime_symlink_is_rejected_and_nested_symlinks_do_not_delete_targets() -> Result {
    use std::os::unix::fs::symlink;
    let directory = tempfile::TempDir::new()?;
    let outside = tempfile::TempDir::new()?;
    fs::write(outside.path().join("important"), b"retain")?;
    symlink(outside.path(), directory.path().join("runtime-v1"))?;
    assert!(Workspace::open(directory.path()).is_err());
    fs::remove_file(directory.path().join("runtime-v1"))?;
    let workspace = Workspace::open(directory.path())?;
    symlink(outside.path(), workspace.path().join("nested"))?;
    drop(workspace);
    let _restored = Workspace::open(directory.path())?;
    assert_eq!(fs::read(outside.path().join("important"))?, b"retain");
    Ok(())
}

#[cfg(unix)]
#[test]
fn failed_cleanup_prevents_startup_and_can_retry_after_permissions_are_repaired() -> Result {
    use std::os::unix::fs::PermissionsExt;
    let directory = tempfile::TempDir::new()?;
    let workspace = Workspace::open(directory.path())?;
    let blocked = workspace.path().join("blocked");
    fs::create_dir(&blocked)?;
    fs::write(blocked.join("cache"), b"disposable")?;
    fs::set_permissions(&blocked, fs::Permissions::from_mode(0o500))?;
    drop(workspace);
    let failed = Workspace::open(directory.path());
    fs::set_permissions(&blocked, fs::Permissions::from_mode(0o700))?;
    assert!(failed.is_err(), "requires an unprivileged test user");
    let _restored = Workspace::open(directory.path())?;
    assert!(!blocked.exists());
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn orphan_git_descendant_prevents_reclamation_until_it_exits() -> Result {
    use std::{process::Stdio, time::Duration};
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    let directory = tempfile::TempDir::new()?;
    let workspace = Workspace::open(directory.path())?;
    let cache = crate::git_cache::GitCache::create(
        workspace.path().into(),
        cellule_ltx::DiskBudget::new(1 << 20),
        "refs/heads/main",
    )
    .await?;
    let data = workspace.path().join("directory.sqlite");
    fs::write(&data, b"old database")?;
    let mut child = crate::native_git::command(&cache.git_dir())?
        .args(["-c", "alias.hold=!printf '%s\\n' $$; read release", "hold"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()?;
    let leader = child.id().ok_or("child PID")?;
    let mut input = child.stdin.take().ok_or("child stdin")?;
    let mut output = BufReader::new(child.stdout.take().ok_or("child stdout")?);
    let mut pid = String::new();
    tokio::time::timeout(Duration::from_secs(5), output.read_line(&mut pid)).await??;
    assert_ne!(pid.trim().parse::<u32>()?, leader);
    drop(workspace);
    child.kill().await?;
    child.wait().await?;
    assert!(
        Workspace::open(directory.path())
            .is_err_and(|error| error.kind() == io::ErrorKind::WouldBlock)
    );
    assert!(data.exists());
    assert!(cache.git_dir().exists());
    let abandoned = cache.git_dir();
    drop(cache);
    assert!(
        abandoned.exists(),
        "cache cleanup must retain a live worker's fence"
    );
    input.write_all(b"exit\n").await?;
    drop(input);
    let restored = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match Workspace::open(directory.path()) {
                Ok(workspace) => return Ok::<_, io::Error>(workspace),
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    tokio::time::sleep(Duration::from_millis(10)).await
                }
                Err(error) => return Err(error),
            }
        }
    })
    .await??;
    assert!(!data.exists());
    assert!(!abandoned.exists());
    assert_eq!(fs::read_dir(restored.path())?.count(), 1);
    Ok(())
}
