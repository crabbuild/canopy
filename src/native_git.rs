//! Process policy shared by every native Git operation on a disposable cache.

use std::path::Path;

use tokio::process::Command;

pub(crate) fn command(git_dir: &Path) -> Command {
    let mut command = Command::new("git");
    // Host configuration can redirect objects, execute helpers or emit traces
    // outside our accounting. Provider credentials must not reach Git or hooks.
    command.env_clear();
    for name in ["PATH", "SystemRoot"] {
        if let Some(value) = std::env::var_os(name) {
            command.env(name, value);
        }
    }
    command
        .current_dir(git_dir)
        .env("HOME", git_dir)
        .env("XDG_CONFIG_HOME", git_dir)
        .env("TMPDIR", git_dir)
        .env("TMP", git_dir)
        .env("TEMP", git_dir)
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_ATTR_NOSYSTEM", "1")
        .env(
            "GIT_CONFIG_GLOBAL",
            if cfg!(windows) { "NUL" } else { "/dev/null" },
        )
        .env("GIT_TERMINAL_PROMPT", "0")
        .env("LC_ALL", "C")
        .arg("--no-replace-objects")
        .args(["-c", "protocol.allow=never"]);
    command
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::process::Stdio;
    use tokio::io::AsyncWriteExt;

    #[tokio::test]
    async fn host_settings_cannot_redirect_objects_or_reach_helpers()
    -> Result<(), Box<dyn std::error::Error>> {
        const CHILD_ROOT: &str = "CANOPY_TEST_GIT_ENVIRONMENT_ROOT";
        let Some(root) = std::env::var_os(CHILD_ROOT) else {
            // A separate process gives the worker a contaminated host environment
            // without mutating environment shared by concurrent tests.
            let root = tempfile::TempDir::new()?;
            let config = root.path().join("config");
            std::fs::write(&config, "[canopy]\npoison = host\n")?;
            let status = Command::new(std::env::current_exe()?)
                .args([
                    "--exact",
                    "native_git::tests::host_settings_cannot_redirect_objects_or_reach_helpers",
                    "--nocapture",
                ])
                .current_dir(root.path())
                .env(CHILD_ROOT, root.path())
                .env("CANOPY_TEST_PROVIDER_KEY", "synthetic-provider-secret")
                .env("GIT_CONFIG_GLOBAL", &config)
                .env("GIT_CONFIG_SYSTEM", &config)
                .env("GIT_CONFIG_COUNT", "1")
                .env("GIT_CONFIG_KEY_0", "canopy.poison")
                .env("GIT_CONFIG_VALUE_0", "environment")
                .env("GIT_OBJECT_DIRECTORY", root.path().join("outside"))
                .env("GIT_DIR", root.path().join("wrong.git"))
                .env("GIT_EXEC_PATH", root.path().join("exec"))
                .env("GIT_TRACE", root.path().join("trace"))
                .status()
                .await?;
            assert!(status.success());
            assert_eq!(std::fs::read_dir(root.path())?.count(), 1);
            return Ok(());
        };
        let root = Path::new(&root);
        // Exercise relative cache roots too: subprocess cwd must not reinterpret
        // an already resolved cache path against itself.
        let cache = crate::git_cache::GitCache::create(
            ".".into(),
            cellule_ltx::DiskBudget::new(1 << 20),
            "refs/heads/main",
        )
        .await?;
        let git_dir = cache.git_dir();
        assert!(git_dir.is_absolute());
        let config = command(&git_dir)
            .args(["config", "--get", "canopy.poison"])
            .output()
            .await?;
        assert_eq!(config.status.code(), Some(1));
        let helper = command(&git_dir)
            .args([
                "-c",
                "alias.probe=!test -z \"$CANOPY_TEST_PROVIDER_KEY$GIT_OBJECT_DIRECTORY$GIT_TRACE\" && test \"$PWD\" = \"$TMPDIR\" && test \"$TEMP\" = \"$TMPDIR\" && test \"$TMP\" = \"$TMPDIR\" && printf isolated",
                "probe",
            ])
            .output()
            .await?;
        assert!(helper.status.success());
        assert_eq!(helper.stdout, b"isolated");
        let mut writer = command(&git_dir)
            .args(["hash-object", "-w", "--stdin"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;
        let body = b"canonical cache object\n";
        writer.stdin.take().ok_or("stdin")?.write_all(body).await?;
        let output = writer.wait_with_output().await?;
        assert!(output.status.success());
        let oid = hex::encode(crate::object_id(crate::ObjectKind::Blob, body));
        assert_eq!(output.stdout, format!("{oid}\n").as_bytes());
        assert!(
            git_dir
                .join("objects")
                .join(&oid[..2])
                .join(&oid[2..])
                .is_file()
        );
        assert!(!root.join("outside").exists());
        assert!(!root.join("trace").exists());
        Ok(())
    }
}
