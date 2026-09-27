use super::*;
use std::process::Stdio;
use tokio::io::AsyncWriteExt;

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;
const AUTH: &str = "http.extraHeader=Authorization: Bearer local-test-token";

async fn edit_refs(path: &Path, commands: &str) -> Result {
    let mut child = Command::new("git")
        .current_dir(path)
        .args(["update-ref", "--stdin"])
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()?;
    let mut stdin = child.stdin.take().ok_or("missing Git stdin")?;
    stdin.write_all(commands.as_bytes()).await?;
    drop(stdin);
    let output = child.wait_with_output().await?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(())
}

async fn generation(client: &reqwest::Client, api: &str) -> Result<i64> {
    let response: serde_json::Value = client
        .get(format!("{api}/default-branch"))
        .bearer_auth("local-test-token")
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    response["generation"]
        .as_i64()
        .ok_or("missing ref generation".into())
}

#[tokio::test(flavor = "multi_thread")]
async fn bulk_mirror_publication_is_atomic_and_survives_restart() -> Result {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let workspace = tempfile::TempDir::new()?;
    let address = available_address().await?;
    let server = CanopyServer::start(
        config(address, workspace.path().join("first")),
        Arc::clone(&store),
    )
    .await?;
    let url = create_repository(address, "bulk").await?;
    let api = format!("http://{address}/api/repositories/bulk");
    let client = reqwest::Client::new();
    let repo: serde_json::Value = client
        .get(&api)
        .bearer_auth("local-test-token")
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    client
        .put(format!("{api}/branch-rules"))
        .bearer_auth("local-test-token")
        .json(
            &serde_json::json!({"repository_id":repo["repository_id"],"rule":{
                "reference":"refs/heads/main","expected_version":0,"enabled":true,
                "deny_deletions":true,"fast_forward_only":true,"require_pull_request":false,
                "required_approvals":0,"required_checks":[]
            }}),
        )
        .send()
        .await?
        .error_for_status()?;
    let source = workspace.path().join("source");
    run_git(None, &["init", "-b", "main", path_str(&source)?]).await?;
    run_git(
        Some(&source),
        &[
            "-c",
            "user.name=Canopy",
            "-c",
            "user.email=test@example.invalid",
            "-c",
            "commit.gpgsign=false",
            "commit",
            "--allow-empty",
            "-m",
            "Initial",
        ],
    )
    .await?;
    let oid = String::from_utf8(run_git(Some(&source), &["rev-parse", "HEAD"]).await?)?
        .trim()
        .to_owned();
    // Long valid names make the complete plan exceed the Cell's 1 MiB input
    // ceiling; one successful generation proves publication did not split it.
    let names: Vec<_> = (0..4096)
        .map(|index| format!("refs/tags/{}-{index:04}", "r".repeat(230)))
        .collect();
    let create: String = names
        .iter()
        .map(|name| format!("create {name} {oid}\n"))
        .collect();
    edit_refs(&source, &create).await?;
    let before = generation(&client, &api).await?;
    run_git(Some(&source), &["-c", AUTH, "push", "--mirror", &url]).await?;
    assert_eq!(generation(&client, &api).await?, before + 1);
    let expected = run_git(Some(&source), &["show-ref"]).await?;
    server.shutdown().await?;

    let address = available_address().await?;
    let restored =
        CanopyServer::start(config(address, workspace.path().join("restored")), store).await?;
    let url = format!("http://{address}/canopy/bulk.git");
    let api = format!("http://{address}/api/repositories/bulk");
    let mirror = workspace.path().join("mirror.git");
    run_git(
        None,
        &["-c", AUTH, "clone", "--mirror", &url, path_str(&mirror)?],
    )
    .await?;
    assert_eq!(run_git(Some(&mirror), &["show-ref"]).await?, expected);
    run_git(Some(&mirror), &["fsck", "--strict", "--full"]).await?;

    // A valid sibling commits on ordinary rejection, while atomic rejection
    // leaves it absent. A Git-valid name over Canopy's bound gets a Git report.
    let long = format!("refs/tags/{}/{}", "a".repeat(150), "b".repeat(150));
    for atomic in [true, false] {
        let mut command = Command::new("git");
        command
            .current_dir(&source)
            .args(["-c", "credential.helper=", "-c", AUTH, "push"]);
        if atomic {
            command.arg("--atomic");
        }
        let output = command
            .args([&url, "HEAD:refs/tags/accepted", &format!("HEAD:{long}")])
            .output()
            .await?;
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(!output.status.success());
        assert!(stderr.contains("Canopy ref name exceeds"), "{stderr}");
        assert!(!stderr.contains("HTTP 500"), "{stderr}");
        assert_eq!(
            generation(&client, &api).await?,
            before + if atomic { 1 } else { 2 }
        );
        let visible = run_git(None, &["-c", AUTH, "ls-remote", &url, "refs/tags/accepted"]).await?;
        assert_eq!(visible.is_empty(), atomic);
    }

    let deletes: String = names
        .iter()
        .map(|name| format!("delete {name}\n"))
        .collect();
    edit_refs(&source, &deletes).await?;
    run_git(Some(&source), &["-c", AUTH, "push", "--mirror", &url]).await?;
    assert_eq!(generation(&client, &api).await?, before + 3);
    assert_eq!(
        run_git(None, &["-c", AUTH, "ls-remote", "--refs", &url]).await?,
        format!("{oid}\trefs/heads/main\n").as_bytes()
    );
    let mut request = Vec::new();
    for index in 0..100_001 {
        let capabilities = if index == 0 {
            "\0report-status side-band-64k"
        } else {
            ""
        };
        let command = format!(
            "{oid} {} refs/heads/limit-{index}{capabilities}\n",
            "0".repeat(40)
        );
        request.extend_from_slice(format!("{:04x}{command}", command.len() + 4).as_bytes());
    }
    request.extend_from_slice(b"0000");
    let push_id = uuid::Uuid::new_v4().to_string();
    let post = || {
        client
            .post(format!("{url}/git-receive-pack"))
            .bearer_auth("local-test-token")
            .header("Content-Type", "application/x-git-receive-pack-request")
            .header("Idempotency-Key", &push_id)
            .body(request.clone())
    };
    let report = post().send().await?.error_for_status()?.bytes().await?;
    assert!(
        report
            .windows(b"Canopy push command limit exceeded".len())
            .any(|part| part == b"Canopy push command limit exceeded")
    );
    assert!(
        report
            .windows(b"pre-receive hook declined".len())
            .any(|part| part == b"pre-receive hook declined")
    );
    assert_eq!(
        post().send().await?.error_for_status()?.bytes().await?,
        report
    );
    assert_eq!(generation(&client, &api).await?, before + 3);
    restored.shutdown().await?;
    Ok(())
}
