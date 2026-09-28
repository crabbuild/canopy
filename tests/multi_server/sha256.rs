use super::*;

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

#[tokio::test(flavor = "multi_thread")]
async fn sha256_repository_push_clone_fetch_and_restore() -> Result {
    sha256_repository_round_trip(Arc::new(InMemory::new())).await
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "isolated RustFS qualification"]
async fn sha256_real_provider_round_trip() -> Result {
    sha256_repository_round_trip(real_provider_store()?).await
}

async fn sha256_repository_round_trip(store: Arc<dyn ObjectStore>) -> Result {
    let workspace = tempfile::TempDir::new()?;
    let first_address = available_address().await?;
    let first = CanopyServer::start(
        config(first_address, workspace.path().join("first")),
        Arc::clone(&store),
    )
    .await?;
    let response: serde_json::Value = reqwest::Client::new()
        .post(format!("http://{first_address}/api/repositories"))
        .bearer_auth("local-test-token")
        .json(&serde_json::json!({"name": "sha256", "object_format": "sha256"}))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(response["object_format"], "sha256");
    let conflicting = reqwest::Client::new()
        .post(format!("http://{first_address}/api/repositories"))
        .bearer_auth("local-test-token")
        .json(&serde_json::json!({"name": "sha256", "object_format": "sha1"}))
        .send()
        .await?;
    assert_eq!(conflicting.status(), reqwest::StatusCode::CONFLICT);

    let first_url = response["clone_url"].as_str().ok_or("clone URL missing")?;
    let source = workspace.path().join("source");
    run_git(
        None,
        &[
            "init",
            "--object-format=sha256",
            "-b",
            "main",
            path_str(&source)?,
        ],
    )
    .await?;
    run_git(Some(&source), &["config", "user.name", "Canopy Test"]).await?;
    run_git(
        Some(&source),
        &["config", "user.email", "test@example.invalid"],
    )
    .await?;
    run_git(Some(&source), &["lfs", "install", "--local"]).await?;
    run_git(Some(&source), &["lfs", "track", "*.lfs"]).await?;
    tokio::fs::write(source.join("file"), b"sha256 repository\n").await?;
    let large_body = vec![0x69; 900_000];
    tokio::fs::write(source.join("large"), &large_body).await?;
    let lfs_body = vec![0x37; 1_100_000];
    tokio::fs::write(source.join("asset.lfs"), &lfs_body).await?;
    run_git(
        Some(&source),
        &["add", ".gitattributes", "file", "large", "asset.lfs"],
    )
    .await?;
    run_git(
        Some(&source),
        &["-c", "commit.gpgsign=false", "commit", "-m", "first"],
    )
    .await?;
    run_git(
        Some(&source),
        &[
            "-c",
            "tag.gpgsign=false",
            "tag",
            "-a",
            "release",
            "-m",
            "release",
        ],
    )
    .await?;
    run_git(
        Some(&source),
        &[
            "-c",
            "http.extraHeader=Authorization: Bearer local-test-token",
            "push",
            first_url,
            "HEAD:refs/heads/main",
            "refs/tags/release",
        ],
    )
    .await?;
    let incompatible = workspace.path().join("incompatible");
    run_git(None, &["init", "-b", "main", path_str(&incompatible)?]).await?;
    run_git(Some(&incompatible), &["config", "user.name", "Canopy Test"]).await?;
    run_git(
        Some(&incompatible),
        &["config", "user.email", "test@example.invalid"],
    )
    .await?;
    tokio::fs::write(incompatible.join("file"), b"sha1 object\n").await?;
    run_git(Some(&incompatible), &["add", "file"]).await?;
    run_git(
        Some(&incompatible),
        &["-c", "commit.gpgsign=false", "commit", "-m", "incompatible"],
    )
    .await?;
    let rejected = Command::new("git")
        .arg("-C")
        .arg(&incompatible)
        .args([
            "-c",
            "http.extraHeader=Authorization: Bearer local-test-token",
            "push",
            first_url,
            "HEAD:refs/heads/main",
        ])
        .output()
        .await?;
    assert!(!rejected.status.success());
    let reason = String::from_utf8_lossy(&rejected.stderr).to_lowercase();
    assert!(reason.contains("hash algorithm") || reason.contains("object format"));
    let expected = run_git(Some(&source), &["rev-parse", "HEAD"]).await?;
    assert_eq!(expected.trim_ascii().len(), 64);
    first.shutdown().await?;

    let second_address = available_address().await?;
    let second = CanopyServer::start(
        config(second_address, workspace.path().join("second")),
        Arc::clone(&store),
    )
    .await?;
    let second_url = format!("http://{second_address}/canopy/sha256.git");
    let clone = workspace.path().join("clone");
    run_git(
        None,
        &[
            "-c",
            "http.extraHeader=Authorization: Bearer local-test-token",
            "clone",
            &second_url,
            path_str(&clone)?,
        ],
    )
    .await?;
    assert_eq!(
        run_git(Some(&clone), &["rev-parse", "HEAD"]).await?,
        expected
    );
    assert_eq!(tokio::fs::read(clone.join("large")).await?, large_body);
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
    let browse = format!("http://{second_address}/api/repositories/sha256/browse");
    let view: serde_json::Value = reqwest::Client::new()
        .post(&browse)
        .bearer_auth("local-test-token")
        .json(&serde_json::json!({"repository_id": response["repository_id"], "query": {"kind": "resolve"}}))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(
        view["view"]["resolved"]["oid"],
        String::from_utf8_lossy(expected.trim_ascii()).as_ref()
    );
    assert_eq!(
        run_git(Some(&clone), &["rev-parse", "--show-object-format"])
            .await?
            .trim_ascii(),
        b"sha256"
    );
    assert_eq!(
        run_git(Some(&clone), &["rev-parse", "refs/tags/release"])
            .await?
            .trim_ascii()
            .len(),
        64
    );
    run_git(Some(&clone), &["fsck", "--full", "--strict"]).await?;
    let partial = workspace.path().join("partial");
    run_git(
        None,
        &[
            "-c",
            "http.extraHeader=Authorization: Bearer local-test-token",
            "clone",
            "--filter=blob:none",
            "--no-checkout",
            &second_url,
            path_str(&partial)?,
        ],
    )
    .await?;
    assert_eq!(
        run_git(Some(&partial), &["rev-parse", "--show-object-format"])
            .await?
            .trim_ascii(),
        b"sha256"
    );
    tokio::fs::write(source.join("file"), b"sha256 repository updated\n").await?;
    run_git(Some(&source), &["add", "file"]).await?;
    run_git(
        Some(&source),
        &["-c", "commit.gpgsign=false", "commit", "-m", "second"],
    )
    .await?;
    run_git(
        Some(&source),
        &[
            "-c",
            "http.extraHeader=Authorization: Bearer local-test-token",
            "push",
            &second_url,
            "HEAD:refs/heads/main",
        ],
    )
    .await?;
    run_git(
        Some(&clone),
        &[
            "-c",
            "http.extraHeader=Authorization: Bearer local-test-token",
            "fetch",
            &second_url,
            "main",
        ],
    )
    .await?;
    assert_eq!(
        run_git(Some(&clone), &["rev-parse", "FETCH_HEAD"]).await?,
        run_git(Some(&source), &["rev-parse", "HEAD"]).await?
    );
    second.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn sha256_checks_reviews_and_merge_survive_restore() -> Result {
    use super::merge::{AUTH, OWNER, current, new_pull, oid, revision, value};
    use reqwest::{Client, StatusCode};
    use serde_json::json;

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let workspace = tempfile::TempDir::new()?;
    let address = available_address().await?;
    let server = CanopyServer::start(
        config(address, workspace.path().join("first")),
        Arc::clone(&store),
    )
    .await?;
    let client = Client::new();
    let created = value(
        client
            .post(format!("http://{address}/api/repositories"))
            .bearer_auth(OWNER)
            .json(&json!({"name":"sha256-review","object_format":"sha256"})),
    )
    .await?;
    let repository = &created["repository_id"];
    let url = created["clone_url"].as_str().ok_or("clone URL missing")?;
    let repo = format!("http://{address}/api/repositories/sha256-review");
    let source_dir = workspace.path().join("source");
    run_git(
        None,
        &[
            "init",
            "--object-format=sha256",
            "-b",
            "main",
            path_str(&source_dir)?,
        ],
    )
    .await?;
    run_git(Some(&source_dir), &["config", "user.name", "Canopy Test"]).await?;
    run_git(
        Some(&source_dir),
        &["config", "user.email", "test@example.invalid"],
    )
    .await?;
    run_git(
        Some(&source_dir),
        &[
            "-c",
            "commit.gpgsign=false",
            "commit",
            "--allow-empty",
            "-m",
            "Base",
        ],
    )
    .await?;
    let base = oid(&source_dir, "HEAD").await?;
    run_git(
        Some(&source_dir),
        &["-c", AUTH, "push", url, "HEAD:refs/heads/main"],
    )
    .await?;
    tokio::fs::write(
        source_dir.join("reviewed.txt"),
        b"SHA-256 reviewed change\n",
    )
    .await?;
    run_git(Some(&source_dir), &["add", "reviewed.txt"]).await?;
    run_git(
        Some(&source_dir),
        &["-c", "commit.gpgsign=false", "commit", "-m", "Feature"],
    )
    .await?;
    let source = oid(&source_dir, "HEAD").await?;
    assert_eq!(source.len(), 64);
    run_git(
        Some(&source_dir),
        &["-c", AUTH, "push", url, "HEAD:refs/heads/feature"],
    )
    .await?;

    let api = new_pull(
        &client,
        &repo,
        repository,
        "refs/heads/feature",
        &source,
        &base,
    )
    .await?;
    let reviewer = format!("cnp_{}", "81".repeat(32));
    value(
        client
            .post(format!("http://{address}/api/accounts"))
            .bearer_auth(OWNER)
            .json(&json!({"name":"reviewer","token":reviewer,"scope":"write"})),
    )
    .await?;
    value(
        client
            .put(format!("{repo}/collaborators/reviewer"))
            .bearer_auth(OWNER)
            .json(&json!({"role":"write"})),
    )
    .await?;
    let response = client
        .put(format!("{repo}/check-contexts/unit"))
        .bearer_auth(OWNER)
        .json(&json!({"repository_id":repository,"expected_version":0,"enabled":true,"reporter":"canopy"}))
        .send()
        .await?;
    assert_eq!(
        response.status(),
        StatusCode::NO_CONTENT,
        "{}",
        response.text().await?
    );
    let rule = json!({"repository_id":repository,"rule":{"reference":"refs/heads/main","expected_version":0,"enabled":true,"deny_deletions":false,"fast_forward_only":true,"required_checks":["unit"],"require_pull_request":true,"required_approvals":1}});
    let response = client
        .put(format!("{repo}/branch-rules"))
        .bearer_auth(OWNER)
        .json(&rule)
        .send()
        .await?;
    assert_eq!(
        response.status(),
        StatusCode::NO_CONTENT,
        "{}",
        response.text().await?
    );
    let intent = json!({"repository_id":repository,"id":uuid::Uuid::new_v4().to_string(),"revision":revision(&current(&client,&api).await?),"strategy":"fast_forward"});
    let merge_api = format!("{api}/merge");
    let response = client
        .post(&merge_api)
        .bearer_auth(OWNER)
        .json(&intent)
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::CONFLICT);
    let check_id = uuid::Uuid::new_v4().to_string();
    value(client.post(format!("{repo}/commits/{source}/checks")).bearer_auth(OWNER).json(&json!({"repository_id":repository,"id":check_id,"context":"unit","context_version":1}))).await?;
    let response = client.put(format!("{repo}/checks/{check_id}")).bearer_auth(OWNER).json(&json!({"repository_id":repository,"expected_version":1,"state":"success","summary":"Passed"})).send().await?;
    assert_eq!(
        response.status(),
        StatusCode::NO_CONTENT,
        "{}",
        response.text().await?
    );
    let checks = value(
        client
            .get(format!("{repo}/commits/{source}/checks"))
            .bearer_auth(OWNER),
    )
    .await?;
    assert_eq!(checks["checks"][0]["run"]["state"], "success");
    let response = client
        .post(&merge_api)
        .bearer_auth(OWNER)
        .json(&intent)
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::CONFLICT);
    value(client.post(format!("{api}/reviews")).bearer_auth(&reviewer).json(&json!({"repository_id":repository,"id":uuid::Uuid::new_v4().to_string(),"revision":revision(&current(&client,&api).await?),"kind":"approve","body":"Approved"}))).await?;
    let merged = value(client.post(&merge_api).bearer_auth(OWNER).json(&intent)).await?;
    assert_eq!(merged["merge"]["oid"], source);
    server.shutdown().await?;

    let restored_address = available_address().await?;
    let restored = CanopyServer::start(
        config(restored_address, workspace.path().join("restored")),
        store,
    )
    .await?;
    let restored_repo = format!("http://{restored_address}/api/repositories/sha256-review");
    let restored_api = format!("{restored_repo}/pulls/1");
    assert_eq!(
        current(&client, &restored_api).await?["merge"],
        merged["merge"]
    );
    let checks = value(
        client
            .get(format!("{restored_repo}/commits/{source}/checks"))
            .bearer_auth(OWNER),
    )
    .await?;
    assert_eq!(checks["checks"][0]["run"]["state"], "success");
    let clone = workspace.path().join("clone");
    run_git(
        None,
        &[
            "-c",
            AUTH,
            "clone",
            &format!("http://{restored_address}/canopy/sha256-review.git"),
            path_str(&clone)?,
        ],
    )
    .await?;
    assert_eq!(oid(&clone, "HEAD").await?, source);
    assert_eq!(
        tokio::fs::read(clone.join("reviewed.txt")).await?,
        b"SHA-256 reviewed change\n"
    );
    run_git(Some(&clone), &["fsck", "--full", "--strict"]).await?;
    restored.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn sha256_native_merge_candidates_survive_restore_and_publish() -> Result {
    sha256_native_merge_candidates(Arc::new(InMemory::new())).await
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "isolated RustFS qualification"]
async fn sha256_real_provider_native_merge_candidates() -> Result {
    sha256_native_merge_candidates(real_provider_store()?).await
}

async fn sha256_native_merge_candidates(store: Arc<dyn ObjectStore>) -> Result {
    use super::merge::{AUTH, OWNER, current, new_pull, oid, revision, value};
    use reqwest::Client;
    use serde_json::json;

    let workspace = tempfile::TempDir::new()?;
    let address = available_address().await?;
    let server = CanopyServer::start(
        config(address, workspace.path().join("first")),
        Arc::clone(&store),
    )
    .await?;
    let client = Client::new();
    let mut prepared = Vec::new();
    for strategy in ["merge_commit", "squash", "rebase"] {
        let name = format!("sha256-{strategy}");
        let created = value(
            client
                .post(format!("http://{address}/api/repositories"))
                .bearer_auth(OWNER)
                .json(&json!({"name":name,"object_format":"sha256"})),
        )
        .await?;
        let repository = created["repository_id"].clone();
        let url = created["clone_url"].as_str().ok_or("clone URL missing")?;
        let repo = format!("http://{address}/api/repositories/{name}");
        let local = workspace.path().join(&name);
        run_git(
            None,
            &[
                "init",
                "--object-format=sha256",
                "-b",
                "main",
                path_str(&local)?,
            ],
        )
        .await?;
        run_git(Some(&local), &["config", "user.name", "Canopy Test"]).await?;
        run_git(
            Some(&local),
            &["config", "user.email", "test@example.invalid"],
        )
        .await?;
        tokio::fs::write(local.join("shared"), b"common\n").await?;
        run_git(Some(&local), &["add", "shared"]).await?;
        run_git(
            Some(&local),
            &["-c", "commit.gpgsign=false", "commit", "-m", "Common"],
        )
        .await?;
        run_git(Some(&local), &["checkout", "-b", "feature"]).await?;
        tokio::fs::write(local.join("feature"), b"feature change\n").await?;
        run_git(Some(&local), &["add", "feature"]).await?;
        run_git(
            Some(&local),
            &["-c", "commit.gpgsign=false", "commit", "-m", "Feature"],
        )
        .await?;
        let source = oid(&local, "HEAD").await?;
        run_git(Some(&local), &["checkout", "main"]).await?;
        tokio::fs::write(local.join("base"), b"base change\n").await?;
        run_git(Some(&local), &["add", "base"]).await?;
        run_git(
            Some(&local),
            &["-c", "commit.gpgsign=false", "commit", "-m", "Base"],
        )
        .await?;
        let base = oid(&local, "HEAD").await?;
        run_git(
            Some(&local),
            &[
                "-c",
                AUTH,
                "push",
                url,
                "HEAD:refs/heads/main",
                &format!("{source}:refs/heads/feature"),
            ],
        )
        .await?;
        let api = new_pull(
            &client,
            &repo,
            &repository,
            "refs/heads/feature",
            &source,
            &base,
        )
        .await?;
        let request = json!({"repository_id":repository,"id":uuid::Uuid::new_v4().to_string(),"revision":revision(&current(&client,&api).await?),"strategy":strategy,"message":if strategy == "rebase" { "" } else { "Reviewed merge" }});
        let candidate = value(
            client
                .post(format!("{api}/merge-candidates"))
                .bearer_auth(OWNER)
                .json(&request),
        )
        .await?;
        assert_eq!(candidate["candidate"]["result"]["state"], "ready");
        let commit = candidate["candidate"]["result"]["oid"]
            .as_str()
            .ok_or("candidate OID missing")?;
        assert_eq!(commit.len(), 64);
        let fetch_ref = candidate["fetch_ref"]
            .as_str()
            .ok_or("candidate ref missing")?;
        run_git(Some(&local), &["-c", AUTH, "fetch", url, fetch_ref]).await?;
        assert_eq!(oid(&local, "FETCH_HEAD").await?, commit);
        let parents = String::from_utf8(
            run_git(Some(&local), &["show", "-s", "--format=%P", commit]).await?,
        )?;
        assert_eq!(
            parents.trim(),
            if strategy == "merge_commit" {
                format!("{base} {source}")
            } else {
                base
            }
        );
        prepared.push((name, repository, request, candidate));
    }
    server.shutdown().await?;

    let restored_address = available_address().await?;
    let restored = CanopyServer::start(
        config(restored_address, workspace.path().join("restored")),
        store,
    )
    .await?;
    for (name, repository, request, candidate) in prepared {
        let api = format!("http://{restored_address}/api/repositories/{name}/pulls/1");
        let saved = value(
            client
                .get(format!(
                    "{api}/merge-candidates/{}",
                    request["id"].as_str().ok_or("candidate UUID missing")?
                ))
                .bearer_auth(OWNER),
        )
        .await?;
        assert_eq!(saved, candidate);
        let merge = json!({"repository_id":repository,"id":uuid::Uuid::new_v4().to_string(),"revision":request["revision"],"strategy":request["strategy"],"candidate_id":request["id"]});
        let merged = value(
            client
                .post(format!("{api}/merge"))
                .bearer_auth(OWNER)
                .json(&merge),
        )
        .await?;
        assert_eq!(
            merged["merge"]["oid"],
            candidate["candidate"]["result"]["oid"]
        );
        let clone = workspace.path().join(format!("clone-{name}"));
        run_git(
            None,
            &[
                "-c",
                AUTH,
                "clone",
                &format!("http://{restored_address}/canopy/{name}.git"),
                path_str(&clone)?,
            ],
        )
        .await?;
        assert_eq!(oid(&clone, "HEAD").await?, merged["merge"]["oid"]);
        assert_eq!(tokio::fs::read(clone.join("base")).await?, b"base change\n");
        assert_eq!(
            tokio::fs::read(clone.join("feature")).await?,
            b"feature change\n"
        );
        run_git(Some(&clone), &["fsck", "--full", "--strict"]).await?;
    }
    restored.shutdown().await?;
    Ok(())
}
