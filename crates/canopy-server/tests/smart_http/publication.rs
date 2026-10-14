use super::*;
use crate::paused_blobs::PausedBlobs;
use canopy_server::{PushPlan, RefUpdate, branch_rules::BranchRuleEdit};
use std::{sync::atomic::Ordering, time::Duration};

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;
const AUTH: &str = "http.extraHeader=Authorization: Bearer late-writer-token";

fn packet(line: &str) -> Vec<u8> {
    format!("{:04x}{line}", line.len() + 4).into_bytes()
}

pub async fn verify(
    root: &Path,
    repository: &Arc<RepositoryCell>,
    store: &Arc<PausedBlobs>,
    url: &str,
) -> Result {
    let local = root.join("publication-race");
    run_git(None, &["init", "-b", "main", local.to_str().ok_or("path")?]).await?;
    let current = repository
        .ref_state("refs/heads/main", None)
        .await?
        .output
        .ok_or("main")?
        .oid
        .ok_or("main oid")?;
    let client = reqwest::Client::new();
    repository
        .grant_member(
            support::identity()?,
            "canopy",
            "late-writer",
            TokenScope::Write,
        )
        .await?;
    for kind in [
        "refs",
        "policy",
        "access",
        "no-report",
        "storage",
        "storage-sideband",
        "storage-no-report",
        "storage-stock",
        "storage-race",
    ] {
        let storage = kind.starts_with("storage");
        let no_report = kind.ends_with("no-report");
        let raced = kind == "storage-race";
        let reason = if storage {
            "Canopy object ingestion failed"
        } else {
            "Canopy publication rejected"
        };
        tokio::fs::write(
            local.join("large"),
            kind.as_bytes()
                .iter()
                .copied()
                .cycle()
                .take(2 * 1024 * 1024)
                .collect::<Vec<_>>(),
        )
        .await?;
        run_git(Some(&local), &["add", "."]).await?;
        run_git(
            Some(&local),
            &[
                "-c",
                "user.name=Test",
                "-c",
                "user.email=test@example.invalid",
                "-c",
                "commit.gpgsign=false",
                "commit",
                "-m",
                kind,
            ],
        )
        .await?;
        let oid = String::from_utf8(run_git(Some(&local), &["rev-parse", "HEAD"]).await?)?
            .trim()
            .to_owned();
        let reference = format!("refs/heads/late-{kind}");
        // Sort the valid sibling first so a premature per-ref write would leak.
        let sibling = format!("refs/heads/early-{kind}");
        let id = uuid::Uuid::new_v4().to_string();
        let capabilities = match kind {
            "policy" | "storage-sideband" => "report-status-v2 side-band-64k",
            "access" | "storage" | "storage-race" => "report-status",
            _ => "",
        };
        let mut body = packet(&format!(
            "{} {oid} {reference}\0{capabilities}\n",
            "0".repeat(40)
        ));
        body.extend(packet(&format!("{} {oid} {sibling}\n", "0".repeat(40))));
        body.extend_from_slice(b"0000");
        body.extend(run_git(Some(&local), &["pack-objects", "--all", "--stdout"]).await?);
        let post = |id: &str| {
            client
                .post(format!("{url}/git-receive-pack"))
                .bearer_auth("late-writer-token")
                .header("Content-Type", "application/x-git-receive-pack-request")
                .header("Idempotency-Key", id)
                .body(body.clone())
        };
        store.armed.store(true, Ordering::SeqCst);
        let mut child = if kind == "refs" || kind == "storage-stock" {
            Some(
                Command::new("git")
                    .current_dir(&local)
                    .args([
                        "-c",
                        "credential.helper=",
                        "-c",
                        AUTH,
                        "push",
                        "--atomic",
                        url,
                        &format!("HEAD:{reference}"),
                        &format!("HEAD:{sibling}"),
                    ])
                    .env("GIT_TERMINAL_PROMPT", "0")
                    .stdout(std::process::Stdio::piped())
                    .stderr(std::process::Stdio::piped())
                    .spawn()?,
            )
        } else {
            None
        };
        let request = if child.is_none() {
            Some(tokio::spawn(post(&id).send()))
        } else {
            None
        };
        tokio::time::timeout(Duration::from_secs(15), store.entered.notified()).await?;
        if kind == "refs" || kind == "no-report" {
            repository
                .finalize_push(
                    support::identity()?,
                    PushPlan {
                        actor: "canopy".into(),
                        updates: vec![RefUpdate {
                            name: reference.clone(),
                            expected: None,
                            new_oid: Some(current),
                        }],
                    },
                )
                .await?;
        } else if kind == "policy" {
            repository
                .set_branch_rule(
                    support::identity()?,
                    "canopy",
                    BranchRuleEdit {
                        reference: reference.clone(),
                        expected_version: 0,
                        enabled: true,
                        deny_deletions: false,
                        fast_forward_only: false,
                        require_pull_request: true,
                        required_approvals: 0,
                        required_checks: vec![],
                    },
                )
                .await?;
        } else if kind == "access" {
            repository
                .revoke_member(support::identity()?, "canopy", "late-writer")
                .await?;
        }
        let generation = repository.default_branch(None).await?.output.generation;
        store.fail.store(storage, Ordering::SeqCst);
        // A different gateway can commit the same ID while this attempt is paused.
        // The losing ingestion failure must return that success, never a local ng.
        let winner = if raced {
            Some(gateway_request(root, repository, store.clone(), &body, &id).await?)
        } else {
            None
        };
        store.proceed.notify_one();
        if let Some(child) = child.take() {
            let output = child.wait_with_output().await?;
            let stderr = String::from_utf8_lossy(&output.stderr);
            assert!(!output.status.success(), "{stderr}");
            assert!(
                stderr.contains("[remote rejected]") && stderr.contains(reason),
                "{stderr}"
            );
            assert!(
                !stderr.contains("HTTP 409") && !stderr.contains("HTTP 500"),
                "{stderr}"
            );
        }
        if let Some(request) = request {
            let response = request.await??;
            assert_eq!(
                response.status().as_u16(),
                if no_report { 409 } else { 200 }
            );
            let report = response.bytes().await?;
            let text = String::from_utf8_lossy(&report);
            if let Some((status, saved)) = winner {
                assert_eq!(status, 200);
                assert_eq!(report.as_ref(), saved);
                assert!(text.contains(&format!("ok {reference}")), "{text}");
            } else {
                assert!(text.contains(reason), "{text}");
            }
            if !no_report && !raced {
                assert!(
                    text.contains(&format!("ng {reference} "))
                        && text.contains(&format!("ng {sibling} ")),
                    "{text}"
                );
                assert!(!text.contains(&format!("ok {reference}")), "{text}");
            }
            repository
                .grant_member(
                    support::identity()?,
                    "canopy",
                    "late-writer",
                    TokenScope::Write,
                )
                .await?;
            if kind == "policy" {
                repository
                    .set_branch_rule(
                        support::identity()?,
                        "canopy",
                        BranchRuleEdit {
                            reference: reference.clone(),
                            expected_version: 1,
                            enabled: false,
                            deny_deletions: false,
                            fast_forward_only: false,
                            require_pull_request: false,
                            required_approvals: 0,
                            required_checks: vec![],
                        },
                    )
                    .await?;
            }
            // Recreate the gateway with no native/object cache; the saved Cell
            // decision must survive permission/policy or storage recovery.
            let (status, saved) =
                gateway_request(root, repository, Arc::new(InMemory::new()), &body, &id).await?;
            assert_eq!(status, if no_report { 409 } else { 200 });
            assert_eq!(saved, report);
        }
        assert_eq!(
            repository.default_branch(None).await?.output.generation,
            generation + i64::from(raced)
        );
        if !raced {
            assert!(repository.ref_state(&sibling, None).await?.output.is_none());
        }
        if storage && !raced {
            let response = post(&uuid::Uuid::new_v4().to_string()).send().await?;
            assert_eq!(response.status().as_u16(), 200);
            let report = response.bytes().await?;
            if !no_report && kind != "storage-stock" {
                assert!(String::from_utf8_lossy(&report).contains(&format!("ok {reference}")));
            }
            assert_eq!(
                repository.default_branch(None).await?.output.generation,
                generation + 1
            );
        }
        if storage {
            for name in [&reference, &sibling] {
                assert_eq!(
                    hex::encode(
                        repository
                            .ref_state(name, None)
                            .await?
                            .output
                            .ok_or("missing ref")?
                            .oid
                            .ok_or("missing oid")?
                    ),
                    oid
                );
            }
        }
    }
    verify_preparation(root, repository, store, &local, current).await?;
    Ok(())
}

async fn verify_preparation(
    root: &Path,
    repository: &Arc<RepositoryCell>,
    store: &Arc<PausedBlobs>,
    source: &Path,
    current: canopy_server::ObjectId,
) -> Result {
    use sha1::{Digest as _, Sha1};
    let mut pack = b"PACK\0\0\0\x02\0\0\0\0".to_vec();
    pack.extend_from_slice(&Sha1::digest(&pack));
    for kind in ["plain", "sideband", "no-report", "stock", "race"] {
        let scratch = tempfile::tempdir_in(root)?;
        let gateway = Arc::new(GitGateway::new(
            repository.clone(),
            scratch.path().into(),
            store.clone(),
            DiskBudget::new(1 << 30),
            canopy_server::native_resources::NativeResources::default(),
        ));
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let api = Arc::new(GitHttpApi::new(
            gateway,
            "canopy".into(),
            "preparation",
            &format!("http://{address}"),
            Arc::new(|| true),
        )?);
        let (stop, stopped) = oneshot::channel::<()>();
        let serving = tokio::spawn(async move {
            axum::serve(listener, support::git_router(api))
                .with_graceful_shutdown(async move {
                    let _ = stopped.await;
                })
                .await
        });
        let url = format!("http://{address}/canopy/preparation.git");
        let reference = format!("refs/heads/準備-{kind}");
        let sibling = format!("refs/heads/preparation-sibling-{kind}");
        let capabilities = match kind {
            "sideband" => "report-status-v2 side-band-64k",
            "no-report" => "",
            _ => "report-status",
        };
        let mut body = packet(&format!(
            "{} {} {reference}\0{capabilities}\n",
            "0".repeat(40),
            hex::encode(current)
        ));
        body.extend(packet(&format!(
            "{} {} {sibling}\n",
            "0".repeat(40),
            hex::encode(current)
        )));
        body.extend_from_slice(b"0000");
        body.extend_from_slice(&pack);
        let id = uuid::Uuid::new_v4().to_string();
        let client = reqwest::Client::new();
        let post = |id: &str| {
            client
                .post(format!("{url}/git-receive-pack"))
                .bearer_auth("late-writer-token")
                .header("Content-Type", "application/x-git-receive-pack-request")
                .header("Idempotency-Key", id)
                .body(body.clone())
        };
        let generation = repository.default_branch(None).await?.output.generation;
        store.read_armed.store(true, Ordering::SeqCst);
        let child = if kind == "stock" {
            Some(
                Command::new("git")
                    .current_dir(source)
                    .args([
                        "-c",
                        "credential.helper=",
                        "-c",
                        AUTH,
                        "push",
                        "--atomic",
                        &url,
                        &format!("HEAD:{reference}"),
                        &format!("HEAD:{sibling}"),
                    ])
                    .env("GIT_TERMINAL_PROMPT", "0")
                    .stdout(std::process::Stdio::piped())
                    .stderr(std::process::Stdio::piped())
                    .kill_on_drop(true)
                    .spawn()?,
            )
        } else {
            None
        };
        let request = if child.is_none() {
            Some(tokio::spawn(post(&id).send()))
        } else {
            None
        };
        tokio::time::timeout(Duration::from_secs(15), store.entered.notified()).await?;
        let winner = if kind == "race" {
            Some(gateway_request(root, repository, store.clone(), &body, &id).await?)
        } else {
            None
        };
        store.fail.store(true, Ordering::SeqCst);
        store.proceed.notify_one();
        if let Some(child) = child {
            let output = child.wait_with_output().await?;
            let stderr = String::from_utf8_lossy(&output.stderr);
            assert!(!output.status.success(), "{stderr}");
            assert!(
                stderr.contains("Canopy push failed before publication"),
                "{stderr}"
            );
            assert!(!stderr.contains("HTTP 500"), "{stderr}");
        }
        if let Some(request) = request {
            let response = request.await??;
            let status = response.status().as_u16();
            assert_eq!(status, if kind == "no-report" { 409 } else { 200 });
            let report = response.bytes().await?;
            if let Some((status, saved)) = &winner {
                assert_eq!(*status, 200);
                assert_eq!(report.as_ref(), saved);
            } else {
                let text = String::from_utf8_lossy(&report);
                assert!(
                    text.contains("Canopy push failed before publication"),
                    "{text}"
                );
                if kind != "no-report" {
                    for name in [&reference, &sibling] {
                        assert!(text.contains(&format!("ng {name} ")), "{text}");
                    }
                }
            }
            let replay = gateway_request(root, repository, store.clone(), &body, &id).await?;
            assert_eq!(replay, (status, report.to_vec()));
        }
        if winner.is_none() {
            for name in [&reference, &sibling] {
                assert!(repository.ref_state(name, None).await?.output.is_none());
            }
            assert_eq!(
                repository.default_branch(None).await?.output.generation,
                generation
            );
            post(&uuid::Uuid::new_v4().to_string())
                .send()
                .await?
                .error_for_status()?;
        }
        assert_eq!(
            repository.default_branch(None).await?.output.generation,
            generation + 1
        );
        for name in [&reference, &sibling] {
            assert_eq!(
                repository
                    .ref_state(name, None)
                    .await?
                    .output
                    .ok_or("ref")?
                    .oid,
                Some(current)
            );
        }
        let _ = stop.send(());
        serving.await??;
    }
    Ok(())
}

async fn gateway_request(
    root: &Path,
    repository: &Arc<RepositoryCell>,
    store: Arc<dyn ObjectStore>,
    body: &[u8],
    id: &str,
) -> Result<(u16, Vec<u8>)> {
    let scratch = tempfile::tempdir_in(root)?;
    let gateway = GitGateway::new(
        Arc::clone(repository),
        scratch.path().into(),
        store,
        DiskBudget::new(1 << 30),
        canopy_server::native_resources::NativeResources::default(),
    );
    let response = gateway
        .handle(
            canopy_server::git_http::GitHttpRequest {
                method: "POST".into(),
                path_info: "/repo.git/git-receive-pack".into(),
                query: String::new(),
                content_type: Some("application/x-git-receive-pack-request".into()),
                gzip: false,
                protocol_v2: false,
                authenticated: true,
                body: Body::from(body.to_vec()),
            },
            "late-writer",
            Some(uuid::Uuid::parse_str(id)?.into_bytes()),
            None,
        )
        .await?;
    Ok((
        response.status,
        axum::body::to_bytes(response.body, 1 << 20).await?.to_vec(),
    ))
}
