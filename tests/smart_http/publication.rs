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
    store: &PausedBlobs,
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
    for kind in ["refs", "policy", "access", "no-report"] {
        tokio::fs::write(
            local.join("large"),
            vec![kind.as_bytes()[0]; 2 * 1024 * 1024],
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
            "policy" => "report-status-v2 side-band-64k",
            "access" => "report-status",
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
        let mut child = if kind == "refs" {
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
        } else {
            repository
                .revoke_member(support::identity()?, "canopy", "late-writer")
                .await?;
        }
        let generation = repository.default_branch(None).await?.output.generation;
        store.proceed.notify_one();
        if let Some(child) = child.take() {
            let output = child.wait_with_output().await?;
            let stderr = String::from_utf8_lossy(&output.stderr);
            assert!(!output.status.success(), "{stderr}");
            assert!(
                stderr.contains("[remote rejected]")
                    && stderr.contains("Canopy publication rejected"),
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
                if kind == "no-report" { 409 } else { 200 }
            );
            let report = response.bytes().await?;
            let text = String::from_utf8_lossy(&report);
            assert!(text.contains("Canopy publication rejected"), "{text}");
            if kind != "no-report" {
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
            // decision must still reject even after permission/policy is restored.
            tokio::fs::create_dir_all(root.join("replay")).await?;
            let gateway = GitGateway::new(
                Arc::clone(repository),
                root.join("replay"),
                Arc::new(InMemory::new()),
                DiskBudget::new(1 << 30),
            );
            let replay = gateway
                .handle(
                    canopy_server::git_http::GitHttpRequest {
                        method: "POST".into(),
                        path_info: "/repo.git/git-receive-pack".into(),
                        query: String::new(),
                        content_type: Some("application/x-git-receive-pack-request".into()),
                        gzip: false,
                        protocol_v2: false,
                        authenticated: true,
                        body: Body::from(body.clone()),
                    },
                    "late-writer",
                    Some(uuid::Uuid::parse_str(&id)?.into_bytes()),
                    None,
                )
                .await?;
            assert_eq!(replay.status, if kind == "no-report" { 409 } else { 200 });
            assert_eq!(axum::body::to_bytes(replay.body, 1 << 20).await?, report);
        }
        assert_eq!(
            repository.default_branch(None).await?.output.generation,
            generation
        );
        assert!(repository.ref_state(&sibling, None).await?.output.is_none());
    }
    Ok(())
}
