use super::merge::{
    AUTH, OWNER, Result, current, init, new_pull, oid, push, revision, status, value,
};
use super::*;
use reqwest::{Client, StatusCode};
use serde_json::{Value, json};

async fn file(local: &Path, path: &str, body: &[u8], message: &str) -> Result {
    tokio::fs::write(local.join(path), body).await?;
    run_git(Some(local), &["add", "."]).await?;
    run_git(Some(local), &["commit", "-m", message]).await?;
    Ok(())
}
async fn check(client: &Client, repo: &str, repository: &Value, oid: &str) -> Result {
    let id = uuid::Uuid::new_v4().to_string();
    value(
        client
            .post(format!("{repo}/commits/{oid}/checks"))
            .bearer_auth(OWNER)
            .json(
                &json!({"repository_id":repository,"id":id,"context":"unit","context_version":1}),
            ),
    )
    .await?;
    status(client.put(format!("{repo}/checks/{id}")).bearer_auth(OWNER).json(&json!({"repository_id":repository,"expected_version":1,"state":"success","summary":"Candidate tested"})),StatusCode::NO_CONTENT).await
}

#[tokio::test(flavor = "multi_thread")]
async fn native_candidates_are_fetchable_checked_and_recover_before_atomic_merge() -> Result {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();
    let workspace = tempfile::TempDir::new()?;
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let address = available_address().await?;
    let server = CanopyServer::start(
        config(address, workspace.path().join("node")),
        Arc::clone(&store),
    )
    .await?;
    let client = Client::new();
    let reviewer = format!("cnp_{}", "91".repeat(32));
    value(
        client
            .post(format!("http://{address}/api/accounts"))
            .bearer_auth(OWNER)
            .json(&json!({"name":"reviewer","token":reviewer,"scope":"write"})),
    )
    .await?;
    let mut saved = Vec::new();
    for strategy in ["merge_commit", "squash"] {
        let url = create_repository(address, strategy).await?;
        let repo = format!("http://{address}/api/repositories/{strategy}");
        let repository =
            value(client.get(&repo).bearer_auth(OWNER)).await?["repository_id"].clone();
        value(
            client
                .put(format!("{repo}/collaborators/reviewer"))
                .bearer_auth(OWNER)
                .json(&json!({"role":"write"})),
        )
        .await?;
        let local = workspace.path().join(strategy);
        init(&local).await?;
        file(
            &local,
            "shared.txt",
            b"one\ntwo\nthree\nfour\nfive\nsix\nseven\n",
            "Shared ancestor",
        )
        .await?;
        file(&local, "old.txt", b"rename content\n", "Rename source").await?;
        // Exceed the transport streaming threshold while retaining text merges.
        let middle = "unchanged line\n".repeat(600000);
        file(
            &local,
            "large.txt",
            format!("start\n{middle}end\n").as_bytes(),
            "Large ancestor",
        )
        .await?;
        let common = oid(&local, "HEAD").await?;
        file(
            &local,
            "shared.txt",
            b"ONE\ntwo\nthree\nfour\nfive\nsix\nseven\n",
            "Base edit",
        )
        .await?;
        file(
            &local,
            "large.txt",
            format!("START\n{middle}end\n").as_bytes(),
            "Large base edit",
        )
        .await?;
        let base = oid(&local, "HEAD").await?;
        run_git(Some(&local), &["checkout", "-b", "feature", &common]).await?;
        run_git(Some(&local), &["mv", "old.txt", "renamed.txt"]).await?;
        file(&local, "binary.dat", b"\0\x01\xff", "Binary and rename").await?;
        file(
            &local,
            "shared.txt",
            b"one\ntwo\nthree\nfour\nfive\nsix\nSEVEN\n",
            "Source edit",
        )
        .await?;
        file(
            &local,
            "large.txt",
            format!("start\n{middle}END\n").as_bytes(),
            "Large source edit",
        )
        .await?;
        let source = oid(&local, "HEAD").await?;
        push(
            &local,
            &url,
            &[
                &format!("{base}:refs/heads/main"),
                "HEAD:refs/heads/feature",
            ],
            true,
        )
        .await?;
        // Even before any ready candidate exists, the whole namespace is reserved.
        push(
            &local,
            &url,
            &["HEAD:refs/canopy", "HEAD:refs/heads/allowed"],
            false,
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
        let rev = revision(&current(&client, &api).await?);
        status(client.put(format!("{repo}/check-contexts/unit")).bearer_auth(OWNER).json(&json!({"repository_id":repository,"expected_version":0,"enabled":true,"reporter":"canopy"})),StatusCode::NO_CONTENT).await?;
        status(client.put(format!("{repo}/branch-rules")).bearer_auth(OWNER).json(&json!({"repository_id":repository,"rule":{"reference":"refs/heads/main","expected_version":0,"enabled":true,"deny_deletions":true,"fast_forward_only":true,"required_checks":["unit"],"require_pull_request":true,"required_approvals":1}})),StatusCode::NO_CONTENT).await?;
        check(&client, &repo, &repository, &source).await?;
        let request = json!({"repository_id":repository,"id":uuid::Uuid::new_v4().to_string(),"revision":rev,"strategy":strategy,"message":"Reviewed native merge\n\nKeeps both changes."});
        let endpoint = format!("{api}/merge-candidates");
        status(
            client.post(&endpoint).json(&request),
            StatusCode::UNAUTHORIZED,
        )
        .await?;
        let mut invalid = request.clone();
        invalid["strategy"] = json!("fast_forward");
        status(
            client.post(&endpoint).bearer_auth(OWNER).json(&invalid),
            StatusCode::UNPROCESSABLE_ENTITY,
        )
        .await?;
        invalid = request.clone();
        invalid["repository_id"] = json!(uuid::Uuid::new_v4().to_string());
        status(
            client.post(&endpoint).bearer_auth(OWNER).json(&invalid),
            StatusCode::CONFLICT,
        )
        .await?;
        let body = serde_json::to_vec(&request)?;
        let paused = super::tokens::paused_upload(
            address,
            &format!("/api/repositories/{strategy}/pulls/1/merge-candidates"),
            &reviewer,
            body.len(),
        )
        .await?;
        status(
            client
                .delete(format!("{repo}/collaborators/reviewer"))
                .bearer_auth(OWNER),
            StatusCode::NO_CONTENT,
        )
        .await?;
        super::tokens::finish_upload(paused, &body, 404).await?;
        value(
            client
                .put(format!("{repo}/collaborators/reviewer"))
                .bearer_auth(OWNER)
                .json(&json!({"role":"read"})),
        )
        .await?;
        status(
            client.post(&endpoint).bearer_auth(&reviewer).json(&request),
            StatusCode::FORBIDDEN,
        )
        .await?;

        let (a, b) = tokio::join!(
            value(client.post(&endpoint).bearer_auth(OWNER).json(&request)),
            value(client.post(&endpoint).bearer_auth(OWNER).json(&request))
        );
        let candidate = a?;
        assert_eq!(candidate, b?);
        assert_eq!(
            value(
                client
                    .get(format!(
                        "{endpoint}/{}",
                        request["id"].as_str().ok_or("id")?
                    ))
                    .bearer_auth(&reviewer)
            )
            .await?,
            candidate
        );
        value(
            client
                .put(format!("{repo}/collaborators/reviewer"))
                .bearer_auth(OWNER)
                .json(&json!({"role":"write"})),
        )
        .await?;
        status(
            client.post(&endpoint).bearer_auth(&reviewer).json(&request),
            StatusCode::CONFLICT,
        )
        .await?;

        assert_eq!(candidate["repository_id"], repository);
        assert_eq!(candidate["candidate"]["result"]["state"], "ready");
        let commit = candidate["candidate"]["result"]["oid"]
            .as_str()
            .ok_or("candidate OID")?;
        let fetch_ref = candidate["fetch_ref"].as_str().ok_or("candidate ref")?;
        let native = run_git(
            Some(&local),
            &["merge-tree", "--write-tree", &base, &source],
        )
        .await?;
        assert_eq!(
            candidate["candidate"]["result"]["tree_oid"],
            String::from_utf8(native)?.trim()
        );
        run_git(Some(&local), &["-c", AUTH, "fetch", &url, fetch_ref]).await?;
        assert_eq!(oid(&local, "FETCH_HEAD").await?, commit);
        assert_eq!(
            run_git(Some(&local), &["show", &format!("{commit}:large.txt")]).await?,
            format!("START\n{middle}END\n").as_bytes()
        );
        let parents = String::from_utf8(
            run_git(Some(&local), &["show", "-s", "--format=%P", commit]).await?,
        )?;
        assert_eq!(
            parents.trim(),
            if strategy == "merge_commit" {
                format!("{base} {source}")
            } else {
                base.clone()
            }
        );
        assert_eq!(
            run_git(Some(&local), &["show", &format!("{commit}:shared.txt")]).await?,
            b"ONE\ntwo\nthree\nfour\nfive\nsix\nSEVEN\n"
        );
        assert_eq!(
            run_git(Some(&local), &["show", &format!("{commit}:binary.dat")]).await?,
            b"\0\x01\xff"
        );
        push(
            &local,
            &url,
            &[&format!("+HEAD:{fetch_ref}"), "HEAD:refs/heads/mixed"],
            false,
        )
        .await?;
        push(&local, &url, &[&format!(":{fetch_ref}")], false).await?;
        push(
            &local,
            &url,
            &[
                "--atomic",
                &format!("+HEAD:{fetch_ref}"),
                "HEAD:refs/heads/blocked-sibling",
            ],
            false,
        )
        .await?;
        let advertised =
            String::from_utf8(run_git(None, &["-c", AUTH, "ls-remote", "--refs", &url]).await?)?;
        assert!(advertised.contains(&format!("{commit}\t{fetch_ref}\n")));
        assert!(advertised.contains(&format!("{source}\trefs/heads/mixed\n")));
        assert!(!advertised.contains("refs/heads/blocked-sibling"));

        let mut changed = request.clone();
        changed["message"] = json!("different intent");
        status(
            client.post(&endpoint).bearer_auth(OWNER).json(&changed),
            StatusCode::CONFLICT,
        )
        .await?;
        let merge = json!({"repository_id":repository,"id":uuid::Uuid::new_v4().to_string(),"revision":rev,"strategy":strategy,"candidate_id":request["id"]});
        status(
            client
                .post(format!("{api}/merge"))
                .bearer_auth(OWNER)
                .json(&merge),
            StatusCode::CONFLICT,
        )
        .await?;
        value(client.post(format!("{api}/reviews")).bearer_auth(&reviewer).json(&json!({"repository_id":repository,"id":uuid::Uuid::new_v4().to_string(),"revision":rev,"kind":"approve","body":"Reviewed before candidate publication"}))).await?;
        // Success on the source is insufficient: checks must name the candidate.
        status(
            client
                .post(format!("{api}/merge"))
                .bearer_auth(OWNER)
                .json(&merge),
            StatusCode::CONFLICT,
        )
        .await?;
        check(&client, &repo, &repository, commit).await?;
        assert_eq!(current(&client, &api).await?["base"]["oid"], base);
        saved.push((strategy, repository, request, candidate, merge));
    }
    server.shutdown().await?;
    let address = available_address().await?;
    let server =
        CanopyServer::start(config(address, workspace.path().join("restored")), store).await?;
    for (strategy, _repository, request, candidate, merge) in saved {
        let api = format!("http://{address}/api/repositories/{strategy}/pulls/1");
        let endpoint = format!("{api}/merge-candidates");
        assert_eq!(
            value(
                client
                    .get(format!(
                        "{endpoint}/{}",
                        request["id"].as_str().ok_or("id")?
                    ))
                    .bearer_auth(OWNER)
            )
            .await?,
            candidate
        );
        assert_eq!(
            value(client.post(&endpoint).bearer_auth(OWNER).json(&request)).await?,
            candidate
        );
        let result = value(
            client
                .post(format!("{api}/merge"))
                .bearer_auth(OWNER)
                .json(&merge),
        )
        .await?;
        assert_eq!(
            result["merge"]["oid"],
            candidate["candidate"]["result"]["oid"]
        );
        assert_eq!(
            value(
                client
                    .post(format!("{api}/merge"))
                    .bearer_auth(OWNER)
                    .json(&merge)
            )
            .await?,
            result
        );
        let clone = workspace.path().join(format!("clone-{strategy}"));
        run_git(
            None,
            &[
                "-c",
                AUTH,
                "clone",
                &format!("http://{address}/canopy/{strategy}.git"),
                path_str(&clone)?,
            ],
        )
        .await?;
        assert_eq!(oid(&clone, "HEAD").await?, result["merge"]["oid"]);
        run_git(Some(&clone), &["fsck", "--strict"]).await?;
        assert_eq!(
            tokio::fs::read(clone.join("renamed.txt")).await?,
            b"rename content\n"
        );
    }
    server.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn conflicts_and_unrelated_histories_never_publish_and_stale_candidates_cannot_merge()
-> Result {
    use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
    let workspace = tempfile::TempDir::new()?;
    let address = available_address().await?;
    let server = CanopyServer::start(
        config(address, workspace.path().join("node")),
        Arc::new(InMemory::new()),
    )
    .await?;
    let url = create_repository(address, "conflicts").await?;
    let repo = format!("http://{address}/api/repositories/conflicts");
    let client = Client::new();
    let repository = value(client.get(&repo).bearer_auth(OWNER)).await?["repository_id"].clone();
    let local = workspace.path().join("local");
    init(&local).await?;
    file(&local, "conflict.txt", b"ancestor\n", "Common").await?;
    let common = oid(&local, "HEAD").await?;
    file(&local, "conflict.txt", b"base\n", "Base").await?;
    let base = oid(&local, "HEAD").await?;
    run_git(Some(&local), &["checkout", "-b", "source", &common]).await?;
    file(&local, "conflict.txt", b"source\n", "Source").await?;
    let source = oid(&local, "HEAD").await?;
    push(
        &local,
        &url,
        &[&format!("{base}:refs/heads/main"), "HEAD:refs/heads/source"],
        true,
    )
    .await?;
    let api = new_pull(
        &client,
        &repo,
        &repository,
        "refs/heads/source",
        &source,
        &base,
    )
    .await?;
    let initial = current(&client, &api).await?;
    let input = json!({"repository_id":repository,"id":uuid::Uuid::new_v4().to_string(),"revision":revision(&initial),"strategy":"merge_commit","message":"Conflicted merge"});
    let endpoint = format!("{api}/merge-candidates");
    let candidate = value(client.post(&endpoint).bearer_auth(OWNER).json(&input)).await?;
    assert_eq!(
        candidate["candidate"]["result"],
        json!({"state":"conflicted","paths_base64":[URL_SAFE_NO_PAD.encode(b"conflict.txt")]})
    );
    assert_eq!(candidate["fetch_ref"], Value::Null);
    assert_eq!(current(&client, &api).await?, initial);
    assert_eq!(
        value(client.post(&endpoint).bearer_auth(OWNER).json(&input)).await?,
        candidate
    );
    let merge = json!({"repository_id":repository,"id":uuid::Uuid::new_v4().to_string(),"revision":revision(&initial),"strategy":"merge_commit","candidate_id":input["id"]});
    status(
        client
            .post(format!("{api}/merge"))
            .bearer_auth(OWNER)
            .json(&merge),
        StatusCode::CONFLICT,
    )
    .await?;
    run_git(Some(&local), &["checkout", "--orphan", "unrelated"]).await?;
    run_git(
        Some(&local),
        &["commit", "--allow-empty", "-m", "Unrelated"],
    )
    .await?;
    let unrelated = oid(&local, "HEAD").await?;
    push(&local, &url, &["HEAD:refs/heads/unrelated"], true).await?;
    let other = new_pull(
        &client,
        &repo,
        &repository,
        "refs/heads/unrelated",
        &unrelated,
        &base,
    )
    .await?;
    let other_initial = current(&client, &other).await?;
    let request = json!({"repository_id":repository,"id":uuid::Uuid::new_v4().to_string(),"revision":revision(&other_initial),"strategy":"squash","message":"Unrelated squash"});
    let unrelated = value(
        client
            .post(format!("{other}/merge-candidates"))
            .bearer_auth(OWNER)
            .json(&request),
    )
    .await?;
    assert_eq!(
        unrelated["candidate"]["result"],
        json!({"state":"unrelated"})
    );
    assert_eq!(current(&client, &other).await?, other_initial);
    // A new source resolves the conflict, but the old candidate remains an
    // immutable record of the prior input. It cannot be relabelled or published.
    run_git(Some(&local), &["checkout", "source"]).await?;
    file(&local, "conflict.txt", b"ancestor\n", "Resolve source").await?;
    push(&local, &url, &["HEAD:refs/heads/source"], true).await?;
    let mut ready_input = input.clone();
    ready_input["id"] = json!(uuid::Uuid::new_v4().to_string());
    ready_input["revision"] = revision(&current(&client, &api).await?);
    let ready = value(client.post(&endpoint).bearer_auth(OWNER).json(&ready_input)).await?;
    assert_eq!(ready["candidate"]["result"]["state"], "ready");
    let mut ready_merge = merge.clone();
    ready_merge["id"] = json!(uuid::Uuid::new_v4().to_string());
    ready_merge["revision"] = ready_input["revision"].clone();
    ready_merge["candidate_id"] = ready_input["id"].clone();
    let mut wrong = ready_merge.clone();
    wrong["strategy"] = json!("squash");
    status(
        client
            .post(format!("{api}/merge"))
            .bearer_auth(OWNER)
            .json(&wrong),
        StatusCode::CONFLICT,
    )
    .await?;
    status(
        client
            .post(format!("{other}/merge"))
            .bearer_auth(OWNER)
            .json(&ready_merge),
        StatusCode::CONFLICT,
    )
    .await?;
    let same = oid(&local, "HEAD").await?;
    file(&local, "later.txt", b"later\n", "Source moved").await?;
    push(&local, &url, &["HEAD:refs/heads/source"], true).await?;
    push(&local, &url, &[&format!("+{same}:refs/heads/source")], true).await?;
    status(
        client
            .post(format!("{api}/merge"))
            .bearer_auth(OWNER)
            .json(&ready_merge),
        StatusCode::CONFLICT,
    )
    .await?;
    // Readable and exact-retryable does not imply still eligible to publish.
    assert_eq!(
        value(client.post(&endpoint).bearer_auth(OWNER).json(&ready_input)).await?,
        ready
    );
    let mut rebind = ready_merge;
    rebind["revision"] = revision(&current(&client, &api).await?);
    status(
        client
            .post(format!("{api}/merge"))
            .bearer_auth(OWNER)
            .json(&rebind),
        StatusCode::CONFLICT,
    )
    .await?;
    assert_eq!(current(&client, &api).await?["base"]["oid"], base);
    server.shutdown().await?;
    Ok(())
}

async fn git_input(local: &Path, args: &[&str], bytes: &[u8]) -> Result<Vec<u8>> {
    use tokio::io::AsyncWriteExt;
    let mut child = Command::new("git")
        .current_dir(local)
        .args(args)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()?;
    child
        .stdin
        .take()
        .ok_or("stdin missing")?
        .write_all(bytes)
        .await?;
    let output = child.wait_with_output().await?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(output.stdout)
}
async fn commit(local: &Path, tree: &str, parents: &[&str], message: &str) -> Result<String> {
    let mut args = vec!["commit-tree", tree, "-m", message];
    for parent in parents {
        args.extend(["-p", *parent]);
    }
    Ok(String::from_utf8(run_git(Some(local), &args).await?)?
        .trim()
        .into())
}
#[tokio::test(flavor = "multi_thread")]
async fn candidate_native_graph_and_paths_preserve_git_semantics() -> Result {
    use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
    let workspace = tempfile::TempDir::new()?;
    let address = available_address().await?;
    let server = CanopyServer::start(
        config(address, workspace.path().join("node")),
        Arc::new(InMemory::new()),
    )
    .await?;
    let url = create_repository(address, "native").await?;
    let repo = format!("http://{address}/api/repositories/native");
    let client = Client::new();
    let repository = value(client.get(&repo).bearer_auth(OWNER)).await?["repository_id"].clone();
    let local = workspace.path().join("local");
    init(&local).await?;
    let root = oid(&local, "HEAD").await?;
    let empty = oid(&local, "HEAD^{tree}").await?;
    let path = b"raw-\xff\nname";
    let mut tips = Vec::new();
    for text in ["left", "right"] {
        let blob = String::from_utf8(
            git_input(&local, &["hash-object", "-w", "--stdin"], text.as_bytes()).await?,
        )?;
        let mut record = format!("100644 blob {}\t", blob.trim()).into_bytes();
        record.extend_from_slice(path);
        record.push(0);
        let tree = String::from_utf8(git_input(&local, &["mktree", "-z"], &record).await?)?;
        tips.push(commit(&local, tree.trim(), &[&root], text).await?);
    }
    push(
        &local,
        &url,
        &[
            &format!("{}:refs/heads/main", tips[0]),
            &format!("{}:refs/heads/feature", tips[1]),
        ],
        true,
    )
    .await?;
    let api = new_pull(
        &client,
        &repo,
        &repository,
        "refs/heads/feature",
        &tips[1],
        &tips[0],
    )
    .await?;
    let request = json!({"repository_id":repository,"id":uuid::Uuid::new_v4().to_string(),"revision":revision(&current(&client,&api).await?),"strategy":"merge_commit","message":"Raw path conflict"});
    let candidate = value(
        client
            .post(format!("{api}/merge-candidates"))
            .bearer_auth(OWNER)
            .json(&request),
    )
    .await?;
    assert_eq!(
        candidate["candidate"]["result"],
        json!({"state":"conflicted","paths_base64":[URL_SAFE_NO_PAD.encode(path)]})
    );
    // Two best merge bases must be consolidated by native merge-tree.
    let a = commit(&local, &empty, &[&root], "a").await?;
    let b = commit(&local, &empty, &[&root], "b").await?;
    let left = commit(&local, &empty, &[&a, &b], "left merge").await?;
    let right = commit(&local, &empty, &[&b, &a], "right merge").await?;
    let bases =
        String::from_utf8(run_git(Some(&local), &["merge-base", "--all", &left, &right]).await?)?;
    assert_eq!(bases.lines().count(), 2);
    push(
        &local,
        &url,
        &[
            &format!("+{left}:refs/heads/main"),
            &format!("+{right}:refs/heads/feature"),
        ],
        true,
    )
    .await?;
    let mut request = request;
    request["id"] = json!(uuid::Uuid::new_v4().to_string());
    request["revision"] = revision(&current(&client, &api).await?);
    let candidate = value(
        client
            .post(format!("{api}/merge-candidates"))
            .bearer_auth(OWNER)
            .json(&request),
    )
    .await?;
    assert_eq!(candidate["candidate"]["result"]["state"], "ready");
    assert_eq!(candidate["candidate"]["result"]["tree_oid"], empty);
    server.shutdown().await?;
    Ok(())
}
