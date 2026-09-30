use super::*;

fn unified(name: &str, patch: &Value) -> String {
    let old = if patch["before"].is_null() {
        "/dev/null".into()
    } else {
        format!("a/{name}")
    };
    let new = if patch["after"].is_null() {
        "/dev/null".into()
    } else {
        format!("b/{name}")
    };
    let mut text = format!("--- {old}\n+++ {new}\n");
    for hunk in patch["hunks"].as_array().unwrap() {
        text.push_str(&format!(
            "@@ -{},{} +{},{} @@\n",
            hunk["old_start"], hunk["old_lines"], hunk["new_start"], hunk["new_lines"]
        ));
        for line in hunk["lines"].as_array().unwrap() {
            text.push(match line["kind"].as_str().unwrap() {
                "add" => '+',
                "delete" => '-',
                "context" => ' ',
                _ => panic!("unknown line kind"),
            });
            text.push_str(line["text"].as_str().unwrap());
            text.push('\n');
            if line["no_newline"] == true {
                text.push_str("\\ No newline at end of file\n");
            }
        }
    }
    text
}

#[tokio::test(flavor = "multi_thread")]
async fn patches_apply_with_stock_git_and_reject_excess_work_without_truncation() -> Result {
    let workspace = tempfile::TempDir::new()?;
    let address = available_address().await?;
    let server = CanopyServer::start(
        config(address, workspace.path().join("server")),
        Arc::new(InMemory::new()),
    )
    .await?;
    let url = create_repository(address, "patches").await?;
    let repo = format!("http://{address}/api/repositories/patches");
    let api = format!("{repo}/pulls/1/comparison");
    let client = Client::new();
    let local = workspace.path().join("local");
    init(&local).await?;
    run_git(Some(&local), &["config", "core.autocrlf", "false"]).await?;
    let original: String = (0..30).map(|n| format!("line {n}\n")).collect();
    let modified = original
        .replace("line 5\n", "changed 5\ninserted\n")
        .replace("line 24\n", "changed 24\n");
    let cases = [
        ("hunks", Some(original.as_str()), Some(modified.as_str())),
        ("added", None, Some("<script>literal</script>\n雪\n")),
        ("deleted", Some("removed without newline"), None),
        (
            "crlf",
            Some("first\r\nold\r\nlast\r\n"),
            Some("first\r\nnew\r\nlast\r\n"),
        ),
        ("newline", Some("same"), Some("same\n")),
        ("empty", Some(""), Some("was empty\n")),
        ("emptied", Some("now empty\n"), Some("")),
    ];
    for (name, old, _) in cases {
        if let Some(old) = old {
            tokio::fs::write(local.join(name), old).await?;
        }
    }
    run_git(Some(&local), &["add", "."]).await?;
    run_git(Some(&local), &["commit", "-m", "Before"]).await?;
    let base = oid(&local, "HEAD").await?;
    push(&local, &url, "HEAD:refs/heads/main").await?;
    for (name, _, new) in cases {
        if let Some(new) = new {
            tokio::fs::write(local.join(name), new).await?;
        } else {
            tokio::fs::remove_file(local.join(name)).await?;
        }
    }
    tokio::fs::write(local.join("too-many-lines"), "x\n".repeat(20_001)).await?;
    run_git(Some(&local), &["add", "-A"]).await?;
    run_git(Some(&local), &["commit", "-m", "After"]).await?;
    let source = oid(&local, "HEAD").await?;
    push(&local, &url, "HEAD:refs/heads/feature").await?;
    open(&client, &repo, &source, &base).await?;
    let input = request(&client, &repo).await?;
    for (name, old, new) in cases {
        let result = patch(&client, &api, &input, name.as_bytes()).await?;
        assert_eq!(result["status"], "text");
        assert_eq!(result["merge_base"], base);
        assert_eq!(result["revision"], input["target"]["revision"]);
        let patch_path = workspace.path().join("change.patch");
        tokio::fs::write(&patch_path, unified(name, &result)).await?;
        if let Some(old) = old {
            tokio::fs::write(local.join(name), old).await?;
        } else {
            tokio::fs::remove_file(local.join(name)).await?;
        }
        run_git(Some(&local), &["apply", "--check", path_str(&patch_path)?]).await?;
        run_git(Some(&local), &["apply", path_str(&patch_path)?]).await?;
        if let Some(new) = new {
            assert_eq!(tokio::fs::read(local.join(name)).await?, new.as_bytes());
        } else {
            assert!(!local.join(name).exists());
        }
    }
    for (name, code) in [
        ("too-many-lines", StatusCode::PAYLOAD_TOO_LARGE),
        ("absent", StatusCode::NOT_FOUND),
        ("../hunks", StatusCode::UNPROCESSABLE_ENTITY),
    ] {
        let mut bad = input.clone();
        bad["query"] = json!({"kind":"patch","path_base64":URL_SAFE_NO_PAD.encode(name)});
        status(client.post(&api).bearer_auth(OWNER).json(&bad), code).await?;
    }
    server.shutdown().await?;
    Ok(())
}
