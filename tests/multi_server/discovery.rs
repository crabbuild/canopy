use super::*;
use reqwest::{Client, StatusCode};
use serde_json::{Value, json};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
const OWNER_TOKEN: &str = "local-test-token";

async fn send(request: reqwest::RequestBuilder) -> Result<reqwest::Response> {
    let built = request
        .try_clone()
        .ok_or("request cannot be retried")?
        .build()?;
    let listing =
        built.method() == reqwest::Method::GET && built.url().path() == "/api/repositories";
    for _ in 0..10 {
        let response = request
            .try_clone()
            .ok_or("request cannot be retried")?
            .send()
            .await?;
        if response.status() != StatusCode::SERVICE_UNAVAILABLE {
            return Ok(response);
        }
        if listing {
            assert_eq!(
                response
                    .headers()
                    .get("Retry-After")
                    .and_then(|value| value.to_str().ok()),
                Some("1")
            );
        }
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
    Err("repository admission did not recover within ten retries".into())
}

async fn page(client: &Client, base: &str, token: &str, after: Option<&str>) -> Result<Value> {
    let mut request = client
        .get(format!("{base}/api/repositories"))
        .bearer_auth(token);
    if let Some(after) = after {
        request = request.query(&[("after", after)]);
    }
    Ok(send(request).await?.error_for_status()?.json().await?)
}

async fn all_pages(client: &Client, base: &str, token: &str) -> Result<(Vec<Value>, usize)> {
    let mut after: Option<String> = None;
    let mut entries = Vec::new();
    let mut empty_pages = 0;
    for _ in 0..100 {
        let response = page(client, base, token, after.as_deref()).await?;
        let batch = response["repositories"]
            .as_array()
            .ok_or("missing repository page")?;
        assert!(batch.len() <= 32);
        entries.extend(batch.iter().cloned());
        let Some(next) = response["next_cursor"].as_str() else {
            return Ok((entries, empty_pages));
        };
        assert!(
            after.as_deref().is_none_or(|after| next > after),
            "cursor did not advance"
        );
        empty_pages += usize::from(batch.is_empty());
        after = Some(next.to_owned());
    }
    Err("repository listing did not terminate".into())
}

async fn member(client: &Client, base: &str, name: &str, role: Option<&str>) -> Result<()> {
    let url = format!("{base}/api/repositories/{name}/collaborators/reader");
    let request = match role {
        Some(role) => client.put(url).json(&json!({"role": role})),
        None => client.delete(url),
    };
    send(request.bearer_auth(OWNER_TOKEN))
        .await?
        .error_for_status()?;
    Ok(())
}

async fn metadata(client: &Client, base: &str, name: &str, token: &str) -> Result<Value> {
    Ok(send(
        client
            .get(format!("{base}/api/repositories/{name}"))
            .bearer_auth(token),
    )
    .await?
    .error_for_status()?
    .json()
    .await?)
}

#[tokio::test(flavor = "multi_thread")]
async fn repository_discovery_filters_current_acl_and_pages_past_revoked_grants_after_restore()
-> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let workspace = tempfile::TempDir::new()?;
    let address = available_address().await?;
    let server = CanopyServer::start(
        config(address, workspace.path().join("first")),
        Arc::clone(&store),
    )
    .await?;
    let base = format!("http://{address}");
    let client = Client::new();
    let reader = format!("cnp_{}", "ab".repeat(32));
    let outsider = format!("cnp_{}", "cd".repeat(32));
    for (name, token) in [("reader", &reader), ("outsider", &outsider)] {
        client
            .post(format!("{base}/api/accounts"))
            .bearer_auth(OWNER_TOKEN)
            .json(&json!({"name": name, "token": token, "scope": "read"}))
            .send()
            .await?
            .error_for_status()?;
    }
    for path in ["/api/repositories", "/api/repositories/missing"] {
        assert_eq!(
            client.get(format!("{base}{path}")).send().await?.status(),
            StatusCode::UNAUTHORIZED
        );
    }
    for cursor in ["name", "", "00000000-0000-0000-0000-000000000000"] {
        assert_eq!(
            client
                .get(format!("{base}/api/repositories"))
                .bearer_auth(&reader)
                .query(&[("after", cursor)])
                .send()
                .await?
                .status(),
            StatusCode::UNPROCESSABLE_ENTITY
        );
    }
    assert_eq!(
        page(&client, &base, &reader, None).await?["repositories"],
        json!([])
    );
    let mut entries = Vec::new();
    for index in 0..33 {
        let name = format!("repo-{index:02}");
        send(
            client
                .post(format!("{base}/api/repositories"))
                .bearer_auth(OWNER_TOKEN)
                .json(&json!({"name": name})),
        )
        .await?
        .error_for_status()?;
        member(&client, &base, &name, Some("read")).await?;
        let entry = metadata(&client, &base, &name, &reader).await?;
        assert_eq!(entry["role"], "read");
        assert_eq!(entry["default_branch"], "refs/heads/main");
        entries.push(entry);
    }
    entries.sort_by_key(|entry| entry["repository_id"].as_str().unwrap().to_owned());
    let first = page(&client, &base, OWNER_TOKEN, None).await?;
    assert_eq!(
        first["repositories"]
            .as_array()
            .ok_or("missing page")?
            .len(),
        32
    );
    let cursor = first["next_cursor"].as_str().ok_or("missing cursor")?;
    assert_eq!(
        cursor,
        entries[31]["repository_id"]
            .as_str()
            .ok_or("missing UUID")?
    );
    let (visible, _) = all_pages(&client, &base, &reader).await?;
    assert_eq!(
        visible
            .iter()
            .map(|entry| &entry["repository_id"])
            .collect::<Vec<_>>(),
        entries
            .iter()
            .map(|entry| &entry["repository_id"])
            .collect::<Vec<_>>()
    );
    let final_entry = &entries[32];
    let name = final_entry["name"].as_str().ok_or("missing name")?;
    let second = page(&client, &base, &reader, Some(cursor)).await?;
    assert_eq!(
        second["repositories"]
            .as_array()
            .ok_or("missing page")?
            .len(),
        1
    );
    assert!(second["next_cursor"].is_null());
    assert_eq!(
        second["repositories"][0]["repository_id"],
        final_entry["repository_id"]
    );
    assert_eq!(
        page(&client, &base, &outsider, Some(cursor)).await?["repositories"],
        json!([])
    );
    assert_eq!(
        client
            .get(format!("{base}/api/repositories/{name}"))
            .bearer_auth(&outsider)
            .send()
            .await?
            .status(),
        StatusCode::NOT_FOUND
    );

    // Rename does not reorder the UUID cursor or copy an ACL between names.
    client
        .patch(format!("{base}/api/repositories/{name}"))
        .bearer_auth(OWNER_TOKEN)
        .json(&json!({"name": "renamed", "repository_id": final_entry["repository_id"]}))
        .send()
        .await?
        .error_for_status()?;
    assert_eq!(
        client
            .get(format!("{base}/api/repositories/{name}"))
            .bearer_auth(&reader)
            .send()
            .await?
            .status(),
        StatusCode::NOT_FOUND
    );
    let renamed = metadata(&client, &base, "renamed", &reader).await?;
    assert_eq!(renamed["repository_id"], final_entry["repository_id"]);
    assert_eq!(renamed["clone_url"], format!("{base}/canopy/renamed.git"));
    client.put(format!("{base}/api/repositories/renamed/default-branch")).bearer_auth(OWNER_TOKEN)
        .json(&json!({"repository_id": renamed["repository_id"], "reference": "refs/heads/trunk", "expected_generation": renamed["ref_generation"]})).send().await?.error_for_status()?;
    member(&client, &base, "renamed", Some("write")).await?;
    let details = metadata(&client, &base, "renamed", &reader).await?;
    assert_eq!(details["role"], "write");
    assert_eq!(details["default_branch"], "refs/heads/trunk");
    assert_eq!(details["ref_generation"], 1);
    assert_eq!(
        metadata(&client, &base, "renamed", OWNER_TOKEN).await?["role"],
        "admin"
    );
    for entry in &entries[..32] {
        member(
            &client,
            &base,
            entry["name"].as_str().ok_or("missing name")?,
            None,
        )
        .await?;
    }
    let (visible, empty_pages) = all_pages(&client, &base, &reader).await?;
    assert!(empty_pages > 0);
    assert_eq!(visible.len(), 1);
    assert_eq!(visible[0]["name"], "renamed");
    let revoked_name = entries[0]["name"].as_str().ok_or("missing name")?;
    assert_eq!(
        send(
            client
                .get(format!("{base}/api/repositories/{revoked_name}"))
                .bearer_auth(&reader)
        )
        .await?
        .status(),
        StatusCode::NOT_FOUND
    );
    server.shutdown().await?;

    let address = available_address().await?;
    let restored =
        CanopyServer::start(config(address, workspace.path().join("restored")), store).await?;
    let base = format!("http://{address}");
    let (visible, empty_pages) = all_pages(&client, &base, &reader).await?;
    assert!(empty_pages > 0);
    assert_eq!(visible.len(), 1);
    assert_eq!(visible[0]["name"], "renamed");
    let visible_page = page(&client, &base, &reader, Some(cursor)).await?;
    assert_eq!(visible_page["repositories"][0]["name"], "renamed");
    let details = metadata(&client, &base, "renamed", &reader).await?;
    assert_eq!(details["default_branch"], "refs/heads/trunk");
    assert_eq!(details["role"], "write");
    assert_eq!(
        page(&client, &base, &outsider, None).await?["repositories"],
        json!([])
    );
    let owner_page = page(&client, &base, OWNER_TOKEN, None).await?;
    assert_eq!(
        owner_page["repositories"]
            .as_array()
            .ok_or("missing owner page")?
            .len(),
        32
    );
    // Regrant reuses the retained candidate; it becomes visible on its same page.
    member(&client, &base, revoked_name, Some("read")).await?;
    let regranted = page(&client, &base, &reader, None).await?;
    assert_eq!(
        regranted["repositories"]
            .as_array()
            .ok_or("missing page")?
            .len(),
        1
    );
    assert_eq!(regranted["repositories"][0]["name"], revoked_name);
    restored.shutdown().await?;
    Ok(())
}
