use super::*;
use reqwest::{Client, Method, StatusCode};
use serde_json::{Value, json};
use sha2::Digest as _;

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;
const OWNER: &str = "local-test-token";

async fn send(
    client: &Client,
    method: Method,
    url: &str,
    token: &str,
    body: Value,
    status: StatusCode,
) -> Result<Value> {
    let response = client
        .request(method, url)
        .bearer_auth(token)
        .json(&body)
        .send()
        .await?;
    assert_eq!(response.status(), status);
    if status == StatusCode::OK {
        assert_eq!(response.headers()["cache-control"], "no-store");
    }
    let bytes = response.bytes().await?;
    Ok(if status.is_success() && status != StatusCode::NO_CONTENT {
        serde_json::from_slice(&bytes)?
    } else {
        Value::Null
    })
}

async fn page(client: &Client, url: &str) -> Result<Value> {
    send(client, Method::GET, url, OWNER, Value::Null, StatusCode::OK).await
}

#[tokio::test(flavor = "multi_thread")]
async fn administrative_history_is_private_paginated_and_restored_without_retry_duplicates()
-> Result {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let workspace = tempfile::TempDir::new()?;
    let address = available_address().await?;
    let server = CanopyServer::start(
        config(address, workspace.path().join("first")),
        store.clone(),
    )
    .await?;
    let base = format!("http://{address}");
    let audit = format!("{base}/api/audit/accounts");
    let accounts = format!("{base}/api/accounts");
    let client = Client::new();
    let initial = page(&client, &audit).await?;
    assert_eq!(initial["events"].as_array().unwrap().len(), 1);
    assert_eq!(initial["events"][0]["actor"], Value::Null);
    assert_eq!(initial["events"][0]["action"], "account.created");
    let owner_id = initial["events"][0]["token_id"].clone();
    let member = format!("cnp_{}", hex::encode([11; 32]));
    let creation = json!({"name":"member", "token":member, "scope":"admin"});
    for _ in 0..2 {
        send(
            &client,
            Method::POST,
            &accounts,
            OWNER,
            creation.clone(),
            StatusCode::OK,
        )
        .await?;
    }
    let token_path = format!("{accounts}/member/tokens");
    let id = uuid::Uuid::new_v4().to_string();
    let secret = format!("cnp_{}", hex::encode([12; 32]));
    let expiry = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_millis() as i64
        + 60_000;
    let issue = json!({"id":id, "token":secret, "scope":"admin", "expires_at_ms":expiry});
    for _ in 0..2 {
        send(
            &client,
            Method::POST,
            &token_path,
            &member,
            issue.clone(),
            StatusCode::NO_CONTENT,
        )
        .await?;
    }
    // Self-revocation must retain the old actor identity and issued metadata.
    send(
        &client,
        Method::DELETE,
        &format!("{token_path}/{id}"),
        &secret,
        Value::Null,
        StatusCode::NO_CONTENT,
    )
    .await?;
    send(
        &client,
        Method::DELETE,
        &format!("{token_path}/{id}"),
        OWNER,
        Value::Null,
        StatusCode::NO_CONTENT,
    )
    .await?;
    for (token, expected) in [
        ("invalid", StatusCode::UNAUTHORIZED),
        (member.as_str(), StatusCode::FORBIDDEN),
        (secret.as_str(), StatusCode::UNAUTHORIZED),
    ] {
        send(&client, Method::GET, &audit, token, Value::Null, expected).await?;
    }
    for _ in 0..2 {
        send(
            &client,
            Method::POST,
            &format!("{accounts}/member/disable"),
            OWNER,
            Value::Null,
            StatusCode::NO_CONTENT,
        )
        .await?;
    }
    let first = page(&client, &audit).await?;
    let events = first["events"].as_array().unwrap();
    assert_eq!(events.len(), 5);
    assert_eq!(events[0]["action"], "account.disabled");
    assert_eq!(events[0]["actor_token_id"], owner_id);
    assert_eq!(events[1]["actor"], "member");
    assert_eq!(events[1]["actor_token_id"], id);
    assert_eq!(events[1]["token_id"], id);
    assert_eq!(events[1]["expires_at_ms"], expiry);
    assert_eq!(events[2]["action"], "token.issued");
    assert_eq!(events[2]["actor"], "member");
    assert_eq!(events[3]["actor"], "canopy");
    assert_eq!(events[3]["action"], "account.created");
    for event in events {
        assert_eq!(event.as_object().unwrap().len(), 9);
        for private in [
            &member,
            &secret,
            &hex::encode(sha2::Sha256::digest(secret.as_bytes())),
        ] {
            assert!(!event.to_string().contains(private));
        }
    }
    for cursor in ["0", "-1", "abc", "9223372036854775808"] {
        send(
            &client,
            Method::GET,
            &format!("{audit}?before={cursor}"),
            OWNER,
            Value::Null,
            StatusCode::UNPROCESSABLE_ENTITY,
        )
        .await?;
    }
    assert_eq!(
        page(&client, &format!("{audit}?before=1")).await?["events"],
        json!([])
    );
    for n in 0..30 {
        send(&client, Method::POST, &accounts, OWNER, json!({"name":format!("person-{n:02}"), "token":format!("cnp_{}", hex::encode([n + 40; 32])), "scope":"read"}), StatusCode::OK).await?;
    }
    let recent = page(&client, &audit).await?;
    assert_eq!(recent["events"].as_array().unwrap().len(), 32);
    assert_eq!(recent["next_before"], "4");
    let older_url = format!("{audit}?before=4");
    let older = page(&client, &older_url).await?;
    assert_eq!(older["events"].as_array().unwrap().len(), 3);
    assert_eq!(older["next_before"], Value::Null);
    send(
        &client,
        Method::POST,
        &format!("{accounts}/person-00/disable"),
        OWNER,
        Value::Null,
        StatusCode::NO_CONTENT,
    )
    .await?;
    assert_eq!(page(&client, &older_url).await?, older);
    let snapshot = page(&client, &audit).await?;
    server.shutdown().await?;
    let restored =
        CanopyServer::start(config(address, workspace.path().join("restored")), store).await?;
    assert_eq!(page(&client, &audit).await?, snapshot);
    assert_eq!(page(&client, &older_url).await?, older);
    restored.shutdown().await?;
    Ok(())
}
