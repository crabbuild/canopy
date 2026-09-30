use super::*;
use reqwest::{Client, StatusCode};
use serde_json::{Value, json};

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;
const OWNER: &str = "local-test-token";

async fn status(request: reqwest::RequestBuilder, expected: StatusCode) -> Result {
    let response = request.send().await?;
    assert_eq!(response.status(), expected, "{}", response.text().await?);
    Ok(())
}

async fn page(client: &Client, api: &str, after: Option<&str>) -> Result<Value> {
    let mut request = client.get(api).bearer_auth(OWNER);
    if let Some(after) = after {
        request = request.query(&[("after", after)]);
    }
    Ok(request.send().await?.error_for_status()?.json().await?)
}

#[tokio::test(flavor = "multi_thread")]
async fn collaborator_roster_is_owner_only_bounded_and_durable() -> Result {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let workspace = tempfile::TempDir::new()?;
    let address = available_address().await?;
    let server = CanopyServer::start(
        config(address, workspace.path().join("first")),
        Arc::clone(&store),
    )
    .await?;
    create_repository(address, "team").await?;
    let base = format!("http://{address}");
    let api = format!("{base}/api/repositories/team/collaborators");
    let client = Client::new();
    status(client.get(&api), StatusCode::UNAUTHORIZED).await?;
    let empty = page(&client, &api, None).await?;
    assert_eq!(empty["owner"], "canopy");
    assert_eq!(empty["collaborators"], json!([]));
    assert!(empty["next_after"].is_null());

    let outsider = format!("cnp_{}", "ff".repeat(32));
    status(
        client
            .post(format!("{base}/api/accounts"))
            .bearer_auth(OWNER)
            .json(&json!({"name":"outsider", "token":outsider, "scope":"admin"})),
        StatusCode::OK,
    )
    .await?;
    status(
        client.get(&api).bearer_auth(&outsider),
        StatusCode::NOT_FOUND,
    )
    .await?;
    let restricted_owner = format!("cnp_{}", "ef".repeat(32));
    status(client.post(format!("{base}/api/accounts/canopy/tokens")).bearer_auth(OWNER).json(&json!({"id":uuid::Uuid::new_v4().to_string(), "token":restricted_owner, "scope":"read"})), StatusCode::NO_CONTENT).await?;
    status(
        client.get(&api).bearer_auth(&restricted_owner),
        StatusCode::FORBIDDEN,
    )
    .await?;

    let mut expected = Vec::new();
    for index in 0..33 {
        let account = format!("member-{index:02}");
        let token = format!("cnp_{index:064x}");
        let scope = if index == 0 { "admin" } else { "read" };
        let role = if index % 2 == 0 { "read" } else { "write" };
        status(
            client
                .post(format!("{base}/api/accounts"))
                .bearer_auth(OWNER)
                .json(&json!({"name":account, "token":token, "scope":scope})),
            StatusCode::OK,
        )
        .await?;
        status(
            client
                .put(format!("{api}/{account}"))
                .bearer_auth(OWNER)
                .json(&json!({"role":role})),
            StatusCode::OK,
        )
        .await?;
        expected.push(json!({"account": account, "role": role}));
    }
    // Admin token scope does not turn a read collaborator into a repository owner.
    status(
        client.get(&api).bearer_auth(format!("cnp_{:064x}", 0)),
        StatusCode::FORBIDDEN,
    )
    .await?;
    let first = page(&client, &api, None).await?;
    assert_eq!(first["collaborators"], json!(expected[..32]));
    assert_eq!(first["next_after"], "member-31");
    let second = page(&client, &api, Some("member-31")).await?;
    assert_eq!(second["collaborators"], json!(expected[32..]));
    assert!(second["next_after"].is_null());
    for cursor in ["", "../bad", "UPPER"] {
        status(
            client
                .get(&api)
                .bearer_auth(OWNER)
                .query(&[("after", cursor)]),
            StatusCode::UNPROCESSABLE_ENTITY,
        )
        .await?;
    }
    status(
        client.delete(format!("{api}/member-15")).bearer_auth(OWNER),
        StatusCode::NO_CONTENT,
    )
    .await?;
    status(
        client
            .put(format!("{api}/member-00"))
            .bearer_auth(OWNER)
            .json(&json!({"role":"write"})),
        StatusCode::OK,
    )
    .await?;
    expected.remove(15);
    expected[0]["role"] = json!("write");
    // Cursors remain account names even if a prior page's membership changes.
    assert_eq!(
        page(&client, &api, Some("member-31")).await?["collaborators"],
        json!(expected[31..])
    );
    status(
        client.delete(format!("{api}/canopy")).bearer_auth(OWNER),
        StatusCode::NO_CONTENT,
    )
    .await?;
    let updated = page(&client, &api, None).await?;
    assert_eq!(updated["owner"], "canopy");
    assert_eq!(updated["collaborators"], json!(expected));
    assert_eq!(updated["next_after"], "member-32");
    let end = page(&client, &api, Some("member-32")).await?;
    assert_eq!(end["collaborators"], json!([]));
    assert!(end["next_after"].is_null());

    status(
        client
            .patch(format!("{base}/api/repositories/team"))
            .bearer_auth(OWNER)
            .json(&json!({"name":"renamed", "repository_id": empty["repository_id"]})),
        StatusCode::OK,
    )
    .await?;
    status(client.get(&api).bearer_auth(OWNER), StatusCode::NOT_FOUND).await?;
    server.shutdown().await?;
    let restored_address = available_address().await?;
    let restored = CanopyServer::start(
        config(restored_address, workspace.path().join("restored")),
        Arc::clone(&store),
    )
    .await?;
    let restored_api = format!("http://{restored_address}/api/repositories/renamed/collaborators");
    let recovered = page(&client, &restored_api, None).await?;
    assert_eq!(recovered["repository_id"], empty["repository_id"]);
    assert_eq!(recovered["collaborators"], json!(expected));
    status(
        client.get(&restored_api).bearer_auth(&outsider),
        StatusCode::NOT_FOUND,
    )
    .await?;
    status(
        client
            .get(&restored_api)
            .bearer_auth(format!("cnp_{:064x}", 0)),
        StatusCode::FORBIDDEN,
    )
    .await?;
    restored.shutdown().await?;
    Ok(())
}
