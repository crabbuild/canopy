use super::*;
use reqwest::{Client, StatusCode};
use serde_json::{Value, json};

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;
const OWNER: &str = "local-test-token";

async fn read(client: &Client, url: &str, token: &str) -> Result<Value> {
    let response = client
        .get(url)
        .bearer_auth(token)
        .send()
        .await?
        .error_for_status()?;
    assert_eq!(response.headers()["cache-control"], "no-store");
    Ok(response.json().await?)
}

#[tokio::test(flavor = "multi_thread")]
async fn session_identity_and_account_pages_preserve_authority_after_restore() -> Result {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let workspace = tempfile::TempDir::new()?;
    let address = available_address().await?;
    let server = CanopyServer::start(
        config(address, workspace.path().join("first")),
        store.clone(),
    )
    .await?;
    let client = Client::new();
    let base = format!("http://{address}");
    let session = format!("{base}/api/session");
    let accounts = format!("{base}/api/accounts");
    for endpoint in [&session, &accounts] {
        assert_eq!(
            client.get(endpoint).send().await?.status(),
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            client
                .get(endpoint)
                .bearer_auth("invalid")
                .send()
                .await?
                .status(),
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            client
                .get(endpoint)
                .basic_auth("wrong", Some(OWNER))
                .send()
                .await?
                .status(),
            StatusCode::UNAUTHORIZED
        );
    }
    let initial = read(&client, &session, OWNER).await?;
    assert_eq!(initial["account"], "canopy");
    assert_eq!(initial["site_admin"], true);
    let metadata = read(
        &client,
        &format!("{base}/api/accounts/canopy/tokens"),
        OWNER,
    )
    .await?;
    assert_eq!(initial["token_id"], metadata["tokens"][0]["id"]);
    assert_eq!(initial.as_object().ok_or("session object")?.len(), 4);

    // A full page plus one checks the exclusive cursor and retained disabled rows.
    for n in 0..32 {
        client
            .post(&accounts)
            .bearer_auth(OWNER)
            .json(&json!({
                "name": format!("member-{n:02}"),
                "token": format!("cnp_{}", hex::encode([n + 1; 32])),
                "scope": if n == 0 { "read" } else { "admin" },
            }))
            .send()
            .await?
            .error_for_status()?;
    }
    for (n, scope) in [(0, "read"), (1, "admin")] {
        let token = format!("cnp_{}", hex::encode([n + 1; 32]));
        let member = read(&client, &session, &token).await?;
        assert_eq!(member["account"], format!("member-{n:02}"));
        assert_eq!(member["token_scope"], scope);
        assert_eq!(member["site_admin"], false);
        assert_eq!(
            client
                .get(&accounts)
                .bearer_auth(&token)
                .send()
                .await?
                .status(),
            StatusCode::FORBIDDEN
        );
    }
    client
        .post(format!("{accounts}/member-00/disable"))
        .bearer_auth(OWNER)
        .send()
        .await?
        .error_for_status()?;
    let first = read(&client, &accounts, OWNER).await?;
    assert_eq!(
        first["accounts"].as_array().ok_or("account page")?.len(),
        32
    );
    assert_eq!(
        first["accounts"][1],
        json!({"name":"member-00", "enabled":false})
    );
    assert_eq!(first["next_after"], "member-30");
    let last = read(&client, &format!("{accounts}?after=member-30"), OWNER).await?;
    assert_eq!(
        last,
        json!({"accounts":[{"name":"member-31", "enabled":true}], "next_after":null})
    );
    assert_eq!(
        client
            .get(format!("{accounts}?after=bad%2Fcursor"))
            .bearer_auth(OWNER)
            .send()
            .await?
            .status(),
        StatusCode::UNPROCESSABLE_ENTITY
    );
    assert_eq!(
        client
            .get(&session)
            .bearer_auth(format!("cnp_{}", hex::encode([1; 32])))
            .send()
            .await?
            .status(),
        StatusCode::UNAUTHORIZED
    );

    let rotated = format!("cnp_{}", hex::encode([99; 32]));
    let id = uuid::Uuid::new_v4().to_string();
    client
        .post(format!("{accounts}/canopy/tokens"))
        .bearer_auth(OWNER)
        .json(&json!({"id":id, "token":rotated, "scope":"admin"}))
        .send()
        .await?
        .error_for_status()?;
    assert_eq!(read(&client, &session, &rotated).await?["token_id"], id);
    client
        .delete(format!("{accounts}/canopy/tokens/{id}"))
        .bearer_auth(&rotated)
        .send()
        .await?
        .error_for_status()?;
    assert_eq!(
        client
            .get(&session)
            .bearer_auth(&rotated)
            .send()
            .await?
            .status(),
        StatusCode::UNAUTHORIZED
    );
    server.shutdown().await?;

    let restored =
        CanopyServer::start(config(address, workspace.path().join("restored")), store).await?;
    assert_eq!(read(&client, &session, OWNER).await?, initial);
    assert_eq!(read(&client, &accounts, OWNER).await?, first);
    restored.shutdown().await?;
    Ok(())
}
