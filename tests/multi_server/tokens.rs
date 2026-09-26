use super::*;
use reqwest::{Client, StatusCode};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;
const OWNER: &str = "local-test-token";

fn secret(byte: u8) -> String {
    format!("cnp_{}", hex::encode([byte; 32]))
}

async fn status(request: reqwest::RequestBuilder, expected: StatusCode) -> Result {
    let response = request.send().await?;
    assert_eq!(response.status(), expected, "{}", response.text().await?);
    Ok(())
}

async fn page(client: &Client, api: &str, token: &str, after: Option<&str>) -> Result<Value> {
    let mut request = client.get(api).bearer_auth(token);
    if let Some(after) = after {
        request = request.query(&[("after", after)]);
    }
    Ok(request.send().await?.error_for_status()?.json().await?)
}

fn issue(
    client: &Client,
    api: &str,
    actor: &str,
    id: &str,
    token: &str,
    scope: &str,
) -> reqwest::RequestBuilder {
    client
        .post(api)
        .bearer_auth(actor)
        .json(&json!({"id": id, "token": token, "scope": scope}))
}

async fn paused_upload(
    address: std::net::SocketAddr,
    path: &str,
    token: &str,
    bytes: usize,
) -> Result<tokio::net::TcpStream> {
    let mut socket = tokio::net::TcpStream::connect(address).await?;
    socket.write_all(format!("POST {path} HTTP/1.1\r\nHost: {address}\r\nAuthorization: Bearer {token}\r\nContent-Type: application/json\r\nContent-Length: {bytes}\r\nExpect: 100-continue\r\nConnection: close\r\n\r\n").as_bytes()).await?;
    let mut continued = vec![0; b"HTTP/1.1 100 Continue\r\n\r\n".len()];
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        socket.read_exact(&mut continued),
    )
    .await??;
    assert_eq!(continued, b"HTTP/1.1 100 Continue\r\n\r\n");
    Ok(socket)
}

async fn finish_upload(mut socket: tokio::net::TcpStream, body: &[u8], status: u16) -> Result {
    socket.write_all(body).await?;
    let mut reply = Vec::new();
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        socket.read_to_end(&mut reply),
    )
    .await??;
    assert!(
        reply.starts_with(format!("HTTP/1.1 {status} ").as_bytes()),
        "{}",
        String::from_utf8_lossy(&reply)
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn token_rotation_revocation_and_last_admin_survive_owner_restore() -> Result {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let workspace = tempfile::TempDir::new()?;
    let address = available_address().await?;
    let server = CanopyServer::start(
        config(address, workspace.path().join("first")),
        Arc::clone(&store),
    )
    .await?;
    let client = Client::new();
    let base = format!("http://{address}");
    let owner_api = format!("{base}/api/accounts/canopy/tokens");
    let bob_api = format!("{base}/api/accounts/bob/tokens");
    let repo_url = create_repository(address, "tokens").await?;
    status(client.get(&owner_api), StatusCode::UNAUTHORIZED).await?;
    let initial = page(&client, &owner_api, OWNER, None).await?;
    let first_id = initial["tokens"][0]["id"]
        .as_str()
        .ok_or("missing initial token")?;
    status(
        issue(
            &client,
            &owner_api,
            OWNER,
            &uuid::Uuid::new_v4().to_string(),
            &secret(61),
            "read",
        ),
        StatusCode::NO_CONTENT,
    )
    .await?;
    status(
        client
            .delete(format!("{owner_api}/{first_id}"))
            .bearer_auth(OWNER),
        StatusCode::CONFLICT,
    )
    .await?;

    let bob = secret(1);
    status(
        client
            .post(format!("{base}/api/accounts"))
            .bearer_auth(OWNER)
            .json(&json!({"name":"bob", "token":bob, "scope":"admin"})),
        StatusCode::OK,
    )
    .await?;
    status(
        client
            .put(format!("{base}/api/repositories/tokens/collaborators/bob"))
            .bearer_auth(OWNER)
            .json(&json!({"role":"read"})),
        StatusCode::OK,
    )
    .await?;
    status(
        client.get(&owner_api).bearer_auth(&bob),
        StatusCode::FORBIDDEN,
    )
    .await?;
    status(
        client.post(&owner_api).bearer_auth(&bob).json(&json!({})),
        StatusCode::FORBIDDEN,
    )
    .await?;
    status(
        client.get(&bob_api).basic_auth("canopy", Some(&bob)),
        StatusCode::UNAUTHORIZED,
    )
    .await?;
    let bob_initial = page(&client, &bob_api, &bob, None).await?;
    let bob_id = bob_initial["tokens"][0]["id"]
        .as_str()
        .ok_or("missing bob token")?;
    let reader = secret(2);
    let reader_id = uuid::Uuid::new_v4().to_string();
    for _ in 0..2 {
        status(
            issue(&client, &bob_api, &bob, &reader_id, &reader, "read"),
            StatusCode::NO_CONTENT,
        )
        .await?;
    }
    status(
        issue(&client, &bob_api, &bob, &reader_id, &reader, "write"),
        StatusCode::CONFLICT,
    )
    .await?;
    status(
        issue(
            &client,
            &owner_api,
            OWNER,
            &uuid::Uuid::new_v4().to_string(),
            &reader,
            "read",
        ),
        StatusCode::CONFLICT,
    )
    .await?;
    status(
        client.get(&bob_api).bearer_auth(&reader),
        StatusCode::FORBIDDEN,
    )
    .await?;
    status(
        issue(
            &client,
            &bob_api,
            &reader,
            &uuid::Uuid::new_v4().to_string(),
            &secret(3),
            "admin",
        ),
        StatusCode::FORBIDDEN,
    )
    .await?;
    for (id, token, scope) in [
        ("bad", reader.as_str(), "read"),
        (reader_id.as_str(), "short", "read"),
        (reader_id.as_str(), reader.as_str(), "unknown"),
    ] {
        status(
            issue(&client, &bob_api, OWNER, id, token, scope),
            StatusCode::UNPROCESSABLE_ENTITY,
        )
        .await?;
    }
    status(
        client
            .get(&bob_api)
            .bearer_auth(OWNER)
            .query(&[("after", "bad")]),
        StatusCode::UNPROCESSABLE_ENTITY,
    )
    .await?;

    // A 100 Continue response proves authentication completed and body reading
    // started. Revoking that exact credential must still fence token issuance.
    let delayed_id = uuid::Uuid::new_v4().to_string();
    let delayed_token = secret(4);
    let body =
        serde_json::to_vec(&json!({"id":delayed_id, "token":delayed_token, "scope":"admin"}))?;
    let socket = paused_upload(address, "/api/accounts/bob/tokens", &bob, body.len()).await?;
    status(
        client
            .delete(format!("{bob_api}/{bob_id}"))
            .bearer_auth(OWNER),
        StatusCode::NO_CONTENT,
    )
    .await?;
    finish_upload(socket, &body, 404).await?;
    for token in [&bob, &delayed_token] {
        status(
            client
                .get(format!("{repo_url}/info/refs?service=git-upload-pack"))
                .bearer_auth(token),
            StatusCode::UNAUTHORIZED,
        )
        .await?;
    }
    // Revocation keeps the secret and ID reserved; account creation cannot revive it.
    status(
        issue(&client, &bob_api, OWNER, bob_id, &bob, "admin"),
        StatusCode::CONFLICT,
    )
    .await?;
    status(
        client
            .post(format!("{base}/api/accounts"))
            .bearer_auth(OWNER)
            .json(&json!({"name":"bob", "token":bob, "scope":"admin"})),
        StatusCode::CONFLICT,
    )
    .await?;

    let source = workspace.path().join("source");
    run_git(None, &["init", "-b", "main", path_str(&source)?]).await?;
    run_git(Some(&source), &["config", "user.name", "Canopy Test"]).await?;
    run_git(
        Some(&source),
        &["config", "user.email", "canopy@example.invalid"],
    )
    .await?;
    std::fs::write(source.join("README.md"), b"Revocable Git credentials\n")?;
    run_git(Some(&source), &["add", "."]).await?;
    run_git(Some(&source), &["commit", "-m", "Token fixture"]).await?;
    run_git(
        Some(&source),
        &[
            "-c",
            "http.extraHeader=Authorization: Bearer local-test-token",
            "push",
            &repo_url,
            "main",
        ],
    )
    .await?;
    let reader_header = format!("http.extraHeader=Authorization: Bearer {reader}");
    run_git(
        None,
        &[
            "-c",
            &reader_header,
            "clone",
            &repo_url,
            path_str(&workspace.path().join("reader"))?,
        ],
    )
    .await?;

    // Read both index pages, including retained revoked metadata. Responses must
    // never expose a digest or secret, and an exact issue retry added no record.
    for n in 10..43 {
        status(
            issue(
                &client,
                &bob_api,
                OWNER,
                &uuid::Uuid::new_v4().to_string(),
                &secret(n),
                "read",
            ),
            StatusCode::NO_CONTENT,
        )
        .await?;
    }
    let first = page(&client, &bob_api, OWNER, None).await?;
    assert_eq!(
        first["tokens"].as_array().ok_or("missing tokens")?.len(),
        32
    );
    let after = first["next_after"].as_str().ok_or("missing cursor")?;
    let second = page(&client, &bob_api, OWNER, Some(after)).await?;
    let all: Vec<_> = first["tokens"]
        .as_array()
        .ok_or("first page missing")?
        .iter()
        .chain(second["tokens"].as_array().ok_or("second page missing")?)
        .collect();
    assert_eq!(all.len(), 35);
    assert!(second["next_after"].is_null());
    let ids: Vec<_> = all
        .iter()
        .map(|token| token["id"].as_str().ok_or("missing ID"))
        .collect::<std::result::Result<_, _>>()?;
    assert!(ids.windows(2).all(|pair| pair[0] < pair[1]));
    for token in all {
        let keys: Vec<_> = token
            .as_object()
            .ok_or("invalid token")?
            .keys()
            .map(String::as_str)
            .collect();
        assert_eq!(keys, ["created_at_ms", "enabled", "id", "scope"]);
    }

    let owner_a = secret(50);
    let owner_b = secret(51);
    let id_a = uuid::Uuid::new_v4().to_string();
    let id_b = uuid::Uuid::new_v4().to_string();
    for (id, token) in [(&id_a, &owner_a), (&id_b, &owner_b)] {
        status(
            issue(&client, &owner_api, OWNER, id, token, "admin"),
            StatusCode::NO_CONTENT,
        )
        .await?;
    }
    let delayed_account_token = secret(60);
    let account_body = serde_json::to_vec(
        &json!({"name":"delayed", "token": delayed_account_token, "scope":"admin"}),
    )?;
    let account_upload = paused_upload(address, "/api/accounts", OWNER, account_body.len()).await?;
    status(
        client
            .delete(format!("{owner_api}/{first_id}"))
            .bearer_auth(&owner_a),
        StatusCode::NO_CONTENT,
    )
    .await?;
    finish_upload(account_upload, &account_body, 403).await?;
    status(
        client
            .get(format!("{base}/api/accounts/delayed/tokens"))
            .bearer_auth(&owner_a),
        StatusCode::NOT_FOUND,
    )
    .await?;
    let (a, b) = tokio::join!(
        client
            .delete(format!("{owner_api}/{id_a}"))
            .bearer_auth(&owner_a)
            .send(),
        client
            .delete(format!("{owner_api}/{id_b}"))
            .bearer_auth(&owner_b)
            .send(),
    );
    let a = a?.status();
    let b = b?.status();
    assert!(matches!(
        (a, b),
        (StatusCode::NO_CONTENT, StatusCode::CONFLICT)
            | (StatusCode::CONFLICT, StatusCode::NO_CONTENT)
    ));
    let surviving_owner = if a == StatusCode::CONFLICT {
        owner_a
    } else {
        owner_b
    };
    status(
        client.get(&owner_api).bearer_auth(OWNER),
        StatusCode::UNAUTHORIZED,
    )
    .await?;
    status(
        client
            .delete(format!("{bob_api}/{reader_id}"))
            .bearer_auth(&surviving_owner),
        StatusCode::NO_CONTENT,
    )
    .await?;
    status(
        client
            .delete(format!("{bob_api}/{reader_id}"))
            .bearer_auth(&surviving_owner),
        StatusCode::NO_CONTENT,
    )
    .await?;
    server.shutdown().await?;

    let restored_address = available_address().await?;
    let mut restored_config = config(restored_address, workspace.path().join("restored"));
    restored_config.token = surviving_owner.clone();
    let restored = CanopyServer::start(restored_config, Arc::clone(&store)).await?;
    let restored_repo = format!("http://{restored_address}/canopy/tokens.git");
    for token in [OWNER, &bob, &reader, &delayed_token, &delayed_account_token] {
        status(
            client
                .get(format!("{restored_repo}/info/refs?service=git-upload-pack"))
                .bearer_auth(token),
            StatusCode::UNAUTHORIZED,
        )
        .await?;
        status(
            client
                .post(format!("{restored_repo}/info/lfs/objects/batch"))
                .bearer_auth(token)
                .json(&json!({"operation":"download", "objects":[]})),
            StatusCode::UNAUTHORIZED,
        )
        .await?;
    }
    let owner_header = format!("http.extraHeader=Authorization: Bearer {surviving_owner}");
    let clone = workspace.path().join("restored-clone");
    run_git(
        None,
        &[
            "-c",
            &owner_header,
            "clone",
            &restored_repo,
            path_str(&clone)?,
        ],
    )
    .await?;
    assert_eq!(
        std::fs::read(clone.join("README.md"))?,
        b"Revocable Git credentials\n"
    );
    run_git(Some(&clone), &["fsck", "--strict", "--full"]).await?;
    let recovered = page(
        &client,
        &format!("http://{restored_address}/api/accounts/canopy/tokens"),
        &surviving_owner,
        None,
    )
    .await?;
    assert_eq!(
        recovered["tokens"]
            .as_array()
            .ok_or("missing tokens")?
            .iter()
            .filter(|token| token["enabled"] == true && token["scope"] == "admin")
            .count(),
        1
    );
    restored.shutdown().await?;
    Ok(())
}
