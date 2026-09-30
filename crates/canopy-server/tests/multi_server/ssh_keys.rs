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

async fn page(client: &Client, api: &str) -> Result<Value> {
    Ok(client
        .get(api)
        .bearer_auth(OWNER)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?)
}

async fn generate_key(
    root: &Path,
    name: &str,
    algorithm: &str,
    bits: &str,
) -> Result<(String, String)> {
    let path = root.join(name);
    let result = Command::new("ssh-keygen")
        .args([
            "-q",
            "-t",
            algorithm,
            "-b",
            bits,
            "-N",
            "",
            "-C",
            name,
            "-f",
            path_str(&path)?,
        ])
        .output()
        .await?;
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let public = tokio::fs::read_to_string(path.with_extension("pub")).await?;
    let result = Command::new("ssh-keygen")
        .args([
            "-l",
            "-E",
            "sha256",
            "-f",
            path_str(&path.with_extension("pub"))?,
        ])
        .output()
        .await?;
    assert!(result.status.success());
    let fingerprint = String::from_utf8(result.stdout)?
        .split_whitespace()
        .nth(1)
        .ok_or("missing fingerprint")?
        .to_owned();
    Ok((public, fingerprint))
}

#[tokio::test(flavor = "multi_thread")]
async fn ssh_key_api_preserves_openssh_identity_revocation_and_audit_after_disk_loss() -> Result {
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
    let api = format!("{base}/api/accounts/canopy/ssh-keys");
    let member_api = format!("{base}/api/accounts/member/ssh-keys");
    let member = format!("cnp_{}", hex::encode([51; 32]));
    status(
        client
            .post(format!("{base}/api/accounts"))
            .bearer_auth(OWNER)
            .json(&json!({"name":"member", "token":member, "scope":"admin"})),
        StatusCode::OK,
    )
    .await?;
    status(client.get(&api), StatusCode::UNAUTHORIZED).await?;
    status(client.get(&api).bearer_auth(&member), StatusCode::FORBIDDEN).await?;
    let mut keys = Vec::new();
    for (algorithm, bits) in [("ed25519", "256"), ("ecdsa", "256"), ("rsa", "2048")] {
        let (public, fingerprint) =
            generate_key(workspace.path(), algorithm, algorithm, bits).await?;
        let id = uuid::Uuid::new_v4().to_string();
        let input = json!({"id":id, "public_key":public, "scope":"write"});
        for _ in 0..2 {
            status(
                client.post(&api).bearer_auth(OWNER).json(&input),
                StatusCode::NO_CONTENT,
            )
            .await?;
        }
        // A comment change is the same credential, including across accounts.
        let canonical = public
            .split_whitespace()
            .take(2)
            .collect::<Vec<_>>()
            .join(" ");
        let changed_comment =
            json!({"id":id, "public_key":format!("{canonical} changed comment"), "scope":"write"});
        status(
            client.post(&api).bearer_auth(OWNER).json(&changed_comment),
            StatusCode::NO_CONTENT,
        )
        .await?;
        status(client.post(&member_api).bearer_auth(&member).json(&json!({"id":uuid::Uuid::new_v4().to_string(), "public_key":public, "scope":"read"})), StatusCode::CONFLICT).await?;
        keys.push((id, canonical, fingerprint));
    }
    let records = page(&client, &api).await?;
    let records = records["keys"].as_array().ok_or("key page")?;
    assert_eq!(records.len(), 3);
    for (id, public, fingerprint) in &keys {
        let row = records
            .iter()
            .find(|row| row["id"] == *id)
            .ok_or("missing key")?;
        assert_eq!(
            (row["public_key"].as_str(), row["fingerprint"].as_str()),
            (Some(public.as_str()), Some(fingerprint.as_str()))
        );
    }
    let signed = Command::new("ssh-keygen")
        .args([
            "-q",
            "-s",
            path_str(&workspace.path().join("ed25519"))?,
            "-I",
            "certificate-test",
            path_str(&workspace.path().join("ecdsa.pub"))?,
        ])
        .output()
        .await?;
    assert!(
        signed.status.success(),
        "{}",
        String::from_utf8_lossy(&signed.stderr)
    );
    let certificate = tokio::fs::read_to_string(workspace.path().join("ecdsa-cert.pub")).await?;
    let (weak_rsa, _) = generate_key(workspace.path(), "weak", "rsa", "1024").await?;
    for (id, public, scope) in [
        (uuid::Uuid::new_v4().to_string(), weak_rsa, "write"),
        (uuid::Uuid::new_v4().to_string(), certificate, "write"),
        (
            uuid::Uuid::new_v4().to_string(),
            "ssh-ed25519 invalid-base64".into(),
            "write",
        ),
        (
            uuid::Uuid::new_v4().to_string(),
            format!("command=\"sh\" {}", keys[0].1),
            "write",
        ),
        (
            uuid::Uuid::new_v4().to_string(),
            format!("{}\n{}", keys[0].1, keys[1].1),
            "write",
        ),
        (uuid::Uuid::new_v4().to_string(), keys[0].1.clone(), "admin"),
        ("invalid".to_owned(), keys[0].1.clone(), "write"),
    ] {
        status(
            client
                .post(&api)
                .bearer_auth(OWNER)
                .json(&json!({"id":id, "public_key":public, "scope":scope})),
            StatusCode::UNPROCESSABLE_ENTITY,
        )
        .await?;
    }
    // Revocation after HTTP authentication must fence the eventual Directory write.
    let registrar = format!("cnp_{}", hex::encode([52; 32]));
    let registrar_id = uuid::Uuid::new_v4().to_string();
    let tokens_api = format!("{base}/api/accounts/canopy/tokens");
    status(
        client
            .post(&tokens_api)
            .bearer_auth(OWNER)
            .json(&json!({"id":registrar_id, "token":registrar, "scope":"admin"})),
        StatusCode::NO_CONTENT,
    )
    .await?;
    let (pending, _) = generate_key(workspace.path(), "pending", "ed25519", "256").await?;
    let body = serde_json::to_vec(
        &json!({"id":uuid::Uuid::new_v4().to_string(), "public_key":pending, "scope":"read"}),
    )?;
    let socket = tokens::paused_upload(
        address,
        "/api/accounts/canopy/ssh-keys",
        &registrar,
        body.len(),
    )
    .await?;
    status(
        client
            .delete(format!("{tokens_api}/{registrar_id}"))
            .bearer_auth(OWNER),
        StatusCode::NO_CONTENT,
    )
    .await?;
    tokens::finish_upload(socket, &body, 404).await?;
    for _ in 0..2 {
        status(
            client
                .delete(format!("{api}/{}", keys[0].0))
                .bearer_auth(OWNER),
            StatusCode::NO_CONTENT,
        )
        .await?;
    }
    let revived = json!({"id":keys[0].0, "public_key":keys[0].1, "scope":"write"});
    status(
        client.post(&api).bearer_auth(OWNER).json(&revived),
        StatusCode::CONFLICT,
    )
    .await?;
    let before = page(&client, &api).await?;
    assert_eq!(before["keys"].as_array().ok_or("keys")?.len(), 3);
    let audit_api = format!("{base}/api/audit/accounts");
    let before_audit = page(&client, &audit_api).await?;
    let events: Vec<_> = before_audit["events"]
        .as_array()
        .ok_or("events")?
        .iter()
        .filter(|event| event["ssh_key_id"].is_string())
        .collect();
    assert_eq!(events.len(), 4);
    assert_eq!(events[0]["action"], "ssh_key.revoked");
    assert_eq!(events[0]["ssh_key_id"], keys[0].0);
    assert!(events.iter().all(|event| event["token_id"].is_null()));
    server.shutdown().await?;
    let address = available_address().await?;
    let restored =
        CanopyServer::start(config(address, workspace.path().join("restored")), store).await?;
    let api = format!("http://{address}/api/accounts/canopy/ssh-keys");
    assert_eq!(page(&client, &api).await?, before);
    assert_eq!(
        page(&client, &format!("http://{address}/api/audit/accounts")).await?,
        before_audit
    );
    status(
        client.post(&api).bearer_auth(OWNER).json(&revived),
        StatusCode::CONFLICT,
    )
    .await?;
    restored.shutdown().await?;
    Ok(())
}
