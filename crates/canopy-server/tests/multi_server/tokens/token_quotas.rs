use super::*;

fn credential() -> (String, String) {
    (
        uuid::Uuid::new_v4().to_string(),
        format!(
            "cnp_{}{}",
            uuid::Uuid::new_v4().simple(),
            uuid::Uuid::new_v4().simple()
        ),
    )
}

async fn all_tokens(client: &Client, api: &str) -> Result<Vec<Value>> {
    let mut tokens = Vec::new();
    let mut cursor = None;
    loop {
        let result = page(client, api, OWNER, cursor.as_deref()).await?;
        tokens.extend(
            result["tokens"]
                .as_array()
                .ok_or("missing tokens")?
                .iter()
                .cloned(),
        );
        cursor = result["next_after"].as_str().map(str::to_owned);
        if cursor.is_none() {
            return Ok(tokens);
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn account_limits_serialize_races_preserve_retries_and_survive_recovery() -> Result {
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
    let api = format!("{base}/api/accounts/limited/tokens");
    let (_, administrator) = credential();
    let account = json!({"name":"limited", "token":administrator, "scope":"admin"});
    status(
        client
            .post(format!("{base}/api/accounts"))
            .bearer_auth(OWNER)
            .json(&account),
        StatusCode::OK,
    )
    .await?;
    let initial = all_tokens(&client, &api).await?;
    let initial_id = initial[0]["id"].as_str().ok_or("initial ID")?;
    // Include both permanent and expiring credentials in the same capacity pool.
    for index in 0..62 {
        let (id, token) = credential();
        let expires = (index % 2 == 0)
            .then(|| now_ms().map(|now| now + 300_000))
            .transpose()?;
        status(
            client.post(&api).bearer_auth(&administrator).json(&json!({
                "id":id, "token":token, "scope":"read", "expires_at_ms":expires,
            })),
            StatusCode::NO_CONTENT,
        )
        .await?;
    }
    let left = credential();
    let right = credential();
    let (a, b) = tokio::join!(
        issue(&client, &api, OWNER, &left.0, &left.1, "read").send(),
        issue(&client, &api, &administrator, &right.0, &right.1, "read").send(),
    );
    let (a, b) = (a?.status(), b?.status());
    assert!(matches!(
        (a, b),
        (StatusCode::NO_CONTENT, StatusCode::TOO_MANY_REQUESTS)
            | (StatusCode::TOO_MANY_REQUESTS, StatusCode::NO_CONTENT)
    ));
    let (winner, loser) = if a == StatusCode::NO_CONTENT {
        (left, right)
    } else {
        (right, left)
    };
    status(
        issue(&client, &api, OWNER, &winner.0, &winner.1, "read"),
        StatusCode::NO_CONTENT,
    )
    .await?;
    status(
        issue(&client, &api, OWNER, &winner.0, &winner.1, "write"),
        StatusCode::CONFLICT,
    )
    .await?;
    status(
        client
            .get(format!("{base}/api/repositories"))
            .bearer_auth(&loser.1),
        StatusCode::UNAUTHORIZED,
    )
    .await?;
    let full = all_tokens(&client, &api).await?;
    assert_eq!(full.len(), 64);
    // Account creation retries cannot reset usage or mint another initial token.
    status(
        client
            .post(format!("{base}/api/accounts"))
            .bearer_auth(OWNER)
            .json(&account),
        StatusCode::OK,
    )
    .await?;
    status(
        issue(&client, &api, OWNER, &loser.0, &loser.1, "read"),
        StatusCode::TOO_MANY_REQUESTS,
    )
    .await?;
    let revoke = full
        .iter()
        .find_map(|row| {
            row["id"]
                .as_str()
                .filter(|id| *id != initial_id && *id != winner.0)
        })
        .ok_or("revocable token")?;
    status(
        client.delete(format!("{api}/{revoke}")).bearer_auth(OWNER),
        StatusCode::NO_CONTENT,
    )
    .await?;
    status(
        issue(&client, &api, OWNER, &loser.0, &loser.1, "read"),
        StatusCode::NO_CONTENT,
    )
    .await?;
    server.shutdown().await?;

    let address = available_address().await?;
    let server = CanopyServer::start(
        config(address, workspace.path().join("restored")),
        Arc::clone(&store),
    )
    .await?;
    let base = format!("http://{address}");
    let api = format!("{base}/api/accounts/limited/tokens");
    let refused = credential();
    status(
        issue(&client, &api, OWNER, &refused.0, &refused.1, "read"),
        StatusCode::TOO_MANY_REQUESTS,
    )
    .await?;
    let records = all_tokens(&client, &api).await?;
    assert_eq!(records.len(), 65);
    // Release active slots, retaining issuance history. Neither caller identity
    // nor revocation may reset the account's rolling issuance budget.
    for row in &records {
        let id = row["id"].as_str().ok_or("token ID")?;
        if id != initial_id && row["enabled"] == true {
            status(
                client.delete(format!("{api}/{id}")).bearer_auth(OWNER),
                StatusCode::NO_CONTENT,
            )
            .await?;
        }
    }
    for _ in records.len()..256 {
        let (id, token) = credential();
        status(
            issue(&client, &api, &administrator, &id, &token, "read"),
            StatusCode::NO_CONTENT,
        )
        .await?;
        status(
            client.delete(format!("{api}/{id}")).bearer_auth(OWNER),
            StatusCode::NO_CONTENT,
        )
        .await?;
    }
    let response = issue(
        &client,
        &api,
        &administrator,
        &refused.0,
        &refused.1,
        "read",
    )
    .send()
    .await?;
    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    assert!(response.text().await?.contains("past 24 hours"));
    status(
        issue(&client, &api, OWNER, initial_id, &administrator, "admin"),
        StatusCode::NO_CONTENT,
    )
    .await?;
    status(
        client
            .get(format!("{base}/api/repositories"))
            .bearer_auth(&refused.1),
        StatusCode::UNAUTHORIZED,
    )
    .await?;
    assert_eq!(all_tokens(&client, &api).await?.len(), 256);
    // A second account has independent limits, even when the same admin issues.
    status(
        issue(
            &client,
            &format!("{base}/api/accounts/canopy/tokens"),
            OWNER,
            &refused.0,
            &refused.1,
            "read",
        ),
        StatusCode::NO_CONTENT,
    )
    .await?;
    server.shutdown().await?;

    let address = available_address().await?;
    let server = CanopyServer::start(
        config(address, workspace.path().join("daily-restored")),
        store,
    )
    .await?;
    let api = format!("http://{address}/api/accounts/limited/tokens");
    let (id, token) = credential();
    status(
        issue(&client, &api, OWNER, &id, &token, "read"),
        StatusCode::TOO_MANY_REQUESTS,
    )
    .await?;
    assert_eq!(all_tokens(&client, &api).await?.len(), 256);
    server.shutdown().await?;
    Ok(())
}
