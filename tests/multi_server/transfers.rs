use super::*;
use sha2::{Digest as _, Sha256};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    time::{Duration, timeout},
};

#[tokio::test(flavor = "multi_thread")]
async fn corrupt_lfs_download_cannot_complete_its_http_content_length()
-> Result<(), Box<dyn std::error::Error>> {
    use object_store::ObjectStoreExt;
    let files = tempfile::TempDir::new()?;
    let address = available_address().await?;
    let settings = config(address, files.path().join("server"));
    let prefix = settings.store_prefix.clone();
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let server = CanopyServer::start(settings, store.clone()).await?;
    let url = create_repository(address, "corruption").await?;
    let client = reqwest::Client::new();
    let repository: serde_json::Value = client
        .get(format!("http://{address}/api/repositories/corruption"))
        .bearer_auth("local-test-token")
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let id = uuid::Uuid::parse_str(repository["repository_id"].as_str().ok_or("missing id")?)?;
    let mut bytes = vec![11; 9 * 1024 * 1024];
    let oid = hex::encode(Sha256::digest(&bytes));
    let endpoint = format!("{url}/info/lfs/objects/{oid}");
    client
        .put(&endpoint)
        .bearer_auth("local-test-token")
        .body(bytes.clone())
        .send()
        .await?
        .error_for_status()?;
    let key = StorePath::from(format!(
        "{prefix}/repos/{}/lfs/{oid}.parts/0000000000000001",
        id.simple()
    ));
    bytes[8 * 1024 * 1024] = 12;
    store
        .put(&key, bytes[8 * 1024 * 1024..].to_vec().into())
        .await?;
    let response = client
        .get(&endpoint)
        .bearer_auth("local-test-token")
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    assert_eq!(response.content_length(), Some(9 * 1024 * 1024));
    assert!(response.bytes().await.is_err());
    server.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn account_transfer_quotas_preserve_other_accounts_and_disconnect_allows_retry()
-> Result<(), Box<dyn std::error::Error>> {
    let workspace = tempfile::TempDir::new()?;
    let address = available_address().await?;
    let server = CanopyServer::start(
        config(address, workspace.path().join("node")),
        Arc::new(InMemory::new()),
    )
    .await?;
    for name in ["first", "second"] {
        create_repository(address, name).await?;
    }
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()?;
    let base = format!("http://{address}");
    let member = format!("cnp_{}", hex::encode([41; 32]));
    let observer = format!("cnp_{}", hex::encode([42; 32]));
    let owner_alias = format!("cnp_{}", hex::encode([43; 32]));
    for (account, token) in [("member", &member), ("observer", &observer)] {
        client
            .post(format!("{base}/api/accounts"))
            .bearer_auth("local-test-token")
            .json(&serde_json::json!({"name":account,"token":token,"scope":"write"}))
            .send()
            .await?
            .error_for_status()?
            .bytes()
            .await?;
        for name in ["first", "second"] {
            client
                .put(format!(
                    "{base}/api/repositories/{name}/collaborators/{account}"
                ))
                .bearer_auth("local-test-token")
                .json(&serde_json::json!({"role":"write"}))
                .send()
                .await?
                .error_for_status()?
                .bytes()
                .await?;
        }
    }
    client.post(format!("{base}/api/accounts/canopy/tokens"))
        .bearer_auth("local-test-token")
        .json(&serde_json::json!({"id":uuid::Uuid::new_v4().to_string(),"token":owner_alias,"scope":"write"}))
        .send().await?.error_for_status()?.bytes().await?;
    let body = b"admitted LFS upload";
    let oid = hex::encode(Sha256::digest(body));
    let mut uploads = Vec::new();
    let refs = format!("{base}/canopy/first.git/info/refs?service=git-upload-pack");
    for index in 0..8 {
        if index == 4 {
            // Tokens and repository names cannot split one account's quota.
            full(&client, &base, &owner_alias, &oid).await?;
            let response = client
                .get(&refs)
                .bearer_auth(&member)
                .send()
                .await?
                .error_for_status()?;
            assert!(
                response
                    .bytes()
                    .await?
                    .starts_with(b"001e# service=git-upload-pack\n")
            );
        }
        let token = if index >= 4 {
            member.as_str()
        } else if index % 2 == 0 {
            "local-test-token"
        } else {
            owner_alias.as_str()
        };
        let name = if index % 2 == 0 { "first" } else { "second" };
        let mut stream = TcpStream::connect(address).await?;
        let headers = format!(
            "PUT /canopy/{name}.git/info/lfs/objects/{oid} HTTP/1.1\r\nHost: {address}\r\nAuthorization: Bearer {token}\r\nContent-Length: {}\r\nExpect: 100-continue\r\nConnection: close\r\n\r\n",
            body.len()
        );
        stream.write_all(headers.as_bytes()).await?;
        let mut response = vec![0; b"HTTP/1.1 100 Continue\r\n\r\n".len()];
        timeout(Duration::from_secs(5), stream.read_exact(&mut response)).await??;
        assert_eq!(response, b"HTTP/1.1 100 Continue\r\n\r\n");
        uploads.push(stream);
    }
    // Two independent accounts consume the node's eight slots; a third is bounded.
    full(&client, &base, &observer, &oid).await?;
    for path in ["healthz", "readyz", "api/repositories"] {
        let response = client
            .get(format!("http://{address}/{path}"))
            .bearer_auth("local-test-token")
            .send()
            .await?;
        assert_eq!(response.status(), reqwest::StatusCode::OK);
        response.bytes().await?;
    }
    drop(uploads.remove(0));
    timeout(Duration::from_secs(5), async {
        loop {
            let response = client
                .get(&refs)
                .bearer_auth("local-test-token")
                .send()
                .await?;
            if response.status() == reqwest::StatusCode::OK {
                assert!(
                    response
                        .bytes()
                        .await?
                        .starts_with(b"001e# service=git-upload-pack\n")
                );
                break Ok::<_, reqwest::Error>(());
            }
            assert_eq!(response.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await??;
    for mut stream in uploads {
        stream.write_all(body).await?;
        let mut response = Vec::new();
        timeout(Duration::from_secs(5), stream.read_to_end(&mut response)).await??;
        assert!(
            response.starts_with(b"HTTP/1.1 200 OK\r\n"),
            "{}",
            String::from_utf8_lossy(&response)
        );
    }
    for name in ["first", "second"] {
        let fetched = client
            .get(format!(
                "http://{address}/canopy/{name}.git/info/lfs/objects/{oid}"
            ))
            .bearer_auth("local-test-token")
            .send()
            .await?
            .error_for_status()?
            .bytes()
            .await?;
        assert_eq!(fetched.as_ref(), body);
    }
    server.shutdown().await?;
    Ok(())
}

async fn full(
    client: &reqwest::Client,
    base: &str,
    token: &str,
    oid: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    for path in [
        format!("{base}/canopy/first.git/info/refs?service=git-upload-pack"),
        format!("{base}/canopy/second.git/info/lfs/objects/{oid}"),
    ] {
        let response = client.get(path).bearer_auth(token).send().await?;
        assert_eq!(response.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(response.headers()["retry-after"], "1");
    }
    let response = client
        .post(format!("{base}/api/repositories/first/browse"))
        .bearer_auth(token)
        .json(&serde_json::json!({}))
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(response.headers()["retry-after"], "1");
    for operation in ["comparison", "threads", "merge", "merge-candidates"] {
        let response = client
            .post(format!("{base}/api/repositories/first/pulls/1/{operation}"))
            .bearer_auth(token)
            .json(&serde_json::json!({}))
            .send()
            .await?;
        assert_eq!(response.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(response.headers()["retry-after"], "1");
        response.bytes().await?;
    }
    Ok(())
}
