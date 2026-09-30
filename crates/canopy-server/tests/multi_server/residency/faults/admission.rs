use super::*;

#[tokio::test(flavor = "multi_thread")]
async fn one_accounts_cold_requests_leave_capacity_for_another_account() -> Result {
    let fixture = Fixture::new().await?;
    let base = format!("http://{}", fixture.address);
    let reader = format!("cnp_{}", hex::encode([91; 32]));
    fixture
        .client
        .post(format!("{base}/api/accounts"))
        .bearer_auth("local-test-token")
        .json(&serde_json::json!({"name":"reader","token":reader,"scope":"read"}))
        .send()
        .await?
        .error_for_status()?;
    fixture
        .client
        .put(format!(
            "{base}/api/repositories/second/collaborators/reader"
        ))
        .bearer_auth("local-test-token")
        .json(&serde_json::json!({"role":"read"}))
        .send()
        .await?
        .error_for_status()?;
    let second = fixture
        .client
        .get(format!("{base}/api/repositories/second"))
        .bearer_auth(&reader)
        .send()
        .await?
        .error_for_status()?
        .json::<serde_json::Value>()
        .await?;
    let second = uuid::Uuid::parse_str(second["repository_id"].as_str().ok_or("repository id")?)?;
    // All original repositories are cold. The other account must actually
    // activate a Cell; reading an already resident route cannot prove isolation.
    for name in ["fourth", "fifth", "sixth"] {
        create(&fixture.client, fixture.address, name).await?;
    }
    assert!(!fixture.repository_dir.exists());
    assert!(
        !fixture
            .repository_dir
            .parent()
            .ok_or("repository directory")?
            .join(second.simple().to_string())
            .exists()
    );
    *fixture.store.paused_read.lock().unwrap() = Some(
        fixture
            .layout
            .control_path(fixture.target.cell_id().as_bytes()),
    );
    let request = || {
        fixture
            .client
            .get(format!("{base}/api/repositories/original"))
            .bearer_auth("local-test-token")
    };
    let first = request();
    let first = tokio::spawn(async move { first.send().await });
    fixture.store.wait().await?;
    let mut waiting = Vec::new();
    let outcome = async {
        for _ in 0..15 {
            let request = request();
            let mut waiter = tokio::spawn(async move { request.send().await });
            assert!(
                timeout(Duration::from_millis(50), &mut waiter)
                    .await
                    .is_err()
            );
            waiting.push(waiter);
        }
        let response = timeout(Duration::from_secs(2), request().send()).await??;
        assert_eq!(response.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
        for path in [
            "/api/repositories/original/issues",
            "/canopy/original.git/info/refs?service=git-upload-pack",
        ] {
            let request = fixture
                .client
                .get(format!("{base}{path}"))
                .bearer_auth("local-test-token")
                .header("Git-Protocol", "version=2");
            let response = timeout(Duration::from_secs(2), request.send()).await??;
            assert_eq!(response.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
        }
        let other = fixture
            .client
            .get(format!("{base}/api/repositories/second/issues"))
            .bearer_auth(&reader);
        let response = timeout(Duration::from_secs(3), other.send())
            .await??
            .error_for_status()?
            .json::<serde_json::Value>()
            .await?;
        assert_eq!(response["issues"], serde_json::json!([]));
        for waiter in waiting.drain(..) {
            waiter.abort();
            let _ = waiter.await;
        }
        // HTTP cancellation cannot release capacity held by supervised Cell work.
        let response = timeout(Duration::from_secs(2), request().send()).await??;
        assert_eq!(response.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
        let warm = fixture
            .client
            .get(format!(
                "{base}/canopy/second.git/info/refs?service=git-upload-pack"
            ))
            .bearer_auth("local-test-token")
            .header("Git-Protocol", "version=2");
        let bytes = timeout(Duration::from_secs(2), warm.send())
            .await??
            .error_for_status()?
            .bytes()
            .await?;
        assert!(bytes.starts_with(b"000eversion 2\n"));
        Ok::<_, Box<dyn std::error::Error>>(())
    }
    .await;
    fixture.store.proceed.notify_one();
    let first = first.await??;
    for waiter in waiting {
        let _ = waiter.await?;
    }
    outcome?;
    first.error_for_status()?;
    fixture.clone_original(fixture.address).await?;
    fixture.server.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn cold_admission_stays_bounded_after_clients_disconnect() -> Result {
    let fixture = Fixture::new().await?;
    let pending = fixture.interrupt_release(ReleaseFault::Pause).await?;
    let request = || {
        fixture
            .client
            .get(format!(
                "http://{}/api/repositories/original",
                fixture.address
            ))
            .bearer_auth("local-test-token")
    };
    let mut waiting = Vec::new();
    let overload = async {
        // Fill activation admission gradually so Directory query admission does
        // not reject the burst first. One of this account's 16 permits belongs
        // to release; cancellation must retain both global and account charges.
        for _ in 0..15 {
            let request = request();
            let mut waiter = tokio::spawn(async move { request.send().await });
            assert!(
                timeout(Duration::from_millis(50), &mut waiter)
                    .await
                    .is_err()
            );
            waiting.push(waiter);
        }
        let response = timeout(Duration::from_secs(2), request().send()).await??;
        assert_eq!(response.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
        for waiter in waiting.drain(..) {
            waiter.abort();
            let _ = waiter.await;
        }
        let response = timeout(Duration::from_secs(2), request().send()).await??;
        assert_eq!(response.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
        fixture.read_warm_repository().await
    }
    .await;
    fixture.store.proceed.notify_one();
    pending.await??.error_for_status()?;
    overload?;
    create(&fixture.client, fixture.address, "fourth").await?;
    fixture.clone_original(fixture.address).await?;
    fixture.server.shutdown().await?;
    Ok(())
}
