use std::{collections::VecDeque, convert::Infallible, future::poll_fn};

use super::*;

struct Frames(VecDeque<Frame<Bytes>>);

impl HttpBody for Frames {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Infallible>>> {
        Poll::Ready(self.0.pop_front().map(Ok))
    }
}

#[tokio::test]
async fn a_streamed_response_pins_residency_through_data_and_trailers() {
    let pin = Arc::new(());
    let mut trailers = axum::http::HeaderMap::new();
    trailers.insert("x-test", "complete".parse().unwrap());
    let mut body = PinnedBody {
        body: Body::new(Frames(VecDeque::from([
            Frame::data(Bytes::from_static(b"payload")),
            Frame::trailers(trailers),
        ]))),
        pin: Some(Arc::clone(&pin)),
    };
    let frame = poll_fn(|cx| Pin::new(&mut body).poll_frame(cx))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(frame.into_data().unwrap(), b"payload"[..]);
    assert_eq!(Arc::strong_count(&pin), 2);
    let frame = poll_fn(|cx| Pin::new(&mut body).poll_frame(cx))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(frame.into_trailers().unwrap()["x-test"], "complete");
    assert_eq!(Arc::strong_count(&pin), 2);
    assert!(
        poll_fn(|cx| Pin::new(&mut body).poll_frame(cx))
            .await
            .is_none()
    );
    assert_eq!(Arc::strong_count(&pin), 1);
}

#[test]
fn disconnect_releases_a_response_pin_without_polling_the_body() {
    let pin = Arc::new(());
    let response = Body::new(PinnedBody {
        body: Body::from("unread"),
        pin: Some(Arc::clone(&pin)),
    });
    assert_eq!(Arc::strong_count(&pin), 2);
    drop(response);
    assert_eq!(Arc::strong_count(&pin), 1);
}

// Exercise the real HTTP cache after the runtime interrupts a started read and
// releases its Cell. A passing low-level fence test alone cannot cover this seam.
#[tokio::test(flavor = "multi_thread")]
async fn metadata_after_started_query_deadline_reacquires_released_cell()
-> Result<(), Box<dyn std::error::Error>> {
    use super::super::{RunningServer, ServerConfig};
    use cellule_runtime::{
        ApplicationId, Digest, TenantId, control::ControlState, control::authority::CellAuthority,
        identity::NodeId, ltx::CellStorageLayout,
    };
    use cellule_store::Store;
    use ed25519_dalek::SigningKey;
    use object_store::{ObjectStore, memory::InMemory, path::Path as StorePath};
    use std::time::Duration;

    let workspace = tempfile::TempDir::new()?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let tenant = TenantId::from_bytes([61; 16]);
    let application = ApplicationId::from_bytes([62; 16]);
    let prefix = StorePath::from("query-deadline-residency-test");
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let layout = CellStorageLayout::new(
        Store::new(Arc::clone(&store)),
        prefix.clone(),
        *application.as_bytes(),
    );
    let server = RunningServer::start(
        ServerConfig {
            tenant,
            application,
            node: NodeId::from_bytes([64; 16]),
            fleet: Digest::from_bytes([65; 32]),
            image: Digest::from_bytes([66; 32]),
            signing_key: SigningKey::from_bytes(&[67; 32]),
            owner: "canopy".into(),
            token: "local-test-token".into(),
            public_url: format!("http://{address}"),
            peer_endpoint: "https://canopy.test".into(),
            peer_ca_pem: None,
            listen: address,
            ssh: None,
            data_dir: workspace.path().join("server"),
            store_prefix: prefix,
            local_disk_limit_bytes: 1 << 30,
            max_active_repositories: 3,
        },
        store,
        Some(listener),
    )
    .await?;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()?;
    let outcome = async {
        let mut repositories = Vec::new();
        for name in ["timed-out-read", "healthy"] {
            let value = client
                .post(format!("http://{address}/api/repositories"))
                .bearer_auth("local-test-token")
                .json(&serde_json::json!({"name": name}))
                .send()
                .await?
                .error_for_status()?
                .json::<serde_json::Value>()
                .await?;
            repositories.push(uuid::Uuid::parse_str(
                value["repository_id"].as_str().ok_or("repository id missing")?,
            )?);
        }
        // Populate Canopy's normal metadata fast path before fencing its handle.
        client
            .get(format!("http://{address}/api/repositories/timed-out-read"))
            .bearer_auth("local-test-token")
            .send()
            .await?
            .error_for_status()?;
        let target = repository_target(tenant, application, repositories[0].into_bytes())?;
        let handle = server
            .node
            .runtime()
            .resident_handle(&target, cellule_runtime::cell::catalog::CatalogRole::Sql)
            .await?
            .ok_or("repository has no resident handle")?;
        let authority = CellAuthority::new(layout);
        let before = authority.load(target.cell_id()).await?.ok_or("control missing")?;
        let interrupted = tokio::time::timeout(
            Duration::from_secs(7),
            handle.query(64, 64, |connection| {
                let value = connection.query_row(
                    "WITH RECURSIVE counter(value) AS (VALUES(0) UNION ALL SELECT value + 1 FROM counter WHERE value < 1000000000) SELECT sum(value) FROM counter",
                    [],
                    |row| row.get::<_, i64>(0),
                )?;
                Ok(value.to_be_bytes().to_vec())
            }),
        )
        .await?;
        assert!(matches!(interrupted, Err(Error::Deadline)), "{interrupted:?}");
        assert!(matches!(
            handle.query(1, 1, |_| Ok(Vec::new())).await,
            Err(Error::Fenced)
        ));
        let released = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let control = authority.load(target.cell_id()).await?.ok_or("control missing")?;
                if control.value().state == ControlState::Idle && control.value().owner.is_none() {
                    break Ok::<_, Box<dyn std::error::Error>>(control);
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await??;
        assert_eq!(released.value().root, before.value().root);
        let healthy = client
            .get(format!("http://{address}/api/repositories/healthy"))
            .bearer_auth("local-test-token")
            .send()
            .await?
            .error_for_status()?
            .json::<serde_json::Value>()
            .await?;
        assert_eq!(healthy["repository_id"], repositories[1].to_string());
        let response = client
            .get(format!("http://{address}/api/repositories/timed-out-read"))
            .bearer_auth("local-test-token")
            .send()
            .await?;
        let status = response.status();
        let body = response.text().await?;
        // Drain the owned node before asserting the signal, even on the red run.
        Ok::<_, Box<dyn std::error::Error>>((status, body, repositories[0]))
    }
    .await;
    let drained = server.shutdown().await;
    let (status, body, repository) = outcome?;
    drained?;
    assert_eq!(
        status,
        reqwest::StatusCode::OK,
        "released Cell metadata: {body}"
    );
    let value: serde_json::Value = serde_json::from_str(&body)?;
    assert_eq!(value["repository_id"], repository.to_string());
    Ok(())
}
