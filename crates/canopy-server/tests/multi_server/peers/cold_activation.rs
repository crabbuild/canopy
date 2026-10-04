//! Synchronize the actual idle-owner CAS, not just arrival at the gateways.

use std::{
    fmt,
    pin::Pin,
    sync::{
        Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use cellule_runtime::{
    control::{Control, ControlState, authority::CellAuthority},
    ltx::CellStorageLayout,
};
use futures_core::Stream;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use tokio::sync::Barrier;

use super::*;

#[derive(Debug)]
struct CompetingClaims {
    inner: InMemory,
    target: Mutex<Option<StorePath>>,
    claims: AtomicUsize,
    barrier: Barrier,
    delay_claim_reply: bool,
    claim_reply_paused: AtomicBool,
    reply_path: Mutex<Option<StorePath>>,
    reads_while_paused: AtomicUsize,
}

impl CompetingClaims {
    fn new(delay_claim_reply: bool) -> Self {
        Self {
            inner: InMemory::new(),
            target: Mutex::new(None),
            claims: AtomicUsize::new(0),
            barrier: Barrier::new(2),
            delay_claim_reply,
            claim_reply_paused: AtomicBool::new(false),
            reply_path: Mutex::new(None),
            reads_while_paused: AtomicUsize::new(0),
        }
    }
}

impl fmt::Display for CompetingClaims {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("competing-idle-claims")
    }
}

type StoreStream<T> = Pin<Box<dyn Stream<Item = object_store::Result<T>> + Send + 'static>>;

#[async_trait::async_trait]
impl ObjectStore for CompetingClaims {
    async fn put_opts(
        &self,
        path: &StorePath,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        let synchronize = {
            let mut target = self.target.lock().unwrap();
            if target.as_ref() == Some(path)
                && Control::decode(
                    &payload
                        .iter()
                        .flat_map(|bytes| bytes.iter().copied())
                        .collect::<Vec<_>>(),
                )
                .is_ok_and(|control| {
                    control.state == ControlState::Recovering && control.root.is_some()
                })
            {
                if self.claims.fetch_add(1, Ordering::SeqCst) == 1 {
                    // Only these two claims wait; rollback/renewal stays runnable.
                    *target = None;
                }
                true
            } else {
                false
            }
        };
        if synchronize {
            tokio::time::timeout(Duration::from_secs(10), self.barrier.wait())
                .await
                .map_err(|source| object_store::Error::Generic {
                    store: "competing-idle-claims",
                    source: Box::new(source),
                })?;
        }
        let result = self.inner.put_opts(path, payload, options).await;
        if synchronize && result.is_ok() && self.delay_claim_reply {
            // Publish ownership, but do not let its caller restore/activate yet.
            // This tests the gap between winning authority and a serving handle.
            *self.reply_path.lock().unwrap() = Some(path.clone());
            self.claim_reply_paused.store(true, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_secs(1)).await;
            self.claim_reply_paused.store(false, Ordering::SeqCst);
        }
        result
    }

    async fn put_multipart_opts(
        &self,
        path: &StorePath,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(path, options).await
    }

    async fn get_opts(
        &self,
        path: &StorePath,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        if self.claim_reply_paused.load(Ordering::SeqCst)
            && self.reply_path.lock().unwrap().as_ref() == Some(path)
        {
            self.reads_while_paused.fetch_add(1, Ordering::SeqCst);
        }
        self.inner.get_opts(path, options).await
    }

    fn delete_stream(&self, paths: StoreStream<StorePath>) -> StoreStream<StorePath> {
        self.inner.delete_stream(paths)
    }

    fn list(&self, prefix: Option<&StorePath>) -> StoreStream<ObjectMeta> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&StorePath>,
    ) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &StorePath,
        to: &StorePath,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn simultaneous_cold_gateways_follow_the_winning_live_owner() -> Result {
    qualify_competing_cold_gateways(false).await
}

#[tokio::test(flavor = "multi_thread")]
async fn competing_cold_gateway_waits_for_the_winners_serving_handle() -> Result {
    qualify_competing_cold_gateways(true).await
}

async fn qualify_competing_cold_gateways(delay_claim_reply: bool) -> Result {
    let store = Arc::new(CompetingClaims::new(delay_claim_reply));
    let files = tempfile::TempDir::new()?;
    let a = available_address().await?;
    let b = available_address().await?;
    let seed_config = config(a, files.path().join("seed"));
    let tenant = seed_config.tenant;
    let application = seed_config.application;
    let layout = CellStorageLayout::new(
        cellule_store::Store::new(store.clone()),
        seed_config.store_prefix.clone(),
        *application.as_bytes(),
    );
    let seed = CanopyServer::start(seed_config, store.clone()).await?;
    create_repository(a, "cold-race").await?;
    let client = Client::builder().timeout(Duration::from_secs(15)).build()?;
    let detail: serde_json::Value = client
        .get(format!("http://{a}/api/repositories/cold-race"))
        .bearer_auth("local-test-token")
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let repository_id =
        uuid::Uuid::parse_str(detail["repository_id"].as_str().ok_or("missing ID")?)?;
    let target = canopy_server::repository_target(tenant, application, repository_id.into_bytes())?;
    seed.shutdown().await?;
    let authority = CellAuthority::new(layout.clone());
    let before = authority
        .load(target.cell_id())
        .await?
        .ok_or("missing control")?;
    assert_eq!(before.value().state, ControlState::Idle);
    assert!(before.value().root.is_some());

    let (ca, tls) = tls_config()?;
    let (peer_a, _proxy_a) = proxy(a, Arc::clone(&tls)).await?;
    let (peer_b, _proxy_b) = proxy(b, tls).await?;
    let mut settings_a = config(a, files.path().join("first"));
    settings_a.node = NodeId::from_bytes(uuid::Uuid::new_v4().into_bytes());
    settings_a.signing_key = SigningKey::from_bytes(&[71; 32]);
    settings_a.peer_endpoint = peer_a;
    settings_a.peer_ca_pem = Some(ca.clone());
    let mut settings_b = config(b, files.path().join("second"));
    settings_b.node = NodeId::from_bytes(uuid::Uuid::new_v4().into_bytes());
    settings_b.signing_key = SigningKey::from_bytes(&[72; 32]);
    settings_b.peer_endpoint = peer_b;
    settings_b.peer_ca_pem = Some(ca);
    let first = CanopyServer::start(settings_a, store.clone()).await?;
    let second = CanopyServer::start(settings_b, store.clone()).await?;

    *store.target.lock().unwrap() = Some(layout.control_path(target.cell_id().as_bytes()));
    let request = |address| {
        client
            .get(format!("http://{address}/api/repositories/cold-race"))
            .bearer_auth("local-test-token")
            .send()
    };
    let (left, right) = tokio::join!(request(a), request(b));
    let (left, right) = (left?, right?);
    let statuses = [left.status(), right.status()];
    let mut identities = Vec::new();
    for response in [left, right] {
        if response.status() == StatusCode::OK {
            identities.push(response.json::<serde_json::Value>().await?["repository_id"].clone());
        }
    }
    let after = authority
        .load(target.cell_id())
        .await?
        .ok_or("missing control")?;
    let local_path = format!(
        "canopy-pack-v1/{}/repository.sqlite",
        repository_id.simple()
    );
    let local_owners = ["first", "second"]
        .into_iter()
        .filter(|name| files.path().join(name).join(&local_path).exists())
        .count();
    first.shutdown().await?;
    second.shutdown().await?;

    assert_eq!(
        store.claims.load(Ordering::SeqCst),
        2,
        "both gateways must reach the idle-owner CAS"
    );
    assert_eq!(
        after.value().root,
        before.value().root,
        "reads must not publish a new root"
    );
    assert_eq!(
        local_owners, 1,
        "only the winning owner may activate local SQL"
    );
    if delay_claim_reply {
        assert!(
            store.reads_while_paused.load(Ordering::SeqCst) >= 3,
            "the loser must resolve remote authority before the winner can activate"
        );
    }
    assert_eq!(statuses, [StatusCode::OK, StatusCode::OK]);
    assert!(
        identities
            .iter()
            .all(|identity| identity == &detail["repository_id"])
    );
    Ok(())
}
