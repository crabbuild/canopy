use super::*;
use crate::packs::publication::{
    BeginRequest, CustodyAction, DEFAULT_LEASE_MS, LeaseCheck, PreparationAuthority,
    PreparationReply, PreparationSession, PreparedCustody, PublicationError, PublicationOutcome,
    PublicationState, RegisteredCustody,
};
use crate::{
    ObjectFormat,
    server::{RunningServer, ServerConfig, mutation_identity},
};
use cellule_runtime::primitives::sql::{SqlBatch, SqlStatement, SqlValue};
use cellule_runtime::{ApplicationId, SessionId, TenantId, identity::NodeId};
use ed25519_dalek::SigningKey;
use object_store::{memory::InMemory, path::Path as StorePath};
use tokio::time::{Duration, timeout};

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

pub(super) async fn server() -> Result<(RunningServer, tempfile::TempDir)> {
    let files = tempfile::TempDir::new()?;
    let server = RunningServer::start(
        ServerConfig {
            tenant: TenantId::from_bytes([71; 16]),
            application: ApplicationId::from_bytes([72; 16]),
            node: NodeId::from_bytes([73; 16]),
            fleet: cellule_runtime::Digest::from_bytes([74; 32]),
            image: cellule_runtime::Digest::from_bytes([75; 32]),
            signing_key: SigningKey::from_bytes(&[76; 32]),
            owner: "canopy".into(),
            token: "local-recovery-test".into(),
            public_url: "http://127.0.0.1".into(),
            peer_endpoint: "https://recovery.test".into(),
            peer_ca_pem: None,
            listen: "127.0.0.1:0".parse()?,
            ssh: None,
            data_dir: files.path().join("node"),
            store_prefix: StorePath::from("resident-recovery"),
            local_disk_limit_bytes: 1 << 30,
            native_limits: crate::native_resources::NativeLimits::default(),
            max_active_repositories: 3,
        },
        Arc::new(InMemory::new()),
        None,
    )
    .await?;
    Ok((server, files))
}
pub(super) async fn create(
    manager: &Arc<RepositoryManager>,
    name: &str,
    format: ObjectFormat,
) -> Result<RepositoryEntry> {
    Ok(timeout(Duration::from_secs(10), async {
        loop {
            match manager.create(name, format).await {
                Ok(entry) => return Ok(entry),
                // Settlement/movement admission is transient. Retry the same
                // directory reservation, never a different logical repository.
                Err(ServerError::Runtime(Error::CellDraining | Error::Capacity(_))) => {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                Err(error) => return Err(error),
            }
        }
    })
    .await??)
}
pub(super) async fn loaded(
    manager: &RepositoryManager,
    id: [u8; 16],
) -> Result<(Arc<RepositoryCell>, CellClient, Arc<RecoveryServices>)> {
    let loaded = manager.loaded.lock().await;
    let repository = loaded.get(&id).ok_or("repository not resident")?;
    assert!(repository.local && repository.initialized);
    Ok((
        Arc::clone(&repository.repository),
        repository.client.clone(),
        Arc::clone(
            repository
                .recovery
                .as_ref()
                .ok_or("production recovery absent")?,
        ),
    ))
}
fn request(id: [u8; 16]) -> BeginRequest {
    BeginRequest {
        repository: id,
        operation: [81; 16],
        request_digest: [82; 32],
        actor: "canopy".into(),
        lease_ms: DEFAULT_LEASE_MS,
    }
}
async fn held_renewal(
    manager: &RepositoryManager,
    entry: &RepositoryEntry,
) -> Result<crate::packs::publication::PublicationTicket> {
    let (repository, client, service) = loaded(manager, entry.repository_id).await?;
    let prepared = PreparedCustody::prepare(
        &client,
        &repository.target,
        CustodyAction::BeginPreparation(request(entry.repository_id)),
        mutation_identity()?,
    )
    .await?;
    let saved = prepared.register(&client, mutation_identity()?).await?;
    let committed = saved.recover_preparation(&client).await?;
    let PreparationReply::Granted(lease) = committed.output else {
        return Err("preparation refused".into());
    };
    let session = Arc::new(
        PreparationSession::open(
            client,
            repository.target.clone(),
            LeaseCheck {
                token: lease.token,
                actor: "canopy".into(),
            },
            Some(committed.receipt),
            PreparationAuthority::node(manager.peer.clone(), repository.target.clone()),
        )
        .await?,
    );
    Ok(service.coordinator.try_reserve(
        session
            .ready_renew(mutation_identity()?, DEFAULT_LEASE_MS)
            .await?,
    )?)
}

#[tokio::test]
async fn production_scanner_retires_authentic_orphan_without_inventing_original_execution() -> Result
{
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let (server, _files) = server().await?;
        let manager = &server.repositories;
        let entry = create(manager, "orphan", format).await?;
        let (repository, client, _service) = loaded(manager, entry.repository_id).await?;
        let mut identity = mutation_identity()?;
        identity.expires_at_ms = identity.issued_at_ms + 1000;
        let prepared = PreparedCustody::prepare(
            &client,
            &repository.target,
            CustodyAction::BeginPreparation(request(entry.repository_id)),
            identity,
        )
        .await?;
        let original = prepared.evidence().clone();
        let saved = prepared.register(&client, mutation_identity()?).await?;
        drop((prepared, saved));
        let stopped = timeout(Duration::from_secs(10), async {
            loop {
                if let Some(saved) =
                    RegisteredCustody::load_latest(&client, &repository.target, [81; 16]).await?
                    && saved.stop_fact().is_some()
                {
                    return Ok::<_, crate::packs::publication::CustodyError>(saved);
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await??;
        assert_eq!(stopped.evidence(), &original);
        assert!(!stopped.settled());
        let output = repository
            .sql
            .query(
                None,
                SqlBatch {
                    statements: vec![SqlStatement {
                        sql: "SELECT count(*) FROM catalog_operations WHERE id=?1".into(),
                        parameters: vec![SqlValue::Blob(vec![81; 16])],
                    }],
                },
            )
            .await?;
        assert_eq!(output.output[0].rows[0], vec![SqlValue::Integer(0)]);
        drop((client, repository, stopped));
        server.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn production_eviction_joins_idle_scanners_and_preserves_busy_originals_and_restoration()
-> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let (server, _files) = server().await?;
        let manager = &server.repositories;
        let one = create(manager, "one", format).await?;
        let (_, _, service) = loaded(manager, one.repository_id).await?;
        let held = held_renewal(manager, &one).await?;
        assert_eq!(manager.publication_budget.stats().foreground, 1);
        let two = create(manager, "two", format).await?;
        let three = create(manager, "three", format).await?;
        let four = create(manager, "four", format).await?;
        assert_eq!(manager.loaded.lock().await.len(), 3);
        assert!(manager.loaded.lock().await.contains_key(&one.repository_id));
        assert!(matches!(held.state(), PublicationState::Held));
        assert!(!service.coordinator.stats().await.closed);
        assert_eq!(manager.publication_budget.stats().foreground, 1);
        held.discard_held().await?;
        let evicted = {
            let loaded = manager.loaded.lock().await;
            [two, three, four]
                .into_iter()
                .find(|entry| !loaded.contains_key(&entry.repository_id))
                .ok_or("no idle repository evicted")?
        };
        // Production restores the same certified identity after terminal recovery
        // retirement; no legacy SQL objects or compatibility decoder is added.
        timeout(Duration::from_secs(10), async {
            loop {
                match manager
                    .load(ReadIdentity::Account("canopy"), evicted.clone())
                    .await
                {
                    Ok(route) => return Ok(route),
                    Err(ServerError::Runtime(Error::CellDraining | Error::Capacity(_))) => {
                        tokio::time::sleep(Duration::from_millis(20)).await
                    }
                    Err(error) => return Err(error),
                }
            }
        })
        .await??;
        assert!(
            manager
                .loaded
                .lock()
                .await
                .contains_key(&evicted.repository_id)
        );
        assert_eq!(manager.loaded.lock().await.len(), 3);
        assert_eq!(manager.publication_budget.stats().foreground, 0);
        server.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn production_shutdown_keeps_held_command_cell_heartbeat_and_workspace_until_producer_drains()
-> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let (server, files) = server().await?;
        let manager = Arc::clone(&server.repositories);
        let entry = create(&manager, "held", format).await?;
        let held = held_renewal(&manager, &entry).await?;
        let node = Arc::clone(&server.node);
        let directory = server.directory.clone();
        let session: SessionId = server.advertisement.lock().await.advertisement().session();
        let mut shutdown = tokio::spawn(server.shutdown());
        timeout(Duration::from_secs(5), async {
            while !manager.publication_budget.stats().closed {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        assert!(
            timeout(Duration::from_millis(30), &mut shutdown)
                .await
                .is_err()
        );
        assert!(!node.is_shutting_down());
        assert!(
            directory
                .is_live(session, crate::server::unix_now_ms()?)
                .await?
        );
        assert!(
            crate::server::workspace::Workspace::open(&files.path().join("node"))
                .is_err_and(|error| error.kind() == std::io::ErrorKind::WouldBlock)
        );
        assert!(matches!(held.state(), PublicationState::Held));
        assert_eq!(manager.publication_budget.stats().foreground, 1);
        held.discard_held().await?;
        timeout(Duration::from_secs(10), shutdown).await???;
        assert!(node.is_shutting_down());
        assert!(
            !directory
                .is_live(session, crate::server::unix_now_ms()?)
                .await?
        );
        assert_eq!(manager.publication_budget.stats().foreground, 0);
        drop((manager, held));
        let _reopened = crate::server::workspace::Workspace::open(&files.path().join("node"))?;
    }
    Ok(())
}

#[tokio::test]
async fn production_shutdown_recovers_other_repositories_while_one_producer_holds_its_command()
-> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        for fault in [1, 2, 3] {
            let (server, _files) = server().await?;
            let manager = Arc::clone(&server.repositories);
            let entries = [
                create(&manager, "held-first", format).await?,
                create(&manager, "recover-second", format).await?,
            ];
            // Stop automatic discovery without cancelling an owned round. The
            // test observes recovery performed by the actual shutdown owner.
            manager.recovery_scans.close();
            let order: Vec<_> = manager.loaded.lock().await.keys().copied().collect();
            let first = entries
                .iter()
                .find(|entry| entry.repository_id == order[0])
                .unwrap();
            let second = entries
                .iter()
                .find(|entry| entry.repository_id == order[1])
                .unwrap();
            let held = held_renewal(&manager, first).await?;
            let (repository, client, service) = loaded(&manager, second.repository_id).await?;
            let uncertain = held_renewal(&manager, second).await?;
            service.coordinator.fault_for_test(fault);
            uncertain.activate().await?;
            let PublicationState::Uncertain(error) =
                timeout(Duration::from_secs(10), uncertain.wait()).await?
            else {
                return Err("fault did not preserve uncertainty".into());
            };
            let PublicationError::Preparation(cellule_runtime::InvocationError::Pending(original)) =
                error.as_ref()
            else {
                return Err("exact original evidence absent".into());
            };
            let sequence = match client.resolve(original).await? {
                cellule_runtime::Resolution::Committed(receipt) => Some(receipt.commit_sequence()),
                cellule_runtime::Resolution::Absent => None,
                other => return Err(format!("unexpected original resolution {other:?}").into()),
            };
            assert_eq!(sequence.is_some(), fault != 1);
            assert_eq!(manager.publication_budget.stats().foreground, 2);
            let node = Arc::clone(&server.node);
            let mut shutdown = tokio::spawn(server.shutdown());
            // wait() intentionally returns retained uncertainty immediately.
            // Observe its later terminal state without driving recovery here.
            let resolved = timeout(Duration::from_secs(10), async {
                loop {
                    let state = uncertain.state();
                    if matches!(
                        state,
                        PublicationState::Finished(_) | PublicationState::Discarded
                    ) {
                        return state;
                    }
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await?;
            let PublicationState::Finished(Ok(PublicationOutcome::Preparation(outcome))) = resolved
            else {
                return Err(
                    format!("shutdown did not recover the exact renewal: {resolved:?}").into(),
                );
            };
            let saved = RegisteredCustody::load_latest(&client, &repository.target, [81; 16])
                .await?
                .ok_or("renewal registration absent")?;
            assert_eq!(saved.evidence(), original.as_ref());
            assert_eq!(outcome.committed, saved.recover_preparation(&client).await?);
            if let Some(sequence) = sequence {
                assert_eq!(outcome.committed.receipt.commit_sequence, sequence);
            }
            assert!(
                timeout(Duration::from_millis(30), &mut shutdown)
                    .await
                    .is_err()
            );
            assert!(!node.is_shutting_down());
            assert!(matches!(held.state(), PublicationState::Held));
            assert_eq!(manager.publication_budget.stats().foreground, 1);
            assert!(service.coordinator.stats().await.closed);
            held.discard_held().await?;
            timeout(Duration::from_secs(10), shutdown).await???;
            assert!(node.is_shutting_down());
            assert_eq!(manager.publication_budget.stats().foreground, 0);
        }
    }
    Ok(())
}
