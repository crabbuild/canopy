//! Exercise the actual manager, shared pool and server shutdown, not fixture wiring.
use super::recovery::{create, loaded, server};
use super::*;
use crate::{ObjectFormat, ObjectId};
use cellule_runtime::primitives::sql::{SqlBatch, SqlStatement, SqlValue};
use tokio::time::{Duration, timeout};
type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

async fn retained(repository: &RepositoryCell) -> Result<i64> {
    let result = repository
        .sql
        .query(
            None,
            SqlBatch {
                statements: vec![SqlStatement {
                    sql: "SELECT count(*) FROM catalog_serving_pins".into(),
                    parameters: vec![],
                }],
            },
        )
        .await?;
    let Some([SqlValue::Integer(count)]) = result
        .output
        .first()
        .and_then(|set| set.rows.first())
        .map(Vec::as_slice)
    else {
        return Err("serving count absent".into());
    };
    Ok(*count)
}
fn missing(format: ObjectFormat) -> ObjectId {
    match format {
        ObjectFormat::Sha1 => ObjectId::Sha1([7; 20]),
        ObjectFormat::Sha256 => ObjectId::Sha256([7; 32]),
    }
}

#[tokio::test]
async fn production_resident_shares_generation_and_busy_eviction_resumes_before_exact_drain()
-> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let (server, _files) = server().await?;
        let entry = create(&server.repositories, "pooled", format).await?;
        let (repository, _client, service) =
            loaded(&server.repositories, entry.repository_id).await?;
        let first = repository
            .serving_snapshot(ReadIdentity::Account("canopy"))
            .await?;
        let second = repository
            .serving_snapshot(ReadIdentity::Account("canopy"))
            .await?;
        assert_eq!(first.fact(), second.fact());
        assert_eq!(retained(&repository).await?, 1);
        assert!(!service.quiesce().await);
        assert!(!service.coordinator.stats().await.closed);
        assert_eq!(first.headers(&[missing(format)]).await?, vec![None]);
        assert!(
            repository
                .serving_snapshot(ReadIdentity::Anonymous)
                .await
                .is_err()
        );
        drop((first, second));
        assert!(timeout(Duration::from_secs(8), service.quiesce()).await?);
        assert_eq!(retained(&repository).await?, 0);
        assert!(service.coordinator.stats().await.closed);
        assert!(service.quiesce().await); // retry after a later Cell release refusal
        assert!(
            repository
                .serving_snapshot(ReadIdentity::Account("canopy"))
                .await
                .is_err()
        );
        server.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn production_shutdown_keeps_publication_cell_heartbeat_and_workspace_until_last_borrow()
-> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let (server, files) = server().await?;
        let manager = server.repositories.clone();
        let entry = create(&manager, "borrowed", format).await?;
        let (repository, _client, service) = loaded(&manager, entry.repository_id).await?;
        let first = repository
            .serving_snapshot(ReadIdentity::Account("canopy"))
            .await?;
        let clone = first.clone();
        let node = server.node.clone();
        let directory = server.directory.clone();
        let session = server.advertisement.lock().await.advertisement().session();
        let mut shutdown = tokio::spawn(server.shutdown());
        timeout(Duration::from_secs(8), async {
            loop {
                if repository
                    .serving_snapshot(ReadIdentity::Account("canopy"))
                    .await
                    .is_err()
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await?;
        assert!(!manager.publication_budget.stats().closed);
        assert_eq!(retained(&repository).await?, 1);
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
        drop(first);
        assert!(
            timeout(Duration::from_millis(50), &mut shutdown)
                .await
                .is_err()
        );
        assert_eq!(retained(&repository).await?, 1);
        drop(clone);
        timeout(Duration::from_secs(10), shutdown).await???;
        assert!(node.is_shutting_down());
        assert!(manager.publication_budget.stats().closed);
        assert!(service.coordinator.stats().await.closed);
        assert!(
            !directory
                .is_live(session, crate::server::unix_now_ms()?)
                .await?
        );
        timeout(Duration::from_secs(2), service.serving.close_and_drain()).await?;
    }
    Ok(())
}
