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
async fn production_browser_refs_use_certified_joint_roots_and_ignore_legacy_ref_tables() -> Result
{
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let (server, _files) = server().await?;
        let manager = server.repositories.clone();
        let entry = create(&manager, "browser-refs", format).await?;
        let (repository, _, _) = loaded(&manager, entry.repository_id).await?;
        // Trusted divergence isolates consumer authority. This old row must not
        // become visible without publication of the immutable joint ref root.
        repository.sql.batch(crate::server::mutation_identity()?, SqlBatch {
            statements: vec![
                SqlStatement { sql: "UPDATE ref_generation SET default_branch='refs/heads/legacy',generation=99 WHERE singleton=1".into(), parameters: vec![] },
                SqlStatement { sql: "INSERT INTO refs(name,oid,version) VALUES('refs/heads/legacy',?1,1)".into(), parameters: vec![SqlValue::Blob(missing(format).to_vec())] },
            ],
        }).await?;
        let client = reqwest::Client::new();
        let url = format!(
            "http://{}/api/repositories/browser-refs/browse",
            server.address
        );
        for query in [
            serde_json::json!({"kind":"resolve"}),
            serde_json::json!({"kind":"refs"}),
        ] {
            let response = client
                .post(&url)
                .bearer_auth("local-recovery-test")
                .json(&serde_json::json!({
                    "repository_id": uuid::Uuid::from_bytes(entry.repository_id).to_string(), "query":query,
                }))
                .send()
                .await?;
            assert_eq!(response.status(), reqwest::StatusCode::OK);
            let body: serde_json::Value = response.json().await?;
            if query["kind"] == "resolve" {
                assert_eq!(body["view"]["resolved"]["reference"], "refs/heads/main");
                assert!(body["view"]["resolved"]["oid"].is_null());
                assert!(body["view"]["resolved"]["version"].is_null());
                assert_eq!(body["view"]["resolved"]["generation"], 0);
            } else {
                assert_eq!(body["view"]["refs"]["default_branch"], "refs/heads/main");
                assert_eq!(body["view"]["refs"]["entries"], serde_json::json!([]));
                assert_eq!(body["view"]["refs"]["generation"], 0);
            }
        }
        let changed = client.post(&url).bearer_auth("local-recovery-test").json(&serde_json::json!({
            "repository_id": uuid::Uuid::from_bytes(entry.repository_id).to_string(), "query":{"kind":"refs","generation":99},
        })).send().await?;
        assert_eq!(changed.status(), reqwest::StatusCode::CONFLICT);
        let long = format!("refs/heads/{}", "\"".repeat(65_000));
        let continued = client
            .post(&url)
            .bearer_auth("local-recovery-test")
            .json(&serde_json::json!({
                "repository_id": uuid::Uuid::from_bytes(entry.repository_id).to_string(),
                "query":{"kind":"refs","generation":0,"after":long},
            }))
            .send()
            .await?;
        assert_eq!(continued.status(), reqwest::StatusCode::OK);
        let oversized = format!("refs/heads/{}", "x".repeat(65_536));
        let invalid = client
            .post(&url)
            .bearer_auth("local-recovery-test")
            .json(&serde_json::json!({
                "repository_id": uuid::Uuid::from_bytes(entry.repository_id).to_string(),
                "query":{"kind":"refs","generation":0,"after":oversized},
            }))
            .send()
            .await?;
        assert_eq!(invalid.status(), reqwest::StatusCode::UNPROCESSABLE_ENTITY);
        let anonymous = client
            .post(&url)
            .json(&serde_json::json!({
                "repository_id": uuid::Uuid::from_bytes(entry.repository_id).to_string(), "query":{"kind":"resolve"},
            }))
            .send()
            .await?;
        assert_ne!(anonymous.status(), reqwest::StatusCode::OK);
        drop(repository);
        timeout(Duration::from_secs(10), server.shutdown()).await??;
    }
    Ok(())
}

#[tokio::test]
async fn shutdown_refuses_unpublished_serving_constructor_and_joins_it_before_workspace_release()
-> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let (server, files) = server().await?;
        let manager = server.repositories.clone();
        let (entered, observed) = tokio::sync::oneshot::channel();
        let (proceed, waiting) = tokio::sync::oneshot::channel();
        *manager.serving_construction_gate.lock().await = Some((entered, waiting));
        let work = manager.clone();
        let creating = tokio::spawn(async move { work.create("late", format).await });
        timeout(Duration::from_secs(8), observed).await??;
        let repository = manager
            .loaded
            .lock()
            .await
            .values()
            .find(|loaded| loaded.name == "late")
            .ok_or("late resident absent")?
            .repository
            .clone();
        // This public capability must be unavailable until its lifecycle owner
        // is registered in the manager's drain inventory.
        let premature = repository
            .serving_snapshot(ReadIdentity::Account("canopy"))
            .await;
        let unavailable = premature.is_err();
        assert!(repository.staging_coordinator().is_err());
        drop(premature);
        let mut shutdown = tokio::spawn(server.shutdown());
        timeout(Duration::from_secs(8), manager.serving_stop.cancelled()).await?;
        assert!(
            timeout(Duration::from_millis(50), &mut shutdown)
                .await
                .is_err()
        );
        assert!(!manager.publication_budget.stats().closed);
        assert!(
            crate::server::workspace::Workspace::open(&files.path().join("node"))
                .is_err_and(|error| error.kind() == std::io::ErrorKind::WouldBlock)
        );
        proceed.send(()).map_err(|_| "construction disappeared")?;
        let result = timeout(Duration::from_secs(8), creating).await??;
        timeout(Duration::from_secs(10), shutdown).await???;
        assert!(
            unavailable,
            "unpublished constructor exposed serving before registered ownership"
        );
        assert!(repository.staging_coordinator().is_err());
        assert!(matches!(
            result,
            Err(crate::server::ServerError::Runtime(
                cellule_runtime::Error::CellDraining
            ))
        ));
        assert!(
            repository
                .serving_snapshot(ReadIdentity::Account("canopy"))
                .await
                .is_err()
        );
    }
    Ok(())
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

mod browser;
