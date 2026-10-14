//! Physical jobs retain original admission after the async producer is gone.
use super::*;

// Always release a blocked test job, including after an assertion or timeout.
struct Release(Option<std::sync::mpsc::Sender<()>>);
impl Drop for Release {
    fn drop(&mut self) {
        if let Some(sender) = self.0.take() {
            let _ = sender.send(());
        }
    }
}
fn held_worker(
    context: StagingContext,
) -> (Release, oneshot::Receiver<()>, tokio::task::JoinHandle<()>) {
    let (entered, running) = oneshot::channel();
    let (release, wait) = std::sync::mpsc::channel();
    let owner = context.physical_owner();
    let worker = tokio::task::spawn_blocking(move || {
        let _owner = owner;
        let _ = entered.send(());
        let _ = wait.recv();
    });
    (Release(Some(release)), running, worker)
}

#[tokio::test]
async fn physical_creating_worker_prevents_bind_and_credit_reuse_after_result_transfer() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let f = Fixture::new(format).await?;
        let c = StagingCoordinator::new(
            f.target.clone(),
            StagingLimits {
                workers: 1,
                workers_per_actor: 1,
                ..StagingLimits::default()
            },
            f.authority(),
        )?;
        let ticket = submit(&f, &c, [233; 16], "owner").await?;
        let lease = active(&ticket).await?;
        let work = ticket.spawn(|context| async move { Ok(held_worker(context)) })?;
        let (release, running, worker) = work.wait().await.map_err(|e| e.to_string())?;
        timeout(Duration::from_secs(10), running).await??;
        // The result was transferred and the producer has exited. Only the
        // detached physical job owns the original worker admission now.
        ticket.seal()?;
        assert_eq!(c.stats().workers, 1);
        assert!(matches!(
            ticket.spawn(|_| async { Ok(()) }),
            Err(StagingError::Capacity)
        ));
        assert!(
            f.client()
                .query::<CheckPreparation>(&f.target, None, check(lease.token))
                .await?
                .output
                .is_none()
        );
        assert!(!worker.is_finished());
        drop(release);
        timeout(Duration::from_secs(10), worker).await??;
        assert!(matches!(terminal(&ticket).await?, StagingState::Bound(_)));
        assert_eq!(c.stats().workers, 0);
        assert!(c.close_and_drain().await.is_empty());
        f.runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn physical_bound_worker_outlives_custody_fence_async_abort_and_observer_drop() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let f = Fixture::new(format).await?;
        let c = StagingCoordinator::new(f.target.clone(), StagingLimits::default(), f.authority())?;
        let ticket = bound::bind(&f, &c, [234; 16], "owner").await?;
        let session = ticket.bound_session()?;
        let (physical, running) = oneshot::channel();
        let work = ticket.spawn_bound(move |_, context| async move {
            let held = held_worker(context);
            physical.send(held).map_err(|_| StagingError::Worker)?;
            std::future::pending::<std::result::Result<(), StagingError>>().await
        })?;
        let (release, entered, worker) = timeout(Duration::from_secs(10), running).await??;
        timeout(Duration::from_secs(10), entered).await??;
        drop(work);
        ticket.expire_bound_for_test()?;
        timeout(Duration::from_secs(10), async {
            while !matches!(ticket.state(), StagingState::Fenced(_)) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await?;
        assert!(session.live_lease().is_err());
        assert!(ticket.bound_session().is_err());
        assert_eq!(c.stats().workers, 1);
        assert_eq!(c.stats().admitted, 1);
        drop(ticket);
        let closing = c.clone();
        let close = tokio::spawn(async move { closing.close_and_drain().await });
        timeout(Duration::from_secs(10), async {
            while !c.stats().closed {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        assert!(!close.is_finished());
        drop(release);
        timeout(Duration::from_secs(10), worker).await??;
        assert!(timeout(Duration::from_secs(10), close).await??.is_empty());
        assert_eq!(c.stats().workers, 0);
        assert_eq!(c.stats().admitted, 0);
        f.runtime.shutdown().await?;
    }
    Ok(())
}
