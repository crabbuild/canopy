//! A physical owner retains the node share after all async observers leave.
use super::super::super::staging_service::StagingBudget;
use super::*;

struct Release(Option<std::sync::mpsc::Sender<()>>);
impl Drop for Release {
    fn drop(&mut self) {
        if let Some(sender) = self.0.take() {
            let _ = sender.send(());
        }
    }
}

#[tokio::test]
async fn shared_staging_worker_budget_survives_detached_work_and_terminal_observers() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let one = Fixture::new(format).await?;
        let two = Fixture::new(format).await?;
        let budget = StagingBudget::new(4, 2)?;
        let a = StagingCoordinator::new_with_budget(
            one.target.clone(),
            StagingLimits::default(),
            one.authority(),
            budget.clone(),
        )?;
        let b = StagingCoordinator::new_with_budget(
            two.target.clone(),
            StagingLimits::default(),
            two.authority(),
            budget.clone(),
        )?;
        let first = submit(&one, &a, [211; 16], "owner").await?;
        let second = submit(&two, &b, [212; 16], "owner").await?;
        active(&first).await?;
        active(&second).await?;
        assert_eq!(budget.available(), (2, 2));
        let (release, wait) = std::sync::mpsc::channel();
        let release = Release(Some(release));
        let (entered, running) = oneshot::channel();
        let worker = first
            .spawn(move |context| async move {
                let owner = context.physical_owner();
                Ok(tokio::task::spawn_blocking(move || {
                    let _owner = owner;
                    let _ = entered.send(());
                    let _ = wait.recv();
                }))
            })?
            .wait()
            .await
            .map_err(|error| error.to_string())?;
        timeout(Duration::from_secs(5), running).await??;
        assert_eq!(budget.available(), (2, 1));
        assert!(matches!(
            second.spawn(|_| async { Ok(()) }),
            Err(StagingError::Capacity)
        ));
        first.stop();
        assert!(!worker.is_finished());
        assert!(a.try_quiesce().is_none());
        drop(release);
        timeout(Duration::from_secs(5), worker).await??;
        timeout(Duration::from_secs(5), async {
            while a.stats().admitted != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        assert_eq!(budget.available(), (3, 2));
        // The old terminal observer remains alive, but neither node credit is
        // charged to that observer after the last physical worker exits.
        assert!(matches!(first.state(), StagingState::Stopped));
        assert_eq!(
            second
                .spawn(|_| async { Ok(7) })?
                .wait()
                .await
                .map_err(|e| e.to_string())?,
            7
        );
        let pause = a.try_quiesce().ok_or("idle staging could not pause")?;
        let ready = ReadyStaging::new(
            one.client(),
            one.target.clone(),
            one.begin([213; 16]),
            identity()?,
        )
        .await?;
        let (error, ready) = a.submit(ready).err().ok_or("paused admission opened")?;
        assert!(matches!(error, StagingError::Closed));
        drop(pause); // Cancellation/failure restores admission without new state.
        let retry = a.submit(ready).map_err(|(error, _)| error)?;
        active(&retry).await?;
        assert_eq!(budget.available(), (2, 2));
        assert!(a.close_and_drain().await.is_empty());
        assert!(b.close_and_drain().await.is_empty());
        assert_eq!(budget.available(), (4, 2));
        one.runtime.shutdown().await?;
        two.runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn shared_staging_operation_budget_retains_exact_uncertainty_until_recovery() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        for fault in [1, 2, 3] {
            let fixture = Fixture::new(format).await?;
            let budget = StagingBudget::new(2, 2)?;
            let coordinator = StagingCoordinator::new_with_budget(
                fixture.target.clone(),
                StagingLimits::default(),
                fixture.authority(),
                budget.clone(),
            )?;
            coordinator.fault_for_test(fault);
            let ticket = submit(&fixture, &coordinator, [214; 16], "owner").await?;
            assert!(matches!(
                terminal(&ticket).await?,
                StagingState::Uncertain(_)
            ));
            let original = ticket
                .custody_evidence_for_test()
                .ok_or("custody absent")?
                .0;
            let pending = coordinator.close_and_drain().await;
            assert_eq!(pending.len(), 1);
            assert_eq!(budget.available(), (1, 2));
            assert!(coordinator.try_quiesce().is_none());
            coordinator.recover(&ticket)?;
            assert!(
                timeout(Duration::from_secs(10), coordinator.close_and_drain())
                    .await?
                    .is_empty()
            );
            assert_eq!(budget.available(), (2, 2));
            assert!(matches!(
                fixture.client().resolve(&original).await?,
                cellule_runtime::Resolution::Committed(_)
            ));
            assert_eq!(
                RegisteredCustody::load_latest(&fixture.client(), &fixture.target, [214; 16])
                    .await?
                    .ok_or("exact custody lost")?
                    .evidence()
                    .clone(),
                original
            );
            fixture.runtime.shutdown().await?;
        }
    }
    Ok(())
}
