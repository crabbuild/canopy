use super::*;
use tokio::time::{Duration, timeout};

#[tokio::test]
async fn scan_pause_joins_active_work_and_preserves_resume_and_terminal_stop() {
    let tasks = TaskTracker::new();
    let budget = RecoveryScanBudget::new(4, tasks).unwrap();
    let settings = budget.settings(RecoveryScanLimits::default(), "owner");
    let control = ScanControl::default();
    let round = control.enter(&settings).await.unwrap();
    let pause = control.pause();
    tokio::pin!(pause);
    assert!(
        timeout(Duration::from_millis(20), &mut pause)
            .await
            .is_err()
    );
    drop(round);
    timeout(Duration::from_secs(1), &mut pause).await.unwrap();
    assert!(
        timeout(Duration::from_millis(20), control.enter(&settings))
            .await
            .is_err()
    );
    control.resume();
    let round = control.enter(&settings).await.unwrap();
    control.stop();
    let pause = control.pause();
    tokio::pin!(pause);
    assert!(
        timeout(Duration::from_millis(20), &mut pause)
            .await
            .is_err()
    );
    drop(round);
    timeout(Duration::from_secs(1), &mut pause).await.unwrap();
    control.resume();
    assert!(control.enter(&settings).await.is_none());
    // A stop observed before interval registration must not wait one interval.
    timeout(Duration::from_millis(50), settings.delay(&control))
        .await
        .unwrap();
}

#[tokio::test]
async fn shared_read_budget_bounds_account_rounds_and_tracks_shutdown_without_cancellation() {
    let tasks = TaskTracker::new();
    let budget = RecoveryScanBudget::new(4, tasks.clone()).unwrap();
    let a = budget.settings(RecoveryScanLimits::default(), "a");
    let another_repository = budget.settings(RecoveryScanLimits::default(), "a");
    let b = budget.settings(RecoveryScanLimits::default(), "b");
    let p1 = a.acquire().await.unwrap();
    let p2 = another_repository.acquire().await.unwrap();
    assert!(another_repository.acquire().await.is_err());
    let p3 = b.acquire().await.unwrap();
    let p4 = b.acquire().await.unwrap();
    assert_eq!(budget.in_flight(), 4);
    drop((p2, p3, p4));
    let (release, waiting) = tokio::sync::oneshot::channel();
    let (entered, observing) = tokio::sync::oneshot::channel();
    let scope = a.clone();
    let task = a.spawn(async move {
        let control = ScanControl::default();
        let round = control.enter(&scope).await.unwrap();
        let _ = entered.send(());
        let _ = waiting.await;
        drop((round, p1));
        assert!(control.enter(&scope).await.is_none());
    });
    observing.await.unwrap();
    budget.close();
    tasks.close();
    assert!(
        timeout(Duration::from_millis(20), tasks.wait())
            .await
            .is_err()
    );
    assert_eq!(budget.in_flight(), 1);
    release.send(()).unwrap();
    timeout(Duration::from_secs(1), tasks.wait()).await.unwrap();
    task.await.unwrap();
    assert_eq!(budget.in_flight(), 0);
    assert!(ScanControl::default().enter(&a).await.is_none());
}

#[test]
fn scanner_settings_reject_invalid_rounds_and_account_keys() {
    for limit in [0, 1, 65, u16::MAX] {
        assert!(matches!(
            RecoveryScanBudget::new(limit, TaskTracker::new()),
            Err(RootRecoveryError::InvalidScanLimits)
        ));
    }
    let budget = RecoveryScanBudget::new(2, TaskTracker::new()).unwrap();
    assert!(
        budget
            .settings(RecoveryScanLimits::default(), "")
            .validate()
            .is_err()
    );
    assert!(
        budget
            .settings(RecoveryScanLimits::default(), &"a".repeat(4097))
            .validate()
            .is_err()
    );
    assert!(
        budget
            .settings(
                RecoveryScanLimits {
                    page: 0,
                    ..RecoveryScanLimits::default()
                },
                "a"
            )
            .validate()
            .is_err()
    );
}
