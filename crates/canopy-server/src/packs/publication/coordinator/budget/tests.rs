use super::*;
use tokio::time::{Duration, timeout};

fn limits() -> PublicationLimits {
    PublicationLimits {
        operations: 8,
        per_actor: 2,
        command_bytes: 3 * COMMAND_RESERVATION + 4 * MAINTENANCE_RESERVATION,
        in_flight: 4,
        maintenance_operations: 4,
        maintenance_in_flight: 2,
        foreground_burst: 3,
    }
}

#[test]
fn aggregate_reservations_keep_class_and_account_headroom() {
    let budget = PublicationBudget::new(limits()).unwrap();
    let other_repository = budget.clone();
    let foreground = PublicationClass::Foreground;
    let maintenance = PublicationClass::Maintenance;
    let a = budget
        .reserve(foreground, "a", COMMAND_RESERVATION)
        .unwrap();
    let b = other_repository
        .reserve(foreground, "a", COMMAND_RESERVATION)
        .unwrap();
    assert!(matches!(
        budget.reserve(foreground, "a", 1),
        Err(PublicationScheduleError::Capacity)
    ));
    let c = budget
        .reserve(foreground, "b", COMMAND_RESERVATION)
        .unwrap();
    assert!(matches!(
        other_repository.reserve(foreground, "b", 1),
        Err(PublicationScheduleError::Capacity)
    ));
    let m1 = budget
        .reserve(maintenance, "a", MAINTENANCE_RESERVATION)
        .unwrap();
    let m2 = other_repository
        .reserve(maintenance, "a", MAINTENANCE_RESERVATION)
        .unwrap();
    assert!(matches!(
        budget.reserve(maintenance, "a", 1),
        Err(PublicationScheduleError::Capacity)
    ));
    let m3 = budget
        .reserve(maintenance, "b", MAINTENANCE_RESERVATION)
        .unwrap();
    let m4 = budget
        .reserve(maintenance, "c", MAINTENANCE_RESERVATION)
        .unwrap();
    assert!(matches!(
        budget.reserve(maintenance, "d", 1),
        Err(PublicationScheduleError::Capacity)
    ));
    let stats = budget.stats();
    assert_eq!(
        (stats.foreground, stats.maintenance, stats.accounts),
        (3, 4, 3)
    );
    assert_eq!(stats.command_bytes, limits().command_bytes);
    drop((a, b, c));
    let foreground_again = budget
        .reserve(foreground, "d", COMMAND_RESERVATION)
        .unwrap();
    drop((foreground_again, m1, m2, m3, m4));
    let stats = budget.stats();
    assert_eq!(
        (
            stats.foreground,
            stats.maintenance,
            stats.accounts,
            stats.command_bytes
        ),
        (0, 0, 0, 0)
    );
}

#[tokio::test]
async fn account_waiters_leave_node_slots_for_other_accounts_and_maintenance() {
    let budget = PublicationBudget::new(limits()).unwrap();
    let mut reservations = Vec::new();
    for class in [PublicationClass::Foreground, PublicationClass::Maintenance] {
        for actor in ["a", "a", "b", "c"] {
            reservations.push(budget.reserve(class, actor, 1).unwrap());
        }
    }
    let foreground = PublicationClass::Foreground;
    let maintenance = PublicationClass::Maintenance;
    let a = budget.dispatch(foreground, "a").await;
    let a_waiter = budget.dispatch(foreground, "a");
    tokio::pin!(a_waiter);
    assert!(
        timeout(Duration::from_millis(20), &mut a_waiter)
            .await
            .is_err()
    );
    // If the account waiter acquired the node gate first, this would hang.
    let b = timeout(Duration::from_secs(1), budget.dispatch(foreground, "b"))
        .await
        .unwrap();
    let c_waiter = budget.dispatch(foreground, "c");
    tokio::pin!(c_waiter);
    assert!(
        timeout(Duration::from_millis(20), &mut c_waiter)
            .await
            .is_err()
    );
    let m_a = timeout(Duration::from_secs(1), budget.dispatch(maintenance, "a"))
        .await
        .unwrap();
    let m_a_waiter = budget.dispatch(maintenance, "a");
    tokio::pin!(m_a_waiter);
    assert!(
        timeout(Duration::from_millis(20), &mut m_a_waiter)
            .await
            .is_err()
    );
    let m_b = timeout(Duration::from_secs(1), budget.dispatch(maintenance, "b"))
        .await
        .unwrap();
    assert_eq!(
        (
            budget.stats().foreground_dispatch,
            budget.stats().maintenance_dispatch
        ),
        (2, 2)
    );
    budget.close();
    assert!(matches!(
        budget.reserve(foreground, "d", 1),
        Err(PublicationScheduleError::Closed)
    ));
    drop(a);
    // c already entered the node FIFO while a's second job waited only on
    // its account gate. c must receive the newly available node slot first.
    let c = timeout(Duration::from_secs(1), &mut c_waiter)
        .await
        .unwrap();
    assert!(
        timeout(Duration::from_millis(20), &mut a_waiter)
            .await
            .is_err()
    );
    drop(b);
    let a_again = timeout(Duration::from_secs(1), &mut a_waiter)
        .await
        .unwrap();
    drop(m_a);
    let m_a_again = timeout(Duration::from_secs(1), &mut m_a_waiter)
        .await
        .unwrap();
    drop((a_again, c, m_a_again, m_b));
    assert_eq!(
        (
            budget.stats().foreground_dispatch,
            budget.stats().maintenance_dispatch
        ),
        (0, 0)
    );
    drop(reservations);
    assert_eq!(budget.stats().accounts, 0);
}

#[tokio::test]
async fn canceling_dispatch_wait_keeps_command_credit_and_releases_partial_gates() {
    let budget = PublicationBudget::new(limits()).unwrap();
    let foreground = PublicationClass::Foreground;
    let mut reservations = Vec::new();
    for actor in ["a", "b", "c"] {
        reservations.push(budget.reserve(foreground, actor, 1).unwrap());
    }
    let a = budget.dispatch(foreground, "a").await;
    let b = budget.dispatch(foreground, "b").await;
    // c obtains its account gate, then waits for the global gate. Dropping
    // that future must give c back its account share for exact recovery.
    assert!(
        timeout(Duration::from_millis(20), budget.dispatch(foreground, "c"))
            .await
            .is_err()
    );
    assert_eq!(budget.stats().foreground, 3);
    assert_eq!(budget.stats().foreground_dispatch, 2);
    drop(a);
    let c = timeout(Duration::from_secs(1), budget.dispatch(foreground, "c"))
        .await
        .unwrap();
    drop((b, c, reservations));
    assert_eq!(
        (
            budget.stats().foreground,
            budget.stats().foreground_dispatch,
            budget.stats().accounts
        ),
        (0, 0, 0)
    );
}

#[test]
fn invalid_profiles_and_oversized_credits_cannot_wrap_or_consume_reservations() {
    for invalid in [
        PublicationLimits {
            maintenance_operations: 1,
            ..limits()
        },
        PublicationLimits {
            maintenance_in_flight: 1,
            ..limits()
        },
        PublicationLimits {
            in_flight: 3,
            ..limits()
        },
        PublicationLimits {
            in_flight: 0,
            ..limits()
        },
        PublicationLimits {
            command_bytes: 0,
            ..limits()
        },
    ] {
        assert!(matches!(
            PublicationBudget::new(invalid),
            Err(PublicationScheduleError::InvalidLimits)
        ));
    }
    let budget = PublicationBudget::new(limits()).unwrap();
    for bytes in [0, u64::MAX, limits().command_bytes] {
        assert!(matches!(
            budget.reserve(PublicationClass::Foreground, "a", bytes),
            Err(PublicationScheduleError::Capacity)
        ));
    }
    assert_eq!(
        (budget.stats().accounts, budget.stats().command_bytes),
        (0, 0)
    );
    let budget = PublicationBudget::new(PublicationLimits {
        command_bytes: u64::MAX,
        ..limits()
    })
    .unwrap();
    let bytes = u64::MAX - 4 * MAINTENANCE_RESERVATION;
    let foreground = budget
        .reserve(PublicationClass::Foreground, "a", bytes)
        .unwrap();
    assert!(matches!(
        budget.reserve(PublicationClass::Foreground, "b", 1),
        Err(PublicationScheduleError::Capacity)
    ));
    let maintenance = budget
        .reserve(PublicationClass::Maintenance, "b", MAINTENANCE_RESERVATION)
        .unwrap();
    assert_eq!(
        budget.stats().command_bytes,
        bytes + MAINTENANCE_RESERVATION
    );
    drop((foreground, maintenance));
    assert_eq!(budget.stats().command_bytes, 0);
}
