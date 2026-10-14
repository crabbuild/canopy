use super::*;

fn node_limits() -> PublicationLimits {
    PublicationLimits {
        operations: 8,
        per_actor: 2,
        command_bytes: (24 << 20) + (32 << 10),
        in_flight: 4,
        maintenance_operations: 4,
        maintenance_in_flight: 2,
        foreground_burst: 3,
    }
}

async fn prepared_outcome(
    fixture: &Fixture,
    operation: u8,
    actor: &str,
) -> Result<(
    ReadyCatalogPush,
    tempfile::TempDir,
    DiskBudget,
    std::sync::Weak<PreparationSession>,
)> {
    let (prepared, root, disk) = empty(fixture, [operation; 16], actor).await?;
    let session = Arc::new(prepared.base.session.clone());
    let weak = Arc::downgrade(&session);
    let ready = session
        .ready_outcome(identity()?, request(refused()))
        .await?;
    drop((prepared, session));
    cleaned(root.path(), &disk).await?;
    Ok((ready, root, disk, weak))
}

#[tokio::test]
async fn held_commands_charge_one_node_across_repositories_and_return_exact_refusals() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let first = Fixture::new(format).await?;
        let second = Fixture::new(format).await?;
        for fixture in [&first, &second] {
            edit(
                fixture,
                "INSERT INTO repository_members VALUES('writer','write')",
            )
            .await?;
        }
        let node = PublicationBudget::new(node_limits())?;
        let a = PublicationCoordinator::new(
            first.target.clone(),
            PublicationLimits::default(),
            node.clone(),
        )?;
        let b = PublicationCoordinator::new(
            second.target.clone(),
            PublicationLimits::default(),
            node.clone(),
        )?;
        let (ready, root1, disk1, weak1) = prepared_outcome(&first, 51, "owner").await?;
        let t1 = a.try_reserve(ready)?;
        let (ready, root2, disk2, weak2) = prepared_outcome(&second, 52, "owner").await?;
        let t2 = b.try_reserve(ready)?;
        let (ready, root3, disk3, weak3) = prepared_outcome(&second, 53, "owner").await?;
        let original = ready.evidence_for_test();
        let failure = b
            .try_reserve(ready)
            .err()
            .ok_or("aggregate account quota bypassed")?;
        assert_eq!(failure.reason, PublicationScheduleError::Capacity);
        let ReadyPublication::Push(ready) = failure.ready else {
            return Err("ready variant changed".into());
        };
        assert_eq!(ready.evidence_for_test(), original);
        let (writer, root4, disk4, weak4) = prepared_outcome(&second, 54, "writer").await?;
        let t4 = b.try_reserve(writer)?;
        let (writer, root5, disk5, weak5) = prepared_outcome(&first, 55, "writer").await?;
        let writer_original = writer.evidence_for_test();
        let failure = a
            .try_reserve(writer)
            .err()
            .ok_or("aggregate command bytes bypassed")?;
        assert_eq!(failure.reason, PublicationScheduleError::Capacity);
        let ReadyPublication::Push(writer) = failure.ready else {
            return Err("ready variant changed".into());
        };
        assert_eq!(writer.evidence_for_test(), writer_original);
        let stats = node.stats();
        assert_eq!(
            (
                stats.foreground,
                stats.command_bytes,
                stats.accounts,
                stats.foreground_dispatch
            ),
            (3, 24 << 20, 2, 0)
        );
        assert!(weak1.upgrade().is_some());
        // Observer handles survive credit return, but the retained session
        // must already be gone when that return is observed.
        t1.discard_held().await?;
        assert!(weak1.upgrade().is_none());
        cleaned(root1.path(), &disk1).await?;
        assert_eq!(node.stats().foreground, 2);
        let t3 = b.try_reserve(ready)?;
        assert_eq!(node.stats().foreground, 3);
        t2.discard_held().await?;
        let t5 = a.try_reserve(writer)?;
        node.close();
        let (ready, root6, disk6, weak6) = prepared_outcome(&first, 56, "writer").await?;
        let original = ready.evidence_for_test();
        let failure = a
            .try_reserve(ready)
            .err()
            .ok_or("closed node admitted new work")?;
        assert_eq!(failure.reason, PublicationScheduleError::Closed);
        let ReadyPublication::Push(ready) = failure.ready else {
            return Err("ready variant changed".into());
        };
        assert_eq!(ready.evidence_for_test(), original);
        drop(ready);
        assert!(weak6.upgrade().is_none());
        for ticket in [&t3, &t4, &t5] {
            ticket.discard_held().await?;
        }
        assert!(a.close_and_drain().await.is_empty());
        assert!(b.close_and_drain().await.is_empty());
        assert!(matches!(t1.state(), PublicationState::Discarded));
        assert_eq!(
            (
                node.stats().foreground,
                node.stats().accounts,
                node.stats().command_bytes
            ),
            (0, 0, 0)
        );
        for (root, disk, weak) in [
            (root2, disk2, weak2),
            (root3, disk3, weak3),
            (root4, disk4, weak4),
            (root5, disk5, weak5),
            (root6, disk6, weak6),
        ] {
            assert!(weak.upgrade().is_none());
            cleaned(root.path(), &disk).await?;
        }
        first.runtime.shutdown().await?;
        second.runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn account_transport_across_repositories_does_not_block_another_account() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let first = Fixture::new(format).await?;
        let second = Fixture::new(format).await?;
        edit(
            &second,
            "INSERT INTO repository_members VALUES('writer','write')",
        )
        .await?;
        let node = PublicationBudget::new(node_limits())?;
        let a = PublicationCoordinator::new(
            first.target.clone(),
            PublicationLimits::default(),
            node.clone(),
        )?;
        let b = PublicationCoordinator::new(
            second.target.clone(),
            PublicationLimits::default(),
            node.clone(),
        )?;
        let (release, entered) = a.pause_for_test().await;
        let (ready, root1, disk1, weak1) = prepared_outcome(&first, 61, "owner").await?;
        let t1 = a.submit(ready).await?;
        timeout(Duration::from_secs(5), entered).await??;
        let (ready, root2, disk2, weak2) = prepared_outcome(&second, 62, "owner").await?;
        let t2 = b.submit(ready).await?;
        assert!(timeout(Duration::from_millis(30), t2.wait()).await.is_err());
        assert_eq!(
            (node.stats().foreground, node.stats().foreground_dispatch),
            (2, 1)
        );
        drop(t2);
        assert!(weak2.upgrade().is_some());
        let (ready, root3, disk3, weak3) = prepared_outcome(&second, 63, "writer").await?;
        let writer = b.submit(ready).await?;
        finished(timeout(Duration::from_secs(10), writer.wait()).await?)?;
        assert_eq!(writer.response().await?, refused());
        assert!(weak3.upgrade().is_none());
        assert_eq!(
            (node.stats().foreground, node.stats().foreground_dispatch),
            (2, 1)
        );
        node.close();
        let t2 = b.pending([62; 16]).await.ok_or("waiting original lost")?;
        release.send(()).map_err(|_| "paused dispatcher lost")?;
        for ticket in [&t1, &t2] {
            finished(timeout(Duration::from_secs(10), ticket.wait()).await?)?;
            assert_eq!(ticket.response().await?, refused());
        }
        assert!(a.close_and_drain().await.is_empty());
        assert!(b.close_and_drain().await.is_empty());
        assert_eq!(
            (
                node.stats().foreground,
                node.stats().foreground_dispatch,
                node.stats().command_bytes,
                node.stats().accounts
            ),
            (0, 0, 0, 0)
        );
        for (root, disk, weak) in [
            (root1, disk1, weak1),
            (root2, disk2, weak2),
            (root3, disk3, weak3),
        ] {
            assert!(weak.upgrade().is_none());
            cleaned(root.path(), &disk).await?;
        }
        first.runtime.shutdown().await?;
        second.runtime.shutdown().await?;
    }
    Ok(())
}
