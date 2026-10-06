//! Selected audit retention and exact recovery after transient pins are released.
use super::super::{durable_recovery, terminal_retention};
use super::*;
use canopy_object_storage::artifact::{ArtifactKey, ArtifactKind};
use object_store::ObjectStoreExt;

async fn archived_result(
    f: &Fixture,
    graph: &Graph,
    check: &LeaseCheck,
) -> Result<cellule_runtime::Committed<MergeOutcome>> {
    let saved = RegisteredRootRecovery::load(&f.client(), &f.target, &graph.store, check)
        .await?
        .ok_or("merge archive missing")?;
    match saved
        .dispatch_any(
            &f.client(),
            &graph.store,
            &f.authority(),
            &std::sync::atomic::AtomicBool::new(false),
        )
        .await
    {
        Ok(PublicationOutcome::Merge(value)) => Ok(value),
        Err(PublicationError::Merge(InvocationError::Rejected(value))) => Ok(*value),
        other => Err(format!("unexpected archived merge: {other:?}").into()),
    }
}

async fn retained_pin(f: &Fixture, check: &LeaseCheck, expected: u64) -> Result {
    let token = check.token;
    f.handle.query(0,128,move |db| {
        assert_eq!(db.query_row("SELECT count(*) FROM catalog_leases WHERE incarnation=?1 AND admission_sequence=?2 AND recovery IS NOT NULL",rusqlite::params![token.owner.incarnation.as_bytes().as_slice(),token.attempt],|r|r.get::<_,u64>(0))?,expected);
        Ok(Vec::new())
    }).await?;
    Ok(())
}

#[tokio::test]
async fn native_merge_applied_attempt_releases_pin_without_losing_original_uuid_or_receipt()
-> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let (f, graph, request) = initial(format, false).await?;
        let (prepared, root, budget) = preparation(&f, &graph).await?;
        let check = prepared.base.capability().2.clone();
        let (command, registered) =
            prepared_command(&f, &prepared, request.clone(), root.path(), budget.clone()).await?;
        let admin = terminal_retention::maintenance(&f.handle, f.repository).await?;
        assert!(
            registered
                .ready_terminal_release(f.client(), &graph.store, admin.clone(), identity()?)
                .await
                .is_err()
        );
        let original = command.execute().await?;
        let released = registered
            .ready_terminal_release(f.client(), &graph.store, admin, identity()?)
            .await?
            .complete()
            .await?;
        assert_eq!(released.output, TerminalReleaseReply::Released);
        let token = check.token;
        f.handle.query(0,128,move |db| {
            assert_eq!(db.query_row("SELECT count(*) FROM catalog_leases WHERE incarnation=?1 AND admission_sequence=?2",rusqlite::params![token.owner.incarnation.as_bytes().as_slice(),token.attempt],|r|r.get::<_,u64>(0))?,0);
            assert_eq!(db.query_row("SELECT count(*) FROM catalog_recovery_receipts",[],|r|r.get::<_,u64>(0))?,1);
            Ok(Vec::new())
        }).await?;
        drop(registered);
        drop(prepared);
        cleaned(root.path(), &budget).await?;
        let restored = RegisteredRootRecovery::load(&f.client(), &f.target, &graph.store, &check)
            .await?
            .ok_or("merge archive absent")?;
        let PublicationOutcome::Merge(recovered) = restored
            .dispatch_any(
                &f.client(),
                &graph.store,
                &f.authority(),
                &std::sync::atomic::AtomicBool::new(false),
            )
            .await?
        else {
            return Err("merge archive purpose".into());
        };
        assert_eq!(
            (recovered.output, recovered.receipt),
            (original.output.clone(), original.receipt)
        );
        let (retry, retry_root, retry_budget) = preparation(&f, &graph).await?;
        let (retry_command, retry_recovery) =
            prepared_command(&f, &retry, request, retry_root.path(), retry_budget.clone()).await?;
        assert_eq!(retry_command.execute().await?.output, original.output);
        // A terminal replay closes only its fresh operation in the same final
        // transaction. Its independent pin can then select the original audit.
        let admin = terminal_retention::maintenance(&f.handle, f.repository).await?;
        assert_eq!(
            retry_recovery
                .ready_terminal_release(f.client(), &graph.store, admin, identity()?)
                .await?
                .complete()
                .await?
                .output,
            TerminalReleaseReply::Released
        );
        merged_roots(&f, &graph).await?;
        drop(retry);
        cleaned(retry_root.path(), &retry_budget).await?;
        drop(graph.prepared);
        cleaned(graph.root.path(), &graph.budget).await?;
        f.runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn negative_merge_archive_keeps_its_denial_after_same_uuid_succeeds() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let (f, graph, request) = initial(format, false).await?;
        let (prepared, root, budget) = preparation(&f, &graph).await?;
        let check = prepared.base.capability().2.clone();
        let (command, saved) =
            prepared_command(&f, &prepared, request.clone(), root.path(), budget.clone()).await?;
        edit(
            &f,
            "INSERT INTO branch_rules VALUES('refs/heads/main',1,1,1,1,1,1)",
        )
        .await?;
        let original = command.execute().await?;
        assert_eq!(original.output, MergeOutcome::ReviewsRequired);
        let admin = terminal_retention::maintenance(&f.handle, f.repository).await?;
        retained_pin(&f, &check, 1).await?;
        let release = saved
            .ready_terminal_release(f.client(), &graph.store, admin, identity()?)
            .await?;
        assert_eq!(
            release.complete().await?.output,
            TerminalReleaseReply::Released
        );
        retained_pin(&f, &check, 0).await?;
        drop(prepared);
        cleaned(root.path(), &budget).await?;

        edit(&f,"UPDATE branch_rules SET required_approvals=0,version=2 WHERE reference='refs/heads/main'").await?;
        let (retry, retry_root, retry_budget) = preparation(&f, &graph).await?;
        let (command, saved) =
            prepared_command(&f, &retry, request, retry_root.path(), retry_budget.clone()).await?;
        assert!(matches!(
            command.execute().await?.output,
            MergeOutcome::Applied { .. }
        ));
        assert_eq!(
            saved
                .ready_terminal_release(
                    f.client(),
                    &graph.store,
                    terminal_retention::maintenance(&f.handle, f.repository).await?,
                    identity()?
                )
                .await?
                .complete()
                .await?
                .output,
            TerminalReleaseReply::Released
        );
        let recovered = archived_result(&f, &graph, &check).await?;
        assert_eq!(
            (recovered.output, recovered.receipt),
            (original.output, original.receipt)
        );
        merged_roots(&f, &graph).await?;
        drop(retry);
        cleaned(retry_root.path(), &retry_budget).await?;
        drop(graph.prepared);
        cleaned(graph.root.path(), &graph.budget).await?;
        f.runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn missing_or_corrupt_selected_merge_metadata_retains_pin_and_original_result() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let (f, graph, request) = initial(format, false).await?;
        let (prepared, root, budget) = preparation(&f, &graph).await?;
        let check = prepared.base.capability().2.clone();
        let (command, saved) =
            prepared_command(&f, &prepared, request, root.path(), budget.clone()).await?;
        let original = command.execute().await?;
        let publication = f
            .handle
            .query(0, 128, |db| {
                Ok(db.query_row(
                    "SELECT publication FROM pull_merges WHERE pull_number=1",
                    [],
                    |r| r.get::<_, Vec<u8>>(0),
                )?)
            })
            .await?;
        let mut d = BoundedDecoder::new(&publication, 128)?;
        let audit = crate::packs::input_artifact::StoredInputRoot::decode(&mut d)?;
        d.finish()?;
        let catalog = prepared.catalog();
        let snapshot =
            crate::packs::catalog::CatalogSnapshot::download(&graph.store, catalog).await?;
        let encoded = f
            .handle
            .query(0, 128, |db| {
                Ok(db.query_row(
                    "SELECT refs FROM catalog_generations WHERE generation=2",
                    [],
                    |r| r.get::<_, Vec<u8>>(0),
                )?)
            })
            .await?;
        let mut d = BoundedDecoder::new(&encoded, 128)?;
        let refs = RefStateSnapshotRoot::decode(&mut d)?;
        d.finish()?;
        let ref_node = refs
            .read(&graph.store)
            .await?
            .root
            .ok_or("ref index missing")?;
        let edges = [
            (audit.operation, ArtifactKind::InputRoot, audit.artifact),
            (
                catalog.operation,
                ArtifactKind::CatalogNode,
                catalog.artifact,
            ),
            (
                snapshot.directory.operation,
                ArtifactKind::CatalogNode,
                snapshot.directory.artifact,
            ),
            (refs.operation(), ArtifactKind::InputRoot, refs.artifact()),
            (
                ref_node.operation,
                ArtifactKind::CatalogNode,
                ref_node.artifact,
            ),
        ];
        let admin = terminal_retention::maintenance(&f.handle, f.repository).await?;
        for (operation, kind, artifact) in edges {
            let path = graph.store.path(
                ArtifactKey {
                    operation,
                    kind,
                    binding_digest: artifact.digest,
                },
                artifact.digest,
            )?;
            let bytes = graph.provider.get(&path).await?.bytes().await?;
            for corrupt in [false, true] {
                graph.provider.delete(&path).await?;
                if corrupt {
                    graph
                        .provider
                        .put(&path, vec![0u8; bytes.len()].into())
                        .await?;
                }
                assert!(
                    saved
                        .ready_terminal_release(
                            f.client(),
                            &graph.store,
                            admin.clone(),
                            identity()?
                        )
                        .await
                        .is_err(),
                    "accepted missing/corrupt {kind:?}"
                );
                retained_pin(&f, &check, 1).await?;
                let recovered = archived_result(&f, &graph, &check).await?;
                assert_eq!(
                    (recovered.output, recovered.receipt),
                    (original.output.clone(), original.receipt)
                );
                graph.provider.put(&path, bytes.clone().into()).await?;
            }
        }
        assert_eq!(
            saved
                .ready_terminal_release(f.client(), &graph.store, admin, identity()?)
                .await?
                .complete()
                .await?
                .output,
            TerminalReleaseReply::Released
        );
        drop(prepared);
        cleaned(root.path(), &budget).await?;
        drop(graph.prepared);
        cleaned(graph.root.path(), &graph.budget).await?;
        f.runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn merge_retirement_fences_authority_and_rolls_back_archive_and_pin_together() -> Result {
    let (f, graph, request) = initial(ObjectFormat::Sha256, false).await?;
    let (prepared, root, budget) = preparation(&f, &graph).await?;
    let check = prepared.base.capability().2.clone();
    let (command, saved) =
        prepared_command(&f, &prepared, request, root.path(), budget.clone()).await?;
    let original = command.execute().await?;
    let admin = terminal_retention::maintenance(&f.handle, f.repository).await?;
    for wrong_owner in [true, false] {
        let mut wrong = admin.clone();
        if wrong_owner {
            wrong.owner.epoch += 1;
        } else {
            wrong.actor = "outsider".into();
        }
        let release = saved
            .ready_terminal_release(f.client(), &graph.store, wrong, identity()?)
            .await?;
        assert!(
            matches!(release.complete().await,Err(PublicationError::TerminalRelease(InvocationError::Rejected(value))) if value.output==TerminalReleaseReply::Denied(PreparationDenial::Unauthorized))
        );
        retained_pin(&f, &check, 1).await?;
    }
    let release = saved
        .ready_terminal_release(f.client(), &graph.store, admin, identity()?)
        .await?;
    edit(&f,"CREATE TRIGGER merge_release_late_fault BEFORE DELETE ON catalog_leases WHEN OLD.recovery IS NOT NULL BEGIN SELECT RAISE(ABORT,'late merge release fault'); END").await?;
    let failed = release.clone().complete().await;
    assert!(
        matches!(failed,Err(PublicationError::TerminalRelease(InvocationError::NotStarted(Error::Sqlite(rusqlite::Error::SqliteFailure(_,Some(ref message)))))) if message=="late merge release fault"),
        "{failed:?}"
    );
    assert!(matches!(
        f.client().resolve(&release.evidence_for_test()).await?,
        Resolution::Absent
    ));
    retained_pin(&f, &check, 1).await?;
    f.handle
        .query(0, 128, |db| {
            assert_eq!(
                db.query_row("SELECT count(*) FROM catalog_recovery_receipts", [], |r| {
                    r.get::<_, u64>(0)
                })?,
                0
            );
            Ok(Vec::new())
        })
        .await?;
    edit(&f, "DROP TRIGGER merge_release_late_fault").await?;
    assert_eq!(
        release.complete().await?.output,
        TerminalReleaseReply::Released
    );
    retained_pin(&f, &check, 0).await?;
    for mutation in [
        "UPDATE pull_merges SET publication=zeroblob(32)",
        "DELETE FROM pull_merges",
        "INSERT OR REPLACE INTO pull_merges SELECT * FROM pull_merges",
    ] {
        assert!(
            edit(&f, mutation).await.is_err(),
            "mutable permanent audit: {mutation}"
        );
    }
    let recovered = archived_result(&f, &graph, &check).await?;
    assert_eq!(
        (recovered.output, recovered.receipt),
        (original.output, original.receipt)
    );
    merged_roots(&f, &graph).await?;
    drop(prepared);
    cleaned(root.path(), &budget).await?;
    drop(graph.prepared);
    cleaned(graph.root.path(), &graph.budget).await?;
    f.runtime.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn archived_merge_and_release_receipts_survive_sqlite_loss_and_owner_restoration() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let (f, graph, request) = initial(format, false).await?;
        let (prepared, root, budget) = preparation(&f, &graph).await?;
        let check = prepared.base.capability().2.clone();
        let (command, saved) =
            prepared_command(&f, &prepared, request, root.path(), budget.clone()).await?;
        let original = command.execute().await?;
        let release = saved
            .ready_terminal_release(
                f.client(),
                &graph.store,
                terminal_retention::maintenance(&f.handle, f.repository).await?,
                identity()?,
            )
            .await?;
        let released = release.clone().complete().await?;
        retained_pin(&f, &check, 0).await?;
        for (key, descriptor) in saved.command_bodies_for_test() {
            let path = graph.store.path(key, descriptor.digest)?;
            graph.provider.delete(&path).await?;
        }
        drop(prepared);
        cleaned(root.path(), &budget).await?;
        let (runtime, handle, client) = durable_recovery::restore_owner(&f, &check).await?;
        assert!(handle.owner_fence().epoch > check.token.owner.epoch);
        let restored = RegisteredRootRecovery::load(&client, &f.target, &graph.store, &check)
            .await?
            .ok_or("restored merge archive missing")?;
        let PublicationOutcome::Merge(recovered) = restored
            .dispatch_any(
                &client,
                &graph.store,
                &f.authority(),
                &std::sync::atomic::AtomicBool::new(false),
            )
            .await?
        else {
            return Err("restored merge purpose".into());
        };
        assert_eq!(
            (recovered.output, recovered.receipt),
            (original.output, original.receipt)
        );
        let recovered_release = release.with_client_for_test(client).complete().await?;
        assert_eq!(
            (recovered_release.output, recovered_release.receipt),
            (released.output, released.receipt)
        );
        drop(graph.prepared);
        cleaned(graph.root.path(), &graph.budget).await?;
        runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn terminal_merge_refusal_closure_rolls_back_with_original_phase_and_sdk_acceptance() -> Result
{
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let (f, graph, request) = initial(format, false).await?;
        let (prepared, root, budget) = preparation(&f, &graph).await?;
        let (command, saved) =
            prepared_command(&f, &prepared, request, root.path(), budget.clone()).await?;
        edit(
            &f,
            "INSERT INTO branch_rules VALUES('refs/heads/main',1,1,1,1,1,1)",
        )
        .await?;
        edit(&f,"CREATE TRIGGER terminal_merge_close_fault BEFORE DELETE ON catalog_operations BEGIN SELECT RAISE(ABORT,'terminal merge close fault'); END").await?;
        let before = phase_state(&f).await?;
        let domain = domain_state(&f).await?;
        let operation = prepared.token().operation;
        let remaining = domain_state_except_attempt(&f, Some(operation)).await?;
        assert!(command.clone().execute().await.is_err());
        assert_eq!(phase_state(&f).await?, before);
        assert_eq!(domain_state(&f).await?, domain);
        assert!(matches!(
            f.client().resolve(command.evidence()).await?,
            Resolution::Absent
        ));
        edit(&f, "DROP TRIGGER terminal_merge_close_fault").await?;
        let original = command.execute().await?;
        assert_eq!(original.output, MergeOutcome::ReviewsRequired);
        assert_eq!(
            domain_state_except_attempt(&f, Some(operation)).await?,
            remaining
        );
        assert_operation(&f, operation, 0).await?;
        assert_eq!(
            saved
                .ready_terminal_release(
                    f.client(),
                    &graph.store,
                    terminal_retention::maintenance(&f.handle, f.repository).await?,
                    identity()?
                )
                .await?
                .complete()
                .await?
                .output,
            TerminalReleaseReply::Released
        );
        let recovered = archived_result(&f, &graph, prepared.base.capability().2).await?;
        assert_eq!(
            (recovered.output, recovered.receipt),
            (original.output, original.receipt)
        );
        drop(prepared);
        cleaned(root.path(), &budget).await?;
        drop(graph.prepared);
        cleaned(graph.root.path(), &graph.budget).await?;
        f.runtime.shutdown().await?;
    }
    Ok(())
}
