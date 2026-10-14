//! Same native catalog fixtures as merges, with real SDK phase and retirement.
use super::*;
use super::{
    native_merge::{initial, preparation},
    prepare::cleaned,
    publishing::edit,
};
use crate::packs::ref_state::RefStateIndex;
use canopy_object_storage::artifact::{ArtifactKey, ArtifactKind};
use cellule_runtime::{PreparedCommand, Resolution};
use object_store::ObjectStoreExt;

fn request(reference: &str, generation: i64) -> HeadRequest {
    HeadRequest {
        reference: reference.into(),
        expected_generation: generation,
    }
}
async fn command(
    f: &Fixture,
    prepared: &PreparedCatalog,
    request: HeadRequest,
) -> Result<(PreparedCommand<PublishNativeHead>, RegisteredRootRecovery)> {
    let command = f
        .client()
        .prepare_command::<PublishNativeHead>(
            &f.target,
            identity()?,
            prepared.native_head_proof(request).await?,
        )
        .await?;
    let saved = super::super::recovery::persist(
        &prepared.base.session,
        &command,
        super::super::recovery::Kind::Head,
        &prepared.base.indexes().store(),
        identity()?,
        0,
    )
    .await?;
    Ok((command, saved))
}
async fn state(f: &Fixture) -> Result<Vec<u8>> {
    let roots = super::publishing::state(&f.handle).await?;
    let phases = f.handle.query(0,65536, |db| {
        let mut s = db.prepare("SELECT recovery_phase,recovery_phase_revision FROM catalog_leases ORDER BY admission_sequence")?;
        let phases = s.query_map([], |r| Ok((r.get::<_,Option<Vec<u8>>>(0)?,r.get::<_,u64>(1)?)))?.collect::<rusqlite::Result<Vec<_>>>()?;
        let heads: u64 = db.query_row("SELECT count(*) FROM catalog_head_updates",[],|r|r.get(0))?;
        serde_json::to_vec(&(phases,heads)).map_err(|_|Error::Command("HEAD test state"))
    }).await?;
    Ok([roots, phases].concat())
}
async fn recover(
    f: &Fixture,
    saved: &RegisteredRootRecovery,
    store: &canopy_object_storage::artifact::ArtifactStore,
) -> Result<cellule_runtime::Committed<PublicationReply>> {
    match saved
        .dispatch_any(
            &f.client(),
            store,
            &f.authority(),
            &std::sync::atomic::AtomicBool::new(false),
        )
        .await
    {
        Ok(PublicationOutcome::Head(value)) => Ok(value),
        Err(PublicationError::Head(InvocationError::Rejected(value))) => Ok(*value),
        other => Err(format!("wrong HEAD recovery purpose: {other:?}").into()),
    }
}
#[tokio::test]
async fn native_head_reuses_ref_tree_and_preserves_original_receipt_after_typed_retirement()
-> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let (f, graph, _) = initial(format, false).await?;
        let (prepared, root, budget) = preparation(&f, &graph).await?;
        let check = prepared.base.capability().2.clone();
        let old = prepared
            .base()
            .refs
            .ok_or("missing ref snapshot")?
            .read(&graph.store)
            .await?;
        let (command, saved) = command(&f, &prepared, request("refs/heads/feature", 1)).await?;
        let original = command.clone().execute().await?;
        let PublicationReply::Published(published) = original.output else {
            return Err("HEAD refused".into());
        };
        assert_eq!((published.generation, published.ref_generation), (2, 2));
        let audit = f
            .handle
            .query(0, 1024, |db| {
                assert_eq!(
                    db.query_row("SELECT generation FROM ref_generation", [], |r| r
                        .get::<_, u64>(0))?,
                    2
                );
                assert_eq!(
                    db.query_row("SELECT default_branch FROM ref_generation", [], |r| r
                        .get::<_, String>(0))?,
                    "refs/heads/feature"
                );
                assert_eq!(
                    db.query_row("SELECT count(*) FROM refs", [], |r| r.get::<_, u64>(0))?,
                    0
                );
                Ok(
                    db.query_row("SELECT fact FROM catalog_head_updates", [], |r| {
                        r.get::<_, Vec<u8>>(0)
                    })?,
                )
            })
            .await?;
        let mut d = BoundedDecoder::new(&audit, 512)?;
        let fact = GenerationFact::decode(&mut d)?;
        d.finish()?;
        let refs = fact.refs.ok_or("missing HEAD outcome refs")?;
        let updated = refs.read(&graph.store).await?;
        assert_eq!(
            (
                updated.root.clone(),
                updated.generation,
                updated.default_branch.as_str()
            ),
            (old.root, 2, "refs/heads/feature")
        );
        let index = RefStateIndex::new(graph.store.clone(), format);
        assert_eq!(
            index
                .read(updated.root, "refs/heads/feature")
                .await?
                .ok_or("missing feature")?
                .version,
            1
        );
        assert_eq!(command.execute().await?.receipt, original.receipt);
        let admin = super::terminal_retention::maintenance(&f.handle, f.repository).await?;
        // Corrupt/missing selected metadata must retain the original independent pin.
        let path = graph.store.path(
            ArtifactKey {
                operation: refs.operation(),
                binding_digest: refs.artifact().digest,
                kind: ArtifactKind::InputRoot,
            },
            refs.artifact().digest,
        )?;
        let bytes = graph.provider.get(&path).await?.bytes().await?;
        graph.provider.delete(&path).await?;
        assert!(
            saved
                .ready_terminal_release(f.client(), &graph.store, admin.clone(), identity()?)
                .await
                .is_err()
        );
        assert_eq!(
            recover(&f, &saved, &graph.store).await?.receipt,
            original.receipt
        );
        graph.provider.put(&path, bytes.into()).await?;
        assert_eq!(
            saved
                .ready_terminal_release(f.client(), &graph.store, admin, identity()?)
                .await?
                .complete()
                .await?
                .output,
            TerminalReleaseReply::Released
        );
        for (key, descriptor) in saved.command_bodies_for_test() {
            graph
                .provider
                .delete(&graph.store.path(key, descriptor.digest)?)
                .await?;
        }
        drop(prepared);
        cleaned(root.path(), &budget).await?;
        // Permanent outcome selectors cannot be altered or deleted.
        let id = check.token.operation;
        f.handle
            .query(0, 128, move |db| {
                assert!(
                    db.execute(
                        "UPDATE catalog_head_updates SET actor='replacement' WHERE id=?1",
                        [id.as_slice()]
                    )
                    .is_err()
                );
                assert!(
                    db.execute(
                        "DELETE FROM catalog_head_updates WHERE id=?1",
                        [id.as_slice()]
                    )
                    .is_err()
                );
                Ok(Vec::new())
            })
            .await?;
        let (runtime, handle, client) = super::durable_recovery::restore_owner(&f, &check).await?;
        assert!(handle.owner_fence().epoch > check.token.owner.epoch);
        let restored = RegisteredRootRecovery::load(&client, &f.target, &graph.store, &check)
            .await?
            .ok_or("HEAD archive missing")?;
        let PublicationOutcome::Head(replay) = restored
            .dispatch_any(
                &client,
                &graph.store,
                &f.authority(),
                &std::sync::atomic::AtomicBool::new(false),
            )
            .await?
        else {
            return Err("wrong restored HEAD purpose".into());
        };
        assert_eq!(
            (replay.output, replay.receipt),
            (original.output, original.receipt)
        );
        runtime.shutdown().await?;
        drop(graph.prepared);
        cleaned(graph.root.path(), &graph.budget).await?;
    }
    Ok(())
}
#[tokio::test]
async fn native_head_denials_recheck_current_authority_generation_and_native_branch_existence()
-> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        for mode in 0..5 {
            let (f, graph, _) = initial(format, false).await?;
            let (prepared, root, budget) = preparation(&f, &graph).await?;
            let req = match mode {
                0 => request("refs/heads/absent", 1),
                1 => request("refs/heads/feature", 0),
                _ => request("refs/heads/feature", 1),
            };
            let (command, saved) = command(&f, &prepared, req).await?;
            let reason = match mode {
                2 => {
                    edit(&f, "UPDATE repository_identity SET owner='replacement'").await?;
                    PreparationDenial::Unauthorized
                }
                3 => {
                    edit(&f,"INSERT INTO catalog_generations SELECT 2,catalog,certificate,refs FROM catalog_generations WHERE generation=1; UPDATE catalog_state SET generation=2").await?;
                    PreparationDenial::Conflict
                }
                4 => {
                    edit(&f,"UPDATE catalog_operations SET expires_at_ms=0; UPDATE catalog_leases SET expires_at_ms=0").await?;
                    PreparationDenial::Expired
                }
                _ => PreparationDenial::Conflict,
            };
            let roots_before = f
                .handle
                .query(0, 128, |db| {
                    serde_json::to_vec(&(
                        db.query_row("SELECT generation FROM catalog_state", [], |r| {
                            r.get::<_, u64>(0)
                        })?,
                        db.query_row("SELECT count(*) FROM catalog_head_updates", [], |r| {
                            r.get::<_, u64>(0)
                        })?,
                    ))
                    .map_err(|_| Error::Command("HEAD test roots"))
                })
                .await?;
            let original = if matches!(mode, 2 | 4) {
                // Authoritative SDK absence after restart must settle the same
                // frozen denial, without a fresh Write observation hiding it.
                drop(command);
                recover(&f, &saved, &graph.store).await?
            } else {
                command.execute().await?
            };
            assert_eq!(original.output, PublicationReply::Denied(reason));
            let after = f.handle.query(0,128,|db| {
                assert_eq!(db.query_row("SELECT count(*) FROM catalog_operations WHERE actor='owner' AND generation IS NOT NULL",[],|r|r.get::<_,u64>(0))?,1);
                serde_json::to_vec(&(db.query_row("SELECT generation FROM catalog_state",[],|r|r.get::<_,u64>(0))?,db.query_row("SELECT count(*) FROM catalog_head_updates",[],|r|r.get::<_,u64>(0))?)).map_err(|_|Error::Command("HEAD test roots"))
            }).await?;
            assert_eq!(after, roots_before);
            assert_eq!(
                recover(&f, &saved, &graph.store).await?.receipt,
                original.receipt
            );
            drop(prepared);
            cleaned(root.path(), &budget).await?;
            drop(graph.prepared);
            cleaned(graph.root.path(), &graph.budget).await?;
            f.runtime.shutdown().await?;
        }
    }
    Ok(())
}
#[tokio::test]
async fn native_head_registration_and_late_sql_failure_keep_original_command_and_atomic_roots()
-> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let (f, graph, _) = initial(format, false).await?;
        let (prepared, root, budget) = preparation(&f, &graph).await?;
        let mut proof = prepared
            .native_head_proof(request("refs/heads/feature", 1))
            .await?;
        let mut e = BoundedEncoder::new(NATIVE_HEAD_BYTES)?;
        proof.encode(&mut e)?;
        let bytes = e.finish();
        let mut d = BoundedDecoder::new(&bytes, NATIVE_HEAD_BYTES)?;
        assert_eq!(NativeHeadProof::decode(&mut d)?, proof);
        d.finish()?;
        // A request/root mutation cannot borrow an authentic catalog's authority.
        proof.request.reference = "refs/heads/main".into();
        assert!(
            proof
                .encode(&mut BoundedEncoder::new(NATIVE_HEAD_BYTES)?)
                .is_err()
        );
        let unregistered = f
            .client()
            .prepare_command::<PublishNativeHead>(
                &f.target,
                identity()?,
                prepared
                    .native_head_proof(request("refs/heads/feature", 1))
                    .await?,
            )
            .await?;
        let before = state(&f).await?;
        assert!(matches!(
            unregistered.execute().await,
            Err(InvocationError::NotStarted(_))
        ));
        assert_eq!(state(&f).await?, before);
        let (command, _) = command(&f, &prepared, request("refs/heads/feature", 1)).await?;
        edit(&f,"CREATE TRIGGER abort_head BEFORE INSERT ON catalog_head_updates BEGIN SELECT RAISE(ABORT,'late HEAD failure'); END").await?;
        let before = state(&f).await?;
        assert!(command.clone().execute().await.is_err());
        assert_eq!(state(&f).await?, before);
        assert!(matches!(
            f.client().resolve(command.evidence()).await?,
            Resolution::Absent
        ));
        edit(&f, "DROP TRIGGER abort_head").await?;
        assert!(matches!(
            command.execute().await?.output,
            PublicationReply::Published(_)
        ));
        drop(prepared);
        cleaned(root.path(), &budget).await?;
        drop(graph.prepared);
        cleaned(graph.root.path(), &graph.budget).await?;
        f.runtime.shutdown().await?;
    }
    Ok(())
}
