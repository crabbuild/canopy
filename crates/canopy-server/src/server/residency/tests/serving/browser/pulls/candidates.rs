//! Native candidate editorial preparation against actual resident ref facts.
use super::*;
use crate::pulls::{
    candidates::{
        CandidateOutcome, CandidateRequest, CandidateResult,
        command::{CandidateAction, CandidateRefRequest, PrepareCandidate},
    },
    merge::MergeStrategy,
};

async fn intent(repository: &RepositoryCell, native: &BrowseFixture) -> Result<CandidateAction> {
    let data = data(native);
    let created = repository
        .create_pull(crate::server::mutation_identity()?, "canopy", data.view())
        .await?;
    let PullChange::Applied(number) = created.output else {
        return Err("candidate pull refused".into());
    };
    let revision = repository
        .pull_review_policy("canopy", number)
        .await?
        .output
        .and_then(|policy| policy.revision)
        .ok_or("candidate revision missing")?;
    Ok(CandidateAction::Reserve {
        actor: "canopy".into(),
        number,
        request: CandidateRequest {
            id: uuid::Uuid::new_v4().to_string(),
            revision,
            strategy: MergeStrategy::MergeCommit,
            message: "Native candidate".into(),
        },
        created_ms: crate::server::mutation_identity()?.issued_at_ms,
    })
}

async fn reserve(
    repository: &RepositoryCell,
    input: CandidateRefRequest,
) -> Result<CandidateOutcome> {
    match repository
        .application
        .command::<PrepareCandidate>(
            &repository.target,
            crate::server::mutation_identity()?,
            input,
        )
        .await
    {
        Ok(value) => Ok(value.output),
        Err(InvocationError::Rejected(value)) => Ok(value.output),
        Err(error) => Err(error.into()),
    }
}
async fn candidate_count(repository: &RepositoryCell) -> Result<i64> {
    let value = repository
        .sql
        .query(
            None,
            SqlBatch {
                statements: vec![SqlStatement {
                    sql: "SELECT count(*) FROM merge_candidates".into(),
                    parameters: vec![],
                }],
            },
        )
        .await?;
    let Some([SqlValue::Integer(count)]) = value.output[0].rows.first().map(Vec::as_slice) else {
        return Err("candidate count missing".into());
    };
    Ok(*count)
}

async fn joint_state(repository: &RepositoryCell) -> Result<Vec<Vec<SqlValue>>> {
    let value=repository.sql.query(None,SqlBatch {statements:vec![SqlStatement {
        sql:"SELECT s.generation,g.catalog,g.certificate,g.refs FROM catalog_state s JOIN catalog_generations g ON g.generation=s.generation WHERE s.singleton=1".into(),parameters:vec![],
    }]}).await?;
    Ok(value
        .output
        .into_iter()
        .next()
        .ok_or("candidate joint state missing")?
        .rows)
}

async fn fault_sql(handle: &cellule_runtime::cell::actor::CellHandle, sql: &'static str) -> Result {
    let identity = crate::server::mutation_identity()?;
    handle
        .execute(
            identity,
            cellule_runtime::Digest::from_bytes(*blake3::hash(sql.as_bytes()).as_bytes()),
            identity.issued_at_ms,
            sql.len(),
            0,
            move |tx| {
                tx.execute_batch(sql)?;
                Ok(cellule_runtime::cell::executor::HandlerOutcome::Success(
                    Vec::new(),
                ))
            },
        )
        .await?;
    Ok(())
}

#[tokio::test]
async fn native_candidate_reservation_uses_certified_refs_without_legacy_ref_authority() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let (server, _files) = server().await?;
        let (repository, native, _) = fixture(&server, format).await?;
        let action = intent(&repository, &native).await?;
        let joint = joint_state(&repository).await?;
        let CandidateOutcome::Applied(candidate) = repository
            .candidate_action(crate::server::mutation_identity()?, action.clone())
            .await?
            .output
        else {
            return Err("native candidate reservation refused".into());
        };
        assert_eq!(candidate.result, CandidateResult::Pending);
        let original = (*candidate).clone();
        assert_eq!(
            candidate.request.revision.source_oid,
            hex::encode(native.main)
        );
        assert_eq!(
            candidate.request.revision.base_oid,
            hex::encode(native.side)
        );
        assert_eq!(
            repository
                .merge_candidate("canopy", candidate.number, &candidate.request.id)
                .await?
                .output,
            Some(*candidate)
        );
        let legacy = repository
            .sql
            .query(
                None,
                SqlBatch {
                    statements: vec![SqlStatement {
                        sql: "SELECT count(*) FROM refs".into(),
                        parameters: vec![],
                    }],
                },
            )
            .await?;
        assert_eq!(legacy.output[0].rows[0], vec![SqlValue::Integer(0)]);
        // Logical UUID replay keeps the first creation timestamp and intent.
        let CandidateAction::Reserve {
            mut created_ms,
            actor,
            number,
            request,
        } = action
        else {
            return Err("reserve intent missing".into());
        };
        created_ms += 100;
        let replay = CandidateAction::Reserve {
            created_ms,
            actor: actor.clone(),
            number,
            request: request.clone(),
        };
        let CandidateOutcome::Applied(replayed) = repository
            .candidate_action(crate::server::mutation_identity()?, replay)
            .await?
            .output
        else {
            return Err("candidate UUID replay refused".into());
        };
        assert_eq!(*replayed, original);
        let mut collision = request.clone();
        collision.message.push_str(" changed");
        assert!(
            matches!(repository.candidate_action(crate::server::mutation_identity()?, CandidateAction::Reserve {
            actor:actor.clone(), number, request:collision, created_ms,
        }).await, Err(InvocationError::Rejected(value)) if matches!(value.output,CandidateOutcome::Conflict))
        );
        assert_eq!(candidate_count(&repository).await?, 1);

        // Negative preparation changes only editorial state. Its first result
        // wins; it cannot create a fetch ref or claim generated publication.
        let finish = CandidateAction::Finish {
            actor: actor.clone(),
            id: request.id.clone(),
            result: CandidateResult::Unrelated,
        };
        let CandidateOutcome::Applied(finished) = repository
            .candidate_action(crate::server::mutation_identity()?, finish)
            .await?
            .output
        else {
            return Err("negative candidate finish refused".into());
        };
        assert_eq!(finished.result, CandidateResult::Unrelated);
        let CandidateOutcome::Applied(replayed) = repository
            .candidate_action(
                crate::server::mutation_identity()?,
                CandidateAction::Finish {
                    actor: actor.clone(),
                    id: request.id.clone(),
                    result: CandidateResult::Conflicted {
                        paths_base64: vec![],
                    },
                },
            )
            .await?
            .output
        else {
            return Err("negative result replay refused".into());
        };
        assert_eq!(*finished, *replayed);
        let attempted = CandidateAction::Finish {
            actor: actor.clone(),
            id: request.id.clone(),
            result: CandidateResult::Ready {
                oid: hex::encode(native.main),
                tree_oid: hex::encode(native.tree),
            },
        };
        assert!(
            repository
                .prepare_candidate_action(attempted.clone())
                .await
                .is_err()
        );
        // Even an authentic current read observation bound to this exact Ready
        // payload cannot authorize generated catalog/ref publication.
        let snapshot = repository
            .serving_snapshot(ReadIdentity::Account(&actor))
            .await?;
        let selection = snapshot
            .ref_selection(
                attempted.digest()?,
                &["refs/heads/main".into(), "refs/heads/side".into()],
            )
            .await?;
        assert!(
            reserve(
                &repository,
                CandidateRefRequest {
                    selection,
                    action: attempted
                }
            )
            .await
            .is_err()
        );
        assert_eq!(
            repository
                .merge_candidate("canopy", number, &request.id)
                .await?
                .output,
            Some(*finished)
        );
        drop(snapshot);
        assert_eq!(candidate_count(&repository).await?, 1);
        assert_eq!(joint_state(&repository).await?, joint);
        server.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn native_candidate_receiver_binds_purpose_payload_actor_cell_and_current_generation()
-> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let (server, _files) = server().await?;
        let (repository, native, _) = fixture(&server, format).await?;
        let action = intent(&repository, &native).await?;
        let (snapshot, input) = repository.prepare_candidate_action(action).await?;
        let mut invalid = Vec::new();
        let mut payload = input.clone();
        let CandidateAction::Reserve { request, .. } = &mut payload.action else {
            return Err("reserve missing".into());
        };
        request.message.push_str(" substituted");
        invalid.push(payload);
        let mut facts = input.clone();
        facts.selection.facts[0]
            .state
            .as_mut()
            .ok_or("ref absent")?
            .version += 1;
        invalid.push(facts);
        let mut missing = input.clone();
        missing.selection.proof = None;
        invalid.push(missing);
        let mut forged = input.clone();
        forged.selection.proof = Some(damaged(
            forged.selection.proof.as_ref().ok_or("proof absent")?,
        )?);
        invalid.push(forged);
        let mut wrong_repository = input.clone();
        wrong_repository.selection.repository = uuid::Uuid::new_v4().into_bytes();
        invalid.push(wrong_repository);
        let (pull_snapshot, pull_input) = prepare(&repository, "canopy", data(&native)).await?;
        let mut purpose = input.clone();
        purpose.selection = pull_input.selection;
        invalid.push(purpose);
        for refused in invalid {
            assert!(matches!(
                reserve(&repository, refused).await?,
                CandidateOutcome::Conflict
            ));
            assert_eq!(candidate_count(&repository).await?, 0);
        }
        let mut wrong_actor = input.clone();
        wrong_actor.selection.actor = Some("another-actor".into());
        assert!(reserve(&repository, wrong_actor).await.is_err());
        let foreign = create(&server.repositories, "candidate-foreign", format).await?;
        let (other, _, _) = loaded(&server.repositories, foreign.repository_id).await?;
        assert!(matches!(
            reserve(&other, input.clone()).await?,
            CandidateOutcome::Conflict
        ));
        assert_eq!(candidate_count(&other).await?, 0);
        // Retaining identical roots under a new joint generation still fences
        // an old serving observation; it must not reserve an editorial row.
        install(&repository, 3, native.catalog, native.refs).await?;
        assert!(matches!(
            reserve(&repository, input).await?,
            CandidateOutcome::Conflict
        ));
        assert_eq!(candidate_count(&repository).await?, 0);
        drop((snapshot, pull_snapshot));
        server.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn native_candidate_receiver_rechecks_late_access_and_editorial_revision() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let (server, _files) = server().await?;
        let (repository, native, _) = fixture(&server, format).await?;
        let action = intent(&repository, &native).await?;
        let (snapshot, input) = repository.prepare_candidate_action(action.clone()).await?;
        repository
            .edit_pull(
                crate::server::mutation_identity()?,
                "canopy",
                1,
                PullEdit {
                    expected_version: 1,
                    title: "Changed after preparation",
                    body: "",
                    state: PullState::Open,
                    draft: false,
                },
            )
            .await?;
        assert!(matches!(
            reserve(&repository, input).await?,
            CandidateOutcome::Conflict
        ));
        assert_eq!(candidate_count(&repository).await?, 0);
        drop(snapshot);
        let CandidateAction::Reserve {
            request,
            created_ms,
            ..
        } = action
        else {
            return Err("reserve missing".into());
        };
        for role in [None, Some(crate::server::TokenScope::Read)] {
            repository
                .grant_member(
                    crate::server::mutation_identity()?,
                    "canopy",
                    "candidate-writer",
                    crate::server::TokenScope::Write,
                )
                .await?;
            let mut request = request.clone();
            request.id = uuid::Uuid::new_v4().to_string();
            request.revision.pull_version = 2;
            let (snapshot, input) = repository
                .prepare_candidate_action(CandidateAction::Reserve {
                    actor: "candidate-writer".into(),
                    number: 1,
                    request,
                    created_ms,
                })
                .await?;
            if let Some(role) = role {
                repository
                    .grant_member(
                        crate::server::mutation_identity()?,
                        "canopy",
                        "candidate-writer",
                        role,
                    )
                    .await?;
            } else {
                repository
                    .revoke_member(
                        crate::server::mutation_identity()?,
                        "canopy",
                        "candidate-writer",
                    )
                    .await?;
            }
            let result = reserve(&repository, input).await?;
            assert!(matches!(
                (role, result),
                (None, CandidateOutcome::NotFound) | (Some(_), CandidateOutcome::Forbidden)
            ));
            assert_eq!(candidate_count(&repository).await?, 0);
            drop(snapshot);
        }
        server.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn native_candidate_reservation_sql_failure_rolls_back_sdk_acceptance_and_retries_original_command()
-> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let (server, _files) = server().await?;
        let (repository, native, _) = fixture(&server, format).await?;
        let action = intent(&repository, &native).await?;
        let (snapshot, input) = repository.prepare_candidate_action(action).await?;
        let (_, client, _) = loaded(&server.repositories, repository.id).await?;
        let command = client
            .prepare_command::<PrepareCandidate>(
                &repository.target,
                crate::server::mutation_identity()?,
                input,
            )
            .await?;
        let joint = joint_state(&repository).await?;
        let handle = server
            .node
            .runtime()
            .resident_handle(
                &repository.target,
                cellule_runtime::cell::catalog::CatalogRole::Sql,
            )
            .await?
            .ok_or("candidate Cell not resident")?;
        // Fault installation belongs to the trusted test harness. The product
        // SQL primitive correctly refuses statement separators in trigger DDL.
        fault_sql(&handle,"CREATE TRIGGER candidate_insert_fault AFTER INSERT ON merge_candidates BEGIN SELECT RAISE(ABORT,'candidate insertion fault'); END").await?;
        assert!(command.clone().execute().await.is_err());
        assert_eq!(candidate_count(&repository).await?, 0);
        assert_eq!(joint_state(&repository).await?, joint);
        assert!(matches!(
            client.resolve(command.evidence()).await?,
            cellule_runtime::Resolution::Absent
        ));
        fault_sql(&handle, "DROP TRIGGER candidate_insert_fault").await?;
        let applied = command.clone().execute().await?;
        let replayed = command.execute().await?;
        assert_eq!(applied.receipt, replayed.receipt);
        let (CandidateOutcome::Applied(applied), CandidateOutcome::Applied(replayed)) =
            (applied.output, replayed.output)
        else {
            return Err("original candidate command refused".into());
        };
        assert_eq!(applied, replayed);
        assert_eq!(candidate_count(&repository).await?, 1);
        assert_eq!(joint_state(&repository).await?, joint);
        drop(snapshot);
        server.shutdown().await?;
    }
    Ok(())
}
