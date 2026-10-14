//! Real resident, immutable ref roots and final typed receiver authorization.
use super::*;
use crate::pulls::{
    NewPull, NewReview, PullChange, PullEdit, PullRevision, PullState, ReviewKind,
    native::{
        CreateData, CreateNativePull, CreateRequest, ReadData, ReadKind, ReadNativePulls,
        ReadReply, ReadRequest, ReviewData, ReviewNativePull, ReviewRequest,
    },
};
use cellule_runtime::codec::BoundedDecoder;
use cellule_runtime::{Committed, InvocationError};

mod candidates;

fn data(native: &BrowseFixture) -> CreateData {
    CreateData {
        id: uuid::Uuid::new_v4().into_bytes(),
        title: "Native review".into(),
        body: "Immutable refs".into(),
        draft: false,
        source_ref: "refs/heads/main".into(),
        source_oid: hex::encode(native.main),
        base_ref: "refs/heads/side".into(),
        base_oid: hex::encode(native.side),
    }
}
async fn prepare(
    repository: &RepositoryCell,
    actor: &str,
    data: CreateData,
) -> Result<(crate::packs::publication::ServingSnapshot, CreateRequest)> {
    let snapshot = repository
        .serving_snapshot(ReadIdentity::Account(actor))
        .await?;
    let mut names = vec![data.source_ref.clone(), data.base_ref.clone()];
    names.sort();
    let selection = snapshot.ref_selection(data.digest()?, &names).await?;
    Ok((snapshot, CreateRequest { selection, data }))
}
async fn execute(
    repository: &RepositoryCell,
    input: CreateRequest,
) -> Result<Committed<PullChange>> {
    match repository
        .application
        .command::<CreateNativePull>(
            &repository.target,
            crate::server::mutation_identity()?,
            input,
        )
        .await
    {
        Ok(value) => Ok(value),
        Err(InvocationError::Rejected(value)) => Ok(*value),
        Err(error) => Err(error.into()),
    }
}
async fn count(repository: &RepositoryCell) -> Result<i64> {
    let result = repository
        .sql
        .query(
            None,
            SqlBatch {
                statements: vec![SqlStatement {
                    sql: "SELECT count(*) FROM pull_requests".into(),
                    parameters: vec![],
                }],
            },
        )
        .await?;
    let Some([SqlValue::Integer(n)]) = result.output[0].rows.first().map(Vec::as_slice) else {
        return Err("pull count missing".into());
    };
    Ok(*n)
}
fn damaged<T: WireValue>(proof: &T) -> Result<T> {
    let mut e = BoundedEncoder::new(1024)?;
    proof.encode(&mut e)?;
    let mut bytes = e.finish();
    *bytes.last_mut().ok_or("proof empty")? ^= 1;
    let mut d = BoundedDecoder::new(&bytes, 1024)?;
    let proof = T::decode(&mut d)?;
    d.finish()?;
    Ok(proof)
}
#[tokio::test]
async fn native_pull_receivers_bind_request_actor_cell_exact_refs_and_current_generation() -> Result
{
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let (server, _files) = server().await?;
        let (repository, native, _) = fixture(&server, format).await?;
        let (snapshot, input) = prepare(&repository, "canopy", data(&native)).await?;
        assert_eq!(
            execute(&repository, input.clone()).await?.output,
            PullChange::Applied(1)
        );
        assert_eq!(
            repository
                .pull("canopy", 1)
                .await?
                .output
                .ok_or("pull absent")?
                .summary
                .source
                .oid,
            Some(hex::encode(native.main))
        );
        let mut invalid = Vec::new();
        let mut forged = input.clone();
        forged.selection.proof = Some(damaged(
            forged.selection.proof.as_ref().ok_or("proof absent")?,
        )?);
        invalid.push(forged);
        let mut changed_payload = input.clone();
        changed_payload.data.title.push_str(" substituted");
        invalid.push(changed_payload);
        let mut wrong_repo = input.clone();
        wrong_repo.selection.repository = uuid::Uuid::new_v4().into_bytes();
        invalid.push(wrong_repo);
        let mut changed_oid = input.clone();
        changed_oid.selection.facts[0]
            .state
            .as_mut()
            .ok_or("ref absent")?
            .oid = Some(native.previous);
        invalid.push(changed_oid);
        let mut changed_version = input.clone();
        changed_version.selection.facts[0]
            .state
            .as_mut()
            .ok_or("ref absent")?
            .version += 1;
        invalid.push(changed_version);
        let mut changed_name = input.clone();
        changed_name.selection.facts[0].name = "refs/heads/other".into();
        invalid.push(changed_name);
        let mut missing_proof = input.clone();
        missing_proof.selection.proof = None;
        invalid.push(missing_proof);
        let mut missing_fact = input.clone();
        missing_fact.selection.facts.remove(0);
        invalid.push(missing_fact);
        repository
            .grant_member(
                crate::server::mutation_identity()?,
                "canopy",
                "reader",
                crate::server::TokenScope::Read,
            )
            .await?;
        let mut wrong_actor = input.clone();
        wrong_actor.selection.actor = Some("reader".into());
        invalid.push(wrong_actor);
        for input in invalid {
            assert_eq!(
                execute(&repository, input).await?.output,
                PullChange::Conflict
            );
            assert_eq!(count(&repository).await?, 1);
        }
        let entry = create(&server.repositories, "foreign-pull", format).await?;
        let (other, _, _) = loaded(&server.repositories, entry.repository_id).await?;
        assert_eq!(
            execute(&other, input.clone()).await?.output,
            PullChange::Conflict
        );
        assert_eq!(count(&other).await?, 0);
        drop(other);
        let result = repository
            .application
            .query::<ReadNativePulls>(
                &repository.target,
                None,
                ReadRequest {
                    selection: input.selection.clone(),
                    data: ReadData {
                        kind: ReadKind::Detail(1),
                        metadata: [42; 32],
                    },
                },
            )
            .await?;
        assert!(
            matches!(result.output, ReadReply::Changed),
            "creation proof cannot authorize another purpose"
        );
        // Large valid names increase the admitted request, not certificate size.
        let mut long = data(&native);
        long.source_ref = format!("refs/heads/{}", "x".repeat(60_000));
        let (long_snapshot, long_input) = prepare(&repository, "canopy", long).await?;
        let mut e = BoundedEncoder::new(1024)?;
        long_input
            .selection
            .proof
            .as_ref()
            .ok_or("long proof absent")?
            .encode(&mut e)?;
        assert!(e.finish().len() < 1024);
        assert_eq!(
            execute(&repository, long_input).await?.output,
            PullChange::Conflict
        );
        drop(long_snapshot);
        // Same immutable roots under a later joint fact cannot authorize old ref policy.
        install(&repository, 3, native.catalog, native.refs).await?;
        assert_eq!(
            execute(&repository, input.clone()).await?.output,
            PullChange::Conflict
        );
        let original = &input.data;
        assert_eq!(
            repository
                .create_pull(
                    crate::server::mutation_identity()?,
                    "canopy",
                    NewPull {
                        id: original.id,
                        title: &original.title,
                        body: &original.body,
                        draft: original.draft,
                        source_ref: &original.source_ref,
                        source_oid: &original.source_oid,
                        base_ref: &original.base_ref,
                        base_oid: &original.base_oid
                    }
                )
                .await?
                .output,
            PullChange::Applied(1)
        );
        drop(snapshot);
        timeout(Duration::from_secs(15), async {
            let pool = repository
                .serving
                .lock()
                .unwrap()
                .as_ref()
                .and_then(std::sync::Weak::upgrade)
                .ok_or("pool absent")?;
            pool.close_and_drain().await;
            Result::Ok(())
        })
        .await??;
        assert_eq!(retained(&repository).await?, 0);
        assert_eq!(
            execute(&repository, input).await?.output,
            PullChange::Conflict
        );
        drop(repository);
        timeout(Duration::from_secs(15), server.shutdown()).await??;
    }
    Ok(())
}
#[tokio::test]
async fn native_pull_receivers_recheck_late_revocation_without_creating_editorial_rows() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let (server, _files) = server().await?;
        let (repository, native, _) = fixture(&server, format).await?;
        repository
            .grant_member(
                crate::server::mutation_identity()?,
                "canopy",
                "reader",
                crate::server::TokenScope::Read,
            )
            .await?;
        let (snapshot, input) = prepare(&repository, "reader", data(&native)).await?;
        repository
            .revoke_member(crate::server::mutation_identity()?, "canopy", "reader")
            .await?;
        assert_eq!(
            execute(&repository, input).await?.output,
            PullChange::NotFound
        );
        assert_eq!(count(&repository).await?, 0);
        assert!(repository.pulls("reader", 0, None).await?.output.is_none());
        drop(snapshot);
        drop(repository);
        timeout(Duration::from_secs(15), server.shutdown()).await??;
    }
    Ok(())
}

async fn read_request(
    repository: &RepositoryCell,
    actor: ReadIdentity<'_>,
) -> Result<(crate::packs::publication::ServingSnapshot, ReadRequest)> {
    read_kind_request(repository, actor, ReadKind::Detail(1)).await
}
async fn read_kind_request(
    repository: &RepositoryCell,
    actor: ReadIdentity<'_>,
    kind: ReadKind,
) -> Result<(crate::packs::publication::ServingSnapshot, ReadRequest)> {
    let selected = repository
        .sql
        .query(
            None,
            SqlBatch {
                statements: vec![SqlStatement {
        sql: "SELECT number,version,source_ref,base_ref FROM pull_requests WHERE number=1".into(),
        parameters: vec![],
    }],
            },
        )
        .await?;
    let (data, names) = ReadData::selected(kind, &selected.output[0].rows)?;
    let snapshot = repository.serving_snapshot(actor).await?;
    let selection = snapshot.ref_selection(data.digest()?, &names).await?;
    Ok((snapshot, ReadRequest { selection, data }))
}
async fn read(repository: &RepositoryCell, input: ReadRequest) -> Result<ReadReply> {
    Ok(repository
        .application
        .query::<ReadNativePulls>(&repository.target, None, input)
        .await?
        .output)
}
async fn prepared_policy(
    repository: &RepositoryCell,
    request: &ReadRequest,
) -> Result<Vec<SqlValue>> {
    let ReadReply::Rows(Some(mut sets)) = read(repository, request.clone()).await? else {
        return Err("prepared native policy unavailable".into());
    };
    if sets.len() != 1 || sets[0].rows.len() != 1 {
        return Err("prepared native policy row count".into());
    }
    Ok(sets.remove(0).rows.remove(0))
}
async fn review(repository: &RepositoryCell, input: ReviewRequest) -> Result<PullChange> {
    match repository
        .application
        .command::<ReviewNativePull>(
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

#[tokio::test]
async fn native_pull_receivers_recheck_editorial_version_and_review_payload_in_final_transaction()
-> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let (server, _files) = server().await?;
        let (repository, native, _) = fixture(&server, format).await?;
        let (creation, input) = prepare(&repository, "canopy", data(&native)).await?;
        assert_eq!(
            execute(&repository, input).await?.output,
            PullChange::Applied(1)
        );
        repository
            .grant_member(
                crate::server::mutation_identity()?,
                "canopy",
                "reviewer",
                crate::server::TokenScope::Write,
            )
            .await?;
        let (observation, old_read) =
            read_request(&repository, ReadIdentity::Account("canopy")).await?;
        assert!(matches!(
            read(&repository, old_read.clone()).await?,
            ReadReply::Rows(Some(_))
        ));
        let data = ReviewData {
            number: 1,
            id: uuid::Uuid::new_v4().into_bytes(),
            revision: PullRevision {
                pull_version: 1,
                source_oid: hex::encode(native.main),
                source_version: 1,
                base_oid: hex::encode(native.side),
                base_version: 1,
            },
            kind: ReviewKind::Approve,
            body: "Approved exact revision".into(),
        };
        let reviewer = repository
            .serving_snapshot(ReadIdentity::Account("reviewer"))
            .await?;
        let selection = reviewer
            .ref_selection(
                data.digest()?,
                &["refs/heads/main".into(), "refs/heads/side".into()],
            )
            .await?;
        let old_review = ReviewRequest { selection, data };
        let mut altered = old_review.clone();
        altered.data.body.push_str(" substituted");
        assert_eq!(review(&repository, altered).await?, PullChange::Conflict);
        assert_eq!(
            repository
                .edit_pull(
                    crate::server::mutation_identity()?,
                    "canopy",
                    1,
                    PullEdit {
                        expected_version: 1,
                        title: "Edited after preparation",
                        body: "",
                        state: PullState::Open,
                        draft: false,
                    }
                )
                .await?
                .output,
            PullChange::Applied(1)
        );
        // Git generation is unchanged. The final query must still detect the
        // intervening editorial edit; a prepared review cannot approve it.
        assert!(matches!(
            read(&repository, old_read).await?,
            ReadReply::Changed
        ));
        assert_eq!(
            review(&repository, old_review.clone()).await?,
            PullChange::Conflict
        );
        assert!(
            repository
                .pull_reviews("canopy", 1, 0)
                .await?
                .output
                .ok_or("reviews absent")?
                .is_empty()
        );
        let (fresh, request) = read_request(&repository, ReadIdentity::Account("canopy")).await?;
        assert!(matches!(
            read(&repository, request).await?,
            ReadReply::Rows(Some(_))
        ));
        repository
            .revoke_member(crate::server::mutation_identity()?, "canopy", "reviewer")
            .await?;
        assert_eq!(review(&repository, old_review).await?, PullChange::NotFound);
        assert!(
            repository
                .pull_reviews("canopy", 1, 0)
                .await?
                .output
                .ok_or("reviews absent")?
                .is_empty()
        );
        drop((creation, observation, reviewer, fresh, repository));
        timeout(Duration::from_secs(15), server.shutdown()).await??;
    }
    Ok(())
}

#[tokio::test]
async fn native_pull_receivers_recheck_anonymous_visibility_after_proof_preparation() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let (server, _files) = server().await?;
        let (repository, native, _) = fixture(&server, format).await?;
        let (creation, input) = prepare(&repository, "canopy", data(&native)).await?;
        assert_eq!(
            execute(&repository, input).await?.output,
            PullChange::Applied(1)
        );
        repository
            .sql
            .batch(
                crate::server::mutation_identity()?,
                SqlBatch {
                    statements: vec![SqlStatement {
                        sql: "UPDATE ref_generation SET visibility='public' WHERE singleton=1"
                            .into(),
                        parameters: vec![],
                    }],
                },
            )
            .await?;
        let (snapshot, request) = read_request(&repository, ReadIdentity::Anonymous).await?;
        assert!(matches!(
            read(&repository, request.clone()).await?,
            ReadReply::Rows(Some(_))
        ));
        assert_eq!(
            repository
                .pulls(ReadIdentity::Anonymous, 0, None)
                .await?
                .output
                .ok_or("public list absent")?
                .len(),
            1
        );
        repository
            .sql
            .batch(
                crate::server::mutation_identity()?,
                SqlBatch {
                    statements: vec![SqlStatement {
                        sql: "UPDATE ref_generation SET visibility='private' WHERE singleton=1"
                            .into(),
                        parameters: vec![],
                    }],
                },
            )
            .await?;
        assert!(matches!(
            read(&repository, request).await?,
            ReadReply::Rows(None)
        ));
        assert!(
            repository
                .pulls(ReadIdentity::Anonymous, 0, None)
                .await?
                .output
                .is_none()
        );
        drop((creation, snapshot, repository));
        timeout(Duration::from_secs(15), server.shutdown()).await??;
    }
    Ok(())
}

async fn replace_main_ref(
    server: &crate::server::RunningServer,
    repository: &RepositoryCell,
    oid: Option<ObjectId>,
    expected: crate::refs::RefExpectation,
) -> Result {
    let store = Arc::new(ArtifactStore::new(
        server.repositories.external_store.clone(),
        repository.repository_id(),
    ));
    let snapshot = repository
        .serving_snapshot(ReadIdentity::Account("canopy"))
        .await?;
    let fact = snapshot.fact();
    let mut refs = fact.refs.ok_or("refs absent")?.read(&store).await?;
    let index =
        crate::packs::ref_state::RefStateIndex::new(store.clone(), repository.object_format());
    let transition = index
        .prepare(
            refs.root.clone(),
            operation(500 + 2 * fact.generation),
            &crate::PushPlan {
                actor: "canopy".into(),
                updates: vec![crate::RefUpdate {
                    name: "refs/heads/main".into(),
                    expected: Some(expected),
                    new_oid: oid,
                }],
            },
        )
        .await?;
    refs.root = Some(transition.root());
    refs.generation = fact.generation + 1;
    let root =
        RefStateSnapshotRoot::upload(&store, operation(501 + 2 * fact.generation), refs).await?;
    drop(snapshot);
    // Trusted immutable-root fixture; this qualifies the reader, not a merge writer.
    install(
        repository,
        (fact.generation + 1) as i64,
        fact.catalog.ok_or("catalog absent")?,
        root,
    )
    .await
}

#[tokio::test]
async fn native_review_policy_observes_current_rules_reviews_membership_and_ref_aba() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let (server, _files) = server().await?;
        let (repository, native, _) = fixture(&server, format).await?;
        let (creation, input) = prepare(&repository, "canopy", data(&native)).await?;
        assert_eq!(
            execute(&repository, input).await?.output,
            PullChange::Applied(1)
        );
        let initial = repository
            .pull_review_policy("canopy", 1)
            .await?
            .output
            .ok_or("native review policy missing")?;
        assert!(initial.ready && initial.reviews_satisfied);
        assert_eq!(initial.required_approvals, 0);
        let revision = initial.revision.ok_or("revision absent")?;
        assert_eq!(revision.source_oid, hex::encode(native.main));
        assert_eq!(revision.base_oid, hex::encode(native.side));
        let (policy_snapshot, policy_request) = read_kind_request(
            &repository,
            ReadIdentity::Account("canopy"),
            ReadKind::ReviewPolicy(1),
        )
        .await?;
        let mut different_purpose = policy_request.clone();
        different_purpose.data.kind = ReadKind::Detail(1);
        assert!(matches!(
            read(&repository, different_purpose).await?,
            ReadReply::Changed
        ));
        let mut different_number = policy_request.clone();
        different_number.data.kind = ReadKind::ReviewPolicy(2);
        assert!(matches!(
            read(&repository, different_number).await?,
            ReadReply::Changed
        ));
        repository
            .set_branch_rule(
                crate::server::mutation_identity()?,
                "canopy",
                crate::branch_rules::BranchRuleEdit {
                    reference: "refs/heads/side".into(),
                    expected_version: 0,
                    enabled: true,
                    deny_deletions: false,
                    fast_forward_only: false,
                    required_checks: vec![],
                    require_pull_request: true,
                    required_approvals: 2,
                },
            )
            .await?;
        let mut old = None;
        for reviewer in ["one", "two"] {
            repository
                .grant_member(
                    crate::server::mutation_identity()?,
                    "canopy",
                    reviewer,
                    crate::server::TokenScope::Write,
                )
                .await?;
            let id = uuid::Uuid::new_v4().into_bytes();
            let value = repository
                .review_pull(
                    crate::server::mutation_identity()?,
                    reviewer,
                    1,
                    NewReview {
                        id,
                        revision: &revision,
                        kind: ReviewKind::Approve,
                        body: "Reviewed exact refs",
                    },
                )
                .await?;
            assert!(matches!(value.output, PullChange::Applied(_)));
            if reviewer == "two" {
                old = Some(id);
            }
        }
        let current = repository
            .pull_review_policy("canopy", 1)
            .await?
            .output
            .ok_or("policy absent")?;
        assert_eq!(
            (
                current.rule_version,
                current.required_approvals,
                current.approvals
            ),
            (1, 2, 2)
        );
        assert!(current.reviews_satisfied);
        // Prepared before the rule and reviews existed: final policy is fresh
        // SQL, while the exact native refs and editorial selection stay bound.
        let prepared = prepared_policy(&repository, &policy_request).await?;
        assert_eq!(
            &prepared[9..14],
            &[
                SqlValue::Integer(1),
                SqlValue::Integer(1),
                SqlValue::Integer(2),
                SqlValue::Integer(2),
                SqlValue::Integer(0),
            ]
        );
        let (reviewer_snapshot, reviewer_request) = read_kind_request(
            &repository,
            ReadIdentity::Account("two"),
            ReadKind::ReviewPolicy(1),
        )
        .await?;
        repository
            .revoke_member(crate::server::mutation_identity()?, "canopy", "two")
            .await?;
        assert!(matches!(
            read(&repository, reviewer_request).await?,
            ReadReply::Rows(None)
        ));
        repository
            .grant_member(
                crate::server::mutation_identity()?,
                "canopy",
                "two",
                crate::server::TokenScope::Write,
            )
            .await?;
        let retry = repository
            .review_pull(
                crate::server::mutation_identity()?,
                "two",
                1,
                NewReview {
                    id: old.ok_or("old review absent")?,
                    revision: &revision,
                    kind: ReviewKind::Approve,
                    body: "Reviewed exact refs",
                },
            )
            .await?;
        assert!(matches!(retry.output, PullChange::Applied(_)));
        let current = repository
            .pull_review_policy("canopy", 1)
            .await?
            .output
            .ok_or("policy absent")?;
        assert_eq!(current.approvals, 1);
        assert!(!current.reviews_satisfied);
        assert_eq!(
            prepared_policy(&repository, &policy_request).await?[12],
            SqlValue::Integer(1)
        );
        repository
            .review_pull(
                crate::server::mutation_identity()?,
                "two",
                1,
                NewReview {
                    id: uuid::Uuid::new_v4().into_bytes(),
                    revision: &revision,
                    kind: ReviewKind::Approve,
                    body: "Fresh grant",
                },
            )
            .await?;
        assert_eq!(
            repository
                .pull_review_policy("canopy", 1)
                .await?
                .output
                .ok_or("policy absent")?
                .approvals,
            2
        );
        for (kind, approvals, changes) in [
            (ReviewKind::RequestChanges, 1, 1),
            (ReviewKind::Comment, 1, 1),
            (ReviewKind::Approve, 2, 0),
        ] {
            repository
                .review_pull(
                    crate::server::mutation_identity()?,
                    "two",
                    1,
                    NewReview {
                        id: uuid::Uuid::new_v4().into_bytes(),
                        revision: &revision,
                        kind,
                        body: "Current decision",
                    },
                )
                .await?;
            let current = prepared_policy(&repository, &policy_request).await?;
            assert_eq!(
                &current[12..14],
                &[SqlValue::Integer(approvals), SqlValue::Integer(changes)]
            );
            let public = repository
                .pull_review_policy("canopy", 1)
                .await?
                .output
                .ok_or("policy absent")?;
            assert_eq!(public.reviews_satisfied, kind == ReviewKind::Approve);
        }
        replace_main_ref(
            &server,
            &repository,
            None,
            crate::refs::RefExpectation {
                oid: Some(native.main),
                version: 1,
            },
        )
        .await?;
        let deleted = repository
            .pull_review_policy("canopy", 1)
            .await?
            .output
            .ok_or("deleted policy absent")?;
        assert!(!deleted.ready && deleted.revision.is_none());
        assert_eq!(deleted.approvals, 0);
        assert!(matches!(
            read(&repository, policy_request).await?,
            ReadReply::Changed
        ));
        replace_main_ref(
            &server,
            &repository,
            Some(native.main),
            crate::refs::RefExpectation {
                oid: None,
                version: 2,
            },
        )
        .await?;
        let recreated = repository
            .pull_review_policy("canopy", 1)
            .await?
            .output
            .ok_or("recreated policy absent")?;
        assert!(recreated.ready);
        assert_eq!(
            recreated
                .revision
                .ok_or("recreated revision absent")?
                .source_version,
            3
        );
        assert_eq!(recreated.approvals, 0);
        assert!(!recreated.reviews_satisfied);
        drop((creation, policy_snapshot, reviewer_snapshot, repository));
        timeout(Duration::from_secs(15), server.shutdown()).await??;
    }
    Ok(())
}

#[tokio::test]
async fn native_thread_receiver_binds_verified_anchor_and_rechecks_editorial_revision() -> Result {
    use crate::git_read::{ComparisonTarget, Side, patch::LineAnchor};
    use crate::pulls::{
        native::CreateNativeThread,
        native::threads::{ThreadData, ThreadRequest},
        threads::ThreadIntent,
    };
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let (server, _files) = server().await?;
        let (repository, native, _) = fixture(&server, format).await?;
        let (creation, input) = prepare(&repository, "canopy", data(&native)).await?;
        assert_eq!(
            execute(&repository, input).await?.output,
            PullChange::Applied(1)
        );
        let revision = PullRevision {
            pull_version: 1,
            source_oid: hex::encode(native.main),
            source_version: 1,
            base_oid: hex::encode(native.side),
            base_version: 1,
        };
        let data = ThreadData {
            number: 1,
            intent: ThreadIntent {
                id: uuid::Uuid::new_v4().into_bytes(),
                target: ComparisonTarget::Current {
                    revision: revision.clone(),
                },
                path_base64: "ZmlsZQ".into(),
                side: Side::After,
                line: 1,
                body: "Original discussion".into(),
            },
            anchor: LineAnchor {
                revision,
                merge_base: hex::encode(native.side),
                path_base64: "ZmlsZQ".into(),
                side: Side::After,
                line: 1,
                blob_oid: hex::encode(native.main),
            },
        };
        let snapshot = repository
            .serving_snapshot(ReadIdentity::Account("canopy"))
            .await?;
        let selection = snapshot
            .ref_selection(
                data.digest()?,
                &["refs/heads/main".into(), "refs/heads/side".into()],
            )
            .await?;
        // Each substitution keeps valid shape but must invalidate its purpose MAC.
        let encoded = serde_json::to_vec(&data)?;
        for change in ["body", "blob", "line"] {
            let mut substituted: ThreadData = serde_json::from_slice(&encoded)?;
            match change {
                "body" => substituted.intent.body.push_str(" substituted"),
                "blob" => substituted.anchor.blob_oid = hex::encode(native.side),
                _ => {
                    substituted.intent.line = 2;
                    substituted.anchor.line = 2;
                }
            }
            let result = repository
                .application
                .command::<CreateNativeThread>(
                    &repository.target,
                    crate::server::mutation_identity()?,
                    ThreadRequest {
                        selection: selection.clone(),
                        data: substituted,
                    },
                )
                .await;
            let Err(InvocationError::Rejected(result)) = result else {
                return Err("substituted anchor accepted".into());
            };
            assert_eq!(result.output, PullChange::Conflict);
            assert!(
                repository
                    .threads("canopy", 1, 0)
                    .await?
                    .ok_or("thread page absent")?
                    .is_empty()
            );
        }
        repository
            .edit_pull(
                crate::server::mutation_identity()?,
                "canopy",
                1,
                PullEdit {
                    expected_version: 1,
                    title: "Edited while anchor was prepared",
                    body: "",
                    state: PullState::Open,
                    draft: false,
                },
            )
            .await?;
        let result = repository
            .application
            .command::<CreateNativeThread>(
                &repository.target,
                crate::server::mutation_identity()?,
                ThreadRequest { selection, data },
            )
            .await;
        let Err(InvocationError::Rejected(result)) = result else {
            return Err("stale anchor accepted".into());
        };
        assert_eq!(result.output, PullChange::Conflict);
        assert!(
            repository
                .threads("canopy", 1, 0)
                .await?
                .ok_or("thread page absent")?
                .is_empty()
        );
        drop((snapshot, creation, repository));
        timeout(Duration::from_secs(15), server.shutdown()).await??;
    }
    Ok(())
}
