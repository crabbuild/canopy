//! Real resident, immutable ref roots and final typed receiver authorization.
use super::*;
use crate::pulls::{
    NewPull, PullChange, PullEdit, PullRevision, PullState, ReviewKind,
    native::{
        CreateData, CreateNativePull, CreateRequest, ReadData, ReadKind, ReadNativePulls,
        ReadReply, ReadRequest, ReviewData, ReviewNativePull, ReviewRequest,
    },
};
use cellule_runtime::codec::BoundedDecoder;
use cellule_runtime::{Committed, InvocationError};

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
    let (data, names) = ReadData::selected(ReadKind::Detail(1), &selected.output[0].rows)?;
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
