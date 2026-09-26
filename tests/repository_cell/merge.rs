use super::branch_rules::{commit, identity, plan};
use super::*;
use canopy_server::{
    FinalizePush,
    branch_rules::BranchRuleEdit,
    pulls::{
        NewPull, NewReview, PullChange, PullRevision, PullState, ReviewKind,
        merge::{MergeOutcome, MergeRequest, MergeStrategy},
    },
};
use cellule_runtime::{SqlBatch, SqlCell, SqlStatement, SqlValue};
type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

pub async fn verify(
    repo: &RepositoryCell,
    sql: &SqlCell<RepositoryModule>,
    app: &ApplicationHandle<CanopyApplication>,
    target: &CellTarget,
) -> Result {
    let tree = objects::put(repo, identity()?, ObjectKind::Tree, b"")
        .await?
        .output;
    let base = commit(repo, tree, &[], "Merge base").await?;
    let source = commit(repo, tree, &[base], "Merge source").await?;
    for name in ["refs/canopy", "refs/canopy/merge-candidates/forged"] {
        let plan = plan(repo, name, Some(source)).await?;
        assert!(matches!(
            app.command::<FinalizePush>(target, identity()?, plan).await,
            Err(InvocationError::Rejected(_))
        ));
        assert!(repo.ref_state(name, None).await?.output.is_none());
    }
    let base_ref = "refs/heads/merge-base";
    let source_ref = "refs/heads/merge-source";
    repo.finalize_push(identity()?, plan(repo, base_ref, Some(base)).await?)
        .await?;
    repo.finalize_push(identity()?, plan(repo, source_ref, Some(source)).await?)
        .await?;
    repo.grant_member(identity()?, "canopy", "merge-author", TokenScope::Read)
        .await?;
    let PullChange::Applied(number) = repo
        .create_pull(
            identity()?,
            "merge-author",
            NewPull {
                id: uuid::Uuid::new_v4().into_bytes(),
                title: "Merge",
                body: "",
                draft: false,
                source_ref,
                source_oid: &hex::encode(source),
                base_ref,
                base_oid: &hex::encode(base),
            },
        )
        .await?
        .output
    else {
        return Err("missing pull".into());
    };
    let edit = |version| BranchRuleEdit {
        reference: base_ref.into(),
        expected_version: version,
        enabled: true,
        deny_deletions: false,
        fast_forward_only: false,
        required_checks: vec![],
        require_pull_request: true,
        required_approvals: 1,
    };
    repo.set_branch_rule(identity()?, "canopy", edit(0)).await?;
    let revision = PullRevision {
        pull_version: 1,
        source_oid: hex::encode(source),
        source_version: 1,
        base_oid: hex::encode(base),
        base_version: 1,
    };
    let request = MergeRequest {
        id: uuid::Uuid::new_v4().to_string(),
        revision: revision.clone(),
        strategy: MergeStrategy::FastForward,
        candidate_id: None,
    };
    assert!(
        matches!(repo.merge_pull(identity()?,"outsider",number,request.clone()).await,Err(InvocationError::Rejected(rejected)) if rejected.output == MergeOutcome::NotFound)
    );
    assert!(
        matches!(repo.merge_pull(identity()?,"merge-author",number,request.clone()).await,Err(InvocationError::Rejected(rejected)) if rejected.output == MergeOutcome::Forbidden)
    );
    let denied_identity = identity()?;
    assert!(
        matches!(repo.merge_pull(denied_identity,"canopy",number,request.clone()).await,Err(InvocationError::Rejected(rejected)) if rejected.output == MergeOutcome::ReviewsRequired)
    );
    repo.review_pull(
        identity()?,
        "canopy",
        number,
        NewReview {
            id: uuid::Uuid::new_v4().into_bytes(),
            revision: &revision,
            kind: ReviewKind::Approve,
            body: "Approved",
        },
    )
    .await?;
    assert!(
        repo.pull_review_policy("canopy", number)
            .await?
            .output
            .unwrap()
            .reviews_satisfied
    );
    // The raw publication command has no merge capability, even for the owner
    // and a tip with an eligible approved proposal.
    let direct = plan(repo, base_ref, Some(source)).await?;
    assert!(
        matches!(app.command::<FinalizePush>(target,identity()?,direct).await,Err(InvocationError::Rejected(rejected)) if !rejected.output)
    );
    assert!(
        matches!(repo.merge_pull(denied_identity,"canopy",number,request.clone()).await,Err(InvocationError::Rejected(rejected)) if rejected.output == MergeOutcome::ReviewsRequired)
    );
    // Force a constraint failure after the ref update inside the merge command.
    // The injected duplicate parent record is removed after proving rollback.
    let injected = uuid::Uuid::new_v4().into_bytes();
    sql.batch(identity()?,SqlBatch {statements:vec![SqlStatement {
        sql:"INSERT INTO pull_merges (id,binding,pull_number,oid,merged_ms,pull_version,source_oid,source_version,base_oid,base_version) VALUES (?1,?2,?3,?4,0,1,?5,1,?4,1)".into(),parameters:vec![SqlValue::Blob(injected.to_vec()),SqlValue::Blob(vec![0;32]),SqlValue::Integer(number),SqlValue::Blob(base.to_vec()),SqlValue::Blob(source.to_vec())],
    }]}).await?;
    let before = repo.ref_state(base_ref, None).await?.output;
    let generation = repo.refs_page("", None).await?.output.generation;
    assert!(
        repo.merge_pull(identity()?, "canopy", number, request.clone())
            .await
            .is_err()
    );
    assert_eq!(repo.ref_state(base_ref, None).await?.output, before);
    assert_eq!(
        repo.refs_page("", None).await?.output.generation,
        generation
    );
    assert_eq!(
        repo.pull("canopy", number)
            .await?
            .output
            .unwrap()
            .summary
            .state,
        PullState::Open
    );
    sql.batch(
        identity()?,
        SqlBatch {
            statements: vec![SqlStatement {
                sql: "DELETE FROM pull_merges WHERE id = ?1".into(),
                parameters: vec![SqlValue::Blob(injected.to_vec())],
            }],
        },
    )
    .await?;
    let command_id = identity()?;
    let merged = repo
        .merge_pull(command_id, "canopy", number, request.clone())
        .await?;
    let MergeOutcome::Applied { merge: record } = &merged.output else {
        return Err("merge not applied".into());
    };
    assert_eq!(record.oid, hex::encode(source));
    assert_eq!(
        repo.merge_pull(command_id, "canopy", number, request.clone())
            .await?
            .receipt,
        merged.receipt
    );
    let mut stricter = edit(1);
    stricter.required_approvals = 2;
    repo.set_branch_rule(identity()?, "canopy", stricter)
        .await?;
    assert_eq!(
        repo.merge_pull(identity()?, "canopy", number, request)
            .await?
            .output,
        merged.output
    );
    let published = repo.ref_state(base_ref, None).await?.output.unwrap();
    assert_eq!(published.oid, Some(source));
    assert_eq!(published.version, 2);
    Ok(())
}
