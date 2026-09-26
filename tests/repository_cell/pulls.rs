use super::*;
use canopy_server::pulls::{
    NewPull, NewReview, PullChange, PullEdit, PullRevision, PullState, ReviewKind,
};
type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;
fn identity() -> Result<MutationIdentity> {
    let now = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis())?;
    Ok(MutationIdentity {
        request_id: RequestId::from_bytes(uuid::Uuid::new_v4().into_bytes()),
        issued_at_ms: now,
        expires_at_ms: now + 60_000,
    })
}
async fn number(repo: &RepositoryCell, actor: &str, input: NewPull<'_>) -> Result<i64> {
    let PullChange::Applied(number) = repo.create_pull(identity()?, actor, input).await?.output
    else {
        return Err("pull creation failed".into());
    };
    Ok(number)
}
pub async fn verify(repo: &RepositoryCell) -> Result {
    let tree = objects::put(repo, identity()?, ObjectKind::Tree, b"")
        .await?
        .output;
    let body = format!(
        "tree {}\nauthor Test <test@example.invalid> 0 +0000\ncommitter Test <test@example.invalid> 0 +0000\n\nPull base\n",
        hex::encode(tree)
    );
    let base = objects::put(repo, identity()?, ObjectKind::Commit, body.as_bytes())
        .await?
        .output;
    let body = format!(
        "tree {}\nparent {}\nauthor Test <test@example.invalid> 0 +0000\ncommitter Test <test@example.invalid> 0 +0000\n\nPull source\n",
        hex::encode(tree),
        hex::encode(base)
    );
    let source = objects::put(repo, identity()?, ObjectKind::Commit, body.as_bytes())
        .await?
        .output;
    repo.finalize_push(
        identity()?,
        PushPlan {
            actor: "canopy".into(),
            updates: vec![
                RefUpdate {
                    name: "refs/heads/pull-base".into(),
                    expected: None,
                    new_oid: Some(base),
                },
                RefUpdate {
                    name: "refs/heads/pull-source".into(),
                    expected: None,
                    new_oid: Some(source),
                },
            ],
        },
    )
    .await?;
    repo.grant_member(identity()?, "canopy", "pull-author", TokenScope::Read)
        .await?;
    repo.grant_member(identity()?, "canopy", "pull-reviewer", TokenScope::Write)
        .await?;
    let base_oid = hex::encode(base);
    let source_oid = hex::encode(source);
    let id = uuid::Uuid::new_v4().into_bytes();
    let title = "é".repeat(128);
    let body = "x".repeat(16384);
    let new = || NewPull {
        id,
        title: &title,
        body: &body,
        draft: false,
        source_ref: "refs/heads/pull-source",
        source_oid: &source_oid,
        base_ref: "refs/heads/pull-base",
        base_oid: &base_oid,
    };
    assert_eq!(
        repo.create_pull(identity()?, "outsider", new())
            .await?
            .output,
        PullChange::NotFound
    );
    let create_id = identity()?;
    let created = repo.create_pull(create_id, "pull-author", new()).await?;
    assert_eq!(
        repo.create_pull(create_id, "pull-author", new())
            .await?
            .receipt,
        created.receipt
    );
    let PullChange::Applied(pull) = created.output else {
        return Err("missing pull".into());
    };
    let other = number(
        repo,
        "pull-author",
        NewPull {
            id: uuid::Uuid::new_v4().into_bytes(),
            ..new()
        },
    )
    .await?;
    assert!(repo.pull("outsider", pull).await?.output.is_none());
    assert!(repo.pulls("outsider", 0, None).await?.output.is_none());
    assert!(
        repo.pull_reviews("outsider", pull, 0)
            .await?
            .output
            .is_none()
    );
    let revision = PullRevision {
        pull_version: 1,
        source_oid: source_oid.clone(),
        source_version: 1,
        base_oid: base_oid.clone(),
        base_version: 1,
    };
    let review_id = uuid::Uuid::new_v4().into_bytes();
    let review = || NewReview {
        id: review_id,
        revision: &revision,
        kind: ReviewKind::Approve,
        body: &body,
    };
    assert_eq!(
        repo.review_pull(identity()?, "pull-author", pull, review())
            .await?
            .output,
        PullChange::Forbidden
    );
    assert_eq!(
        repo.review_pull(identity()?, "outsider", pull, review())
            .await?
            .output,
        PullChange::NotFound
    );
    let review_mutation = identity()?;
    let result = repo
        .review_pull(review_mutation, "pull-reviewer", pull, review())
        .await?;
    assert_eq!(
        repo.review_pull(review_mutation, "pull-reviewer", pull, review())
            .await?
            .receipt,
        result.receipt
    );
    assert_eq!(
        repo.review_pull(identity()?, "pull-reviewer", other, review())
            .await?
            .output,
        PullChange::Conflict
    );
    assert!(repo.pull_reviews("canopy", pull, 0).await?.output.unwrap()[0].applicable);
    let stale = PullRevision {
        source_version: 2,
        ..revision.clone()
    };
    assert_eq!(
        repo.review_pull(
            identity()?,
            "pull-reviewer",
            pull,
            NewReview {
                id: uuid::Uuid::new_v4().into_bytes(),
                revision: &stale,
                ..review()
            }
        )
        .await?
        .output,
        PullChange::Conflict
    );
    // Only authorized ACL changes can advance the reviewer's grant version.
    assert!(
        !repo
            .revoke_member(identity()?, "pull-author", "pull-reviewer")
            .await?
            .output
    );
    assert!(repo.pull_reviews("canopy", pull, 0).await?.output.unwrap()[0].applicable);
    repo.revoke_member(identity()?, "canopy", "pull-reviewer")
        .await?;
    assert_eq!(
        repo.review_pull(identity()?, "pull-reviewer", pull, review())
            .await?
            .output,
        PullChange::NotFound
    );
    repo.grant_member(identity()?, "canopy", "pull-reviewer", TokenScope::Write)
        .await?;
    repo.review_pull(identity()?, "pull-reviewer", pull, review())
        .await?;
    assert!(!repo.pull_reviews("canopy", pull, 0).await?.output.unwrap()[0].applicable);
    let owner_review = NewReview {
        id: uuid::Uuid::new_v4().into_bytes(),
        ..review()
    };
    assert!(matches!(
        repo.review_pull(identity()?, "canopy", pull, owner_review)
            .await?
            .output,
        PullChange::Applied(_)
    ));
    let edit = || PullEdit {
        expected_version: 1,
        title: "Edited",
        body: "Changed",
        state: PullState::Closed,
        draft: false,
    };
    assert_eq!(
        repo.edit_pull(identity()?, "outsider", pull, edit())
            .await?
            .output,
        PullChange::NotFound
    );
    assert_eq!(
        repo.edit_pull(identity()?, "pull-author", pull, edit())
            .await?
            .output,
        PullChange::Applied(pull)
    );
    assert_eq!(
        repo.edit_pull(identity()?, "pull-author", pull, edit())
            .await?
            .output,
        PullChange::Conflict
    );
    repo.create_pull(identity()?, "pull-author", new()).await?;
    assert_eq!(
        repo.pull("canopy", pull)
            .await?
            .output
            .unwrap()
            .summary
            .title,
        "Edited"
    );
    assert!(
        repo.pull_reviews("canopy", pull, 0)
            .await?
            .output
            .unwrap()
            .iter()
            .all(|r| !r.applicable)
    );
    repo.revoke_member(identity()?, "canopy", "pull-author")
        .await?;
    assert_eq!(
        repo.create_pull(identity()?, "pull-author", new())
            .await?
            .output,
        PullChange::NotFound
    );
    assert_eq!(
        repo.edit_pull(
            identity()?,
            "pull-author",
            pull,
            PullEdit {
                expected_version: 2,
                ..edit()
            }
        )
        .await?
        .output,
        PullChange::NotFound
    );
    Ok(())
}
