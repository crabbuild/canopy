use super::*;
use canopy_server::issues::{
    CommentEdit, IssueChange, IssueEdit, IssueState, NewComment, NewIssue,
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

pub async fn exercise(repository: &RepositoryCell) -> Result {
    let id = uuid::Uuid::new_v4().into_bytes();
    let title = "界".repeat(85);
    let body = "\n".repeat(canopy_server::issues::ISSUE_BODY_LIMIT);
    let new_issue = || NewIssue {
        id,
        title: &title,
        body: &body,
    };
    assert_eq!(
        repository
            .create_issue(identity()?, "outsider", new_issue())
            .await?
            .output,
        IssueChange::NotFound
    );
    assert!(
        repository
            .issues("outsider", 0, None)
            .await?
            .output
            .is_none()
    );
    repository
        .grant_member(identity()?, "canopy", "reporter", TokenScope::Read)
        .await?;
    let creation = identity()?;
    let created = repository
        .create_issue(creation, "reporter", new_issue())
        .await?;
    assert_eq!(created.output, IssueChange::Applied(1));
    let replayed = repository
        .create_issue(creation, "reporter", new_issue())
        .await?;
    assert_eq!(created.receipt, replayed.receipt);
    assert_eq!(
        repository
            .issue("reporter", 1)
            .await?
            .output
            .ok_or("missing issue")?
            .body,
        body
    );
    assert!(repository.issue("outsider", 1).await?.output.is_none());
    assert!(
        repository
            .issue_comments("outsider", 1, 0)
            .await?
            .output
            .is_none()
    );
    assert!(
        repository
            .issue_comments("canopy", 2, 0)
            .await?
            .output
            .is_none()
    );
    let new_comment = || NewComment {
        id: uuid::Uuid::new_v4().into_bytes(),
        body: "comment",
    };
    assert_eq!(
        repository
            .create_issue_comment(identity()?, "reporter", 1, new_comment())
            .await?
            .output,
        IssueChange::Applied(1)
    );
    let edit = || IssueEdit {
        expected_version: 1,
        title: "Closed",
        body: "Finished",
        state: IssueState::Closed,
    };
    let comment_edit = || CommentEdit {
        expected_version: 1,
        body: "Edited",
    };
    repository
        .grant_member(identity()?, "canopy", "discussion-reader", TokenScope::Read)
        .await?;
    assert_eq!(
        repository
            .edit_issue(identity()?, "discussion-reader", 1, edit())
            .await?
            .output,
        IssueChange::Forbidden
    );
    assert_eq!(
        repository
            .edit_issue_comment(identity()?, "discussion-reader", 1, 1, comment_edit())
            .await?
            .output,
        IssueChange::Forbidden
    );
    repository
        .grant_member(
            identity()?,
            "canopy",
            "discussion-reader",
            TokenScope::Write,
        )
        .await?;
    let update = identity()?;
    let closed = repository
        .edit_issue(update, "discussion-reader", 1, edit())
        .await?;
    assert_eq!(closed.output, IssueChange::Applied(1));
    assert_eq!(
        repository
            .edit_issue(update, "discussion-reader", 1, edit())
            .await?
            .receipt,
        closed.receipt
    );
    assert_eq!(
        repository
            .edit_issue(identity()?, "discussion-reader", 1, edit())
            .await?
            .output,
        IssueChange::Conflict
    );
    assert_eq!(
        repository
            .edit_issue_comment(identity()?, "discussion-reader", 1, 1, comment_edit())
            .await?
            .output,
        IssueChange::Applied(1)
    );
    repository
        .revoke_member(identity()?, "canopy", "reporter")
        .await?;
    assert_eq!(
        repository
            .create_issue(identity()?, "reporter", new_issue())
            .await?
            .output,
        IssueChange::NotFound
    );
    assert_eq!(
        repository
            .create_issue_comment(identity()?, "reporter", 1, new_comment())
            .await?
            .output,
        IssueChange::NotFound
    );
    assert_eq!(
        repository
            .edit_issue(identity()?, "reporter", 1, edit())
            .await?
            .output,
        IssueChange::NotFound
    );
    assert_eq!(
        repository
            .edit_issue_comment(identity()?, "reporter", 1, 1, comment_edit())
            .await?
            .output,
        IssueChange::NotFound
    );
    repository
        .revoke_member(identity()?, "canopy", "discussion-reader")
        .await?;
    assert_eq!(
        repository
            .issues("canopy", 0, Some(IssueState::Closed))
            .await?
            .output
            .ok_or("missing page")?
            .len(),
        1
    );
    assert_eq!(
        repository
            .issue("canopy", 1)
            .await?
            .output
            .ok_or("missing issue")?
            .summary
            .version,
        2
    );
    Ok(())
}
