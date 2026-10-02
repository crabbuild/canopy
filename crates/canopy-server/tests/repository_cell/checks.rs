use super::*;
use canopy_server::checks::{CheckChange, CheckContextEdit, CheckEdit, CheckState, NewCheck};

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;
fn identity() -> Result<MutationIdentity> {
    let now = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis())?;
    Ok(MutationIdentity {
        request_id: RequestId::from_bytes(uuid::Uuid::new_v4().into_bytes()),
        issued_at_ms: now,
        expires_at_ms: now + 60_000,
    })
}

pub async fn exercise(repository: &RepositoryCell, oid: canopy_server::ObjectId) -> Result {
    let policy = || CheckContextEdit {
        expected_version: 0,
        reporter: "ci-agent",
        enabled: true,
    };
    assert_eq!(
        repository
            .set_check_context(identity()?, "outsider", "unit", policy())
            .await?
            .output,
        CheckChange::Forbidden
    );
    assert_eq!(
        repository
            .set_check_context(identity()?, "canopy", "unit", policy())
            .await?
            .output,
        CheckChange::NotFound
    );
    repository
        .grant_member(identity()?, "canopy", "ci-agent", TokenScope::Read)
        .await?;
    let config_identity = identity()?;
    let configured = repository
        .set_check_context(config_identity, "canopy", "unit", policy())
        .await?;
    assert_eq!(configured.output, CheckChange::Applied);
    assert_eq!(
        repository
            .set_check_context(config_identity, "canopy", "unit", policy())
            .await?
            .receipt,
        configured.receipt
    );
    let id = uuid::Uuid::new_v4().into_bytes();
    let new_check = || NewCheck {
        id,
        oid,
        context: "unit",
        context_version: 1,
    };
    assert_eq!(
        repository
            .start_check(identity()?, "canopy", new_check())
            .await?
            .output,
        CheckChange::Forbidden
    );
    let creation = identity()?;
    let created = repository
        .start_check(creation, "ci-agent", new_check())
        .await?;
    assert_eq!(created.output, CheckChange::Applied);
    assert_eq!(
        repository
            .start_check(creation, "ci-agent", new_check())
            .await?
            .receipt,
        created.receipt
    );
    assert!(
        repository
            .check_contexts("outsider", None)
            .await?
            .output
            .is_none()
    );
    assert!(repository.check_run("outsider", id).await?.output.is_none());
    assert!(
        repository
            .commit_checks("outsider", oid, None)
            .await?
            .output
            .is_none()
    );
    let summary = "\n".repeat(4096);
    let edit = || CheckEdit {
        expected_version: 1,
        state: CheckState::Success,
        summary: &summary,
    };
    assert_eq!(
        repository
            .update_check(identity()?, "canopy", id, edit())
            .await?
            .output,
        CheckChange::Forbidden
    );
    repository
        .revoke_member(identity()?, "canopy", "ci-agent")
        .await?;
    assert_eq!(
        repository
            .start_check(identity()?, "ci-agent", new_check())
            .await?
            .output,
        CheckChange::NotFound
    );
    assert_eq!(
        repository
            .update_check(identity()?, "ci-agent", id, edit())
            .await?
            .output,
        CheckChange::NotFound
    );
    repository
        .grant_member(identity()?, "canopy", "ci-agent", TokenScope::Read)
        .await?;
    let completion = identity()?;
    let completed = repository
        .update_check(completion, "ci-agent", id, edit())
        .await?;
    assert_eq!(completed.output, CheckChange::Applied);
    assert_eq!(
        repository
            .update_check(completion, "ci-agent", id, edit())
            .await?
            .receipt,
        completed.receipt
    );
    assert_eq!(
        repository
            .update_check(
                identity()?,
                "ci-agent",
                id,
                CheckEdit {
                    expected_version: 2,
                    state: CheckState::Failure,
                    summary: "Must not overwrite"
                }
            )
            .await?
            .output,
        CheckChange::Conflict
    );
    assert_eq!(
        repository
            .check_run("canopy", id)
            .await?
            .output
            .ok_or("missing check")?
            .summary,
        summary
    );
    repository
        .set_check_context(
            identity()?,
            "canopy",
            "unit",
            CheckContextEdit {
                expected_version: 1,
                reporter: "ci-agent",
                enabled: false,
            },
        )
        .await?;
    assert_eq!(
        repository
            .commit_checks("canopy", oid, None)
            .await?
            .output
            .ok_or("missing checks")?
            .len(),
        0
    );
    assert_eq!(
        repository
            .start_check(identity()?, "ci-agent", new_check())
            .await?
            .output,
        CheckChange::Conflict
    );
    repository
        .revoke_member(identity()?, "canopy", "ci-agent")
        .await?;
    Ok(())
}
