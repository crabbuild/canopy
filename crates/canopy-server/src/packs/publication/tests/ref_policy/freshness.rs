use super::*;

#[tokio::test]
async fn current_acl_and_new_direct_push_rules_are_checked_before_registration() -> Result {
    let (fixture, graph) = rooted(ObjectFormat::Sha256).await?;
    protect(&fixture, &graph).await?;
    let changes = plan(vec![update("refs/heads/main", None, Some(graph.tip))]);
    for (sql, reason) in [
        (
            "UPDATE repository_identity SET owner='another'",
            PreparationDenial::Unauthorized,
        ),
        (
            "UPDATE branch_rules SET require_pull_request=1,version=version+1",
            PreparationDenial::Conflict,
        ),
    ] {
        let attempt = fresh(&fixture, &graph).await?;
        let pending = attempt
            .prepared
            .ref_policy_preparation(
                changes.clone(),
                attempt.root.path(),
                attempt.budget.clone(),
                limits(),
            )
            .await?;
        let command = arm(
            &fixture,
            &attempt,
            pending.page(&attempt.prepared, 0).await?,
        )
        .await?;
        edit(&fixture, sql).await?;
        denied(&fixture, &command, reason).await?;
        assert!(pending.ready(&attempt.prepared).await.is_err());
        edit(&fixture, "UPDATE repository_identity SET owner='owner'").await?;
        finish_graph(attempt).await?;
    }
    // A freshly certified and registered page still observes the current
    // pull-request requirement; an old receipt is not fresh write authority.
    let attempt = fresh(&fixture, &graph).await?;
    let pending = attempt
        .prepared
        .ref_policy_preparation(
            changes,
            attempt.root.path(),
            attempt.budget.clone(),
            limits(),
        )
        .await?;
    let command = arm(
        &fixture,
        &attempt,
        pending.page(&attempt.prepared, 0).await?,
    )
    .await?;
    denied(&fixture, &command, PreparationDenial::Conflict).await?;
    finish_graph(attempt).await?;
    close(fixture, graph).await
}

#[tokio::test]
async fn watched_updates_and_rule_context_edits_invalidate_fresh_readiness() -> Result {
    let (fixture, graph) = rooted(ObjectFormat::Sha256).await?;
    protect(&fixture, &graph).await?;
    let changes = plan(vec![update("refs/heads/main", None, Some(graph.tip))]);
    let watched = format!(
        "oid=X'{}' AND context='ci' AND context_version=1",
        hex::encode(graph.tip)
    );
    // Even successful-to-successful updates invalidate their exact witness.
    // A new preparation observes and validates the replacement facts.
    for change in [
        "state='failure',version=version+1".to_string(),
        "reporter='another',version=version+1".into(),
        format!("oid=X'{}',version=version+1", hex::encode(graph.blob)),
        "context_version=2,version=version+1".into(),
        "state='success',version=version+1".into(),
    ] {
        let (pending, _, _) = registered(&fixture, &graph, changes.clone()).await?;
        edit(&fixture,&format!("UPDATE check_runs SET {change} WHERE number=(SELECT max(number) FROM check_runs WHERE {watched})")).await?;
        assert!(matches!(
            pending.ready(&graph.prepared).await,
            Err(RefPolicyPreparationError::Context)
        ));
        edit(&fixture, &run_sql(graph.tip, "ci", 1, "success")).await?;
    }
    for change in [
        "UPDATE branch_rules SET deny_deletions=1,version=version+1",
        "UPDATE branch_rules SET require_pull_request=1,version=version+1",
        "DELETE FROM branch_required_checks WHERE reference='refs/heads/main'",
        "UPDATE check_contexts SET reporter='another',version=version+1",
    ] {
        let attempt = fresh(&fixture, &graph).await?;
        let (pending, page, command) = registered(&fixture, &attempt, changes.clone()).await?;
        let original = Box::pin(command.clone().execute()).await?;
        let probe = arm(&fixture, &attempt, page).await?;
        edit(&fixture, change).await?;
        assert!(matches!(
            pending.ready(&attempt.prepared).await,
            Err(RefPolicyPreparationError::Context)
        ));
        let before = state(&fixture.handle).await?;
        let known = Box::pin(command.clone().execute()).await?;
        assert_eq!(
            (known.output, known.receipt),
            (original.output, original.receipt)
        );
        assert_eq!(state(&fixture.handle).await?, before);
        denied(&fixture, &probe, PreparationDenial::Conflict).await?;
        finish_graph(attempt).await?;
        // Restore configuration through actual mutations; do not reset epochs.
        edit(&fixture,"UPDATE branch_rules SET require_pull_request=0,version=version+1; INSERT OR IGNORE INTO branch_required_checks VALUES('refs/heads/main','ci'); UPDATE check_contexts SET reporter='owner',version=1").await?;
        edit(&fixture, &run_sql(graph.tip, "ci", 1, "success")).await?;
    }
    close(fixture, graph).await
}

#[tokio::test]
async fn integer_counters_watch_immutability_and_invalid_guard_resurrection_are_refused() -> Result
{
    let (fixture, graph) = rooted(ObjectFormat::Sha1).await?;
    protect(&fixture, &graph).await?;
    let (pending, _, _) = registered(
        &fixture,
        &graph,
        plan(vec![update("refs/heads/main", None, Some(graph.tip))]),
    )
    .await?;
    for sql in [
        "UPDATE ref_policy_guards SET next=next+0.5,total=total+0.5",
        "UPDATE ref_policy_guards SET policy_epoch=policy_epoch+0.5",
        "UPDATE ref_policy_watches SET context_version=context_version+0.5",
        "UPDATE ref_policy_watches SET run_number=run_number+0.5",
        "DELETE FROM ref_policy_watches",
        "UPDATE ref_policy_epoch SET version=version+2",
        "UPDATE ref_policy_budget SET watches=1.5",
        "INSERT OR REPLACE INTO ref_policy_guards SELECT * FROM ref_policy_guards",
        "INSERT OR REPLACE INTO ref_policy_watches SELECT * FROM ref_policy_watches",
        "INSERT OR REPLACE INTO ref_policy_epoch SELECT * FROM ref_policy_epoch",
        "INSERT OR REPLACE INTO ref_policy_budget SELECT * FROM ref_policy_budget",
    ] {
        let before = state(&fixture.handle).await?;
        assert!(edit(&fixture, sql).await.is_err(), "accepted {sql}");
        assert_eq!(state(&fixture.handle).await?, before, "{sql}");
    }
    pending.ready(&graph.prepared).await?;
    edit(&fixture, "UPDATE ref_policy_guards SET valid=0").await?;
    let before = state(&fixture.handle).await?;
    assert!(
        edit(&fixture, "UPDATE ref_policy_guards SET valid=1")
            .await
            .is_err()
    );
    assert_eq!(state(&fixture.handle).await?, before);
    assert!(pending.ready(&graph.prepared).await.is_err());
    close(fixture, graph).await
}
