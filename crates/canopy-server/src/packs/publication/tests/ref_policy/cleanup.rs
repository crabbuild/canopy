use super::*;

fn request(fixture: &Fixture, id: [u8; 16]) -> RefPolicyReap {
    RefPolicyReap {
        maintenance: MaintenanceRequest {
            repository: fixture.repository,
            actor: "owner".into(),
            owner: fixture.handle.owner_fence(),
        },
        id,
    }
}

#[tokio::test]
async fn cleanup_is_bounded_transactional_and_retains_live_invalid_tombstones() -> Result {
    let (fixture, graph) = rooted(ObjectFormat::Sha256).await?;
    protect(&fixture, &graph).await?;
    let (pending, page, command) = registered(
        &fixture,
        &graph,
        plan(vec![update("refs/heads/main", None, Some(graph.tip))]),
    )
    .await?;
    let id = pending.intent().id;
    let original = Box::pin(command.clone().execute()).await?;
    let conflict = arm(&fixture, &graph, page.clone()).await?;
    let plans=fixture.handle.query(0,8192,move|connection| {
        let statements=[
            "EXPLAIN QUERY PLAN SELECT guard FROM ref_policy_watches WHERE oid=?1 AND context=?2 AND context_version=?3 AND run_number<=?4",
            "EXPLAIN QUERY PLAN SELECT oid,context,context_version,run_number FROM ref_policy_watches WHERE guard=?1 ORDER BY oid,context,context_version,run_number LIMIT 512",
        ];
        let mut result=String::new();
        for sql in statements {
            let mut q=connection.prepare(sql)?;
            let count=q.parameter_count();
            let mut rows=q.query(rusqlite::params_from_iter(std::iter::repeat_n(rusqlite::types::Value::Null,count)))?;
            while let Some(row)=rows.next()? {
                result.push_str(&row.get::<_,String>(3)?);
                result.push('\n');
            }
        }
        Ok(result.into_bytes())
    }).await?;
    let plans = String::from_utf8(plans)?;
    assert!(plans.contains("ref_policy_watches_by_check"), "{plans}");
    assert!(plans.contains("USING PRIMARY KEY (guard=?)"), "{plans}");
    assert!(!plans.contains("SCAN ref_policy_watches"), "{plans}");
    let input = request(&fixture, id);
    let before = state(&fixture.handle).await?;
    assert!(matches!(fixture.client().command::<ReapRefPolicyGuard>(
        &fixture.target,identity()?,input.clone()).await,
        Err(InvocationError::Rejected(value)) if value.output==RefPolicyReapReply::Denied(PreparationDenial::Conflict)));
    assert_eq!(state(&fixture.handle).await?, before);
    // Trusted fixture injection exercises the indexed 512-row boundary without
    // pretending that 600 synthetic contexts prove large-team capacity.
    edit(&fixture,&format!("WITH RECURSIVE seq(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM seq WHERE x<599) INSERT INTO ref_policy_watches SELECT X'{}',X'{}',printf('context-%04d',x),1,2 FROM seq; UPDATE ref_policy_budget SET watches=600; UPDATE ref_policy_guards SET valid=0",hex::encode(id),hex::encode(graph.tip))).await?;
    edit(&fixture,"CREATE TRIGGER fail_watch_budget BEFORE UPDATE ON ref_policy_budget BEGIN SELECT RAISE(ABORT,'late watch budget fault'); END").await?;
    let before = state(&fixture.handle).await?;
    let mutation = identity()?;
    assert!(
        fixture
            .client()
            .command::<ReapRefPolicyGuard>(&fixture.target, mutation, input.clone())
            .await
            .is_err()
    );
    assert_eq!(state(&fixture.handle).await?, before);
    edit(&fixture, "DROP TRIGGER fail_watch_budget").await?;
    assert_eq!(
        fixture
            .client()
            .command::<ReapRefPolicyGuard>(&fixture.target, mutation, input.clone())
            .await?
            .output,
        RefPolicyReapReply::Reaped {
            watches: 512,
            removed: false
        }
    );
    assert_eq!(
        fixture
            .client()
            .command::<ReapRefPolicyGuard>(&fixture.target, identity()?, input.clone())
            .await?
            .output,
        RefPolicyReapReply::Reaped {
            watches: 88,
            removed: false
        }
    );
    let before = state(&fixture.handle).await?;
    let known = Box::pin(command.clone().execute()).await?;
    assert_eq!(
        (known.output, known.receipt),
        (original.output, original.receipt)
    );
    assert_eq!(state(&fixture.handle).await?, before);
    denied(&fixture, &conflict, PreparationDenial::Conflict).await?;
    fixture
        .client()
        .command::<AbortPreparation>(&fixture.target, identity()?, check(graph.prepared.token()))
        .await?;
    assert_eq!(
        fixture
            .client()
            .command::<ReapRefPolicyGuard>(&fixture.target, identity()?, input.clone())
            .await?
            .output,
        RefPolicyReapReply::Reaped {
            watches: 0,
            removed: true
        }
    );
    let competing = fixture
        .client()
        .prepare_command::<RegisterRefPolicyPage>(&fixture.target, identity()?, page)
        .await?;
    super::super::mandatory_registration::not_started(&fixture, &competing).await?;
    // An independently registered original command also reaches the actual
    // missing-operation denial after abort, rather than failing at the gate.
    let attempt = fresh(&fixture, &graph).await?;
    let pending = attempt
        .prepared
        .ref_policy_preparation(
            plan(vec![update("refs/heads/main", None, Some(attempt.tip))]),
            attempt.root.path(),
            attempt.budget.clone(),
            limits(),
        )
        .await?;
    let missing = arm(
        &fixture,
        &attempt,
        pending.page(&attempt.prepared, 0).await?,
    )
    .await?;
    fixture
        .client()
        .command::<AbortPreparation>(
            &fixture.target,
            identity()?,
            check(attempt.prepared.token()),
        )
        .await?;
    denied(&fixture, &missing, PreparationDenial::Missing).await?;
    finish_graph(attempt).await?;
    close(fixture, graph).await
}

#[tokio::test]
async fn restored_owner_cannot_replay_old_guard_into_new_writes_and_can_reap_its_watches() -> Result
{
    let (fixture, graph) = rooted(ObjectFormat::Sha1).await?;
    protect(&fixture, &graph).await?;
    let (pending, page, command) = registered(
        &fixture,
        &graph,
        plan(vec![update("refs/heads/main", None, Some(graph.tip))]),
    )
    .await?;
    let mutation = command.evidence().identity();
    let original = Box::pin(command.clone().execute()).await?;
    graph.ticket.stop();
    assert!(graph.staging.close_and_drain().await.is_empty());
    fixture.handle.drain().await?;
    fixture.runtime.shutdown().await?;
    let session = SessionId::from_bytes([232; 16]);
    let runtime = CellRuntime::new(SqlWorkerPool::new(1, 4)?, 64 << 20, session)?;
    let authority = CellAuthority::new(fixture.layout.clone());
    let idle = authority
        .load(fixture.target.cell_id())
        .await?
        .ok_or("idle")?;
    let provision = CellCatalog::new(fixture.layout.clone(), fixture.target.tenant())
        .lookup(fixture.target.cell_id())
        .await?
        .ok_or("provision")?;
    let handle = runtime
        .acquire_idle_restored(
            provision,
            fixture.replica.clone(),
            authority,
            idle,
            fixture.root.path().join("ref-policy-restored.sqlite"),
            Owner {
                session,
                endpoint: "https://ref-policy-restored.invalid".into(),
            },
        )
        .await?;
    assert!(handle.owner_fence().epoch > graph.prepared.token().owner.epoch);
    let client = CellClient::local(Arc::clone(&fixture.registry), handle.clone());
    let exact = client
        .command::<RegisterRefPolicyPage>(&fixture.target, mutation, page.clone())
        .await?;
    assert_eq!(
        (exact.output, exact.receipt),
        (original.output, original.receipt)
    );
    let before = state(&handle).await?;
    let competing = client
        .prepare_command::<RegisterRefPolicyPage>(&fixture.target, identity()?, page)
        .await?;
    assert!(matches!(
        Box::pin(competing.clone().execute()).await,
        Err(InvocationError::NotStarted(_))
    ));
    assert!(matches!(
        client.resolve(competing.evidence()).await?,
        Resolution::Absent
    ));
    assert_eq!(state(&handle).await?, before);
    assert_eq!(
        client
            .command::<ReapRefPolicyGuard>(
                &fixture.target,
                identity()?,
                RefPolicyReap {
                    maintenance: MaintenanceRequest {
                        owner: handle.owner_fence(),
                        ..request(&fixture, pending.intent().id).maintenance
                    },
                    id: pending.intent().id,
                }
            )
            .await?
            .output,
        RefPolicyReapReply::Reaped {
            watches: 1,
            removed: true
        }
    );
    finish_graph(graph).await?;
    runtime.shutdown().await?;
    Ok(())
}
