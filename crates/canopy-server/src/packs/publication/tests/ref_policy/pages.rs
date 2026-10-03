use super::*;

async fn denied(fixture: &Fixture, page: RefPolicyPage, reason: PreparationDenial) -> Result {
    let before = state(&fixture.handle).await?;
    let result = fixture
        .client()
        .command::<RegisterRefPolicyPage>(&fixture.target, identity()?, page)
        .await;
    assert!(
        matches!(result,Err(InvocationError::Rejected(ref value))
        if value.output == RefPolicyReply::Denied(reason)),
        "{result:?}"
    );
    assert_eq!(state(&fixture.handle).await?, before);
    Ok(())
}

#[tokio::test]
async fn pages_bound_bytes_and_updates_remap_bits_and_replay_only_contiguous_intent() -> Result {
    let (fixture, graph) = rooted(ObjectFormat::Sha256).await?;
    let changes = plan(
        (0..134)
            .map(|i| {
                let name = if i < 5 {
                    format!("refs/tags/{}-{i}", "x".repeat(65_520))
                } else {
                    format!("refs/tags/tag-{i:04}")
                };
                update(
                    &name,
                    (i % 2 == 1).then_some((graph.blob, 1)),
                    Some(graph.tip),
                )
            })
            .collect(),
    );
    let pending = graph
        .prepared
        .ref_policy_preparation(changes, graph.root.path(), graph.budget.clone(), limits())
        .await?;
    let first = pending.page(&graph.prepared, 0).await?;
    assert_eq!(first.proof.plan.updates.len(), 3);
    let second = pending.page(&graph.prepared, 3).await?;
    assert_eq!(second.proof.plan.updates.len(), 128);
    let third = pending.page(&graph.prepared, 131).await?;
    assert_eq!(third.proof.plan.updates.len(), 3);
    for page in [&first, &second, &third] {
        let mut e = BoundedEncoder::new(REF_POLICY_PAGE_BYTES)?;
        page.encode(&mut e)?;
        let bytes = e.finish();
        let mut d = BoundedDecoder::new(&bytes, REF_POLICY_PAGE_BYTES)?;
        assert_eq!(RefPolicyPage::decode(&mut d)?, *page);
        d.finish()?;
        for i in 0..page.proof.plan.updates.len() {
            assert_eq!(
                super::super::super::ref_proof::proven(&page.proof.ancestry, i),
                (page.offset as usize + i).is_multiple_of(2)
            );
        }
    }
    denied(&fixture, second.clone(), PreparationDenial::Missing).await?;
    let mutation = identity()?;
    let receipt = fixture
        .client()
        .command::<RegisterRefPolicyPage>(&fixture.target, mutation, first.clone())
        .await?;
    assert_eq!(
        receipt.output,
        RefPolicyReply::Registered(RefPolicyProgress {
            next: 3,
            total: 134,
            valid: true,
        })
    );
    let same = fixture
        .client()
        .command::<RegisterRefPolicyPage>(&fixture.target, mutation, first.clone())
        .await?;
    assert_eq!(
        (receipt.receipt, receipt.output),
        (same.receipt, same.output)
    );
    assert!(pending.ready(&graph.prepared).await.is_err());
    denied(
        &fixture,
        pending.page(&graph.prepared, 2).await?,
        PreparationDenial::Conflict,
    )
    .await?;
    let mut altered = second.clone();
    altered.offset = 4;
    denied(&fixture, altered, PreparationDenial::Unauthorized).await?;
    for page in [second, third] {
        fixture
            .client()
            .command::<RegisterRefPolicyPage>(&fixture.target, identity()?, page)
            .await?;
    }
    let before = state(&fixture.handle).await?;
    let replay = fixture
        .client()
        .command::<RegisterRefPolicyPage>(&fixture.target, identity()?, first)
        .await?;
    assert!(matches!(replay.output,RefPolicyReply::Registered(progress) if progress.ready()));
    assert_eq!(state(&fixture.handle).await?, before);
    pending.ready(&graph.prepared).await?;
    close(fixture, graph).await
}

#[tokio::test]
async fn late_cursor_failure_rolls_back_guard_watches_budget_and_then_exact_retry_succeeds()
-> Result {
    let (fixture, graph) = rooted(ObjectFormat::Sha1).await?;
    protect(&fixture, &graph).await?;
    let pending = graph
        .prepared
        .ref_policy_preparation(
            plan(vec![update("refs/heads/main", None, Some(graph.tip))]),
            graph.root.path(),
            graph.budget.clone(),
            limits(),
        )
        .await?;
    let page = pending.page(&graph.prepared, 0).await?;
    edit(&fixture,"CREATE TRIGGER fail_guard_cursor BEFORE UPDATE OF next ON ref_policy_guards BEGIN SELECT RAISE(ABORT,'late guard cursor fault'); END").await?;
    let before = state(&fixture.handle).await?;
    let mutation = identity()?;
    assert!(
        fixture
            .client()
            .command::<RegisterRefPolicyPage>(&fixture.target, mutation, page.clone())
            .await
            .is_err()
    );
    assert_eq!(state(&fixture.handle).await?, before);
    edit(&fixture, "DROP TRIGGER fail_guard_cursor").await?;
    let result = fixture
        .client()
        .command::<RegisterRefPolicyPage>(&fixture.target, mutation, page.clone())
        .await?;
    assert!(matches!(result.output,RefPolicyReply::Registered(progress) if progress.ready()));
    let before = state(&fixture.handle).await?;
    fixture
        .client()
        .command::<RegisterRefPolicyPage>(&fixture.target, identity()?, page)
        .await?;
    assert_eq!(state(&fixture.handle).await?, before);
    close(fixture, graph).await
}

#[tokio::test]
async fn watch_and_guard_capacity_refusals_precede_every_write() -> Result {
    let (fixture, graph) = rooted(ObjectFormat::Sha256).await?;
    protect(&fixture, &graph).await?;
    let pending = graph
        .prepared
        .ref_policy_preparation(
            plan(vec![update("refs/heads/main", None, Some(graph.tip))]),
            graph.root.path(),
            graph.budget.clone(),
            limits(),
        )
        .await?;
    let page = pending.page(&graph.prepared, 0).await?;
    edit(
        &fixture,
        &format!("UPDATE ref_policy_budget SET watches={MAX_REF_POLICY_WATCHES}"),
    )
    .await?;
    denied(&fixture, page.clone(), PreparationDenial::Capacity).await?;
    edit(&fixture, "UPDATE ref_policy_budget SET watches=0").await?;
    let token = page.proof.certificate.data()?.token;
    let mut e = BoundedEncoder::new(256)?;
    token.encode(&mut e)?;
    edit(&fixture,&format!("WITH RECURSIVE seq(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM seq WHERE x<{MAX_REF_POLICY_GUARDS}) INSERT INTO ref_policy_guards SELECT CAST(printf('%016d',x) AS BLOB),zeroblob(32),X'{}',0,1,0,0 FROM seq",hex::encode(e.finish()))).await?;
    denied(&fixture, page.clone(), PreparationDenial::Capacity).await?;
    edit(&fixture, "DELETE FROM ref_policy_guards").await?;
    fixture
        .client()
        .command::<RegisterRefPolicyPage>(&fixture.target, identity()?, page)
        .await?;
    pending.ready(&graph.prepared).await?;
    close(fixture, graph).await
}
