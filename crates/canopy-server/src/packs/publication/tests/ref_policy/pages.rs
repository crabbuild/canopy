use super::*;

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
        .ref_policy_preparation(
            changes.clone(),
            graph.root.path(),
            graph.budget.clone(),
            limits(),
        )
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
    // Each terminal denial has its own registered attempt. A negative page
    // cannot be followed by positive pages on the same publication pin.
    for case in 0..3 {
        let attempt = fresh(&fixture, &graph).await?;
        let intent = attempt
            .prepared
            .ref_policy_preparation(
                changes.clone(),
                attempt.root.path(),
                attempt.budget.clone(),
                limits(),
            )
            .await?;
        if case != 0 {
            let prefix = arm(&fixture, &attempt, intent.page(&attempt.prepared, 0).await?).await?;
            assert!(matches!(Box::pin(prefix.execute()).await?.output,
                RefPolicyReply::Registered(progress) if progress.next == 3 && progress.valid));
        }
        let mut candidate = intent
            .page(&attempt.prepared, if case == 1 { 2 } else { 3 })
            .await?;
        let reason = match case {
            0 => PreparationDenial::Missing,
            1 => PreparationDenial::Conflict,
            2 => {
                candidate.offset = 4;
                PreparationDenial::Unauthorized
            }
            _ => unreachable!(),
        };
        let command = arm(&fixture, &attempt, candidate).await?;
        denied(&fixture, &command, reason).await?;
        assert!(
            arm(&fixture, &attempt, intent.page(&attempt.prepared, 0).await?)
                .await
                .is_err()
        );
        finish_graph(attempt).await?;
    }
    let command = arm(&fixture, &graph, first.clone()).await?;
    let receipt = Box::pin(command.clone().execute()).await?;
    assert_eq!(
        receipt.output,
        RefPolicyReply::Registered(RefPolicyProgress {
            next: 3,
            total: 134,
            valid: true,
        })
    );
    let same = Box::pin(command.clone().execute()).await?;
    assert_eq!(
        (receipt.receipt, &receipt.output),
        (same.receipt, &same.output)
    );
    assert!(pending.ready(&graph.prepared).await.is_err());
    for page in [second, third] {
        let next = arm(&fixture, &graph, page).await?;
        assert!(matches!(Box::pin(next.execute()).await?.output,
            RefPolicyReply::Registered(progress) if progress.valid));
    }
    let exact = Box::pin(command.clone().execute()).await?;
    assert_eq!(
        (&exact.output, exact.receipt),
        (&receipt.output, receipt.receipt)
    );
    let logical = arm(&fixture, &graph, first).await?;
    let before = state(&fixture.handle).await?;
    let replay = Box::pin(logical.execute()).await?;
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
    let command = arm(&fixture, &graph, page).await?;
    edit(&fixture,"CREATE TRIGGER fail_guard_cursor BEFORE UPDATE OF next ON ref_policy_guards BEGIN SELECT RAISE(ABORT,'late guard cursor fault'); END").await?;
    let before = state(&fixture.handle).await?;
    let phase = super::super::mandatory_registration::registration_state(&fixture).await?;
    let error = Box::pin(command.clone().execute()).await.unwrap_err();
    assert!(
        format!("{error:?}").contains("late guard cursor fault"),
        "{error:?}"
    );
    assert!(matches!(
        fixture.client().resolve(command.evidence()).await?,
        Resolution::Absent
    ));
    assert_eq!(state(&fixture.handle).await?, before);
    assert_eq!(
        super::super::mandatory_registration::registration_state(&fixture).await?,
        phase
    );
    edit(&fixture, "DROP TRIGGER fail_guard_cursor").await?;
    let result = Box::pin(command.clone().execute()).await?;
    assert!(matches!(result.output,RefPolicyReply::Registered(progress) if progress.ready()));
    let before = state(&fixture.handle).await?;
    let replay = Box::pin(command.clone().execute()).await?;
    assert_eq!(
        (&replay.output, replay.receipt),
        (&result.output, result.receipt)
    );
    assert_eq!(state(&fixture.handle).await?, before);
    close(fixture, graph).await
}

#[tokio::test]
async fn watch_and_guard_capacity_refusals_precede_every_write() -> Result {
    let (fixture, graph) = rooted(ObjectFormat::Sha256).await?;
    protect(&fixture, &graph).await?;
    for watches in [true, false] {
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
        let page = pending.page(&attempt.prepared, 0).await?;
        let command = arm(&fixture, &attempt, page.clone()).await?;
        if watches {
            edit(
                &fixture,
                &format!("UPDATE ref_policy_budget SET watches={MAX_REF_POLICY_WATCHES}"),
            )
            .await?;
        } else {
            let token = page.proof.certificate.data()?.token;
            let mut e = BoundedEncoder::new(256)?;
            token.encode(&mut e)?;
            edit(&fixture,&format!("WITH RECURSIVE seq(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM seq WHERE x<{MAX_REF_POLICY_GUARDS}) INSERT INTO ref_policy_guards SELECT CAST(printf('%016d',x) AS BLOB),zeroblob(32),X'{}',0,1,0,0 FROM seq",hex::encode(e.finish()))).await?;
        }
        denied(&fixture, &command, PreparationDenial::Capacity).await?;
        if watches {
            edit(&fixture, "UPDATE ref_policy_budget SET watches=0").await?;
        } else {
            edit(&fixture, "DELETE FROM ref_policy_guards").await?;
        }
        finish_graph(attempt).await?;
    }
    let (pending, _, _) = registered(
        &fixture,
        &graph,
        plan(vec![update("refs/heads/main", None, Some(graph.tip))]),
    )
    .await?;
    pending.ready(&graph.prepared).await?;
    close(fixture, graph).await
}
