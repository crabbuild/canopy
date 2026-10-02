use super::*;
use super::{
    prepare::cleaned,
    publishing::{Graph, assembled, edit, next_graph, plan, state, update},
};
use crate::{
    git_http::GitHttpResponse, packs::metadata::tests::limits, push::VerifiedPushCertificate,
};
use sha2::{Digest as _, Sha256};

pub(super) fn packet(out: &mut Vec<u8>, body: &[u8]) {
    assert!(body.len() + 4 <= 65520);
    out.extend_from_slice(format!("{:04x}", body.len() + 4).as_bytes());
    out.extend_from_slice(body);
}
fn response(plan: &crate::PushPlan, sideband: bool, progress: usize) -> GitHttpResponse {
    let mut report = Vec::new();
    packet(&mut report, b"unpack ok\n");
    for update in &plan.updates {
        packet(&mut report, format!("ok {}\n", update.name).as_bytes());
    }
    packet(&mut report, b"ng refs/heads/hook-refused hook declined\n");
    report.extend_from_slice(b"0000");
    let body = if sideband {
        let mut body = Vec::new();
        for _ in 0..progress {
            packet(&mut body, &[&[2u8][..], &vec![b'x'; 60_000]].concat());
        }
        for chunk in report.chunks(37) {
            packet(&mut body, &[&[1u8][..], chunk].concat());
        }
        body.extend_from_slice(b"0000");
        body
    } else {
        report
    };
    GitHttpResponse {
        status: 200,
        headers: vec![
            (
                "Content-Type".into(),
                "application/x-git-receive-pack-result".into(),
            ),
            ("X-Native-Trace".into(), "preserved".into()),
        ],
        body,
    }
}
async fn completion(
    graph: &Graph,
    request: PushCompletionRequest,
) -> Result<CatalogPushCompletion> {
    Ok(Box::pin(graph.prepared.push_completion(
        request,
        graph.root.path(),
        graph.budget.clone(),
        limits(),
    ))
    .await?)
}
fn request(
    graph: &Graph,
    name: &str,
    sideband: bool,
    progress: usize,
    signed: bool,
) -> PushCompletionRequest {
    let plan = plan(vec![update(name, None, Some(graph.initial))]);
    PushCompletionRequest {
        response: response(&plan, sideband, progress),
        plan: Some(plan),
        options: vec!["canopy.note=reviewed".into()],
        certificate: signed.then(|| VerifiedPushCertificate {
            target: graph.prepared.base.capability().1.clone(),
            request_digest: graph.prepared.token().request_digest,
            body: vec![b's'; 600_000],
            signer: "owner".into(),
            key: "native-verified-key".into(),
        }),
    }
}
fn completed(reply: CatalogCompletionReply) -> Result<CompletedCatalogPush> {
    match reply {
        CatalogCompletionReply::Completed(value) => Ok(value),
        CatalogCompletionReply::Denied(reason) => Err(format!("denied {reason:?}").into()),
    }
}
async fn stored(handle: &CellHandle, id: [u8; 16]) -> Result<GitHttpResponse> {
    let bytes = handle
        .query(0, 2 << 20, move |connection| {
            let (status, headers, size, digest): (u16, String, usize, Vec<u8>) = connection
                .query_row(
                    "SELECT status,headers,size,digest FROM push_responses WHERE id=?1",
                    [id.as_slice()],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                )?;
            let mut statement = connection.prepare(
                "SELECT part,body FROM push_response_chunks WHERE response_id=?1 ORDER BY part",
            )?;
            let mut body = Vec::new();
            for (at, row) in statement
                .query_map([id.as_slice()], |row| {
                    Ok((row.get::<_, usize>(0)?, row.get::<_, Vec<u8>>(1)?))
                })?
                .enumerate()
            {
                let (part, chunk) = row?;
                if part != at {
                    return Err(Error::Command("stored response chunk gap"));
                }
                body.extend_from_slice(&chunk);
            }
            if body.len() != size || blake3::hash(&body).as_bytes().as_slice() != digest {
                return Err(Error::Command("stored response digest"));
            }
            let mut encoder = BoundedEncoder::new(2 << 20)?;
            encoder.write_u32(u32::from(status))?;
            encoder.write_text(&headers)?;
            encoder.write_bytes(&body)?;
            Ok(encoder.finish())
        })
        .await?;
    let mut decoder = BoundedDecoder::new(&bytes, 2 << 20)?;
    let status = u16::try_from(decoder.read_u32()?)?;
    let headers = serde_json::from_str(decoder.read_text()?)?;
    let body = decoder.read_bytes()?.to_vec();
    decoder.finish()?;
    Ok(GitHttpResponse {
        status,
        headers,
        body,
    })
}
pub(super) async fn counts(handle: &CellHandle) -> Result<Vec<u64>> {
    let bytes = handle
        .query(0, 1024, |connection| {
            let counts = [
                "pushes",
                "push_responses",
                "push_response_chunks",
                "push_certificates",
                "push_certificate_chunks",
            ]
            .into_iter()
            .map(|table| {
                connection.query_row(&format!("SELECT count(*) FROM {table}"), [], |row| {
                    row.get::<_, u64>(0)
                })
            })
            .collect::<rusqlite::Result<Vec<_>>>()?;
            serde_json::to_vec(&counts).map_err(|_| Error::Command("outcome counts"))
        })
        .await?;
    Ok(serde_json::from_slice(&bytes)?)
}
async fn reject(
    fixture: &Fixture,
    input: CatalogPushCompletion,
    reason: PreparationDenial,
) -> Result {
    let before = state(&fixture.handle).await?;
    let outcomes = counts(&fixture.handle).await?;
    let result = Box::pin(fixture.client().command::<CompleteCatalogPush>(
        &fixture.target,
        identity()?,
        input,
    ))
    .await;
    assert!(
        matches!(result,Err(InvocationError::Rejected(ref value)) if value.output==CatalogCompletionReply::Denied(reason)),
        "{result:?}"
    );
    assert_eq!(state(&fixture.handle).await?, before);
    assert_eq!(counts(&fixture.handle).await?, outcomes);
    Ok(())
}

#[tokio::test]
async fn exact_native_response_options_and_signed_bytes_commit_with_catalog_refs() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let fixture = Fixture::new(format).await?;
        let graph = assembled(&fixture, [91; 16], 0).await?;
        let before = state(&fixture.handle).await?;
        let mut native = request(&graph, "refs/heads/開発", true, 10, true);
        // Maximum legal note count with JSON escaping exceeds the old 32 KiB
        // table constraint. Reuse options with a correctly bounded 64 KiB field.
        native.options = vec![format!("canopy.note={}", "\\".repeat(1012)); 16];
        let expected = native.response.clone();
        let options = native.options.clone();
        let input = completion(&graph, native).await?;
        assert_eq!(state(&fixture.handle).await?, before);
        assert_eq!(counts(&fixture.handle).await?, vec![0; 5]);
        let mut encoder = BoundedEncoder::new(4 << 20)?;
        input.encode(&mut encoder)?;
        let bytes = encoder.finish();
        let mut decoder = BoundedDecoder::new(&bytes, 4 << 20)?;
        assert_eq!(CatalogPushCompletion::decode(&mut decoder)?, input);
        decoder.finish()?;
        let mutation = identity()?;
        let first = Box::pin(fixture.client().command::<CompleteCatalogPush>(
            &fixture.target,
            mutation,
            input.clone(),
        ))
        .await?;
        let output = completed(first.output)?;
        assert!(!output.rejected);
        assert_eq!(
            output
                .publication
                .map(|value| (value.generation, value.ref_generation)),
            Some((1, 1))
        );
        assert_eq!(stored(&fixture.handle, output.response_id).await?, expected);
        assert_eq!(
            graph.prepared.completed_push_response(&first).await?,
            expected
        );
        assert_eq!(counts(&fixture.handle).await?, vec![1, 1, 2, 1, 2]);
        let operation = graph.prepared.token().operation;
        fixture.handle.query(0,1<<20,move|connection| {
            let (saved_options,digest,size):(String,Vec<u8>,usize)=connection.query_row("SELECT p.options,c.digest,c.size FROM pushes p JOIN push_certificates c ON c.push_id=p.id WHERE p.id=?1",[operation.as_slice()],|row|Ok((row.get(0)?,row.get(1)?,row.get(2)?)))?;
            assert_eq!(serde_json::from_str::<Vec<String>>(&saved_options).unwrap(),options);
            let mut statement=connection.prepare("SELECT body FROM push_certificate_chunks WHERE push_id=?1 ORDER BY part")?;
            let body=statement.query_map([operation.as_slice()],|row|row.get::<_,Vec<u8>>(0))?.collect::<rusqlite::Result<Vec<_>>>()?.concat();
            assert_eq!(body,vec![b's';600_000]);
            assert_eq!(size,body.len());
            assert_eq!(digest,Sha256::digest(&body).as_slice());
            Ok(Vec::new())
        }).await?;
        let after = state(&fixture.handle).await?;
        let replay = Box::pin(fixture.client().command::<CompleteCatalogPush>(
            &fixture.target,
            mutation,
            input.clone(),
        ))
        .await?;
        assert_eq!(replay.output, first.output);
        assert_eq!(replay.receipt, first.receipt);
        let logical = Box::pin(fixture.client().command::<CompleteCatalogPush>(
            &fixture.target,
            identity()?,
            input,
        ))
        .await?;
        assert_eq!(logical.output, first.output);
        assert_eq!(state(&fixture.handle).await?, after);
        assert_eq!(stored(&fixture.handle, output.response_id).await?, expected);
        drop(graph.prepared);
        cleaned(graph.root.path(), &graph.budget).await?;
        fixture.runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn completion_payload_tampering_and_stripping_cannot_publish() -> Result {
    let fixture = Fixture::new(ObjectFormat::Sha1).await?;
    let graph = assembled(&fixture, [92; 16], 0).await?;
    let input = completion(&graph, request(&graph, "refs/heads/main", false, 0, true)).await?;
    for at in 0..8 {
        let mut bad = input.clone();
        match at {
            0 => bad.response_id = [99; 16],
            1 => bad.response.status = 201,
            2 => bad.response.headers.push(("X-Added".into(), "x".into())),
            3 => bad.response.body.push(b'x'),
            4 => bad.options = vec!["canopy.note=altered".into()],
            5 => bad.signed = None,
            6 => bad.signed.as_mut().unwrap().body[0] ^= 1,
            _ => bad.signed.as_mut().unwrap().key.push('x'),
        }
        reject(&fixture, bad, PreparationDenial::Unauthorized).await?;
    }
    let CompletionCatalogProof::Refs(proof) = input.proof else {
        panic!("refs")
    };
    // A caller cannot strip the network payload and commit only its refs.
    let result = fixture
        .client()
        .command::<PublishCatalogRefs>(&fixture.target, identity()?, proof)
        .await;
    assert!(
        matches!(result,Err(InvocationError::Rejected(ref value)) if value.output==PublicationReply::Denied(PreparationDenial::Unauthorized))
    );
    assert_eq!(counts(&fixture.handle).await?, vec![0; 5]);
    let native = request(&graph, "refs/heads/main", false, 0, false);
    for body in [b"0000".to_vec(), b"0014unpack ok\n0000".to_vec()] {
        // A bare flush is permitted when report-status was declined; a malformed
        // report never becomes an acknowledged publication.
        if body == b"0000" {
            continue;
        }
        assert!(
            completion(
                &graph,
                PushCompletionRequest {
                    response: GitHttpResponse {
                        body,
                        ..native.response.clone()
                    },
                    plan: native.plan.clone(),
                    options: vec![],
                    certificate: None
                }
            )
            .await
            .is_err()
        );
    }
    let mut extra = native.response.clone();
    let mut report = Vec::new();
    packet(&mut report, b"unpack ok\n");
    packet(&mut report, b"ok refs/heads/other\n");
    report.extend_from_slice(b"0000");
    extra.body = report;
    assert!(
        completion(
            &graph,
            PushCompletionRequest {
                response: extra,
                plan: native.plan,
                options: vec![],
                certificate: None
            }
        )
        .await
        .is_err()
    );
    drop(graph.prepared);
    cleaned(graph.root.path(), &graph.budget).await?;
    fixture.runtime.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn final_policy_and_permission_refusals_save_exact_rejections_without_publishing() -> Result {
    for (revoke, report_status) in [(false, true), (true, true), (false, false), (true, false)] {
        let fixture = Fixture::new(ObjectFormat::Sha256).await?;
        let graph = assembled(&fixture, [93; 16], 0).await?;
        let mut native = request(&graph, "refs/heads/main", true, 1, false);
        if !report_status {
            native.response.body.clear();
        }
        let input = completion(&graph, native).await?;
        let expected =
            crate::push::report::rejected_report(&input.response, crate::push::report::REJECTED)?;
        if revoke {
            edit(
                &fixture,
                "UPDATE repository_identity SET owner='successor';",
            )
            .await?;
        } else {
            edit(
                &fixture,
                "INSERT INTO branch_rules(reference,version,enabled,deny_deletions,fast_forward,require_pull_request,required_approvals) VALUES('refs/heads/main',1,1,0,1,1,0);",
            )
            .await?;
        }
        let mutation = identity()?;
        let first = fixture
            .client()
            .command::<CompleteCatalogPush>(&fixture.target, mutation, input.clone())
            .await?;
        let output = completed(first.output)?;
        assert!(output.rejected);
        assert_eq!(output.publication, None);
        assert_eq!(
            graph.prepared.completed_push_response(&first).await?,
            expected
        );
        assert_eq!(stored(&fixture.handle, output.response_id).await?, expected);
        fixture
            .handle
            .query(0, 1024, |connection| {
                assert_eq!(
                    connection
                        .query_row("SELECT generation FROM catalog_state", [], |row| row
                            .get::<_, u64>(0))?,
                    0
                );
                assert_eq!(
                    connection
                        .query_row("SELECT count(*) FROM refs", [], |row| row.get::<_, u64>(0))?,
                    0
                );
                Ok(Vec::new())
            })
            .await?;
        assert_eq!(fixture.counts().await?, (0, 1));
        let logical = fixture
            .client()
            .command::<CompleteCatalogPush>(&fixture.target, identity()?, input)
            .await?;
        assert_eq!(logical.output, first.output);
        drop(graph.prepared);
        cleaned(graph.root.path(), &graph.budget).await?;
        fixture.runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn late_response_failure_rolls_back_catalog_refs_certificate_and_all_chunks() -> Result {
    let fixture = Fixture::new(ObjectFormat::Sha1).await?;
    let graph = assembled(&fixture, [94; 16], 0).await?;
    let input = completion(&graph, request(&graph, "refs/heads/main", true, 10, true)).await?;
    edit(&fixture,"CREATE TRIGGER fail_last_response BEFORE INSERT ON push_response_chunks WHEN NEW.part=1 BEGIN SELECT RAISE(ABORT,'late response chunk failure'); END;").await?;
    let before = state(&fixture.handle).await?;
    let failed = Box::pin(fixture.client().command::<CompleteCatalogPush>(
        &fixture.target,
        identity()?,
        input.clone(),
    ))
    .await;
    assert!(failed.is_err(), "{failed:?}");
    assert_eq!(state(&fixture.handle).await?, before);
    assert_eq!(counts(&fixture.handle).await?, vec![0; 5]);
    assert_eq!(fixture.counts().await?, (1, 1));
    edit(&fixture, "DROP TRIGGER fail_last_response;").await?;
    let committed = Box::pin(fixture.client().command::<CompleteCatalogPush>(
        &fixture.target,
        identity()?,
        input.clone(),
    ))
    .await?;
    let output = completed(committed.output)?;
    assert!(output.publication.is_some());
    assert_eq!(
        stored(&fixture.handle, output.response_id).await?,
        input.response
    );
    drop(graph.prepared);
    cleaned(graph.root.path(), &graph.budget).await?;
    fixture.runtime.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn signed_certificate_replay_is_a_durable_refusal_and_outcome_bytes_are_immutable() -> Result
{
    let fixture = Fixture::new(ObjectFormat::Sha1).await?;
    let graph = assembled(&fixture, [95; 16], 0).await?;
    let input = completion(&graph, request(&graph, "refs/heads/main", false, 0, true)).await?;
    let first = completed(
        fixture
            .client()
            .command::<CompleteCatalogPush>(&fixture.target, identity()?, input)
            .await?
            .output,
    )?;
    let next = next_graph(&fixture, &graph, [96; 16]).await?;
    let input = completion(&next, request(&next, "refs/heads/second", false, 0, true)).await?;
    let expected = crate::push::report::rejected_report(
        &input.response,
        "Canopy signed push certificate was already used",
    )?;
    let refusal = completed(
        fixture
            .client()
            .command::<CompleteCatalogPush>(&fixture.target, identity()?, input)
            .await?
            .output,
    )?;
    assert!(refusal.rejected);
    assert_eq!(refusal.publication, None);
    assert_eq!(
        stored(&fixture.handle, refusal.response_id).await?,
        expected
    );
    assert_eq!(counts(&fixture.handle).await?, vec![2, 2, 2, 1, 2]);
    for sql in [
        "UPDATE pushes SET options='[]' WHERE rejected=0;",
        "UPDATE pushes SET completion_digest=zeroblob(32);",
        "UPDATE pushes SET rejected=1 WHERE rejected=0;",
        "UPDATE push_responses SET digest=zeroblob(32);",
        "UPDATE push_response_chunks SET body=x'00';",
        "UPDATE push_certificate_chunks SET body=x'00';",
        "UPDATE push_certificates SET key='changed';",
        "INSERT OR REPLACE INTO push_responses SELECT * FROM push_responses;",
        "INSERT OR REPLACE INTO push_response_chunks SELECT * FROM push_response_chunks;",
        "INSERT OR REPLACE INTO push_certificates SELECT * FROM push_certificates;",
        "INSERT OR REPLACE INTO push_certificate_chunks SELECT * FROM push_certificate_chunks;",
    ] {
        assert!(edit(&fixture, sql).await.is_err(), "{sql}");
    }
    assert_eq!(
        stored(&fixture.handle, refusal.response_id).await?,
        expected
    );
    assert!(
        !stored(&fixture.handle, first.response_id)
            .await?
            .body
            .is_empty()
    );
    drop(graph.prepared);
    cleaned(graph.root.path(), &graph.budget).await?;
    drop(next.prepared);
    cleaned(next.root.path(), &next.budget).await?;
    fixture.runtime.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn empty_commands_and_native_failures_complete_without_a_catalog_generation() -> Result {
    let fixture = Fixture::new(ObjectFormat::Sha256).await?;
    let graph = assembled(&fixture, [97; 16], 0).await?;
    let mut failed = Vec::new();
    packet(&mut failed, b"unpack invalid pack\n");
    packet(&mut failed, b"ng refs/heads/main unpacker error\n");
    failed.extend_from_slice(b"0000");
    for (at, response) in [
        GitHttpResponse {
            status: 200,
            headers: vec![],
            body: b"0000".to_vec(),
        },
        GitHttpResponse {
            status: 200,
            headers: vec![],
            body: failed,
        },
        GitHttpResponse {
            status: 413,
            headers: vec![("Content-Type".into(), "text/plain".into())],
            body: b"native input too large\n".to_vec(),
        },
    ]
    .into_iter()
    .enumerate()
    {
        let owner = if at == 0 {
            None
        } else {
            Some(next_graph(&fixture, &graph, [at as u8 + 100; 16]).await?)
        };
        let graph = owner.as_ref().unwrap_or(&graph);
        let input = completion(
            graph,
            PushCompletionRequest {
                plan: None,
                response: response.clone(),
                options: vec![],
                certificate: None,
            },
        )
        .await?;
        let result = completed(
            fixture
                .client()
                .command::<CompleteCatalogPush>(&fixture.target, identity()?, input)
                .await?
                .output,
        )?;
        assert_eq!(result.publication, None);
        assert!(!result.rejected);
        assert_eq!(stored(&fixture.handle, result.response_id).await?, response);
        if let Some(graph) = owner {
            drop(graph.prepared);
            cleaned(graph.root.path(), &graph.budget).await?;
        }
    }
    fixture
        .handle
        .query(0, 1024, |connection| {
            assert_eq!(
                connection.query_row("SELECT generation FROM catalog_state", [], |row| row
                    .get::<_, u64>(0))?,
                0
            );
            Ok(Vec::new())
        })
        .await?;
    drop(graph.prepared);
    cleaned(graph.root.path(), &graph.budget).await?;
    fixture.runtime.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn moving_catalog_remains_retryable_and_owner_restore_preserves_exact_completion() -> Result {
    let fixture = Fixture::new(ObjectFormat::Sha1).await?;
    let graph = assembled(&fixture, [104; 16], 0).await?;
    let pending = assembled(&fixture, [105; 16], 0).await?;
    let input = completion(&graph, request(&graph, "refs/heads/main", true, 1, false)).await?;
    let stale = completion(
        &pending,
        request(&pending, "refs/heads/pending", false, 0, false),
    )
    .await?;
    let mutation = identity()?;
    let first = fixture
        .client()
        .command::<CompleteCatalogPush>(&fixture.target, mutation, input.clone())
        .await?;
    let output = completed(first.output)?;
    assert!(matches!(
        pending.prepared.completed_push_response(&first).await,
        Err(CatalogPushResponseError::Invalid)
    ));
    reject(&fixture, stale.clone(), PreparationDenial::Conflict).await?;
    let before = state(&fixture.handle).await?;
    let outcomes = counts(&fixture.handle).await?;
    let response = stored(&fixture.handle, output.response_id).await?;
    fixture.handle.drain().await?;
    fixture.runtime.shutdown().await?;
    let session = SessionId::from_bytes([106; 16]);
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
            fixture.root.path().join("completion-b.sqlite"),
            Owner {
                session,
                endpoint: "https://completion-b.invalid".into(),
            },
        )
        .await?;
    let client = CellClient::local(Arc::clone(&fixture.registry), handle.clone());
    let replay = client
        .command::<CompleteCatalogPush>(&fixture.target, mutation, input.clone())
        .await?;
    assert_eq!(replay.output, first.output);
    assert_eq!(replay.receipt, first.receipt);
    let logical = client
        .command::<CompleteCatalogPush>(&fixture.target, identity()?, input)
        .await?;
    assert_eq!(logical.output, first.output);
    assert_eq!(stored(&handle, output.response_id).await?, response);
    assert_eq!(
        replay_push_response(
            &client,
            &fixture.target,
            fixture.begin(graph.prepared.token().operation),
            None
        )
        .await?,
        Some(response)
    );
    let failed = client
        .command::<CompleteCatalogPush>(&fixture.target, identity()?, stale)
        .await;
    assert!(
        matches!(failed,Err(InvocationError::Rejected(ref value)) if value.output==CatalogCompletionReply::Denied(PreparationDenial::Stale))
    );
    assert_eq!(state(&handle).await?, before);
    assert_eq!(counts(&handle).await?, outcomes);
    drop(graph.prepared);
    cleaned(graph.root.path(), &graph.budget).await?;
    drop(pending.prepared);
    cleaned(pending.root.path(), &pending.budget).await?;
    runtime.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn service_completion_checks_native_witness_scope_and_supports_checked_noop_refs() -> Result {
    let fixture = Fixture::new(ObjectFormat::Sha1).await?;
    let graph = assembled(&fixture, [107; 16], 0).await?;
    let before = state(&fixture.handle).await?;
    for at in 0..3 {
        let mut native = request(&graph, "refs/heads/main", false, 0, true);
        let certificate = native.certificate.as_mut().unwrap();
        match at {
            0 => certificate.request_digest[0] ^= 1,
            1 => {
                certificate.target = crate::repository_target(
                    fixture.target.tenant(),
                    ApplicationId::from_bytes([99; 16]),
                    fixture.repository,
                )?
            }
            _ => certificate.signer = "another-account".into(),
        }
        assert!(completion(&graph, native).await.is_err());
        assert_eq!(state(&fixture.handle).await?, before);
    }
    let first = Box::pin(graph.prepared.complete_push(
        identity()?,
        request(&graph, "refs/heads/main", false, 0, true),
        graph.root.path(),
        graph.budget.clone(),
        limits(),
    ))
    .await?;
    assert!(!completed(first.output)?.rejected);
    let next = next_graph(&fixture, &graph, [108; 16]).await?;
    let same = plan(vec![update(
        "refs/heads/main",
        Some((graph.initial, 1)),
        Some(graph.initial),
    )]);
    let native = PushCompletionRequest {
        response: response(&same, false, 0),
        plan: Some(same),
        options: vec![],
        certificate: None,
    };
    let second = Box::pin(next.prepared.complete_push(
        identity()?,
        native,
        next.root.path(),
        next.budget.clone(),
        limits(),
    ))
    .await?;
    assert_eq!(
        completed(second.output)?
            .publication
            .map(|value| (value.generation, value.ref_generation)),
        Some((2, 2))
    );
    assert!(
        next.prepared
            .completed_push_response(&second)
            .await?
            .body
            .windows(18)
            .any(|bytes| bytes == b"ok refs/heads/main")
    );
    // A reused certificate accompanying an already failed native unpack records
    // its refusal without attempting to turn the failed report into success.
    let failure = next_graph(&fixture, &graph, [109; 16]).await?;
    let mut native = request(&failure, "refs/heads/unused", false, 0, true);
    native.plan = None;
    let mut body = Vec::new();
    packet(&mut body, b"unpack invalid pack\n");
    packet(&mut body, b"ng refs/heads/unused unpacker error\n");
    body.extend_from_slice(b"0000");
    native.response.body = body.clone();
    let failed = Box::pin(failure.prepared.complete_push(
        identity()?,
        native,
        failure.root.path(),
        failure.budget.clone(),
        limits(),
    ))
    .await?;
    assert!(completed(failed.output)?.rejected);
    assert_eq!(
        failure
            .prepared
            .completed_push_response(&failed)
            .await?
            .body,
        body
    );
    for graph in [graph, next, failure] {
        drop(graph.prepared);
        cleaned(graph.root.path(), &graph.budget).await?;
    }
    fixture.runtime.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn completed_preflight_prevents_repreparation_and_preserves_the_logical_identity() -> Result {
    let fixture = Fixture::new(ObjectFormat::Sha1).await?;
    let operation = [110; 16];
    let begin = fixture.begin(operation);
    assert_eq!(
        fixture
            .client()
            .query::<CheckCompletedPush>(&fixture.target, None, begin.clone())
            .await?
            .output,
        None
    );
    let graph = assembled(&fixture, operation, 0).await?;
    assert_eq!(
        fixture
            .client()
            .query::<CheckCompletedPush>(&fixture.target, None, begin.clone())
            .await?
            .output,
        None
    );
    let first = Box::pin(graph.prepared.complete_push(
        identity()?,
        request(&graph, "refs/heads/main", false, 0, false),
        graph.root.path(),
        graph.budget.clone(),
        limits(),
    ))
    .await?;
    let before = state(&fixture.handle).await?;
    let found = fixture
        .client()
        .query::<CheckCompletedPush>(&fixture.target, Some(first.receipt), begin.clone())
        .await?;
    assert_eq!(found.output, Some(first.output));
    let response = graph.prepared.completed_push_response(&first).await?;
    assert_eq!(
        replay_push_response(
            &fixture.client(),
            &fixture.target,
            begin.clone(),
            Some(first.receipt)
        )
        .await?,
        Some(response)
    );
    let result = fixture
        .client()
        .command::<BeginPreparation>(&fixture.target, identity()?, begin.clone())
        .await;
    assert!(
        matches!(result,Err(InvocationError::Rejected(ref value)) if value.output==PreparationReply::Denied(PreparationDenial::Conflict))
    );
    assert_eq!(state(&fixture.handle).await?, before);
    assert_eq!(fixture.counts().await?, (0, 1));
    let mut wrong = begin.clone();
    wrong.request_digest[0] ^= 1;
    assert_eq!(
        fixture
            .client()
            .query::<CheckCompletedPush>(&fixture.target, None, wrong)
            .await?
            .output,
        Some(CatalogCompletionReply::Denied(PreparationDenial::Conflict))
    );
    let mut outsider = begin;
    outsider.actor = "outsider".into();
    assert_eq!(
        fixture
            .client()
            .query::<CheckCompletedPush>(&fixture.target, None, outsider)
            .await?
            .output,
        Some(CatalogCompletionReply::Denied(
            PreparationDenial::Unauthorized
        ))
    );
    assert_eq!(state(&fixture.handle).await?, before);
    drop(graph.prepared);
    cleaned(graph.root.path(), &graph.budget).await?;
    fixture.runtime.shutdown().await?;
    Ok(())
}

mod outcome;
