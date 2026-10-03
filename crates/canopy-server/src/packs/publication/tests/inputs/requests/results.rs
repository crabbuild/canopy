//! Synthetic completions qualify retention/recovery; native CGI validity is
//! covered independently by the receive/publication/cold-clone composition.
use super::*;
use crate::git_http::GitHttpResponse;
use canopy_object_storage::artifact::{ArtifactKey, ArtifactKind};
use object_store::ObjectStoreExt;
mod signed;

pub(super) fn completion(
    actor: &str,
    format: ObjectFormat,
    count: u32,
    progress: bool,
) -> PushCompletionRequest {
    let oid = crate::ObjectId::try_from(&[23; 32][..format.bytes()]).unwrap();
    let plan = crate::PushPlan {
        actor: actor.into(),
        updates: (0..count)
            .map(|n| crate::RefUpdate {
                name: format!("refs/heads/{n:06}{}", "x".repeat(200)),
                expected: (n % 3 != 0).then_some(crate::RefExpectation {
                    oid: (n % 3 == 1).then_some(oid),
                    version: n as i64 + 1,
                }),
                new_oid: Some(oid),
            })
            .collect(),
    };
    let mut report = Vec::new();
    super::super::super::completion::packet(&mut report, b"unpack ok\n");
    for update in &plan.updates {
        super::super::super::completion::packet(
            &mut report,
            format!("ok {}\n", update.name).as_bytes(),
        );
    }
    report.extend(b"0000");
    let body = if progress {
        let mut body = Vec::new();
        while body.len() <= canopy_object_storage::external::PART_BYTES {
            super::super::super::completion::packet(
                &mut body,
                &[&[2][..], &vec![b'p'; 60_000]].concat(),
            );
        }
        for chunk in report.chunks(60_000) {
            super::super::super::completion::packet(&mut body, &[&[1][..], chunk].concat());
        }
        body.extend(b"0000");
        body
    } else {
        report
    };
    PushCompletionRequest {
        plan: Some(plan),
        response: GitHttpResponse {
            status: 200,
            headers: vec![
                (
                    "Content-Type".into(),
                    "application/x-git-receive-pack-result".into(),
                ),
                ("Content-Length".into(), body.len().to_string()),
                ("X-Native-Test".into(), "preserved".into()),
            ],
            body,
        },
        options: vec!["canopy.note=result".into()],
        certificate: None,
    }
}
pub(super) async fn retain(
    request: &Request,
    native: PushCompletionRequest,
) -> Result<NativeInputCertificate> {
    let store = request.store.clone();
    let prior = request.proof.clone();
    let directory = request.directory.path().to_owned();
    let disk = request.disk.clone();
    let repository = request.fixture.repository;
    let format = request.fixture.format;
    let proof = request
        .ticket
        .spawn(move |context| async move {
            let result = context
                .retain_native_result(&store, &prior, native, &directory, &disk)
                .await
                .map_err(|error| StagingError::Input(Box::new(error)))?;
            let token = context.token()?;
            let proof = context
                .append_native_result(
                    store.clone(),
                    &prior,
                    records(repository, token.artifact_operation, format, 1),
                    result,
                )
                .await
                .map_err(|error| StagingError::Input(Box::new(error)))?;
            // Artifacts and an issued MAC are insufficient until the exact
            // checkpoint is committed to this lease.
            assert!(
                context
                    .reopen_native_result(&store, &directory, &disk, None)
                    .await
                    .is_err()
            );
            Ok(proof)
        })?
        .wait()
        .await
        .map_err(|error| error.to_string())?;
    assert_eq!(request.disk.used(), 0);
    request
        .ticket
        .register_inputs(proof.clone(), identity()?)
        .map_err(|(error, _)| error)?
        .wait()
        .await
        .map_err(|error| error.to_string())?;
    Ok(proof)
}
#[tokio::test]
async fn native_result_checkpoint_preserves_large_framed_plan_response_and_final_inventory()
-> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let request = Request::new(format, true, false, [208; 16]).await?;
        let native = completion("owner", format, 20_000, true);
        let plan = native.plan.clone();
        let expected = native.response.clone();
        let options = native.options.clone();
        let mut inline = BoundedEncoder::new(4 << 20)?;
        assert!(plan.as_ref().unwrap().encode(&mut inline).is_err());
        let proof = retain(&request, native).await?;
        assert_eq!(proof.wire_request()?, request.proof.wire_request()?);
        assert!(proof.native_result()?.is_some());
        let mut e = BoundedEncoder::new(CERTIFICATE_BYTES)?;
        proof.encode(&mut e)?;
        assert!(e.finish().len() < 1024);
        let store = request.store.clone();
        let directory = request.directory.path().to_owned();
        let disk = request.disk.clone();
        let prior = proof.clone();
        let repository = request.fixture.repository;
        let recovered = request
            .ticket
            .spawn(move |context| async move {
                let token = context.token()?;
                assert!(
                    context
                        .append_native_inputs(
                            store.clone(),
                            &prior,
                            records(repository, token.artifact_operation, format, 2)
                        )
                        .await
                        .is_err()
                );
                context
                    .reopen_native_result(&store, &directory, &disk, None)
                    .await
                    .map_err(|error| StagingError::Input(Box::new(error)))
            })?
            .wait()
            .await
            .map_err(|error| error.to_string())?;
        assert_eq!(recovered.plan, plan);
        assert_eq!(recovered.response, expected);
        assert_eq!(recovered.options, options);
        assert!(recovered.certificate.is_none());
        drop(recovered);
        assert_eq!(request.disk.used(), 0);
        request.ticket.seal()?;
        assert!(matches!(
            timeout(Duration::from_secs(10), request.ticket.wait_terminal()).await?,
            StagingState::Bound(_)
        ));
        let recovered = request
            .ticket
            .bound_session()?
            .reopen_native_result(
                &request.store,
                request.directory.path(),
                &request.disk,
                None,
            )
            .await?;
        assert_eq!(recovered.plan, plan);
        assert_eq!(recovered.response, expected);
        drop(recovered);
        assert_eq!(request.disk.used(), 0);
        assert!(request.coordinator.close_and_drain().await.is_empty());
        request.fixture.runtime.shutdown().await?;
    }
    Ok(())
}
#[tokio::test]
async fn native_result_checkpoint_requires_registered_custody_and_rejects_corrupt_bodies() -> Result
{
    let request = Request::new(ObjectFormat::Sha256, false, false, [209; 16]).await?;
    let native = completion("owner", ObjectFormat::Sha256, 1, true);
    let digest = *blake3::hash(&native.response.body).as_bytes();
    let suffix = native.response.body[canopy_object_storage::external::PART_BYTES..].to_vec();
    let proof = retain(&request, native).await?;
    let key = ArtifactKey {
        operation: proof.token()?.artifact_operation,
        binding_digest: digest,
        kind: ArtifactKind::InputBody,
    };
    let path = request.store.path(key, digest)?;
    let part = canopy_object_storage::external::part(&path, 1);
    request
        .provider
        .put(&part, bytes::Bytes::from(vec![b'!'; suffix.len()]).into())
        .await?;
    let store = request.store.clone();
    let directory = request.directory.path().to_owned();
    let disk = request.disk.clone();
    request
        .ticket
        .spawn(move |context| async move {
            assert!(
                context
                    .reopen_native_result(&store, &directory, &disk, None)
                    .await
                    .is_err()
            );
            assert_eq!(disk.used(), 0);
            Ok(())
        })?
        .wait()
        .await
        .map_err(|error| error.to_string())?;
    request
        .provider
        .put(&part, bytes::Bytes::from(suffix).into())
        .await?;
    mutate(
        &request.fixture.handle,
        "UPDATE repository_identity SET owner='other' WHERE singleton=1".into(),
    )
    .await?;
    let store = request.store.clone();
    let directory = request.directory.path().to_owned();
    let disk = request.disk.clone();
    request
        .ticket
        .spawn(move |context| async move {
            assert!(
                context
                    .reopen_native_result(&store, &directory, &disk, None)
                    .await
                    .is_err()
            );
            assert_eq!(disk.used(), 0);
            Ok(())
        })?
        .wait()
        .await
        .map_err(|error| error.to_string())?;
    assert!(request.coordinator.close_and_drain().await.is_empty());
    request.fixture.runtime.shutdown().await?;
    Ok(())
}
