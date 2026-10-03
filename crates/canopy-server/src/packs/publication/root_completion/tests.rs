use super::*;
type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

pub(in crate::packs::publication) fn reseal_fixture(completion: &mut RootPushCompletion) -> Result {
    let mut data = completion.proof.certificate.data()?;
    data.completion_digest = Some(completion.outcomes.binding()?);
    completion.proof.certificate = CatalogCertificate::seal(&data, &[16; 32])?;
    Ok(())
}

pub(in crate::packs::publication) async fn response(
    root: NativeOutcomeRoot,
    store: &ArtifactStore,
) -> Result<GitHttpResponse> {
    let record: OutcomeRecord = root.0.read(store, INPUT_ROOT_BYTES).await?;
    let mut reader = store
        .read(
            native_result::body_key(record.body_operation, record.response.body),
            record.response.body,
        )
        .await?;
    let mut body = Vec::new();
    while let Some(part) = reader.next().await? {
        body.extend_from_slice(&part);
    }
    Ok(GitHttpResponse {
        status: record.response.status,
        headers: record.response.headers,
        body,
    })
}

pub(in crate::packs::publication) fn change_namespace(
    root: NativeOutcomeRoot,
) -> NativeOutcomeRoot {
    NativeOutcomeRoot(StoredInputRoot {
        operation: operation(1),
        artifact: root.artifact(),
    })
}
pub(in crate::packs::publication) async fn audit_native(
    root: NativeOutcomeRoot,
    store: &ArtifactStore,
) -> Result<NativeResultRoot> {
    let record: OutcomeRecord = root.0.read(store, INPUT_ROOT_BYTES).await?;
    let mut e = BoundedEncoder::new(128)?;
    root.encode(&mut e)?;
    let bytes = e.finish();
    let mut d = BoundedDecoder::new(&bytes, 128)?;
    assert!(NativeResultRoot::decode(&mut d)?.read(store).await.is_err());
    Ok(record.native)
}

fn operation(sequence: u64) -> [u8; 16] {
    let mut bytes = *b"CANOPY0100000000";
    bytes[8..].copy_from_slice(&sequence.to_be_bytes());
    bytes
}
fn root() -> NativeOutcomeRoot {
    NativeOutcomeRoot(StoredInputRoot {
        operation: operation(1),
        artifact: ArtifactDescriptor {
            size: 100,
            digest: [1; 32],
            manifest_digest: [2; 32],
        },
    })
}
#[test]
fn outcome_bundle_bounds_signed_key_and_all_three_roots_without_payload() -> Result {
    let mut value = RootPushOutcomes {
        response_id: *uuid::Uuid::new_v4().as_bytes(),
        ref_generation: 1,
        native: root(),
        rejected: root(),
        replayed: root(),
        signed: Some(RootSignedPushFact {
            digest: [3; 32],
            key: "k".repeat(4096),
            size: crate::push::MAX_RESPONSE_BYTES as u64,
        }),
    };
    let mut e = BoundedEncoder::new(ROOT_COMPLETION_BYTES)?;
    value.encode(&mut e)?;
    let bytes = e.finish();
    assert!(bytes.len() < 5120);
    let mut d = BoundedDecoder::new(&bytes, ROOT_COMPLETION_BYTES)?;
    assert_eq!(RootPushOutcomes::decode(&mut d)?, value);
    d.finish()?;
    value.signed.as_mut().unwrap().key.push('x');
    assert!(
        value
            .encode(&mut BoundedEncoder::new(ROOT_COMPLETION_BYTES)?)
            .is_err()
    );
    value.signed.as_mut().unwrap().key = "invalid\nkey".into();
    assert!(
        value
            .encode(&mut BoundedEncoder::new(ROOT_COMPLETION_BYTES)?)
            .is_err()
    );
    value.signed = None;
    value.ref_generation = 0;
    // The shared bundle represents ref-free outcomes too. Its enclosing
    // command must enforce the zero/nonzero distinction, not this codec.
    let mut e = BoundedEncoder::new(ROOT_COMPLETION_BYTES)?;
    value.encode(&mut e)?;
    let bytes = e.finish();
    let mut d = BoundedDecoder::new(&bytes, ROOT_COMPLETION_BYTES)?;
    assert_eq!(RootPushOutcomes::decode(&mut d)?, value);
    d.finish()?;
    value.ref_generation = i64::MAX as u64 + 1;
    assert!(
        value
            .encode(&mut BoundedEncoder::new(ROOT_COMPLETION_BYTES)?)
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn frozen_rejections_preserve_large_progress_and_native_failures_and_detect_late_corruption()
-> Result {
    use object_store::ObjectStoreExt;
    let provider = std::sync::Arc::new(object_store::memory::InMemory::new());
    let store = ArtifactStore::new(provider.clone(), *uuid::Uuid::new_v4().as_bytes());
    let operation = operation(2);
    let descriptor = root();
    let mut e = BoundedEncoder::new(128)?;
    descriptor.encode(&mut e)?;
    let bytes = e.finish();
    let native = NativeResultRoot::decode(&mut BoundedDecoder::new(&bytes, 128)?)?;
    assert_ne!(operation, native.operation());
    // This isolated report test intentionally has no request/native artifacts.
    // Actual witness creation is tested through the native receive composition.
    fn packet(body: &mut Vec<u8>, payload: &[u8]) {
        body.extend(format!("{:04x}", payload.len() + 4).as_bytes());
        body.extend(payload);
    }
    let mut report = Vec::new();
    packet(&mut report, b"unpack ok\n");
    packet(&mut report, b"ok refs/heads/main\n");
    packet(&mut report, b"ng refs/heads/failed native failure\n");
    report.extend(b"0000");
    let mut body = Vec::new();
    while body.len() <= canopy_object_storage::external::PART_BYTES {
        let mut progress = vec![b'p'; 60_000];
        progress[0] = 2;
        packet(&mut body, &progress);
    }
    packet(&mut body, &[&[1][..], report.as_slice()].concat());
    body.extend(b"0000");
    let response = GitHttpResponse {
        status: 200,
        headers: vec![
            ("Content-Length".into(), body.len().to_string()),
            ("X-Native-Test".into(), "preserved".into()),
        ],
        body,
    };
    let root = outcome::retain_rejection(&store, operation, native, &response, REPLAYED).await?;
    let record: OutcomeRecord = root.0.read(&store, INPUT_ROOT_BYTES).await?;
    assert_eq!(record.body_operation, operation);
    assert_eq!(record.native, native);
    let expected = crate::push::report::rejected_report(&response, REPLAYED)?;
    assert!(
        expected
            .body
            .windows(b"ng refs/heads/failed native failure".len())
            .any(|part| part == b"ng refs/heads/failed native failure")
    );
    assert_eq!(self::response(root, &store).await?, expected);
    let path = store.path(
        native_result::body_key(operation, record.response.body),
        record.response.body.digest,
    )?;
    provider
        .put(
            &canopy_object_storage::external::part(&path, 1),
            bytes::Bytes::from_static(b"corrupt late part").into(),
        )
        .await?;
    assert!(self::response(root, &store).await.is_err());
    Ok(())
}

#[tokio::test]
async fn frozen_no_report_rejection_is_explicit_http_failure() -> Result {
    let store = ArtifactStore::new(
        std::sync::Arc::new(object_store::memory::InMemory::new()),
        *uuid::Uuid::new_v4().as_bytes(),
    );
    let descriptor = root();
    let mut e = BoundedEncoder::new(128)?;
    descriptor.encode(&mut e)?;
    let bytes = e.finish();
    let native = NativeResultRoot::decode(&mut BoundedDecoder::new(&bytes, 128)?)?;
    for body in [vec![], b"0000".to_vec()] {
        let original = GitHttpResponse {
            status: 200,
            headers: vec![],
            body,
        };
        let root = outcome::retain_rejection(
            &store,
            operation(2),
            native,
            &original,
            crate::push::report::REJECTED,
        )
        .await?;
        let frozen = response(root, &store).await?;
        assert_eq!(frozen.status, 409);
        assert_eq!(
            frozen.body,
            format!("{}\n", crate::push::report::REJECTED).into_bytes()
        );
    }
    Ok(())
}
