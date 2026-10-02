use super::*;
use crate::{git_gateway::preflight::EncodedPush, git_http::GitHttpRequest, git_input::GitInput};
use cellule_ltx::DiskBudget;
use std::io::Write;

struct Request {
    fixture: Fixture,
    coordinator: StagingCoordinator,
    ticket: StagingTicket,
    store: Arc<ArtifactStore>,
    provider: Arc<InMemory>,
    directory: tempfile::TempDir,
    disk: DiskBudget,
    raw: Vec<u8>,
    encoded_size: u64,
    proof: NativeInputCertificate,
}
fn raw_request(format: ObjectFormat, large: bool) -> Vec<u8> {
    let mut bytes = Vec::new();
    super::super::completion::packet(&mut bytes, b"push-cert\0report-status push-options\n");
    for line in [
        "certificate version 0.1\n".into(),
        "push-option canopy.note=retained\n".into(),
        "\n".into(),
        format!(
            "{} {} refs/heads/main\n",
            "00".repeat(format.bytes()),
            "12".repeat(format.bytes())
        ),
        "-----BEGIN SSH SIGNATURE-----\n".into(),
        "unverified-request-fixture\n".into(),
        "-----END SSH SIGNATURE-----\n".into(),
        "push-cert-end\n".into(),
    ] {
        super::super::completion::packet(&mut bytes, line.as_bytes());
    }
    bytes.extend(b"0000");
    super::super::completion::packet(&mut bytes, b"canopy.note=retained");
    bytes.extend(b"0000PACKunvalidated-request-fixture");
    if large {
        bytes.resize(canopy_object_storage::external::PART_BYTES + 1024, b'x');
    }
    bytes
}
impl Request {
    async fn new(
        format: ObjectFormat,
        gzip: bool,
        large: bool,
        operation: [u8; 16],
    ) -> Result<Self> {
        let fixture = Fixture::new(format).await?;
        let directory = tempfile::TempDir::new()?;
        let disk = DiskBudget::new(32 << 20);
        let raw = raw_request(format, large);
        let wire = if gzip {
            let mut encoder =
                flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
            encoder.write_all(&raw)?;
            encoder.finish()?
        } else {
            raw.clone()
        };
        let encoded_size = wire.len() as u64;
        let encoded = EncodedPush::new(
            GitHttpRequest {
                method: "POST".into(),
                path_info: "/repo.git/git-receive-pack".into(),
                query: String::new(),
                content_type: Some("application/x-git-receive-pack-request".into()),
                gzip,
                protocol_v2: false,
                authenticated: true,
                body: GitInput::receive(
                    axum::body::Body::from(wire),
                    directory.path(),
                    &disk,
                    None,
                    None,
                )
                .await?,
            },
            &fixture.target,
            fixture.repository,
            format,
            "owner",
            operation,
        )
        .await?;
        let coordinator =
            StagingCoordinator::new(fixture.target.clone(), StagingLimits::default())?;
        let ticket = coordinator
            .submit(
                ReadyStaging::new(
                    fixture.client(),
                    fixture.target.clone(),
                    encoded.identity().clone(),
                    identity()?,
                )
                .await?,
            )
            .map_err(|(error, _)| error)?;
        assert!(matches!(
            timeout(Duration::from_secs(10), ticket.wait()).await?,
            StagingState::Active(_)
        ));
        let provider = Arc::new(InMemory::new());
        let store = Arc::new(ArtifactStore::new(provider.clone(), fixture.repository));
        let upload = store.clone();
        let missing_root = directory.path().to_owned();
        let missing_disk = disk.clone();
        let worker = ticket.spawn(move |context| async move {
            assert!(
                context
                    .reopen_push_request(&upload, &missing_root, &missing_disk, None, None)
                    .await
                    .is_err()
            );
            let (_encoded, saved) = encoded
                .retain(&context, &upload)
                .await
                .map_err(|error| StagingError::Input(Box::new(error)))?;
            context
                .seal_push_inputs(upload, std::iter::empty(), saved)
                .await
                .map_err(|error| StagingError::Input(Box::new(error)))
        })?;
        let proof = worker.wait().await.map_err(|error| error.to_string())?;
        assert!(proof.root()?.is_none());
        assert!(proof.wire_request()?.is_some());
        assert_eq!(disk.used(), 0);
        ticket
            .register_inputs(proof.clone(), identity()?)
            .map_err(|(error, _)| error)?
            .wait()
            .await
            .map_err(|error| error.to_string())?;
        Ok(Self {
            fixture,
            coordinator,
            ticket,
            store,
            provider,
            directory,
            disk,
            raw,
            encoded_size,
            proof,
        })
    }
    async fn verify(&self) -> Result {
        let store = self.store.clone();
        let root = self.directory.path().to_owned();
        let disk = self.disk.clone();
        let expected = self.raw.clone();
        let expected_digest = self.proof.token()?.request_digest;
        let size = self.encoded_size;
        self.ticket
            .spawn(move |context| async move {
                assert!(
                    context
                        .reopen_push_request(&store, &root, &disk, Some(size - 1), None)
                        .await
                        .is_err()
                );
                let encoded = context
                    .reopen_push_request(&store, &root, &disk, Some(size), None)
                    .await
                    .map_err(|error| StagingError::Input(Box::new(error)))?;
                assert_eq!(encoded.identity().request_digest, expected_digest);
                let preflight = encoded
                    .decode(&root, &disk, None)
                    .await
                    .map_err(|error| StagingError::Input(Box::new(error)))?;
                assert_eq!(preflight.identity().request_digest, expected_digest);
                let request = preflight.into_native_request();
                assert!(!request.gzip);
                assert_eq!(
                    request
                        .body
                        .prefix(expected.len())
                        .await
                        .map_err(|error| StagingError::Input(Box::new(error)))?,
                    expected
                );
                Ok(())
            })?
            .wait()
            .await
            .map_err(|error| error.to_string())?;
        assert_eq!(self.disk.used(), 0);
        Ok(())
    }
}

#[tokio::test]
async fn request_checkpoint_recovers_large_plain_and_gzip_intent_and_appends_native_inventory()
-> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        for gzip in [false, true] {
            let request = Request::new(format, gzip, true, [201; 16]).await?;
            request.verify().await?;
            let wire = request.proof.wire_request()?.unwrap();
            assert!(wire.artifact().size < 1024);
            let record = wire.read(&request.store).await?;
            assert_eq!(record.request.body.size, request.encoded_size);
            let store = request.store.clone();
            let prior = request.proof.clone();
            let repository = request.fixture.repository;
            let next = request
                .ticket
                .spawn(move |context| async move {
                    let token = context.token()?;
                    context
                        .append_native_inputs(
                            store,
                            &prior,
                            records(repository, token.artifact_operation, format, 2),
                        )
                        .await
                        .map_err(|error| StagingError::Input(Box::new(error)))
                })?
                .wait()
                .await
                .map_err(|error| error.to_string())?;
            assert_eq!(next.wire_request()?, Some(wire));
            assert_eq!(next.root()?.unwrap().record_count, 2);
            let mutation = identity()?;
            request
                .ticket
                .register_inputs(next.clone(), mutation)
                .map_err(|(error, _)| error)?
                .wait()
                .await
                .map_err(|error| error.to_string())?;
            let replay = request
                .fixture
                .client()
                .command::<RegisterStagedInputs>(&request.fixture.target, mutation, next.clone())
                .await?;
            assert!(matches!(replay.output, StagingReply::Granted(_)));
            denied(
                request
                    .fixture
                    .client()
                    .command::<RegisterStagedInputs>(
                        &request.fixture.target,
                        identity()?,
                        request.proof.clone(),
                    )
                    .await,
                PreparationDenial::Conflict,
            );
            let store = request.store.clone();
            let prior = next.clone();
            let same = request
                .ticket
                .spawn(move |context| async move {
                    let token = context.token()?;
                    context
                        .append_native_inputs(
                            store,
                            &prior,
                            records(repository, token.artifact_operation, format, 2),
                        )
                        .await
                        .map_err(|error| StagingError::Input(Box::new(error)))
                })?
                .wait()
                .await
                .map_err(|error| error.to_string())?;
            assert_eq!(same, next);
            let token = next.token()?;
            let revision=request.fixture.handle.query(0,32,move|connection|Ok(connection.query_row("SELECT input_checkpoint_revision FROM catalog_leases WHERE incarnation=?1 AND admission_sequence=?2",rusqlite::params![token.owner.incarnation.as_bytes().as_slice(),token.attempt as i64],|row|row.get::<_,i64>(0))?.to_be_bytes().to_vec())).await?;
            assert_eq!(revision, 1i64.to_be_bytes());
            request.ticket.seal()?;
            let StagingState::Bound(_) =
                timeout(Duration::from_secs(10), request.ticket.wait_terminal()).await?
            else {
                return Err("bound".into());
            };
            let encoded = request
                .ticket
                .bound_session()?
                .reopen_push_request(
                    &request.store,
                    request.directory.path(),
                    &request.disk,
                    None,
                    None,
                )
                .await?;
            assert_eq!(encoded.identity().request_digest, token.request_digest);
            drop(encoded);
            assert_eq!(request.disk.used(), 0);
            assert!(request.coordinator.close_and_drain().await.is_empty());
            request.fixture.runtime.shutdown().await?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn request_checkpoint_owner_restore_adopts_original_bytes_after_source_pin_expiry() -> Result
{
    let request = Request::new(ObjectFormat::Sha256, true, false, [202; 16]).await?;
    request.ticket.stop();
    assert!(request.coordinator.close_and_drain().await.is_empty());
    request.fixture.handle.drain().await?;
    request.fixture.runtime.shutdown().await?;
    let session = SessionId::from_bytes([203; 16]);
    let runtime = CellRuntime::new(SqlWorkerPool::new(1, 4)?, 64 << 20, session)?;
    let authority = CellAuthority::new(request.fixture.layout.clone());
    let idle = authority
        .load(request.fixture.target.cell_id())
        .await?
        .ok_or("idle")?;
    let provision = CellCatalog::new(
        request.fixture.layout.clone(),
        request.fixture.target.tenant(),
    )
    .lookup(request.fixture.target.cell_id())
    .await?
    .ok_or("provision")?;
    let directory = tempfile::TempDir::new()?;
    let handle = runtime
        .acquire_idle_restored(
            provision,
            request.fixture.replica.clone(),
            authority,
            idle,
            directory.path().join("restored.sqlite"),
            Owner {
                session,
                endpoint: "https://request-restored.invalid".into(),
            },
        )
        .await?;
    let client = CellClient::local(request.fixture.registry.clone(), handle.clone());
    let coordinator =
        StagingCoordinator::new(request.fixture.target.clone(), StagingLimits::default())?;
    let old = request.proof.token()?;
    let ticket = coordinator
        .submit(
            ReadyStaging::claim(
                client.clone(),
                request.fixture.target.clone(),
                LeaseRequest {
                    check: LeaseCheck {
                        token: old,
                        actor: "owner".into(),
                    },
                    lease_ms: DEFAULT_LEASE_MS,
                },
                identity()?,
            )
            .await?,
        )
        .map_err(|(error, _)| error)?;
    let StagingState::Active(current) = timeout(Duration::from_secs(10), ticket.wait()).await?
    else {
        return Err("claim".into());
    };
    assert_ne!(current.token.owner, old.owner);
    let store = request.store.clone();
    let prior = request.proof.clone();
    let proof = ticket
        .spawn(move |context| async move {
            context
                .adopt_native_inputs(store, &prior)
                .await
                .map_err(|error| StagingError::Input(Box::new(error)))
        })?
        .wait()
        .await
        .map_err(|error| error.to_string())?;
    assert_eq!(proof.wire_request()?, request.proof.wire_request()?);
    ticket
        .register_inputs(proof, identity()?)
        .map_err(|(error, _)| error)?
        .wait()
        .await
        .map_err(|error| error.to_string())?;
    handle.execute(identity()?,Digest::from_bytes([204;32]),sql::now(0)?,64,0,move|tx|{tx.execute("UPDATE catalog_leases SET expires_at_ms=0 WHERE incarnation=?1 AND admission_sequence=?2",rusqlite::params![old.owner.incarnation.as_bytes().as_slice(),old.attempt as i64])?;Ok(cellule_runtime::cell::executor::HandlerOutcome::Success(Vec::new()))}).await?;
    assert!(
        check(&client, &request.fixture.target, old)
            .await?
            .is_none()
    );
    drop(request.directory);
    let disk = DiskBudget::new(1 << 20);
    let restored_path = directory.path().to_owned();
    let work_disk = disk.clone();
    let store = request.store.clone();
    let expected = request.raw;
    ticket
        .spawn(move |context| async move {
            let encoded = context
                .reopen_push_request(&store, &restored_path, &work_disk, None, None)
                .await
                .map_err(|error| StagingError::Input(Box::new(error)))?;
            assert_eq!(encoded.identity().request_digest, old.request_digest);
            let native = encoded
                .decode(&restored_path, &work_disk, None)
                .await
                .map_err(|error| StagingError::Input(Box::new(error)))?
                .into_native_request();
            assert_eq!(
                native
                    .body
                    .prefix(expected.len())
                    .await
                    .map_err(|error| StagingError::Input(Box::new(error)))?,
                expected
            );
            Ok(())
        })?
        .wait()
        .await
        .map_err(|error| error.to_string())?;
    assert_eq!(disk.used(), 0);
    assert!(coordinator.close_and_drain().await.is_empty());
    runtime.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn request_checkpoint_rejects_late_part_corruption_and_revoked_custody() -> Result {
    use object_store::ObjectStoreExt;
    let request = Request::new(ObjectFormat::Sha256, false, true, [205; 16]).await?;
    let record = request
        .proof
        .wire_request()?
        .unwrap()
        .read(&request.store)
        .await?;
    let path = request
        .store
        .path(record.body_key(), record.request.body.digest)?;
    assert!(record.request.body.size > canopy_object_storage::external::PART_BYTES as u64);
    let part = canopy_object_storage::external::part(&path, 1);
    request
        .provider
        .put(&part, bytes::Bytes::from(vec![b'y'; 1024]).into())
        .await?;
    let store = request.store.clone();
    let root = request.directory.path().to_owned();
    let disk = request.disk.clone();
    request
        .ticket
        .spawn(move |context| async move {
            // First authenticated part is spooled before the corrupt second part.
            assert!(
                context
                    .reopen_push_request(&store, &root, &disk, None, None)
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
        .put(&part, bytes::Bytes::from(vec![b'x'; 1024]).into())
        .await?;
    request.verify().await?;
    mutate(
        &request.fixture.handle,
        "UPDATE repository_identity SET owner='other' WHERE singleton=1".into(),
    )
    .await?;
    let store = request.store.clone();
    let root = request.directory.path().to_owned();
    let disk = request.disk.clone();
    request
        .ticket
        .spawn(move |context| async move {
            assert!(
                context
                    .reopen_push_request(&store, &root, &disk, None, None)
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

#[tokio::test]
async fn request_checkpoint_append_recovers_exact_uncertain_registration_and_old_observers()
-> Result {
    for fault in [1, 2, 3] {
        let request = Request::new(ObjectFormat::Sha1, false, false, [206; 16]).await?;
        let previous = request.ticket.pending_inputs().ok_or("previous observer")?;
        let original = previous.wait().await.map_err(|error| error.to_string())?;
        let store = request.store.clone();
        let prior = request.proof.clone();
        let repository = request.fixture.repository;
        let next = request
            .ticket
            .spawn(move |context| async move {
                let token = context.token()?;
                context
                    .append_native_inputs(
                        store,
                        &prior,
                        records(repository, token.artifact_operation, ObjectFormat::Sha1, 1),
                    )
                    .await
                    .map_err(|error| StagingError::Input(Box::new(error)))
            })?
            .wait()
            .await
            .map_err(|error| error.to_string())?;
        let mutation = identity()?;
        request.coordinator.fault_for_test(fault);
        let observer = request
            .ticket
            .register_inputs(next.clone(), mutation)
            .map_err(|(error, _)| error)?;
        drop(observer);
        assert!(matches!(
            timeout(Duration::from_secs(10), request.ticket.wait_terminal()).await?,
            StagingState::Uncertain(_)
        ));
        assert!(
            request
                .ticket
                .register_inputs(next.clone(), identity()?)
                .is_err()
        );
        assert_eq!(request.coordinator.stats().command_bytes, 12 << 10);
        request.coordinator.recover(&request.ticket)?;
        let registered = request
            .ticket
            .pending_inputs()
            .ok_or("append observer")?
            .wait()
            .await
            .map_err(|error| error.to_string())?;
        let replay = request
            .fixture
            .client()
            .command::<RegisterStagedInputs>(&request.fixture.target, mutation, next.clone())
            .await?;
        assert_eq!(registered, replay.receipt);
        assert_eq!(
            previous.wait().await.map_err(|error| error.to_string())?,
            original
        );
        assert!(registered.commit_sequence > original.commit_sequence);
        assert_eq!(
            check(
                &request.fixture.client(),
                &request.fixture.target,
                next.token()?
            )
            .await?,
            Some(next)
        );
        request.verify().await?;
        assert!(request.coordinator.close_and_drain().await.is_empty());
        request.fixture.runtime.shutdown().await?;
    }
    Ok(())
}
