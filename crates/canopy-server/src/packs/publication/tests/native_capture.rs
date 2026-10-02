use super::*;
use super::{
    completion::packet,
    prepare::cleaned,
    publishing::{plan, update},
};
use crate::{
    git_http::{GitHttpBackend, GitHttpRequest, NativeCaptureError},
    git_input::GitInput,
    native_resources::{NativeClass, NativeResources},
    packs::{
        catalog::{CatalogFileLimits, CatalogFiles, CatalogIndexes, CatalogReader},
        metadata::tests::{fixture as input_fixture, limits},
        verification::{
            PhysicalVerifier,
            physical::tests::{independence::git_input, physical_limits},
        },
    },
};
use canopy_object_storage::artifact::ArtifactStore;
use cellule_ltx::DiskBudget;
use tokio::time::{Duration, timeout};

#[tokio::test]
async fn native_receive_stages_verifies_and_publishes_then_clones_after_cache_loss() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let fixture = Fixture::new(format).await?;
        let source = input_fixture(format, 4).await?;
        let tip = source
            .objects
            .values()
            .find(|(object, _)| object.kind == crate::ObjectKind::Commit)
            .ok_or("tip")?
            .0
            .oid;
        let pack_path = std::fs::read_dir(source.root.path().join("objects/pack"))?
            .find_map(|entry| {
                entry
                    .ok()
                    .map(|e| e.path())
                    .filter(|p| p.extension().is_some_and(|e| e == "pack"))
            })
            .ok_or("pack")?;
        let work_root = tempfile::TempDir::new()?;
        let disk = DiskBudget::new(256 << 20);
        let native = NativeResources::default();
        let backend = GitHttpBackend::initialize(
            work_root.path().into(),
            disk.clone(),
            "refs/heads/main",
            format,
            native.scope(NativeClass::Foreground),
        )
        .await?;
        let cache_weak = Arc::downgrade(&backend.cache);
        let mut body = Vec::new();
        packet(
            &mut body,
            format!(
                "{} {} refs/heads/main\0report-status ofs-delta object-format={}\n",
                "0".repeat(format.bytes() * 2),
                hex::encode(tip),
                format.as_str()
            )
            .as_bytes(),
        );
        body.extend_from_slice(b"0000");
        body.extend_from_slice(&std::fs::read(pack_path)?);
        let encoded = crate::git_gateway::preflight::EncodedPush::new(
            GitHttpRequest {
                method: "POST".into(),
                path_info: "/repo.git/git-receive-pack".into(),
                query: String::new(),
                content_type: Some("application/x-git-receive-pack-request".into()),
                gzip: false,
                protocol_v2: false,
                body: GitInput::receive(
                    axum::body::Body::from(body),
                    work_root.path(),
                    &disk,
                    Some(4 << 20),
                    None,
                )
                .await?,
                authenticated: true,
            },
            &fixture.target,
            fixture.repository,
            format,
            "owner",
            [160; 16],
        )
        .await?;
        let request_digest = encoded.identity().request_digest;
        let coordinator =
            StagingCoordinator::new(fixture.target.clone(), StagingLimits::default())?;
        let ready = ReadyStaging::new(
            fixture.client(),
            fixture.target.clone(),
            encoded.identity().clone(),
            identity()?,
        )
        .await?;
        let ticket = coordinator.submit(ready).map_err(|(error, _)| error)?;
        let StagingState::Active(initial) = timeout(Duration::from_secs(10), ticket.wait()).await?
        else {
            return Err("staging not active".into());
        };
        assert_eq!(initial.token.request_digest, request_digest);
        let store = Arc::new(ArtifactStore::new(
            Arc::new(InMemory::new()),
            fixture.repository,
        ));
        let upload_store = Arc::clone(&store);
        let retained = ticket.spawn(move |context| async move {
            let (encoded, saved) = encoded
                .retain(&context, &upload_store)
                .await
                .map_err(|error| StagingError::Input(Box::new(error)))?;
            let request = context
                .seal_push_inputs(upload_store, std::iter::empty(), saved)
                .await
                .map_err(|error| StagingError::Input(Box::new(error)))?;
            Ok((encoded, request))
        })?;
        let (encoded, request_checkpoint) =
            retained.wait().await.map_err(|error| error.to_string())?;
        ticket
            .register_inputs(request_checkpoint.clone(), identity()?)
            .map_err(|(error, _)| error)?
            .wait()
            .await
            .map_err(|error| error.to_string())?;
        assert!(request_checkpoint.root()?.is_none());
        assert!(request_checkpoint.wire_request()?.is_some());
        let producer = backend.clone();
        let upload_store = Arc::clone(&store);
        let request_root = work_root.path().to_owned();
        let request_disk = disk.clone();
        let task = ticket.spawn(move |context| async move {
            let preflight = encoded
                .decode(&request_root, &request_disk, None)
                .await
                .map_err(|error| StagingError::Input(Box::new(error)))?;
            let response = producer
                .run_native_receive(preflight.into_native_request())
                .await
                .map_err(|error| StagingError::Input(Box::new(error)))?;
            let inputs = producer
                .stage_native_packs(&context, &upload_store, physical_limits())
                .await
                .map_err(|error| StagingError::Input(Box::new(error)))?;
            let expected = response.clone();
            let result = context
                .retain_native_result(
                    &upload_store,
                    &request_checkpoint,
                    PushCompletionRequest {
                        plan: Some(plan(vec![update("refs/heads/main", None, Some(tip))])),
                        response,
                        options: Vec::new(),
                        certificate: None,
                    },
                    &request_root,
                    &request_disk,
                )
                .await
                .map_err(|error| StagingError::Input(Box::new(error)))?;
            let certificate = context
                .append_native_result(
                    upload_store,
                    &request_checkpoint,
                    inputs.iter().copied(),
                    result,
                )
                .await
                .map_err(|error| StagingError::Input(Box::new(error)))?;
            Ok((inputs, certificate, expected))
        })?;
        let (inputs, input_certificate, response) = task
            .wait()
            .await
            .map_err(|error| format!("capture: {error:?}"))?;
        assert_eq!(response.status, 200);
        assert!(
            response
                .body
                .windows(b"ok refs/heads/main".len())
                .any(|bytes| bytes == b"ok refs/heads/main")
        );
        // Small native receives must remain pack/index pairs, not loose bodies.
        assert_eq!(
            std::fs::read_dir(backend.git_dir().join("objects"))?.count(),
            2
        );
        assert!(input_certificate.wire_request()?.is_some());
        assert!(input_certificate.native_result()?.is_some());
        assert_eq!(inputs.len(), 1);
        assert_eq!(inputs[0].operation, initial.token.artifact_operation);
        assert_ne!(inputs[0].pack.manifest_digest, [0; 32]);
        assert_ne!(inputs[0].index.manifest_digest, [0; 32]);
        let checkpoint = ticket
            .register_inputs(input_certificate.clone(), identity()?)
            .map_err(|(error, _)| error)?;
        let checkpoint_receipt = checkpoint.wait().await.map_err(|error| error.to_string())?;
        assert_eq!(
            fixture
                .client()
                .query::<CheckStagedInputs>(
                    &fixture.target,
                    Some(checkpoint_receipt),
                    LeaseCheck {
                        token: initial.token,
                        actor: "owner".into()
                    }
                )
                .await?
                .output,
            Some(input_certificate)
        );
        drop((backend, source));
        assert!(cache_weak.upgrade().is_none());
        cleaned(work_root.path(), &disk).await?;
        let recover_store = store.clone();
        let recover_root = work_root.path().to_owned();
        let recover_disk = disk.clone();
        let recovered = ticket.spawn(move |context| async move {
            let encoded = context
                .reopen_push_request(&recover_store, &recover_root, &recover_disk, None, None)
                .await
                .map_err(|error| StagingError::Input(Box::new(error)))?;
            assert_eq!(encoded.identity().request_digest, request_digest);
            let request = encoded
                .decode(&recover_root, &recover_disk, None)
                .await
                .map_err(|error| StagingError::Input(Box::new(error)))?
                .into_native_request();
            assert!(
                request
                    .body
                    .packet_prefix(4096)
                    .await
                    .map_err(|error| StagingError::Input(Box::new(error)))?
                    .windows(b"refs/heads/main".len())
                    .any(|part| part == b"refs/heads/main")
            );
            context
                .reopen_native_result(&recover_store, &recover_root, &recover_disk, None)
                .await
                .map_err(|error| StagingError::Input(Box::new(error)))
        })?;
        let recovered = recovered.wait().await.map_err(|error| error.to_string())?;
        assert_eq!(recovered.response, response);
        drop(response);
        cleaned(work_root.path(), &disk).await?;
        let physical_root = Arc::new(tempfile::TempDir::new()?);
        let physical_disk = DiskBudget::new(256 << 20);
        let mut verifier = PhysicalVerifier::download(
            physical_root.path(),
            physical_disk.clone(),
            &store,
            inputs[0],
            physical_limits(),
            native.scope(NativeClass::Foreground),
        )
        .await?;
        let segment = verifier.inspect_next_shard(inputs[0].object_count).await?;
        let witness = verifier.finish().await?;
        ticket.seal()?;
        assert!(matches!(
            timeout(Duration::from_secs(10), ticket.wait_terminal()).await?,
            StagingState::Bound(_)
        ));
        let indexes = Arc::new(CatalogIndexes::new(Arc::clone(&store), format));
        let files = Arc::new(CatalogFiles::new(
            fixture.root.path(),
            DiskBudget::new(64 << 20),
            Arc::clone(&store),
            format,
            CatalogFileLimits::default(),
        )?);
        let base = Arc::new(ticket.open_base(indexes, files).await?);
        let expected = recovered.response.clone();
        let producer_root = Arc::clone(&physical_root);
        let producer_disk = physical_disk.clone();
        let publication_identity = identity()?;
        let work = ticket.spawn_bound(move |_| async move {
            let result = async {
                let mut builder = CatalogPreparation::new(
                    producer_root.path(),
                    producer_disk.clone(),
                    base,
                    limits(),
                )
                .await?;
                builder.begin_pack(witness)?;
                builder.add_segment(segment).await?;
                builder.finish_pack().await?;
                let prepared = Arc::new(builder.finish().await?);
                let ready = Box::pin(prepared.ready_push(
                    publication_identity,
                    recovered,
                    producer_root.path(),
                    producer_disk.clone(),
                    limits(),
                ))
                .await?;
                Ok::<_, Box<dyn std::error::Error + Send + Sync>>((prepared, ready))
            }
            .await;
            result.map_err(StagingError::Input)
        })?;
        let (prepared, ready) = work.wait().await.map_err(|error| error.to_string())?;
        let publications =
            PublicationCoordinator::new(fixture.target.clone(), PublicationLimits::default())?;
        let observer = ticket.publish(&publications, ready)?;
        let completed =
            super::coordinator::finished(timeout(Duration::from_secs(10), observer.wait()).await?)?;
        assert!(matches!(
            timeout(Duration::from_secs(10), ticket.wait_terminal()).await?,
            StagingState::Published(Ok(_))
        ));
        assert!(matches!(
            completed.output,
            CatalogCompletionReply::Completed(CompletedCatalogPush {
                rejected: false,
                publication: Some(PublishedRefs {
                    generation: 1,
                    ref_generation: 1,
                    ..
                }),
                ..
            })
        ));
        assert_eq!(observer.response().await?, expected);
        let sql = cellule_runtime::primitives::sql::SqlCell::<RepositoryModule>::new(
            fixture.client(),
            fixture.target.clone(),
        )?;
        let observed = sql
            .query(
                Some(completed.receipt),
                super::super::sql::statement(super::super::sql::CURRENT, vec![]),
            )
            .await?;
        let published =
            super::super::sql::generation(&observed.output, fixture.repository, format)?
                .catalog
                .ok_or("published root")?;
        assert_eq!(published, prepared.catalog());
        drop(prepared);
        cleaned(physical_root.path(), &physical_disk).await?;
        assert!(coordinator.close_and_drain().await.is_empty());
        assert!(publications.close_and_drain().await.is_empty());
        // A new admitted workspace reconstructs from the committed catalog's
        // source, with no producer descriptors/cache or SQL Git object records.
        let cold_root = tempfile::TempDir::new()?;
        let cold_disk = DiskBudget::new(64 << 20);
        let indexes = Arc::new(CatalogIndexes::new(Arc::clone(&store), format));
        let files = CatalogFiles::new(
            cold_root.path(),
            cold_disk.clone(),
            Arc::clone(&store),
            format,
            CatalogFileLimits::default(),
        )?;
        let reader = CatalogReader::open(indexes, published).await?;
        let selected = reader
            .lookup(tip, &files, &files)
            .await?
            .ok_or("canonical tip")?;
        let cold = GitHttpBackend::initialize(
            cold_root.path().into(),
            cold_disk.clone(),
            "refs/heads/main",
            format,
            native.scope(NativeClass::Foreground),
        )
        .await?;
        cold.cache
            .download_native(&store, selected.source.record.native())
            .await?;
        cold.cache
            .store_refs(&std::collections::BTreeMap::from([(
                "refs/heads/main".into(),
                crate::RefExpectation {
                    oid: Some(tip),
                    version: 1,
                },
            )]))
            .await?;
        let client = tempfile::TempDir::new()?;
        let clone_path = client.path().join("clone");
        git_input(
            client.path(),
            &[
                "-c",
                "protocol.file.allow=always",
                "clone",
                "--no-local",
                cold.git_dir().to_str().ok_or("cache path")?,
                clone_path.to_str().ok_or("clone path")?,
            ],
            &[],
        )
        .await?;
        git_input(&clone_path, &["fsck", "--full"], &[]).await?;
        let cloned = git_input(&clone_path, &["rev-parse", "HEAD"], &[]).await?;
        assert_eq!(String::from_utf8(cloned)?.trim(), hex::encode(tip));
        fixture.handle.query(0,1024,|db| {
            assert_eq!(db.query_row("SELECT count(*) FROM sqlite_schema WHERE name IN ('objects','object_edges','object_closure','git_packs')",[],|r|r.get::<_,u64>(0))?,0);
            Ok(Vec::new())
        }).await?;
        fixture.runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn native_capture_rejects_scope_limits_mutation_and_active_native_workers() -> Result {
    let fixture = Fixture::new(ObjectFormat::Sha1).await?;
    let source = input_fixture(ObjectFormat::Sha1, 4).await?;
    let root = tempfile::TempDir::new()?;
    let disk = DiskBudget::new(16 << 20);
    let native = NativeResources::default();
    let backend = GitHttpBackend::initialize(
        root.path().into(),
        disk.clone(),
        "refs/heads/main",
        ObjectFormat::Sha1,
        native.scope(NativeClass::Foreground),
    )
    .await?;
    for entry in std::fs::read_dir(source.root.path().join("objects/pack"))? {
        let entry = entry?;
        std::fs::copy(
            entry.path(),
            backend
                .git_dir()
                .join("objects/pack")
                .join(entry.file_name()),
        )?;
    }
    backend.cache.reconcile().await?;
    let store = Arc::new(ArtifactStore::new(
        Arc::new(InMemory::new()),
        fixture.repository,
    ));
    let coordinator = StagingCoordinator::new(fixture.target.clone(), StagingLimits::default())?;
    let ready = ReadyStaging::new(
        fixture.client(),
        fixture.target.clone(),
        fixture.begin([161; 16]),
        identity()?,
    )
    .await?;
    let ticket = coordinator.submit(ready).map_err(|(error, _)| error)?;
    assert!(matches!(
        timeout(Duration::from_secs(10), ticket.wait()).await?,
        StagingState::Active(_)
    ));
    let task = ticket.spawn(move |context| async move {
        rejection_checks(context, backend, store)
            .await
            .map_err(StagingError::Input)
    })?;
    task.wait()
        .await
        .map_err(|error| format!("capture checks: {error:?}"))?;
    ticket.seal()?;
    assert!(matches!(
        timeout(Duration::from_secs(10), ticket.wait_terminal()).await?,
        StagingState::Bound(_)
    ));
    assert!(coordinator.close_and_drain().await.is_empty());
    fixture.runtime.shutdown().await?;
    Ok(())
}

async fn rejection_checks(
    context: StagingContext,
    backend: GitHttpBackend,
    store: Arc<ArtifactStore>,
) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let other = ArtifactStore::new(Arc::new(InMemory::new()), [99; 16]);
    assert!(matches!(
        backend
            .stage_native_packs(&context, &other, physical_limits())
            .await,
        Err(NativeCaptureError::Context)
    ));
    let small = crate::packs::verification::PhysicalLimits {
        max_pack_bytes: 1,
        ..physical_limits()
    };
    assert!(matches!(
        backend.stage_native_packs(&context, &store, small).await,
        Err(NativeCaptureError::Limit)
    ));
    let path = std::fs::read_dir(backend.git_dir().join("objects/pack"))?
        .find_map(|entry| {
            entry
                .ok()
                .map(|e| e.path())
                .filter(|p| p.extension().is_some_and(|e| e == "pack"))
        })
        .ok_or("pack")?;
    let loose = backend.git_dir().join("objects/aa");
    std::fs::create_dir(&loose)?;
    assert!(matches!(
        backend
            .stage_native_packs(&context, &store, physical_limits())
            .await,
        Err(NativeCaptureError::Context)
    ));
    std::fs::remove_dir(&loose)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
    }
    #[cfg(not(unix))]
    {
        let mut permissions = std::fs::metadata(&path)?.permissions();
        permissions.set_readonly(false);
        std::fs::set_permissions(&path, permissions)?;
    }
    let original = std::fs::read(&path)?;
    let mut corrupted = original.clone();
    corrupted[12] ^= 1;
    std::fs::write(&path, &corrupted)?;
    assert!(matches!(
        backend
            .stage_native_packs(&context, &store, physical_limits())
            .await,
        Err(NativeCaptureError::Binding(_))
    ));
    std::fs::write(&path, original)?;
    let mut command = crate::native_git::command(&backend.git_dir())?;
    command
        .args(["hash-object", "--stdin"])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null());
    let mut worker = crate::native_git::process::GitProcess::spawn(
        command,
        Arc::clone(&backend.cache),
        backend
            .cache
            .native
            .try_admit(crate::native_resources::NativeWork::Read)?,
    )?;
    let blocked = matches!(backend.stage_native_packs(&context,&store,physical_limits()).await,Err(NativeCaptureError::Io(error)) if error.kind()==std::io::ErrorKind::WouldBlock);
    drop(worker.child.stdin.take());
    let status = timeout(Duration::from_secs(5), worker.wait()).await??;
    drop(worker);
    assert!(blocked && status.success());
    let first = backend
        .stage_native_packs(&context, &store, physical_limits())
        .await?;
    let replay = backend
        .stage_native_packs(&context, &store, physical_limits())
        .await?;
    assert_eq!(first, replay);
    assert_eq!(first.len(), 1);
    Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
}
