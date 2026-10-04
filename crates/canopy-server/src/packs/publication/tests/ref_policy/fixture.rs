//! Scoped synthetic results isolate policy semantics; genuine CGI is qualified
//! independently by native_capture. Native pack bytes and custody are real.
use super::*;
use crate::git_gateway::preflight::EncodedPush;
use crate::git_http::{GitHttpRequest, GitHttpResponse};
use crate::git_input::GitInput;
use crate::packs::catalog::{CatalogFileLimits, CatalogFiles, CatalogIndexes};
use crate::packs::verification::physical::tests::upload_fixture;

pub(super) fn attempt<'a>(
    f: &'a Fixture,
    provider: Arc<dyn object_store::ObjectStore>,
    store: Arc<ArtifactStore>,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Graph>> + 'a>> {
    Box::pin(async move {
        let source = crate::packs::metadata::tests::fixture(f.format, 4).await?;
        let tip = source
            .objects
            .values()
            .find(|(object, _)| object.kind == crate::ObjectKind::Commit)
            .ok_or("policy fixture commit")?
            .0
            .oid;
        let blob = source
            .objects
            .values()
            .find(|(object, _)| object.kind == crate::ObjectKind::Blob)
            .ok_or("policy fixture blob")?
            .0
            .oid;
        let pack = std::fs::read_dir(source.root.path().join("objects/pack"))?
            .find_map(|entry| {
                entry
                    .ok()
                    .map(|entry| entry.path())
                    .filter(|path| path.extension().is_some_and(|ext| ext == "pack"))
            })
            .ok_or("policy fixture native pack")?;
        let root = tempfile::TempDir::new()?;
        let budget = DiskBudget::new(256 << 20);
        let changes = plan(vec![update("refs/heads/main", None, Some(tip))]);
        let mut wire = Vec::new();
        super::super::completion::packet(
            &mut wire,
            format!(
                "{} {} refs/heads/main\0report-status object-format={}\n",
                "0".repeat(f.format.bytes() * 2),
                hex::encode(tip),
                f.format.as_str()
            )
            .as_bytes(),
        );
        wire.extend_from_slice(b"0000");
        wire.extend_from_slice(&std::fs::read(pack)?);
        let encoded = EncodedPush::new(
            GitHttpRequest {
                method: "POST".into(),
                path_info: "/repo.git/git-receive-pack".into(),
                query: String::new(),
                content_type: Some("application/x-git-receive-pack-request".into()),
                gzip: false,
                protocol_v2: false,
                authenticated: true,
                body: GitInput::receive(
                    axum::body::Body::from(wire),
                    root.path(),
                    &budget,
                    None,
                    None,
                )
                .await?,
            },
            &f.target,
            f.repository,
            f.format,
            "owner",
            *uuid::Uuid::new_v4().as_bytes(),
        )
        .await?;
        let staging =
            StagingCoordinator::new(f.target.clone(), StagingLimits::default(), f.authority())?;
        let ticket = staging
            .submit(
                ReadyStaging::new(
                    f.client(),
                    f.target.clone(),
                    encoded.identity().clone(),
                    identity()?,
                )
                .await?,
            )
            .map_err(|(error, _)| error)?;
        let StagingState::Active(active) = timeout(Duration::from_secs(10), ticket.wait()).await?
        else {
            return Err("policy fixture admission".into());
        };
        let native = upload_fixture(
            source,
            active.token.artifact_operation,
            provider.clone(),
            store.clone(),
        )
        .await?;
        let descriptor = native.descriptor;
        let upload = store.clone();
        let prior = ticket
            .spawn(move |context| async move {
                let (_, saved) = encoded
                    .retain(&context, &upload)
                    .await
                    .map_err(|error| StagingError::Input(Box::new(error)))?;
                context
                    .seal_push_inputs(upload, [descriptor], saved)
                    .await
                    .map_err(|error| StagingError::Input(Box::new(error)))
            })?
            .wait()
            .await
            .map_err(|error| error.to_string())?;
        ticket
            .register_inputs(prior.clone(), identity()?)
            .map_err(|(error, _)| error)?
            .wait()
            .await
            .map_err(|error| error.to_string())?;
        let mut report = Vec::new();
        super::super::completion::packet(&mut report, b"unpack ok\n");
        super::super::completion::packet(&mut report, b"ok refs/heads/main\n");
        report.extend_from_slice(b"0000");
        let upload = store.clone();
        let directory = root.path().to_owned();
        let disk = budget.clone();
        let checkpoint = ticket
            .spawn(move |context| async move {
                let result = context
                    .retain_native_result(
                        &upload,
                        &prior,
                        PushCompletionRequest {
                            plan: Some(changes),
                            response: GitHttpResponse {
                                status: 200,
                                headers: vec![(
                                    "Content-Type".into(),
                                    "application/x-git-receive-pack-result".into(),
                                )],
                                body: report,
                            },
                            options: Vec::new(),
                            certificate: None,
                        },
                        &directory,
                        &disk,
                    )
                    .await
                    .map_err(|error| StagingError::Input(Box::new(error)))?;
                context
                    .append_native_result(upload, &prior, std::iter::empty(), result)
                    .await
                    .map_err(|error| StagingError::Input(Box::new(error)))
            })?
            .wait()
            .await
            .map_err(|error| error.to_string())?;
        assert!(checkpoint.wire_request()?.is_some());
        assert!(checkpoint.native_result()?.is_some());
        ticket
            .register_inputs(checkpoint, identity()?)
            .map_err(|(error, _)| error)?
            .wait()
            .await
            .map_err(|error| error.to_string())?;
        ticket.seal()?;
        assert!(matches!(
            timeout(Duration::from_secs(10), ticket.wait_terminal()).await?,
            StagingState::Bound(_)
        ));
        let indexes = Arc::new(CatalogIndexes::new(store.clone(), f.format));
        let files = Arc::new(CatalogFiles::new(
            f.root.path(),
            DiskBudget::new(64 << 20),
            store.clone(),
            f.format,
            CatalogFileLimits::default(),
        )?);
        let base = Arc::new(ticket.open_base(indexes, files).await?);
        let mut builder =
            CatalogPreparation::new(root.path(), budget.clone(), base, limits()).await?;
        let (witness, segments) =
            super::super::prepare::physical(&native, root.path(), budget.clone()).await?;
        builder.begin_retained_pack(witness).await?;
        for segment in segments {
            builder.add_segment(segment).await?;
        }
        builder.finish_pack().await?;
        let prepared = builder.finish().await?;
        let refusal = Arc::new(
            Box::pin(Arc::new(ticket.bound_session()?).ready_root_refusal(
                identity()?,
                &store,
                root.path(),
                budget.clone(),
                None,
            ))
            .await?,
        );
        Ok(Graph {
            catalog: CatalogGraph {
                prepared,
                root,
                budget,
                initial: tip,
                tip,
                other: tip,
                blob,
                store,
            },
            staging,
            ticket,
            refusal,
            head: Mutex::new(None),
            provider,
        })
    })
}
