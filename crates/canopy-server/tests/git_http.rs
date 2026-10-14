use canopy_server::git_http::{GitHttpBackend, GitHttpRequest};

#[tokio::test]
async fn git_backend_advertises_smart_fetch_and_authenticated_push()
-> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::TempDir::new()?;
    let backend = GitHttpBackend::initialize(
        root.path().to_path_buf(),
        cellule_ltx::DiskBudget::new(1 << 20),
        "refs/heads/main",
        canopy_server::ObjectFormat::Sha1,
        canopy_server::native_resources::NativeResources::default()
            .scope(canopy_server::native_resources::NativeClass::Foreground),
    )
    .await?;
    for (service, content_type) in [
        (
            "git-upload-pack",
            "application/x-git-upload-pack-advertisement",
        ),
        (
            "git-receive-pack",
            "application/x-git-receive-pack-advertisement",
        ),
    ] {
        let response = backend
            .run(GitHttpRequest {
                method: "GET".into(),
                path_info: "/repo.git/info/refs".into(),
                query: format!("service={service}"),
                content_type: None,
                gzip: false,
                protocol_v2: false,
                body: canopy_server::git_input::GitInput::receive(
                    axum::body::Body::empty(),
                    root.path(),
                    &cellule_ltx::DiskBudget::new(1 << 20),
                    Some(1 << 20),
                    None,
                )
                .await?,
                authenticated: true,
            })
            .await?;
        assert_eq!(response.status, 200);
        assert!(response.headers.iter().any(|(name, value)| {
            name.eq_ignore_ascii_case("Content-Type") && value == content_type
        }));
        let banner = format!("# service={service}\n");
        assert!(
            response
                .body
                .starts_with(format!("{:04x}{banner}", banner.len() + 4).as_bytes())
        );
    }
    Ok(())
}

#[tokio::test]
async fn backend_instances_share_native_capacity_and_recover_after_release()
-> Result<(), Box<dyn std::error::Error>> {
    use canopy_server::native_resources::{NativeClass, NativeResources, NativeUsage, NativeWork};
    let root = tempfile::TempDir::new()?;
    let disk = cellule_ltx::DiskBudget::new(2 << 20);
    let native = NativeResources::default();
    let scope = native.scope(NativeClass::Foreground);
    let mut backends = Vec::new();
    for _ in 0..2 {
        backends.push(
            GitHttpBackend::initialize(
                root.path().into(),
                disk.clone(),
                "refs/heads/main",
                canopy_server::ObjectFormat::Sha256,
                scope.clone(),
            )
            .await?,
        );
    }
    let mut active = Vec::new();
    while let Ok(permit) = scope.try_admit(NativeWork::Read) {
        active.push(permit);
    }
    let held = native.usage()?;
    for backend in &backends {
        let input = canopy_server::git_input::GitInput::receive(
            axum::body::Body::empty(),
            root.path(),
            &disk,
            Some(1 << 20),
            None,
        )
        .await?;
        let error = backend
            .run(GitHttpRequest {
                method: "GET".into(),
                path_info: "/repo.git/info/refs".into(),
                query: "service=git-upload-pack".into(),
                content_type: None,
                gzip: false,
                protocol_v2: false,
                body: input,
                authenticated: true,
            })
            .await
            .unwrap_err();
        assert!(
            matches!(error, canopy_server::git_http::GitHttpError::Io(error) if error.kind() == std::io::ErrorKind::WouldBlock)
        );
        assert_eq!(native.usage()?, held);
    }
    drop(active);
    assert_eq!(native.usage()?, NativeUsage::default());
    for backend in &backends {
        let input = canopy_server::git_input::GitInput::receive(
            axum::body::Body::empty(),
            root.path(),
            &disk,
            Some(1 << 20),
            None,
        )
        .await?;
        let response = backend
            .run(GitHttpRequest {
                method: "GET".into(),
                path_info: "/repo.git/info/refs".into(),
                query: "service=git-upload-pack".into(),
                content_type: None,
                gzip: false,
                protocol_v2: false,
                body: input,
                authenticated: true,
            })
            .await?;
        assert_eq!(response.status, 200);
    }
    assert_eq!(native.usage()?, NativeUsage::default());
    Ok(())
}
