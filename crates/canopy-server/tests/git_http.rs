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
