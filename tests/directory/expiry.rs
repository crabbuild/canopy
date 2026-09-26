use super::*;
use canopy_server::directory::{DisableAccountOutcome, TokenAuthority, TokenChange};

fn authority(account: &str, digest: [u8; 32]) -> TokenAuthority<'_> {
    TokenAuthority {
        actor_digest: digest,
        site_owner: "owner",
        account,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn queued_credentials_expire_at_execution_and_cannot_create_or_revoke_authority()
-> Result<(), Box<dyn std::error::Error>> {
    let application = Arc::new(CanopyApplication::compile(build_descriptor(
        include_bytes!("../../Cargo.lock"),
        "credential-expiry-test",
    ))?);
    let tenant = TenantId::from_bytes([71; 16]);
    let application_id = ApplicationId::from_bytes([72; 16]);
    let target = directory::directory_target(tenant, application_id)?;
    let session = SessionId::from_bytes([73; 16]);
    let runtime = runtime(session)?;
    let layout = CellStorageLayout::new(
        Store::new(Arc::new(InMemory::new())),
        StorePath::from("expiry-test"),
        *application_id.as_bytes(),
    );
    let files = tempfile::TempDir::new()?;
    let handle = bootstrap(
        &runtime,
        &application.registry(),
        &layout,
        &target,
        (DirectoryModule::NAME, directory::SCHEMA),
        session,
        &files.path().join("directory.sqlite"),
    )
    .await?;
    let cell = DirectoryCell::new(
        &app_handle(&application, tenant, application_id, handle.clone()),
        target,
    )?;
    let initial = random_identity()?;
    cell.create_account(initial, "owner", [1; 32], TokenScope::Admin)
        .await?;
    cell.create_account(random_identity()?, "member", [2; 32], TokenScope::Read)
        .await?;
    let identity = random_identity()?;
    let expiry = identity.issued_at_ms + 1500;
    assert_eq!(
        cell.issue_token(
            identity,
            authority("owner", [1; 32]),
            [3; 16],
            [3; 32],
            TokenScope::Admin,
            Some(expiry)
        )
        .await?
        .output,
        TokenChange::Applied
    );
    assert!(cell.authenticate([3; 32], None).await?.output.is_some());

    // Hold the SQL worker before queueing authentication and a mutation. Both
    // requests are polled before expiry, then execute with their old context time.
    let (entered, observed) = tokio::sync::oneshot::channel();
    let (release, released) = std::sync::mpsc::channel();
    let blocker = tokio::spawn(async move {
        handle
            .query(0, 1, move |_| {
                let _ = entered.send(());
                released
                    .recv_timeout(std::time::Duration::from_secs(4))
                    .map_err(|source| cellule_runtime::Error::Facility {
                        name: "test release",
                        source: Box::new(source),
                    })?;
                Ok(Vec::new())
            })
            .await
    });
    observed.await?;
    let stale_identity = random_identity()?;
    let authentication = cell.authenticate([3; 32], None);
    let issuance = cell.issue_token(
        stale_identity,
        authority("owner", [3; 32]),
        [4; 16],
        [4; 32],
        TokenScope::Admin,
        None,
    );
    tokio::pin!(authentication, issuance);
    tokio::select! {
        result = &mut authentication => panic!("query escaped blocked worker: {result:?}"),
        result = &mut issuance => panic!("command escaped blocked worker: {result:?}"),
        _ = tokio::time::sleep(std::time::Duration::from_millis(25)) => {}
    }
    let now = random_identity()?.issued_at_ms;
    assert!(now < expiry, "fixture did not queue before expiry");
    tokio::time::sleep(std::time::Duration::from_millis((expiry - now + 20) as u64)).await;
    release.send(())?;
    blocker.await??;
    assert_eq!(authentication.await?.output, None);
    assert_eq!(issuance.await?.output, TokenChange::NotFound);
    assert_eq!(cell.authenticate([4; 32], None).await?.output, None);
    assert_eq!(
        cell.tokens(authority("owner", [3; 32]), None).await?.output,
        None
    );
    assert_eq!(
        cell.revoke_token(random_identity()?, authority("member", [3; 32]), [0; 16])
            .await?
            .output,
        TokenChange::NotFound
    );
    assert_eq!(
        cell.disable_account(random_identity()?, authority("member", [3; 32]))
            .await?
            .output,
        DisableAccountOutcome::Forbidden
    );
    assert!(cell.account_exists("member").await?.output);
    assert_eq!(
        cell.create_account_authorized(
            random_identity()?,
            authority("new", [3; 32]),
            [5; 32],
            TokenScope::Admin
        )
        .await?
        .output,
        CreateAccountOutcome::Forbidden
    );
    assert!(!cell.account_exists("new").await?.output);
    // Expiry cannot be removed, extended or replayed into a fresh active token.
    for expires in [None, Some(expiry), Some(expiry + 60_000)] {
        assert_eq!(
            cell.issue_token(
                random_identity()?,
                authority("owner", [1; 32]),
                [3; 16],
                [3; 32],
                TokenScope::Admin,
                expires
            )
            .await?
            .output,
            TokenChange::Conflict
        );
    }
    assert_eq!(
        cell.revoke_token(
            random_identity()?,
            authority("owner", [1; 32]),
            *initial.request_id.as_bytes()
        )
        .await?
        .output,
        TokenChange::LastAdmin
    );
    assert_eq!(
        cell.revoke_token(random_identity()?, authority("owner", [1; 32]), [3; 16])
            .await?
            .output,
        TokenChange::Applied
    );
    runtime.shutdown().await?;
    Ok(())
}
