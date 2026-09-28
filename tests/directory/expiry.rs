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
        &app_handle(&application, tenant, application_id, handle.clone())?,
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
    let account_page = cell.accounts([3; 32], "owner", None);
    let audit_page = cell.account_events([3; 32], "owner", None);
    let issuance = cell.issue_token(
        stale_identity,
        authority("owner", [3; 32]),
        [4; 16],
        [4; 32],
        TokenScope::Admin,
        None,
    );
    tokio::pin!(authentication, issuance, account_page, audit_page);
    tokio::select! {
        result = &mut audit_page => panic!("audit page escaped blocked worker: {result:?}"),
        result = &mut account_page => panic!("account page escaped blocked worker: {result:?}"),
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
    assert_eq!(account_page.await?.output, None);
    assert_eq!(audit_page.await?.output, None);
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

#[tokio::test(flavor = "multi_thread")]
async fn issuance_window_and_expiry_release_capacity_without_deleting_history()
-> Result<(), Box<dyn std::error::Error>> {
    use cellule_runtime::{
        primitives::sql::SqlBatch, primitives::sql::SqlStatement, primitives::sql::SqlValue,
    };
    let application = Arc::new(CanopyApplication::compile(build_descriptor(
        include_bytes!("../../Cargo.lock"),
        "credential-window-test",
    ))?);
    let tenant = TenantId::from_bytes([71; 16]);
    let application_id = ApplicationId::from_bytes([72; 16]);
    let target = directory::directory_target(tenant, application_id)?;
    let session = SessionId::from_bytes([73; 16]);
    let runtime = runtime(session)?;
    let layout = CellStorageLayout::new(
        Store::new(Arc::new(InMemory::new())),
        StorePath::from("credential-window"),
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
    let app = app_handle(&application, tenant, application_id, handle)?;
    let cell = DirectoryCell::new(&app, target.clone())?;
    cell.create_account(random_identity()?, "owner", [1; 32], TokenScope::Admin)
        .await?;
    cell.create_account(random_identity()?, "member", [2; 32], TokenScope::Admin)
        .await?;
    let now = random_identity()?.issued_at_ms;
    let boundary = now + 2000;
    let created = boundary - 24 * 60 * 60 * 1000;
    // Published historical rows stand in for credentials issued nearly a day ago;
    // no clock override or privileged time parameter reaches the product handler.
    let sql = app.sql::<DirectoryModule>(target)?;
    sql.batch(random_identity()?, SqlBatch { statements: vec![
        SqlStatement {
            sql: "WITH RECURSIVE history(n) AS (VALUES(1) UNION ALL SELECT n + 1 FROM history WHERE n < 255) INSERT INTO access_tokens (id, digest, account, scope, enabled, created_ms, expires_ms) SELECT randomblob(16), randomblob(32), 'owner', 'read', n % 2, ?1, ?1 + 1 FROM history".into(),
            parameters: vec![SqlValue::Integer(created)],
        },
        SqlStatement {
            sql: "WITH RECURSIVE active(n) AS (VALUES(1) UNION ALL SELECT n + 1 FROM active WHERE n < 63) INSERT INTO access_tokens (id, digest, account, scope, enabled, created_ms, expires_ms) SELECT randomblob(16), randomblob(32), 'member', 'read', 1, ?1, ?2 FROM active".into(),
            parameters: vec![SqlValue::Integer(now), SqlValue::Integer(boundary)],
        },
    ]}).await?;
    let denied = random_identity()?;
    assert_eq!(
        cell.issue_token(
            denied,
            authority("owner", [1; 32]),
            [10; 16],
            [10; 32],
            TokenScope::Read,
            None
        )
        .await?
        .output,
        TokenChange::IssuanceLimit
    );
    assert_eq!(
        cell.issue_token(
            random_identity()?,
            authority("member", [1; 32]),
            [11; 16],
            [11; 32],
            TokenScope::Read,
            None
        )
        .await?
        .output,
        TokenChange::ActiveLimit
    );
    let wait = boundary - random_identity()?.issued_at_ms;
    assert!(wait > 0, "quota fixture did not fill before its boundary");
    tokio::time::sleep(std::time::Duration::from_millis((wait + 20) as u64)).await;
    // Replaying a decided command cannot turn a rejected attempt into a mutation.
    assert_eq!(
        cell.issue_token(
            denied,
            authority("owner", [1; 32]),
            [10; 16],
            [10; 32],
            TokenScope::Read,
            None
        )
        .await?
        .output,
        TokenChange::IssuanceLimit
    );
    for (account, id, digest) in [
        ("owner", [10; 16], [10; 32]),
        ("member", [11; 16], [11; 32]),
    ] {
        assert_eq!(
            cell.issue_token(
                random_identity()?,
                authority(account, [1; 32]),
                id,
                digest,
                TokenScope::Read,
                None
            )
            .await?
            .output,
            TokenChange::Applied
        );
        assert!(cell.authenticate(digest, None).await?.output.is_some());
    }
    let retained = sql
        .query(
            None,
            SqlBatch {
                statements: vec![SqlStatement {
        sql: "SELECT account, count(*) FROM access_tokens GROUP BY account ORDER BY account".into(),
        parameters: vec![],
    }],
            },
        )
        .await?
        .output;
    assert_eq!(
        retained[0].rows,
        vec![
            vec![SqlValue::Text("member".into()), SqlValue::Integer(65)],
            vec![SqlValue::Text("owner".into()), SqlValue::Integer(257)],
        ]
    );
    runtime.shutdown().await?;
    Ok(())
}
