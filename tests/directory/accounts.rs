use super::*;
use canopy_server::directory::{DisableAccountOutcome, TokenAuthority, TokenChange};

#[tokio::test(flavor = "multi_thread")]
async fn disable_rechecks_exact_admin_credential_inside_the_directory_transaction()
-> Result<(), Box<dyn std::error::Error>> {
    let application = Arc::new(CanopyApplication::compile(build_descriptor(
        include_bytes!("../../Cargo.lock"),
        "account-disable-test",
    ))?);
    let tenant = TenantId::from_bytes([71; 16]);
    let application_id = ApplicationId::from_bytes([72; 16]);
    let target = directory::directory_target(tenant, application_id)?;
    let session = SessionId::from_bytes([73; 16]);
    let runtime = runtime(session)?;
    let layout = CellStorageLayout::new(
        Store::new(Arc::new(InMemory::new())),
        StorePath::from("disable-test"),
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
        &app_handle(&application, tenant, application_id, handle),
        target,
    )?;
    let initial = random_identity()?;
    cell.create_account(initial, "owner", [1; 32], TokenScope::Admin)
        .await?;
    cell.create_account(random_identity()?, "member", [4; 32], TokenScope::Admin)
        .await?;
    for (id, digest, scope) in [
        ([2; 16], [2; 32], TokenScope::Admin),
        ([3; 16], [3; 32], TokenScope::Read),
    ] {
        assert_eq!(
            cell.issue_token(
                random_identity()?,
                authority([1; 32], "owner"),
                id,
                digest,
                scope,
                None
            )
            .await?
            .output,
            TokenChange::Applied
        );
    }
    assert_eq!(
        cell.revoke_token(
            random_identity()?,
            authority([2; 32], "owner"),
            *initial.request_id.as_bytes()
        )
        .await?
        .output,
        TokenChange::Applied
    );
    for (actor, target, expected) in [
        ([1; 32], "member", DisableAccountOutcome::Forbidden),
        ([3; 32], "member", DisableAccountOutcome::Forbidden),
        ([4; 32], "member", DisableAccountOutcome::Forbidden),
        ([0; 32], "missing", DisableAccountOutcome::Forbidden),
        ([2; 32], "owner", DisableAccountOutcome::SiteOwner),
        ([2; 32], "missing", DisableAccountOutcome::NotFound),
    ] {
        assert_eq!(
            cell.disable_account(random_identity()?, authority(actor, target))
                .await?
                .output,
            expected
        );
    }
    assert!(cell.authenticate([4; 32], None).await?.output.is_some());
    let identity = random_identity()?;
    let disabled = cell
        .disable_account(identity, authority([2; 32], "member"))
        .await?;
    assert_eq!(disabled.output, DisableAccountOutcome::Disabled);
    let replay = cell
        .disable_account(identity, authority([2; 32], "member"))
        .await?;
    assert_eq!(replay.receipt, disabled.receipt);
    assert_eq!(replay.output, disabled.output);
    assert_eq!(
        cell.authenticate([4; 32], Some(disabled.receipt))
            .await?
            .output,
        None
    );
    assert!(!cell.account_exists("member").await?.output);
    assert_eq!(
        cell.create_account(random_identity()?, "member", [5; 32], TokenScope::Admin)
            .await?
            .output,
        CreateAccountOutcome::NameTaken
    );
    assert_eq!(cell.authenticate([5; 32], None).await?.output, None);
    runtime.shutdown().await?;
    Ok(())
}

fn authority(actor_digest: [u8; 32], account: &str) -> TokenAuthority<'_> {
    TokenAuthority {
        actor_digest,
        site_owner: "owner",
        account,
    }
}
