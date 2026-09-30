use super::*;
use canopy_server::directory::{SshKey, SshKeyChange, TokenAuthority, TokenChange};

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

fn authority(actor: [u8; 32], account: &str) -> TokenAuthority<'_> {
    TokenAuthority {
        actor_digest: actor,
        site_owner: "owner",
        account,
    }
}

fn key(n: u32) -> Result<SshKey> {
    let mut seed = [0; 32];
    seed[..4].copy_from_slice(&n.to_be_bytes());
    let bytes = ed25519_dalek::SigningKey::from_bytes(&seed)
        .verifying_key()
        .to_bytes();
    let public = ssh_key::PublicKey::from(ssh_key::public::Ed25519PublicKey(bytes));
    Ok(SshKey::parse(&public.to_openssh()?)?)
}

async fn fixture(schema: &'static str) -> Result<(CellRuntime, DirectoryCell, tempfile::TempDir)> {
    let application = Arc::new(CanopyApplication::compile(build_descriptor(
        include_bytes!("../../../../Cargo.lock"),
        "ssh-key-test",
    ))?);
    let tenant = TenantId::from_bytes([71; 16]);
    let application_id = ApplicationId::from_bytes([72; 16]);
    let target = directory::directory_target(tenant, application_id)?;
    let session = SessionId::from_bytes([73; 16]);
    let runtime = runtime(session)?;
    let layout = CellStorageLayout::new(
        Store::new(Arc::new(InMemory::new())),
        StorePath::from("ssh-key-test"),
        *application_id.as_bytes(),
    );
    let files = tempfile::TempDir::new()?;
    let handle = bootstrap(
        &runtime,
        &application.registry(),
        &layout,
        &target,
        (DirectoryModule::NAME, schema),
        session,
        &files.path().join("directory.sqlite"),
    )
    .await?;
    let cell = DirectoryCell::new(
        &app_handle(&application, tenant, application_id, handle)?,
        target,
    )?;
    cell.create_account(random_identity()?, "owner", [1; 32], TokenScope::Admin)
        .await?;
    cell.create_account(random_identity()?, "member", [2; 32], TokenScope::Admin)
        .await?;
    Ok((runtime, cell, files))
}

#[tokio::test(flavor = "multi_thread")]
async fn ssh_key_identity_fences_ownership_revocation_and_disabled_accounts() -> Result {
    let (runtime, cell, _files) = fixture(directory::SCHEMA).await?;
    let key = key(1)?;
    assert!(cell.ssh_identity(&key, None).await?.output.is_none());
    let identity = random_identity()?;
    let registered = cell
        .register_ssh_key(
            identity,
            authority([2; 32], "member"),
            [1; 16],
            &key,
            TokenScope::Write,
        )
        .await?;
    assert_eq!(registered.output, SshKeyChange::Applied);
    let replay = cell
        .register_ssh_key(
            identity,
            authority([2; 32], "member"),
            [1; 16],
            &key,
            TokenScope::Write,
        )
        .await?;
    assert_eq!(replay.receipt, registered.receipt);
    let principal = cell
        .ssh_identity(&key, Some(registered.receipt))
        .await?
        .output
        .ok_or("missing SSH identity")?;
    assert_eq!(
        (
            principal.key_id,
            principal.account.as_str(),
            principal.scope
        ),
        ([1; 16], "member", TokenScope::Write)
    );
    let recommented = SshKey::parse(&format!("{} another-device\n", key.public_key()))?;
    assert_eq!(key, recommented);
    for (actor, account, id, scope, expected) in [
        (
            [2; 32],
            "member",
            [1; 16],
            TokenScope::Write,
            SshKeyChange::Applied,
        ),
        (
            [2; 32],
            "owner",
            [2; 16],
            TokenScope::Read,
            SshKeyChange::NotFound,
        ),
        (
            [1; 32],
            "owner",
            [2; 16],
            TokenScope::Read,
            SshKeyChange::Conflict,
        ),
        (
            [2; 32],
            "member",
            [2; 16],
            TokenScope::Write,
            SshKeyChange::Conflict,
        ),
        (
            [2; 32],
            "member",
            [1; 16],
            TokenScope::Read,
            SshKeyChange::Conflict,
        ),
    ] {
        assert_eq!(
            cell.register_ssh_key(
                random_identity()?,
                authority(actor, account),
                id,
                &recommented,
                scope
            )
            .await?
            .output,
            expected
        );
    }
    // A public key fingerprint is an identifier, never a bearer credential.
    let fingerprint = ssh_key::PublicKey::from_openssh(key.public_key())?
        .fingerprint(ssh_key::HashAlg::Sha256)
        .sha256()
        .ok_or("fingerprint")?;
    assert!(cell.authenticate(fingerprint, None).await?.output.is_none());
    assert!(
        cell.register_ssh_key(
            random_identity()?,
            authority([1; 32], "owner"),
            [3; 16],
            &key,
            TokenScope::Admin
        )
        .await
        .is_err()
    );
    assert!(
        cell.ssh_keys(authority([2; 32], "owner"), None)
            .await?
            .output
            .is_none()
    );
    assert_eq!(
        cell.revoke_ssh_key(random_identity()?, authority([2; 32], "owner"), [1; 16])
            .await?
            .output,
        SshKeyChange::NotFound
    );
    for _ in 0..2 {
        assert_eq!(
            cell.revoke_ssh_key(random_identity()?, authority([1; 32], "member"), [1; 16])
                .await?
                .output,
            SshKeyChange::Applied
        );
    }
    assert!(cell.ssh_identity(&key, None).await?.output.is_none());
    assert_eq!(
        cell.register_ssh_key(
            random_identity()?,
            authority([1; 32], "member"),
            [1; 16],
            &key,
            TokenScope::Write
        )
        .await?
        .output,
        SshKeyChange::Conflict
    );
    let replacement = self::key(2)?;
    cell.register_ssh_key(
        random_identity()?,
        authority([2; 32], "member"),
        [2; 16],
        &replacement,
        TokenScope::Read,
    )
    .await?;
    assert_eq!(
        cell.ssh_identity(&replacement, None)
            .await?
            .output
            .ok_or("read identity")?
            .scope,
        TokenScope::Read
    );
    cell.disable_account(random_identity()?, authority([1; 32], "member"))
        .await?;
    assert!(
        cell.ssh_identity(&replacement, None)
            .await?
            .output
            .is_none()
    );
    assert!(
        cell.ssh_keys(authority([1; 32], "member"), None)
            .await?
            .output
            .is_none()
    );
    let events = cell
        .account_events([1; 32], "owner", None)
        .await?
        .output
        .ok_or("audit")?;
    let keys: Vec<_> = events
        .iter()
        .filter(|event| event.ssh_key_id.is_some())
        .collect();
    assert_eq!(
        keys.iter()
            .map(|event| event.action.as_str())
            .collect::<Vec<_>>(),
        [
            "ssh_key.registered",
            "ssh_key.revoked",
            "ssh_key.registered"
        ]
    );
    assert_eq!(keys[1].ssh_key_id, Some([1; 16]));
    assert_eq!(keys[1].token_id, None);
    assert_eq!(keys[1].actor.as_deref(), Some("owner"));
    runtime.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn ssh_key_mutations_recheck_authority_and_roll_back_when_audit_fails() -> Result {
    const FAULT_SCHEMA: &str = concat!(
        include_str!("../../src/directory_schema.sql"),
        "CREATE TRIGGER fail_key_audit BEFORE INSERT ON account_events WHEN NEW.action = 'ssh_key.registered' BEGIN SELECT RAISE(ABORT, 'audit unavailable'); END;"
    );
    let (runtime, cell, _files) = fixture(FAULT_SCHEMA).await?;
    let key = key(5)?;
    assert!(
        cell.register_ssh_key(
            random_identity()?,
            authority([1; 32], "owner"),
            [5; 16],
            &key,
            TokenScope::Write
        )
        .await
        .is_err()
    );
    assert!(cell.ssh_identity(&key, None).await?.output.is_none());
    assert!(
        cell.ssh_keys(authority([1; 32], "owner"), None)
            .await?
            .output
            .ok_or("page")?
            .is_empty()
    );
    // Revoke a separate admin token to exercise Directory authorization directly,
    // without an HTTP precheck masking a missing transactional credential check.
    cell.issue_token(
        random_identity()?,
        authority([1; 32], "owner"),
        [9; 16],
        [9; 32],
        TokenScope::Admin,
        None,
    )
    .await?;
    assert_eq!(
        cell.revoke_token(random_identity()?, authority([1; 32], "owner"), [9; 16])
            .await?
            .output,
        TokenChange::Applied
    );
    assert_eq!(
        cell.register_ssh_key(
            random_identity()?,
            authority([9; 32], "owner"),
            [5; 16],
            &key,
            TokenScope::Write
        )
        .await?
        .output,
        SshKeyChange::NotFound
    );
    runtime.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn ssh_key_revocation_rolls_back_when_its_audit_is_unavailable() -> Result {
    const FAULT_SCHEMA: &str = concat!(
        include_str!("../../src/directory_schema.sql"),
        "CREATE TRIGGER fail_key_audit BEFORE INSERT ON account_events WHEN NEW.action = 'ssh_key.revoked' BEGIN SELECT RAISE(ABORT, 'audit unavailable'); END;"
    );
    let (runtime, cell, _files) = fixture(FAULT_SCHEMA).await?;
    let key = key(6)?;
    cell.register_ssh_key(
        random_identity()?,
        authority([1; 32], "owner"),
        [6; 16],
        &key,
        TokenScope::Read,
    )
    .await?;
    assert!(
        cell.revoke_ssh_key(random_identity()?, authority([1; 32], "owner"), [6; 16])
            .await
            .is_err()
    );
    assert!(cell.ssh_identity(&key, None).await?.output.is_some());
    let events = cell
        .account_events([1; 32], "owner", None)
        .await?
        .output
        .ok_or("audit")?;
    assert!(!events.iter().any(|event| event.action == "ssh_key.revoked"));
    runtime.shutdown().await?;
    Ok(())
}

#[test]
fn rsa_strength_uses_actual_modulus_bits_instead_of_rounded_bytes() -> Result {
    for (first, accepted) in [(0x7f, false), (0x80, true)] {
        let mut modulus = vec![0xff; 256];
        modulus[0] = first;
        let rsa = ssh_key::public::RsaPublicKey::new(
            ssh_key::Mpint::from_positive_bytes(&[1, 0, 1]),
            ssh_key::Mpint::from_positive_bytes(&modulus),
        )?;
        assert_eq!(rsa.key_size(), 2048);
        let public = ssh_key::PublicKey::from(rsa).to_openssh()?;
        assert_eq!(SshKey::parse(&public).is_ok(), accepted);
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn ssh_key_quotas_bound_active_keys_and_registration_churn() -> Result {
    let (runtime, cell, _files) = fixture(directory::SCHEMA).await?;
    for n in 1..=256_u32 {
        let key = key(n)?;
        let mut id = [0; 16];
        id[..4].copy_from_slice(&n.to_be_bytes());
        assert_eq!(
            cell.register_ssh_key(
                random_identity()?,
                authority([1; 32], "owner"),
                id,
                &key,
                TokenScope::Read
            )
            .await?
            .output,
            SshKeyChange::Applied
        );
        if n == 64 {
            assert_eq!(
                cell.register_ssh_key(
                    random_identity()?,
                    authority([1; 32], "owner"),
                    [255; 16],
                    &self::key(257)?,
                    TokenScope::Read
                )
                .await?
                .output,
                SshKeyChange::ActiveLimit
            );
            // Exact retries do not consume quota or create extra audit entries.
            assert_eq!(
                cell.register_ssh_key(
                    random_identity()?,
                    authority([1; 32], "owner"),
                    id,
                    &key,
                    TokenScope::Read
                )
                .await?
                .output,
                SshKeyChange::Applied
            );
            for old in 1..=64_u32 {
                let mut old_id = [0; 16];
                old_id[..4].copy_from_slice(&old.to_be_bytes());
                cell.revoke_ssh_key(random_identity()?, authority([1; 32], "owner"), old_id)
                    .await?;
            }
        } else if n > 64 {
            cell.revoke_ssh_key(random_identity()?, authority([1; 32], "owner"), id)
                .await?;
        }
    }
    assert_eq!(
        cell.register_ssh_key(
            random_identity()?,
            authority([1; 32], "owner"),
            [255; 16],
            &key(257)?,
            TokenScope::Read
        )
        .await?
        .output,
        SshKeyChange::IssuanceLimit
    );
    let mut after = None;
    let mut ids = std::collections::BTreeSet::new();
    loop {
        let page = cell
            .ssh_keys(authority([1; 32], "owner"), after)
            .await?
            .output
            .ok_or("page")?;
        for key in &page {
            assert!(!key.enabled);
            assert!(ids.insert(key.id));
        }
        if page.len() < directory::SSH_KEY_PAGE_SIZE {
            break;
        }
        after = page.last().map(|key| key.id);
    }
    assert_eq!(ids.len(), 256);
    runtime.shutdown().await?;
    Ok(())
}
