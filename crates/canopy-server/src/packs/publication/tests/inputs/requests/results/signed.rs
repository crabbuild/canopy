use super::*;
use crate::{
    CanopyApplication, build_descriptor,
    directory::{
        self, DirectoryCell, DirectoryModule, SshKey, SshKeyChange, TokenAuthority, TokenScope,
    },
    push::VerifiedPushCertificate,
};
use cellule_app::{ApplicationHandle, CellApplication};

struct Signers {
    runtime: CellRuntime,
    cell: DirectoryCell,
    handle: CellHandle,
    key: SshKey,
    _files: tempfile::TempDir,
}
fn authority() -> TokenAuthority<'static> {
    TokenAuthority {
        actor_digest: [1; 32],
        site_owner: "owner",
        account: "owner",
    }
}
impl Signers {
    async fn new(target: &CellTarget) -> Result<Self> {
        let app = Arc::new(CanopyApplication::compile(build_descriptor(
            include_bytes!("../../../../../../../../../Cargo.lock"),
            "native-result-signers",
        ))?);
        let target = directory::directory_target(target.tenant(), target.application())?;
        let session = SessionId::from_bytes([219; 16]);
        let runtime = CellRuntime::new(SqlWorkerPool::new(1, 4)?, 64 << 20, session)?;
        let layout = CellStorageLayout::new(
            Store::new(Arc::new(InMemory::new())),
            object_store::path::Path::from("native-result-signers"),
            *target.application().as_bytes(),
        );
        let provision = CellCatalog::new(layout.clone(), target.tenant())
            .provision(CatalogEntry::new(
                &target,
                CatalogRole::Sql,
                app.registry()
                    .module_code(DirectoryModule::NAME)
                    .ok_or("directory code")?,
                1,
            )?)
            .await?;
        let incarnation = IncarnationId::from_bytes([220; 16]);
        let control = CellAuthority::new(layout.clone())
            .create_initial(
                &provision,
                incarnation,
                Owner {
                    session,
                    endpoint: "https://native-result-signers.invalid".into(),
                },
            )
            .await?;
        let replica = CellReplica::new(
            layout.clone(),
            *target.cell_id().as_bytes(),
            *incarnation.as_bytes(),
            Limits::default(),
        )?;
        let files = tempfile::TempDir::new()?;
        let handle = runtime
            .bootstrap(
                provision,
                replica,
                CellAuthority::new(layout),
                control,
                files.path().join("directory.sqlite"),
                |tx| {
                    tx.execute_batch(directory::SCHEMA)?;
                    Ok(())
                },
            )
            .await?;
        let application = ApplicationHandle::<CanopyApplication>::new(
            CellClient::local(app.registry(), handle.clone()),
            app,
            target.tenant(),
            target.application(),
        )?;
        let cell = DirectoryCell::new(&application, target)?;
        cell.create_account(identity()?, "owner", [1; 32], TokenScope::Admin)
            .await?;
        let bytes = ed25519_dalek::SigningKey::from_bytes(&[7; 32])
            .verifying_key()
            .to_bytes();
        let key = SshKey::parse(
            &ssh_key::PublicKey::from(ssh_key::public::Ed25519PublicKey(bytes)).to_openssh()?,
        )?;
        assert_eq!(
            cell.register_ssh_key(identity()?, authority(), [1; 16], &key, TokenScope::Write)
                .await?
                .output,
            SshKeyChange::Applied
        );
        Ok(Self {
            runtime,
            cell,
            handle,
            key,
            _files: files,
        })
    }
}

#[tokio::test]
async fn native_result_checkpoint_rechecks_scoped_signer_authority() -> Result {
    let request = Request::new(ObjectFormat::Sha256, false, false, [221; 16]).await?;
    let signers = Signers::new(&request.fixture.target).await?;
    let foreign = crate::repository_target(
        TenantId::from_bytes([222; 16]),
        request.fixture.target.application(),
        request.fixture.repository,
    )?;
    let foreign_signers = Signers::new(&foreign).await?;
    let mut native = completion("owner", ObjectFormat::Sha256, 1, false);
    // Synthetic opaque witness tests custody, not cryptographic verification.
    // Native signature verification has independent real-SSH integration tests.
    let signed_body = vec![b's'; canopy_object_storage::external::PART_BYTES + 33];
    let fingerprint = signers.key.fingerprint().to_owned();
    native.certificate = Some(VerifiedPushCertificate {
        target: request.fixture.target.clone(),
        request_digest: request.proof.token()?.request_digest,
        signer: "owner".into(),
        key: fingerprint.clone(),
        body: signed_body.clone(),
    });
    retain(&request, native).await?;
    request.ticket.seal()?;
    assert!(matches!(
        timeout(Duration::from_secs(10), request.ticket.wait_terminal()).await?,
        StagingState::Bound(_)
    ));
    let session = request.ticket.bound_session()?;
    assert!(
        session
            .reopen_native_result(
                &request.store,
                request.directory.path(),
                &request.disk,
                None
            )
            .await
            .is_err()
    );
    assert!(
        session
            .reopen_native_result(
                &request.store,
                request.directory.path(),
                &request.disk,
                Some(&foreign_signers.cell)
            )
            .await
            .is_err()
    );
    let recovered = session
        .reopen_native_result(
            &request.store,
            request.directory.path(),
            &request.disk,
            Some(&signers.cell),
        )
        .await?;
    let certificate = recovered.certificate.ok_or("missing certificate")?;
    assert_eq!(certificate.body, signed_body);
    assert_eq!(certificate.key, fingerprint);
    assert_eq!(certificate.signer, "owner");
    assert_eq!(certificate.target, request.fixture.target);
    assert_eq!(
        certificate.request_digest,
        request.proof.token()?.request_digest
    );
    mutate(
        &signers.handle,
        "UPDATE accounts SET enabled=0 WHERE name='owner'".into(),
    )
    .await?;
    assert!(
        session
            .reopen_native_result(
                &request.store,
                request.directory.path(),
                &request.disk,
                Some(&signers.cell)
            )
            .await
            .is_err()
    );
    mutate(
        &signers.handle,
        "UPDATE accounts SET enabled=1 WHERE name='owner'; UPDATE ssh_keys SET scope='read'".into(),
    )
    .await?;
    assert!(
        session
            .reopen_native_result(
                &request.store,
                request.directory.path(),
                &request.disk,
                Some(&signers.cell)
            )
            .await
            .is_err()
    );
    mutate(&signers.handle, "UPDATE ssh_keys SET scope='write'".into()).await?;
    assert_eq!(
        signers
            .cell
            .revoke_ssh_key(identity()?, authority(), [1; 16])
            .await?
            .output,
        SshKeyChange::Applied
    );
    assert!(
        session
            .reopen_native_result(
                &request.store,
                request.directory.path(),
                &request.disk,
                Some(&signers.cell)
            )
            .await
            .is_err()
    );
    assert_eq!(request.disk.used(), 0);
    assert!(request.coordinator.close_and_drain().await.is_empty());
    request.fixture.runtime.shutdown().await?;
    signers.runtime.shutdown().await?;
    foreign_signers.runtime.shutdown().await?;
    Ok(())
}
