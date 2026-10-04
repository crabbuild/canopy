use super::*;
use crate::packs::publication::{
    PublicationBudget, PublicationCoordinator, PublicationLimits, PublicationOutcome,
    PublicationState,
};
use crate::{CanopyApplication, build_descriptor, repository_target};
use cellule_app::{ApplicationHandle, CellApplication, CompiledApplication};
use cellule_ltx::{CellReplica, Limits};
use cellule_runtime::{
    ApplicationId, CellModule, CellRuntime, Resolution, SessionId, TenantId,
    cell::{
        actor::CellHandle,
        catalog::{CatalogEntry, CatalogRole, CellCatalog},
        worker::SqlWorkerPool,
    },
    control::{Owner, authority::CellAuthority},
    ltx::CellStorageLayout,
};
use cellule_store::Store;
use object_store::{memory::InMemory, path::Path as StorePath};
use tokio::time::{Duration, timeout};

type TestResult<T = ()> = Result<T, Failure>;
async fn expire(original: &RegisteredCustody) -> TestResult {
    loop {
        let expiry = original.evidence().identity().expires_at_ms;
        let now = super::super::unix_now_ms()?;
        if now > expiry {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(u64::try_from(expiry - now + 1)?)).await;
    }
}
struct Fixture {
    files: tempfile::TempDir,
    repository: RepositoryCell,
    client: CellClient,
    runtime: CellRuntime,
    handle: CellHandle,
    authority: PreparationAuthority,
    provider: Arc<dyn ObjectStore>,
    layout: CellStorageLayout,
    replica: CellReplica,
    application: Arc<CompiledApplication>,
    publication_budget: PublicationBudget,
}
impl Fixture {
    async fn new(format: ObjectFormat) -> TestResult<Self> {
        let application = Arc::new(CanopyApplication::compile(build_descriptor(
            include_bytes!("../../../Cargo.toml"),
            "startup-custody-test",
        ))?);
        let tenant = TenantId::from_bytes([91; 16]);
        let application_id = ApplicationId::from_bytes([92; 16]);
        let id = *uuid::Uuid::new_v4().as_bytes();
        let target = repository_target(tenant, application_id, id)?;
        let provider: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let layout = CellStorageLayout::new(
            Store::new(Arc::clone(&provider)),
            StorePath::from("startup-custody"),
            *application_id.as_bytes(),
        );
        let registry = application.registry();
        let proof = CellCatalog::new(layout.clone(), tenant)
            .provision(CatalogEntry::new(
                &target,
                CatalogRole::Sql,
                registry
                    .module_code(RepositoryModule::NAME)
                    .ok_or("module")?,
                1,
            )?)
            .await?;
        let incarnation = IncarnationId::from_bytes([93; 16]);
        let session = SessionId::from_bytes([94; 16]);
        let control_authority = CellAuthority::new(layout.clone());
        let control = control_authority
            .create_initial(
                &proof,
                incarnation,
                Owner {
                    session,
                    endpoint: "https://startup-custody.invalid".into(),
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
        let runtime = CellRuntime::new(SqlWorkerPool::new(1, 4)?, 64 << 20, session)?;
        let handle = runtime
            .bootstrap(
                proof,
                replica.clone(),
                control_authority,
                control,
                files.path().join("repository.sqlite"),
                |tx| {
                    tx.execute_batch(crate::REPOSITORY_SCHEMA)?;
                    Ok(())
                },
            )
            .await?;
        let client = CellClient::local(registry.clone(), handle.clone());
        let app =
            ApplicationHandle::new(client.clone(), application.clone(), tenant, application_id)?;
        let repository = RepositoryCell::new(&app, target.clone(), id, format)?;
        repository
            .ensure_owner(super::super::mutation_identity()?, "owner")
            .await?;
        Ok(Self {
            files,
            repository,
            client,
            runtime,
            handle,
            authority: PreparationAuthority::local(layout.clone(), target),
            provider,
            layout,
            replica,
            application,
            publication_budget: PublicationBudget::new(PublicationLimits::default())?,
        })
    }
    fn input(&self) -> BeginRequest {
        request(&self.repository, "owner")
    }
    async fn boot(&self) -> TestResult {
        ensure(
            InitializationCustody {
                authority: self.authority.clone(),
                maintenance: MaintenanceRequest {
                    repository: self.repository.id,
                    actor: "owner".into(),
                    owner: self.handle.owner_fence(),
                },
            },
            &self.repository,
            self.client.clone(),
            Arc::clone(&self.provider),
            self.files.path(),
            DiskBudget::new(64 << 20),
            true,
        )
        .await
    }
    async fn register(&self, action: CustodyAction, short: bool) -> TestResult<RegisteredCustody> {
        let mut identity = super::super::mutation_identity()?;
        if short {
            identity.expires_at_ms = identity.issued_at_ms + 1_000;
        }
        let ready =
            PreparedCustody::prepare(&self.client, &self.repository.target, action, identity)
                .await?;
        Ok(ready
            .register(&self.client, super::super::mutation_identity()?)
            .await?)
    }
    async fn stop(&self, original: &RegisteredCustody) -> TestResult {
        expire(original).await?;
        let queue = PublicationCoordinator::new(
            self.repository.target.clone(),
            PublicationLimits::default(),
            self.publication_budget.clone(),
        )?;
        let ready = original
            .ready_stop(
                self.client.clone(),
                super::super::mutation_identity()?,
                &self.authority,
            )
            .await?;
        let ticket = queue.submit(ready).await?;
        let PublicationState::Finished(Ok(PublicationOutcome::CustodyStop(outcome))) =
            timeout(Duration::from_secs(10), ticket.wait()).await?
        else {
            return Err("wrong stop outcome".into());
        };
        assert!(outcome.stop.is_some());
        assert!(queue.close_and_drain().await.is_empty());
        Ok(())
    }
    async fn restore(&mut self) -> TestResult {
        let old = self.handle.owner_fence();
        self.handle.drain().await?;
        self.runtime.shutdown().await?;
        std::fs::remove_file(self.files.path().join("repository.sqlite"))?;
        for name in ["repository.sqlite-wal", "repository.sqlite-shm"] {
            let path = self.files.path().join(name);
            if path.exists() {
                std::fs::remove_file(path)?;
            }
        }
        let session = SessionId::from_bytes([95; 16]);
        let runtime = CellRuntime::new(SqlWorkerPool::new(1, 4)?, 64 << 20, session)?;
        let authority = CellAuthority::new(self.layout.clone());
        let target = &self.repository.target;
        let idle = authority.load(target.cell_id()).await?.ok_or("idle")?;
        let proof = CellCatalog::new(self.layout.clone(), target.tenant())
            .lookup(target.cell_id())
            .await?
            .ok_or("provision")?;
        let handle = runtime
            .acquire_idle_restored(
                proof,
                self.replica.clone(),
                authority,
                idle,
                self.files.path().join("restored.sqlite"),
                Owner {
                    session,
                    endpoint: "https://startup-restored.invalid".into(),
                },
            )
            .await?;
        assert!(handle.owner_fence().epoch > old.epoch);
        self.client = CellClient::local(self.application.registry(), handle.clone());
        let app = ApplicationHandle::new(
            self.client.clone(),
            self.application.clone(),
            target.tenant(),
            target.application(),
        )?;
        self.repository = RepositoryCell::new(
            &app,
            target.clone(),
            self.repository.id,
            self.repository.object_format,
        )?;
        self.runtime = runtime;
        self.handle = handle;
        Ok(())
    }
    async fn edit(&self, sql: &'static str) -> TestResult {
        self.handle
            .execute(
                super::super::mutation_identity()?,
                cellule_runtime::Digest::from_bytes(*blake3::hash(sql.as_bytes()).as_bytes()),
                super::super::unix_now_ms()?,
                sql.len(),
                0,
                move |tx| {
                    tx.execute_batch(sql)?;
                    Ok(cellule_runtime::cell::executor::HandlerOutcome::Success(
                        Vec::new(),
                    ))
                },
            )
            .await?;
        Ok(())
    }
    async fn assert_original_preserved(&self, original: &RegisteredCustody) -> TestResult {
        assert!(
            matches!(original.recover_preparation(&self.client).await, Err(InvocationError::Pending(value)) if *value == *original.evidence())
        );
        assert!(matches!(
            self.client.resolve(original.evidence()).await?,
            Resolution::Expired
        ));
        let history = self.handle.query(0, 8, |c| Ok(c.query_row("SELECT count(*) FROM catalog_custody_commands WHERE phase IS NULL AND stopped IS NOT NULL", [], |r| r.get::<_, u64>(0))?.to_be_bytes().to_vec())).await?;
        assert_eq!(
            u64::from_be_bytes(history.try_into().map_err(|_| "count")?),
            1
        );
        Ok(())
    }
}

#[tokio::test]
async fn stopped_startup_begin_can_initialize_without_inventing_an_original_result() -> TestResult {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let f = Fixture::new(format).await?;
        let original = f
            .register(CustodyAction::BeginPreparation(f.input()), true)
            .await?;
        f.stop(&original).await?;
        f.boot().await?;
        f.assert_original_preserved(&original).await?;
        let head =
            RegisteredCustody::load_latest(&f.client, &f.repository.target, f.input().operation)
                .await?
                .ok_or("head")?;
        assert_ne!(head.evidence(), original.evidence());
        assert!(matches!(head.action()?, CustodyAction::BeginPreparation(_)));
        assert!(head.settled());
        f.boot().await?;
        assert_eq!(
            RegisteredCustody::load_latest(&f.client, &f.repository.target, f.input().operation)
                .await?
                .ok_or("head")?
                .evidence(),
            head.evidence()
        );
        f.runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn expired_unexecuted_startup_begin_is_retired_by_the_admitted_transition() -> TestResult {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let f = Fixture::new(format).await?;
        let original = f
            .register(CustodyAction::BeginPreparation(f.input()), true)
            .await?;
        expire(&original).await?;
        f.boot().await?;
        f.assert_original_preserved(&original).await?;
        f.runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn stopped_startup_claim_and_renew_use_current_attempt_after_real_owner_restore() -> TestResult
{
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        for renew in [false, true] {
            for stop_before_restore in [false, true] {
                let mut f = Fixture::new(format).await?;
                let begin = f
                    .register(CustodyAction::BeginPreparation(f.input()), false)
                    .await?;
                let committed = begin.recover_preparation(&f.client).await?;
                let PreparationReply::Granted(lease) = committed.output else {
                    return Err("begin grant".into());
                };
                let prior = lease.token;
                let request = LeaseRequest {
                    check: LeaseCheck {
                        token: prior,
                        actor: "owner".into(),
                    },
                    lease_ms: DEFAULT_LEASE_MS,
                };
                let action = if renew {
                    CustodyAction::RenewPreparation(request)
                } else {
                    CustodyAction::ClaimPreparation(request)
                };
                let original = f.register(action, true).await?;
                if stop_before_restore {
                    f.stop(&original).await?;
                } else {
                    expire(&original).await?;
                }
                f.restore().await?;
                f.boot().await?;
                f.assert_original_preserved(&original).await?;
                let head = RegisteredCustody::load_latest(
                    &f.client,
                    &f.repository.target,
                    f.input().operation,
                )
                .await?
                .ok_or("head")?;
                let CustodyAction::ClaimPreparation(request) = head.action()? else {
                    return Err("successor must claim observed attempt".into());
                };
                assert_eq!(request.check.token, prior);
                let PreparationReply::Granted(next) =
                    head.recover_preparation(&f.client).await?.output
                else {
                    return Err("successor grant".into());
                };
                assert_eq!(next.token.owner, f.handle.owner_fence());
                assert_ne!(next.token, prior);
                f.runtime.shutdown().await?;
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn known_startup_result_precedes_sdk_expiry_and_unexpired_absence_reuses_original()
-> TestResult {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        for execute in [false, true] {
            let f = Fixture::new(format).await?;
            let original = f
                .register(CustodyAction::BeginPreparation(f.input()), execute)
                .await?;
            let prior = if execute {
                Some(original.recover_preparation(&f.client).await?)
            } else {
                None
            };
            if execute {
                expire(&original).await?;
            }
            f.boot().await?;
            let head = RegisteredCustody::load_latest(
                &f.client,
                &f.repository.target,
                f.input().operation,
            )
            .await?
            .ok_or("head")?;
            assert_eq!(head.evidence(), original.evidence());
            assert!(head.stop_fact().is_none());
            let result = head.recover_preparation(&f.client).await?;
            if let Some(prior) = prior {
                assert_eq!(result.receipt, prior.receipt);
            }
            f.runtime.shutdown().await?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn stopped_startup_cannot_turn_inconsistent_binding_into_absence() -> TestResult {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        for sql in [
            "UPDATE catalog_operations SET actor='other'",
            "UPDATE catalog_operations SET request_digest=zeroblob(32)",
            "UPDATE repository_identity SET owner='other'",
        ] {
            let f = Fixture::new(format).await?;
            let begin = f
                .register(CustodyAction::BeginPreparation(f.input()), false)
                .await?;
            let PreparationReply::Granted(lease) =
                begin.recover_preparation(&f.client).await?.output
            else {
                return Err("begin grant".into());
            };
            let original = f
                .register(
                    CustodyAction::RenewPreparation(LeaseRequest {
                        check: LeaseCheck {
                            token: lease.token,
                            actor: "owner".into(),
                        },
                        lease_ms: DEFAULT_LEASE_MS,
                    }),
                    true,
                )
                .await?;
            f.stop(&original).await?;
            f.edit(sql).await?;
            assert!(f.boot().await.is_err());
            let latest = RegisteredCustody::load_latest(
                &f.client,
                &f.repository.target,
                f.input().operation,
            )
            .await?
            .ok_or("head")?;
            assert_eq!(latest.evidence(), original.evidence());
            assert!(!latest.settled());
            f.assert_original_preserved(&original).await?;
            f.runtime.shutdown().await?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn unavailable_owner_keeps_expired_startup_original_unretired_until_authority_returns()
-> TestResult {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        for corrupt in [false, true] {
            let f = Fixture::new(format).await?;
            let original = f
                .register(CustodyAction::BeginPreparation(f.input()), true)
                .await?;
            expire(&original).await?;
            let path = f
                .layout
                .control_path(f.repository.target.cell_id().as_bytes());
            let (control, _) = f.layout.store().get_with_etag(&path).await?;
            if corrupt {
                f.layout
                    .store()
                    .put_overwrite(&path, bytes::Bytes::from_static(b"invalid control"))
                    .await?;
            } else {
                f.layout.store().delete(&path).await?;
            }
            let error = f
                .boot()
                .await
                .expect_err("missing owner must refuse retirement");
            assert!(matches!(
                error.downcast_ref::<CustodyError>(),
                Some(CustodyError::Owner(_))
            ));
            let head = RegisteredCustody::load_latest(
                &f.client,
                &f.repository.target,
                f.input().operation,
            )
            .await?
            .ok_or("head")?;
            assert_eq!(head.evidence(), original.evidence());
            assert!(!head.closed());
            f.layout.store().put_overwrite(&path, control).await?;
            f.boot().await?;
            f.assert_original_preserved(&original).await?;
            f.runtime.shutdown().await?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn startup_does_not_retire_another_purpose_at_its_logical_operation() -> TestResult {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let f = Fixture::new(format).await?;
        let original = f
            .register(CustodyAction::BeginStaging(f.input()), true)
            .await?;
        expire(&original).await?;
        assert!(f.boot().await.is_err());
        let head =
            RegisteredCustody::load_latest(&f.client, &f.repository.target, f.input().operation)
                .await?
                .ok_or("head")?;
        assert_eq!(head.evidence(), original.evidence());
        assert!(!head.closed());
        f.runtime.shutdown().await?;
    }
    Ok(())
}
