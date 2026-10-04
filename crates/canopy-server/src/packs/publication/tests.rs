use super::*;
mod attestation;
mod compaction;
mod completion;
mod coordinator;
mod custody;
mod durable_policy;
mod durable_recovery;
mod frontier;
mod initialization;
mod initialization_recovery;
mod initialization_retirement;
mod inputs;
mod mandatory_registration;
mod namespaces;
mod native_capture;
mod policy_dispatch;
mod policy_refusal;
mod preparation_receipt;
mod prepare;
mod publishing;
mod reconcile;
mod recovery_discovery;
mod ref_policy;
mod ref_snapshot;
mod refs;
mod root_completion;
mod root_dispatch;
mod root_outcome;
mod staged_durable;
mod staging;
mod staging_receipt;
mod staging_service;
mod terminal_retention;
use cellule_ltx::{CellReplica, CellStorageLayout, Limits};
use cellule_runtime::{
    ApplicationId, BuildDescriptor, CellClient, CellRuntime, CellTarget, Digest, InvocationError,
    MigrationDescriptor, ModuleDescriptor, MutationIdentity, NamespaceDescriptor, Registry,
    SessionId, SqlWorkerPool, TenantId,
    cell::{
        actor::CellHandle,
        catalog::{CatalogEntry, CatalogRole, CellCatalog},
    },
    control::{Owner, authority::CellAuthority},
    identity::RequestId,
};
use cellule_store::Store;
use object_store::{memory::InMemory, path::Path};
use std::{
    sync::{Arc, OnceLock},
    time::{SystemTime, UNIX_EPOCH},
};
type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;
struct Module;
impl CellModule for Module {
    const NAME: &'static str = "repository";
    fn descriptor(&self) -> &'static ModuleDescriptor {
        static DESCRIPTOR: OnceLock<ModuleDescriptor> = OnceLock::new();
        DESCRIPTOR.get_or_init(|| {
            let descriptor = |id| cellule_runtime::registry::OperationDescriptor {
                id,
                codec_version: 1,
                schema_min: 1,
                schema_max: 1,
                input_limit: 4096,
                output_limit: 4096,
            };
            let mut ref_descriptor = descriptor(90);
            ref_descriptor.input_limit = 64 << 10;
            let mut publish_descriptor = descriptor(18);
            publish_descriptor.input_limit = 4 << 20;
            let mut complete_descriptor = descriptor(19);
            complete_descriptor.input_limit = 4 << 20;
            let mut commands = super::registry::COMMANDS.to_vec();
            commands.extend([publish_descriptor, complete_descriptor, ref_descriptor]);
            // Raw domain receivers qualify their invariants here. Production
            // binds only the mandatory registered custody envelope.
            for (id, codec) in [
                (BeginPreparation::ID, BeginPreparation::CODEC_VERSION),
                (ClaimPreparation::ID, ClaimPreparation::CODEC_VERSION),
                (RenewPreparation::ID, RenewPreparation::CODEC_VERSION),
                (BeginStaging::ID, BeginStaging::CODEC_VERSION),
                (ClaimStaging::ID, ClaimStaging::CODEC_VERSION),
                (RenewStaging::ID, RenewStaging::CODEC_VERSION),
                (BindStaging::ID, BindStaging::CODEC_VERSION),
            ] {
                let mut domain = descriptor(id);
                domain.codec_version = codec;
                commands.push(domain);
            }
            let mut queries = super::registry::QUERIES.to_vec();
            queries.push(descriptor(20));
            ModuleDescriptor {
                name: Self::NAME,
                source_digest: Digest::from_bytes([11; 32]),
                retained_codes: &[],
                schema_min: 1,
                schema_max: 1,
                migrations: Box::leak(Box::new([MigrationDescriptor {
                    version: 1,
                    sql: SCHEMA,
                    digest: Digest::from_bytes(*blake3::hash(SCHEMA.as_bytes()).as_bytes()),
                }])),
                commands: Box::leak(commands.into_boxed_slice()),
                queries: Box::leak(queries.into_boxed_slice()),
                workflow_definitions: &[],
                activity_types: &[],
                namespaces: Box::leak(Box::new([NamespaceDescriptor {
                    id: crate::REPOSITORIES,
                    name: "repository",
                    role: CatalogRole::Sql,
                    shards: 1,
                    effect_targets: &[],
                    dead_letter: None,
                }])),
            }
        })
    }
    fn register(self, registry: &mut RegistryBuilder) -> cellule_runtime::Result<()> {
        cellule_runtime::primitives::sql::register_sql::<RepositoryModule>(registry)?;
        super::register(registry)?;
        registry.bind_command::<BeginPreparation>()?;
        registry.bind_command::<ClaimPreparation>()?;
        registry.bind_command::<RenewPreparation>()?;
        registry.bind_command::<BeginStaging>()?;
        registry.bind_command::<ClaimStaging>()?;
        registry.bind_command::<RenewStaging>()?;
        registry.bind_command::<BindStaging>()?;
        registry.bind_command::<PublishCatalogRefs>()?;
        registry.bind_command::<CompleteCatalogPush>()?;
        registry.bind_query::<CheckCompletedPush>()?;
        registry.bind_command::<refs::FixtureRefs>()
    }
}
struct Fixture {
    root: tempfile::TempDir,
    target: CellTarget,
    repository: [u8; 16],
    format: ObjectFormat,
    layout: CellStorageLayout,
    replica: CellReplica,
    registry: Arc<Registry>,
    runtime: CellRuntime,
    handle: CellHandle,
}
impl Fixture {
    async fn new(format: ObjectFormat) -> Result<Self> {
        Self::with_artifact_sequence(format, 0).await
    }
    async fn with_artifact_sequence(format: ObjectFormat, artifact_sequence: i64) -> Result<Self> {
        let mut builder = RegistryBuilder::new(BuildDescriptor {
            source_revision: "packed-publication-test".into(),
            cargo_lock_digest: Digest::from_bytes([10; 32]),
        });
        builder.register(Module)?;
        let registry = Arc::new(builder.finish()?);
        let repository = *uuid::Uuid::new_v4().as_bytes();
        let tenant = TenantId::from_bytes([12; 16]);
        let application = ApplicationId::from_bytes([13; 16]);
        let target = crate::repository_target(tenant, application, repository)?;
        let layout = CellStorageLayout::new(
            Store::new(Arc::new(InMemory::new())),
            Path::from("packed-publication"),
            *application.as_bytes(),
        );
        let incarnation = IncarnationId::from_bytes([14; 16]);
        let replica = CellReplica::new(
            layout.clone(),
            *target.cell_id().as_bytes(),
            *incarnation.as_bytes(),
            Limits::default(),
        )?;
        let session = SessionId::from_bytes([15; 16]);
        let runtime = CellRuntime::new(SqlWorkerPool::new(1, 4)?, 64 << 20, session)?;
        let catalog = CellCatalog::new(layout.clone(), tenant);
        let proof = catalog
            .provision(CatalogEntry::new(
                &target,
                CatalogRole::Sql,
                registry.module_code("repository").ok_or("code")?,
                1,
            )?)
            .await?;
        let authority = CellAuthority::new(layout.clone());
        let control = authority
            .create_initial(
                &proof,
                incarnation,
                Owner {
                    session,
                    endpoint: "https://owner-a.invalid".into(),
                },
            )
            .await?;
        let root = tempfile::TempDir::new()?;
        let handle=runtime.bootstrap(proof,replica.clone(),authority,control,root.path().join("a.sqlite"),move|tx|{
            tx.execute_batch(SCHEMA)?;
            tx.execute("INSERT INTO repository_identity(singleton,repository_id,object_format,owner,push_cert_seed,artifact_sequence) VALUES(1,?1,?2,'owner',?3,?4)",rusqlite::params![repository.as_slice(),format.as_str(),[16u8;32].as_slice(),artifact_sequence])?;Ok(())
        }).await?;
        Ok(Self {
            root,
            target,
            repository,
            format,
            layout,
            replica,
            registry,
            runtime,
            handle,
        })
    }
    fn client(&self) -> CellClient {
        CellClient::local(Arc::clone(&self.registry), self.handle.clone())
    }
    fn begin(&self, operation: [u8; 16]) -> BeginRequest {
        BeginRequest {
            repository: self.repository,
            operation,
            request_digest: [17; 32],
            actor: "owner".into(),
            lease_ms: DEFAULT_LEASE_MS,
        }
    }
    async fn counts(&self) -> Result<(u64, u64)> {
        counts(&self.handle).await
    }
    async fn counts_for(&self, token: PreparationToken) -> Result<(u64, u64)> {
        let bytes = self.handle.query(0, 16, move |connection| {
            let parameters = rusqlite::params![token.owner.incarnation.as_bytes().as_slice(), token.attempt];
            let operations: u64 = connection.query_row("SELECT count(*) FROM catalog_operations WHERE incarnation=?1 AND admission_sequence=?2", parameters, |row| row.get(0))?;
            let leases: u64 = connection.query_row("SELECT count(*) FROM catalog_leases WHERE incarnation=?1 AND admission_sequence=?2", parameters, |row| row.get(0))?;
            Ok([operations.to_be_bytes(), leases.to_be_bytes()].concat())
        }).await?;
        Ok((
            u64::from_be_bytes(bytes[..8].try_into()?),
            u64::from_be_bytes(bytes[8..].try_into()?),
        ))
    }
    // Trusted fixture injection only. Production generation facts require the
    // complete catalog verifier and fenced publisher; a digest is not a proof.
    async fn install_catalog(&self, generation: u64, catalog: StoredCatalog) -> Result<()> {
        self.install_generation(generation, catalog, None).await
    }
    async fn install_generation(
        &self,
        generation: u64,
        catalog: StoredCatalog,
        refs: Option<RefStateSnapshotRoot>,
    ) -> Result<()> {
        let mut encoder = BoundedEncoder::new(256)?;
        catalog.encode(&mut encoder)?;
        let bytes = encoder.finish();
        let refs = refs
            .map(|refs| {
                let mut e = BoundedEncoder::new(128)?;
                refs.encode(&mut e)?;
                Ok::<_, CodecError>(e.finish())
            })
            .transpose()?;
        self.handle
            .execute(
                identity()?,
                Digest::from_bytes([64; 32]),
                sql::now(0)?,
                bytes.len(),
                0,
                move |tx| {
                    tx.execute(
                        "INSERT INTO catalog_generations(generation,catalog,certificate,refs) VALUES(?1,?2,?3,?4)",
                        rusqlite::params![generation as i64, bytes, [42u8; 32].as_slice(), refs],
                    )?;
                    tx.execute(
                        "UPDATE catalog_state SET generation=?1 WHERE singleton=1",
                        [generation as i64],
                    )?;
                    Ok(cellule_runtime::cell::executor::HandlerOutcome::Success(
                        Vec::new(),
                    ))
                },
            )
            .await?;
        Ok(())
    }
    async fn install_empty_root(&self, generation: u64) -> Result<StoredCatalog> {
        use crate::packs::{catalog::CatalogSnapshot, directory::snapshot::DirectorySnapshot};
        use canopy_object_storage::artifact::ArtifactStore;
        let store = ArtifactStore::new(Arc::new(InMemory::new()), self.repository);
        let directory = DirectorySnapshot::empty(self.repository, self.format)
            .upload(&store, [generation as u8 + 40; 16])
            .await?;
        let catalog = CatalogSnapshot {
            directory,
            sources: None,
        }
        .upload(&store, [generation as u8 + 50; 16])
        .await?;
        self.install_catalog(generation, catalog).await?;
        Ok(catalog)
    }
}
fn identity() -> std::io::Result<MutationIdentity> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(std::io::Error::other)?;
    let now = i64::try_from(elapsed.as_millis()).map_err(std::io::Error::other)?;
    Ok(MutationIdentity {
        request_id: RequestId::from_bytes(uuid::Uuid::new_v4().into_bytes()),
        issued_at_ms: now,
        expires_at_ms: now + 60_000,
    })
}
fn lease(reply: PreparationReply) -> Result<PreparationLease> {
    match reply {
        PreparationReply::Granted(lease) => Ok(*lease),
        PreparationReply::Denied(reason) => Err(format!("denied: {reason:?}").into()),
    }
}
fn check(token: PreparationToken) -> LeaseCheck {
    LeaseCheck {
        token,
        actor: "owner".into(),
    }
}
fn request(token: PreparationToken) -> LeaseRequest {
    LeaseRequest {
        check: check(token),
        lease_ms: DEFAULT_LEASE_MS,
    }
}
fn rejected(
    result: std::result::Result<
        cellule_runtime::Committed<PreparationReply>,
        InvocationError<PreparationReply>,
    >,
    reason: PreparationDenial,
) {
    assert!(
        matches!(result,Err(InvocationError::Rejected(ref outcome)) if outcome.output==PreparationReply::Denied(reason))
    );
}
async fn counts(handle: &CellHandle) -> Result<(u64, u64)> {
    let bytes = handle
        .query(0, 16, |connection| {
            let operations: u64 =
                connection.query_row("SELECT count(*) FROM catalog_operations", [], |row| {
                    row.get(0)
                })?;
            let leases: u64 =
                connection
                    .query_row("SELECT count(*) FROM catalog_leases", [], |row| row.get(0))?;
            Ok([operations.to_be_bytes(), leases.to_be_bytes()].concat())
        })
        .await?;
    Ok((
        u64::from_be_bytes(bytes[..8].try_into()?),
        u64::from_be_bytes(bytes[8..].try_into()?),
    ))
}

#[tokio::test]
async fn preparation_replay_renewal_abort_and_record_recreation_keep_attempt_identity() -> Result {
    let fixture = Fixture::new(ObjectFormat::Sha256).await?;
    let client = fixture.client();
    let input = fixture.begin([20; 16]);
    let mutation = identity()?;
    let first = client
        .command::<BeginPreparation>(&fixture.target, mutation, input.clone())
        .await?;
    let started = lease(first.output.clone())?;
    assert_eq!(started.token.owner, fixture.handle.owner_fence());
    assert_eq!(started.token.artifact_operation, artifact_number(1));
    assert_eq!(started.base.generation, 0);
    assert!(started.base.catalog.is_none());
    let replay = client
        .command::<BeginPreparation>(&fixture.target, mutation, input.clone())
        .await?;
    assert_eq!(replay.output, first.output);
    assert_eq!(replay.receipt, first.receipt);
    assert_eq!(fixture.counts().await?, (1, 1));
    let duplicate = lease(
        client
            .command::<BeginPreparation>(&fixture.target, identity()?, input.clone())
            .await?
            .output,
    )?;
    assert_eq!(duplicate.token, started.token);
    assert_eq!(duplicate.expires_at_ms, started.expires_at_ms);
    let mut conflict = input.clone();
    conflict.request_digest[0] ^= 1;
    rejected(
        client
            .command::<BeginPreparation>(&fixture.target, identity()?, conflict)
            .await,
        PreparationDenial::Conflict,
    );
    let mut renewal = request(started.token);
    renewal.lease_ms = 1;
    let renewed = lease(
        client
            .command::<RenewPreparation>(&fixture.target, identity()?, renewal)
            .await?
            .output,
    )?;
    assert_eq!(renewed.token, started.token);
    assert!(renewed.expires_at_ms >= started.expires_at_ms);
    assert!(
        client
            .query::<CheckPreparation>(&fixture.target, None, check(started.token))
            .await?
            .output
            .is_some()
    );
    assert!(
        client
            .command::<AbortPreparation>(&fixture.target, identity()?, check(started.token))
            .await?
            .output
    );
    assert_eq!(fixture.counts().await?, (0, 1));
    assert!(
        client
            .query::<CheckPreparation>(&fixture.target, None, check(started.token))
            .await?
            .output
            .is_none()
    );
    let next = lease(
        client
            .command::<BeginPreparation>(&fixture.target, identity()?, input)
            .await?
            .output,
    )?;
    assert_eq!(next.token.owner, started.token.owner);
    assert!(next.token.attempt > started.token.attempt);
    assert_eq!(next.token.artifact_operation, artifact_number(2));
    rejected(
        client
            .command::<RenewPreparation>(&fixture.target, identity()?, request(started.token))
            .await,
        PreparationDenial::Stale,
    );
    rejected(
        client
            .command::<ClaimPreparation>(&fixture.target, identity()?, request(started.token))
            .await,
        PreparationDenial::Stale,
    );
    assert_eq!(fixture.counts().await?, (1, 2));
    fixture.runtime.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn successor_claim_rejects_the_old_owner_and_preserves_exact_replayed_result() -> Result {
    let fixture = Fixture::new(ObjectFormat::Sha1).await?;
    let input = fixture.begin([21; 16]);
    let mutation = identity()?;
    let first = fixture
        .client()
        .command::<BeginPreparation>(&fixture.target, mutation, input.clone())
        .await?;
    let old = lease(first.output.clone())?;
    fixture.handle.drain().await?;
    fixture.runtime.shutdown().await?;
    let session = SessionId::from_bytes([22; 16]);
    let runtime = CellRuntime::new(SqlWorkerPool::new(1, 4)?, 64 << 20, session)?;
    let authority = CellAuthority::new(fixture.layout.clone());
    let idle = authority
        .load(fixture.target.cell_id())
        .await?
        .ok_or("idle")?;
    let proof = CellCatalog::new(fixture.layout.clone(), fixture.target.tenant())
        .lookup(fixture.target.cell_id())
        .await?
        .ok_or("proof")?;
    let handle = runtime
        .acquire_idle_restored(
            proof,
            fixture.replica.clone(),
            authority,
            idle,
            fixture.root.path().join("b.sqlite"),
            Owner {
                session,
                endpoint: "https://owner-b.invalid".into(),
            },
        )
        .await?;
    let client = CellClient::local(Arc::clone(&fixture.registry), handle.clone());
    assert!(handle.owner_fence().epoch > old.token.owner.epoch);
    let replay = client
        .command::<BeginPreparation>(&fixture.target, mutation, input)
        .await?;
    assert_eq!(replay.output, first.output);
    assert_eq!(replay.receipt, first.receipt);
    rejected(
        client
            .command::<RenewPreparation>(&fixture.target, identity()?, request(old.token))
            .await,
        PreparationDenial::Stale,
    );
    let claimed = lease(
        client
            .command::<ClaimPreparation>(&fixture.target, identity()?, request(old.token))
            .await?
            .output,
    )?;
    assert_eq!(claimed.token.owner, handle.owner_fence());
    assert_eq!(old.token.artifact_operation, artifact_number(1));
    assert_eq!(claimed.token.artifact_operation, artifact_number(2));
    assert!(claimed.token.attempt > old.token.attempt);
    assert_eq!(counts(&handle).await?, (1, 2));
    assert!(
        client
            .query::<CheckPreparation>(&fixture.target, None, check(old.token))
            .await?
            .output
            .is_none()
    );
    assert!(
        client
            .query::<CheckPreparation>(&fixture.target, None, check(claimed.token))
            .await?
            .output
            .is_some()
    );
    let mut wrong = request(claimed.token);
    wrong.check.token.owner.incarnation = IncarnationId::from_bytes([99; 16]);
    rejected(
        client
            .command::<RenewPreparation>(&fixture.target, identity()?, wrong)
            .await,
        PreparationDenial::Stale,
    );
    rejected(
        client
            .command::<ClaimPreparation>(&fixture.target, identity()?, request(old.token))
            .await,
        PreparationDenial::Stale,
    );
    runtime.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn rebase_claim_keeps_the_previous_generation_pinned_and_revocation_stops_renewal() -> Result
{
    let fixture = Fixture::new(ObjectFormat::Sha256).await?;
    let first_catalog = fixture.install_empty_root(1).await?;
    let client = fixture.client();
    let first = lease(
        client
            .command::<BeginPreparation>(&fixture.target, identity()?, fixture.begin([23; 16]))
            .await?
            .output,
    )?;
    assert_eq!(first.base.catalog, Some(first_catalog));
    let second_catalog = fixture.install_empty_root(2).await?;
    let next = lease(
        client
            .command::<ClaimPreparation>(&fixture.target, identity()?, request(first.token))
            .await?
            .output,
    )?;
    assert_eq!(next.base.generation, 2);
    assert_eq!(next.base.catalog, Some(second_catalog));
    assert_eq!(fixture.counts().await?, (1, 2));
    // The old generation is neither current nor the operation's current base,
    // but its independent lease still forbids deleting its authoritative fact.
    assert!(
        fixture
            .handle
            .execute(
                identity()?,
                Digest::from_bytes([64; 32]),
                sql::now(0)?,
                1,
                0,
                |tx| {
                    tx.execute("DELETE FROM catalog_generations WHERE generation=1", [])?;
                    Ok(cellule_runtime::cell::executor::HandlerOutcome::Success(
                        Vec::new(),
                    ))
                }
            )
            .await
            .is_err()
    );
    let empty = fixture
        .handle
        .query(0, 8, |connection| {
            let count: u64 = connection.query_row(
                "SELECT count(*) FROM catalog_leases WHERE generation=1",
                [],
                |row| row.get(0),
            )?;
            Ok(count.to_be_bytes().to_vec())
        })
        .await?;
    assert_eq!(
        u64::from_be_bytes(empty.try_into().map_err(|_| "count")?),
        1
    );
    let maintenance = MaintenanceRequest {
        repository: fixture.repository,
        actor: "owner".into(),
        owner: fixture.handle.owner_fence(),
    };
    assert_eq!(
        client
            .command::<ReapPreparation>(&fixture.target, identity()?, maintenance.clone())
            .await?
            .output,
        0
    );
    fixture
        .handle
        .execute(
            identity()?,
            Digest::from_bytes([64; 32]),
            sql::now(0)?,
            1,
            0,
            |tx| {
                tx.execute(
                    "INSERT INTO repository_members(account,role) VALUES('writer','write')",
                    [],
                )?;
                Ok(cellule_runtime::cell::executor::HandlerOutcome::Success(
                    Vec::new(),
                ))
            },
        )
        .await?;
    let mut input = fixture.begin([24; 16]);
    input.actor = "writer".into();
    let writer = lease(
        client
            .command::<BeginPreparation>(&fixture.target, identity()?, input)
            .await?
            .output,
    )?;
    fixture
        .handle
        .execute(
            identity()?,
            Digest::from_bytes([64; 32]),
            sql::now(0)?,
            1,
            0,
            |tx| {
                tx.execute("DELETE FROM repository_members WHERE account='writer'", [])?;
                Ok(cellule_runtime::cell::executor::HandlerOutcome::Success(
                    Vec::new(),
                ))
            },
        )
        .await?;
    let mut renew = request(writer.token);
    renew.check.actor = "writer".into();
    rejected(
        client
            .command::<RenewPreparation>(&fixture.target, identity()?, renew)
            .await,
        PreparationDenial::Unauthorized,
    );
    let probe = LeaseCheck {
        token: writer.token,
        actor: "writer".into(),
    };
    assert!(
        client
            .query::<CheckPreparation>(&fixture.target, None, probe)
            .await?
            .output
            .is_none()
    );
    // Expire just the superseded pin. The live operation and its new base
    // remain intact; the reaper may now remove only the obsolete SQL fact.
    fixture
        .handle
        .execute(
            identity()?,
            Digest::from_bytes([65; 32]),
            sql::now(0)?,
            128,
            0,
            |tx| {
                tx.execute(
                    "UPDATE catalog_leases SET expires_at_ms=0 WHERE generation=1",
                    [],
                )?;
                Ok(cellule_runtime::cell::executor::HandlerOutcome::Success(
                    Vec::new(),
                ))
            },
        )
        .await?;
    assert_eq!(
        client
            .command::<ReapPreparation>(&fixture.target, identity()?, maintenance)
            .await?
            .output,
        2
    );
    let generations = fixture
        .handle
        .query(0, 16, |connection| {
            let old: u64 = connection.query_row(
                "SELECT count(*) FROM catalog_generations WHERE generation=1",
                [],
                |row| row.get(0),
            )?;
            let current: u64 = connection.query_row(
                "SELECT generation FROM catalog_state WHERE singleton=1",
                [],
                |row| row.get(0),
            )?;
            Ok([old.to_be_bytes(), current.to_be_bytes()].concat())
        })
        .await?;
    assert_eq!(
        generations,
        [0u64.to_be_bytes(), 2u64.to_be_bytes()].concat()
    );
    assert!(
        client
            .query::<CheckPreparation>(&fixture.target, None, check(next.token))
            .await?
            .output
            .is_some()
    );
    fixture.runtime.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn expired_attempts_cannot_renew_and_reaping_respects_bounded_indexed_work() -> Result {
    let fixture = Fixture::new(ObjectFormat::Sha1).await?;
    let client = fixture.client();
    let mut original_id = [0; 16];
    original_id[15] = 1;
    let started = lease(
        client
            .command::<BeginPreparation>(&fixture.target, identity()?, fixture.begin(original_id))
            .await?
            .output,
    )?;
    fixture
        .handle
        .execute(
            identity()?,
            Digest::from_bytes([64; 32]),
            sql::now(0)?,
            1,
            0,
            |tx| {
                tx.execute("UPDATE catalog_operations SET expires_at_ms=0", [])?;
                tx.execute("UPDATE catalog_leases SET expires_at_ms=0", [])?;
                Ok(cellule_runtime::cell::executor::HandlerOutcome::Success(
                    Vec::new(),
                ))
            },
        )
        .await?;
    rejected(
        client
            .command::<RenewPreparation>(&fixture.target, identity()?, request(started.token))
            .await,
        PreparationDenial::Expired,
    );
    assert!(
        client
            .query::<CheckPreparation>(&fixture.target, None, check(started.token))
            .await?
            .output
            .is_none()
    );
    let owner = fixture.handle.owner_fence();
    // Fixture preparation is bounded too: reuse parsed statements and avoid a
    // monolithic setup competing with unrelated native verifier test workers.
    // The full quota and the publisher/reaper deadlines below remain unchanged.
    for first in (1..MAX_OPERATIONS).step_by(REAP_ROWS as usize) {
        let last = (first + REAP_ROWS).min(MAX_OPERATIONS);
        fixture.handle.execute(
            identity()?, Digest::from_bytes([65; 32]), sql::now(0)?, 128, 0,
            move |tx| {
                let mut pin = tx.prepare("INSERT INTO catalog_leases(incarnation,admission_sequence,operation,owner_epoch,artifact_operation,generation,expires_at_ms) VALUES(?1,?2,?3,?4,?5,0,0)")?;
                let mut operation = tx.prepare("INSERT INTO catalog_operations(id,actor,request_digest,incarnation,owner_epoch,admission_sequence,artifact_operation,generation,expires_at_ms) VALUES(?1,'owner',?2,?3,?4,?5,?6,0,0)")?;
                for n in first..last {
                    let mut id = [0u8; 16];
                    id[..8].copy_from_slice(&n.to_be_bytes());
                    let seq = 1_000_000 + n as i64;
                    pin.execute(rusqlite::params![owner.incarnation.as_bytes().as_slice(), seq, id.as_slice(), owner.epoch.to_be_bytes().as_slice(), artifact_number(seq as u64).as_slice()])?;
                    operation.execute(rusqlite::params![id.as_slice(), [17u8; 32].as_slice(), owner.incarnation.as_bytes().as_slice(), owner.epoch.to_be_bytes().as_slice(), seq, artifact_number(seq as u64).as_slice()])?;
                }
                Ok(cellule_runtime::cell::executor::HandlerOutcome::Success(Vec::new()))
            },
        ).await?;
    }
    rejected(
        client
            .command::<BeginPreparation>(&fixture.target, identity()?, fixture.begin([26; 16]))
            .await,
        PreparationDenial::Capacity,
    );
    assert_eq!(fixture.counts().await?, (MAX_OPERATIONS, MAX_OPERATIONS));
    let removed = client
        .command::<ReapPreparation>(
            &fixture.target,
            identity()?,
            MaintenanceRequest {
                repository: fixture.repository,
                actor: "owner".into(),
                owner,
            },
        )
        .await?
        .output;
    assert_eq!(removed, REAP_ROWS * 2);
    assert_eq!(
        fixture.counts().await?,
        (MAX_OPERATIONS - REAP_ROWS, MAX_OPERATIONS - REAP_ROWS)
    );
    let fresh = lease(
        client
            .command::<BeginPreparation>(&fixture.target, identity()?, fixture.begin(original_id))
            .await?
            .output,
    )?;
    assert!(fresh.token.attempt > started.token.attempt);
    let plans=fixture.handle.query(0,4096,|connection|{
        let mut text=String::new();for sql in ["SELECT id FROM catalog_operations WHERE expires_at_ms<=0 ORDER BY expires_at_ms,id LIMIT 512","SELECT l.incarnation,l.admission_sequence FROM catalog_leases l WHERE l.expires_at_ms<=0 AND NOT EXISTS(SELECT 1 FROM catalog_operations o WHERE o.incarnation=l.incarnation AND o.admission_sequence=l.admission_sequence) ORDER BY l.expires_at_ms,l.incarnation,l.admission_sequence LIMIT 512"] {
            let mut statement=connection.prepare(&format!("EXPLAIN QUERY PLAN {sql}"))?;for row in statement.query_map([],|row|row.get::<_,String>(3))? {text.push_str(&row?);text.push('\n');}
        }Ok(text.into_bytes())
    }).await?;
    let plans = String::from_utf8(plans)?;
    assert!(
        plans.contains("catalog_operations_by_expiry")
            && plans.contains("catalog_leases_by_expiry")
            && plans.contains("catalog_operations_by_lease")
    );
    assert!(!plans.contains("TEMP B-TREE"));
    fixture.runtime.shutdown().await?;
    Ok(())
}

#[test]
fn fresh_schema_and_codecs_reject_incomplete_facts_and_support_full_owner_epochs() -> Result {
    let mut connection = rusqlite::Connection::open_in_memory()?;
    connection.execute_batch("PRAGMA foreign_keys=ON;")?;
    connection.execute_batch(SCHEMA)?;
    let legacy:u64=connection.query_row("SELECT count(*) FROM sqlite_schema WHERE name IN ('objects','object_edges','object_closure','object_uploads','object_chunks','git_packs','commit_parents','commit_ancestry')",[],|row|row.get(0))?;
    assert_eq!(legacy, 0);
    assert!(
        connection
            .execute(
                "INSERT INTO catalog_generations(generation,catalog,certificate) VALUES(1,NULL,zeroblob(32))",
                []
            )
            .is_err()
    );
    assert!(
        connection
            .execute(
                "INSERT INTO catalog_generations(generation,catalog,certificate) VALUES(1,zeroblob(1),NULL)",
                []
            )
            .is_err()
    );
    assert!(
        connection
            .execute(
                "UPDATE catalog_generations SET certificate=zeroblob(32) WHERE generation=0",
                []
            )
            .is_err()
    );
    for refs in [
        SqlValue::Text("x".into()),
        SqlValue::Blob(Vec::new()),
        SqlValue::Blob(vec![0; 129]),
    ] {
        let value = match refs {
            SqlValue::Text(value) => rusqlite::types::Value::Text(value),
            SqlValue::Blob(value) => rusqlite::types::Value::Blob(value),
            _ => unreachable!(),
        };
        assert!(connection.execute("INSERT INTO catalog_generations(generation,catalog,certificate,refs) VALUES(1,zeroblob(1),zeroblob(32),?1)", [value]).is_err());
    }
    connection.execute(
        "INSERT INTO catalog_generations(generation,catalog,certificate) VALUES(1,zeroblob(1),zeroblob(32))",
        [],
    )?;
    connection.execute(
        "INSERT INTO catalog_leases(incarnation,admission_sequence,operation,owner_epoch,artifact_operation,generation,expires_at_ms) VALUES(zeroblob(16),1,zeroblob(16),x'0000000000000001',x'43414e4f505930310000000000000001',0,100)",
        [],
    )?;
    let inconsistent = connection.transaction()?;
    inconsistent.execute("INSERT INTO catalog_operations(id,actor,request_digest,incarnation,owner_epoch,admission_sequence,artifact_operation,generation,expires_at_ms) VALUES(zeroblob(16),'owner',zeroblob(32),zeroblob(16),x'0000000000000001',1,x'43414e4f505930310000000000000001',1,100)",[])?;
    assert!(inconsistent.commit().is_err());
    // Retained SQL roots cannot grow with the complete publication history.
    // These opaque bytes deliberately test SQL constraints, not certification.
    let roots = connection.transaction()?;
    for generation in 2..MAX_RETAINED_GENERATIONS {
        roots.execute(
            "INSERT INTO catalog_generations(generation,catalog,certificate) VALUES(?1,zeroblob(1),zeroblob(32))",
            [generation as i64],
        )?;
    }
    roots.commit()?;
    assert!(
        connection
            .execute(
                "INSERT INTO catalog_generations(generation,catalog,certificate) VALUES(?1,zeroblob(1),zeroblob(32))",
                [MAX_RETAINED_GENERATIONS as i64],
            )
            .is_err()
    );
    let retained: u64 =
        connection.query_row("SELECT count(*) FROM catalog_generations", [], |row| {
            row.get(0)
        })?;
    assert_eq!(retained, MAX_RETAINED_GENERATIONS);
    // Even an expired independent floor protects every later fact until the
    // pin itself is removed; reaching capacity cannot bypass that retention.
    assert!(
        connection
            .execute("DELETE FROM catalog_generations WHERE generation=2", [])
            .is_err()
    );
    connection.execute("DELETE FROM catalog_leases", [])?;
    connection.execute("DELETE FROM catalog_generations WHERE generation=2", [])?;
    connection.execute(
        "INSERT INTO catalog_generations(generation,catalog,certificate) VALUES(?1,zeroblob(1),zeroblob(32))",
        [MAX_RETAINED_GENERATIONS as i64],
    )?;
    let token = PreparationToken {
        repository: uuid::Uuid::new_v4().into_bytes(),
        operation: [31; 16],
        artifact_operation: artifact_number(1),
        request_digest: [32; 32],
        owner: OwnerFence {
            incarnation: IncarnationId::from_bytes([33; 16]),
            epoch: u64::MAX,
        },
        attempt: 1,
    };
    let mut encoder = BoundedEncoder::new(4096)?;
    token.encode(&mut encoder)?;
    let bytes = encoder.finish();
    let mut decoder = BoundedDecoder::new(&bytes, 4096)?;
    assert_eq!(PreparationToken::decode(&mut decoder)?, token);
    decoder.finish()?;
    for length in 0..bytes.len() {
        let mut decoder = BoundedDecoder::new(&bytes[..length], 4096)?;
        assert!(PreparationToken::decode(&mut decoder).is_err());
    }
    let mut bad = token;
    bad.attempt = 0;
    assert!(bad.encode(&mut BoundedEncoder::new(4096)?).is_err());
    bad = token;
    bad.owner.epoch = 0;
    assert!(bad.encode(&mut BoundedEncoder::new(4096)?).is_err());
    for namespace in [[0; 16], artifact_number(0), artifact_number(u64::MAX)] {
        bad = token;
        bad.artifact_operation = namespace;
        assert!(bad.encode(&mut BoundedEncoder::new(4096)?).is_err());
    }
    Ok(())
}

#[tokio::test]
async fn foreign_catalog_facts_fail_before_a_preparation_or_pin_is_created() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let fixture = Fixture::new(format).await?;
        let catalog = fixture.install_empty_root(1).await?;
        for foreign in [
            StoredCatalog {
                repository: uuid::Uuid::new_v4().into_bytes(),
                ..catalog
            },
            StoredCatalog {
                format: if format == ObjectFormat::Sha1 {
                    ObjectFormat::Sha256
                } else {
                    ObjectFormat::Sha1
                },
                ..catalog
            },
        ] {
            // Each immutable fact is inserted separately. Trusted production
            // publication must reject it even earlier, before inserting it.
            let generation = if foreign.repository != fixture.repository {
                2
            } else {
                3
            };
            fixture.install_catalog(generation, foreign).await?;
            assert!(
                fixture
                    .client()
                    .command::<BeginPreparation>(
                        &fixture.target,
                        identity()?,
                        fixture.begin([35; 16]),
                    )
                    .await
                    .is_err()
            );
            assert_eq!(fixture.counts().await?, (0, 0));
        }
        fixture.runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn authoritative_base_resolution_uses_live_queried_facts_and_fences_failed_renewal_replay()
-> Result {
    use crate::packs::{
        catalog::{CatalogFileLimits, CatalogFiles},
        closure::{BaseResolver, ClosureError},
    };
    use cellule_ltx::DiskBudget;
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let fixture = Fixture::new(format).await?;
        let native =
            crate::packs::catalog::tests::prepared_for_repository(format, fixture.repository)
                .await?;
        fixture.install_catalog(1, native.stored).await?;
        let client = fixture.client();
        let started = client
            .command::<BeginPreparation>(&fixture.target, identity()?, fixture.begin([36; 16]))
            .await?;
        let granted = lease(started.output)?;
        let budget = DiskBudget::new(128 << 20);
        let files = Arc::new(CatalogFiles::new(
            fixture.root.path(),
            budget.clone(),
            Arc::clone(&native.store),
            format,
            CatalogFileLimits {
                open_files: 1,
                cached_files: 1,
                ..CatalogFileLimits::default()
            },
        )?);
        let resolver = PreparationBaseResolver::open(
            client.clone(),
            fixture.target.clone(),
            check(granted.token),
            Arc::clone(&native.indexes),
            Arc::clone(&files),
            Some(started.receipt),
        )
        .await?;
        let base = resolver.context().base.ok_or("base")?;
        assert_eq!(base.catalog, native.stored);
        assert_eq!(base.generation, 1);
        let native_ids: Vec<_> = native.fixture.objects.keys().copied().collect();
        let ids: Vec<_> = (0..512).map(|n| native_ids[n % native_ids.len()]).collect();
        let batch = resolver.resolve(base, &ids).await?;
        assert_eq!(batch.base, base);
        assert_eq!(batch.objects.len(), ids.len());
        for (oid, object) in ids.iter().zip(batch.objects) {
            let object = object.ok_or("base object")?;
            assert!(object.certified);
            assert_eq!(object.header.object, native.fixture.objects[oid].0);
        }
        let mut foreign = base;
        foreign.generation += 1;
        assert!(matches!(
            resolver.resolve(foreign, &ids).await,
            Err(ClosureError::Integrity)
        ));
        assert!(matches!(
            resolver.resolve(base, &vec![ids[0]; 513]).await,
            Err(ClosureError::Integrity)
        ));
        let renewal = identity()?;
        resolver.renew(renewal, DEFAULT_LEASE_MS).await?;
        fixture
            .handle
            .execute(
                identity()?,
                Digest::from_bytes([64; 32]),
                sql::now(0)?,
                128,
                0,
                |tx| {
                    tx.execute("UPDATE catalog_operations SET expires_at_ms=0", [])?;
                    tx.execute("UPDATE catalog_leases SET expires_at_ms=0", [])?;
                    Ok(cellule_runtime::cell::executor::HandlerOutcome::Success(
                        Vec::new(),
                    ))
                },
            )
            .await?;
        // The exact renewal RPC replays success, but the subsequent fresh query
        // sees expiry. It cannot restart a local deadline from the old reply.
        assert!(matches!(
            resolver.renew(renewal, DEFAULT_LEASE_MS).await,
            Err(PreparationBaseError::Inactive)
        ));
        assert!(matches!(
            resolver.resolve(base, &ids).await,
            Err(ClosureError::LeaseExpired)
        ));
        assert!(matches!(
            PreparationBaseResolver::open(
                client,
                fixture.target.clone(),
                check(granted.token),
                Arc::clone(&native.indexes),
                Arc::clone(&files),
                None
            )
            .await,
            Err(PreparationBaseError::Inactive)
        ));
        drop(resolver);
        drop(files);
        assert_eq!(budget.used(), 0);
        fixture.runtime.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn lease_quota_rejects_claim_without_mutating_the_existing_attempt() -> Result {
    let fixture = Fixture::new(ObjectFormat::Sha256).await?;
    let client = fixture.client();
    let started = lease(
        client
            .command::<BeginPreparation>(&fixture.target, identity()?, fixture.begin([37; 16]))
            .await?
            .output,
    )?;
    let owner = fixture.handle.owner_fence();
    let expires = started.expires_at_ms;
    for first in (1..MAX_GENERATION_LEASES).step_by(REAP_ROWS as usize) {
        let last = (first + REAP_ROWS).min(MAX_GENERATION_LEASES);
        fixture.handle.execute(
            identity()?, Digest::from_bytes([64; 32]), sql::now(0)?, 128, 0,
            move |tx| {
                let mut insert = tx.prepare("INSERT INTO catalog_leases(incarnation,admission_sequence,operation,owner_epoch,artifact_operation,generation,expires_at_ms) VALUES(?1,?2,zeroblob(16),?4,?5,0,?3)")?;
                for n in first..last {
                    insert.execute(rusqlite::params![
                        owner.incarnation.as_bytes().as_slice(),
                        1_000_000 + n as i64,
                        expires,
                        owner.epoch.to_be_bytes().as_slice(),
                        artifact_number(1_000_000 + n).as_slice()
                    ])?;
                }
                Ok(cellule_runtime::cell::executor::HandlerOutcome::Success(Vec::new()))
            },
        ).await?;
    }
    rejected(
        client
            .command::<ClaimPreparation>(&fixture.target, identity()?, request(started.token))
            .await,
        PreparationDenial::Capacity,
    );
    assert_eq!(fixture.counts().await?, (1, MAX_GENERATION_LEASES));
    let active = client
        .query::<CheckPreparation>(&fixture.target, None, check(started.token))
        .await?
        .output
        .ok_or("active")?;
    assert_eq!(active.token, started.token);
    assert_eq!(active.expires_at_ms, started.expires_at_ms);
    fixture.runtime.shutdown().await?;
    Ok(())
}

fn artifact_number(sequence: u64) -> [u8; 16] {
    let mut value = *b"CANOPY01\0\0\0\0\0\0\0\0";
    value[8..].copy_from_slice(&sequence.to_be_bytes());
    value
}
