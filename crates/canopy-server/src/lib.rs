//! Canopy repository state on one durable SQLite Cell per Git repository.

use std::sync::OnceLock;

use cellule_app::{ApplicationBuilder, ApplicationHandle, CellApplication, CellType};
use cellule_runtime::{
    ApplicationId, BuildDescriptor, CellModule, CellTarget, Digest, Error, MigrationDescriptor,
    ModuleDescriptor, NamespaceDescriptor, NamespaceId, RegistryBuilder, SqlCell, SqlModule,
    TenantId, cell::catalog::CatalogRole, primitives::sql::register_sql,
    registry::OperationDescriptor,
};

mod access;
mod admission;
mod api;
pub use admission::AdmissionPermit;
mod visibility;
pub use visibility::{RepositoryVisibility, Visibility};
pub mod branch_rules;
pub mod checks;
mod default_branch;
pub mod deployment;
pub mod directory;
pub(crate) use canopy_git_format as git_format;
pub(crate) use canopy_object_storage::external;
pub use git_format::{ObjectFormat, ObjectId, ObjectIdError, ObjectKind, object_id};
pub mod git;
mod graph;
pub mod http;
pub mod issues;
pub mod lfs;
pub mod native_resources;
pub mod packs;
pub mod pulls;
mod push;
mod refs;
pub mod server;
pub mod ssh;
mod transfer;
mod web;

pub use access::{COLLABORATOR_PAGE_SIZE, Collaborator, ReadIdentity};
pub use default_branch::DefaultBranch;
pub use push::{PushCertificateReceipt, PushError, PushReceipt, VerifiedPushCertificate};
pub use refs::{PushPlan, RefExpectation, RefPage, RefUpdate};

pub const REPOSITORIES: NamespaceId = NamespaceId::from_bytes([71; 16]);
pub const REPOSITORY_SCHEMA_VERSION: u32 = 4;
pub const INLINE_OBJECT_LIMIT: usize = 768 * 1024;
// SQLite's on-disk page count and maximum page size bound representable databases.
// This is a format boundary, not a repository quota.
pub const REPOSITORY_DATABASE_LIMIT_BYTES: u64 = (u32::MAX as u64 - 1) * 65_536;

pub(crate) fn replica_limits(database: u64, capture: u64) -> cellule_ltx::Limits {
    cellule_ltx::Limits {
        max_database_bytes: database,
        max_capture_bytes: capture,
        // LTX validates these against addressable index space. Snapshots and
        // recovery plans must not retain the dependency's small default quotas.
        max_file_bytes: (usize::MAX / 8) as u64,
        max_plan_bytes: (usize::MAX / 8) as u64,
        max_segments: usize::MAX / 8,
    }
}

/// Fresh packed schema shared by release migrations and actual Cell activation.
pub const REPOSITORY_SCHEMA: &str = packs::publication::SCHEMA;
const SCHEMA: &str = REPOSITORY_SCHEMA;
use packs::publication::registry::{COMMANDS, QUERIES};

const fn operation(id: u32) -> OperationDescriptor {
    operation_with_codec(id, 1)
}

const fn operation_with_codec(id: u32, codec_version: u32) -> OperationDescriptor {
    OperationDescriptor {
        id,
        codec_version,
        schema_min: REPOSITORY_SCHEMA_VERSION,
        schema_max: REPOSITORY_SCHEMA_VERSION,
        input_limit: 1 << 20,
        output_limit: 1 << 20,
    }
}

/// Derives one Repository Cell target from its canonical repository UUID.
pub fn repository_target(
    tenant: TenantId,
    application: ApplicationId,
    repository: [u8; 16],
) -> cellule_runtime::Result<CellTarget> {
    validate_repository_id(repository)?;
    CellTarget::new(
        tenant,
        application,
        REPOSITORIES,
        &repository_cell_type()?.entity_partition(&repository)?,
    )
}

pub(crate) fn validate_repository_id(repository: [u8; 16]) -> cellule_runtime::Result<()> {
    if !(1..=8).contains(&(repository[6] >> 4)) || repository[8] >> 6 != 2 {
        return Err(Error::Identity("repository UUID is not canonical"));
    }
    Ok(())
}

pub struct RepositoryModule;

impl SqlModule for RepositoryModule {
    const MODULE: &'static str = Self::NAME;
    const BATCH_COMMAND_ID: u32 = 1;
    const BATCH_QUERY_ID: u32 = 2;
}

impl CellModule for RepositoryModule {
    const NAME: &'static str = "repository";

    fn descriptor(&self) -> &'static ModuleDescriptor {
        static MIGRATIONS: OnceLock<[MigrationDescriptor; 1]> = OnceLock::new();
        static DESCRIPTOR: OnceLock<ModuleDescriptor> = OnceLock::new();
        DESCRIPTOR.get_or_init(|| ModuleDescriptor {
            name: Self::NAME,
            source_digest: {
                let mut source = blake3::Hasher::new();
                source.update(include_bytes!("lib.rs"));
                source.update(include_bytes!("deployment/mod.rs"));
                source.update(include_bytes!("deployment/maintenance.rs"));
                source.update(include_bytes!("deployment/retention.rs"));
                source.update(include_bytes!("deployment/recovery.rs"));
                source.update(include_bytes!("deployment/root.rs"));
                source.update(include_bytes!("deployment/backup/bodies.rs"));
                source.update(include_bytes!("deployment/backup/native.rs"));
                source.update(include_bytes!("packs/backup.rs"));
                source.update(include_bytes!("packs/directory/index/visit.rs"));
                source.update(include_bytes!("packs/publication/backup.rs"));
                source.update(include_bytes!("packs/publication/recovery/backup.rs"));
                source.update(include_bytes!("server/mod.rs"));
                source.update(include_bytes!("server/catalog_admission/mod.rs"));
                source.update(include_bytes!("server/lifecycle/mod.rs"));
                source.update(include_bytes!("admission.rs"));
                source.update(include_bytes!("server/workspace/mod.rs"));
                source.update(include_bytes!("../../canopy-git-format/src/lib.rs"));
                source.update(include_bytes!(
                    "../../canopy-git-format/src/pack_index/mod.rs"
                ));
                source.update(include_bytes!("refs.rs"));
                source.update(include_bytes!("default_branch.rs"));
                source.update(include_bytes!("graph/mod.rs"));
                source.update(include_bytes!("branch_rules/mod.rs"));
                source.update(include_bytes!("branch_rules/command.rs"));
                source.update(include_bytes!("git/mod.rs"));
                source.update(include_bytes!("git/objects/mod.rs"));
                source.update(include_bytes!("git/cache/artifacts.rs"));
                source.update(include_bytes!("git/cache/verified.rs"));
                source.update(include_bytes!("git/cache/serving_refs.rs"));
                source.update(include_bytes!("git/cache/cleanup.rs"));
                source.update(include_bytes!("git/cache/mod.rs"));
                source.update(include_bytes!("git/cache/maintenance.rs"));
                source.update(include_bytes!("packs/catalog/native/mod.rs"));
                source.update(include_bytes!("packs/catalog/sparse.rs"));
                source.update(include_bytes!("packs/catalog/reader.rs"));
                source.update(include_bytes!("packs/catalog/graph_spool/mod.rs"));
                source.update(include_bytes!("packs/catalog/files.rs"));
                source.update(include_bytes!("native_resources/mod.rs"));
                source.update(include_bytes!("git/native/mod.rs"));
                source.update(include_bytes!("git/native/process.rs"));
                source.update(include_bytes!("git/native/process/fence.rs"));
                source.update(include_bytes!("git/gateway/mod.rs"));
                source.update(include_bytes!("git/gateway/merge.rs"));
                source.update(include_bytes!("git/gateway/head.rs"));
                source.update(include_bytes!("git/gateway/candidates/mod.rs"));
                source.update(include_bytes!("git/gateway/fetch.rs"));
                source.update(include_bytes!("git/gateway/discovery.rs"));
                source.update(include_bytes!("git/gateway/ssh.rs"));
                source.update(include_bytes!("git/gateway/preflight.rs"));
                source.update(include_bytes!("git/gateway/preflight/retention.rs"));
                source.update(include_bytes!("git/gateway/branch_policy.rs"));
                source.update(include_bytes!("git/gateway/push.rs"));
                source.update(include_bytes!("git/gateway/push/native.rs"));
                source.update(include_bytes!("git/input/mod.rs"));
                source.update(include_bytes!("git/http/capture.rs"));
                source.update(include_bytes!("git/http/mod.rs"));
                source.update(include_bytes!("packs/verification/mod.rs"));
                source.update(include_bytes!("packs/verification/physical.rs"));
                source.update(include_bytes!("packs/verification/physical/staged/mod.rs"));
                source.update(include_bytes!("packs/closure/verifier.rs"));
                source.update(include_bytes!("packs/directory/mod.rs"));
                source.update(include_bytes!("packs/directory/writer.rs"));
                source.update(include_bytes!("packs/publication/prepare.rs"));
                source.update(include_bytes!("packs/verification/spool/mod.rs"));
                source.update(include_bytes!("packs/wire_request.rs"));
                source.update(include_bytes!("packs/input_artifact.rs"));
                source.update(include_bytes!("packs/directory/index/mod.rs"));
                source.update(include_bytes!("packs/directory/index/record.rs"));
                source.update(include_bytes!("packs/directory/index/codec.rs"));
                source.update(include_bytes!("packs/directory/index/cursor.rs"));
                source.update(include_bytes!("packs/directory/index/changes.rs"));
                source.update(include_bytes!("packs/directory/index/update.rs"));
                source.update(include_bytes!("packs/directory/index/bulk.rs"));
                source.update(include_bytes!("packs/directory/index/rewrite.rs"));
                source.update(include_bytes!("packs/sources/codec.rs"));
                source.update(include_bytes!("packs/sources/inputs.rs"));
                source.update(include_bytes!("packs/ref_state/mod.rs"));
                source.update(include_bytes!("packs/ref_state/record.rs"));
                source.update(include_bytes!("packs/ref_state/transition.rs"));
                source.update(include_bytes!("packs/ref_state/snapshot.rs"));
                source.update(include_bytes!("packs/publication/native_result.rs"));
                source.update(include_bytes!("packs/publication/native_result/codec.rs"));
                source.update(include_bytes!(
                    "packs/publication/native_result/plan/mod.rs"
                ));
                source.update(include_bytes!("packs/publication/root_completion/mod.rs"));
                source.update(include_bytes!("packs/publication/root_completion/codec.rs"));
                source.update(include_bytes!(
                    "packs/publication/root_completion/publish.rs"
                ));
                source.update(include_bytes!("packs/publication/root_completion/read.rs"));
                source.update(include_bytes!(
                    "packs/publication/root_completion/ref_free.rs"
                ));
                source.update(include_bytes!(
                    "packs/publication/root_completion/result.rs"
                ));
                source.update(include_bytes!("packs/publication/outcome.rs"));
                source.update(include_bytes!("packs/publication/commands.rs"));
                source.update(include_bytes!(
                    "packs/publication/root_completion/prepare.rs"
                ));
                source.update(include_bytes!(
                    "packs/publication/root_completion/outcome.rs"
                ));
                source.update(include_bytes!("packs/publication/completion.rs"));
                source.update(include_bytes!("packs/publication/ref_proof.rs"));
                source.update(include_bytes!("packs/publication/ref_snapshot.rs"));
                source.update(include_bytes!("packs/publication/native_candidate.rs"));
                source.update(include_bytes!("packs/publication/native_merge.rs"));
                source.update(include_bytes!("packs/publication/native_head.rs"));
                source.update(include_bytes!(
                    "packs/publication/candidate_publication/mod.rs"
                ));
                source.update(include_bytes!(
                    "packs/publication/candidate_publication/publish.rs"
                ));
                source.update(include_bytes!(
                    "packs/publication/candidate_publication/audit.rs"
                ));
                source.update(include_bytes!(
                    "packs/publication/coordinator/native_candidate.rs"
                ));
                source.update(include_bytes!("git/gateway/candidates/produce.rs"));
                source.update(include_bytes!("packs/publication/native_head/publish.rs"));
                source.update(include_bytes!(
                    "packs/publication/coordinator/native_head.rs"
                ));
                source.update(include_bytes!("packs/publication/native_merge/audit.rs"));
                source.update(include_bytes!(
                    "packs/publication/coordinator/native_merge.rs"
                ));
                source.update(include_bytes!("packs/publication/initialization.rs"));
                source.update(include_bytes!(
                    "packs/publication/coordinator/initialization.rs"
                ));
                source.update(include_bytes!(
                    "packs/publication/recovery/initialization.rs"
                ));
                source.update(include_bytes!("packs/publication/ref_policy/mod.rs"));
                source.update(include_bytes!("packs/publication/ref_policy/codec.rs"));
                source.update(include_bytes!("packs/publication/ref_policy/commands.rs"));
                source.update(include_bytes!("packs/publication/ref_policy/prepare.rs"));
                source.update(include_bytes!("packs/publication/ref_policy/schema.sql"));
                source.update(include_bytes!(
                    "packs/publication/initialization/publish.rs"
                ));
                source.update(include_bytes!("packs/publication/staging_service.rs"));
                source.update(include_bytes!(
                    "packs/publication/staging_service/driver.rs"
                ));
                source.update(include_bytes!("packs/publication/staging_receipt.rs"));
                source.update(include_bytes!("packs/publication/admission_receipt.rs"));
                source.update(include_bytes!("packs/publication/custody/mod.rs"));
                source.update(include_bytes!("packs/publication/custody/codec.rs"));
                source.update(include_bytes!("packs/publication/custody/commands.rs"));
                source.update(include_bytes!("packs/publication/custody/dispatch.rs"));
                source.update(include_bytes!("packs/publication/preparation_receipt.rs"));
                source.update(include_bytes!("packs/publication/coordinator.rs"));
                source.update(include_bytes!(
                    "packs/publication/coordinator/serving_drain.rs"
                ));
                source.update(include_bytes!(
                    "packs/publication/coordinator/budget/mod.rs"
                ));
                source.update(include_bytes!("packs/publication/scan/mod.rs"));
                source.update(include_bytes!("packs/publication/custody/scan.rs"));
                source.update(include_bytes!("packs/publication/coordinator/policy.rs"));
                source.update(include_bytes!("packs/publication/coordinator/roots.rs"));
                source.update(include_bytes!("packs/publication/coordinator/work.rs"));
                source.update(include_bytes!("packs/publication/coordinator/recovery.rs"));
                source.update(include_bytes!(
                    "packs/publication/staging_service/publication.rs"
                ));
                source.update(include_bytes!("packs/publication/recovery/mod.rs"));
                source.update(include_bytes!("packs/publication/recovery/codec.rs"));
                source.update(include_bytes!("packs/publication/recovery/registration.rs"));
                source.update(include_bytes!("packs/publication/recovery/ready.rs"));
                source.update(include_bytes!("packs/publication/recovery/supervisor.rs"));
                source.update(include_bytes!("packs/publication/recovery/phase.rs"));
                source.update(include_bytes!("packs/publication/recovery/archive.rs"));
                source.update(include_bytes!(
                    "packs/publication/root_completion/retention.rs"
                ));
                source.update(include_bytes!("packs/publication/exact.rs"));
                source.update(include_bytes!("packs/publication/mod.rs"));
                source.update(include_bytes!("packs/publication/serving.rs"));
                source.update(include_bytes!("packs/publication/serving/codec.rs"));
                source.update(include_bytes!("packs/publication/serving/commands.rs"));
                source.update(include_bytes!("packs/publication/serving/command_owner.rs"));
                source.update(include_bytes!("packs/publication/serving/session.rs"));
                source.update(include_bytes!("packs/publication/serving/session/reads.rs"));
                source.update(include_bytes!("packs/publication/serving/session/body.rs"));
                source.update(include_bytes!("packs/publication/serving/session/edges.rs"));
                source.update(include_bytes!(
                    "packs/publication/serving/session/workspace.rs"
                ));
                source.update(include_bytes!(
                    "packs/publication/serving/session/workspace/prepare.rs"
                ));
                source.update(include_bytes!(
                    "packs/publication/serving/session/native_base.rs"
                ));
                source.update(include_bytes!("git/read/mod.rs"));
                source.update(include_bytes!("git/read/browse.rs"));
                source.update(include_bytes!("git/read/graph.rs"));
                source.update(include_bytes!("git/read/trees.rs"));
                source.update(include_bytes!("git/read/patch/mod.rs"));
                source.update(include_bytes!("packs/publication/serving/session/refs.rs"));
                source.update(include_bytes!("packs/publication/serving/lifecycle.rs"));
                source.update(include_bytes!("packs/publication/serving/pool.rs"));
                source.update(include_bytes!(
                    "packs/publication/serving/session/handoff.rs"
                ));
                source.update(include_bytes!("packs/publication/serving/ownership.rs"));
                source.update(include_bytes!("packs/publication/serving/schema.sql"));
                source.update(include_bytes!("packs/publication/ref_observation.rs"));
                source.update(include_bytes!(
                    "packs/publication/ref_observation/selection.rs"
                ));
                source.update(include_bytes!(
                    "packs/publication/serving/session/ref_observation.rs"
                ));
                source.update(include_bytes!("pulls/native/mod.rs"));
                source.update(include_bytes!("pulls/native/codec.rs"));
                source.update(include_bytes!("pulls/native/client.rs"));
                source.update(include_bytes!("pulls/native/threads.rs"));
                source.update(include_bytes!("pulls/threads.rs"));
                source.update(include_bytes!("pulls/native/reads.rs"));
                source.update(include_bytes!("packs/publication/registry.rs"));
                source.update(include_bytes!("server/catalog_initialization/mod.rs"));
                source.update(include_bytes!("server/residency/mod.rs"));
                source.update(include_bytes!("server/residency/recovery.rs"));
                source.update(include_bytes!("server/peer.rs"));
                source.update(include_bytes!("packs/publication/codec.rs"));
                source.update(include_bytes!("packs/publication/sql.rs"));
                source.update(include_bytes!("packs/publication/schema.sql"));
                source.update(include_bytes!("packs/publication/certificate.rs"));
                source.update(include_bytes!("packs/publication/publish.rs"));
                source.update(include_bytes!("packs/publication/compaction/publish.rs"));
                source.update(include_bytes!("packs/metadata/transport.rs"));
                source.update(include_bytes!("packs/publication/inputs.rs"));
                source.update(include_bytes!(
                    "../../canopy-object-storage/src/artifact/mod.rs"
                ));
                source.update(include_bytes!(
                    "../../canopy-object-storage/src/external.rs"
                ));
                source.update(include_bytes!("push/mod.rs"));
                source.update(include_bytes!("push/report.rs"));
                source.update(include_bytes!("access.rs"));
                source.update(include_bytes!("visibility.rs"));
                source.update(include_bytes!("checks/native.rs"));
                source.update(include_bytes!("checks/native/codec.rs"));
                source.update(include_bytes!("packs/publication/commit_membership.rs"));
                source.update(include_bytes!(
                    "packs/publication/serving/session/membership.rs"
                ));
                source.update(include_bytes!("checks/mod.rs"));
                source.update(include_bytes!("checks/mutations.rs"));
                source.update(include_bytes!("pulls/mod.rs"));
                source.update(include_bytes!("pulls/merge/mod.rs"));
                source.update(include_bytes!("pulls/candidates/mod.rs"));
                source.update(include_bytes!("pulls/candidates/rebase.rs"));
                source.update(include_bytes!("pulls/candidates/command.rs"));
                source.update(include_bytes!("pulls/merge/command.rs"));
                source.update(include_bytes!("pulls/mutations.rs"));
                source.update(include_bytes!("issues/mod.rs"));
                source.update(include_bytes!("issues/mutations.rs"));
                source.update(include_bytes!("lfs/mod.rs"));
                source.update(include_bytes!("lfs/locks.rs"));
                source.update(SCHEMA.as_bytes());
                Digest::from_bytes(*source.finalize().as_bytes())
            },
            retained_codes: &[],
            schema_min: REPOSITORY_SCHEMA_VERSION,
            schema_max: REPOSITORY_SCHEMA_VERSION,
            migrations: MIGRATIONS.get_or_init(|| {
                [MigrationDescriptor {
                    version: REPOSITORY_SCHEMA_VERSION,
                    sql: SCHEMA,
                    digest: Digest::from_bytes(*blake3::hash(SCHEMA.as_bytes()).as_bytes()),
                }]
            }),
            commands: &COMMANDS,
            queries: &QUERIES,
            workflow_definitions: &[],
            activity_types: &[],
            namespaces: &[NamespaceDescriptor {
                id: REPOSITORIES,
                name: Self::NAME,
                role: CatalogRole::Sql,
                shards: 1,
                effect_targets: &[],
                dead_letter: None,
            }],
        })
    }

    fn register(self, registry: &mut RegistryBuilder) -> cellule_runtime::Result<()> {
        register_sql::<Self>(registry)?;
        packs::publication::register(registry)
    }
}

pub struct CanopyApplication;

impl CellApplication for CanopyApplication {
    const NAME: &'static str = "canopy";

    fn register(builder: &mut ApplicationBuilder) -> cellule_runtime::Result<()> {
        builder.register(directory::DirectoryModule)?;
        builder.register(RepositoryModule)?;
        builder.cell_type(directory::cell_type()?)?;
        builder.cell_type(repository_cell_type()?)
    }
}

fn repository_cell_type() -> cellule_runtime::Result<CellType> {
    CellType::new(
        RepositoryModule::NAME,
        "repository",
        REPOSITORIES,
        CatalogRole::Sql,
        1,
    )?
    .with_entity_partitions()?
    .with_schema_range(REPOSITORY_SCHEMA_VERSION, REPOSITORY_SCHEMA_VERSION)?
    .with_limits(REPOSITORY_DATABASE_LIMIT_BYTES, 64 * 1024 * 1024)
}

/// Product-owned capability for one repository's SQLite state.
pub struct RepositoryCell {
    id: [u8; 16],
    object_format: ObjectFormat,
    sql: SqlCell<RepositoryModule>,
    application: ApplicationHandle<CanopyApplication>,
    target: CellTarget,
    serving: std::sync::Mutex<Option<std::sync::Weak<packs::publication::ServingPool>>>,
    staging: std::sync::Mutex<Option<std::sync::Weak<packs::publication::StagingCoordinator>>>,
}

impl RepositoryCell {
    pub fn object_format(&self) -> ObjectFormat {
        self.object_format
    }

    pub fn repository_id(&self) -> [u8; 16] {
        self.id
    }

    pub fn new(
        application: &ApplicationHandle<CanopyApplication>,
        target: CellTarget,
        id: [u8; 16],
        object_format: ObjectFormat,
    ) -> cellule_runtime::Result<Self> {
        if target != repository_target(target.tenant(), target.application(), id)? {
            return Err(Error::Identity("repository UUID differs from Cell target"));
        }
        Ok(Self {
            id,
            object_format,
            sql: application.sql::<RepositoryModule>(target.clone())?,
            application: application.clone(),
            target,
            serving: std::sync::Mutex::new(None),
            staging: std::sync::Mutex::new(None),
        })
    }

    pub(crate) fn attach_serving(&self, pool: &std::sync::Arc<packs::publication::ServingPool>) {
        *self.serving.lock().expect("repository serving pool") =
            Some(std::sync::Arc::downgrade(pool));
    }
    pub(crate) fn attach_staging(
        &self,
        staging: &std::sync::Arc<packs::publication::StagingCoordinator>,
    ) {
        *self.staging.lock().expect("repository staging coordinator") =
            Some(std::sync::Arc::downgrade(staging));
    }
    /// Obtain the resident's owned write lifecycle. Admission still checks live
    /// custody and actor limits; retaining this handle cannot keep a Cell resident.
    pub fn staging_coordinator(
        &self,
    ) -> Result<
        std::sync::Arc<packs::publication::StagingCoordinator>,
        packs::publication::StagingError,
    > {
        let staging = self
            .staging
            .lock()
            .expect("repository staging coordinator")
            .as_ref()
            .and_then(std::sync::Weak::upgrade)
            .ok_or(packs::publication::StagingError::Inactive)?;
        if staging.stats().closed {
            return Err(packs::publication::StagingError::Closed);
        }
        Ok(staging)
    }
    /// Borrow the resident's certified joint generation; a detached caller
    /// cannot abandon its acquisition or extend a released residency.
    pub async fn serving_snapshot(
        &self,
        actor: ReadIdentity<'_>,
    ) -> std::result::Result<
        packs::publication::ServingSnapshot,
        packs::publication::ServingOwnerError,
    > {
        actor
            .validate()
            .map_err(packs::publication::ServingReadError::Capability)?;
        let pool = self
            .serving
            .lock()
            .expect("repository serving pool")
            .as_ref()
            .and_then(std::sync::Weak::upgrade)
            .ok_or(packs::publication::ServingReadError::Inactive)?;
        let actor = match actor {
            ReadIdentity::Anonymous => None,
            ReadIdentity::Account(value) => Some(value.to_owned()),
        };
        pool.snapshot(actor).await
    }
}

/// Build evidence for the statically linked Canopy Cell application.
pub fn build_descriptor(lock: &[u8], revision: &str) -> BuildDescriptor {
    BuildDescriptor {
        source_revision: revision.into(),
        cargo_lock_digest: Digest::from_bytes(*blake3::hash(lock).as_bytes()),
    }
}
