//! Canopy repository state on one durable SQLite Cell per Git repository.

use std::sync::OnceLock;

use cellule_app::{ApplicationBuilder, ApplicationHandle, CellApplication, CellType};
use cellule_runtime::{
    ApplicationId, BuildDescriptor, CellModule, CellTarget, Committed, Digest, Error,
    MigrationDescriptor, ModuleDescriptor, NamespaceDescriptor, NamespaceId, Observed, Receipt,
    RegistryBuilder, SqlCell, SqlModule, TenantId, cell::catalog::CatalogRole,
    primitives::sql::SqlBatch, primitives::sql::SqlStatement, primitives::sql::SqlValue,
    primitives::sql::register_sql, registry::OperationDescriptor,
};

mod access;
mod admission;
pub use admission::AdmissionPermit;
mod visibility;
pub use visibility::{RepositoryVisibility, Visibility};
mod ancestry;
pub mod branch_rules;
pub mod checks;
mod default_branch;
pub mod deployment;
pub mod directory;
mod external;
mod git_cache;
mod git_format;
pub use git_format::{ObjectFormat, ObjectId, ObjectIdError, object_id};
pub mod git_gateway;
pub mod git_http;
pub mod git_input;
mod git_objects;
pub mod git_pack;
mod git_read;
mod graph;
pub mod http;
pub mod issues;
pub mod lfs;
mod native_git;
mod object_batch;
mod object_packs;
mod object_reads;
pub mod pulls;
mod push;
mod refs;
mod repository_http;
pub mod server;
pub mod ssh;
mod transfer;
mod web;

pub use access::{COLLABORATOR_PAGE_SIZE, Collaborator, ReadIdentity};
pub use default_branch::DefaultBranch;
pub use object_batch::ObjectBatch;
pub use object_packs::{ObjectStageError, PackedObject};
pub use push::{PushCertificateReceipt, PushError, PushReceipt};
pub use refs::{FinalizePush, PushPlan, RefExpectation, RefPage, RefReadError, RefUpdate};

pub const REPOSITORIES: NamespaceId = NamespaceId::from_bytes([71; 16]);
pub const REPOSITORY_SCHEMA_VERSION: u32 = 3;
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

const SCHEMA: &str = include_str!("schema.sql");
const COMMANDS: [OperationDescriptor; 9] = [
    operation(1),
    operation_with_codec(3, 4),
    operation_with_codec(4, 6),
    OperationDescriptor {
        input_limit: object_batch::INPUT_LIMIT,
        ..operation_with_codec(5, 4)
    },
    operation_with_codec(6, 4),
    operation_with_codec(7, 2),
    operation_with_codec(8, 2),
    operation_with_codec(9, 4),
    operation_with_codec(10, 2),
];
const QUERIES: [OperationDescriptor; 1] = [operation(2)];

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

/// Git object kind used when calculating the canonical object ID.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum ObjectKind {
    Blob,
    Tree,
    Commit,
    Tag,
}

impl ObjectKind {
    pub const fn git_name(self) -> &'static str {
        match self {
            Self::Blob => "blob",
            Self::Tree => "tree",
            Self::Commit => "commit",
            Self::Tag => "tag",
        }
    }
}

/// In-memory ingress or a verified immutable native pack reference.
/// `Input` is never persisted; only `Packed` appears in stored object pages.
pub enum ObjectStorage {
    Input(Vec<u8>),
    Packed(Box<PackedObject>),
}

/// Immutable Git object returned by one bounded Repository Cell read.
pub struct StoredObject {
    pub oid: crate::ObjectId,
    pub kind: ObjectKind,
    pub storage: ObjectStorage,
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
                source.update(include_bytes!("git_format.rs"));
                source.update(include_bytes!("git_pack.rs"));
                source.update(include_bytes!("git_pack/store.rs"));
                source.update(include_bytes!("git_pack/build.rs"));
                source.update(include_bytes!("refs.rs"));
                source.update(include_bytes!("default_branch.rs"));
                source.update(include_bytes!("graph.rs"));
                source.update(include_bytes!("ancestry.rs"));
                source.update(include_bytes!("branch_rules.rs"));
                source.update(include_bytes!("branch_rules/command.rs"));
                source.update(include_bytes!("graph/preparation.rs"));
                source.update(include_bytes!("graph/stream.rs"));
                source.update(include_bytes!("object_batch.rs"));
                source.update(include_bytes!("object_packs.rs"));
                source.update(include_bytes!("object_reads.rs"));
                source.update(include_bytes!("external.rs"));
                source.update(include_bytes!("push.rs"));
                source.update(include_bytes!("push/plan.rs"));
                source.update(include_bytes!("push/report.rs"));
                source.update(include_bytes!("access.rs"));
                source.update(include_bytes!("visibility.rs"));
                source.update(include_bytes!("checks.rs"));
                source.update(include_bytes!("checks/mutations.rs"));
                source.update(include_bytes!("pulls.rs"));
                source.update(include_bytes!("pulls/merge.rs"));
                source.update(include_bytes!("pulls/candidates.rs"));
                source.update(include_bytes!("pulls/candidates/rebase.rs"));
                source.update(include_bytes!("pulls/candidates/command.rs"));
                source.update(include_bytes!("pulls/merge/command.rs"));
                source.update(include_bytes!("pulls/mutations.rs"));
                source.update(include_bytes!("issues.rs"));
                source.update(include_bytes!("issues/mutations.rs"));
                source.update(include_bytes!("lfs.rs"));
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
        registry.bind_command::<FinalizePush>()?;
        registry.bind_command::<push::CompletePush>()?;
        registry.bind_command::<object_batch::PutObjects>()?;
        registry.bind_command::<graph::CertifyObjects>()?;
        registry.bind_command::<ancestry::CertifyAncestry>()?;
        registry.bind_command::<branch_rules::command::SetBranchRule>()?;
        registry.bind_command::<pulls::merge::command::MergePull>()?;
        registry.bind_command::<pulls::candidates::command::PrepareCandidate>()
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
    packs: git_pack::PackStore,
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
        packs: git_pack::PackStore,
    ) -> cellule_runtime::Result<Self> {
        if target != repository_target(target.tenant(), target.application(), id)? {
            return Err(Error::Identity("repository UUID differs from Cell target"));
        }
        if packs.repository_id() != id {
            return Err(Error::Identity("pack store UUID differs from repository"));
        }
        Ok(Self {
            id,
            object_format,
            sql: application.sql::<RepositoryModule>(target.clone())?,
            application: application.clone(),
            target,
            packs,
        })
    }

    /// Prepares bounded graph certificates, then publishes one all-or-none ref plan.
    pub async fn finalize_push(
        &self,
        identity: cellule_runtime::MutationIdentity,
        plan: PushPlan,
    ) -> std::result::Result<Committed<bool>, cellule_runtime::InvocationError<bool>> {
        self.prepare_graph(&plan).await?;
        self.prepare_branch_proofs(&plan).await?;
        self.application
            .command::<FinalizePush>(&self.target, identity, plan)
            .await
    }

    pub async fn object(
        &self,
        oid: crate::ObjectId,
        minimum: Option<Receipt>,
    ) -> std::result::Result<
        Observed<Option<(ObjectKind, Vec<u8>)>>,
        cellule_runtime::InvocationError<Vec<cellule_runtime::primitives::sql::SqlResultSet>>,
    > {
        let headers = self
            .sql
            .query(
                minimum,
                SqlBatch {
                    statements: vec![SqlStatement {
                        sql: "SELECT oid FROM objects WHERE oid = ?1".into(),
                        parameters: vec![SqlValue::Blob(oid.to_vec())],
                    }],
                },
            )
            .await?;
        if headers.output.first().is_none_or(|set| set.rows.is_empty()) {
            return Ok(Observed {
                output: None,
                receipt: headers.receipt,
            });
        }
        let objects = self
            .object_records(Observed {
                output: vec![oid],
                receipt: headers.receipt,
            })
            .await?;
        let object = objects.output.into_iter().next().ok_or_else(|| {
            cellule_runtime::InvocationError::NotStarted(Error::Command("missing packed object"))
        })?;
        let ObjectStorage::Packed(reference) = object.storage else {
            return Err(cellule_runtime::InvocationError::NotStarted(
                Error::Command("invalid persisted storage"),
            ));
        };
        let pack = self
            .packs
            .load(&reference.pack)
            .await
            .map_err(object_packs::pack_invocation)?;
        let bytes = tokio::task::spawn_blocking(move || {
            let (kind, body) = pack
                .data()?
                .object(oid, 64 * 1024 * 1024)?
                .ok_or(git_pack::PackError::Integrity)?;
            if kind != object.kind
                || body.len() as u64 != reference.size
                || blake3::hash(&body).as_bytes() != &reference.digest
            {
                return Err(git_pack::PackError::Integrity);
            }
            Ok((kind, body))
        })
        .await
        .map_err(|error| object_packs::pack_invocation(git_pack::PackError::Task(error)))?
        .map_err(object_packs::pack_invocation)?;
        Ok(Observed {
            output: Some(bytes),
            receipt: objects.receipt,
        })
    }
}

/// Build evidence for the statically linked Canopy Cell application.
pub fn build_descriptor(lock: &[u8], revision: &str) -> BuildDescriptor {
    BuildDescriptor {
        source_revision: revision.into(),
        cargo_lock_digest: Digest::from_bytes(*blake3::hash(lock).as_bytes()),
    }
}
