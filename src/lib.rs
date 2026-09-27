//! Canopy repository state on one durable SQLite Cell per Git repository.

use std::sync::OnceLock;

use cellule_app::{ApplicationBuilder, ApplicationHandle, CellApplication, CellType};
use cellule_runtime::{
    ApplicationId, BuildDescriptor, CatalogRole, CellModule, CellTarget, Committed, Digest, Error,
    MigrationDescriptor, ModuleDescriptor, NamespaceDescriptor, NamespaceId, Observed,
    OperationDescriptor, Receipt, RegistryBuilder, SqlBatch, SqlCell, SqlModule, SqlStatement,
    SqlValue, TenantId, register_sql,
};
use sha1::{Digest as _, Sha1};

mod access;
mod visibility;
pub use visibility::{RepositoryVisibility, Visibility};
mod ancestry;
pub mod branch_rules;
pub mod checks;
mod default_branch;
pub mod deployment;
pub mod directory;
mod git_cache;
pub mod git_gateway;
pub mod git_http;
pub mod git_input;
mod git_objects;
mod git_read;
mod graph;
pub mod http;
pub mod issues;
pub mod large_blob;
pub mod lfs;
mod native_git;
mod object_batch;
mod object_chunks;
mod object_reads;
pub mod pulls;
mod push;
mod refs;
mod repository_http;
pub mod server;
mod transfer;
mod web;

pub use access::{COLLABORATOR_PAGE_SIZE, Collaborator, ReadIdentity};
pub use default_branch::DefaultBranch;
pub use object_batch::ObjectBatch;
pub use object_chunks::{MAX_SQLITE_OBJECT_BYTES, ObjectStageError};
pub use push::PushError;
pub use refs::{FinalizePush, PushPlan, RefExpectation, RefPage, RefReadError, RefUpdate};

pub const REPOSITORIES: NamespaceId = NamespaceId::from_bytes([71; 16]);
pub const INLINE_OBJECT_LIMIT: usize = 768 * 1024;
pub const REPOSITORY_DATABASE_LIMIT_BYTES: u64 = 512 * 1024 * 1024;

const SCHEMA: &str = include_str!("schema.sql");
const COMMANDS: [OperationDescriptor; 9] = [
    operation(1),
    operation_with_codec(3, 3),
    operation(4),
    operation_with_codec(5, 2),
    operation(6),
    operation(7),
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
        schema_min: 1,
        schema_max: 1,
        input_limit: 1 << 20,
        output_limit: 1 << 20,
    }
}

/// The repository UUID is the complete partition key of one Repository Cell.
pub fn repository_target(
    tenant: TenantId,
    application: ApplicationId,
    repository: [u8; 16],
) -> cellule_runtime::Result<CellTarget> {
    validate_repository_id(repository)?;
    CellTarget::new(tenant, application, REPOSITORIES, &repository)
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

/// Byte location for a verified Git object record.
pub enum ObjectStorage {
    Inline(Vec<u8>),
    Chunked {
        upload: [u8; 16],
        size: u64,
        blake3: [u8; 32],
    },
    External {
        size: u64,
        blake3: [u8; 32],
        sha256: [u8; 32],
    },
}

/// Immutable Git object returned by one bounded Repository Cell read.
pub struct StoredObject {
    pub oid: [u8; 20],
    pub kind: ObjectKind,
    pub storage: ObjectStorage,
}

/// Computes the SHA-1 Git object ID from canonical type, length and bytes.
#[must_use]
pub fn object_id(kind: ObjectKind, body: &[u8]) -> [u8; 20] {
    let mut sha = Sha1::new();
    sha.update(kind.git_name().as_bytes());
    sha.update(b" ");
    sha.update(body.len().to_string().as_bytes());
    sha.update([0]);
    sha.update(body);
    sha.finalize().into()
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
                source.update(include_bytes!("refs.rs"));
                source.update(include_bytes!("default_branch.rs"));
                source.update(include_bytes!("graph.rs"));
                source.update(include_bytes!("ancestry.rs"));
                source.update(include_bytes!("branch_rules.rs"));
                source.update(include_bytes!("branch_rules/command.rs"));
                source.update(include_bytes!("graph/preparation.rs"));
                source.update(include_bytes!("object_batch.rs"));
                source.update(include_bytes!("object_chunks.rs"));
                source.update(include_bytes!("object_reads.rs"));
                source.update(include_bytes!("large_blob.rs"));
                source.update(include_bytes!("push.rs"));
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
                source.update(SCHEMA.as_bytes());
                Digest::from_bytes(*source.finalize().as_bytes())
            },
            retained_codes: &[],
            schema_min: 1,
            schema_max: 1,
            migrations: MIGRATIONS.get_or_init(|| {
                [MigrationDescriptor {
                    version: 1,
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
        builder.cell_type(
            CellType::entity_uuid(RepositoryModule::NAME, "repository", REPOSITORIES)?
                .with_limits(REPOSITORY_DATABASE_LIMIT_BYTES, 64 * 1024 * 1024)?,
        )
    }
}

/// Product-owned capability for one repository's SQLite state.
pub struct RepositoryCell {
    sql: SqlCell<RepositoryModule>,
    application: ApplicationHandle<CanopyApplication>,
    target: CellTarget,
}

impl RepositoryCell {
    pub fn repository_id(&self) -> [u8; 16] {
        let mut id = [0; 16];
        id.copy_from_slice(self.target.partition());
        id
    }

    pub fn new(
        application: &ApplicationHandle<CanopyApplication>,
        target: CellTarget,
    ) -> cellule_runtime::Result<Self> {
        Ok(Self {
            sql: application.sql::<RepositoryModule>(target.clone())?,
            application: application.clone(),
            target,
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
        oid: [u8; 20],
        minimum: Option<Receipt>,
    ) -> std::result::Result<
        Observed<Option<(ObjectKind, Vec<u8>)>>,
        cellule_runtime::InvocationError<Vec<cellule_runtime::SqlResultSet>>,
    > {
        let result = self
            .sql
            .query(
                minimum,
                SqlBatch {
                    statements: vec![SqlStatement {
                        sql:
                            "SELECT kind, body, digest, size, chunk_id FROM objects WHERE oid = ?1"
                                .into(),
                        parameters: vec![SqlValue::Blob(oid.to_vec())],
                    }],
                },
            )
            .await?;
        let Some(row) = result.output.first().and_then(|set| set.rows.first()) else {
            return Ok(Observed {
                output: None,
                receipt: result.receipt,
            });
        };
        let [
            SqlValue::Text(kind),
            body,
            SqlValue::Blob(digest),
            SqlValue::Integer(size),
            upload,
        ] = row.as_slice()
        else {
            return Err(cellule_runtime::InvocationError::NotStarted(
                Error::Command("invalid stored object row"),
            ));
        };
        let kind = match kind.as_str() {
            "blob" => ObjectKind::Blob,
            "tree" => ObjectKind::Tree,
            "commit" => ObjectKind::Commit,
            "tag" => ObjectKind::Tag,
            _ => {
                return Err(cellule_runtime::InvocationError::NotStarted(
                    Error::Command("invalid stored object kind"),
                ));
            }
        };
        let body = match (body, upload) {
            (SqlValue::Blob(bytes), SqlValue::Null)
                if usize::try_from(*size).ok() == Some(bytes.len()) =>
            {
                bytes.clone()
            }
            (SqlValue::Null, SqlValue::Blob(upload)) => {
                let invalid = || {
                    cellule_runtime::InvocationError::NotStarted(Error::Command(
                        "invalid object chunk reference",
                    ))
                };
                self.chunked_body(
                    oid,
                    kind,
                    upload.as_slice().try_into().map_err(|_| invalid())?,
                    u64::try_from(*size).map_err(|_| invalid())?,
                    digest.as_slice().try_into().map_err(|_| invalid())?,
                )
                .await?
            }
            _ => {
                return Err(cellule_runtime::InvocationError::NotStarted(
                    Error::Command("invalid stored object body"),
                ));
            }
        };
        if object_id(kind, &body) != oid || blake3::hash(&body).as_bytes() != digest.as_slice() {
            return Err(cellule_runtime::InvocationError::NotStarted(
                Error::Command("corrupt stored object"),
            ));
        }
        Ok(Observed {
            output: Some((kind, body)),
            receipt: result.receipt,
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
