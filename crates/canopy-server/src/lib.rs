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
pub(crate) use canopy_object_storage::external;
mod git_cache;
pub(crate) use canopy_git_format as git_format;
pub use git_format::{ObjectFormat, ObjectId, ObjectIdError, ObjectKind, object_id};
pub mod git_gateway;
pub mod git_http;
pub mod git_input;
mod git_objects;
mod git_read;
mod graph;
pub mod http;
pub mod issues;
pub mod blob {
    //! Verified, immutable Git blob bodies stored outside the Repository Cell.

    pub(crate) use canopy_object_storage::blob::blob_path;
    pub use canopy_object_storage::blob::{
        LargeBlobError, LargeBlobRead, LargeBlobReference, LargeBlobStore,
    };
}
pub mod lfs;
mod native_git;
pub mod native_resources;
mod object_batch;
mod object_chunks;
mod object_reads;
mod pack_store;
pub mod packs;
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
pub use object_chunks::ObjectStageError;
pub use push::{PushCertificateReceipt, PushError, PushReceipt, VerifiedPushCertificate};
pub use refs::{FinalizePush, PushPlan, RefExpectation, RefPage, RefReadError, RefUpdate};

pub const REPOSITORIES: NamespaceId = NamespaceId::from_bytes([71; 16]);
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
        ..operation_with_codec(5, 5)
    },
    operation_with_codec(6, 3),
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
        schema_min: 1,
        schema_max: 1,
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

/// Byte location for a verified Git object record.
pub enum ObjectStorage {
    Inline(Vec<u8>),
    Chunked {
        upload: [u8; 16],
        size: u64,
        blake3: [u8; 32],
    },
    Packed {
        size: u64,
        blake3: [u8; 32],
        pack: [u8; 32],
    },
    External {
        size: u64,
        blake3: [u8; 32],
        sha256: [u8; 32],
    },
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
                source.update(include_bytes!("../../canopy-git-format/src/lib.rs"));
                source.update(include_bytes!(
                    "../../canopy-git-format/src/pack_index/mod.rs"
                ));
                source.update(include_bytes!("refs.rs"));
                source.update(include_bytes!("default_branch.rs"));
                source.update(include_bytes!("graph/mod.rs"));
                source.update(include_bytes!("ancestry.rs"));
                source.update(include_bytes!("branch_rules/mod.rs"));
                source.update(include_bytes!("branch_rules/command.rs"));
                source.update(include_bytes!("graph/preparation.rs"));
                source.update(include_bytes!("object_batch/mod.rs"));
                source.update(include_bytes!("object_chunks/mod.rs"));
                source.update(include_bytes!("object_reads/mod.rs"));
                source.update(include_bytes!("pack_store.rs"));
                source.update(include_bytes!("git_objects/mod.rs"));
                source.update(include_bytes!("native_resources.rs"));
                source.update(include_bytes!("native_git.rs"));
                source.update(include_bytes!("native_git/process.rs"));
                source.update(include_bytes!("native_git/process/fence.rs"));
                source.update(include_bytes!("git_gateway/mod.rs"));
                source.update(include_bytes!("git_gateway/preflight.rs"));
                source.update(include_bytes!("git_gateway/preflight/retention.rs"));
                source.update(include_bytes!("git_gateway/branch_policy.rs"));
                source.update(include_bytes!("git_gateway/push.rs"));
                source.update(include_bytes!("git_input/mod.rs"));
                source.update(include_bytes!("git_http/capture.rs"));
                source.update(include_bytes!("packs/wire_request.rs"));
                source.update(include_bytes!("packs/input_artifact.rs"));
                source.update(include_bytes!("packs/directory/index/mod.rs"));
                source.update(include_bytes!("packs/directory/index/record.rs"));
                source.update(include_bytes!("packs/directory/index/codec.rs"));
                source.update(include_bytes!("packs/directory/index/cursor.rs"));
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
                source.update(include_bytes!("packs/publication/native_result/plan.rs"));
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
                source.update(include_bytes!("packs/publication/initialization.rs"));
                source.update(include_bytes!("packs/publication/ref_policy/mod.rs"));
                source.update(include_bytes!("packs/publication/ref_policy/codec.rs"));
                source.update(include_bytes!("packs/publication/ref_policy/commands.rs"));
                source.update(include_bytes!("packs/publication/ref_policy/prepare.rs"));
                source.update(include_bytes!("packs/publication/ref_policy/schema.sql"));
                source.update(include_bytes!(
                    "packs/publication/initialization/publish.rs"
                ));
                source.update(include_bytes!("packs/publication/recovery/mod.rs"));
                source.update(include_bytes!("packs/publication/recovery/codec.rs"));
                source.update(include_bytes!("packs/publication/recovery/registration.rs"));
                source.update(include_bytes!("packs/publication/recovery/ready.rs"));
                source.update(include_bytes!("packs/publication/recovery/phase.rs"));
                source.update(include_bytes!("packs/publication/exact.rs"));
                source.update(include_bytes!("packs/publication/mod.rs"));
                source.update(include_bytes!("packs/publication/codec.rs"));
                source.update(include_bytes!("packs/publication/sql.rs"));
                source.update(include_bytes!("packs/publication/schema.sql"));
                source.update(include_bytes!("packs/publication/certificate.rs"));
                source.update(include_bytes!("packs/publication/publish.rs"));
                source.update(include_bytes!("packs/publication/compaction/publish.rs"));
                source.update(include_bytes!("packs/metadata/transport.rs"));
                source.update(include_bytes!("packs/publication/inputs.rs"));
                source.update(include_bytes!(
                    "../../canopy-object-storage/src/artifact.rs"
                ));
                source.update(include_bytes!(
                    "../../canopy-object-storage/src/blob/mod.rs"
                ));
                source.update(include_bytes!(
                    "../../canopy-object-storage/src/external.rs"
                ));
                source.update(include_bytes!("push/mod.rs"));
                source.update(include_bytes!("push/plan.rs"));
                source.update(include_bytes!("push/report.rs"));
                source.update(include_bytes!("access.rs"));
                source.update(include_bytes!("visibility.rs"));
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
    .with_limits(REPOSITORY_DATABASE_LIMIT_BYTES, 64 * 1024 * 1024)
}

/// Product-owned capability for one repository's SQLite state.
pub struct RepositoryCell {
    id: [u8; 16],
    object_format: ObjectFormat,
    sql: SqlCell<RepositoryModule>,
    application: ApplicationHandle<CanopyApplication>,
    target: CellTarget,
    // Gateways own the cache lifetime. Sharing the reader through a weak
    // reference must not retain its original disk budget after gateway eviction.
    pack_readers: std::sync::Mutex<Vec<std::sync::Weak<pack_store::PackReader>>>,
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
            pack_readers: std::sync::Mutex::new(Vec::new()),
        })
    }

    /// Prepares bounded graph certificates, then publishes one all-or-none ref plan.
    pub async fn finalize_push(
        &self,
        identity: cellule_runtime::MutationIdentity,
        plan: PushPlan,
    ) -> std::result::Result<Committed<bool>, cellule_runtime::InvocationError<bool>> {
        // Keep each transport-heavy phase in its own allocation. Embedding all
        // three futures multiplies stack copies when debug callers poll a push.
        Box::pin(self.prepare_graph(&plan)).await?;
        Box::pin(self.prepare_branch_proofs(&plan)).await?;
        Box::pin(
            self.application
                .command::<FinalizePush>(&self.target, identity, plan),
        )
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
        let result = self
            .sql
            .query(
                minimum,
                SqlBatch {
                    statements: vec![SqlStatement {
                        sql:
                            "SELECT kind, body, digest, size, chunk_id, storage, external_sha256 FROM objects WHERE oid = ?1"
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
            SqlValue::Text(storage),
            locator,
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
        if storage == "packed" {
            let SqlValue::Blob(pack) = locator else {
                return Err(cellule_runtime::InvocationError::NotStarted(
                    Error::Command("invalid pack locator"),
                ));
            };
            let pack: [u8; 32] = pack.as_slice().try_into().map_err(|_| {
                cellule_runtime::InvocationError::NotStarted(Error::Command("invalid pack locator"))
            })?;
            if *size < 0 || *size > INLINE_OBJECT_LIMIT as i64 {
                return Err(cellule_runtime::InvocationError::NotStarted(
                    Error::Command("object exceeds bounded body reader"),
                ));
            }
            let record = self.pack_record(pack).await?;
            let reader = self
                .pack_readers
                .lock()
                .map_err(|_| {
                    cellule_runtime::InvocationError::NotStarted(Error::Command(
                        "packed reader registry poisoned",
                    ))
                })?
                .iter()
                .find_map(std::sync::Weak::upgrade)
                .ok_or(cellule_runtime::InvocationError::NotStarted(
                    Error::Command("packed reader unavailable"),
                ))?;
            let body = reader
                .read_blob(
                    record,
                    oid,
                    *size as u64,
                    digest.as_slice().try_into().map_err(|_| {
                        cellule_runtime::InvocationError::NotStarted(Error::Command(
                            "invalid packed digest",
                        ))
                    })?,
                )
                .await
                .map_err(|error| {
                    cellule_runtime::InvocationError::NotStarted(Error::Facility {
                        name: "packed Git body",
                        source: Box::new(error),
                    })
                })?;
            if kind != ObjectKind::Blob
                || body.len() as i64 != *size
                || object_id(oid.format(), kind, &body) != oid
                || blake3::hash(&body).as_bytes() != digest.as_slice()
            {
                return Err(cellule_runtime::InvocationError::NotStarted(
                    Error::Command("corrupt packed Git body"),
                ));
            }
            return Ok(Observed {
                output: Some((kind, body)),
                receipt: result.receipt,
            });
        }
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
        if object_id(oid.format(), kind, &body) != oid
            || blake3::hash(&body).as_bytes() != digest.as_slice()
        {
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
