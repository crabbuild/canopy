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

pub mod git_gateway;
pub mod git_http;
pub mod http;
pub mod large_blob;
pub mod lfs;
mod refs;
pub mod server;

pub use refs::{FinalizePush, PushPlan, RefExpectation, RefUpdate};

pub const REPOSITORIES: NamespaceId = NamespaceId::from_bytes([71; 16]);
pub const INLINE_OBJECT_LIMIT: usize = 768 * 1024;
pub const REPOSITORY_DATABASE_LIMIT_BYTES: u64 = 512 * 1024 * 1024;

const SCHEMA: &str = include_str!("schema.sql");
const COMMANDS: [OperationDescriptor; 2] = [operation(1), operation(3)];
const QUERIES: [OperationDescriptor; 1] = [operation(2)];

const fn operation(id: u32) -> OperationDescriptor {
    OperationDescriptor {
        id,
        codec_version: 1,
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
    if !(1..=8).contains(&(repository[6] >> 4)) || repository[8] >> 6 != 2 {
        return Err(Error::Identity("repository UUID is not canonical"));
    }
    CellTarget::new(tenant, application, REPOSITORIES, &repository)
}

/// Git object kind used when calculating the canonical object ID.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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
        registry.bind_command::<FinalizePush>()
    }
}

pub struct CanopyApplication;

impl CellApplication for CanopyApplication {
    const NAME: &'static str = "canopy";

    fn register(builder: &mut ApplicationBuilder) -> cellule_runtime::Result<()> {
        builder.register(RepositoryModule)?;
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

    /// Publishes one all-or-none ref plan after its objects are durable.
    pub async fn finalize_push(
        &self,
        identity: cellule_runtime::MutationIdentity,
        plan: PushPlan,
    ) -> std::result::Result<Committed<bool>, cellule_runtime::InvocationError<bool>> {
        self.application
            .command::<FinalizePush>(&self.target, identity, plan)
            .await
    }

    pub async fn put_inline_object(
        &self,
        identity: cellule_runtime::MutationIdentity,
        kind: ObjectKind,
        body: &[u8],
    ) -> std::result::Result<
        Committed<[u8; 20]>,
        cellule_runtime::InvocationError<Vec<cellule_runtime::SqlResultSet>>,
    > {
        if body.len() > INLINE_OBJECT_LIMIT {
            return Err(cellule_runtime::InvocationError::NotStarted(
                Error::Command("object exceeds inline limit"),
            ));
        }
        let oid = object_id(kind, body);
        let digest = blake3::hash(body);
        let committed = self.sql.batch(identity, SqlBatch {
            statements: vec![SqlStatement {
                sql: "INSERT INTO objects (oid, kind, size, digest, storage, body) VALUES (?1, ?2, ?3, ?4, 'inline', ?5) ON CONFLICT(oid) DO NOTHING".into(),
                parameters: vec![
                    SqlValue::Blob(oid.to_vec()),
                    SqlValue::Text(kind.git_name().into()),
                    SqlValue::Integer(i64::try_from(body.len()).map_err(|_| cellule_runtime::InvocationError::NotStarted(Error::Command("object size overflows")))?),
                    SqlValue::Blob(digest.as_bytes().to_vec()),
                    SqlValue::Blob(body.to_vec()),
                ],
            }],
        }).await?;
        match self.object(oid, Some(committed.receipt)).await?.output {
            Some((stored_kind, stored_body)) if stored_kind == kind && stored_body == body => {}
            _ => {
                return Err(cellule_runtime::InvocationError::InvalidPublishedResult {
                    receipt: committed.receipt,
                    source: Box::new(Error::Command("conflicting stored object")),
                });
            }
        }
        Ok(Committed {
            output: oid,
            receipt: committed.receipt,
        })
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
                        sql: "SELECT kind, body, digest FROM objects WHERE oid = ?1".into(),
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
            SqlValue::Blob(body),
            SqlValue::Blob(digest),
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
        if object_id(kind, body) != oid || blake3::hash(body).as_bytes() != digest.as_slice() {
            return Err(cellule_runtime::InvocationError::NotStarted(
                Error::Command("corrupt stored object"),
            ));
        }
        Ok(Observed {
            output: Some((kind, body.clone())),
            receipt: result.receipt,
        })
    }

    /// Checks whether a Git object ID is already recorded.
    pub async fn object_exists(
        &self,
        oid: [u8; 20],
    ) -> std::result::Result<
        Observed<bool>,
        cellule_runtime::InvocationError<Vec<cellule_runtime::SqlResultSet>>,
    > {
        let result = self
            .sql
            .query(
                None,
                SqlBatch {
                    statements: vec![SqlStatement {
                        sql: "SELECT 1 FROM objects WHERE oid = ?1 LIMIT 1".into(),
                        parameters: vec![SqlValue::Blob(oid.to_vec())],
                    }],
                },
            )
            .await?;
        Ok(Observed {
            output: result
                .output
                .first()
                .is_some_and(|set| !set.rows.is_empty()),
            receipt: result.receipt,
        })
    }

    /// Records a previously uploaded immutable large Git blob.
    pub async fn put_external_blob(
        &self,
        identity: cellule_runtime::MutationIdentity,
        oid: [u8; 20],
        size: u64,
        blake3: [u8; 32],
        sha256: [u8; 32],
    ) -> std::result::Result<
        Committed<()>,
        cellule_runtime::InvocationError<Vec<cellule_runtime::SqlResultSet>>,
    > {
        let size = i64::try_from(size).map_err(|_| {
            cellule_runtime::InvocationError::NotStarted(Error::Command(
                "blob size overflows SQLite",
            ))
        })?;
        let committed = self
            .sql
            .batch(
                identity,
                SqlBatch {
                    statements: vec![SqlStatement {
                        sql: "INSERT INTO objects (oid, kind, size, digest, storage, external_sha256) VALUES (?1, 'blob', ?2, ?3, 'external', ?4) ON CONFLICT(oid) DO NOTHING".into(),
                        parameters: vec![
                            SqlValue::Blob(oid.to_vec()),
                            SqlValue::Integer(size),
                            SqlValue::Blob(blake3.to_vec()),
                            SqlValue::Blob(sha256.to_vec()),
                        ],
                    }],
                },
            )
            .await?;
        let record = self
            .sql
            .query(
                Some(committed.receipt),
                SqlBatch {
                    statements: vec![SqlStatement {
                        sql: "SELECT kind, size, digest, storage, external_sha256 FROM objects WHERE oid = ?1".into(),
                        parameters: vec![SqlValue::Blob(oid.to_vec())],
                    }],
                },
            )
            .await?;
        let expected = [
            SqlValue::Text("blob".into()),
            SqlValue::Integer(size),
            SqlValue::Blob(blake3.to_vec()),
            SqlValue::Text("external".into()),
            SqlValue::Blob(sha256.to_vec()),
        ];
        if record
            .output
            .first()
            .and_then(|set| set.rows.first())
            .map(Vec::as_slice)
            != Some(expected.as_slice())
        {
            return Err(cellule_runtime::InvocationError::InvalidPublishedResult {
                receipt: committed.receipt,
                source: Box::new(Error::Command("conflicting Git object identity")),
            });
        }
        Ok(Committed {
            output: (),
            receipt: committed.receipt,
        })
    }

    /// Reads the next verified object in OID order for cold cache hydration.
    pub async fn next_object(
        &self,
        after: Option<[u8; 20]>,
    ) -> std::result::Result<
        Observed<Option<StoredObject>>,
        cellule_runtime::InvocationError<Vec<cellule_runtime::SqlResultSet>>,
    > {
        let result = self
            .sql
            .query(
                None,
                SqlBatch {
                    statements: vec![SqlStatement {
                        sql: "SELECT oid, kind, size, digest, storage, body, external_sha256 FROM objects WHERE oid > ?1 ORDER BY oid LIMIT 1".into(),
                        parameters: vec![SqlValue::Blob(after.map_or_else(Vec::new, |oid| oid.to_vec()))],
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
            SqlValue::Blob(oid),
            SqlValue::Text(kind),
            SqlValue::Integer(size),
            SqlValue::Blob(digest),
            SqlValue::Text(storage),
            body,
            external_sha256,
        ] = row.as_slice()
        else {
            return Err(cellule_runtime::InvocationError::NotStarted(
                Error::Command("invalid stored object row"),
            ));
        };
        let oid: [u8; 20] = oid.as_slice().try_into().map_err(|_| {
            cellule_runtime::InvocationError::NotStarted(Error::Command("invalid stored object ID"))
        })?;
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
        let storage = match (storage.as_str(), body, external_sha256) {
            ("inline", SqlValue::Blob(body), SqlValue::Null)
                if *size >= 0
                    && usize::try_from(*size).ok() == Some(body.len())
                    && object_id(kind, body) == oid
                    && blake3::hash(body).as_bytes() == digest.as_slice() =>
            {
                ObjectStorage::Inline(body.clone())
            }
            ("external", SqlValue::Null, SqlValue::Blob(sha256))
                if kind == ObjectKind::Blob && *size >= 0 =>
            {
                let blake3 = digest.as_slice().try_into().map_err(|_| {
                    cellule_runtime::InvocationError::NotStarted(Error::Command(
                        "invalid blob digest",
                    ))
                })?;
                let sha256 = sha256.as_slice().try_into().map_err(|_| {
                    cellule_runtime::InvocationError::NotStarted(Error::Command(
                        "invalid SHA-256 digest",
                    ))
                })?;
                ObjectStorage::External {
                    size: u64::try_from(*size).map_err(|_| {
                        cellule_runtime::InvocationError::NotStarted(Error::Command(
                            "invalid blob size",
                        ))
                    })?,
                    blake3,
                    sha256,
                }
            }
            _ => {
                return Err(cellule_runtime::InvocationError::NotStarted(
                    Error::Command("corrupt stored object"),
                ));
            }
        };
        Ok(Observed {
            output: Some(StoredObject { oid, kind, storage }),
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
