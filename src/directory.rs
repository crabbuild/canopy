//! Durable owner/name to Repository Cell identity mapping.

mod tokens;
pub use tokens::{TOKEN_PAGE_SIZE, TokenAuthority, TokenChange, TokenInfo};

use std::sync::OnceLock;

use cellule_app::{ApplicationHandle, CellType};
use cellule_runtime::{
    ApplicationId, CatalogRole, CellModule, CellTarget, Committed, Digest, Error, InvocationError,
    MigrationDescriptor, ModuleDescriptor, MutationIdentity, NamespaceDescriptor, NamespaceId,
    Observed, OperationDescriptor, Receipt, RegistryBuilder, SqlBatch, SqlCell, SqlModule,
    SqlResultSet, SqlStatement, SqlValue, TenantId, partition_for_shard, register_sql,
};

use crate::{CanopyApplication, validate_repository_id};

pub const DIRECTORY: NamespaceId = NamespaceId::from_bytes([72; 16]);
pub const SCHEMA: &str = include_str!("directory_schema.sql");
pub const REPOSITORY_PAGE_SIZE: usize = 32;

const COMMANDS: [OperationDescriptor; 1] = [operation(1)];
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

pub struct DirectoryModule;

impl SqlModule for DirectoryModule {
    const MODULE: &'static str = Self::NAME;
    const BATCH_COMMAND_ID: u32 = 1;
    const BATCH_QUERY_ID: u32 = 2;
}

impl CellModule for DirectoryModule {
    const NAME: &'static str = "directory";

    fn descriptor(&self) -> &'static ModuleDescriptor {
        static MIGRATIONS: OnceLock<[MigrationDescriptor; 1]> = OnceLock::new();
        static DESCRIPTOR: OnceLock<ModuleDescriptor> = OnceLock::new();
        DESCRIPTOR.get_or_init(|| ModuleDescriptor {
            name: Self::NAME,
            source_digest: {
                let mut source = blake3::Hasher::new();
                source.update(include_bytes!("directory.rs"));
                source.update(include_bytes!("directory/tokens.rs"));
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
                id: DIRECTORY,
                name: Self::NAME,
                role: CatalogRole::Sql,
                shards: 1,
                effect_targets: &[],
                dead_letter: None,
            }],
        })
    }

    fn register(self, registry: &mut RegistryBuilder) -> cellule_runtime::Result<()> {
        register_sql::<Self>(registry)
    }
}

pub fn cell_type() -> cellule_runtime::Result<CellType> {
    CellType::new(
        DirectoryModule::NAME,
        "directory",
        DIRECTORY,
        CatalogRole::Sql,
        1,
    )
}

pub fn directory_target(
    tenant: TenantId,
    application: ApplicationId,
) -> cellule_runtime::Result<CellTarget> {
    CellTarget::new(tenant, application, DIRECTORY, &partition_for_shard(0))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RepositoryState {
    Pending,
    Ready,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RepositoryEntry {
    pub owner: String,
    pub name: String,
    pub repository_id: [u8; 16],
    pub state: RepositoryState,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RenameOutcome {
    Renamed(RepositoryEntry),
    NotFound,
    NameTaken,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum TokenScope {
    Read,
    Write,
    Admin,
}

impl TokenScope {
    pub(crate) fn parse(value: &str) -> Option<Self> {
        match value {
            "read" => Some(Self::Read),
            "write" => Some(Self::Write),
            "admin" => Some(Self::Admin),
            _ => None,
        }
    }

    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::Write => "write",
            Self::Admin => "admin",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Principal {
    pub account: String,
    pub scope: TokenScope,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CreateAccountOutcome {
    Created(Principal),
    NameTaken,
    Forbidden,
}

pub struct DirectoryCell {
    sql: SqlCell<DirectoryModule>,
}

impl DirectoryCell {
    pub fn new(
        application: &ApplicationHandle<CanopyApplication>,
        target: CellTarget,
    ) -> cellule_runtime::Result<Self> {
        Ok(Self {
            sql: application.sql::<DirectoryModule>(target)?,
        })
    }

    /// Creates a bootstrap account through a trusted Directory capability.
    pub async fn create_account(
        &self,
        identity: MutationIdentity,
        name: &str,
        token_digest: [u8; 32],
        scope: TokenScope,
    ) -> Result<Committed<CreateAccountOutcome>, InvocationError<Vec<SqlResultSet>>> {
        self.create_account_inner(identity, name, token_digest, scope, None)
            .await
    }

    /// Creates an account only while the site's authorizing admin credential is active.
    pub async fn create_account_authorized(
        &self,
        identity: MutationIdentity,
        authority: TokenAuthority<'_>,
        token_digest: [u8; 32],
        scope: TokenScope,
    ) -> Result<Committed<CreateAccountOutcome>, InvocationError<Vec<SqlResultSet>>> {
        validate_component(authority.site_owner).map_err(InvocationError::NotStarted)?;
        self.create_account_inner(
            identity,
            authority.account,
            token_digest,
            scope,
            Some((authority.actor_digest, authority.site_owner)),
        )
        .await
    }

    async fn create_account_inner(
        &self,
        identity: MutationIdentity,
        name: &str,
        token_digest: [u8; 32],
        scope: TokenScope,
        authority: Option<([u8; 32], &str)>,
    ) -> Result<Committed<CreateAccountOutcome>, InvocationError<Vec<SqlResultSet>>> {
        validate_component(name).map_err(InvocationError::NotStarted)?;
        // Only trusted bootstrap bypasses an existing credential. HTTP account
        // creation rechecks the site admin in the transaction that issues its token.
        let authorized = "?6 IS NULL OR EXISTS (SELECT 1 FROM access_tokens t JOIN accounts a ON a.name = t.account WHERE t.digest = ?6 AND t.account = ?7 AND t.scope = 'admin' AND t.enabled = 1 AND a.enabled = 1)";
        let parameters = vec![
            SqlValue::Text(name.into()),
            SqlValue::Blob(token_digest.to_vec()),
            SqlValue::Text(scope.as_str().into()),
            SqlValue::Blob(identity.request_id.as_bytes().to_vec()),
            SqlValue::Integer(identity.issued_at_ms),
            authority.map_or(SqlValue::Null, |(digest, _)| {
                SqlValue::Blob(digest.to_vec())
            }),
            authority.map_or(SqlValue::Null, |(_, owner)| SqlValue::Text(owner.into())),
        ];
        let committed = self.sql.batch(identity, SqlBatch {
            statements: vec![
                SqlStatement { sql: format!("SELECT {authorized}"), parameters: parameters.clone() },
                SqlStatement {
                    sql: format!("INSERT INTO accounts (name, enabled) SELECT ?1, 1 WHERE ({authorized}) AND NOT EXISTS (SELECT 1 FROM access_tokens WHERE digest = ?2) ON CONFLICT(name) DO NOTHING"),
                    parameters: parameters.clone(),
                },
                SqlStatement {
                    sql: format!("INSERT INTO access_tokens (digest, account, scope, enabled, id, created_ms) SELECT ?2, ?1, ?3, 1, ?4, ?5 FROM accounts WHERE name = ?1 AND enabled = 1 AND ({authorized}) AND NOT EXISTS (SELECT 1 FROM access_tokens WHERE account = ?1) ON CONFLICT DO NOTHING"),
                    parameters,
                },
            ],
        }).await?;
        if !matches!(
            committed
                .output
                .first()
                .and_then(|set| set.rows.first())
                .map(Vec::as_slice),
            Some([SqlValue::Integer(1)])
        ) {
            return Ok(Committed {
                output: CreateAccountOutcome::Forbidden,
                receipt: committed.receipt,
            });
        }
        let authenticated = self
            .authenticate(token_digest, Some(committed.receipt))
            .await?
            .output;
        let output = match authenticated {
            Some(principal) if principal.account == name && principal.scope == scope => {
                CreateAccountOutcome::Created(principal)
            }
            _ => CreateAccountOutcome::NameTaken,
        };
        Ok(Committed {
            output,
            receipt: committed.receipt,
        })
    }

    /// Resolves one active token digest to its persisted account and scope.
    pub async fn authenticate(
        &self,
        token_digest: [u8; 32],
        minimum: Option<Receipt>,
    ) -> Result<Observed<Option<Principal>>, InvocationError<Vec<SqlResultSet>>> {
        let result = self.sql.query(minimum, SqlBatch {
            statements: vec![SqlStatement {
                sql: "SELECT a.name, t.scope FROM access_tokens AS t JOIN accounts AS a ON a.name = t.account WHERE t.digest = ?1 AND t.enabled = 1 AND a.enabled = 1".into(),
                parameters: vec![SqlValue::Blob(token_digest.to_vec())],
            }],
        }).await?;
        let principal = result
            .output
            .first()
            .and_then(|set| set.rows.first())
            .map(|row| {
                let [SqlValue::Text(account), SqlValue::Text(scope)] = row.as_slice() else {
                    return Err(Error::Command("invalid account row"));
                };
                validate_component(account)?;
                let scope =
                    TokenScope::parse(scope).ok_or(Error::Command("invalid token scope"))?;
                Ok(Principal {
                    account: account.clone(),
                    scope,
                })
            })
            .transpose()
            .map_err(InvocationError::NotStarted)?;
        Ok(Observed {
            output: principal,
            receipt: result.receipt,
        })
    }

    /// Checks whether an active account name can receive repository access.
    pub async fn account_exists(
        &self,
        name: &str,
    ) -> Result<Observed<bool>, InvocationError<Vec<SqlResultSet>>> {
        validate_component(name).map_err(InvocationError::NotStarted)?;
        let result = self
            .sql
            .query(
                None,
                SqlBatch {
                    statements: vec![SqlStatement {
                        sql: "SELECT 1 FROM accounts WHERE name = ?1 AND enabled = 1".into(),
                        parameters: vec![SqlValue::Text(name.into())],
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

    /// Reserves one stable UUID so a failed Repository Cell bootstrap can be retried.
    pub async fn reserve(
        &self,
        identity: MutationIdentity,
        owner: &str,
        name: &str,
        repository_id: [u8; 16],
    ) -> Result<Committed<RepositoryEntry>, InvocationError<Vec<SqlResultSet>>> {
        validate_name(owner, name).map_err(InvocationError::NotStarted)?;
        validate_repository_id(repository_id).map_err(InvocationError::NotStarted)?;
        let committed = self.sql.batch(identity, SqlBatch {
            statements: vec![SqlStatement {
                sql: "INSERT INTO repositories (owner, name, repository_id, state) VALUES (?1, ?2, ?3, 'pending') ON CONFLICT(owner, name) DO NOTHING".into(),
                parameters: vec![
                    SqlValue::Text(owner.into()),
                    SqlValue::Text(name.into()),
                    SqlValue::Blob(repository_id.to_vec()),
                ],
            }],
        }).await?;
        let entry = self
            .lookup(owner, name, Some(committed.receipt))
            .await?
            .output
            .ok_or_else(|| InvocationError::InvalidPublishedResult {
                receipt: committed.receipt,
                source: Box::new(Error::Command("repository reservation vanished")),
            })?;
        Ok(Committed {
            output: entry,
            receipt: committed.receipt,
        })
    }

    /// Makes a provisioned Repository Cell discoverable at its reserved name.
    pub async fn activate(
        &self,
        identity: MutationIdentity,
        entry: &RepositoryEntry,
    ) -> Result<Committed<RepositoryEntry>, InvocationError<Vec<SqlResultSet>>> {
        let committed = self.sql.batch(identity, SqlBatch {
            statements: vec![SqlStatement {
                sql: "UPDATE repositories SET state = 'ready' WHERE owner = ?1 AND name = ?2 AND repository_id = ?3 AND state = 'pending'".into(),
                parameters: vec![
                    SqlValue::Text(entry.owner.clone()),
                    SqlValue::Text(entry.name.clone()),
                    SqlValue::Blob(entry.repository_id.to_vec()),
                ],
            }],
        }).await?;
        let current = self
            .lookup(&entry.owner, &entry.name, Some(committed.receipt))
            .await?
            .output;
        match current {
            Some(current)
                if current.repository_id == entry.repository_id
                    && current.state == RepositoryState::Ready =>
            {
                Ok(Committed {
                    output: current,
                    receipt: committed.receipt,
                })
            }
            _ => Err(InvocationError::InvalidPublishedResult {
                receipt: committed.receipt,
                source: Box::new(Error::Command("repository activation conflicted")),
            }),
        }
    }

    pub async fn lookup(
        &self,
        owner: &str,
        name: &str,
        minimum: Option<Receipt>,
    ) -> Result<Observed<Option<RepositoryEntry>>, InvocationError<Vec<SqlResultSet>>> {
        validate_name(owner, name).map_err(InvocationError::NotStarted)?;
        let result = self.sql.query(minimum, SqlBatch {
            statements: vec![SqlStatement {
                sql: "SELECT owner, name, repository_id, state FROM repositories WHERE owner = ?1 AND name = ?2".into(),
                parameters: vec![SqlValue::Text(owner.into()), SqlValue::Text(name.into())],
            }],
        }).await?;
        let entry = result
            .output
            .first()
            .and_then(|set| set.rows.first())
            .map(|row| decode_entry(row))
            .transpose()
            .map_err(InvocationError::NotStarted)?;
        Ok(Observed {
            output: entry,
            receipt: result.receipt,
        })
    }

    /// Finds a ready name only if the viewer owns it or has an access candidate.
    ///
    /// A candidate still requires a current Repository Cell ACL check.
    pub async fn lookup_candidate(
        &self,
        account: &str,
        owner: &str,
        name: &str,
    ) -> Result<Observed<Option<RepositoryEntry>>, InvocationError<Vec<SqlResultSet>>> {
        validate_component(account).map_err(InvocationError::NotStarted)?;
        validate_name(owner, name).map_err(InvocationError::NotStarted)?;
        let result = self.sql.query(None, SqlBatch {
            statements: vec![SqlStatement {
                sql: "SELECT owner, name, repository_id, state FROM repositories r WHERE owner = ?1 AND name = ?2 AND state = 'ready' AND (owner = ?3 OR EXISTS (SELECT 1 FROM repository_discovery d WHERE d.repository_id = r.repository_id AND d.account = ?3))".into(),
                parameters: vec![SqlValue::Text(owner.into()), SqlValue::Text(name.into()), SqlValue::Text(account.into())],
            }],
        }).await?;
        let output = result
            .output
            .first()
            .and_then(|set| set.rows.first())
            .map(|row| decode_entry(row))
            .transpose()
            .map_err(InvocationError::NotStarted)?;
        Ok(Observed {
            output,
            receipt: result.receipt,
        })
    }

    /// Atomically moves a ready name while preserving its Repository Cell UUID.
    pub async fn rename(
        &self,
        identity: MutationIdentity,
        owner: &str,
        old_name: &str,
        new_name: &str,
        repository_id: [u8; 16],
    ) -> Result<Committed<RenameOutcome>, InvocationError<Vec<SqlResultSet>>> {
        validate_name(owner, old_name).map_err(InvocationError::NotStarted)?;
        validate_component(new_name).map_err(InvocationError::NotStarted)?;
        validate_repository_id(repository_id).map_err(InvocationError::NotStarted)?;
        let committed = self.sql.batch(identity, SqlBatch {
            statements: vec![SqlStatement {
                // The precondition and collision check are in the same Cell transaction.
                sql: "UPDATE repositories SET name = ?3 WHERE owner = ?1 AND name = ?2 AND repository_id = ?4 AND state = 'ready' AND NOT EXISTS (SELECT 1 FROM repositories WHERE owner = ?1 AND name = ?3)".into(),
                parameters: vec![
                    SqlValue::Text(owner.into()),
                    SqlValue::Text(old_name.into()),
                    SqlValue::Text(new_name.into()),
                    SqlValue::Blob(repository_id.to_vec()),
                ],
            }],
        }).await?;
        let destination = self
            .lookup(owner, new_name, Some(committed.receipt))
            .await?
            .output;
        let output = match destination {
            Some(entry)
                if entry.repository_id == repository_id
                    && entry.state == RepositoryState::Ready =>
            {
                RenameOutcome::Renamed(entry)
            }
            Some(_) => RenameOutcome::NameTaken,
            None => RenameOutcome::NotFound,
        };
        Ok(Committed {
            output,
            receipt: committed.receipt,
        })
    }

    /// Records a listing candidate before granting repository-local access.
    ///
    /// Candidates are retained after revocation; callers must check the Repository
    /// Cell ACL before exposing metadata. Only the directory owner may record one.
    pub async fn remember_access(
        &self,
        identity: MutationIdentity,
        actor: &str,
        account: &str,
        repository_id: [u8; 16],
    ) -> Result<Committed<bool>, InvocationError<Vec<SqlResultSet>>> {
        validate_component(actor).map_err(InvocationError::NotStarted)?;
        validate_component(account).map_err(InvocationError::NotStarted)?;
        validate_repository_id(repository_id).map_err(InvocationError::NotStarted)?;
        let result = self.sql.batch(identity, SqlBatch {
            statements: vec![SqlStatement {
                sql: "INSERT INTO repository_discovery (account, repository_id) SELECT ?2, repository_id FROM repositories WHERE owner = ?1 AND repository_id = ?3 AND state = 'ready' AND owner != ?2 AND EXISTS (SELECT 1 FROM accounts WHERE name = ?2 AND enabled = 1) ON CONFLICT(account, repository_id) DO UPDATE SET repository_id = excluded.repository_id".into(),
                parameters: vec![SqlValue::Text(actor.into()), SqlValue::Text(account.into()), SqlValue::Blob(repository_id.to_vec())],
            }],
        }).await?;
        Ok(Committed {
            output: result
                .output
                .first()
                .is_some_and(|set| set.rows_affected == 1),
            receipt: result.receipt,
        })
    }

    /// Reads a bounded UUID-ordered page of owned repositories and access candidates.
    ///
    /// A candidate is not an authorization decision; its current Cell ACL must
    /// be checked. Pending repositories are excluded and renames retain position.
    pub async fn list_candidates(
        &self,
        account: &str,
        after: Option<[u8; 16]>,
    ) -> Result<Observed<Vec<RepositoryEntry>>, InvocationError<Vec<SqlResultSet>>> {
        validate_component(account).map_err(InvocationError::NotStarted)?;
        if let Some(after) = after {
            validate_repository_id(after).map_err(InvocationError::NotStarted)?;
        }
        let result = self.sql.query(None, SqlBatch {
            statements: vec![SqlStatement {
                // Owner entries and candidates are disjoint: remember_access refuses
                // self-grants. Both UUID ranges can merge without sorting every grant.
                sql: "SELECT owner, name, repository_id, state FROM repositories WHERE owner = ?1 AND state = 'ready' AND repository_id > ?2 UNION ALL SELECT r.owner, r.name, d.repository_id, r.state FROM repository_discovery d JOIN repositories r ON r.repository_id = d.repository_id WHERE d.account = ?1 AND d.repository_id > ?2 AND r.state = 'ready' ORDER BY repository_id LIMIT ?3".into(),
                parameters: vec![SqlValue::Text(account.into()), SqlValue::Blob(after.map_or_else(Vec::new, |id| id.to_vec())), SqlValue::Integer(REPOSITORY_PAGE_SIZE as i64)],
            }],
        }).await?;
        let entries = result
            .output
            .first()
            .ok_or_else(|| {
                InvocationError::NotStarted(Error::Command("directory query returned no result"))
            })?
            .rows
            .iter()
            .map(|row| decode_entry(row))
            .collect::<cellule_runtime::Result<Vec<_>>>()
            .map_err(InvocationError::NotStarted)?;
        Ok(Observed {
            output: entries,
            receipt: result.receipt,
        })
    }
}

fn decode_entry(row: &[SqlValue]) -> cellule_runtime::Result<RepositoryEntry> {
    let [
        SqlValue::Text(owner),
        SqlValue::Text(name),
        SqlValue::Blob(id),
        SqlValue::Text(state),
    ] = row
    else {
        return Err(Error::Command("invalid directory row"));
    };
    validate_name(owner, name)?;
    let repository_id = id
        .as_slice()
        .try_into()
        .map_err(|_| Error::Command("invalid repository UUID"))?;
    validate_repository_id(repository_id)?;
    let state = match state.as_str() {
        "pending" => RepositoryState::Pending,
        "ready" => RepositoryState::Ready,
        _ => return Err(Error::Command("invalid repository state")),
    };
    Ok(RepositoryEntry {
        owner: owner.clone(),
        name: name.clone(),
        repository_id,
        state,
    })
}

fn validate_name(owner: &str, name: &str) -> cellule_runtime::Result<()> {
    validate_component(owner)?;
    validate_component(name)
}

pub(crate) fn validate_component(value: &str) -> cellule_runtime::Result<()> {
    if value.is_empty()
        || value.len() > 64
        || value.starts_with('.')
        || value.ends_with('.')
        || value.contains("..")
        || !value.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'-' | b'_' | b'.')
        })
    {
        return Err(Error::Command("invalid repository owner or name"));
    }
    Ok(())
}
