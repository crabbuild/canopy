//! Certified empty-catalog initialization before exposing a repository route.
use crate::{
    ObjectFormat, RepositoryCell, RepositoryModule,
    packs::{
        catalog::{CatalogFileLimits, CatalogFiles, CatalogIndexes, CatalogSnapshot},
        directory::snapshot::DirectorySnapshot,
        metadata::MetadataLimits,
        publication::{
            BeginPreparation, BeginRequest, CatalogPreparation, CheckInitializedCatalog,
            ClaimPreparation, DEFAULT_LEASE_MS, GenerationFact, InitializationReply, LeaseCheck,
            LeaseRequest, PreparationBaseResolver, PreparationDenial, PreparationReply,
            PreparationToken, PublicationError, RegisteredRootRecovery,
        },
    },
};
use canopy_object_storage::artifact::ArtifactStore;
use cellule_ltx::DiskBudget;
use cellule_runtime::{
    CellClient, Error, InvocationError, Receipt,
    identity::IncarnationId,
    primitives::sql::{SqlBatch, SqlCell, SqlStatement, SqlValue},
    registry::OwnerFence,
};
use object_store::ObjectStore;
use std::{path::Path, sync::Arc};

type Failure = Box<dyn std::error::Error + Send + Sync>;

fn request(repository: &RepositoryCell, owner: &str) -> BeginRequest {
    let mut hash = blake3::Hasher::new();
    hash.update(b"canopy.repository.initialization.v1\0");
    for field in [
        repository.target.tenant().as_bytes().as_slice(),
        repository.target.application().as_bytes().as_slice(),
        repository.id.as_slice(),
        repository.object_format.as_str().as_bytes(),
        owner.as_bytes(),
    ] {
        hash.update(&(field.len() as u64).to_be_bytes());
        hash.update(field);
    }
    let request_digest = *hash.finalize().as_bytes();
    let mut operation = [0; 16];
    operation.copy_from_slice(&request_digest[..16]);
    operation[0] |= 1;
    BeginRequest {
        repository: repository.id,
        operation,
        request_digest,
        actor: owner.into(),
        lease_ms: DEFAULT_LEASE_MS,
    }
}

async fn verify(
    fact: GenerationFact,
    store: &ArtifactStore,
    format: ObjectFormat,
) -> Result<(), Failure> {
    if fact.generation != 1 || fact.certificate.is_none() {
        return Err(Error::Command("invalid repository initialization fact").into());
    }
    let stored = fact
        .catalog
        .ok_or(Error::Command("initial catalog absent"))?;
    if stored.repository != store.repository() || stored.format != format {
        return Err(Error::Command("initial catalog context differs").into());
    }
    let catalog = CatalogSnapshot::download(store, stored).await?;
    let directory = DirectorySnapshot::download(store, catalog.directory).await?;
    let refs = fact
        .refs
        .ok_or(Error::Command("initial refs absent"))?
        .read(store)
        .await?;
    if catalog.sources.is_some()
        || !directory.level_zero.is_empty()
        || directory.levels.iter().any(Option::is_some)
        || refs.repository != store.repository()
        || refs.format != format
        || refs.generation != 0
        || refs.root.is_some()
        || refs.default_branch != "refs/heads/main"
    {
        return Err(Error::Command("initial catalog is not the certified empty state").into());
    }
    Ok(())
}

/// The caller's tracked cold-transition task owns this work through cancellation.
/// Ready repositories only observe their immutable initialization; they cannot
/// reconstruct missing ownership or publish a new empty catalog during restore.
pub(super) async fn ensure(
    repository: &RepositoryCell,
    client: CellClient,
    owner: &str,
    provider: Arc<dyn ObjectStore>,
    workspace: &Path,
    budget: DiskBudget,
    pending: bool,
) -> Result<(), Failure> {
    let input = request(repository, owner);
    let target = &repository.target;
    let store = Arc::new(ArtifactStore::new(provider, repository.id));
    if let Some(fact) = client
        .query::<CheckInitializedCatalog>(target, None, input.clone())
        .await?
        .output
    {
        return verify(fact, &store, repository.object_format).await;
    }
    if !pending {
        return Err(Error::Command("ready repository has no certified initialization").into());
    }
    let started_at = std::time::Instant::now();
    let recovered =
        RegisteredRootRecovery::load_initialization(&client, target, &store, &input).await?;
    let claim = if let Some(recovered) = recovered {
        match recovered.recover_initialization(&client, &store).await {
            Ok(committed) => {
                let InitializationReply::Initialized(fact) = committed.output else {
                    return Err(Error::Command("invalid recovered initialization reply").into());
                };
                return verify(*fact, &store, repository.object_format).await;
            }
            Err(PublicationError::Initialization(InvocationError::Rejected(ref value)))
                if matches!(
                    value.output,
                    InitializationReply::Denied(
                        PreparationDenial::Stale | PreparationDenial::Expired
                    )
                ) =>
            {
                Some(LeaseCheck {
                    token: recovered.token(),
                    actor: owner.into(),
                })
            }
            Err(error) => return Err(error.into()),
        }
    } else {
        None
    };
    let started = if let Some(check) = claim {
        client
            .command::<ClaimPreparation>(
                target,
                super::mutation_identity()?,
                LeaseRequest {
                    check,
                    lease_ms: DEFAULT_LEASE_MS,
                },
            )
            .await?
    } else {
        match client
            .command::<BeginPreparation>(target, super::mutation_identity()?, input.clone())
            .await
        {
            Ok(started) => started,
            Err(InvocationError::Rejected(rejected))
                if matches!(
                    rejected.output,
                    PreparationReply::Denied(PreparationDenial::Stale | PreparationDenial::Expired)
                ) =>
            {
                // Claim only after a known domain refusal. Read the exact old
                // binding; the Claim receiver verifies its pin and actual owner.
                let check = prior_attempt(&client, repository, &input, rejected.receipt).await?;
                client
                    .command::<ClaimPreparation>(
                        target,
                        super::mutation_identity()?,
                        LeaseRequest {
                            check,
                            lease_ms: DEFAULT_LEASE_MS,
                        },
                    )
                    .await?
            }
            Err(error) => {
                // A logical initialization can win between the first query and
                // Begin. Only a known conflict may use that exact retained result;
                // uncertain command evidence stays an error, never fresh admission.
                if matches!(&error, InvocationError::Rejected(value)
                if value.output == PreparationReply::Denied(PreparationDenial::Conflict))
                    && let Some(fact) = client
                        .query::<CheckInitializedCatalog>(target, None, input)
                        .await?
                        .output
                {
                    return verify(fact, &store, repository.object_format).await;
                }
                return Err(error.into());
            }
        }
    };
    let PreparationReply::Granted(lease) = started.output else {
        return Err(Error::Command("repository initialization admission denied").into());
    };
    let check = LeaseCheck {
        token: lease.token,
        actor: owner.into(),
    };
    let indexes = Arc::new(CatalogIndexes::new(
        Arc::clone(&store),
        repository.object_format,
    ));
    let files = Arc::new(CatalogFiles::new(
        workspace,
        budget.clone(),
        Arc::clone(&store),
        repository.object_format,
        CatalogFileLimits::default(),
    )?);
    let base = Arc::new(
        PreparationBaseResolver::open(
            client.clone(),
            target.clone(),
            check,
            indexes,
            files,
            Some(started.receipt),
        )
        .await?,
    );
    let prepared = Arc::new(
        CatalogPreparation::new(workspace, budget, base, MetadataLimits::default())
            .await?
            .finish()
            .await?,
    );
    let ready = prepared
        .ready_initialization(super::mutation_identity()?)
        .await?;
    let registered = ready
        .persist_recovery(&store, super::mutation_identity()?)
        .await?;
    let committed = ready.complete(&registered, &store).await?;
    let InitializationReply::Initialized(fact) = committed.output else {
        return Err(Error::Command("repository initialization publication denied").into());
    };
    verify(*fact, &store, repository.object_format).await?;
    tracing::debug!(repository = %hex::encode(repository.id), elapsed_seconds = started_at.elapsed().as_secs_f64(), "certified repository catalog initialized");
    Ok(())
}

fn fixed<const N: usize>(value: &SqlValue) -> Result<[u8; N], Error> {
    match value {
        SqlValue::Blob(bytes) => bytes
            .as_slice()
            .try_into()
            .map_err(|_| Error::Command("invalid prior initialization binding")),
        _ => Err(Error::Command("invalid prior initialization binding")),
    }
}
async fn prior_attempt(
    client: &CellClient,
    repository: &RepositoryCell,
    input: &BeginRequest,
    minimum: Receipt,
) -> Result<LeaseCheck, Failure> {
    let sql = SqlCell::<RepositoryModule>::new(client.clone(), repository.target.clone())?;
    let observed = sql.query(Some(minimum), SqlBatch { statements: vec![SqlStatement {
        sql: "SELECT o.incarnation,o.owner_epoch,o.admission_sequence,o.artifact_operation FROM catalog_operations o JOIN repository_identity r ON r.singleton=1 WHERE o.id=?1 AND o.actor=?2 AND o.request_digest=?3 AND o.generation=0 AND r.owner=?2 AND r.repository_id=?4 AND r.object_format=?5".into(),
        parameters: vec![SqlValue::Blob(input.operation.to_vec()), SqlValue::Text(input.actor.clone()), SqlValue::Blob(input.request_digest.to_vec()), SqlValue::Blob(repository.id.to_vec()), SqlValue::Text(repository.object_format.as_str().into())],
    }] }).await?;
    let Some([incarnation, epoch, SqlValue::Integer(sequence), operation]) = observed
        .output
        .first()
        .and_then(|set| set.rows.first())
        .map(Vec::as_slice)
    else {
        return Err(Error::Command("prior initialization attempt absent").into());
    };
    let attempt = u64::try_from(*sequence)
        .map_err(|_| Error::Command("invalid prior initialization sequence"))?;
    if attempt == 0 {
        return Err(Error::Command("invalid prior initialization sequence").into());
    }
    Ok(LeaseCheck {
        actor: input.actor.clone(),
        token: PreparationToken {
            repository: repository.id,
            operation: input.operation,
            request_digest: input.request_digest,
            owner: OwnerFence {
                incarnation: IncarnationId::from_bytes(fixed(incarnation)?),
                epoch: u64::from_be_bytes(fixed(epoch)?),
            },
            attempt,
            artifact_operation: fixed(operation)?,
        },
    })
}
