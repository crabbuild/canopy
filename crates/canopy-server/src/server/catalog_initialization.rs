//! Certified empty-catalog initialization before exposing a repository route.
use crate::{
    ObjectFormat, RepositoryCell, RepositoryModule,
    packs::{
        catalog::{CatalogFileLimits, CatalogFiles, CatalogIndexes},
        metadata::MetadataLimits,
        publication::{
            BeginRequest, CatalogPreparation, CheckInitializedCatalog, CheckPreparation,
            CustodyAction, CustodyError, DEFAULT_LEASE_MS, GenerationFact, InitializationReply,
            LeaseCheck, LeaseRequest, MaintenanceRequest, PreparationAuthority,
            PreparationBaseResolver, PreparationDenial, PreparationReply, PreparationToken,
            PreparedCustody, PublicationError, RegisteredCustody, RegisteredRootRecovery,
            TerminalReleaseReply,
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

fn initialization_custody(action: &CustodyAction, input: &BeginRequest) -> bool {
    match action {
        CustodyAction::BeginPreparation(request) => request == input,
        CustodyAction::ClaimPreparation(request) | CustodyAction::RenewPreparation(request) => {
            let check = &request.check;
            check.actor == input.actor
                && check.token.repository == input.repository
                && check.token.operation == input.operation
                && check.token.request_digest == input.request_digest
        }
        _ => false,
    }
}
async fn custody_command(
    client: &CellClient,
    target: &cellule_runtime::CellTarget,
    action: CustodyAction,
) -> Result<cellule_runtime::Committed<PreparationReply>, Failure> {
    let prepared =
        PreparedCustody::prepare(client, target, action, super::mutation_identity()?).await?;
    let registered = prepared
        .register(client, super::mutation_identity()?)
        .await?;
    Ok(registered.recover_preparation(client).await?)
}

async fn verify(
    fact: GenerationFact,
    store: &ArtifactStore,
    format: ObjectFormat,
) -> Result<(), Failure> {
    crate::packs::publication::verify_initial_catalog(fact, store, format).await?;
    Ok(())
}

/// The caller's tracked cold-transition task owns this work through cancellation.
/// Ready repositories only observe their immutable initialization; they cannot
/// reconstruct missing ownership or publish a new empty catalog during restore.
pub(super) struct InitializationCustody {
    pub authority: PreparationAuthority,
    pub maintenance: MaintenanceRequest,
}

pub(super) async fn ensure(
    custody: InitializationCustody,
    repository: &RepositoryCell,
    client: CellClient,
    provider: Arc<dyn ObjectStore>,
    workspace: &Path,
    budget: DiskBudget,
    pending: bool,
) -> Result<(), Failure> {
    let InitializationCustody {
        authority,
        maintenance,
    } = custody;
    let owner = maintenance.actor.as_str();
    let input = request(repository, owner);
    let target = &repository.target;
    let store = Arc::new(ArtifactStore::new(provider, repository.id));
    if let Some(fact) = client
        .query::<CheckInitializedCatalog>(target, None, input.clone())
        .await?
        .output
    {
        return verify_and_retire(repository, &client, &store, &input, fact, &maintenance).await;
    }
    if !pending {
        return Err(Error::Command("ready repository has no certified initialization").into());
    }
    let started_at = std::time::Instant::now();
    let recovered =
        RegisteredRootRecovery::load_initialization(&client, target, &store, &input).await?;
    let claim = if let Some(ref recovered) = recovered {
        match recovered
            .recover_initialization(&client, &store, &authority)
            .await
        {
            Ok(committed) => {
                let InitializationReply::Initialized(fact) = committed.output else {
                    return Err(Error::Command("invalid recovered initialization reply").into());
                };
                verify(*fact, &store, repository.object_format).await?;
                retire(recovered, client, &store, &maintenance).await?;
                return Ok(());
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
    // Discover the exact latest custody phase before constructing another SDK
    // identity. Both accepted and denied Begin/Claim/Renew survive process loss.
    let custody = startup_head(&client, target, &input, &authority).await?;
    let refused_attempt = claim.as_ref().map(|check| check.token);
    let action = if let Some(check) = claim {
        CustodyAction::ClaimPreparation(LeaseRequest {
            check,
            lease_ms: DEFAULT_LEASE_MS,
        })
    } else {
        CustodyAction::BeginPreparation(input.clone())
    };
    if let Some(ref custody) = custody
        && !initialization_custody(&custody.action()?, &input)
    {
        return Err(Error::Command("initialization custody context differs").into());
    }
    // A stop closes registration, not execution: never manufacture a receipt or
    // denial for the original. Observe the current operation after the separate
    // authenticated stop receipt, then let the new receiver authorize a successor.
    let stopped = custody.as_ref().and_then(RegisteredCustody::stop_fact);
    let action = if let Some(stopped) = stopped {
        match observed_attempt(&client, repository, &input, stopped.receipt).await? {
            Some(check) => CustodyAction::ClaimPreparation(LeaseRequest {
                check,
                lease_ms: DEFAULT_LEASE_MS,
            }),
            None => CustodyAction::BeginPreparation(input.clone()),
        }
    } else {
        action
    };
    let replay = custody.as_ref().filter(|saved| saved.stop_fact().is_none());
    let result = if let Some(custody) = replay {
        custody
            .recover_preparation(&client)
            .await
            .map_err(|error| Box::new(error) as Failure)
    } else {
        custody_command(&client, target, action.clone()).await
    };
    let started = match result {
        Ok(started) => started,
        Err(error) => {
            let error = match error.downcast::<InvocationError<PreparationReply>>() {
                Ok(error) => *error,
                Err(error) => return Err(error),
            };
            if let InvocationError::Rejected(ref rejected) = error
                && matches!(
                    rejected.output,
                    PreparationReply::Denied(PreparationDenial::Stale | PreparationDenial::Expired)
                )
            {
                let prior = replay
                    .map(RegisteredCustody::action)
                    .transpose()?
                    .unwrap_or(action);
                let check = match prior {
                    CustodyAction::ClaimPreparation(request)
                    | CustodyAction::RenewPreparation(request) => request.check,
                    CustodyAction::BeginPreparation(_) => {
                        prior_attempt(&client, repository, &input, rejected.receipt).await?
                    }
                    _ => {
                        return Err(Error::Command("initialization custody purpose differs").into());
                    }
                };
                custody_command(
                    &client,
                    target,
                    CustodyAction::ClaimPreparation(LeaseRequest {
                        check,
                        lease_ms: DEFAULT_LEASE_MS,
                    }),
                )
                .await?
            } else {
                // Only a known conflict can observe a winning initialization.
                // Unknown/expired SDK evidence never authorizes a new Begin.
                if matches!(&error, InvocationError::Rejected(value) if value.output == PreparationReply::Denied(PreparationDenial::Conflict))
                    && let Some(fact) = client
                        .query::<CheckInitializedCatalog>(target, None, input.clone())
                        .await?
                        .output
                {
                    return verify_and_retire(
                        repository,
                        &client,
                        &store,
                        &input,
                        fact,
                        &maintenance,
                    )
                    .await;
                }
                return Err(error.into());
            }
        }
    };
    let PreparationReply::Granted(ref original) = started.output else {
        return Err(Error::Command("initialization custody grant absent").into());
    };
    // A historical result is knowledge only. Fresh custody and the actual owner
    // are required before using its token; never restart its recorded clock.
    let check = LeaseCheck {
        token: original.token,
        actor: owner.into(),
    };
    let current =
        if original.token.owner == maintenance.owner && refused_attempt != Some(original.token) {
            client
                .query::<CheckPreparation>(target, Some(started.receipt), check.clone())
                .await?
                .output
        } else {
            None
        };
    let started = if let Some(current) = current {
        if current.token != original.token
            || current.base != original.base
            || current.format != original.format
        {
            return Err(Error::Command("initialization custody result differs").into());
        }
        started
    } else {
        custody_command(
            &client,
            target,
            CustodyAction::ClaimPreparation(LeaseRequest {
                check,
                lease_ms: DEFAULT_LEASE_MS,
            }),
        )
        .await?
    };
    if let Some(recovered) = recovered {
        retire(&recovered, client.clone(), &store, &maintenance).await?;
    }
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
            authority.clone(),
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
    retire(&registered, client, &store, &maintenance).await?;
    tracing::debug!(repository = %hex::encode(repository.id), elapsed_seconds = started_at.elapsed().as_secs_f64(), "certified repository catalog initialized");
    Ok(())
}

/// Already owned by the account-bounded, tracked cold transition. No background
/// outbox or new native work is introduced; ambiguous original outcomes retain
/// their exact identity, and only receiver-accepted closure permits a successor.
async fn startup_head(
    client: &CellClient,
    target: &cellule_runtime::CellTarget,
    input: &BeginRequest,
    authority: &PreparationAuthority,
) -> Result<Option<RegisteredCustody>, Failure> {
    let Some(saved) = RegisteredCustody::load_latest(client, target, input.operation).await? else {
        return Ok(None);
    };
    if !initialization_custody(&saved.action()?, input) {
        return Err(Error::Command("initialization custody context differs").into());
    }
    if saved.closed() || saved.evidence().identity().expires_at_ms >= super::unix_now_ms()? {
        return Ok(Some(saved));
    }
    // Journal knowledge precedes SDK expiry. Never retire an already known
    // grant/denial merely because this previously loaded DTO has no phase.
    if !matches!(saved.recover_preparation(client).await,
        Err(InvocationError::Pending(ref evidence)) if **evidence == *saved.evidence())
    {
        return Ok(Some(saved));
    }
    match saved
        .ready_stop(client.clone(), super::mutation_identity()?, authority)
        .await
    {
        Ok(ready) => {
            let outcome = ready.complete_tracked().await?;
            if outcome.original != *saved.evidence() {
                return Err(Error::Command("initialization retirement original differs").into());
            }
        }
        Err(CustodyError::Stopped(_)) => {} // Another helper already recorded closure.
        Err(error) => return Err(error.into()),
    }
    let current = RegisteredCustody::load_latest(client, target, input.operation)
        .await?
        .ok_or(Error::Command("initialization custody disappeared"))?;
    if !initialization_custody(&current.action()?, input)
        || (current.evidence() == saved.evidence() && !current.closed())
    {
        return Err(Error::Command("initialization retirement not established").into());
    }
    Ok(Some(current))
}

async fn verify_and_retire(
    repository: &RepositoryCell,
    client: &CellClient,
    store: &ArtifactStore,
    input: &BeginRequest,
    fact: GenerationFact,
    maintenance: &MaintenanceRequest,
) -> Result<(), Failure> {
    verify(fact, store, repository.object_format).await?;
    if let Some(recovered) =
        RegisteredRootRecovery::load_initialization(client, &repository.target, store, input)
            .await?
    {
        retire(&recovered, client.clone(), store, maintenance).await?;
    }
    Ok(())
}

async fn retire(
    registered: &RegisteredRootRecovery,
    client: CellClient,
    store: &ArtifactStore,
    maintenance: &MaintenanceRequest,
) -> Result<(), Failure> {
    let result = registered
        .ready_terminal_release(
            client,
            store,
            maintenance.clone(),
            super::mutation_identity()?,
        )
        .await?
        .complete()
        .await?;
    if result.output != TerminalReleaseReply::Released {
        return Err(Error::Command("initialization retirement denied").into());
    }
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
    observed_attempt(client, repository, input, minimum)
        .await?
        .ok_or_else(|| Error::Command("prior initialization attempt absent").into())
}
async fn observed_attempt(
    client: &CellClient,
    repository: &RepositoryCell,
    input: &BeginRequest,
    minimum: Receipt,
) -> Result<Option<LeaseCheck>, Failure> {
    let sql = SqlCell::<RepositoryModule>::new(client.clone(), repository.target.clone())?;
    let observed = sql.query(Some(minimum), SqlBatch { statements: vec![SqlStatement {
        sql: "SELECT r.repository_id,r.object_format,r.owner,o.actor,o.request_digest,o.generation,o.incarnation,o.owner_epoch,o.admission_sequence,o.artifact_operation FROM repository_identity r LEFT JOIN catalog_operations o ON o.id=?1 WHERE r.singleton=1".into(),
        parameters: vec![SqlValue::Blob(input.operation.to_vec())],
    }] }).await?;
    let Some(
        [
            SqlValue::Blob(id),
            SqlValue::Text(format),
            SqlValue::Text(owner),
            actor,
            digest,
            generation,
            incarnation,
            epoch,
            sequence,
            operation,
        ],
    ) = observed
        .output
        .first()
        .and_then(|set| set.rows.first())
        .map(Vec::as_slice)
    else {
        return Err(Error::Command("initialization identity absent or malformed").into());
    };
    if id.as_slice() != repository.id
        || format != repository.object_format.as_str()
        || owner != &input.actor
    {
        return Err(Error::Command("initialization identity differs").into());
    }
    if [
        actor,
        digest,
        generation,
        incarnation,
        epoch,
        sequence,
        operation,
    ]
    .iter()
    .all(|value| matches!(value, SqlValue::Null))
    {
        return Ok(None);
    }
    let (
        SqlValue::Text(actor),
        SqlValue::Blob(digest),
        SqlValue::Integer(0),
        SqlValue::Integer(sequence),
    ) = (actor, digest, generation, sequence)
    else {
        return Err(Error::Command("prior initialization binding malformed").into());
    };
    if actor != &input.actor || digest.as_slice() != input.request_digest {
        return Err(Error::Command("prior initialization binding differs").into());
    }
    let attempt = u64::try_from(*sequence)
        .map_err(|_| Error::Command("invalid prior initialization sequence"))?;
    if attempt == 0 {
        return Err(Error::Command("invalid prior initialization sequence").into());
    }
    Ok(Some(LeaseCheck {
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
    }))
}

#[cfg(test)]
mod tests;
