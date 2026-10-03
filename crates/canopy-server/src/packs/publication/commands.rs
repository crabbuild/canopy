use super::sql::*;
use super::*;
fn denied(reason: PreparationDenial) -> CommandResult<PreparationReply> {
    CommandResult::Rejected(PreparationReply::Denied(reason))
}
pub(super) fn authorized(
    context: &CommandContext<'_, '_>,
    repository: [u8; 16],
    actor: &str,
    role: TokenScope,
) -> cellule_runtime::Result<Option<ObjectFormat>> {
    validate_component(actor)?;
    if context.target()
        != &crate::repository_target(
            context.target().tenant(),
            context.target().application(),
            repository,
        )?
    {
        return Ok(None);
    }
    if !decode_access(&context.sql(&SqlBatch {
        statements: vec![access_statement(actor)],
    })?)?
    .is_some_and(|value| value >= role)
    {
        return Ok(None);
    }
    identity(&context.sql(&statement(IDENTITY, vec![]))?, repository)
}
pub(super) fn load(
    context: &CommandContext<'_, '_>,
    token: PreparationToken,
) -> cellule_runtime::Result<Option<Operation>> {
    operation(
        &context.sql(&statement(OPERATION, vec![blob(token.operation)]))?,
        token.repository,
        token.operation,
    )
}
pub(super) fn fact(
    context: &CommandContext<'_, '_>,
    repository: [u8; 16],
    format: ObjectFormat,
    base: Option<u64>,
) -> cellule_runtime::Result<GenerationFact> {
    generation(
        &context.sql(&match base {
            None => statement(CURRENT, vec![]),
            Some(value) => statement(GENERATION, vec![number(value)?]),
        })?,
        repository,
        format,
    )
}
pub(super) fn matched(row: &Operation, check: &LeaseCheck) -> bool {
    row.actor == check.actor && row.token == check.token
}
pub(super) fn pin(sets: &[SqlResultSet], row: &Operation) -> cellule_runtime::Result<()> {
    let Some(
        [
            operation,
            epoch,
            artifact_operation,
            generation,
            SqlValue::Integer(expires),
        ],
    ) = rows(sets)?.first().map(Vec::as_slice)
    else {
        return Err(Error::Command("catalog attempt pin is absent"));
    };
    if fixed::<16>(operation)? != row.token.operation
        || u64::from_be_bytes(fixed(epoch)?) != row.token.owner.epoch
        || fixed::<16>(artifact_operation)? != row.token.artifact_operation
        || optional_generation(generation)? != row.generation
        || *expires != row.expires
    {
        return Err(Error::Command("catalog attempt pin differs"));
    }
    Ok(())
}
pub(super) fn pin_query(token: PreparationToken) -> cellule_runtime::Result<SqlBatch> {
    Ok(statement(
        "SELECT operation,owner_epoch,artifact_operation,generation,expires_at_ms FROM catalog_leases WHERE incarnation=?1 AND admission_sequence=?2",
        vec![
            blob(token.owner.incarnation.as_bytes()),
            number(token.attempt)?,
        ],
    ))
}
pub(super) fn check_pin(
    context: &CommandContext<'_, '_>,
    row: &Operation,
) -> cellule_runtime::Result<()> {
    pin(&context.sql(&pin_query(row.token)?)?, row)
}
pub(super) fn token(
    context: &CommandContext<'_, '_>,
    repository: [u8; 16],
    operation: [u8; 16],
    request_digest: [u8; 32],
) -> cellule_runtime::Result<PreparationToken> {
    if operation == [0; 16] || context.sequence() == 0 || context.sequence() > i64::MAX as u64 {
        return Err(Error::Command("invalid catalog attempt identity"));
    }
    Ok(PreparationToken {
        repository,
        operation,
        artifact_operation: allocate_artifacts(context)?,
        request_digest,
        owner: context.owner_fence(),
        attempt: context.sequence(),
    })
}

pub(super) fn logical_available(
    context: &CommandContext<'_, '_>,
    input: &BeginRequest,
) -> cellule_runtime::Result<bool> {
    // A logical outcome already exists: callers must look it up before
    // native preparation. Never allocate another namespace for a completed
    // push, or admit an identity conflicting with a pending network push.
    if !rows(&context.sql(&statement(
        "SELECT id FROM catalog_compactions WHERE id=?1 UNION ALL SELECT id FROM catalog_initialization WHERE id=?1",
        vec![blob(input.operation)],
    ))?)?
    .is_empty()
    {
        return Ok(false);
    }
    let saved = context.sql(&statement(
        "SELECT actor,request_digest,response_id,publication FROM pushes WHERE id=?1",
        vec![blob(input.operation)],
    ))?;
    if let Some(row) = rows(&saved)?.first() {
        let [SqlValue::Text(actor), digest, response, publication] = row.as_slice() else {
            return Err(Error::Command("invalid preparation push identity"));
        };
        if *actor != input.actor
            || fixed::<32>(digest)? != input.request_digest
            || *response != SqlValue::Null
            || *publication != SqlValue::Null
        {
            return Ok(false);
        }
    }
    Ok(true)
}

pub struct BeginPreparation;
impl Command for BeginPreparation {
    const MODULE: &'static str = RepositoryModule::NAME;
    const ID: u32 = 11;
    const CODEC_VERSION: u32 = 1;
    type Input = BeginRequest;
    type Output = PreparationReply;
    fn execute(
        context: &mut CommandContext<'_, '_>,
        input: BeginRequest,
    ) -> cellule_runtime::Result<CommandResult<PreparationReply>> {
        let Some(format) = authorized(context, input.repository, &input.actor, TokenScope::Write)?
        else {
            return Ok(denied(PreparationDenial::Unauthorized));
        };
        if !logical_available(context, &input)? {
            return Ok(denied(PreparationDenial::Conflict));
        }
        let now = now(context.now_ms())?;
        let expires = expiry(now, input.lease_ms)?;
        if let Some(existing) = operation(
            &context.sql(&statement(OPERATION, vec![blob(input.operation)]))?,
            input.repository,
            input.operation,
        )? {
            if existing.actor != input.actor
                || existing.token.request_digest != input.request_digest
            {
                return Ok(denied(PreparationDenial::Conflict));
            }
            if existing.token.owner != context.owner_fence() {
                return Ok(denied(PreparationDenial::Stale));
            }
            if existing.expires <= now {
                return Ok(denied(PreparationDenial::Expired));
            }
            if existing.generation.is_none() {
                return Ok(denied(PreparationDenial::Conflict));
            }
            check_pin(context, &existing)?;
            let base = fact(context, input.repository, format, existing.generation)?;
            return Ok(CommandResult::Success(PreparationReply::Granted(Box::new(
                grant(&existing, format, base, now)?,
            ))));
        }
        if !quota(context, true)? {
            return Ok(denied(PreparationDenial::Capacity));
        }
        let base = fact(context, input.repository, format, None)?;
        let new_token = token(
            context,
            input.repository,
            input.operation,
            input.request_digest,
        )?;
        insert_lease(context, new_token, Some(base.generation), expires)?;
        context.sql(&statement("INSERT INTO catalog_operations(id,actor,request_digest,incarnation,owner_epoch,admission_sequence,artifact_operation,generation,expires_at_ms) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9)",vec![blob(input.operation),SqlValue::Text(input.actor.clone()),blob(input.request_digest),blob(new_token.owner.incarnation.as_bytes()),blob(new_token.owner.epoch.to_be_bytes()),number(new_token.attempt)?,blob(new_token.artifact_operation),number(base.generation)?,SqlValue::Integer(expires)]))?;
        let row = Operation {
            actor: input.actor,
            token: new_token,
            generation: Some(base.generation),
            expires,
        };
        Ok(CommandResult::Success(PreparationReply::Granted(Box::new(
            grant(&row, format, base, now)?,
        ))))
    }
}

pub struct ClaimPreparation;
impl Command for ClaimPreparation {
    const MODULE: &'static str = RepositoryModule::NAME;
    const ID: u32 = 12;
    const CODEC_VERSION: u32 = 1;
    type Input = LeaseRequest;
    type Output = PreparationReply;
    fn execute(
        context: &mut CommandContext<'_, '_>,
        input: LeaseRequest,
    ) -> cellule_runtime::Result<CommandResult<PreparationReply>> {
        let check = input.check;
        let Some(format) = authorized(
            context,
            check.token.repository,
            &check.actor,
            TokenScope::Write,
        )?
        else {
            return Ok(denied(PreparationDenial::Unauthorized));
        };
        let Some(existing) = load(context, check.token)? else {
            return Ok(denied(PreparationDenial::Missing));
        };
        if !matched(&existing, &check) {
            return Ok(denied(PreparationDenial::Stale));
        }
        if existing.generation.is_none() {
            return Ok(denied(PreparationDenial::Conflict));
        }
        check_pin(context, &existing)?;
        if !quota(context, false)? {
            return Ok(denied(PreparationDenial::Capacity));
        }
        let now = now(context.now_ms())?;
        let expires = expiry(now, input.lease_ms)?;
        let base = fact(context, check.token.repository, format, None)?;
        let next = token(
            context,
            check.token.repository,
            check.token.operation,
            check.token.request_digest,
        )?;
        insert_lease(context, next, Some(base.generation), expires)?;
        // Keep the previous pin unchanged, even when rebasing to a new root.
        context.sql(&statement("UPDATE catalog_operations SET incarnation=?1,owner_epoch=?2,admission_sequence=?3,generation=?4,expires_at_ms=?5,attestation=NULL,attestation_digest=NULL,artifact_operation=?7 WHERE id=?6",vec![blob(next.owner.incarnation.as_bytes()),blob(next.owner.epoch.to_be_bytes()),number(next.attempt)?,number(base.generation)?,SqlValue::Integer(expires),blob(next.operation),blob(next.artifact_operation)]))?;
        let row = Operation {
            actor: check.actor,
            token: next,
            generation: Some(base.generation),
            expires,
        };
        Ok(CommandResult::Success(PreparationReply::Granted(Box::new(
            grant(&row, format, base, now)?,
        ))))
    }
}

pub struct RenewPreparation;
impl Command for RenewPreparation {
    const MODULE: &'static str = RepositoryModule::NAME;
    const ID: u32 = 13;
    const CODEC_VERSION: u32 = 1;
    type Input = LeaseRequest;
    type Output = PreparationReply;
    fn execute(
        context: &mut CommandContext<'_, '_>,
        input: LeaseRequest,
    ) -> cellule_runtime::Result<CommandResult<PreparationReply>> {
        let check = input.check;
        let Some(format) = authorized(
            context,
            check.token.repository,
            &check.actor,
            TokenScope::Write,
        )?
        else {
            return Ok(denied(PreparationDenial::Unauthorized));
        };
        if check.token.owner != context.owner_fence() {
            return Ok(denied(PreparationDenial::Stale));
        }
        let Some(mut existing) = load(context, check.token)? else {
            return Ok(denied(PreparationDenial::Missing));
        };
        if !matched(&existing, &check) {
            return Ok(denied(PreparationDenial::Stale));
        }
        if existing.generation.is_none() {
            return Ok(denied(PreparationDenial::Conflict));
        }
        check_pin(context, &existing)?;
        let now = now(context.now_ms())?;
        if existing.expires <= now {
            return Ok(denied(PreparationDenial::Expired));
        }
        let base = fact(context, check.token.repository, format, existing.generation)?;
        existing.expires = existing.expires.max(expiry(now, input.lease_ms)?);
        context.sql(&statement("UPDATE catalog_leases SET expires_at_ms=?1 WHERE incarnation=?2 AND admission_sequence=?3",vec![SqlValue::Integer(existing.expires),blob(existing.token.owner.incarnation.as_bytes()),number(existing.token.attempt)?]))?;
        context.sql(&statement(
            "UPDATE catalog_operations SET expires_at_ms=?1 WHERE id=?2",
            vec![
                SqlValue::Integer(existing.expires),
                blob(existing.token.operation),
            ],
        ))?;
        Ok(CommandResult::Success(PreparationReply::Granted(Box::new(
            grant(&existing, format, base, now)?,
        ))))
    }
}

pub struct AbortPreparation;
impl Command for AbortPreparation {
    const MODULE: &'static str = RepositoryModule::NAME;
    const ID: u32 = 14;
    const CODEC_VERSION: u32 = 1;
    type Input = LeaseCheck;
    type Output = bool;
    fn execute(
        context: &mut CommandContext<'_, '_>,
        check: LeaseCheck,
    ) -> cellule_runtime::Result<CommandResult<bool>> {
        if authorized(
            context,
            check.token.repository,
            &check.actor,
            TokenScope::Write,
        )?
        .is_none()
            || check.token.owner != context.owner_fence()
        {
            return Ok(CommandResult::Rejected(false));
        }
        let Some(existing) = load(context, check.token)? else {
            return Ok(CommandResult::Rejected(false));
        };
        if !matched(&existing, &check) {
            return Ok(CommandResult::Rejected(false));
        }
        // Aborting stops admission but cannot retract an already borrowed pin.
        context.sql(&statement(
            "DELETE FROM catalog_operations WHERE id=?1",
            vec![blob(check.token.operation)],
        ))?;
        Ok(CommandResult::Success(true))
    }
}

pub struct CheckPreparation;
impl Query for CheckPreparation {
    const MODULE: &'static str = RepositoryModule::NAME;
    const ID: u32 = 15;
    const CODEC_VERSION: u32 = 1;
    type Input = LeaseCheck;
    type Output = Option<PreparationLease>;
    fn execute(
        context: &mut QueryContext<'_>,
        check: LeaseCheck,
    ) -> cellule_runtime::Result<Option<PreparationLease>> {
        validate_component(&check.actor)?;
        if !decode_access(&context.sql(&SqlBatch {
            statements: vec![access_statement(&check.actor)],
        })?)?
        .is_some_and(|role| role >= TokenScope::Write)
        {
            return Ok(None);
        }
        let Some(format) = identity(
            &context.sql(&statement(IDENTITY, vec![]))?,
            check.token.repository,
        )?
        else {
            return Ok(None);
        };
        let Some(row) = operation(
            &context.sql(&statement(OPERATION, vec![blob(check.token.operation)]))?,
            check.token.repository,
            check.token.operation,
        )?
        else {
            return Ok(None);
        };
        if !matched(&row, &check) {
            return Ok(None);
        }
        let now = now(context.now_ms())?;
        if row.expires <= now {
            return Ok(None);
        }
        let Some(floor) = row.generation else {
            return Ok(None);
        };
        pin(&context.sql(&pin_query(row.token)?)?, &row)?;
        let base = generation(
            &context.sql(&statement(GENERATION, vec![number(floor)?]))?,
            check.token.repository,
            format,
        )?;
        Ok(Some(grant(&row, format, base, now)?))
    }
}

/// Refresh catalog facts under the original attempt without allocating a new
/// namespace, changing its retention floor or submitting a durable Claim.
pub struct CheckPreparationFrontier;
impl Query for CheckPreparationFrontier {
    const MODULE: &'static str = RepositoryModule::NAME;
    const ID: u32 = 21;
    const CODEC_VERSION: u32 = 1;
    type Input = LeaseCheck;
    type Output = Option<PreparationFrontier>;
    fn execute(
        context: &mut QueryContext<'_>,
        check: LeaseCheck,
    ) -> cellule_runtime::Result<Self::Output> {
        let Some(mut lease) = CheckPreparation::execute(context, check)? else {
            return Ok(None);
        };
        // Both facts come from this query's one committed SQLite snapshot.
        let current = generation(
            &context.sql(&statement(CURRENT, vec![]))?,
            lease.token.repository,
            lease.format,
        )?;
        // Sampling before the worker queue must not extend a usable lease.
        lease.observed_at_ms = now(context.now_ms())?;
        if lease.observed_at_ms >= lease.expires_at_ms {
            return Ok(None);
        }
        let frontier = PreparationFrontier { lease, current };
        frontier.validate()?;
        Ok(Some(frontier))
    }
}

pub struct ReapPreparation;
pub(super) const REAP_GENERATIONS: &str = "DELETE FROM catalog_generations WHERE generation IN (SELECT g.generation FROM catalog_generations g WHERE g.generation>0 AND g.generation<(SELECT generation FROM catalog_state WHERE singleton=1) AND g.generation<COALESCE((SELECT min(generation) FROM catalog_leases),9223372036854775807) ORDER BY g.generation LIMIT ?1)";
impl Command for ReapPreparation {
    const MODULE: &'static str = RepositoryModule::NAME;
    const ID: u32 = 16;
    const CODEC_VERSION: u32 = 1;
    type Input = MaintenanceRequest;
    type Output = u64;
    fn execute(
        context: &mut CommandContext<'_, '_>,
        input: MaintenanceRequest,
    ) -> cellule_runtime::Result<CommandResult<u64>> {
        if input.owner != context.owner_fence()
            || authorized(context, input.repository, &input.actor, TokenScope::Admin)?.is_none()
        {
            return Ok(CommandResult::Rejected(0));
        }
        let now = now(context.now_ms())?;
        let operations=context.sql(&statement("DELETE FROM catalog_operations WHERE id IN (SELECT id FROM catalog_operations WHERE expires_at_ms<=?1 ORDER BY expires_at_ms,id LIMIT ?2)",vec![SqlValue::Integer(now),number(REAP_ROWS)?]))?;
        let leases=context.sql(&statement("DELETE FROM catalog_leases WHERE (incarnation,admission_sequence) IN (SELECT l.incarnation,l.admission_sequence FROM catalog_leases l WHERE l.expires_at_ms<=?1 AND NOT EXISTS(SELECT 1 FROM catalog_operations o WHERE o.incarnation=l.incarnation AND o.admission_sequence=l.admission_sequence) ORDER BY l.expires_at_ms,l.incarnation,l.admission_sequence LIMIT ?2)",vec![SqlValue::Integer(now),number(REAP_ROWS)?]))?;
        // This only removes obsolete facts from the current SQL state. Old
        // recovery snapshots retain their own facts. An independent attempt's
        // floor protects all later facts, including generations selected by a
        // read-only frontier refresh. Expired pins protect until actually reaped.
        // Remote deletion still needs all recovery/backup/reader retention.
        let generations = context.sql(&statement(REAP_GENERATIONS, vec![number(REAP_ROWS)?]))?;
        let removed = operations
            .first()
            .ok_or(Error::Command("missing catalog operation reap"))?
            .rows_affected
            + leases
                .first()
                .ok_or(Error::Command("missing catalog lease reap"))?
                .rows_affected
            + generations
                .first()
                .ok_or(Error::Command("missing catalog generation reap"))?
                .rows_affected;
        Ok(CommandResult::Success(removed))
    }
}
