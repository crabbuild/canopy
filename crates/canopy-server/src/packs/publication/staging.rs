//! Long-running input custody without a catalog generation floor. Late binding
//! preserves the creating namespace and expiry and grants no verification proof.
use super::{commands::*, sql::*, *};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StagingLease {
    pub token: PreparationToken,
    pub format: ObjectFormat,
    pub observed_at_ms: i64,
    pub expires_at_ms: i64,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StagingReply {
    Granted(Box<StagingLease>),
    Denied(PreparationDenial),
}
fn denied(reason: PreparationDenial) -> CommandResult<StagingReply> {
    CommandResult::Rejected(StagingReply::Denied(reason))
}
fn granted(
    row: &Operation,
    format: ObjectFormat,
    now: i64,
) -> cellule_runtime::Result<StagingLease> {
    if row.generation.is_some() || row.expires <= now {
        return Err(Error::Command("input staging phase differs"));
    }
    Ok(StagingLease {
        token: row.token,
        format,
        observed_at_ms: now,
        expires_at_ms: row.expires,
    })
}
fn insert_operation(
    context: &CommandContext<'_, '_>,
    row: &Operation,
) -> cellule_runtime::Result<()> {
    context.sql(&statement("INSERT INTO catalog_operations(id,actor,request_digest,incarnation,owner_epoch,admission_sequence,artifact_operation,generation,expires_at_ms) VALUES(?1,?2,?3,?4,?5,?6,?7,NULL,?8)", vec![blob(row.token.operation),SqlValue::Text(row.actor.clone()),blob(row.token.request_digest),blob(row.token.owner.incarnation.as_bytes()),blob(row.token.owner.epoch.to_be_bytes()),number(row.token.attempt)?,blob(row.token.artifact_operation),SqlValue::Integer(row.expires)]))?;
    Ok(())
}

/// Allocate durable input custody using the same bounded attempt and pin rows.
/// A staging lease cannot open a PreparationBaseResolver or certify publication.
pub struct BeginStaging;
impl Command for BeginStaging {
    const MODULE: &'static str = RepositoryModule::NAME;
    const ID: u32 = 24;
    const CODEC_VERSION: u32 = 1;
    type Input = BeginRequest;
    type Output = StagingReply;
    fn execute(
        context: &mut CommandContext<'_, '_>,
        input: BeginRequest,
    ) -> cellule_runtime::Result<CommandResult<StagingReply>> {
        let Some(format) = authorized(context, input.repository, &input.actor, TokenScope::Write)?
        else {
            return Ok(denied(PreparationDenial::Unauthorized));
        };
        if !logical_available(context, &input)? {
            return Ok(denied(PreparationDenial::Conflict));
        }
        let now = now(context.now_ms())?;
        let expires = expiry(now, input.lease_ms)?;
        if let Some(row) = operation(
            &context.sql(&statement(OPERATION, vec![blob(input.operation)]))?,
            input.repository,
            input.operation,
        )? {
            if row.actor != input.actor
                || row.token.request_digest != input.request_digest
                || row.generation.is_some()
            {
                return Ok(denied(PreparationDenial::Conflict));
            }
            if row.token.owner != context.owner_fence() {
                return Ok(denied(PreparationDenial::Stale));
            }
            if row.expires <= now {
                return Ok(denied(PreparationDenial::Expired));
            }
            check_pin(context, &row)?;
            return Ok(CommandResult::Success(StagingReply::Granted(Box::new(
                granted(&row, format, now)?,
            ))));
        }
        if !quota(context, true)? {
            return Ok(denied(PreparationDenial::Capacity));
        }
        let next = token(
            context,
            input.repository,
            input.operation,
            input.request_digest,
        )?;
        insert_lease(context, next, None, expires)?;
        let row = Operation {
            actor: input.actor.clone(),
            token: next,
            generation: None,
            expires,
        };
        insert_operation(context, &row)?;
        let lease = granted(&row, format, now)?;
        super::staging_receipt::save(context, &input, lease)?;
        Ok(CommandResult::Success(StagingReply::Granted(Box::new(
            lease,
        ))))
    }
}

/// Extend only artifact custody. A staging renewal never acquires a floor.
pub struct RenewStaging;
impl Command for RenewStaging {
    const MODULE: &'static str = RepositoryModule::NAME;
    const ID: u32 = 25;
    const CODEC_VERSION: u32 = 1;
    type Input = LeaseRequest;
    type Output = StagingReply;
    fn execute(
        context: &mut CommandContext<'_, '_>,
        input: LeaseRequest,
    ) -> cellule_runtime::Result<CommandResult<StagingReply>> {
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
        let Some(mut row) = load(context, check.token)? else {
            return Ok(denied(PreparationDenial::Missing));
        };
        if !matched(&row, &check) {
            return Ok(denied(PreparationDenial::Stale));
        }
        if row.generation.is_some() {
            return Ok(denied(PreparationDenial::Conflict));
        }
        check_pin(context, &row)?;
        let now = now(context.now_ms())?;
        if row.expires <= now {
            return Ok(denied(PreparationDenial::Expired));
        }
        row.expires = row.expires.max(expiry(now, input.lease_ms)?);
        context.sql(&statement("UPDATE catalog_leases SET expires_at_ms=?1 WHERE incarnation=?2 AND admission_sequence=?3", vec![SqlValue::Integer(row.expires),blob(row.token.owner.incarnation.as_bytes()),number(row.token.attempt)?]))?;
        context.sql(&statement(
            "UPDATE catalog_operations SET expires_at_ms=?1 WHERE id=?2",
            vec![SqlValue::Integer(row.expires), blob(row.token.operation)],
        ))?;
        Ok(CommandResult::Success(StagingReply::Granted(Box::new(
            granted(&row, format, now)?,
        ))))
    }
}

/// Bind once to the current generation, after input upload/normalization. This
/// command neither shortens existing input custody nor renews it. Repeating it
/// returns the original floor; further frontier selection is read-only.
pub struct BindStaging;
impl Command for BindStaging {
    const MODULE: &'static str = RepositoryModule::NAME;
    const ID: u32 = 26;
    const CODEC_VERSION: u32 = 1;
    type Input = LeaseCheck;
    type Output = PreparationReply;
    fn execute(
        context: &mut CommandContext<'_, '_>,
        check: LeaseCheck,
    ) -> cellule_runtime::Result<CommandResult<PreparationReply>> {
        let deny = |reason| CommandResult::Rejected(PreparationReply::Denied(reason));
        let Some(format) = authorized(
            context,
            check.token.repository,
            &check.actor,
            TokenScope::Write,
        )?
        else {
            return Ok(deny(PreparationDenial::Unauthorized));
        };
        if check.token.owner != context.owner_fence() {
            return Ok(deny(PreparationDenial::Stale));
        }
        let Some(mut row) = load(context, check.token)? else {
            return Ok(deny(PreparationDenial::Missing));
        };
        if !matched(&row, &check) {
            return Ok(deny(PreparationDenial::Stale));
        }
        check_pin(context, &row)?;
        let now = now(context.now_ms())?;
        if row.expires <= now {
            return Ok(deny(PreparationDenial::Expired));
        }
        let base = fact(context, check.token.repository, format, row.generation)?;
        if row.generation.is_none() {
            context.sql(&statement("UPDATE catalog_leases SET generation=?1 WHERE incarnation=?2 AND admission_sequence=?3 AND generation IS NULL", vec![number(base.generation)?,blob(row.token.owner.incarnation.as_bytes()),number(row.token.attempt)?]))?;
            context.sql(&statement(
                "UPDATE catalog_operations SET generation=?1 WHERE id=?2 AND generation IS NULL",
                vec![number(base.generation)?, blob(row.token.operation)],
            ))?;
            row.generation = Some(base.generation);
        }
        Ok(CommandResult::Success(PreparationReply::Granted(Box::new(
            grant(&row, format, base, now)?,
        ))))
    }
}

/// Recover input admission under a new admitted attempt. The previous namespace
/// remains independently retained; no unchecked cross-attempt input adoption.
pub struct ClaimStaging;
impl Command for ClaimStaging {
    const MODULE: &'static str = RepositoryModule::NAME;
    const ID: u32 = 28;
    const CODEC_VERSION: u32 = 2;
    type Input = LeaseRequest;
    type Output = StagingReply;
    fn execute(
        context: &mut CommandContext<'_, '_>,
        input: LeaseRequest,
    ) -> cellule_runtime::Result<CommandResult<StagingReply>> {
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
        let Some(row) = load(context, check.token)? else {
            if !super::staging_receipt::restart_matches(context, &check)?
                && !super::custody::restart_matches(context, &check, true)?
            {
                return Ok(denied(PreparationDenial::Missing));
            }
            let begin = BeginRequest {
                repository: check.token.repository,
                operation: check.token.operation,
                request_digest: check.token.request_digest,
                actor: check.actor.clone(),
                lease_ms: input.lease_ms,
            };
            if !logical_available(context, &begin)? {
                return Ok(denied(PreparationDenial::Conflict));
            }
            if !quota(context, true)? {
                return Ok(denied(PreparationDenial::Capacity));
            }
            let now = now(context.now_ms())?;
            let expires = expiry(now, input.lease_ms)?;
            let next = token(
                context,
                begin.repository,
                begin.operation,
                begin.request_digest,
            )?;
            insert_lease(context, next, None, expires)?;
            let row = Operation {
                actor: check.actor,
                token: next,
                generation: None,
                expires,
            };
            insert_operation(context, &row)?;
            return Ok(CommandResult::Success(StagingReply::Granted(Box::new(
                granted(&row, format, now)?,
            ))));
        };
        if !matched(&row, &check) {
            return Ok(denied(PreparationDenial::Stale));
        }
        if row.generation.is_some() {
            return Ok(denied(PreparationDenial::Conflict));
        }
        check_pin(context, &row)?;
        if !quota(context, false)? {
            return Ok(denied(PreparationDenial::Capacity));
        }
        let now = now(context.now_ms())?;
        let expires = expiry(now, input.lease_ms)?;
        let next = token(
            context,
            check.token.repository,
            check.token.operation,
            check.token.request_digest,
        )?;
        insert_lease(context, next, None, expires)?;
        context.sql(&statement("UPDATE catalog_operations SET incarnation=?1,owner_epoch=?2,admission_sequence=?3,artifact_operation=?4,expires_at_ms=?5 WHERE id=?6", vec![blob(next.owner.incarnation.as_bytes()),blob(next.owner.epoch.to_be_bytes()),number(next.attempt)?,blob(next.artifact_operation),SqlValue::Integer(expires),blob(next.operation)]))?;
        let row = Operation {
            actor: check.actor,
            token: next,
            generation: None,
            expires,
        };
        Ok(CommandResult::Success(StagingReply::Granted(Box::new(
            granted(&row, format, now)?,
        ))))
    }
}

pub struct CheckStaging;
impl Query for CheckStaging {
    const MODULE: &'static str = RepositoryModule::NAME;
    const ID: u32 = 27;
    const CODEC_VERSION: u32 = 1;
    type Input = LeaseCheck;
    type Output = Option<StagingLease>;
    fn execute(
        context: &mut QueryContext<'_>,
        check: LeaseCheck,
    ) -> cellule_runtime::Result<Self::Output> {
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
        let now = now(context.now_ms())?;
        if !matched(&row, &check) || row.generation.is_some() || row.expires <= now {
            return Ok(None);
        }
        pin(&context.sql(&pin_query(row.token)?)?, &row)?;
        Ok(Some(granted(&row, format, now)?))
    }
}
