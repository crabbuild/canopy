use super::super::sql::*;
use super::*;
use crate::ReadIdentity;
const ROW: &str = "SELECT incarnation,admission_sequence,owner_epoch,generation,expires_at_ms FROM catalog_serving_pins WHERE reader=?1";
fn access(actor: &Option<String>) -> cellule_runtime::Result<SqlBatch> {
    let actor = actor
        .as_deref()
        .map_or(ReadIdentity::Anonymous, ReadIdentity::Account);
    actor.validate()?;
    Ok(statement(
        &format!("SELECT 1 WHERE {}", crate::access::READ_ACCESS),
        vec![actor.parameter()],
    ))
}
fn context(target: &CellTarget, repository: [u8; 16]) -> cellule_runtime::Result<bool> {
    Ok(*target == crate::repository_target(target.tenant(), target.application(), repository)?)
}
fn row(sets: &[SqlResultSet], token: ServingToken) -> cellule_runtime::Result<Option<i64>> {
    let Some(
        [
            incarnation,
            sequence,
            epoch,
            generation,
            SqlValue::Integer(expires),
        ],
    ) = rows(sets)?.first().map(Vec::as_slice)
    else {
        if rows(sets)?.is_empty() {
            return Ok(None);
        }
        return Err(Error::Command("invalid serving pin row"));
    };
    if fixed::<16>(incarnation)? != *token.owner.incarnation.as_bytes()
        || unsigned(sequence)? != token.admission_sequence
        || u64::from_be_bytes(fixed(epoch)?) != token.owner.epoch
        || unsigned(generation)? != token.generation
    {
        return Ok(None);
    }
    Ok(Some(*expires))
}
fn grant(
    token: ServingToken,
    fact: GenerationFact,
    format: ObjectFormat,
    now: i64,
    expires: i64,
) -> cellule_runtime::Result<ServingLease> {
    let value = ServingLease {
        token,
        fact,
        format,
        observed_at_ms: now,
        expires_at_ms: expires,
    };
    value.validate()?;
    Ok(value)
}
fn denied(reason: ServingDenial) -> CommandResult<ServingReply> {
    CommandResult::Rejected(ServingReply::Denied(reason))
}
pub struct AcquireServingPin;
impl Command for AcquireServingPin {
    const MODULE: &'static str = RepositoryModule::NAME;
    const ID: u32 = 44;
    const CODEC_VERSION: u32 = 1;
    type Input = AcquireServingRequest;
    type Output = ServingReply;
    fn execute(
        context: &mut CommandContext<'_, '_>,
        input: Self::Input,
    ) -> cellule_runtime::Result<CommandResult<Self::Output>> {
        input.encode(&mut BoundedEncoder::new(1024)?)?;
        if !self::context(context.target(), input.repository)?
            || rows(&context.sql(&access(&input.actor)?)?)?.is_empty()
        {
            return Ok(denied(ServingDenial::Unauthorized));
        }
        let Some(format) = identity(
            &context.sql(&statement(IDENTITY, vec![]))?,
            input.repository,
        )?
        else {
            return Ok(denied(ServingDenial::Unauthorized));
        };
        let fact = super::super::commands::fact(context, input.repository, format, None)?;
        if fact.generation == 0 || fact.catalog.is_none() || fact.refs.is_none() {
            return Ok(denied(ServingDenial::Uninitialized));
        }
        if !rows(&context.sql(&statement(ROW, vec![blob(input.reader)]))?)?.is_empty() {
            return Ok(denied(ServingDenial::Conflict));
        }
        let counts = context.sql(&statement(
            "SELECT count(*) FROM (SELECT reader FROM catalog_serving_pins LIMIT ?1)",
            vec![number(MAX_SERVING_PINS + 1)?],
        ))?;
        let Some([count]) = rows(&counts)?.first().map(Vec::as_slice) else {
            return Err(Error::Command("missing serving pin count"));
        };
        if unsigned(count)? >= MAX_SERVING_PINS {
            return Ok(denied(ServingDenial::Capacity));
        }
        let now = now(context.now_ms())?;
        let expires = expiry(now, input.lease_ms)?;
        let token = ServingToken {
            repository: input.repository,
            reader: input.reader,
            owner: context.owner_fence(),
            admission_sequence: context.sequence(),
            generation: fact.generation,
        };
        token.validate()?;
        let changed=context.sql(&statement("INSERT INTO catalog_serving_pins(reader,incarnation,admission_sequence,owner_epoch,generation,expires_at_ms) VALUES(?1,?2,?3,?4,?5,?6)",vec![blob(input.reader),blob(token.owner.incarnation.as_bytes()),number(token.admission_sequence)?,blob(token.owner.epoch.to_be_bytes()),number(token.generation)?,SqlValue::Integer(expires)]))?;
        if changed.first().is_none_or(|set| set.rows_affected != 1) {
            return Err(Error::Command("serving pin was not inserted"));
        }
        Ok(CommandResult::Success(ServingReply::Granted(Box::new(
            grant(token, fact, format, now, expires)?,
        ))))
    }
}
pub struct RenewServingPin;
impl Command for RenewServingPin {
    const MODULE: &'static str = RepositoryModule::NAME;
    const ID: u32 = 45;
    const CODEC_VERSION: u32 = 1;
    type Input = RenewServingRequest;
    type Output = ServingReply;
    fn execute(
        context: &mut CommandContext<'_, '_>,
        input: Self::Input,
    ) -> cellule_runtime::Result<CommandResult<Self::Output>> {
        input.encode(&mut BoundedEncoder::new(1024)?)?;
        let token = input.check.token;
        if !self::context(context.target(), token.repository)?
            || rows(&context.sql(&access(&input.check.actor)?)?)?.is_empty()
        {
            return Ok(denied(ServingDenial::Unauthorized));
        }
        if token.owner != context.owner_fence() {
            return Ok(denied(ServingDenial::Stale));
        }
        let Some(format) = identity(
            &context.sql(&statement(IDENTITY, vec![]))?,
            token.repository,
        )?
        else {
            return Ok(denied(ServingDenial::Unauthorized));
        };
        let Some(expires) = row(
            &context.sql(&statement(ROW, vec![blob(token.reader)]))?,
            token,
        )?
        else {
            return Ok(denied(ServingDenial::Conflict));
        };
        let now = now(context.now_ms())?;
        if expires <= now {
            return Ok(denied(ServingDenial::Expired));
        }
        let expires = expiry(now, input.lease_ms)?.max(expires);
        let changed = context.sql(&statement(
            "UPDATE catalog_serving_pins SET expires_at_ms=?1 WHERE reader=?2",
            vec![SqlValue::Integer(expires), blob(token.reader)],
        ))?;
        if changed.first().is_none_or(|set| set.rows_affected != 1) {
            return Err(Error::Command("serving pin was not renewed"));
        }
        let fact = super::super::commands::fact(
            context,
            token.repository,
            format,
            Some(token.generation),
        )?;
        Ok(CommandResult::Success(ServingReply::Granted(Box::new(
            grant(token, fact, format, now, expires)?,
        ))))
    }
}
pub struct CheckServingPin;
impl Query for CheckServingPin {
    const MODULE: &'static str = RepositoryModule::NAME;
    const ID: u32 = 47;
    const CODEC_VERSION: u32 = 1;
    type Input = ServingCheck;
    type Output = Option<ServingLease>;
    fn execute(
        context: &mut QueryContext<'_>,
        input: Self::Input,
    ) -> cellule_runtime::Result<Self::Output> {
        input.encode(&mut BoundedEncoder::new(1024)?)?;
        let token = input.token;
        // QueryContext is scoped by its trusted CellClient capability. The
        // service additionally verifies actual target/owner before artifact I/O.
        if rows(&context.sql(&access(&input.actor)?)?)?.is_empty() {
            return Ok(None);
        }
        let Some(format) = identity(
            &context.sql(&statement(IDENTITY, vec![]))?,
            token.repository,
        )?
        else {
            return Ok(None);
        };
        let Some(expires) = row(
            &context.sql(&statement(ROW, vec![blob(token.reader)]))?,
            token,
        )?
        else {
            return Ok(None);
        };
        let now = now(context.now_ms())?;
        if expires <= now {
            return Ok(None);
        }
        let fact = generation(
            &context.sql(&statement(GENERATION, vec![number(token.generation)?]))?,
            token.repository,
            format,
        )?;
        Ok(Some(grant(token, fact, format, now, expires)?))
    }
}
pub struct SelectServingGeneration;
impl Query for SelectServingGeneration {
    const MODULE: &'static str = RepositoryModule::NAME;
    const ID: u32 = 48;
    const CODEC_VERSION: u32 = 1;
    type Input = ServingSelection;
    type Output = Option<GenerationFact>;
    fn execute(
        context: &mut QueryContext<'_>,
        input: Self::Input,
    ) -> cellule_runtime::Result<Self::Output> {
        input.encode(&mut BoundedEncoder::new(1024)?)?;
        if rows(&context.sql(&access(&input.actor)?)?)?.is_empty() {
            return Ok(None);
        }
        let Some(format) = identity(
            &context.sql(&statement(IDENTITY, vec![]))?,
            input.repository,
        )?
        else {
            return Ok(None);
        };
        // Both immutable roots come from one indexed head observation. A caller
        // must acquire/check its own exact serving retention before artifact I/O.
        let fact = generation(
            &context.sql(&statement(CURRENT, vec![]))?,
            input.repository,
            format,
        )?;
        if fact.generation == 0 || fact.catalog.is_none() || fact.refs.is_none() {
            return Ok(None);
        }
        Ok(Some(fact))
    }
}
pub struct ReleaseServingPin;
impl Command for ReleaseServingPin {
    const MODULE: &'static str = RepositoryModule::NAME;
    const ID: u32 = 46;
    const CODEC_VERSION: u32 = 1;
    type Input = ServingDrainProof;
    type Output = ServingReleaseReply;
    fn execute(
        context: &mut CommandContext<'_, '_>,
        input: Self::Input,
    ) -> cellule_runtime::Result<CommandResult<Self::Output>> {
        let data = input.data()?;
        let reject = |reason| Ok(CommandResult::Rejected(ServingReleaseReply::Denied(reason)));
        if data.tenant != *context.target().tenant().as_bytes()
            || data.application != *context.target().application().as_bytes()
            || data.token.owner != context.owner_fence()
            || super::super::commands::authorized(
                context,
                data.token.repository,
                &data.administrator,
                TokenScope::Admin,
            )?
            .is_none()
        {
            return reject(ServingDenial::Unauthorized);
        }
        let seeds = context.sql(&statement(
            "SELECT push_cert_seed FROM repository_identity WHERE singleton=1",
            vec![],
        ))?;
        let Some([seed]) = rows(&seeds)?.first().map(Vec::as_slice) else {
            return Err(Error::Command("serving pin seed absent"));
        };
        if !input.0.authenticated(&fixed(seed)?) {
            return reject(ServingDenial::Unauthorized);
        }
        if row(
            &context.sql(&statement(ROW, vec![blob(data.token.reader)]))?,
            data.token,
        )?
        .is_none()
        {
            return reject(ServingDenial::Conflict);
        }
        // Expiry does not remove this root. Only an authenticated drained owner
        // can release it; old workers may still own artifacts after their lease.
        let changed = context.sql(&statement(
            "DELETE FROM catalog_serving_pins WHERE reader=?1",
            vec![blob(data.token.reader)],
        ))?;
        if changed.first().is_none_or(|set| set.rows_affected != 1) {
            return Err(Error::Command("serving pin was not released"));
        }
        Ok(CommandResult::Success(ServingReleaseReply::Released))
    }
}
