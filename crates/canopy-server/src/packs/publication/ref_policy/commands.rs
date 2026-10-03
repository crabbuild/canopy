use super::super::{
    commands::{authorized, check_pin, fact, load, matched, pin, pin_query},
    publish::{authenticate, changed, retention_matches},
    sql::*,
};
use super::*;

const GUARD: &str =
    "SELECT scope,token,policy_epoch,total,next,valid FROM ref_policy_guards WHERE id=?1";
pub(super) const EPOCH: &str = "SELECT version FROM ref_policy_epoch WHERE singleton=1";
pub(super) fn epoch(sets: &[SqlResultSet]) -> cellule_runtime::Result<u64> {
    let Some([value]) = rows(sets)?.first().map(Vec::as_slice) else {
        return Err(Error::Command("missing ref policy epoch"));
    };
    unsigned(value)
}
struct Guard {
    scope: [u8; 32],
    token: PreparationToken,
    epoch: u64,
    progress: RefPolicyProgress,
}
fn guard(sets: &[SqlResultSet]) -> cellule_runtime::Result<Option<Guard>> {
    let Some(row) = rows(sets)?.first() else {
        return Ok(None);
    };
    let [
        hash,
        SqlValue::Blob(bytes),
        version,
        total,
        next,
        SqlValue::Integer(valid),
    ] = row.as_slice()
    else {
        return Err(Error::Command("invalid ref policy guard"));
    };
    let mut d = BoundedDecoder::new(bytes, TOKEN_BYTES)?;
    let token = PreparationToken::decode(&mut d)?;
    d.finish()?;
    if ![0, 1].contains(valid) {
        return Err(Error::Command("invalid ref policy validity"));
    }
    let progress = RefPolicyProgress {
        next: unsigned(next)?,
        total: unsigned(total)?,
        valid: *valid == 1,
    };
    progress.encode(&mut BoundedEncoder::new(32)?)?;
    Ok(Some(Guard {
        scope: fixed(hash)?,
        token,
        epoch: unsigned(version)?,
        progress,
    }))
}
fn rejected(reason: PreparationDenial) -> CommandResult<RefPolicyReply> {
    CommandResult::Rejected(RefPolicyReply::Denied(reason))
}
/// Final command-local readiness. Advisory query replies are never authority.
pub(in crate::packs::publication) fn current(
    context: &CommandContext<'_, '_>,
    data: &super::super::certificate::CertificateData,
    intent: RefPolicyIntent,
) -> cellule_runtime::Result<bool> {
    let Some(row) = guard(&context.sql(&statement(GUARD, vec![blob(intent.id)]))?)? else {
        return Ok(false);
    };
    Ok(
        row.scope == scope(data.token, &data.actor, data.catalog.format, intent)?
            && row.token == data.token
            && row.epoch == intent.epoch
            && row.progress.total == intent.updates
            && row.progress.ready()
            && epoch(&context.sql(&statement(EPOCH, vec![]))?)? == intent.epoch,
    )
}

pub struct RegisterRefPolicyPage;
impl Command for RegisterRefPolicyPage {
    const MODULE: &'static str = RepositoryModule::NAME;
    const ID: u32 = 33;
    const CODEC_VERSION: u32 = 1;
    type Input = RefPolicyPage;
    type Output = RefPolicyReply;
    fn execute(
        context: &mut CommandContext<'_, '_>,
        input: Self::Input,
    ) -> cellule_runtime::Result<CommandResult<Self::Output>> {
        let data = input.proof.certificate.data()?;
        let check = LeaseCheck {
            token: data.token,
            actor: data.actor,
        };
        super::super::recovery::execute(
            context,
            &check,
            super::super::recovery::Kind::Policy,
            RefPolicyReply::Denied,
            |context| Self::domain(context, input),
        )
    }
}
impl RegisterRefPolicyPage {
    fn domain(
        context: &mut CommandContext<'_, '_>,
        input: RefPolicyPage,
    ) -> cellule_runtime::Result<CommandResult<RefPolicyReply>> {
        input.shape()?;
        let Some((data, _)) = authenticate(
            context,
            &input.proof.certificate,
            Some(page_binding(&input)?),
            None,
        )?
        else {
            return Ok(rejected(PreparationDenial::Unauthorized));
        };
        if data.actor != input.proof.plan.actor || data.base.refs.is_none() {
            return Ok(rejected(PreparationDenial::Conflict));
        }
        if super::super::ref_proof::shape(&input.proof.plan, data.catalog.format).is_err() {
            return Ok(rejected(PreparationDenial::Conflict));
        }
        if data.token.owner != context.owner_fence() {
            return Ok(rejected(PreparationDenial::Stale));
        }
        let Some(format) = authorized(
            context,
            data.token.repository,
            &data.actor,
            TokenScope::Write,
        )?
        else {
            return Ok(rejected(PreparationDenial::Unauthorized));
        };
        if format != data.catalog.format {
            return Ok(rejected(PreparationDenial::Conflict));
        }
        let Some(row) = load(context, data.token)? else {
            return Ok(rejected(PreparationDenial::Missing));
        };
        if !matched(
            &row,
            &LeaseCheck {
                token: data.token,
                actor: data.actor.clone(),
            },
        ) {
            return Ok(rejected(PreparationDenial::Stale));
        }
        if row.expires <= now(context.now_ms())? {
            return Ok(rejected(PreparationDenial::Expired));
        }
        check_pin(context, &row)?;
        // Only a retained immutable base is required here. Unrelated root
        // advancement must not invalidate generation-independent predicates.
        if !retention_matches(context, &data, row.generation, format)?
            || fact(
                context,
                data.token.repository,
                format,
                Some(data.base.generation),
            )? != data.base
            || epoch(&context.sql(&statement(EPOCH, vec![]))?)? != input.intent.epoch
        {
            return Ok(rejected(PreparationDenial::Conflict));
        }
        let scoped = scope(data.token, &data.actor, format, input.intent)?;
        let old = guard(&context.sql(&statement(GUARD, vec![blob(input.intent.id)]))?)?;
        let end = input.offset + input.proof.plan.updates.len() as u64;
        if let Some(old) = &old {
            if old.scope != scoped
                || old.token != data.token
                || old.epoch != input.intent.epoch
                || old.progress.total != input.intent.updates
                || !old.progress.valid
            {
                return Ok(rejected(PreparationDenial::Conflict));
            }
            if end <= old.progress.next {
                return Ok(CommandResult::Success(RefPolicyReply::Registered(
                    old.progress,
                )));
            }
            if input.offset != old.progress.next {
                return Ok(rejected(PreparationDenial::Conflict));
            }
        } else {
            if input.offset != 0 {
                return Ok(rejected(PreparationDenial::Missing));
            }
            let count = context.sql(&statement(
                "SELECT count(*) FROM (SELECT id FROM ref_policy_guards LIMIT ?1)",
                vec![number(MAX_REF_POLICY_GUARDS)?],
            ))?;
            let Some([count]) = rows(&count)?.first().map(Vec::as_slice) else {
                return Err(Error::Command("missing guard capacity"));
            };
            if unsigned(count)? >= MAX_REF_POLICY_GUARDS {
                return Ok(rejected(PreparationDenial::Capacity));
            }
        }
        let policies = context.sql(&SqlBatch {
            statements: input
                .proof
                .plan
                .updates
                .iter()
                .enumerate()
                .map(|(i, update)| {
                    crate::branch_rules::policy_statement_with_ancestry(
                        update,
                        super::super::ref_proof::proven(&input.proof.ancestry, i),
                    )
                })
                .collect(),
        })?;
        if policies.len() != input.proof.plan.updates.len() {
            return Err(Error::Command("missing guarded ref policies"));
        }
        for (update, policy) in input.proof.plan.updates.iter().zip(policies) {
            if crate::branch_rules::decode_policy(&[policy])?
                .is_some_and(|rule| !rule.allows(update, true))
            {
                return Ok(rejected(PreparationDenial::Conflict));
            }
        }
        let mut dependencies = std::collections::BTreeSet::new();
        for update in &input.proof.plan.updates {
            let Some(oid) = update.new_oid else {
                continue;
            };
            let sets=context.sql(&statement("SELECT q.context,c.version,r.number FROM branch_required_checks q JOIN branch_rules b ON b.reference=q.reference AND b.enabled=1 JOIN check_contexts c ON c.name=q.context JOIN check_runs r ON r.number=(SELECT number FROM check_runs WHERE oid=?2 AND context=q.context AND context_version=c.version ORDER BY number DESC LIMIT 1) WHERE q.reference=?1 LIMIT 17",vec![SqlValue::Text(update.name.clone()),blob(oid)]))?;
            if rows(&sets)?.len() > 16 {
                return Ok(rejected(PreparationDenial::Capacity));
            }
            for row in rows(&sets)? {
                let [SqlValue::Text(name), version, run] = row.as_slice() else {
                    return Err(Error::Command("invalid guarded check dependency"));
                };
                validate_component(name)?;
                let version = unsigned(version)?;
                let run = unsigned(run)?;
                if version == 0 || run == 0 {
                    return Err(Error::Command("invalid guarded check version"));
                }
                dependencies.insert((oid, name.clone(), version, run));
            }
        }
        let mut missing = Vec::new();
        for (oid, name, version, run) in dependencies {
            if rows(&context.sql(&statement("SELECT 1 FROM ref_policy_watches WHERE guard=?1 AND oid=?2 AND context=?3 AND context_version=?4 AND run_number=?5",vec![blob(input.intent.id),blob(oid),SqlValue::Text(name.clone()),number(version)?,number(run)?]))?)?.is_empty() {
                missing.push((oid,name,version,run));
            }
        }
        let budget = context.sql(&statement(
            "SELECT watches FROM ref_policy_budget WHERE singleton=1",
            vec![],
        ))?;
        let Some([budget]) = rows(&budget)?.first().map(Vec::as_slice) else {
            return Err(Error::Command("missing ref policy watch budget"));
        };
        let added = missing.len() as u64;
        if unsigned(budget)?
            .checked_add(added)
            .is_none_or(|n| n > MAX_REF_POLICY_WATCHES)
        {
            return Ok(rejected(PreparationDenial::Capacity));
        }
        let mut token = BoundedEncoder::new(TOKEN_BYTES)?;
        data.token.encode(&mut token)?;
        if row.expires <= now(context.now_ms())? {
            return Ok(rejected(PreparationDenial::Expired));
        }
        // No rejection after the first write. Policy validation and installing
        // every watch are one Cell transaction, without an observation gap.
        if old.is_none() {
            changed(context.sql(&statement("INSERT INTO ref_policy_guards(id,scope,token,policy_epoch,total,next,valid) VALUES(?1,?2,?3,?4,?5,0,1)",vec![blob(input.intent.id),blob(scoped),blob(token.finish()),number(input.intent.epoch)?,number(input.intent.updates)?]))?)?;
        }
        for (oid, name, version, run) in missing {
            changed(context.sql(&statement("INSERT INTO ref_policy_watches(guard,oid,context,context_version,run_number) VALUES(?1,?2,?3,?4,?5)",vec![blob(input.intent.id),blob(oid),SqlValue::Text(name),number(version)?,number(run)?]))?)?;
        }
        changed(context.sql(&statement(
            "UPDATE ref_policy_budget SET watches=watches+?1 WHERE singleton=1 AND watches<=?2",
            vec![number(added)?, number(MAX_REF_POLICY_WATCHES - added)?],
        ))?)?;
        changed(context.sql(&statement(
            "UPDATE ref_policy_guards SET next=?1 WHERE id=?2 AND next=?3 AND valid=1",
            vec![number(end)?, blob(input.intent.id), number(input.offset)?],
        ))?)?;
        Ok(CommandResult::Success(RefPolicyReply::Registered(
            RefPolicyProgress {
                next: end,
                total: input.intent.updates,
                valid: true,
            },
        )))
    }
}
pub struct CheckRefPolicyGuard;
impl Query for CheckRefPolicyGuard {
    const MODULE: &'static str = RepositoryModule::NAME;
    const ID: u32 = 34;
    const CODEC_VERSION: u32 = 1;
    type Input = RefPolicyLookup;
    type Output = Option<RefPolicyProgress>;
    fn execute(
        context: &mut QueryContext<'_>,
        input: Self::Input,
    ) -> cellule_runtime::Result<Self::Output> {
        validate_component(&input.check.actor)?;
        if !decode_access(&context.sql(&SqlBatch {
            statements: vec![access_statement(&input.check.actor)],
        })?)?
        .is_some_and(|role| role >= TokenScope::Write)
        {
            return Ok(None);
        }
        let Some(format) = identity(
            &context.sql(&statement(IDENTITY, vec![]))?,
            input.check.token.repository,
        )?
        else {
            return Ok(None);
        };
        let Some(op) = operation(
            &context.sql(&statement(
                OPERATION,
                vec![blob(input.check.token.operation)],
            ))?,
            input.check.token.repository,
            input.check.token.operation,
        )?
        else {
            return Ok(None);
        };
        if !matched(&op, &input.check) || op.expires <= now(context.now_ms())? {
            return Ok(None);
        }
        pin(&context.sql(&pin_query(op.token)?)?, &op)?;
        let Some(row) = guard(&context.sql(&statement(GUARD, vec![blob(input.intent.id)]))?)?
        else {
            return Ok(None);
        };
        if row.scope != scope(input.check.token, &input.check.actor, format, input.intent)?
            || row.token != input.check.token
            || row.epoch != input.intent.epoch
            || row.progress.total != input.intent.updates
        {
            return Ok(None);
        }
        Ok(Some(RefPolicyProgress {
            valid: row.progress.valid
                && epoch(&context.sql(&statement(EPOCH, vec![]))?)? == input.intent.epoch,
            ..row.progress
        }))
    }
}
pub struct ReapRefPolicyGuard;
impl Command for ReapRefPolicyGuard {
    const MODULE: &'static str = RepositoryModule::NAME;
    const ID: u32 = 35;
    const CODEC_VERSION: u32 = 1;
    type Input = RefPolicyReap;
    type Output = RefPolicyReapReply;
    fn execute(
        context: &mut CommandContext<'_, '_>,
        input: Self::Input,
    ) -> cellule_runtime::Result<CommandResult<Self::Output>> {
        let request = input.maintenance;
        if request.owner != context.owner_fence() {
            return Ok(CommandResult::Rejected(RefPolicyReapReply::Denied(
                PreparationDenial::Stale,
            )));
        }
        if authorized(
            context,
            request.repository,
            &request.actor,
            TokenScope::Admin,
        )?
        .is_none()
        {
            return Ok(CommandResult::Rejected(RefPolicyReapReply::Denied(
                PreparationDenial::Unauthorized,
            )));
        }
        let Some(row) = guard(&context.sql(&statement(GUARD, vec![blob(input.id)]))?)? else {
            return Ok(CommandResult::Success(RefPolicyReapReply::Reaped {
                watches: 0,
                removed: false,
            }));
        };
        if row.token.repository != request.repository {
            return Err(Error::Command("foreign ref policy guard token"));
        }
        let at = now(context.now_ms())?;
        let live = if row.token.owner == context.owner_fence() {
            load(context, row.token)?.is_some_and(|op| op.token == row.token && op.expires > at)
        } else {
            false
        };
        if live
            && row.progress.valid
            && row.epoch == epoch(&context.sql(&statement(EPOCH, vec![]))?)?
        {
            return Ok(CommandResult::Rejected(RefPolicyReapReply::Denied(
                PreparationDenial::Conflict,
            )));
        }
        if row.progress.valid {
            changed(context.sql(&statement(
                "UPDATE ref_policy_guards SET valid=0 WHERE id=?1 AND valid=1",
                vec![blob(input.id)],
            ))?)?;
        }
        let deleted=context.sql(&statement("DELETE FROM ref_policy_watches WHERE guard=?1 AND (oid,context,context_version,run_number) IN (SELECT oid,context,context_version,run_number FROM ref_policy_watches WHERE guard=?1 ORDER BY oid,context,context_version,run_number LIMIT ?2)",vec![blob(input.id),number(WATCH_REAP_ROWS)?]))?;
        let watches = deleted
            .first()
            .ok_or(Error::Command("missing watch reap result"))?
            .rows_affected;
        if watches > WATCH_REAP_ROWS {
            return Err(Error::Command("watch reaper exceeded its budget"));
        }
        changed(context.sql(&statement(
            "UPDATE ref_policy_budget SET watches=watches-?1 WHERE singleton=1 AND watches>=?1",
            vec![number(watches)?],
        ))?)?;
        let remaining = context.sql(&statement(
            "SELECT EXISTS(SELECT 1 FROM ref_policy_watches WHERE guard=?1)",
            vec![blob(input.id)],
        ))?;
        let Some([SqlValue::Integer(remaining)]) = rows(&remaining)?.first().map(Vec::as_slice)
        else {
            return Err(Error::Command("missing remaining watches"));
        };
        let removed = !live && *remaining == 0;
        if removed {
            changed(context.sql(&statement(
                "DELETE FROM ref_policy_guards WHERE id=?1",
                vec![blob(input.id)],
            ))?)?;
        }
        Ok(CommandResult::Success(RefPolicyReapReply::Reaped {
            watches,
            removed,
        }))
    }
}
