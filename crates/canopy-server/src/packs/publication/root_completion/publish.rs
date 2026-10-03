//! One bounded owner-fenced root and exact native-outcome transaction.
use super::super::{
    commands::{authorized, check_pin, fact, load, matched},
    publish::{authenticate, changed, checkpoint, retention_matches},
    sql::*,
};
use super::*;

pub struct CompleteRootPush;
impl Command for CompleteRootPush {
    const MODULE: &'static str = RepositoryModule::NAME;
    const ID: u32 = 36;
    const CODEC_VERSION: u32 = 1;
    type Input = RootPushCompletion;
    type Output = RootCompletionReply;
    fn execute(
        context: &mut CommandContext<'_, '_>,
        input: Self::Input,
    ) -> cellule_runtime::Result<CommandResult<Self::Output>> {
        input.shape()?;
        let binding = input.outcomes.binding()?;
        let Some((data, key)) = authenticate(
            context,
            &input.proof.certificate,
            Some(ref_policy::root_binding(
                input.proof.guard,
                input.proof.snapshot,
            )?),
            Some(binding),
        )?
        else {
            return Ok(denied(PreparationDenial::Unauthorized));
        };
        // Exact terminal logical replay precedes new authority. It never
        // restores an operation, generation, guard or signed ownership record.
        let saved = context.sql(&statement(read::SAVED, vec![blob(data.token.operation)]))?;
        if !rows(&saved)?.is_empty() {
            let Some(value) = read::saved(&saved, &data.actor, data.token.request_digest)? else {
                return Ok(denied(PreparationDenial::Conflict));
            };
            let row = rows(&saved)?
                .first()
                .ok_or(Error::Command("missing saved root completion"))?;
            if fixed::<32>(&row[3])? != binding {
                return Ok(denied(PreparationDenial::Conflict));
            }
            return Ok(CommandResult::Success(RootCompletionReply::Completed(
                Box::new(value),
            )));
        }
        if data.token.owner != context.owner_fence() {
            return Ok(denied(PreparationDenial::Stale));
        }
        let Some(row) = load(context, data.token)? else {
            return Ok(denied(PreparationDenial::Missing));
        };
        if !matched(
            &row,
            &LeaseCheck {
                token: data.token,
                actor: data.actor.clone(),
            },
        ) {
            return Ok(denied(PreparationDenial::Stale));
        }
        if row.expires <= now(context.now_ms())? {
            return Ok(denied(PreparationDenial::Expired));
        }
        check_pin(context, &row)?;
        let Some(format) = identity(
            &context.sql(&statement(IDENTITY, vec![]))?,
            data.token.repository,
        )?
        else {
            return Ok(denied(PreparationDenial::Unauthorized));
        };
        if format != data.catalog.format
            || !retention_matches(context, &data, row.generation, format)?
            || fact(
                context,
                data.token.repository,
                format,
                Some(data.base.generation),
            )? != data.base
        {
            return Ok(denied(PreparationDenial::Conflict));
        }
        let replayed = if let Some(signed) = &input.outcomes.signed {
            !rows(&context.sql(&statement(
                "SELECT 1 FROM push_certificates WHERE digest=?1",
                vec![blob(signed.digest)],
            ))?)?
            .is_empty()
        } else {
            false
        };
        let ready = !replayed
            && authorized(
                context,
                data.token.repository,
                &data.actor,
                TokenScope::Write,
            )? == Some(format)
            && ref_policy::current(context, &data, input.proof.guard)?;
        // A live publication proposal must reconcile moving roots. An already
        // determined policy/ACL or signed replay refusal changes no root and
        // can be recorded independently of unrelated generation advancement.
        if ready && fact(context, data.token.repository, format, None)? != data.base {
            return Ok(denied(PreparationDenial::Conflict));
        }
        if ready {
            let count = context.sql(&statement(
                "SELECT count(*) FROM (SELECT generation FROM catalog_generations LIMIT ?1)",
                vec![number(MAX_RETAINED_GENERATIONS)?],
            ))?;
            let Some([count]) = rows(&count)?.first().map(Vec::as_slice) else {
                return Err(Error::Command("missing root generation count"));
            };
            if unsigned(count)? >= MAX_RETAINED_GENERATIONS {
                return Ok(denied(PreparationDenial::Capacity));
            }
        }
        let Some(missing) = checkpoint(context, &data, &key)? else {
            return Ok(denied(PreparationDenial::Conflict));
        };
        let certificate = input.proof.certificate.bytes()?;
        let certificate_digest = *blake3::hash(&certificate).as_bytes();
        let publication = ready.then_some(PublishedRefs {
            generation: data.base.generation + 1,
            ref_generation: input.outcomes.ref_generation,
            certificate_digest,
        });
        let terminal = result::PreparedResult::new(
            &LeaseCheck {
                token: data.token,
                actor: data.actor.clone(),
            },
            &input.outcomes,
            binding,
            publication,
            if ready {
                result::Selection::Native
            } else if replayed {
                result::Selection::Replayed
            } else {
                result::Selection::Rejected
            },
            ready.then_some(input.proof.guard.plan_digest),
            context.now_ms(),
        )?;
        let mut encoded_catalog = BoundedEncoder::new(256)?;
        data.catalog.encode(&mut encoded_catalog)?;
        let mut encoded_refs = BoundedEncoder::new(128)?;
        input.proof.snapshot.encode(&mut encoded_refs)?;
        if row.expires <= now(context.now_ms())? {
            return Ok(denied(PreparationDenial::Expired));
        }
        // No rejection after this boundary. Every error rolls back checkpoint,
        // joint roots, ownership, selected outcome and operation consumption.
        if missing {
            changed(context.sql(&statement("UPDATE catalog_operations SET attestation=?1,attestation_digest=?2 WHERE id=?3 AND attestation IS NULL", vec![blob(&certificate),blob(certificate_digest),blob(data.token.operation)]))?)?;
            changed(context.sql(&statement("UPDATE catalog_leases SET attestation=?1,attestation_digest=?2 WHERE incarnation=?3 AND admission_sequence=?4 AND attestation IS NULL", vec![blob(&certificate),blob(certificate_digest),blob(data.token.owner.incarnation.as_bytes()),number(data.token.attempt)?]))?)?;
        }
        if let Some(value) = publication {
            changed(context.sql(&statement("INSERT INTO catalog_generations(generation,catalog,certificate,refs) VALUES(?1,?2,?3,?4)", vec![number(value.generation)?,blob(encoded_catalog.finish()),blob(certificate_digest),blob(encoded_refs.finish())]))?)?;
            changed(context.sql(&statement(
                "UPDATE catalog_state SET generation=?1 WHERE singleton=1 AND generation=?2",
                vec![number(value.generation)?, number(data.base.generation)?],
            ))?)?;
        }
        Ok(CommandResult::Success(terminal.save(context)?))
    }
}
fn denied(reason: PreparationDenial) -> CommandResult<RootCompletionReply> {
    CommandResult::Rejected(RootCompletionReply::Denied(reason))
}
