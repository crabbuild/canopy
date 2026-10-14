//! Original authenticated attempt, joint-root CAS and first result share SDK acceptance.
use super::super::{
    commands::{authorized, check_pin, fact, load, matched},
    publish::{authenticate, changed, checkpoint, retention_matches},
    sql::*,
};
use super::*;
pub struct PublishNativeCandidate;
impl Command for PublishNativeCandidate {
    const MODULE: &'static str = RepositoryModule::NAME;
    const ID: u32 = 55;
    const CODEC_VERSION: u32 = 1;
    type Input = NativeCandidateProof;
    type Output = CandidatePublicationReply;
    fn execute(
        context: &mut CommandContext<'_, '_>,
        input: Self::Input,
    ) -> cellule_runtime::Result<CommandResult<Self::Output>> {
        let data = input.certificate.data()?;
        let check = LeaseCheck {
            token: data.token,
            actor: data.actor,
        };
        recovery::execute(context, &check, recovery::Kind::Candidate, |context| {
            publish(context, input)
        })
    }
}
fn reject(value: CandidatePublicationReply) -> CommandResult<CandidatePublicationReply> {
    CommandResult::Rejected(value)
}
fn denied(reason: PreparationDenial) -> CommandResult<CandidatePublicationReply> {
    reject(CandidatePublicationReply::Denied(reason))
}
fn publish(
    context: &mut CommandContext<'_, '_>,
    proof: NativeCandidateProof,
) -> cellule_runtime::Result<CommandResult<CandidatePublicationReply>> {
    proof.shape()?;
    let Some((data, key)) =
        authenticate(context, &proof.certificate, Some(proof.binding()?), None)?
    else {
        return Ok(denied(PreparationDenial::Unauthorized));
    };
    let check = LeaseCheck {
        token: data.token,
        actor: data.actor.clone(),
    };
    let result = authenticated(context, proof, data, key)?;
    if check.token.owner == context.owner_fence()
        && let Some(row) = load(context, check.token)?
        && matched(&row, &check)
    {
        check_pin(context, &row)?;
        changed(context.sql(&statement(
            "DELETE FROM catalog_operations WHERE id=?1",
            vec![blob(check.token.operation)],
        ))?)?;
    }
    Ok(result)
}
fn authenticated(
    context: &mut CommandContext<'_, '_>,
    proof: NativeCandidateProof,
    data: super::super::certificate::CertificateData,
    key: [u8; 32],
) -> cellule_runtime::Result<CommandResult<CandidatePublicationReply>> {
    if authorized(
        context,
        data.token.repository,
        &data.actor,
        TokenScope::Write,
    )? != Some(data.catalog.format)
    {
        return Ok(reject(CandidatePublicationReply::Forbidden));
    }
    let id = uuid::Uuid::parse_str(&proof.candidate.request.id)
        .map_err(|_| Error::Command("candidate UUID"))?;
    let prior = context.sql(&statement(audit::SAVED, vec![blob(id.as_bytes())]))?;
    let Some((binding, old, stored)) = audit::row(&prior)? else {
        return Ok(reject(CandidatePublicationReply::NotFound));
    };
    let expected_binding = crate::pulls::candidates::intent_binding(&proof.candidate)?;
    if binding != expected_binding
        || old.request != proof.candidate.request
        || old.actor != data.actor
        || old.number != proof.candidate.number
        || old.created_at_ms != proof.candidate.created_at_ms
    {
        return Ok(reject(CandidatePublicationReply::Conflict));
    }
    if old.result != CandidateResult::Pending {
        let reply = match stored {
            Some(value) => value.reply,
            None if !matches!(old.result, CandidateResult::Ready { .. }) => {
                CandidatePublicationReply::Applied {
                    id: *id.as_bytes(),
                    digest: result_digest(&old)?,
                    publication: None,
                }
            }
            None => return Err(Error::Command("Ready candidate has no native audit")),
        };
        if audit::selected(&prior, &reply, &data.actor)?.is_none() {
            return Err(Error::Command("candidate prior result differs"));
        }
        return Ok(CommandResult::Success(reply));
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
    if !retention_matches(context, &data, row.generation, data.catalog.format)?
        || fact(context, data.token.repository, data.catalog.format, None)? != data.base
    {
        return Ok(reject(CandidatePublicationReply::Conflict));
    }
    let policy = context.sql(&SqlBatch {
        statements: vec![crate::pulls::native::with_refs(
            crate::pulls::merge::policy_statement(&data.actor, old.number),
            &proof.selection,
        )],
    })?;
    match crate::pulls::merge::candidate_ready(&policy, &old.request.revision)? {
        None => return Ok(reject(CandidatePublicationReply::NotFound)),
        Some(false) => return Ok(reject(CandidatePublicationReply::Conflict)),
        Some(true) => {}
    }
    let ready = proof.refs.is_some();
    if ready {
        let count = context.sql(&statement(
            "SELECT count(*) FROM (SELECT generation FROM catalog_generations LIMIT ?1)",
            vec![number(MAX_RETAINED_GENERATIONS)?],
        ))?;
        let Some([count]) = rows(&count)?.first().map(Vec::as_slice) else {
            return Err(Error::Command("candidate generation count"));
        };
        if unsigned(count)? >= MAX_RETAINED_GENERATIONS {
            return Ok(denied(PreparationDenial::Capacity));
        }
    }
    let Some(missing) = checkpoint(context, &data, &key)? else {
        return Ok(reject(CandidatePublicationReply::Conflict));
    };
    let bytes = proof.certificate.bytes()?;
    let digest = *blake3::hash(&bytes).as_bytes();
    let publication = if ready {
        Some(PublishedRefs {
            generation: data
                .base
                .generation
                .checked_add(1)
                .filter(|g| *g <= i64::MAX as u64)
                .ok_or(Error::Command("candidate generation exhausted"))?,
            ref_generation: proof
                .ref_generation
                .ok_or(Error::Command("candidate ref generation"))?,
            certificate_digest: digest,
        })
    } else {
        None
    };
    let reply = CandidatePublicationReply::Applied {
        id: *id.as_bytes(),
        digest: result_digest(&proof.candidate)?,
        publication,
    };
    let mut selected = BoundedEncoder::new(512)?;
    audit::Selected {
        reply: reply.clone(),
        root: proof.audit,
    }
    .encode(&mut selected)?;
    let result = serde_json::to_string(&proof.candidate.result)
        .map_err(|_| Error::Command("candidate result encoding"))?;
    let oid = match &proof.candidate.result {
        CandidateResult::Ready { oid, .. } => blob(crate::pulls::merge::oid(oid)?),
        _ => SqlValue::Null,
    };
    let mut catalog = BoundedEncoder::new(256)?;
    data.catalog.encode(&mut catalog)?;
    let mut refs = BoundedEncoder::new(128)?;
    if let Some(root) = proof.refs {
        root.encode(&mut refs)?;
    }
    if row.expires <= now(context.now_ms())? {
        return Ok(denied(PreparationDenial::Expired));
    }
    // No later refusal. A late SQL error rolls back roots, summary, first result,
    // checkpoint, own attempt closure, journal and SDK acceptance together.
    if missing {
        changed(context.sql(&statement("UPDATE catalog_operations SET attestation=?1,attestation_digest=?2 WHERE id=?3 AND attestation IS NULL",vec![blob(&bytes),blob(digest),blob(data.token.operation)]))?)?;
        changed(context.sql(&statement("UPDATE catalog_leases SET attestation=?1,attestation_digest=?2 WHERE incarnation=?3 AND admission_sequence=?4 AND attestation IS NULL",vec![blob(&bytes),blob(digest),blob(data.token.owner.incarnation.as_bytes()),number(data.token.attempt)?]))?)?;
    }
    if let Some(value) = publication {
        changed(context.sql(&statement("INSERT INTO catalog_generations(generation,catalog,certificate,refs) VALUES(?1,?2,?3,?4)",vec![number(value.generation)?,blob(catalog.finish()),blob(digest),blob(refs.finish())]))?)?;
        changed(context.sql(&statement(
            "UPDATE catalog_state SET generation=?1 WHERE singleton=1 AND generation=?2",
            vec![number(value.generation)?, number(data.base.generation)?],
        ))?)?;
        changed(context.sql(&statement(
            "UPDATE ref_generation SET generation=?1 WHERE singleton=1",
            vec![number(value.ref_generation)?],
        ))?)?;
    }
    changed(context.sql(&statement("UPDATE merge_candidates SET result=?2,oid=?3,native_publication=?4 WHERE id=?1 AND json_extract(result,'$.state')='pending'",vec![blob(id.as_bytes()),SqlValue::Text(result),oid,blob(selected.finish())]))?)?;
    Ok(CommandResult::Success(reply))
}
