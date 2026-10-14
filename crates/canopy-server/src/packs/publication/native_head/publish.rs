use super::*;
use crate::packs::publication::{
    commands::{authorized, check_pin, fact, load, matched},
    publish::{authenticate, changed, checkpoint, retention_matches},
    sql::*,
};

pub(in crate::packs::publication) const SAVED: &str =
    "SELECT actor,request_digest,request,result,fact FROM catalog_head_updates WHERE id=?1";
fn denied(reason: PreparationDenial) -> CommandResult<PublicationReply> {
    CommandResult::Rejected(PublicationReply::Denied(reason))
}
pub(in crate::packs::publication) fn selected(
    sets: &[SqlResultSet],
    check: &LeaseCheck,
    reply: PublicationReply,
) -> cellule_runtime::Result<Option<(HeadRequest, GenerationFact)>> {
    let Some(
        [
            SqlValue::Text(actor),
            digest,
            SqlValue::Blob(request),
            SqlValue::Blob(result),
            SqlValue::Blob(fact),
        ],
    ) = rows(sets)?.first().map(Vec::as_slice)
    else {
        if rows(sets)?.is_empty() {
            return Ok(None);
        }
        return Err(Error::Command("invalid symbolic HEAD outcome"));
    };
    if *actor != check.actor || fixed::<32>(digest)? != check.token.request_digest {
        return Ok(None);
    }
    let mut d = BoundedDecoder::new(result, 512)?;
    let saved = PublicationReply::decode(&mut d)?;
    d.finish()?;
    if saved != reply {
        return Ok(None);
    }
    let PublicationReply::Published(published) = reply else {
        return Ok(None);
    };
    let mut d = BoundedDecoder::new(request, NATIVE_HEAD_BYTES)?;
    let request = HeadRequest::decode(&mut d)?;
    d.finish()?;
    let mut d = BoundedDecoder::new(fact, 512)?;
    let fact = GenerationFact::decode(&mut d)?;
    d.finish()?;
    if fact
        .catalog
        .is_none_or(|c| c.repository != check.token.repository)
        || fact.refs.is_none()
        || fact.generation != published.generation
        || fact.certificate != Some(published.certificate_digest)
        || published.ref_generation != request.expected_generation as u64 + 1
    {
        return Err(Error::Command("symbolic HEAD outcome roots differ"));
    }
    Ok(Some((request, fact)))
}

pub struct PublishNativeHead;
impl Command for PublishNativeHead {
    const MODULE: &'static str = RepositoryModule::NAME;
    const ID: u32 = 54;
    const CODEC_VERSION: u32 = 1;
    type Input = NativeHeadProof;
    type Output = PublicationReply;
    fn execute(
        context: &mut CommandContext<'_, '_>,
        input: Self::Input,
    ) -> cellule_runtime::Result<CommandResult<Self::Output>> {
        let data = input.certificate.data()?;
        let check = LeaseCheck {
            token: data.token,
            actor: data.actor,
        };
        recovery::execute(context, &check, recovery::Kind::Head, |context| {
            publish(context, input)
        })
    }
}
fn publish(
    context: &mut CommandContext<'_, '_>,
    proof: NativeHeadProof,
) -> cellule_runtime::Result<CommandResult<PublicationReply>> {
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
    let result = publish_authenticated(context, proof, data, key)?;
    // Authenticated denial is terminal only for this original bound attempt.
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
fn publish_authenticated(
    context: &mut CommandContext<'_, '_>,
    proof: NativeHeadProof,
    data: super::super::certificate::CertificateData,
    key: [u8; 32],
) -> cellule_runtime::Result<CommandResult<PublicationReply>> {
    if authorized(
        context,
        data.token.repository,
        &data.actor,
        TokenScope::Admin,
    )? != Some(data.catalog.format)
    {
        return Ok(denied(PreparationDenial::Unauthorized));
    }
    let owner = context.sql(&statement(
        "SELECT 1 FROM repository_identity WHERE singleton=1 AND owner=?1",
        vec![SqlValue::Text(data.actor.clone())],
    ))?;
    if rows(&owner)?.is_empty() {
        return Ok(denied(PreparationDenial::Unauthorized));
    }
    let prior = context.sql(&statement(SAVED, vec![blob(data.token.operation)]))?;
    if let Some([_, _, SqlValue::Blob(request), SqlValue::Blob(result), _]) =
        rows(&prior)?.first().map(Vec::as_slice)
    {
        let mut e = BoundedEncoder::new(NATIVE_HEAD_BYTES)?;
        proof.request.encode(&mut e)?;
        if *request != e.finish() {
            return Ok(denied(PreparationDenial::Conflict));
        }
        let mut d = BoundedDecoder::new(result, 512)?;
        let reply = PublicationReply::decode(&mut d)?;
        d.finish()?;
        if selected(
            &prior,
            &LeaseCheck {
                token: data.token,
                actor: data.actor,
            },
            reply,
        )?
        .is_none()
        {
            return Ok(denied(PreparationDenial::Conflict));
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
        return Ok(denied(PreparationDenial::Conflict));
    }
    let Some(refs) = proof.refs else {
        return Ok(denied(PreparationDenial::Conflict));
    };
    let count = context.sql(&statement(
        "SELECT count(*) FROM (SELECT generation FROM catalog_generations LIMIT ?1)",
        vec![number(MAX_RETAINED_GENERATIONS)?],
    ))?;
    let Some([count]) = rows(&count)?.first().map(Vec::as_slice) else {
        return Err(Error::Command("missing symbolic HEAD generation count"));
    };
    if unsigned(count)? >= MAX_RETAINED_GENERATIONS {
        return Ok(denied(PreparationDenial::Capacity));
    }
    let Some(missing) = checkpoint(context, &data, &key)? else {
        return Ok(denied(PreparationDenial::Conflict));
    };
    let bytes = proof.certificate.bytes()?;
    let digest = *blake3::hash(&bytes).as_bytes();
    let generation = data
        .base
        .generation
        .checked_add(1)
        .filter(|g| *g <= i64::MAX as u64)
        .ok_or(Error::Command("symbolic HEAD generation exhausted"))?;
    let reply = PublicationReply::Published(PublishedRefs {
        generation,
        ref_generation: proof.request.expected_generation as u64 + 1,
        certificate_digest: digest,
    });
    let fact = GenerationFact {
        generation,
        catalog: Some(data.catalog),
        refs: Some(refs),
        certificate: Some(digest),
    };
    let mut catalog = BoundedEncoder::new(256)?;
    data.catalog.encode(&mut catalog)?;
    // Store the descriptor itself, rather than Option framing, in joint roots.
    let mut refs_root = BoundedEncoder::new(128)?;
    fact.refs
        .ok_or(Error::Command("symbolic HEAD root missing"))?
        .encode(&mut refs_root)?;
    let mut request = BoundedEncoder::new(NATIVE_HEAD_BYTES)?;
    proof.request.encode(&mut request)?;
    let mut result = BoundedEncoder::new(512)?;
    reply.encode(&mut result)?;
    let mut result_fact = BoundedEncoder::new(512)?;
    fact.encode(&mut result_fact)?;
    if row.expires <= now(context.now_ms())? {
        return Ok(denied(PreparationDenial::Expired));
    }
    // No refusal after the first write. SDK acceptance commits the joint roots,
    // original outcome, checkpoint, attempt closure and journal together.
    if missing {
        changed(context.sql(&statement("UPDATE catalog_operations SET attestation=?1,attestation_digest=?2 WHERE id=?3 AND attestation IS NULL", vec![blob(&bytes),blob(digest),blob(data.token.operation)]))?)?;
        changed(context.sql(&statement("UPDATE catalog_leases SET attestation=?1,attestation_digest=?2 WHERE incarnation=?3 AND admission_sequence=?4 AND attestation IS NULL", vec![blob(&bytes),blob(digest),blob(data.token.owner.incarnation.as_bytes()),number(data.token.attempt)?]))?)?;
    }
    changed(context.sql(&statement(
        "INSERT INTO catalog_generations(generation,catalog,certificate,refs) VALUES(?1,?2,?3,?4)",
        vec![
            number(generation)?,
            blob(catalog.finish()),
            blob(digest),
            blob(refs_root.finish()),
        ],
    ))?)?;
    changed(context.sql(&statement(
        "UPDATE catalog_state SET generation=?1 WHERE singleton=1 AND generation=?2",
        vec![number(generation)?, number(data.base.generation)?],
    ))?)?;
    changed(context.sql(&statement(
        "UPDATE ref_generation SET generation=?1,default_branch=?2 WHERE singleton=1",
        vec![
            number(proof.request.expected_generation as u64 + 1)?,
            SqlValue::Text(proof.request.reference),
        ],
    ))?)?;
    changed(context.sql(&statement("INSERT INTO catalog_head_updates(id,actor,request_digest,request,result,fact) VALUES(?1,?2,?3,?4,?5,?6)", vec![blob(data.token.operation),SqlValue::Text(data.actor),blob(data.token.request_digest),blob(request.finish()),blob(result.finish()),blob(result_fact.finish())]))?)?;
    Ok(CommandResult::Success(reply))
}
