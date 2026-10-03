//! Atomic typed catalog/ref publication. The HTTP completion command must call
//! the same core inside its response transaction; this command alone does not
//! integrate network reports, push options/certificates or reviewed merges.
use super::*;
use super::{
    commands::{authorized, check_pin, fact, load, matched},
    sql::*,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PublishedRefs {
    pub generation: u64,
    pub ref_generation: u64,
    pub certificate_digest: [u8; 32],
}
impl WireValue for PublishedRefs {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        if self.generation == 0
            || self.generation > i64::MAX as u64
            || self.ref_generation == 0
            || self.ref_generation > i64::MAX as u64
        {
            return Err(CodecError::Invalid("invalid publication receipt"));
        }
        e.write_u64(self.generation)?;
        e.write_u64(self.ref_generation)?;
        e.write_bytes(&self.certificate_digest)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        let value = Self {
            generation: d.read_u64()?,
            ref_generation: d.read_u64()?,
            certificate_digest: crate::packs::directory::index::codec::fixed(d)?,
        };
        let mut e = BoundedEncoder::new(128)?;
        value.encode(&mut e)?;
        Ok(value)
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PublicationReply {
    Published(PublishedRefs),
    Denied(PreparationDenial),
}
impl WireValue for PublicationReply {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        match self {
            Self::Published(value) => {
                e.write_u8(0)?;
                value.encode(e)
            }
            Self::Denied(reason) => PreparationReply::Denied(*reason).encode(e),
        }
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        match d.read_u8()? {
            0 => Ok(Self::Published(PublishedRefs::decode(d)?)),
            1 => Ok(Self::Denied(PreparationDenial::Unauthorized)),
            2 => Ok(Self::Denied(PreparationDenial::Conflict)),
            3 => Ok(Self::Denied(PreparationDenial::Stale)),
            4 => Ok(Self::Denied(PreparationDenial::Expired)),
            5 => Ok(Self::Denied(PreparationDenial::Capacity)),
            6 => Ok(Self::Denied(PreparationDenial::Missing)),
            _ => Err(CodecError::Invalid("invalid publication reply")),
        }
    }
}
pub struct PublishCatalogRefs;
impl Command for PublishCatalogRefs {
    const MODULE: &'static str = RepositoryModule::NAME;
    const ID: u32 = 18;
    const CODEC_VERSION: u32 = 1;
    type Input = RefPublicationProof;
    type Output = PublicationReply;
    fn execute(
        context: &mut CommandContext<'_, '_>,
        input: RefPublicationProof,
    ) -> cellule_runtime::Result<CommandResult<PublicationReply>> {
        publish(context, &input)
    }
}
fn denied(reason: PreparationDenial) -> CommandResult<PublicationReply> {
    CommandResult::Rejected(PublicationReply::Denied(reason))
}
/// Every negative decision precedes writes. Errors after writes abort the whole
/// Cell transaction. Network completion must persist its exact response here,
/// in the same command, rather than call this typed command then stage a reply.
pub(super) fn publish(
    context: &mut CommandContext<'_, '_>,
    input: &RefPublicationProof,
) -> cellule_runtime::Result<CommandResult<PublicationReply>> {
    let Some((data, key)) = authenticate(
        context,
        &input.certificate,
        Some(super::ref_proof::binding(&input.plan, &input.ancestry)?),
        None,
    )?
    else {
        return Ok(denied(PreparationDenial::Unauthorized));
    };
    publish_authenticated(context, input, data, key)
}
/// Private command-local boundary, shared by typed and network completion.
pub(super) fn authenticate(
    context: &CommandContext<'_, '_>,
    certificate: &CatalogCertificate,
    refs_digest: Option<[u8; 32]>,
    completion_digest: Option<[u8; 32]>,
) -> cellule_runtime::Result<Option<(super::certificate::CertificateData, [u8; 32])>> {
    let data = certificate.data()?;
    if data.tenant != *context.target().tenant().as_bytes()
        || data.application != *context.target().application().as_bytes()
        || context.target()
            != &crate::repository_target(
                context.target().tenant(),
                context.target().application(),
                data.token.repository,
            )?
    {
        return Ok(None);
    }
    let secret = context.sql(&statement("SELECT push_cert_seed FROM repository_identity WHERE singleton=1 AND repository_id=?1 AND object_format=?2", vec![blob(data.token.repository), SqlValue::Text(data.catalog.format.as_str().into())]))?;
    let key = super::attestation::seed(&secret)?;
    if !certificate.authenticated(&key)
        || data.refs_digest != refs_digest
        || data.completion_digest != completion_digest
    {
        return Ok(None);
    }
    Ok(Some((data, key)))
}
pub(super) fn retention_matches(
    context: &CommandContext<'_, '_>,
    data: &super::certificate::CertificateData,
    row_generation: Option<u64>,
    format: ObjectFormat,
) -> cellule_runtime::Result<bool> {
    let Some(row_generation) = row_generation else {
        return Ok(false);
    };
    Ok(inputs::retention_matches(context, data)?
        && row_generation == data.retention_floor
        && fact(context, data.token.repository, format, Some(row_generation))?.certificate
            == data.retention_certificate)
}
pub(super) fn publish_authenticated(
    context: &mut CommandContext<'_, '_>,
    input: &RefPublicationProof,
    data: super::certificate::CertificateData,
    key: [u8; 32],
) -> cellule_runtime::Result<CommandResult<PublicationReply>> {
    if data.compaction {
        return Ok(denied(PreparationDenial::Unauthorized));
    }
    if data.actor != input.plan.actor {
        return Ok(denied(PreparationDenial::Unauthorized));
    }
    let saved = context.sql(&statement(
        "SELECT actor,request_digest,publication,response_id,publication_plan_digest FROM pushes WHERE id=?1",
        vec![blob(data.token.operation)],
    ))?;
    if let Some(row) = rows(&saved)?.first() {
        let [
            SqlValue::Text(actor),
            digest,
            outcome,
            response,
            original_plan,
        ] = row.as_slice()
        else {
            return Err(Error::Command("invalid publication push identity"));
        };
        if *actor != data.actor || fixed::<32>(digest)? != data.token.request_digest {
            return Ok(denied(PreparationDenial::Conflict));
        }
        match outcome {
            SqlValue::Blob(bytes) => {
                if fixed::<32>(original_plan)? != super::ref_proof::plan_digest(&input.plan)? {
                    return Ok(denied(PreparationDenial::Conflict));
                }
                let mut d = BoundedDecoder::new(bytes, 128)?;
                let result = PublishedRefs::decode(&mut d)?;
                d.finish()?;
                return Ok(CommandResult::Success(PublicationReply::Published(result)));
            }
            SqlValue::Null if *response == SqlValue::Null => {}
            SqlValue::Null => return Ok(denied(PreparationDenial::Conflict)),
            _ => return Err(Error::Command("invalid saved publication outcome")),
        }
    }
    // Logical outcome replay above cannot grant a new write. All new writes
    // require the actual admitted fence and current authority/lease/policy.
    if data.token.owner != context.owner_fence() {
        return Ok(denied(PreparationDenial::Stale));
    }
    let Some(format) = authorized(
        context,
        data.token.repository,
        &data.actor,
        TokenScope::Write,
    )?
    else {
        return Ok(denied(PreparationDenial::Unauthorized));
    };
    if format != data.catalog.format {
        return Ok(denied(PreparationDenial::Conflict));
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
    if !retention_matches(context, &data, row.generation, format)?
        || fact(context, data.token.repository, format, None)? != data.base
    {
        return Ok(denied(PreparationDenial::Conflict));
    }
    // A selected immutable ref snapshot cannot be mutated by the inline SQL
    // publisher. The root publisher and all readers must cut over together.
    if data.base.refs.is_some() {
        return Ok(denied(PreparationDenial::Conflict));
    }
    let Some(validated) = crate::refs::validate_refs(context, &input.plan)? else {
        return Ok(denied(PreparationDenial::Conflict));
    };
    for (page, updates) in input.plan.updates.chunks(128).enumerate() {
        let policies = context.sql(&SqlBatch {
            statements: updates
                .iter()
                .enumerate()
                .map(|(at, update)| {
                    crate::branch_rules::policy_statement_with_ancestry(
                        update,
                        super::ref_proof::proven(&input.ancestry, page * 128 + at),
                    )
                })
                .collect(),
        })?;
        if policies.len() != updates.len() {
            return Err(Error::Command("missing final publication policies"));
        }
        for (update, policy) in updates.iter().zip(policies) {
            if crate::branch_rules::decode_policy(&[policy])?
                .is_some_and(|rule| !rule.allows(update, true))
            {
                return Ok(denied(PreparationDenial::Conflict));
            }
        }
    }
    let counts = context.sql(&statement(
        "SELECT count(*) FROM (SELECT generation FROM catalog_generations LIMIT ?1)",
        vec![number(MAX_RETAINED_GENERATIONS)?],
    ))?;
    let Some([count]) = rows(&counts)?.first().map(Vec::as_slice) else {
        return Err(Error::Command("missing publication generation count"));
    };
    if unsigned(count)? >= MAX_RETAINED_GENERATIONS || data.base.generation >= i64::MAX as u64 {
        return Ok(denied(PreparationDenial::Capacity));
    }
    let certificate = input.certificate.bytes()?;
    let certificate_digest = *blake3::hash(&certificate).as_bytes();
    let Some(checkpoint_missing) = checkpoint(context, &data, &key)? else {
        return Ok(denied(PreparationDenial::Conflict));
    };
    if row.expires <= now(context.now_ms())? {
        return Ok(denied(PreparationDenial::Expired));
    }
    // No rejection path below this point. All remaining failures must roll back.
    if checkpoint_missing {
        changed(context.sql(&statement("UPDATE catalog_operations SET attestation=?1,attestation_digest=?2 WHERE id=?3 AND attestation IS NULL", vec![blob(&certificate), blob(certificate_digest), blob(data.token.operation)]))?)?;
        changed(context.sql(&statement("UPDATE catalog_leases SET attestation=?1,attestation_digest=?2 WHERE incarnation=?3 AND admission_sequence=?4 AND attestation IS NULL", vec![blob(&certificate), blob(certificate_digest), blob(data.token.owner.incarnation.as_bytes()), number(data.token.attempt)?]))?)?;
    }
    let generation = data.base.generation + 1;
    let mut encoded_catalog = BoundedEncoder::new(256)?;
    data.catalog.encode(&mut encoded_catalog)?;
    changed(context.sql(&statement(
        "INSERT INTO catalog_generations(generation,catalog,certificate) VALUES(?1,?2,?3)",
        vec![
            number(generation)?,
            blob(encoded_catalog.finish()),
            blob(certificate_digest),
        ],
    ))?)?;
    changed(context.sql(&statement(
        "UPDATE catalog_state SET generation=?1 WHERE singleton=1 AND generation=?2",
        vec![number(generation)?, number(data.base.generation)?],
    ))?)?;
    validated.apply(context)?;
    let refs = context.sql(&statement(
        "SELECT generation FROM ref_generation WHERE singleton=1",
        vec![],
    ))?;
    let Some([ref_generation]) = rows(&refs)?.first().map(Vec::as_slice) else {
        return Err(Error::Command("missing published ref generation"));
    };
    let result = PublishedRefs {
        generation,
        ref_generation: unsigned(ref_generation)?,
        certificate_digest,
    };
    let mut encoded_result = BoundedEncoder::new(128)?;
    result.encode(&mut encoded_result)?;
    if rows(&saved)?.is_empty() {
        changed(context.sql(&statement(
            "INSERT INTO pushes(id,actor,request_digest,publication,publication_plan_digest) VALUES(?1,?2,?3,?4,?5)",
            vec![
                blob(data.token.operation),
                SqlValue::Text(data.actor),
                blob(data.token.request_digest),
                blob(encoded_result.finish()),
                blob(super::ref_proof::plan_digest(&input.plan)?),
            ],
        ))?)?;
    } else {
        changed(context.sql(&statement(
            "UPDATE pushes SET publication=?1,publication_plan_digest=?3 WHERE id=?2 AND publication IS NULL",
            vec![blob(encoded_result.finish()), blob(data.token.operation), blob(super::ref_proof::plan_digest(&input.plan)?)],
        ))?)?;
    }
    changed(context.sql(&statement(
        "DELETE FROM catalog_operations WHERE id=?1",
        vec![blob(data.token.operation)],
    ))?)?;
    Ok(CommandResult::Success(PublicationReply::Published(result)))
}
pub(super) fn checkpoint(
    context: &CommandContext<'_, '_>,
    data: &super::certificate::CertificateData,
    key: &[u8; 32],
) -> cellule_runtime::Result<Option<bool>> {
    let previous = context.sql(&statement(
        "SELECT attestation,attestation_digest FROM catalog_operations WHERE id=?1",
        vec![blob(data.token.operation)],
    ))?;
    let pinned = context.sql(&statement("SELECT attestation,attestation_digest FROM catalog_leases WHERE incarnation=?1 AND admission_sequence=?2", vec![blob(data.token.owner.incarnation.as_bytes()), number(data.token.attempt)?]))?;
    if rows(&previous)? != rows(&pinned)? {
        return Err(Error::Command("publication checkpoint differs from pin"));
    }
    let missing = match rows(&previous)?.first().map(Vec::as_slice) {
        Some([SqlValue::Null, SqlValue::Null]) => true,
        Some([SqlValue::Blob(bytes), digest]) => {
            if fixed::<32>(digest)? != *blake3::hash(bytes).as_bytes() {
                return Err(Error::Command("publication checkpoint digest differs"));
            }
            let mut d = BoundedDecoder::new(bytes, CERTIFICATE_BYTES)?;
            let old = CatalogCertificate::decode(&mut d)?;
            d.finish()?;
            let mut old_data = old.data()?;
            if old_data.base.generation > data.base.generation {
                return Ok(None);
            }
            // Reconciliation changes only selected roots. Exact verified input
            // facts, attempt and retention floor must still match the checkpoint.
            old_data.base = data.base;
            old_data.catalog = data.catalog;
            old_data.refs_digest = data.refs_digest;
            old_data.completion_digest = data.completion_digest;
            if !old.authenticated(key) || old_data != *data {
                return Ok(None);
            }
            false
        }
        _ => return Err(Error::Command("invalid publication checkpoint")),
    };
    Ok(Some(missing))
}
pub(super) fn changed(sets: Vec<SqlResultSet>) -> cellule_runtime::Result<()> {
    if sets.first().is_none_or(|set| set.rows_affected != 1) {
        return Err(Error::Command("publication changed unexpected rows"));
    }
    Ok(())
}
