use super::*;
use crate::packs::publication::{
    commands::{authorized, check_pin, fact, load, matched},
    publish::{authenticate, changed, checkpoint, retention_matches},
    sql::*,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PublishedCompaction {
    pub generation: u64,
    pub certificate_digest: [u8; 32],
}
impl WireValue for PublishedCompaction {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        if self.generation == 0 || self.generation > i64::MAX as u64 {
            return Err(CodecError::Invalid("invalid compaction generation"));
        }
        e.write_u64(self.generation)?;
        e.write_bytes(&self.certificate_digest)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        let value = Self {
            generation: d.read_u64()?,
            certificate_digest: crate::packs::directory::index::codec::fixed(d)?,
        };
        let mut e = BoundedEncoder::new(128)?;
        value.encode(&mut e)?;
        Ok(value)
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CompactionReply {
    Published(PublishedCompaction),
    Denied(PreparationDenial),
}
impl WireValue for CompactionReply {
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
        let tag = d.read_u8()?;
        if tag == 0 {
            return Ok(Self::Published(PublishedCompaction::decode(d)?));
        }
        let reason = match tag {
            1 => PreparationDenial::Unauthorized,
            2 => PreparationDenial::Conflict,
            3 => PreparationDenial::Stale,
            4 => PreparationDenial::Expired,
            5 => PreparationDenial::Capacity,
            6 => PreparationDenial::Missing,
            _ => return Err(CodecError::Invalid("invalid compaction reply")),
        };
        Ok(Self::Denied(reason))
    }
}
fn denied(reason: PreparationDenial) -> CommandResult<CompactionReply> {
    CommandResult::Rejected(CompactionReply::Denied(reason))
}
fn verification(data: &certificate::CertificateData) -> [u8; 32] {
    let mut hash = blake3::Hasher::new();
    hash.update(b"canopy.compaction-verification.v1\0");
    hash.update(&data.inputs_digest);
    hash.update(&data.inventory_digest);
    for count in [data.object_count, data.edge_count, data.input_count] {
        hash.update(&count.to_be_bytes());
    }
    *hash.finalize().as_bytes()
}
fn saved_result(bytes: &[u8]) -> Result<PublishedCompaction, CodecError> {
    let mut d = BoundedDecoder::new(bytes, 128)?;
    let value = PublishedCompaction::decode(&mut d)?;
    d.finish()?;
    Ok(value)
}
pub struct PublishCatalogCompaction;
impl Command for PublishCatalogCompaction {
    const MODULE: &'static str = RepositoryModule::NAME;
    const ID: u32 = 22;
    const CODEC_VERSION: u32 = 1;
    type Input = CatalogCertificate;
    type Output = CompactionReply;
    fn execute(
        context: &mut CommandContext<'_, '_>,
        certificate: Self::Input,
    ) -> cellule_runtime::Result<CommandResult<Self::Output>> {
        let Some((data, key)) = authenticate(context, &certificate, None, None)? else {
            return Ok(denied(PreparationDenial::Unauthorized));
        };
        if !data.compaction || data.object_count == 0 || data.input_count == 0 {
            return Ok(denied(PreparationDenial::Unauthorized));
        }
        let Some(format) = authorized(
            context,
            data.token.repository,
            &data.actor,
            TokenScope::Admin,
        )?
        else {
            return Ok(denied(PreparationDenial::Unauthorized));
        };
        let saved = context.sql(&statement("SELECT actor,request_digest,verification_digest,result FROM catalog_compactions WHERE id=?1", vec![blob(data.token.operation)]))?;
        if let Some(row) = rows(&saved)?.first() {
            let [
                SqlValue::Text(actor),
                digest,
                binding,
                SqlValue::Blob(result),
            ] = row.as_slice()
            else {
                return Err(Error::Command("invalid compaction outcome"));
            };
            if *actor != data.actor
                || fixed::<32>(digest)? != data.token.request_digest
                || fixed::<32>(binding)? != verification(&data)
            {
                return Ok(denied(PreparationDenial::Conflict));
            }
            return Ok(CommandResult::Success(CompactionReply::Published(
                saved_result(result)?,
            )));
        }
        if !rows(&context.sql(&statement(
            "SELECT id FROM pushes WHERE id=?1 AND NOT(actor=?2 AND request_digest=?3 AND initial_staging IS NOT NULL AND response_id IS NULL AND response_root IS NULL AND publication IS NULL)",
            vec![blob(data.token.operation), SqlValue::Text(data.actor.clone()), blob(data.token.request_digest)],
        ))?)?
        .is_empty()
        {
            return Ok(denied(PreparationDenial::Conflict));
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
        if format != data.catalog.format
            || !retention_matches(context, &data, row.generation, format)?
            || fact(context, data.token.repository, format, None)? != data.base
        {
            return Ok(denied(PreparationDenial::Conflict));
        }
        let counts = context.sql(&statement(
            "SELECT count(*) FROM (SELECT generation FROM catalog_generations LIMIT ?1)",
            vec![number(MAX_RETAINED_GENERATIONS)?],
        ))?;
        let Some([count]) = rows(&counts)?.first().map(Vec::as_slice) else {
            return Err(Error::Command("missing compaction generation count"));
        };
        if unsigned(count)? >= MAX_RETAINED_GENERATIONS || data.base.generation >= i64::MAX as u64 {
            return Ok(denied(PreparationDenial::Capacity));
        }
        let Some(missing) = checkpoint(context, &data, &key)? else {
            return Ok(denied(PreparationDenial::Conflict));
        };
        let bytes = certificate.bytes()?;
        let digest = *blake3::hash(&bytes).as_bytes();
        if row.expires <= now(context.now_ms())? {
            return Ok(denied(PreparationDenial::Expired));
        }
        // No rejected result after writes. Every later error aborts the entire
        // Cell transaction; the SDK's selected durability gate controls ACK.
        if missing {
            changed(context.sql(&statement("UPDATE catalog_operations SET attestation=?1,attestation_digest=?2 WHERE id=?3 AND attestation IS NULL", vec![blob(&bytes),blob(digest),blob(data.token.operation)]))?)?;
            changed(context.sql(&statement("UPDATE catalog_leases SET attestation=?1,attestation_digest=?2 WHERE incarnation=?3 AND admission_sequence=?4 AND attestation IS NULL", vec![blob(&bytes),blob(digest),blob(data.token.owner.incarnation.as_bytes()),number(data.token.attempt)?]))?)?;
        }
        let generation = data.base.generation + 1;
        let mut encoded = BoundedEncoder::new(256)?;
        data.catalog.encode(&mut encoded)?;
        let mut refs = BoundedEncoder::new(128)?;
        if let Some(root) = data.base.refs {
            root.encode(&mut refs)?;
        }
        changed(context.sql(&statement(
            "INSERT INTO catalog_generations(generation,catalog,certificate,refs) VALUES(?1,?2,?3,?4)",
            vec![
                number(generation)?, blob(encoded.finish()), blob(digest),
                data.base.refs.map_or(SqlValue::Null, |_| blob(refs.finish())),
            ],
        ))?)?;
        changed(context.sql(&statement(
            "UPDATE catalog_state SET generation=?1 WHERE singleton=1 AND generation=?2",
            vec![number(generation)?, number(data.base.generation)?],
        ))?)?;
        let result = PublishedCompaction {
            generation,
            certificate_digest: digest,
        };
        let mut encoded = BoundedEncoder::new(128)?;
        result.encode(&mut encoded)?;
        changed(context.sql(&statement("INSERT INTO catalog_compactions(id,actor,request_digest,verification_digest,result) VALUES(?1,?2,?3,?4,?5)", vec![blob(data.token.operation),SqlValue::Text(data.actor.clone()),blob(data.token.request_digest),blob(verification(&data)),blob(encoded.finish())]))?)?;
        changed(context.sql(&statement(
            "DELETE FROM catalog_operations WHERE id=?1",
            vec![blob(data.token.operation)],
        ))?)?;
        Ok(CommandResult::Success(CompactionReply::Published(result)))
    }
}
/// Read-only logical recovery/preflight. This cannot allocate a namespace,
/// return a new mutation receipt, or grant retention/deletion authority.
pub struct CheckCompletedCompaction;
impl Query for CheckCompletedCompaction {
    const MODULE: &'static str = RepositoryModule::NAME;
    const ID: u32 = 23;
    const CODEC_VERSION: u32 = 1;
    type Input = BeginRequest;
    type Output = Option<CompactionReply>;
    fn execute(
        context: &mut QueryContext<'_>,
        input: Self::Input,
    ) -> cellule_runtime::Result<Self::Output> {
        validate_component(&input.actor)?;
        if !decode_access(&context.sql(&SqlBatch {
            statements: vec![access_statement(&input.actor)],
        })?)?
        .is_some_and(|role| role >= TokenScope::Admin)
            || identity(
                &context.sql(&statement(IDENTITY, vec![]))?,
                input.repository,
            )?
            .is_none()
        {
            return Ok(Some(CompactionReply::Denied(
                PreparationDenial::Unauthorized,
            )));
        }
        let saved = context.sql(&statement(
            "SELECT actor,request_digest,result FROM catalog_compactions WHERE id=?1",
            vec![blob(input.operation)],
        ))?;
        let Some(row) = rows(&saved)?.first() else {
            return Ok(None);
        };
        let [SqlValue::Text(actor), digest, SqlValue::Blob(result)] = row.as_slice() else {
            return Err(Error::Command("invalid compaction lookup"));
        };
        if *actor != input.actor || fixed::<32>(digest)? != input.request_digest {
            return Ok(Some(CompactionReply::Denied(PreparationDenial::Conflict)));
        }
        Ok(Some(CompactionReply::Published(saved_result(result)?)))
    }
}
