use super::*;
use crate::packs::publication::{
    commands::{authorized, check_pin, fact, load, matched},
    publish::{authenticate, changed, checkpoint, retention_matches},
    sql::*,
};

fn denied(reason: PreparationDenial) -> CommandResult<InitializationReply> {
    CommandResult::Rejected(InitializationReply::Denied(reason))
}
fn verification(
    catalog: StoredCatalog,
    refs: RefStateSnapshotRoot,
) -> Result<[u8; 32], CodecError> {
    let mut e = BoundedEncoder::new(512)?;
    catalog.encode(&mut e)?;
    refs.encode(&mut e)?;
    let mut hash = blake3::Hasher::new();
    hash.update(b"canopy.initialization-outcome.v1\0");
    hash.update(&e.finish());
    Ok(*hash.finalize().as_bytes())
}
fn saved(
    bytes: &[u8],
    repository: [u8; 16],
    format: ObjectFormat,
    digest: [u8; 32],
) -> Result<GenerationFact, CodecError> {
    let mut d = BoundedDecoder::new(bytes, 512)?;
    let fact = GenerationFact::decode(&mut d)?;
    d.finish()?;
    initial_fact(&fact)?;
    if fact
        .catalog
        .is_none_or(|root| root.repository != repository || root.format != format)
    {
        return Err(CodecError::Invalid("invalid initialization result"));
    }
    if verification(
        fact.catalog
            .ok_or(CodecError::Invalid("missing initial catalog"))?,
        fact.refs
            .ok_or(CodecError::Invalid("missing initial refs"))?,
    )? != digest
    {
        return Err(CodecError::Invalid(
            "initialization roots differ from outcome",
        ));
    }
    Ok(fact)
}
pub struct InitializeCatalogRefs;
impl Command for InitializeCatalogRefs {
    const MODULE: &'static str = RepositoryModule::NAME;
    const ID: u32 = 31;
    const CODEC_VERSION: u32 = 2;
    type Input = InitialRefProof;
    type Output = InitializationReply;
    fn execute(
        context: &mut CommandContext<'_, '_>,
        input: Self::Input,
    ) -> cellule_runtime::Result<CommandResult<Self::Output>> {
        let data = input.certificate.data()?;
        let check = LeaseCheck {
            token: data.token,
            actor: data.actor,
        };
        recovery::execute(context, &check, recovery::Kind::Initialization, |context| {
            initialize(context, input)
        })
    }
}

fn initialize(
    context: &mut CommandContext<'_, '_>,
    input: InitialRefProof,
) -> cellule_runtime::Result<CommandResult<InitializationReply>> {
    input.shape()?;
    let Some((data, key)) = authenticate(
        context,
        &input.certificate,
        Some(binding(input.refs)?),
        None,
    )?
    else {
        return Ok(denied(PreparationDenial::Unauthorized));
    };
    let logical = context.sql(&statement("SELECT actor,request_digest,verification_digest,result FROM catalog_initialization WHERE id=?1", vec![blob(data.token.operation)]))?;
    if let Some(row) = rows(&logical)?.first() {
        let [
            SqlValue::Text(actor),
            request,
            digest,
            SqlValue::Blob(bytes),
        ] = row.as_slice()
        else {
            return Err(Error::Command("invalid initialization outcome"));
        };
        if *actor != data.actor
            || fixed::<32>(request)? != data.token.request_digest
            || fixed::<32>(digest)? != verification(data.catalog, input.refs)?
        {
            return Ok(denied(PreparationDenial::Conflict));
        }
        return Ok(CommandResult::Success(InitializationReply::Initialized(
            Box::new(saved(
                bytes,
                data.token.repository,
                data.catalog.format,
                fixed(digest)?,
            )?),
        )));
    }
    // Exact recorded replay above grants no write. New initialization must
    // satisfy the current actual fence, admin role, pin and pristine state.
    if data.token.owner != context.owner_fence() {
        return Ok(denied(PreparationDenial::Stale));
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
    let pristine = context.sql(&statement("SELECT generation=0 AND default_branch=?1 AND NOT EXISTS(SELECT 1 FROM refs) AND NOT EXISTS(SELECT 1 FROM pushes WHERE initial_staging IS NULL OR response_id IS NOT NULL OR publication IS NOT NULL) AND NOT EXISTS(SELECT 1 FROM catalog_compactions) AND NOT EXISTS(SELECT 1 FROM catalog_initialization) AND NOT EXISTS(SELECT 1 FROM catalog_generations WHERE generation>0) FROM ref_generation WHERE singleton=1", vec![SqlValue::Text(INITIAL_HEAD.into())]))?;
    match rows(&pristine)?.first().map(Vec::as_slice) {
        Some([SqlValue::Integer(1)]) => {}
        Some([SqlValue::Integer(0)]) => return Ok(denied(PreparationDenial::Conflict)),
        _ => return Err(Error::Command("missing initialization state")),
    }
    let Some(missing) = checkpoint(context, &data, &key)? else {
        return Ok(denied(PreparationDenial::Conflict));
    };
    let verified_roots = verification(data.catalog, input.refs)?;
    let bytes = input.certificate.bytes()?;
    let digest = *blake3::hash(&bytes).as_bytes();
    let result = GenerationFact {
        generation: 1,
        catalog: Some(data.catalog),
        refs: Some(input.refs),
        certificate: Some(digest),
    };
    let mut encoded = BoundedEncoder::new(512)?;
    result.encode(&mut encoded)?;
    let mut catalog = BoundedEncoder::new(256)?;
    data.catalog.encode(&mut catalog)?;
    let mut refs = BoundedEncoder::new(128)?;
    input.refs.encode(&mut refs)?;
    if row.expires <= now(context.now_ms())? {
        return Ok(denied(PreparationDenial::Expired));
    }
    // No rejection after the first write: later failures abort all roots,
    // the durable outcome and checkpoint together at the Cell ACK gate.
    if missing {
        changed(context.sql(&statement("UPDATE catalog_operations SET attestation=?1,attestation_digest=?2 WHERE id=?3 AND attestation IS NULL", vec![blob(&bytes),blob(digest),blob(data.token.operation)]))?)?;
        changed(context.sql(&statement("UPDATE catalog_leases SET attestation=?1,attestation_digest=?2 WHERE incarnation=?3 AND admission_sequence=?4 AND attestation IS NULL", vec![blob(&bytes),blob(digest),blob(data.token.owner.incarnation.as_bytes()),number(data.token.attempt)?]))?)?;
    }
    changed(context.sql(&statement(
        "INSERT INTO catalog_generations(generation,catalog,certificate,refs) VALUES(1,?1,?2,?3)",
        vec![blob(catalog.finish()), blob(digest), blob(refs.finish())],
    ))?)?;
    changed(context.sql(&statement(
        "UPDATE catalog_state SET generation=1 WHERE singleton=1 AND generation=0",
        vec![],
    ))?)?;
    changed(context.sql(&statement("INSERT INTO catalog_initialization(singleton,id,actor,request_digest,verification_digest,result) VALUES(1,?1,?2,?3,?4,?5)", vec![blob(data.token.operation),SqlValue::Text(data.actor),blob(data.token.request_digest),blob(verified_roots),blob(encoded.finish())]))?)?;
    changed(context.sql(&statement(
        "DELETE FROM catalog_operations WHERE id=?1",
        vec![blob(data.token.operation)],
    ))?)?;
    Ok(CommandResult::Success(InitializationReply::Initialized(
        Box::new(result),
    )))
}

/// Fresh read authorization and exact logical identity; no namespace allocation
/// or synthetic mutation receipt. The one retained result roots empty metadata.
pub struct CheckInitializedCatalog;
impl Query for CheckInitializedCatalog {
    const MODULE: &'static str = RepositoryModule::NAME;
    const ID: u32 = 32;
    const CODEC_VERSION: u32 = 1;
    type Input = BeginRequest;
    type Output = Option<GenerationFact>;
    fn execute(
        context: &mut QueryContext<'_>,
        input: Self::Input,
    ) -> cellule_runtime::Result<Self::Output> {
        validate_component(&input.actor)?;
        // CellClient routes this query through an already-scoped target. Check
        // its persisted repository identity and current admin role here.
        if !decode_access(&context.sql(&SqlBatch {
            statements: vec![access_statement(&input.actor)],
        })?)?
        .is_some_and(|role| role >= TokenScope::Admin)
        {
            return Ok(None);
        }
        let Some(format) = identity(
            &context.sql(&statement(IDENTITY, vec![]))?,
            input.repository,
        )?
        else {
            return Ok(None);
        };
        let outcome = context.sql(&statement("SELECT actor,request_digest,verification_digest,result FROM catalog_initialization WHERE id=?1", vec![blob(input.operation)]))?;
        let Some(
            [
                SqlValue::Text(actor),
                request,
                digest,
                SqlValue::Blob(bytes),
            ],
        ) = rows(&outcome)?.first().map(Vec::as_slice)
        else {
            if rows(&outcome)?.is_empty() {
                return Ok(None);
            }
            return Err(Error::Command("invalid initialization lookup"));
        };
        if *actor != input.actor || fixed::<32>(request)? != input.request_digest {
            return Ok(None);
        }
        Ok(Some(saved(
            bytes,
            input.repository,
            format,
            fixed(digest)?,
        )?))
    }
}
