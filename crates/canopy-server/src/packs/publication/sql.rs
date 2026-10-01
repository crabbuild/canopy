use super::*;
use std::time::{SystemTime, UNIX_EPOCH};
pub(super) const OPERATION: &str = "SELECT actor,request_digest,incarnation,owner_epoch,admission_sequence,artifact_operation,generation,expires_at_ms FROM catalog_operations WHERE id=?1";

pub(super) fn statement(sql: &str, parameters: Vec<SqlValue>) -> SqlBatch {
    SqlBatch {
        statements: vec![SqlStatement {
            sql: sql.into(),
            parameters,
        }],
    }
}
pub(super) fn blob(bytes: impl AsRef<[u8]>) -> SqlValue {
    SqlValue::Blob(bytes.as_ref().to_vec())
}
pub(super) fn number(value: u64) -> cellule_runtime::Result<SqlValue> {
    Ok(SqlValue::Integer(
        i64::try_from(value).map_err(|_| Error::Command("catalog integer overflow"))?,
    ))
}
pub(super) fn rows(sets: &[SqlResultSet]) -> cellule_runtime::Result<&[Vec<SqlValue>]> {
    Ok(&sets
        .first()
        .ok_or(Error::Command("missing catalog SQL result"))?
        .rows)
}
pub(super) fn unsigned(value: &SqlValue) -> cellule_runtime::Result<u64> {
    match value {
        SqlValue::Integer(value) if *value >= 0 => Ok(*value as u64),
        _ => Err(Error::Command("invalid catalog integer")),
    }
}
pub(super) fn fixed<const N: usize>(value: &SqlValue) -> cellule_runtime::Result<[u8; N]> {
    match value {
        SqlValue::Blob(value) => value
            .as_slice()
            .try_into()
            .map_err(|_| Error::Command("invalid catalog bytes")),
        _ => Err(Error::Command("invalid catalog bytes")),
    }
}
// Context time is sampled before the worker queue. Refresh once for lease
// decisions, clamping against logical time just like credential expiry checks.
pub(super) fn now(admitted: i64) -> cellule_runtime::Result<i64> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|source| Error::Facility {
            name: "catalog lease clock",
            source: Box::new(source),
        })?;
    Ok(i64::try_from(elapsed.as_millis())
        .map_err(|_| Error::Command("catalog lease clock overflow"))?
        .max(admitted))
}
pub(super) fn expiry(now: i64, ms: u64) -> cellule_runtime::Result<i64> {
    if ms == 0 || ms > MAX_LEASE_MS {
        return Err(Error::Command("invalid catalog lease duration"));
    }
    now.checked_add(ms as i64)
        .ok_or(Error::Command("catalog lease expiry overflow"))
}
pub(super) fn identity(
    sets: &[SqlResultSet],
    repository: [u8; 16],
) -> cellule_runtime::Result<Option<ObjectFormat>> {
    let Some(row) = rows(sets)?.first() else {
        return Ok(None);
    };
    let [id, SqlValue::Text(format)] = row.as_slice() else {
        return Err(Error::Command("invalid catalog repository identity"));
    };
    if fixed::<16>(id)? != repository {
        return Ok(None);
    }
    Ok(Some(ObjectFormat::parse(format).ok_or(Error::Command(
        "invalid catalog repository format",
    ))?))
}
pub(super) fn generation(
    sets: &[SqlResultSet],
    repository: [u8; 16],
    format: ObjectFormat,
) -> cellule_runtime::Result<GenerationFact> {
    let Some([generation, catalog, certificate]) = rows(sets)?.first().map(Vec::as_slice) else {
        return Err(Error::Command("missing catalog generation fact"));
    };
    let generation = unsigned(generation)?;
    let catalog = match catalog {
        SqlValue::Null => None,
        SqlValue::Blob(bytes) => {
            let mut decoder = BoundedDecoder::new(bytes, 256)?;
            let catalog = StoredCatalog::decode(&mut decoder)?;
            decoder.finish()?;
            if catalog.repository != repository || catalog.format != format {
                return Err(Error::Command("catalog generation context differs"));
            }
            Some(catalog)
        }
        _ => return Err(Error::Command("invalid catalog generation descriptor")),
    };
    let certificate = match certificate {
        SqlValue::Null => None,
        value => Some(fixed(value)?),
    };
    let fact = GenerationFact {
        generation,
        catalog,
        certificate,
    };
    fact.validate()?;
    Ok(fact)
}
#[derive(Clone)]
pub(super) struct Operation {
    pub actor: String,
    pub token: PreparationToken,
    pub generation: u64,
    pub expires: i64,
}
pub(super) fn operation(
    sets: &[SqlResultSet],
    repository: [u8; 16],
    operation: [u8; 16],
) -> cellule_runtime::Result<Option<Operation>> {
    let Some(row) = rows(sets)?.first() else {
        return Ok(None);
    };
    let [
        SqlValue::Text(actor),
        digest,
        incarnation,
        epoch,
        sequence,
        artifact_operation,
        generation,
        SqlValue::Integer(expires),
    ] = row.as_slice()
    else {
        return Err(Error::Command("invalid catalog operation row"));
    };
    let token = PreparationToken {
        repository,
        operation,
        artifact_operation: fixed(artifact_operation)?,
        request_digest: fixed(digest)?,
        owner: OwnerFence {
            incarnation: IncarnationId::from_bytes(fixed(incarnation)?),
            epoch: u64::from_be_bytes(fixed(epoch)?),
        },
        attempt: unsigned(sequence)?,
    };
    if token.owner.epoch == 0 || token.attempt == 0 || *expires < 0 {
        return Err(Error::Command("invalid catalog operation authority"));
    }
    codec::artifact_valid(token.artifact_operation)?;
    Ok(Some(Operation {
        actor: actor.clone(),
        token,
        generation: unsigned(generation)?,
        expires: *expires,
    }))
}
pub(super) fn grant(
    operation: &Operation,
    format: ObjectFormat,
    base: GenerationFact,
    now: i64,
) -> cellule_runtime::Result<PreparationLease> {
    if operation.generation != base.generation || operation.expires <= now {
        return Err(Error::Command("catalog lease and generation differ"));
    }
    Ok(PreparationLease {
        token: operation.token,
        base,
        format,
        observed_at_ms: now,
        expires_at_ms: operation.expires,
    })
}
pub(super) const IDENTITY: &str =
    "SELECT repository_id,object_format FROM repository_identity WHERE singleton=1";
pub(super) const CURRENT: &str = "SELECT g.generation,g.catalog,g.certificate FROM catalog_state s JOIN catalog_generations g ON g.generation=s.generation WHERE s.singleton=1";
pub(super) const GENERATION: &str =
    "SELECT generation,catalog,certificate FROM catalog_generations WHERE generation=?1";
pub(super) fn quota(
    context: &CommandContext<'_, '_>,
    new_operation: bool,
) -> cellule_runtime::Result<bool> {
    let leases = context.sql(&statement(
        "SELECT count(*) FROM (SELECT admission_sequence FROM catalog_leases LIMIT ?1)",
        vec![number(MAX_GENERATION_LEASES + 1)?],
    ))?;
    let Some([count]) = rows(&leases)?.first().map(Vec::as_slice) else {
        return Err(Error::Command("missing catalog lease count"));
    };
    if unsigned(count)? >= MAX_GENERATION_LEASES {
        return Ok(false);
    }
    if new_operation {
        let operations = context.sql(&statement(
            "SELECT count(*) FROM (SELECT id FROM catalog_operations LIMIT ?1)",
            vec![number(MAX_OPERATIONS + 1)?],
        ))?;
        let Some([count]) = rows(&operations)?.first().map(Vec::as_slice) else {
            return Err(Error::Command("missing catalog operation count"));
        };
        if unsigned(count)? >= MAX_OPERATIONS {
            return Ok(false);
        }
    }
    Ok(true)
}
pub(super) fn insert_lease(
    context: &CommandContext<'_, '_>,
    token: PreparationToken,
    generation: u64,
    expires: i64,
) -> cellule_runtime::Result<()> {
    context.sql(&statement("INSERT INTO catalog_leases(incarnation,admission_sequence,operation,owner_epoch,artifact_operation,generation,expires_at_ms) VALUES(?1,?2,?3,?4,?5,?6,?7)",vec![blob(token.owner.incarnation.as_bytes()),number(token.attempt)?,blob(token.operation),blob(token.owner.epoch.to_be_bytes()),blob(token.artifact_operation),number(generation)?,SqlValue::Integer(expires)]))?;
    Ok(())
}

/// Persistent monotonic namespace allocation inside the admitted transaction.
/// It is preserved by snapshots/recovery; isolated rollback restores must use
/// a new provider namespace, as the deployment restore contract requires.
pub(super) fn allocate_artifacts(
    context: &CommandContext<'_, '_>,
) -> cellule_runtime::Result<[u8; 16]> {
    let changed = context.sql(&statement("UPDATE repository_identity SET artifact_sequence=artifact_sequence+1 WHERE singleton=1 AND artifact_sequence<9223372036854775807", vec![]))?;
    if changed.first().is_none_or(|set| set.rows_affected != 1) {
        return Err(Error::Command(
            "artifact namespace allocator is exhausted or absent",
        ));
    }
    let result = context.sql(&statement(
        "SELECT artifact_sequence FROM repository_identity WHERE singleton=1",
        vec![],
    ))?;
    let Some([value]) = rows(&result)?.first().map(Vec::as_slice) else {
        return Err(Error::Command("artifact namespace allocator is absent"));
    };
    let sequence = unsigned(value)?;
    let mut operation = *b"CANOPY01\0\0\0\0\0\0\0\0";
    operation[8..].copy_from_slice(&sequence.to_be_bytes());
    Ok(operation)
}
