use std::collections::BTreeMap;

use cellule_runtime::{
    CellModule, Command, Error, InvocationError, Observed, Receipt, codec::BoundedDecoder,
    codec::BoundedEncoder, codec::CodecError, codec::WireValue, primitives::sql::SqlBatch,
    primitives::sql::SqlResultSet, primitives::sql::SqlStatement, primitives::sql::SqlValue,
    registry::CommandContext, registry::CommandResult,
};

use crate::{
    RepositoryCell, RepositoryModule,
    access::{access_statement, decode_access},
    directory::{TokenScope, validate_component},
};

pub(crate) const MAX_UPDATES: usize = 100_000;
pub(crate) const REF_PAGE_SIZE: usize = 256;
// Cellule bounds SQL results to 1 MiB; leave room for OIDs, versions and the page header.
const REF_PAGE_NAME_BYTES: i64 = 512 * 1024;

/// A bounded ref page tied to one durable ref generation, including deletions.
pub struct RefPage {
    pub generation: i64,
    pub default_branch: String,
    pub refs: Vec<(String, RefExpectation)>,
    pub has_more: bool,
}

#[derive(Debug, thiserror::Error)]
pub enum RefReadError {
    #[error("repository refs changed while reading pages")]
    Changed,
    #[error("repository ref query failed")]
    Cell(#[from] InvocationError<Vec<SqlResultSet>>),
}

/// Expected ref version and optional tip; a missing tip is a retained deletion.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RefExpectation {
    pub oid: Option<crate::ObjectId>,
    pub version: i64,
}

/// One ref mutation in an atomic push plan.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RefUpdate {
    pub name: String,
    pub expected: Option<RefExpectation>,
    pub new_oid: Option<crate::ObjectId>,
}

/// Complete ref plan; all updates commit in one Repository Cell transaction.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PushPlan {
    pub actor: String,
    pub updates: Vec<RefUpdate>,
}

impl RepositoryCell {
    /// Reads a live or deleted ref and its version; None means the name has never existed.
    pub async fn ref_state(
        &self,
        name: &str,
        minimum: Option<Receipt>,
    ) -> std::result::Result<Observed<Option<RefExpectation>>, InvocationError<Vec<SqlResultSet>>>
    {
        let result = self
            .sql
            .query(
                minimum,
                SqlBatch {
                    statements: vec![SqlStatement {
                        sql: "SELECT oid, version FROM refs WHERE name = ?1".into(),
                        parameters: vec![SqlValue::Text(name.into())],
                    }],
                },
            )
            .await?;
        let state = result
            .output
            .first()
            .and_then(|set| set.rows.first())
            .map(|row| decode_ref_row(row))
            .transpose()
            .map_err(InvocationError::NotStarted)?;
        Ok(Observed {
            output: state,
            receipt: result.receipt,
        })
    }

    /// Reads a byte-bounded page of at most 256 refs; continuations require the first page's generation.
    ///
    /// A changed generation rejects the page, requiring a new scan from the start.
    pub async fn refs_page(
        &self,
        after: &str,
        generation: Option<i64>,
    ) -> Result<Observed<RefPage>, RefReadError> {
        if !after.is_empty() && generation.is_none() {
            return Err(InvocationError::NotStarted(Error::Command(
                "ref cursor requires a generation",
            ))
            .into());
        }
        let result = self
            .sql
            .query(
                None,
                SqlBatch {
                    statements: vec![SqlStatement {
                        // One SQLite statement binds the generation even to an empty
                        // final page. Separate observations could miss a concurrent push.
                        sql: r#"
                            WITH candidates AS MATERIALIZED (
                                SELECT name, oid, version FROM refs
                                WHERE name > ?1 ORDER BY name LIMIT ?2
                            ), ranked AS (
                                SELECT name, oid, version,
                                    ROW_NUMBER() OVER (ORDER BY name) AS position,
                                    SUM(LENGTH(CAST(name AS BLOB)) + 64)
                                        OVER (ORDER BY name) AS bytes
                                FROM candidates
                            ), page AS MATERIALIZED (
                                SELECT name, oid, version FROM ranked
                                WHERE bytes <= ?3 OR position = 1
                            )
                            SELECT g.generation, g.default_branch, NULL, NULL, NULL,
                                EXISTS(SELECT 1 FROM refs WHERE name >
                                    COALESCE((SELECT MAX(name) FROM page), ?1))
                            FROM ref_generation g WHERE g.singleton = 1
                            UNION ALL
                            SELECT NULL, NULL, name, oid, version, NULL FROM page
                            ORDER BY name
                        "#
                        .into(),
                        parameters: vec![
                            SqlValue::Text(after.into()),
                            SqlValue::Integer(REF_PAGE_SIZE as i64),
                            SqlValue::Integer(REF_PAGE_NAME_BYTES),
                        ],
                    }],
                },
            )
            .await?;
        let rows = result
            .output
            .first()
            .ok_or_else(|| InvocationError::NotStarted(Error::Command("missing refs page")))?;
        let row = rows
            .rows
            .first()
            .ok_or_else(|| InvocationError::NotStarted(Error::Command("missing ref generation")))?;
        let head = crate::default_branch::decode_head(row).map_err(InvocationError::NotStarted)?;
        let has_more = match row.as_slice() {
            [
                _,
                _,
                SqlValue::Null,
                SqlValue::Null,
                SqlValue::Null,
                SqlValue::Integer(more),
            ] => *more != 0,
            _ => {
                return Err(
                    InvocationError::NotStarted(Error::Command("invalid ref page head")).into(),
                );
            }
        };
        let current = head.generation;
        if generation.is_some_and(|expected| expected != current) {
            return Err(RefReadError::Changed);
        }
        let mut refs = Vec::with_capacity(rows.rows.len().saturating_sub(1));
        for row in rows.rows.iter().skip(1) {
            let [
                SqlValue::Null,
                SqlValue::Null,
                SqlValue::Text(name),
                oid,
                version,
                SqlValue::Null,
            ] = row.as_slice()
            else {
                return Err(
                    InvocationError::NotStarted(Error::Command("invalid stored ref row")).into(),
                );
            };
            let state = decode_ref_row(&[oid.clone(), version.clone()])
                .map_err(InvocationError::NotStarted)?;
            refs.push((name.clone(), state));
        }
        Ok(Observed {
            output: RefPage {
                generation: current,
                default_branch: head.reference,
                refs,
                has_more,
            },
            receipt: result.receipt,
        })
    }
}

impl WireValue for PushPlan {
    fn encode(&self, encoder: &mut BoundedEncoder) -> Result<(), CodecError> {
        self.encode_prefix(encoder)?;
        for update in &self.updates {
            encode_update(update, encoder)?;
        }
        Ok(())
    }

    fn decode(decoder: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        let actor = decoder.read_text()?.to_owned();
        if validate_component(&actor).is_err() {
            return Err(CodecError::Invalid("invalid push actor"));
        }
        let count = decoder.read_count()?;
        if count == 0 || count > MAX_UPDATES {
            return Err(CodecError::Invalid("push update count is outside bounds"));
        }
        let mut updates = Vec::with_capacity(count);
        for _ in 0..count {
            let name = decoder.read_text()?.to_owned();
            let expected = if decoder.read_bool()? {
                Some(RefExpectation {
                    oid: if decoder.read_bool()? {
                        Some(read_oid(decoder)?)
                    } else {
                        None
                    },
                    version: decoder.read_i64()?,
                })
            } else {
                None
            };
            let new_oid = if decoder.read_bool()? {
                Some(read_oid(decoder)?)
            } else {
                None
            };
            updates.push(RefUpdate {
                name,
                expected,
                new_oid,
            });
        }
        Ok(Self { actor, updates })
    }
}
impl PushPlan {
    pub(crate) fn encode_range(
        &self,
        range: std::ops::Range<usize>,
        encoder: &mut BoundedEncoder,
    ) -> Result<(), CodecError> {
        let updates = self
            .updates
            .get(range)
            .ok_or(CodecError::Invalid("push plan range"))?;
        encode_plan_prefix(&self.actor, updates.len(), encoder)?;
        for update in updates {
            encode_update(update, encoder)?;
        }
        Ok(())
    }
    /// Shared wire prefix and update encoding allow bounded incremental hashing
    /// without allocating a second full copy of a large mirror plan.
    pub(crate) fn encode_prefix(&self, encoder: &mut BoundedEncoder) -> Result<(), CodecError> {
        encode_plan_prefix(&self.actor, self.updates.len(), encoder)
    }
}
fn encode_plan_prefix(
    actor: &str,
    count: usize,
    encoder: &mut BoundedEncoder,
) -> Result<(), CodecError> {
    if validate_component(actor).is_err() {
        return Err(CodecError::Invalid("invalid push actor"));
    }
    if count == 0 || count > MAX_UPDATES {
        return Err(CodecError::Invalid("push update count is outside bounds"));
    }
    encoder.write_text(actor)?;
    encoder.write_count(count)
}
pub(crate) fn encode_update(
    update: &RefUpdate,
    encoder: &mut BoundedEncoder,
) -> Result<(), CodecError> {
    encoder.write_text(&update.name)?;
    encode_update_suffix(update, encoder)
}
pub(crate) fn encode_update_suffix(
    update: &RefUpdate,
    encoder: &mut BoundedEncoder,
) -> Result<(), CodecError> {
    encoder.write_bool(update.expected.is_some())?;
    if let Some(expected) = &update.expected {
        encoder.write_bool(expected.oid.is_some())?;
        if let Some(oid) = expected.oid {
            encoder.write_bytes(&oid)?;
        }
        encoder.write_i64(expected.version)?;
    }
    encoder.write_bool(update.new_oid.is_some())?;
    if let Some(oid) = update.new_oid {
        encoder.write_bytes(&oid)?;
    }
    Ok(())
}

fn read_oid(decoder: &mut BoundedDecoder<'_>) -> Result<crate::ObjectId, CodecError> {
    decoder
        .read_bytes()?
        .try_into()
        .map_err(|_| CodecError::Invalid("Git object ID must be 20 or 32 bytes"))
}

/// Checks prepared graph certificates and expected versions, publishing all ref changes in one command.
pub struct FinalizePush;

impl Command for FinalizePush {
    const MODULE: &'static str = RepositoryModule::NAME;
    const ID: u32 = 3;
    const CODEC_VERSION: u32 = 4;
    type Input = PushPlan;
    type Output = bool;

    fn execute(
        context: &mut CommandContext<'_, '_>,
        plan: Self::Input,
    ) -> cellule_runtime::Result<CommandResult<Self::Output>> {
        Ok(if apply_refs(context, &plan, None)? {
            CommandResult::Success(true)
        } else {
            CommandResult::Rejected(false)
        })
    }
}

// A false decision must precede every write: HTTP completion records its
// rejection report in the same transaction without relying on rollback.
pub(crate) fn apply_refs(
    context: &mut CommandContext<'_, '_>,
    plan: &PushPlan,
    merge: Option<&crate::pulls::merge::ReviewedMerge>,
) -> cellule_runtime::Result<bool> {
    let Some(validated) = validate_refs(context, plan)? else {
        return Ok(false);
    };
    if !crate::graph::certified_roots(context, plan)?
        || !crate::branch_rules::policies_allow(context, plan, merge)?
    {
        return Ok(false);
    }
    validated.apply(context)?;
    Ok(true)
}

/// Only ref/ACL validation constructs this result. Catalog membership and
/// current branch/merge policy must also pass before consuming it. It borrows
/// the immutable plan and cannot be reused by another admitted command.
pub(crate) struct ValidatedRefs<'plan> {
    plan: &'plan PushPlan,
    target: cellule_runtime::CellTarget,
    owner: cellule_runtime::registry::OwnerFence,
    sequence: u64,
}
pub(crate) fn validate_refs<'plan>(
    context: &CommandContext<'_, '_>,
    plan: &'plan PushPlan,
) -> cellule_runtime::Result<Option<ValidatedRefs<'plan>>> {
    if plan.updates.is_empty() || plan.updates.len() > MAX_UPDATES {
        return Ok(None);
    }
    if validate_component(&plan.actor).is_err()
        || !decode_access(&context.sql(&SqlBatch {
            statements: vec![access_statement(&plan.actor)],
        })?)?
        .is_some_and(|level| level >= TokenScope::Write)
    {
        return Ok(None);
    }
    let identity = context.sql(&SqlBatch {
        statements: vec![SqlStatement {
            sql: "SELECT object_format FROM repository_identity WHERE singleton = 1".into(),
            parameters: Vec::new(),
        }],
    })?;
    let Some([SqlValue::Text(format)]) = identity
        .first()
        .and_then(|set| set.rows.first())
        .map(Vec::as_slice)
    else {
        return Err(Error::Command("repository identity is absent"));
    };
    let format = crate::ObjectFormat::parse(format)
        .ok_or(Error::Command("invalid repository object format"))?;
    if plan.updates.iter().any(|update| {
        update.new_oid.is_some_and(|oid| oid.format() != format)
            || update
                .expected
                .as_ref()
                .and_then(|old| old.oid)
                .is_some_and(|oid| oid.format() != format)
    }) {
        return Ok(None);
    }
    let updates: BTreeMap<_, _> = plan
        .updates
        .iter()
        .map(|update| (update.name.as_str(), update))
        .collect();
    if updates.len() != plan.updates.len() {
        return Ok(None);
    }
    // A first mirror push can contain thousands of refs. One transactional
    // emptiness check avoids a point lookup and namespace scan for every new
    // name while preserving the ordinary CAS path for tombstones and live refs.
    let emptiness = context.sql(&SqlBatch {
        statements: vec![SqlStatement {
            sql: "SELECT NOT EXISTS (SELECT 1 FROM refs)".into(),
            parameters: Vec::new(),
        }],
    })?;
    let refs_empty = match emptiness
        .first()
        .and_then(|set| set.rows.first())
        .map(Vec::as_slice)
    {
        Some([SqlValue::Integer(0)]) => false,
        Some([SqlValue::Integer(1)]) => true,
        _ => return Err(Error::Command("invalid ref emptiness result")),
    };
    for update in &plan.updates {
        if server_owned_ref(&update.name)
            || !valid_ref_name(&update.name)
            || update
                .expected
                .as_ref()
                .is_some_and(|old| old.version <= 0 || old.version == i64::MAX)
        {
            return Ok(None);
        }
        if update.new_oid.is_none() && update.expected.as_ref().and_then(|old| old.oid).is_none() {
            return Ok(None);
        }
        if (refs_empty && update.expected.is_some())
            || (!refs_empty && current_ref(context, &update.name)? != update.expected)
        {
            return Ok(None);
        }
    }
    for update in plan
        .updates
        .iter()
        .filter(|update| update.new_oid.is_some())
    {
        if existing_namespace_conflict(context, &updates, &update.name, refs_empty)? {
            return Ok(None);
        }
    }
    Ok(Some(ValidatedRefs {
        plan,
        target: context.target().clone(),
        owner: context.owner_fence(),
        sequence: context.sequence(),
    }))
}
impl ValidatedRefs<'_> {
    pub(crate) fn apply(self, context: &mut CommandContext<'_, '_>) -> cellule_runtime::Result<()> {
        if context.target() != &self.target
            || context.owner_fence() != self.owner
            || context.sequence() != self.sequence
        {
            return Err(Error::Command("ref validation belongs to another command"));
        }
        let plan = self.plan;
        for update in &plan.updates {
            let result = match (&update.expected, update.new_oid) {
            (None, Some(new_oid)) => context.sql(&SqlBatch {
                statements: vec![SqlStatement {
                    sql: "INSERT INTO refs (name, oid, version) VALUES (?1, ?2, 1)".into(),
                    parameters: vec![
                        SqlValue::Text(update.name.clone()),
                        SqlValue::Blob(new_oid.to_vec()),
                    ],
                }],
            })?,
            (Some(old), new_oid) => context.sql(&SqlBatch {
                statements: vec![SqlStatement {
                    // Retain deleted names so recreation cannot reset a stale push's version.
                    // The complete expected state was checked in this same transaction above.
                    sql: "UPDATE refs SET oid = ?1, version = version + 1 WHERE name = ?2 AND version = ?3".into(),
                    parameters: vec![
                        new_oid.map_or(SqlValue::Null, |oid| SqlValue::Blob(oid.to_vec())),
                        SqlValue::Text(update.name.clone()),
                        SqlValue::Integer(old.version),
                    ],
                }],
            })?,
            (None, None) => return Err(Error::Command("empty ref mutation")),
        };
            if result.first().is_none_or(|set| set.rows_affected != 1) {
                return Err(Error::Command("ref CAS changed no rows"));
            }
        }
        // Both typed pushes and HTTP completion pass here. Advance only with the
        // ref transaction so paginated readers reject mixed generations, including ABA.
        advance_generation(context)?;
        Ok(())
    }
}

pub(crate) fn server_owned_ref(name: &str) -> bool {
    name == "refs/canopy" || name.starts_with("refs/canopy/")
}

pub(crate) fn advance_generation(context: &CommandContext<'_, '_>) -> cellule_runtime::Result<()> {
    let result = context.sql(&SqlBatch {
        statements: vec![SqlStatement {
            sql: "UPDATE ref_generation SET generation = generation + 1 WHERE singleton = 1 AND generation < 9223372036854775807".into(),
            parameters: vec![],
        }],
    })?;
    if result.first().is_none_or(|set| set.rows_affected != 1) {
        return Err(Error::Command("ref generation cannot advance"));
    }
    Ok(())
}

fn current_ref(
    context: &CommandContext<'_, '_>,
    name: &str,
) -> cellule_runtime::Result<Option<RefExpectation>> {
    let result = context.sql(&SqlBatch {
        statements: vec![SqlStatement {
            sql: "SELECT oid, version FROM refs WHERE name = ?1".into(),
            parameters: vec![SqlValue::Text(name.into())],
        }],
    })?;
    let Some(row) = result.first().and_then(|set| set.rows.first()) else {
        return Ok(None);
    };
    Ok(Some(decode_ref_row(row)?))
}

fn decode_ref_row(row: &[SqlValue]) -> cellule_runtime::Result<RefExpectation> {
    let [oid, SqlValue::Integer(version)] = row else {
        return Err(Error::Command("invalid stored ref"));
    };
    let oid = match oid {
        SqlValue::Null => None,
        SqlValue::Blob(oid) => Some(
            oid.as_slice()
                .try_into()
                .map_err(|_| Error::Command("invalid stored ref object ID"))?,
        ),
        _ => return Err(Error::Command("invalid stored ref object ID")),
    };
    if *version <= 0 {
        return Err(Error::Command("invalid stored ref version"));
    }
    Ok(RefExpectation {
        oid,
        version: *version,
    })
}

fn existing_namespace_conflict(
    context: &CommandContext<'_, '_>,
    updates: &BTreeMap<&str, &RefUpdate>,
    name: &str,
    refs_empty: bool,
) -> cellule_runtime::Result<bool> {
    // Planned deletions remove namespace conflicts in this same transaction.
    // Exact ancestor lookups and indexed descendant pages avoid a full ref scan
    // per update; there is no count-based truncation of the conflict check.
    for (index, _) in name.match_indices('/') {
        let ancestor = &name[..index];
        let live = match updates.get(ancestor) {
            Some(update) => update.new_oid.is_some(),
            None if refs_empty => false,
            None => current_ref(context, ancestor)?.is_some_and(|state| state.oid.is_some()),
        };
        if live {
            return Ok(true);
        }
    }
    let prefix = format!("{name}/");
    let end = format!("{name}0");
    if updates
        .range(prefix.as_str()..end.as_str())
        .any(|(_, update)| update.new_oid.is_some())
    {
        return Ok(true);
    }
    if refs_empty {
        return Ok(false);
    }
    let mut after = prefix.clone();
    loop {
        let result = context.sql(&SqlBatch {
            statements: vec![SqlStatement {
                sql: r#"
                WITH candidates AS MATERIALIZED (
                    SELECT name FROM refs
                    WHERE name > ?1 AND name < ?2 AND oid IS NOT NULL
                    ORDER BY name LIMIT ?3
                ), ranked AS (
                    SELECT name, ROW_NUMBER() OVER (ORDER BY name) AS position,
                        SUM(LENGTH(CAST(name AS BLOB)) + 16)
                            OVER (ORDER BY name) AS bytes
                    FROM candidates
                ), page AS MATERIALIZED (
                    SELECT name FROM ranked WHERE bytes <= ?4 OR position = 1
                )
                SELECT name, EXISTS(
                    SELECT 1 FROM refs WHERE name > (SELECT MAX(name) FROM page)
                        AND name < ?2 AND oid IS NOT NULL
                ) FROM page ORDER BY name
            "#
                .into(),
                parameters: vec![
                    SqlValue::Text(after.clone()),
                    SqlValue::Text(end.clone()),
                    SqlValue::Integer(REF_PAGE_SIZE as i64),
                    SqlValue::Integer(REF_PAGE_NAME_BYTES),
                ],
            }],
        })?;
        let rows = &result
            .first()
            .ok_or(Error::Command("missing ref namespace result"))?
            .rows;
        let has_more = match rows.first().map(Vec::as_slice) {
            Some([SqlValue::Text(_), SqlValue::Integer(more)]) if *more == 0 || *more == 1 => {
                *more == 1
            }
            None => false,
            _ => return Err(Error::Command("invalid ref namespace row")),
        };
        for row in rows {
            let [SqlValue::Text(existing), SqlValue::Integer(_)] = row.as_slice() else {
                return Err(Error::Command("invalid ref namespace row"));
            };
            if updates
                .get(existing.as_str())
                .is_none_or(|update| update.new_oid.is_some())
            {
                return Ok(true);
            }
            after.clone_from(existing);
        }
        if !has_more {
            return Ok(false);
        }
    }
}

pub(crate) fn valid_ref_name(name: &str) -> bool {
    if !name.starts_with("refs/")
        || name.ends_with('/')
        || name.ends_with('.')
        || name.contains("@{")
        || name.contains("..")
        || name.contains("//")
    {
        return false;
    }
    name.split('/').all(|part| {
        !part.is_empty()
            && !part.starts_with('.')
            && !part.ends_with(".lock")
            && part.bytes().all(|byte| {
                byte > b' '
                    && byte != 0x7f
                    && !matches!(byte, b'~' | b'^' | b':' | b'?' | b'*' | b'[' | b'\\')
            })
    })
}

#[cfg(test)]
mod tests {
    use super::valid_ref_name;

    #[test]
    fn ref_format_matches_git_for_utf8_and_forbidden_ascii() -> Result<(), std::io::Error> {
        let mut names: Vec<_> = (1..=127)
            .map(|byte| format!("refs/heads/a{}b", char::from(byte)))
            .collect();
        names.extend(
            [
                "refs/heads/café",
                "refs/heads/開発",
                "refs/tags/🌳",
                "refs/heads/.hidden",
                "refs/heads/a.lock",
                "refs/heads/a.lock/b",
                "refs/heads/a..b",
                "refs/heads/a@{b",
                "refs/heads/a.",
                "refs/heads/a//b",
                "refs/heads/a/",
            ]
            .map(String::from),
        );
        names.push(format!(
            "refs/heads/{}/{}",
            "a".repeat(150),
            "b".repeat(150)
        ));
        for name in names {
            let native = std::process::Command::new("git")
                .args(["check-ref-format", &name])
                .output()?;
            assert_eq!(valid_ref_name(&name), native.status.success(), "{name:?}");
        }
        assert!(!valid_ref_name("refs/heads/a\0b"));
        Ok(())
    }
}
