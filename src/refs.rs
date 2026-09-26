use cellule_runtime::{
    BoundedDecoder, BoundedEncoder, CellModule, CodecError, Command, CommandContext, CommandResult,
    Error, InvocationError, Observed, Receipt, SqlBatch, SqlResultSet, SqlStatement, SqlValue,
    WireValue,
};

use crate::{
    RepositoryCell, RepositoryModule,
    access::{access_statement, decode_access},
    directory::{TokenScope, validate_component},
};

pub(crate) const MAX_UPDATES: usize = 64;
const MAX_REF_NAME_BYTES: usize = 255;
pub(crate) const REF_PAGE_SIZE: usize = 256;

/// A bounded ref page tied to one durable ref generation, including deletions.
pub struct RefPage {
    pub generation: i64,
    pub default_branch: String,
    pub refs: Vec<(String, RefExpectation)>,
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
    pub oid: Option<[u8; 20]>,
    pub version: i64,
}

/// One ref mutation in an atomic push plan.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RefUpdate {
    pub name: String,
    pub expected: Option<RefExpectation>,
    pub new_oid: Option<[u8; 20]>,
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

    /// Reads at most 256 refs; continuations require the first page's generation.
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
                        sql: "SELECT g.generation, g.default_branch, r.name, r.oid, r.version FROM ref_generation g LEFT JOIN (SELECT name, oid, version FROM refs WHERE name > ?1 ORDER BY name LIMIT ?2) r ON 1 = 1 WHERE g.singleton = 1 ORDER BY r.name".into(),
                        parameters: vec![SqlValue::Text(after.into()), SqlValue::Integer(REF_PAGE_SIZE as i64)],
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
        let current = head.generation;
        if generation.is_some_and(|expected| expected != current) {
            return Err(RefReadError::Changed);
        }
        let mut refs = Vec::with_capacity(rows.rows.len());
        for row in &rows.rows {
            if matches!(
                row.as_slice(),
                [_, _, SqlValue::Null, SqlValue::Null, SqlValue::Null]
            ) {
                continue;
            }
            let [_, _, SqlValue::Text(name), oid, version] = row.as_slice() else {
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
            },
            receipt: result.receipt,
        })
    }
}

impl WireValue for PushPlan {
    fn encode(&self, encoder: &mut BoundedEncoder) -> Result<(), CodecError> {
        if validate_component(&self.actor).is_err() {
            return Err(CodecError::Invalid("invalid push actor"));
        }
        if self.updates.is_empty() || self.updates.len() > MAX_UPDATES {
            return Err(CodecError::Invalid("push update count is outside bounds"));
        }
        encoder.write_text(&self.actor)?;
        encoder.write_count(self.updates.len())?;
        for update in &self.updates {
            encoder.write_text(&update.name)?;
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

fn read_oid(decoder: &mut BoundedDecoder<'_>) -> Result<[u8; 20], CodecError> {
    decoder
        .read_bytes()?
        .try_into()
        .map_err(|_| CodecError::Invalid("Git object ID is not 20 bytes"))
}

/// Checks prepared graph certificates and expected versions, publishing all ref changes in one command.
pub struct FinalizePush;

impl Command for FinalizePush {
    const MODULE: &'static str = RepositoryModule::NAME;
    const ID: u32 = 3;
    const CODEC_VERSION: u32 = 3;
    type Input = PushPlan;
    type Output = bool;

    fn execute(
        context: &mut CommandContext<'_, '_>,
        plan: Self::Input,
    ) -> cellule_runtime::Result<CommandResult<Self::Output>> {
        Ok(if apply_push(context, &plan)? {
            CommandResult::Success(true)
        } else {
            CommandResult::Rejected(false)
        })
    }
}

pub(crate) fn apply_push(
    context: &mut CommandContext<'_, '_>,
    plan: &PushPlan,
) -> cellule_runtime::Result<bool> {
    if plan.updates.is_empty() || plan.updates.len() > MAX_UPDATES {
        return Ok(false);
    }
    if validate_component(&plan.actor).is_err()
        || !decode_access(&context.sql(&SqlBatch {
            statements: vec![access_statement(&plan.actor)],
        })?)?
        .is_some_and(|level| level >= TokenScope::Write)
    {
        return Ok(false);
    }
    for (index, update) in plan.updates.iter().enumerate() {
        if !valid_ref_name(&update.name)
            || update
                .expected
                .as_ref()
                .is_some_and(|old| old.version <= 0 || old.version == i64::MAX)
            || plan.updates[..index]
                .iter()
                .any(|previous| previous.name == update.name)
        {
            return Ok(false);
        }
        if update.new_oid.is_none() && update.expected.as_ref().and_then(|old| old.oid).is_none() {
            return Ok(false);
        }
        if current_ref(context, &update.name)? != update.expected {
            return Ok(false);
        }
    }
    for update in plan
        .updates
        .iter()
        .filter(|update| update.new_oid.is_some())
    {
        if plan.updates.iter().any(|other| {
            other.new_oid.is_some()
                && other.name != update.name
                && namespace_conflict(&other.name, &update.name)
        }) || existing_namespace_conflict(context, plan, &update.name)?
        {
            return Ok(false);
        }
    }
    if !crate::graph::certified_roots(context, plan)?
        || !crate::branch_rules::policies_allow(context, plan)?
    {
        return Ok(false);
    }
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
    let result = context.sql(&SqlBatch {
        statements: vec![SqlStatement {
            sql: "UPDATE ref_generation SET generation = generation + 1 WHERE singleton = 1 AND generation < 9223372036854775807".into(),
            parameters: vec![],
        }],
    })?;
    if result.first().is_none_or(|set| set.rows_affected != 1) {
        return Err(Error::Command("ref generation cannot advance"));
    }
    Ok(true)
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
    plan: &PushPlan,
    name: &str,
) -> cellule_runtime::Result<bool> {
    let result = context.sql(&SqlBatch {
        statements: vec![SqlStatement {
            sql: "SELECT name FROM refs WHERE oid IS NOT NULL AND name != ?1 AND (substr(name, 1, length(?2)) = ?2 OR substr(?1, 1, length(name) + 1) = name || '/') LIMIT 65".into(),
            parameters: vec![
                SqlValue::Text(name.into()),
                SqlValue::Text(format!("{name}/")),
            ],
        }],
    })?;
    let Some(rows) = result.first().map(|set| &set.rows) else {
        return Err(Error::Command("missing ref namespace result"));
    };
    for row in rows {
        let [SqlValue::Text(existing)] = row.as_slice() else {
            return Err(Error::Command("invalid ref namespace row"));
        };
        if !plan
            .updates
            .iter()
            .any(|update| update.name == *existing && update.new_oid.is_none())
        {
            return Ok(true);
        }
    }
    Ok(false)
}

fn namespace_conflict(left: &str, right: &str) -> bool {
    left.strip_prefix(right)
        .is_some_and(|suffix| suffix.starts_with('/'))
        || right
            .strip_prefix(left)
            .is_some_and(|suffix| suffix.starts_with('/'))
}

pub(crate) fn valid_ref_name(name: &str) -> bool {
    if !name.starts_with("refs/")
        || name.len() > MAX_REF_NAME_BYTES
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
                byte.is_ascii_graphic()
                    && !matches!(byte, b'~' | b'^' | b':' | b'?' | b'*' | b'[' | b'\\')
            })
    })
}
