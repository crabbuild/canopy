use cellule_runtime::{
    BoundedDecoder, BoundedEncoder, CellModule, CodecError, Command, CommandContext, CommandResult,
    Error, InvocationError, Observed, Receipt, SqlBatch, SqlResultSet, SqlStatement, SqlValue,
    WireValue,
};

use crate::{RepositoryCell, RepositoryModule};

const MAX_UPDATES: usize = 64;
const MAX_REF_NAME_BYTES: usize = 255;

/// Expected published tip and version, captured before a push starts.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RefExpectation {
    pub oid: [u8; 20],
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
    pub updates: Vec<RefUpdate>,
}

impl RepositoryCell {
    /// Reads one ref and the version required for an exact compare-and-swap.
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

    /// Reads at most 256 refs after a stable lexical cursor.
    pub async fn refs_page(
        &self,
        after: &str,
    ) -> std::result::Result<
        Observed<Vec<(String, RefExpectation)>>,
        InvocationError<Vec<SqlResultSet>>,
    > {
        let result = self
            .sql
            .query(
                None,
                SqlBatch {
                    statements: vec![SqlStatement {
                        sql: "SELECT name, oid, version FROM refs WHERE name > ?1 ORDER BY name LIMIT 256".into(),
                        parameters: vec![SqlValue::Text(after.into())],
                    }],
                },
            )
            .await?;
        let rows = result
            .output
            .first()
            .ok_or_else(|| InvocationError::NotStarted(Error::Command("missing refs page")))?;
        let mut refs = Vec::with_capacity(rows.rows.len());
        for row in &rows.rows {
            let [SqlValue::Text(name), oid, version] = row.as_slice() else {
                return Err(InvocationError::NotStarted(Error::Command(
                    "invalid stored ref row",
                )));
            };
            let state = decode_ref_row(&[oid.clone(), version.clone()])
                .map_err(InvocationError::NotStarted)?;
            refs.push((name.clone(), state));
        }
        Ok(Observed {
            output: refs,
            receipt: result.receipt,
        })
    }
}

impl WireValue for PushPlan {
    fn encode(&self, encoder: &mut BoundedEncoder) -> Result<(), CodecError> {
        if self.updates.is_empty() || self.updates.len() > MAX_UPDATES {
            return Err(CodecError::Invalid("push update count is outside bounds"));
        }
        encoder.write_count(self.updates.len())?;
        for update in &self.updates {
            encoder.write_text(&update.name)?;
            encoder.write_bool(update.expected.is_some())?;
            if let Some(expected) = &update.expected {
                encoder.write_bytes(&expected.oid)?;
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
        let count = decoder.read_count()?;
        if count == 0 || count > MAX_UPDATES {
            return Err(CodecError::Invalid("push update count is outside bounds"));
        }
        let mut updates = Vec::with_capacity(count);
        for _ in 0..count {
            let name = decoder.read_text()?.to_owned();
            let expected = if decoder.read_bool()? {
                Some(RefExpectation {
                    oid: read_oid(decoder)?,
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
        Ok(Self { updates })
    }
}

fn read_oid(decoder: &mut BoundedDecoder<'_>) -> Result<[u8; 20], CodecError> {
    decoder
        .read_bytes()?
        .try_into()
        .map_err(|_| CodecError::Invalid("Git object ID is not 20 bytes"))
}

/// Checks every expected version and publishes all ref changes in one Cell command.
pub struct FinalizePush;

impl Command for FinalizePush {
    const MODULE: &'static str = RepositoryModule::NAME;
    const ID: u32 = 3;
    const CODEC_VERSION: u32 = 1;
    type Input = PushPlan;
    type Output = bool;

    fn execute(
        context: &mut CommandContext<'_, '_>,
        plan: Self::Input,
    ) -> cellule_runtime::Result<CommandResult<Self::Output>> {
        if plan.updates.is_empty() || plan.updates.len() > MAX_UPDATES {
            return Ok(CommandResult::Rejected(false));
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
                return Ok(CommandResult::Rejected(false));
            }
            if update.new_oid.is_none() && update.expected.is_none() {
                return Ok(CommandResult::Rejected(false));
            }
            if let Some(new_oid) = update.new_oid
                && !object_exists(context, new_oid)?
            {
                return Ok(CommandResult::Rejected(false));
            }
            if current_ref(context, &update.name)? != update.expected {
                return Ok(CommandResult::Rejected(false));
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
            }) || existing_namespace_conflict(context, &plan, &update.name)?
            {
                return Ok(CommandResult::Rejected(false));
            }
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
                (Some(old), Some(new_oid)) => context.sql(&SqlBatch {
                    statements: vec![SqlStatement {
                        sql: "UPDATE refs SET oid = ?1, version = version + 1 WHERE name = ?2 AND oid = ?3 AND version = ?4".into(),
                        parameters: vec![
                            SqlValue::Blob(new_oid.to_vec()),
                            SqlValue::Text(update.name.clone()),
                            SqlValue::Blob(old.oid.to_vec()),
                            SqlValue::Integer(old.version),
                        ],
                    }],
                })?,
                (Some(old), None) => context.sql(&SqlBatch {
                    statements: vec![SqlStatement {
                        sql: "DELETE FROM refs WHERE name = ?1 AND oid = ?2 AND version = ?3".into(),
                        parameters: vec![
                            SqlValue::Text(update.name.clone()),
                            SqlValue::Blob(old.oid.to_vec()),
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
        Ok(CommandResult::Success(true))
    }
}

fn object_exists(context: &CommandContext<'_, '_>, oid: [u8; 20]) -> cellule_runtime::Result<bool> {
    let result = context.sql(&SqlBatch {
        statements: vec![SqlStatement {
            sql: "SELECT 1 FROM objects WHERE oid = ?1 LIMIT 1".into(),
            parameters: vec![SqlValue::Blob(oid.to_vec())],
        }],
    })?;
    Ok(result.first().is_some_and(|set| !set.rows.is_empty()))
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
    let [SqlValue::Blob(oid), SqlValue::Integer(version)] = row else {
        return Err(Error::Command("invalid stored ref"));
    };
    let oid = oid
        .as_slice()
        .try_into()
        .map_err(|_| Error::Command("invalid stored ref object ID"))?;
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
            sql: "SELECT name FROM refs WHERE name != ?1 AND (substr(name, 1, length(?2)) = ?2 OR substr(?1, 1, length(name) + 1) = name || '/') LIMIT 65".into(),
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

fn valid_ref_name(name: &str) -> bool {
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
