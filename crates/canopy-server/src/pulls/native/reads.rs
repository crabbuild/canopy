use super::*;
#[derive(Clone, Debug)]
pub(crate) enum ReadKind {
    Page {
        after: i64,
        state: Option<PullState>,
    },
    Detail(i64),
    Reviews {
        number: i64,
        after: i64,
    },
}
#[derive(Clone, Debug)]
pub(crate) struct ReadData {
    pub(crate) kind: ReadKind,
    pub(crate) metadata: [u8; 32],
}
impl ReadData {
    /// Bind the bounded editorial selection before privately observing its refs.
    pub(crate) fn selected(
        kind: ReadKind,
        rows: &[Vec<SqlValue>],
    ) -> cellule_runtime::Result<(Self, Vec<String>)> {
        Ok((
            Self {
                kind,
                metadata: row_binding(rows)?,
            },
            selected_names(rows)?,
        ))
    }
}
#[derive(Clone, Debug)]
pub(crate) struct ReadRequest {
    pub(crate) selection: RefSelection,
    pub(crate) data: ReadData,
}
#[derive(Debug)]
pub(crate) enum ReadReply {
    Changed,
    Rows(Option<Vec<SqlResultSet>>),
}

pub(super) fn selector(actor: ReadIdentity<'_>, kind: &ReadKind) -> SqlStatement {
    let (filter, mut parameters) = match kind {
        ReadKind::Page { after, state } => {
            let mut p = vec![actor.parameter(), SqlValue::Integer(*after)];
            let extra = if let Some(state) = state {
                p.push(SqlValue::Text(state.as_str().into()));
                " AND p.state=?3"
            } else {
                ""
            };
            (format!("p.number>?2{extra}"), p)
        }
        ReadKind::Detail(number) | ReadKind::Reviews { number, .. } => (
            "p.number=?2".into(),
            vec![actor.parameter(), SqlValue::Integer(*number)],
        ),
    };
    SqlStatement {
        sql: format!(
            "SELECT p.number,p.version,p.source_ref,p.base_ref FROM pull_requests p WHERE {filter} AND ({ACCESS}) ORDER BY p.number LIMIT {PULL_PAGE_SIZE}"
        ),
        parameters: std::mem::take(&mut parameters),
    }
}
fn row_binding(rows: &[Vec<SqlValue>]) -> cellule_runtime::Result<[u8; 32]> {
    if rows.len() > PULL_PAGE_SIZE {
        return Err(Error::Command("pull selection count"));
    }
    let mut h = blake3::Hasher::new();
    h.update(b"canopy.pull-metadata-selection.v1\0");
    h.update(&(rows.len() as u64).to_le_bytes());
    for row in rows {
        let [
            SqlValue::Integer(number),
            SqlValue::Integer(version),
            SqlValue::Text(source),
            SqlValue::Text(base),
        ] = row.as_slice()
        else {
            return Err(Error::Command("pull selection shape"));
        };
        if *number < 1
            || *version < 1
            || !valid_default_branch(source)
            || !valid_default_branch(base)
        {
            return Err(Error::Command("pull selection fields"));
        }
        h.update(&number.to_le_bytes());
        h.update(&version.to_le_bytes());
        for s in [source, base] {
            h.update(&(s.len() as u64).to_le_bytes());
            h.update(s.as_bytes());
        }
    }
    Ok(*h.finalize().as_bytes())
}
pub(super) fn selected_names(rows: &[Vec<SqlValue>]) -> cellule_runtime::Result<Vec<String>> {
    row_binding(rows)?;
    let names: std::collections::BTreeSet<_> = rows
        .iter()
        .flat_map(|r| r[2..4].iter())
        .map(|v| {
            if let SqlValue::Text(s) = v {
                Ok(s.clone())
            } else {
                Err(Error::Command("pull ref name"))
            }
        })
        .collect::<cellule_runtime::Result<_>>()?;
    let names: Vec<_> = names.into_iter().collect();
    if names.iter().map(String::len).sum::<usize>() > 512 << 10 {
        return Err(Error::Capacity("pull ref selection bytes"));
    }
    Ok(names)
}
pub(super) fn access(actor: ReadIdentity<'_>) -> SqlStatement {
    SqlStatement {
        sql: format!("SELECT ({ACCESS})"),
        parameters: vec![actor.parameter()],
    }
}
pub(super) fn allowed(sets: &[SqlResultSet]) -> cellule_runtime::Result<bool> {
    match sets.first().and_then(|s| s.rows.first()).map(Vec::as_slice) {
        Some([SqlValue::Integer(0)]) => Ok(false),
        Some([SqlValue::Integer(1)]) => Ok(true),
        _ => Err(Error::Command("invalid native pull access")),
    }
}
pub(crate) struct ReadNativePulls;
impl Query for ReadNativePulls {
    const MODULE: &'static str = RepositoryModule::NAME;
    const ID: u32 = 52;
    const CODEC_VERSION: u32 = 1;
    type Input = ReadRequest;
    type Output = ReadReply;
    fn execute(
        context: &mut QueryContext<'_>,
        input: Self::Input,
    ) -> cellule_runtime::Result<Self::Output> {
        input.encode(&mut BoundedEncoder::new(INPUT_BYTES)?)?;
        let actor = input
            .selection
            .actor
            .as_deref()
            .map_or(ReadIdentity::Anonymous, ReadIdentity::Account);
        if !allowed(&context.sql(&SqlBatch {
            statements: vec![access(actor)],
        })?)? {
            return Ok(ReadReply::Rows(None));
        }
        if !input.selection.authorized(
            context.cell_id(),
            None,
            context.now_ms(),
            input.data.digest()?,
            |q| context.sql(q),
        )? {
            return Ok(ReadReply::Changed);
        }
        let selected = context.sql(&SqlBatch {
            statements: vec![selector(actor, &input.data.kind)],
        })?;
        let rows = &selected
            .first()
            .ok_or(Error::Command("pull selection absent"))?
            .rows;
        if row_binding(rows)? != input.data.metadata
            || selected_names(rows)?
                != input
                    .selection
                    .facts
                    .iter()
                    .map(|f| f.name.clone())
                    .collect::<Vec<_>>()
        {
            return Ok(ReadReply::Changed);
        }
        let query = match input.data.kind {
            ReadKind::Page { after, state } => {
                let mut parameters = vec![actor.parameter(), SqlValue::Integer(after)];
                let filter = if let Some(state) = state {
                    parameters.push(SqlValue::Text(state.as_str().into()));
                    "AND p.state=?3"
                } else {
                    ""
                };
                SqlStatement {
                    sql: format!(
                        "SELECT {COLUMNS} FROM {JOINS} WHERE p.number>?2 {filter} AND ({ACCESS}) ORDER BY p.number LIMIT {PULL_PAGE_SIZE}"
                    ),
                    parameters,
                }
            }
            ReadKind::Detail(number) => SqlStatement {
                sql: format!(
                    "SELECT {COLUMNS},p.body,p.initial_source_oid,p.initial_base_oid,merged.id,merged.pull_number,merged.oid,merged.merged_ms,merged.pull_version,merged.source_oid,merged.source_version,merged.base_oid,merged.base_version FROM {JOINS} LEFT JOIN pull_merges merged ON merged.pull_number=p.number WHERE p.number=?2 AND ({ACCESS})"
                ),
                parameters: vec![actor.parameter(), SqlValue::Integer(number)],
            },
            ReadKind::Reviews { number, after } => {
                if rows.is_empty() {
                    return Ok(ReadReply::Rows(None));
                }
                SqlStatement {
                    sql: format!(
                        "SELECT r.number,r.id,r.reviewer,r.kind,r.body,r.pull_version,r.source_oid,r.source_version,r.base_oid,r.base_version,coalesce(({APPLICABLE}),0),r.created_ms FROM pull_reviews r JOIN pull_requests p ON p.number=r.pull_number JOIN refs s ON s.name=p.source_ref JOIN refs b ON b.name=p.base_ref WHERE r.pull_number=?2 AND r.number>?3 AND ({ACCESS}) ORDER BY r.number LIMIT {REVIEW_PAGE_SIZE}"
                    ),
                    parameters: vec![
                        actor.parameter(),
                        SqlValue::Integer(number),
                        SqlValue::Integer(after),
                    ],
                }
            }
        };
        Ok(ReadReply::Rows(Some(context.sql(&SqlBatch {
            statements: vec![with_refs(query, &input.selection)],
        })?)))
    }
}
impl RepositoryCell {
    pub(in crate::pulls) async fn native_pull_rows(
        &self,
        actor: ReadIdentity<'_>,
        kind: ReadKind,
    ) -> Result<Observed<Option<Vec<SqlResultSet>>>, NativePullError> {
        actor.validate()?;
        for _ in 0..3 {
            let selected = self
                .sql
                .query(
                    None,
                    SqlBatch {
                        statements: vec![access(actor), selector(actor, &kind)],
                    },
                )
                .await
                .map_err(|e| NativePullError::Metadata(Box::new(e)))?;
            if !allowed(&selected.output)? {
                return Ok(Observed {
                    output: None,
                    receipt: selected.receipt,
                });
            }
            let rows = &selected
                .output
                .get(1)
                .ok_or(Error::Command("native pull selection absent"))?
                .rows;
            let (data, names) = ReadData::selected(kind.clone(), rows)?;
            let snapshot = self.serving_snapshot(actor).await?;
            let selection = snapshot.ref_selection(data.digest()?, &names).await?;
            let result = self
                .application
                .query::<ReadNativePulls>(
                    &self.target,
                    Some(selected.receipt),
                    ReadRequest { selection, data },
                )
                .await
                .map_err(|e| NativePullError::Read(Box::new(e)))?;
            match result.output {
                ReadReply::Changed => continue,
                ReadReply::Rows(output) => {
                    return Ok(Observed {
                        output,
                        receipt: result.receipt,
                    });
                }
            }
        }
        Err(NativePullError::Changed)
    }
}
