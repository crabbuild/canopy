//! Native ref facts authorize editorial SQL inside one final typed transaction.
use super::*;
use crate::{
    RepositoryModule,
    packs::publication::{REF_SELECTION_BYTES, RefSelection},
};
use cellule_runtime::codec::{BoundedDecoder, BoundedEncoder, CodecError, WireValue};
use cellule_runtime::{
    CellModule, Command, Query,
    registry::{CommandContext, CommandResult, QueryContext},
};
mod codec;
mod reads;
pub(crate) use reads::{ReadData, ReadKind, ReadNativePulls, ReadReply, ReadRequest};
mod client;
pub(crate) mod threads;
pub(crate) use threads::CreateNativeThread;

pub(crate) const INPUT_BYTES: u32 = REF_SELECTION_BYTES + (256 << 10);
pub(crate) const OUTPUT_BYTES: u32 = 1 << 20;
#[derive(Debug, thiserror::Error)]
pub enum NativePullError {
    #[error("invalid native pull request")]
    Invalid(#[from] Error),
    #[error("native pull codec failed")]
    Codec(#[from] CodecError),
    #[error("native pull snapshot unavailable")]
    Owner(#[from] crate::packs::publication::ServingOwnerError),
    #[error("native pull ref selection failed")]
    Serving(#[from] crate::packs::publication::ServingReadError),
    #[error("native pull metadata selection failed")]
    Metadata(#[source] Box<Invocation>),
    #[error("native pull read failed")]
    Read(#[source] Box<dyn std::error::Error + Send + Sync>),
    #[error("native pull command failed")]
    Command(#[source] Box<InvocationError<PullChange>>),
    #[error("native pull refs or editorial selection changed")]
    Changed,
}
#[derive(Clone, Debug)]
pub(crate) struct CreateData {
    pub(crate) id: [u8; 16],
    pub(crate) title: String,
    pub(crate) body: String,
    pub(crate) draft: bool,
    pub(crate) source_ref: String,
    pub(crate) source_oid: String,
    pub(crate) base_ref: String,
    pub(crate) base_oid: String,
}
impl CreateData {
    pub(crate) fn view(&self) -> NewPull<'_> {
        NewPull {
            id: self.id,
            title: &self.title,
            body: &self.body,
            draft: self.draft,
            source_ref: &self.source_ref,
            source_oid: &self.source_oid,
            base_ref: &self.base_ref,
            base_oid: &self.base_oid,
        }
    }
}
#[derive(Clone, Debug)]
pub(crate) struct ReviewData {
    pub(crate) number: i64,
    pub(crate) id: [u8; 16],
    pub(crate) revision: PullRevision,
    pub(crate) kind: ReviewKind,
    pub(crate) body: String,
}
impl ReviewData {
    pub(crate) fn view(&self) -> NewReview<'_> {
        NewReview {
            id: self.id,
            revision: &self.revision,
            kind: self.kind,
            body: &self.body,
        }
    }
}
#[derive(Clone, Debug)]
pub(crate) struct CreateRequest {
    pub(crate) selection: RefSelection,
    pub(crate) data: CreateData,
}
#[derive(Clone, Debug)]
pub(crate) struct ReviewRequest {
    pub(crate) selection: RefSelection,
    pub(crate) data: ReviewData,
}

/// Shadow the retired table only within this statement with authenticated facts.
/// Names/OIDs/versions are bound parameters, never interpolated client SQL.
pub(crate) fn with_refs(mut statement: SqlStatement, selection: &RefSelection) -> SqlStatement {
    if !statement.sql.contains("FROM refs ") && !statement.sql.contains("JOIN refs ") {
        return statement;
    }
    let mut values = Vec::with_capacity(selection.facts.len());
    for fact in &selection.facts {
        let n = statement.parameters.len() + 1;
        values.push(format!("(?{n},?{},?{})", n + 1, n + 2));
        statement.parameters.extend([
            SqlValue::Text(fact.name.clone()),
            fact.state
                .as_ref()
                .and_then(|s| s.oid)
                .map_or(SqlValue::Null, |o| SqlValue::Blob(o.to_vec())),
            SqlValue::Integer(fact.state.as_ref().map_or(0, |s| s.version)),
        ]);
    }
    let refs = if values.is_empty() {
        "SELECT NULL,NULL,0 WHERE 0".into()
    } else {
        format!("VALUES {}", values.join(","))
    };
    statement.sql = format!("WITH refs(name,oid,version) AS ({refs}) {}", statement.sql);
    statement
}
fn transaction(
    context: &mut CommandContext<'_, '_>,
    selection: &RefSelection,
    statements: Vec<SqlStatement>,
) -> cellule_runtime::Result<CommandResult<PullChange>> {
    let statements = statements
        .into_iter()
        .map(|s| with_refs(s, selection))
        .collect();
    let change = mutations::change(&context.sql(&SqlBatch { statements })?)?;
    Ok(match change {
        PullChange::Applied(_) => CommandResult::Success(change),
        _ => CommandResult::Rejected(change),
    })
}
fn denial(
    context: &mut CommandContext<'_, '_>,
    selection: &RefSelection,
) -> cellule_runtime::Result<CommandResult<PullChange>> {
    let actor = selection
        .actor
        .as_deref()
        .map_or(ReadIdentity::Anonymous, ReadIdentity::Account);
    let permitted = reads::allowed(&context.sql(&SqlBatch {
        statements: vec![reads::access(actor)],
    })?)?;
    Ok(CommandResult::Rejected(if permitted {
        PullChange::Conflict
    } else {
        PullChange::NotFound
    }))
}
pub(crate) struct CreateNativePull;
impl Command for CreateNativePull {
    const MODULE: &'static str = RepositoryModule::NAME;
    const ID: u32 = 51;
    const CODEC_VERSION: u32 = 1;
    type Input = CreateRequest;
    type Output = PullChange;
    fn execute(
        context: &mut CommandContext<'_, '_>,
        input: Self::Input,
    ) -> cellule_runtime::Result<CommandResult<Self::Output>> {
        input.encode(&mut BoundedEncoder::new(INPUT_BYTES)?)?;
        if !input.selection.authorized(
            context.target().cell_id(),
            Some(context.owner_fence()),
            context.now_ms(),
            input.data.digest()?,
            |q| context.sql(q),
        )? {
            return denial(context, &input.selection);
        }
        let actor = input
            .selection
            .actor
            .as_deref()
            .ok_or(Error::Command("pull author missing"))?;
        let statements = mutations::create_statements(actor, input.data.view(), context.now_ms())?;
        transaction(context, &input.selection, statements)
    }
}
pub(crate) struct ReviewNativePull;
impl Command for ReviewNativePull {
    const MODULE: &'static str = RepositoryModule::NAME;
    const ID: u32 = 53;
    const CODEC_VERSION: u32 = 1;
    type Input = ReviewRequest;
    type Output = PullChange;
    fn execute(
        context: &mut CommandContext<'_, '_>,
        input: Self::Input,
    ) -> cellule_runtime::Result<CommandResult<Self::Output>> {
        input.encode(&mut BoundedEncoder::new(INPUT_BYTES)?)?;
        if !input.selection.authorized(
            context.target().cell_id(),
            Some(context.owner_fence()),
            context.now_ms(),
            input.data.digest()?,
            |q| context.sql(q),
        )? {
            return denial(context, &input.selection);
        }
        let actor = input
            .selection
            .actor
            .as_deref()
            .ok_or(Error::Command("reviewer missing"))?;
        let statements = mutations::review_statements(
            actor,
            input.data.number,
            input.data.view(),
            context.now_ms(),
        )?;
        transaction(context, &input.selection, statements)
    }
}
