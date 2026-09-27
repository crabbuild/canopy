use super::*;
use crab_cell_runtime::{
    Command, Query, registry::CommandContext, registry::CommandResult, registry::QueryContext,
};

// Credential decisions use one owner timestamp for the whole transaction.
// Crab samples context time before queueing; refresh it to fence expired
// requests without racing separate decision/update statements.
fn bind_time(mut batch: SqlBatch, admitted_at_ms: i64) -> crab_cell_runtime::Result<SqlBatch> {
    let elapsed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|source| Error::Facility {
            name: "credential clock",
            source: Box::new(source),
        })?;
    let now_ms = i64::try_from(elapsed.as_millis())
        .map_err(|_| Error::Command("credential clock overflow"))?
        .max(admitted_at_ms);
    for statement in &mut batch.statements {
        statement.parameters.insert(0, SqlValue::Integer(now_ms));
    }
    Ok(batch)
}

pub(super) struct CredentialCommand;

impl Command for CredentialCommand {
    const MODULE: &'static str = DirectoryModule::NAME;
    const ID: u32 = 3;
    const CODEC_VERSION: u32 = 1;
    type Input = SqlBatch;
    type Output = Vec<SqlResultSet>;

    fn execute(
        context: &mut CommandContext<'_, '_>,
        input: SqlBatch,
    ) -> crab_cell_runtime::Result<CommandResult<Self::Output>> {
        let batch = bind_time(input, context.now_ms())?;
        Ok(CommandResult::Success(context.sql(&batch)?))
    }
}

pub(super) struct CredentialQuery;

impl Query for CredentialQuery {
    const MODULE: &'static str = DirectoryModule::NAME;
    const ID: u32 = 4;
    const CODEC_VERSION: u32 = 1;
    type Input = SqlBatch;
    type Output = Vec<SqlResultSet>;

    fn execute(
        context: &mut QueryContext<'_>,
        input: SqlBatch,
    ) -> crab_cell_runtime::Result<Self::Output> {
        let batch = bind_time(input, context.now_ms())?;
        context.sql(&batch)
    }
}

impl DirectoryCell {
    pub(super) async fn credential_command(
        &self,
        identity: MutationIdentity,
        batch: SqlBatch,
    ) -> Result<Committed<Vec<SqlResultSet>>, InvocationError<Vec<SqlResultSet>>> {
        self.application
            .command::<CredentialCommand>(&self.target, identity, batch)
            .await
    }

    pub(super) async fn credential_query(
        &self,
        minimum: Option<Receipt>,
        batch: SqlBatch,
    ) -> Result<Observed<Vec<SqlResultSet>>, InvocationError<Vec<SqlResultSet>>> {
        self.application
            .query::<CredentialQuery>(&self.target, minimum, batch)
            .await
    }
}
