use super::*;
use cellule_runtime::{
    Command, Query, registry::CommandContext, registry::CommandResult, registry::QueryContext,
};

// Authentication returns at most one row: a validated 64-byte account name,
// a five-byte scope and a 16-byte token ID. Do not reserve the generic 1-MiB
// SQL result ceiling for every small credential decision. Generic credential
// pages retain their existing operation IDs and bounds.
pub(super) const AUTHENTICATE_OPERATION: OperationDescriptor = OperationDescriptor {
    id: 5,
    codec_version: 1,
    schema_min: 1,
    schema_max: 1,
    input_limit: 36, // Canonical Vec<u8>: four-byte length plus SHA-256 digest.
    output_limit: 256,
};

pub(super) struct AuthenticateQuery;

impl Query for AuthenticateQuery {
    const MODULE: &'static str = DirectoryModule::NAME;
    const ID: u32 = AUTHENTICATE_OPERATION.id;
    const CODEC_VERSION: u32 = 1;
    type Input = Vec<u8>;
    type Output = Vec<SqlResultSet>;

    fn execute(
        context: &mut QueryContext<'_>,
        input: Self::Input,
    ) -> cellule_runtime::Result<Self::Output> {
        if input.len() != 32 {
            return Err(Error::Command("invalid authentication digest length"));
        }
        let batch = bind_time(
            SqlBatch {
                statements: vec![SqlStatement {
                    sql: "SELECT a.name, t.scope, t.id FROM access_tokens AS t JOIN accounts AS a ON a.name = t.account WHERE t.digest = ?2 AND t.enabled = 1 AND (t.expires_ms IS NULL OR t.expires_ms > ?1) AND a.enabled = 1".into(),
                    parameters: vec![SqlValue::Blob(input)],
                }],
            },
            context.now_ms(),
        )?;
        context.sql(&batch)
    }
}

// Credential decisions use one owner timestamp for the whole transaction.
// Cellule samples context time before queueing; refresh it to fence expired
// requests without racing separate decision/update statements.
fn bind_time(mut batch: SqlBatch, admitted_at_ms: i64) -> cellule_runtime::Result<SqlBatch> {
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
    ) -> cellule_runtime::Result<CommandResult<Self::Output>> {
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
    ) -> cellule_runtime::Result<Self::Output> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use cellule_runtime::codec::{BoundedDecoder, BoundedEncoder, WireValue};

    #[test]
    fn authentication_bounds_cover_the_largest_valid_principal()
    -> Result<(), Box<dyn std::error::Error>> {
        let digest = vec![7; 32];
        let mut input = BoundedEncoder::new(AUTHENTICATE_OPERATION.input_limit)?;
        digest.encode(&mut input)?;
        let encoded = input.finish();
        assert_eq!(encoded.len(), 36);
        assert_eq!(
            Vec::<u8>::decode(&mut BoundedDecoder::new(&encoded, 36)?)?,
            digest
        );
        assert!(
            vec![7_u8; 33]
                .encode(&mut BoundedEncoder::new(36)?)
                .is_err()
        );

        let name = "a".repeat(64);
        validate_component(&name)?;
        let result = vec![SqlResultSet {
            columns: vec!["name".into(), "scope".into(), "id".into()],
            rows: vec![vec![
                SqlValue::Text(name),
                SqlValue::Text(TokenScope::Admin.as_str().into()),
                SqlValue::Blob(vec![8; 16]),
            ]],
            rows_affected: 0,
        }];
        let mut output = BoundedEncoder::new(AUTHENTICATE_OPERATION.output_limit)?;
        result.encode(&mut output)?;
        let encoded = output.finish();
        assert!(encoded.len() <= 256);
        assert_eq!(
            Vec::<SqlResultSet>::decode(&mut BoundedDecoder::new(&encoded, 256)?)?,
            result
        );
        Ok(())
    }
}
