//! Check metadata receivers consume privately issued, bounded commit membership.
use super::*;
use crate::{
    ObjectId, RepositoryModule,
    packs::publication::{
        CommitMembership, MembershipRequest, ServingOwnerError, ServingReadError,
    },
};
use cellule_runtime::codec::{BoundedDecoder, BoundedEncoder, CodecError, WireValue};
use cellule_runtime::{
    CellId, CellModule, Command, Query,
    registry::{CommandContext, CommandResult, OwnerFence, QueryContext},
};
mod codec;

#[derive(Debug, thiserror::Error)]
pub enum NativeCheckError {
    #[error("invalid native check request")]
    Invalid(#[from] Error),
    #[error("native check membership unavailable")]
    Owner(#[from] ServingOwnerError),
    #[error("native check membership failed")]
    Membership(#[from] ServingReadError),
    #[error("native check page failed")]
    Read(#[source] Box<InvocationError<Option<Vec<SqlResultSet>>>>),
    #[error("native check start failed")]
    Start(#[source] Box<InvocationError<CheckChange>>),
}
#[derive(Clone, Debug)]
pub(crate) struct CommitSelection {
    pub(crate) repository: [u8; 16],
    pub(crate) actor: Option<String>,
    pub(crate) oid: ObjectId,
    pub(crate) membership: Option<CommitMembership>,
}
impl CommitSelection {
    fn authorized(
        &self,
        cell: CellId,
        owner: Option<OwnerFence>,
        now: i64,
        query: impl FnMut(&SqlBatch) -> cellule_runtime::Result<Vec<SqlResultSet>>,
    ) -> cellule_runtime::Result<bool> {
        let Some(proof) = &self.membership else {
            return Ok(false);
        };
        proof.authorize(
            MembershipRequest {
                cell,
                owner,
                repository: self.repository,
                actor: &self.actor,
                oid: self.oid,
                admitted_ms: now,
            },
            query,
        )
    }
}
#[derive(Clone, Debug)]
pub(crate) struct CommitPage {
    pub(crate) selection: CommitSelection,
    pub(crate) after: Option<String>,
}
#[derive(Clone, Debug)]
pub(crate) struct CheckStart {
    pub(crate) selection: CommitSelection,
    pub(crate) id: [u8; 16],
    pub(crate) context: String,
    pub(crate) context_version: i64,
}

pub(crate) struct ReadCommitChecks;
impl Query for ReadCommitChecks {
    const MODULE: &'static str = RepositoryModule::NAME;
    const ID: u32 = 50;
    const CODEC_VERSION: u32 = 1;
    type Input = CommitPage;
    type Output = Option<Vec<SqlResultSet>>;
    fn execute(
        context: &mut QueryContext<'_>,
        input: Self::Input,
    ) -> cellule_runtime::Result<Self::Output> {
        input.encode(&mut BoundedEncoder::new(4096)?)?;
        if !input
            .selection
            .authorized(context.cell_id(), None, context.now_ms(), |q| {
                context.sql(q)
            })?
        {
            return Ok(None);
        }
        let actor = input
            .selection
            .actor
            .as_deref()
            .map_or(ReadIdentity::Anonymous, ReadIdentity::Account);
        Ok(Some(context.sql(&SqlBatch { statements: vec![SqlStatement {
            sql: format!("SELECT c.name, c.reporter, c.enabled, c.version, {RUN_COLUMNS} FROM check_contexts c LEFT JOIN check_runs r ON r.number = (SELECT number FROM check_runs WHERE oid = ?3 AND context = c.name AND context_version = c.version ORDER BY number DESC LIMIT 1) WHERE c.enabled = 1 AND c.name > ?2 AND ({ACCESS}) ORDER BY c.name LIMIT {CHECK_PAGE_SIZE}"),
            parameters: vec![actor.parameter(), SqlValue::Text(input.after.unwrap_or_default()), SqlValue::Blob(input.selection.oid.to_vec())],
        }]})?))
    }
}
pub(crate) struct StartCommitCheck;
impl Command for StartCommitCheck {
    const MODULE: &'static str = RepositoryModule::NAME;
    const ID: u32 = 49;
    const CODEC_VERSION: u32 = 1;
    type Input = CheckStart;
    type Output = CheckChange;
    fn execute(
        context: &mut CommandContext<'_, '_>,
        input: Self::Input,
    ) -> cellule_runtime::Result<CommandResult<Self::Output>> {
        input.encode(&mut BoundedEncoder::new(4096)?)?;
        if !input.selection.authorized(
            context.target().cell_id(),
            Some(context.owner_fence()),
            context.now_ms(),
            |q| context.sql(q),
        )? {
            return Ok(CommandResult::Rejected(CheckChange::NotFound));
        }
        let actor = input
            .selection
            .actor
            .ok_or(Error::Command("check reporter missing"))?;
        let mut parameters = vec![
            SqlValue::Text(actor),
            SqlValue::Blob(input.id.to_vec()),
            SqlValue::Blob(input.selection.oid.to_vec()),
            SqlValue::Text(input.context),
            SqlValue::Integer(input.context_version),
        ];
        let decision = format!(
            "CASE WHEN NOT ({ACCESS}) OR NOT EXISTS (SELECT 1 FROM check_contexts WHERE name = ?4) THEN 'missing' WHEN NOT EXISTS (SELECT 1 FROM check_contexts WHERE name = ?4 AND reporter = ?1) THEN 'forbidden' WHEN NOT EXISTS (SELECT 1 FROM check_contexts WHERE name = ?4 AND version = ?5 AND enabled = 1) OR EXISTS (SELECT 1 FROM check_runs WHERE id = ?2 AND (oid != ?3 OR context != ?4 OR context_version != ?5 OR reporter != ?1)) THEN 'conflict' ELSE 'applied' END"
        );
        let check = SqlStatement {
            sql: format!("SELECT {decision}"),
            parameters: parameters.clone(),
        };
        parameters.push(SqlValue::Integer(context.now_ms()));
        let result = context.sql(&SqlBatch { statements:vec![check, SqlStatement {
            sql:format!("INSERT INTO check_runs (id, oid, context, context_version, reporter, state, version, summary, created_ms, updated_ms) SELECT ?2, ?3, ?4, ?5, ?1, 'queued', 1, '', ?6, ?6 WHERE ({decision}) = 'applied' AND NOT EXISTS (SELECT 1 FROM check_runs WHERE id = ?2)"), parameters,
        }]})?;
        let value = result
            .first()
            .and_then(|s| s.rows.first())
            .map(Vec::as_slice);
        match value {
            Some([SqlValue::Text(v)]) if v == "applied" => {
                Ok(CommandResult::Success(CheckChange::Applied))
            }
            Some([SqlValue::Text(v)]) if v == "missing" => {
                Ok(CommandResult::Rejected(CheckChange::NotFound))
            }
            Some([SqlValue::Text(v)]) if v == "forbidden" => {
                Ok(CommandResult::Rejected(CheckChange::Forbidden))
            }
            Some([SqlValue::Text(v)]) if v == "conflict" => {
                Ok(CommandResult::Rejected(CheckChange::Conflict))
            }
            _ => Err(Error::Command("invalid native check outcome")),
        }
    }
}
impl RepositoryCell {
    pub(super) async fn check_selection(
        &self,
        actor: ReadIdentity<'_>,
        oid: ObjectId,
    ) -> Result<(crate::packs::publication::ServingSnapshot, CommitSelection), NativeCheckError>
    {
        actor.validate()?;
        let snapshot = self.serving_snapshot(actor).await?;
        let membership = snapshot.commit_membership(oid).await?;
        let selection = CommitSelection {
            repository: self.id,
            actor: match actor {
                ReadIdentity::Anonymous => None,
                ReadIdentity::Account(v) => Some(v.into()),
            },
            oid,
            membership,
        };
        Ok((snapshot, selection))
    }
}
