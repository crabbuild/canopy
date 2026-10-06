//! Durable symbolic HEAD, versioned with the repository's ref snapshot.

use cellule_runtime::{
    Committed, Error, InvocationError, MutationIdentity, Observed, Receipt,
    primitives::sql::SqlBatch, primitives::sql::SqlResultSet, primitives::sql::SqlStatement,
    primitives::sql::SqlValue,
};

use crate::{ReadIdentity, RepositoryCell, directory::validate_component, refs::valid_ref_name};

/// Repository symbolic HEAD and the ref generation required to change it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DefaultBranch {
    pub reference: String,
    pub generation: i64,
}

impl RepositoryCell {
    /// Reads symbolic HEAD and its ref generation from one SQLite observation.
    pub async fn default_branch(
        &self,
        minimum: Option<Receipt>,
    ) -> Result<Observed<DefaultBranch>, InvocationError<Vec<SqlResultSet>>> {
        let result = self.sql.query(minimum, SqlBatch {
            statements: vec![SqlStatement {
                sql: "SELECT generation, default_branch FROM ref_generation WHERE singleton = 1".into(),
                parameters: vec![],
            }],
        }).await?;
        let row = result
            .output
            .first()
            .and_then(|set| set.rows.first())
            .ok_or_else(|| InvocationError::NotStarted(Error::Command("missing default branch")))?;
        let output = decode_head(row).map_err(InvocationError::NotStarted)?;
        Ok(Observed {
            output,
            receipt: result.receipt,
        })
    }

    /// Reads the constant-size summary with current access in the same query.
    /// Native publishers update this row atomically with the immutable snapshot;
    /// metadata reads acquire no serving pin and publish no Cell root.
    pub async fn default_branch_for(
        &self,
        actor: ReadIdentity<'_>,
        minimum: Option<Receipt>,
    ) -> Result<Observed<Option<DefaultBranch>>, InvocationError<Vec<SqlResultSet>>> {
        actor.validate().map_err(InvocationError::NotStarted)?;
        let result = self.sql.query(minimum, SqlBatch {
            statements: vec![SqlStatement {
                sql: format!("SELECT generation, default_branch FROM ref_generation WHERE singleton=1 AND ({}) AND EXISTS (SELECT 1 FROM catalog_state s JOIN catalog_generations g ON g.generation=s.generation WHERE s.singleton=1 AND g.refs IS NOT NULL)", crate::access::READ_ACCESS),
                parameters: vec![actor.parameter()],
            }],
        }).await?;
        let output = result
            .output
            .first()
            .ok_or_else(|| {
                InvocationError::NotStarted(Error::Command("missing HEAD query result"))
            })?
            .rows
            .first()
            .map(|row| decode_head(row))
            .transpose()
            .map_err(InvocationError::NotStarted)?;
        Ok(Observed {
            output,
            receipt: result.receipt,
        })
    }

    /// Retired SQL writer: HEAD changes require resident native publication.
    pub async fn set_default_branch(
        &self,
        _identity: MutationIdentity,
        actor: &str,
        expected_generation: i64,
        reference: &str,
    ) -> Result<Committed<bool>, InvocationError<Vec<SqlResultSet>>> {
        validate_component(actor).map_err(InvocationError::NotStarted)?;
        if !valid_default_branch(reference) || !(0..i64::MAX).contains(&expected_generation) {
            return Err(InvocationError::NotStarted(Error::Command(
                "invalid default branch update",
            )));
        }
        Err(InvocationError::NotStarted(Error::Command(
            "default branch changes require resident native publication",
        )))
    }
}

pub(crate) fn valid_default_branch(reference: &str) -> bool {
    reference.starts_with("refs/heads/") && valid_ref_name(reference)
}

pub(crate) fn decode_head(row: &[SqlValue]) -> cellule_runtime::Result<DefaultBranch> {
    match row {
        [SqlValue::Integer(generation), SqlValue::Text(reference), ..]
            if *generation >= 0 && valid_default_branch(reference) =>
        {
            Ok(DefaultBranch {
                reference: reference.clone(),
                generation: *generation,
            })
        }
        _ => Err(Error::Command("invalid repository HEAD")),
    }
}
