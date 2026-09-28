//! Durable symbolic HEAD, versioned with the repository's ref snapshot.

use cellule_runtime::{
    Committed, Error, InvocationError, MutationIdentity, Observed, Receipt,
    primitives::sql::SqlBatch, primitives::sql::SqlResultSet, primitives::sql::SqlStatement,
    primitives::sql::SqlValue,
};

use crate::{RepositoryCell, directory::validate_component, refs::valid_ref_name};

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

    /// Changes HEAD only for the owner at the expected ref generation.
    ///
    /// The target must be a live branch, or there must be no live branches.
    /// False means authorization, generation or target existence failed.
    pub async fn set_default_branch(
        &self,
        identity: MutationIdentity,
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
        // HEAD and the pagination fence change in the same owner-authorized
        // statement. A concurrent push or HEAD ABA makes a stale update fail.
        let result = self.sql.batch(identity, SqlBatch {
            statements: vec![SqlStatement {
                sql: "UPDATE ref_generation SET default_branch = ?1, generation = generation + 1 WHERE singleton = 1 AND generation = ?2 AND EXISTS (SELECT 1 FROM repository_identity WHERE owner = ?3) AND (EXISTS (SELECT 1 FROM refs WHERE name = ?1 AND oid IS NOT NULL) OR NOT EXISTS (SELECT 1 FROM refs WHERE name GLOB 'refs/heads/*' AND oid IS NOT NULL))".into(),
                parameters: vec![SqlValue::Text(reference.into()), SqlValue::Integer(expected_generation), SqlValue::Text(actor.into())],
            }],
        }).await?;
        Ok(Committed {
            output: result
                .output
                .first()
                .is_some_and(|set| set.rows_affected == 1),
            receipt: result.receipt,
        })
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
