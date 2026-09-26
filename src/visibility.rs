//! Versioned repository visibility in the same authority as refs and access grants.

use crate::{RepositoryCell, directory::validate_component};
use cellule_runtime::{
    Committed, Error, InvocationError, MutationIdentity, Observed, SqlBatch, SqlResultSet,
    SqlStatement, SqlValue,
};
use serde::{Deserialize, Serialize};

type Invocation = InvocationError<Vec<SqlResultSet>>;

/// Readers admitted by a repository in addition to its explicit grants.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Visibility {
    Private,
    Public,
}
impl Visibility {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Private => "private",
            Self::Public => "public",
        }
    }
}

/// Visibility and the ref generation required to change it.
#[derive(Debug, Serialize)]
pub struct RepositoryVisibility {
    pub visibility: Visibility,
    pub generation: i64,
}
impl RepositoryCell {
    /// Reads visibility and its generation from one authoritative observation.
    pub async fn visibility(&self) -> Result<Observed<RepositoryVisibility>, Invocation> {
        let observed = self
            .sql
            .query(
                None,
                SqlBatch {
                    statements: vec![SqlStatement {
                        sql:
                            "SELECT visibility, generation FROM ref_generation WHERE singleton = 1"
                                .into(),
                        parameters: vec![],
                    }],
                },
            )
            .await?;
        let output = match observed
            .output
            .first()
            .and_then(|set| set.rows.first())
            .map(Vec::as_slice)
        {
            Some([SqlValue::Text(value), SqlValue::Integer(generation)]) => RepositoryVisibility {
                visibility: match value.as_str() {
                    "private" => Visibility::Private,
                    "public" => Visibility::Public,
                    _ => {
                        return Err(Invocation::NotStarted(Error::Command(
                            "invalid repository visibility",
                        )));
                    }
                },
                generation: *generation,
            },
            _ => {
                return Err(Invocation::NotStarted(Error::Command(
                    "missing repository visibility",
                )));
            }
        };
        Ok(Observed {
            output,
            receipt: observed.receipt,
        })
    }

    /// Changes visibility only for the owner at the expected ref generation.
    pub async fn set_visibility(
        &self,
        identity: MutationIdentity,
        actor: &str,
        expected_generation: i64,
        visibility: Visibility,
    ) -> Result<Committed<bool>, Invocation> {
        validate_component(actor).map_err(Invocation::NotStarted)?;
        if !(0..i64::MAX).contains(&expected_generation) {
            return Err(Invocation::NotStarted(Error::Command(
                "invalid visibility generation",
            )));
        }
        let committed = self.sql.batch(identity, SqlBatch { statements: vec![SqlStatement {
            sql: "UPDATE ref_generation SET visibility = ?1, generation = generation + 1 WHERE singleton = 1 AND generation = ?2 AND EXISTS (SELECT 1 FROM repository_identity WHERE owner = ?3)".into(),
            parameters: vec![SqlValue::Text(visibility.as_str().into()), SqlValue::Integer(expected_generation), SqlValue::Text(actor.into())],
        }] }).await?;
        let output = committed
            .output
            .first()
            .is_some_and(|set| set.rows_affected == 1);
        Ok(Committed {
            output,
            receipt: committed.receipt,
        })
    }
}
