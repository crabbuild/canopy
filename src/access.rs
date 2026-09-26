//! Repository-local ownership and collaborator roles.

use cellule_runtime::{
    Committed, Error, InvocationError, MutationIdentity, Observed, Receipt, SqlBatch, SqlResultSet,
    SqlStatement, SqlValue,
};

use crate::{
    RepositoryCell,
    directory::{TokenScope, validate_component},
};

const ACCESS_QUERY: &str = "SELECT CASE WHEN EXISTS (SELECT 1 FROM repository_identity WHERE owner = ?1) THEN 'admin' ELSE (SELECT role FROM repository_members WHERE account = ?1) END";

pub const COLLABORATOR_PAGE_SIZE: usize = 32;

/// One explicit repository grant; the immutable owner is not a collaborator entry.
#[derive(Debug, PartialEq, Eq)]
pub struct Collaborator {
    pub account: String,
    pub role: TokenScope,
}

impl RepositoryCell {
    /// Reads one owner-authorized page of current collaborator grants.
    ///
    /// Returns `None` for a non-owner. Pages are independent observations in
    /// account-name order; continue after the last account of a full page.
    pub async fn collaborators(
        &self,
        actor: &str,
        after: Option<&str>,
    ) -> Result<Observed<Option<Vec<Collaborator>>>, InvocationError<Vec<SqlResultSet>>> {
        validate_component(actor).map_err(InvocationError::NotStarted)?;
        if let Some(after) = after {
            validate_component(after).map_err(InvocationError::NotStarted)?;
        }
        let observed = self.sql.query(None, SqlBatch { statements: vec![
            SqlStatement {
                sql: "SELECT 1 FROM repository_identity WHERE owner = ?1".into(),
                parameters: vec![SqlValue::Text(actor.into())],
            },
            SqlStatement {
                sql: format!("SELECT account, role FROM repository_members WHERE account > ?2 AND EXISTS (SELECT 1 FROM repository_identity WHERE owner = ?1) ORDER BY account LIMIT {COLLABORATOR_PAGE_SIZE}"),
                parameters: vec![SqlValue::Text(actor.into()), SqlValue::Text(after.unwrap_or("").into())],
            },
        ] }).await?;
        let authorized = observed.output.first().ok_or_else(|| {
            InvocationError::NotStarted(Error::Command("missing roster authorization"))
        })?;
        let output = if authorized.rows.is_empty() {
            None
        } else {
            let rows = observed.output.get(1).ok_or_else(|| {
                InvocationError::NotStarted(Error::Command("missing collaborator page"))
            })?;
            let members = rows
                .rows
                .iter()
                .map(|row| {
                    let [SqlValue::Text(account), SqlValue::Text(role)] = row.as_slice() else {
                        return Err(Error::Command("invalid collaborator row"));
                    };
                    validate_component(account)?;
                    let role = TokenScope::parse(role)
                        .filter(|role| *role != TokenScope::Admin)
                        .ok_or(Error::Command("invalid collaborator role"))?;
                    Ok(Collaborator {
                        account: account.clone(),
                        role,
                    })
                })
                .collect::<cellule_runtime::Result<Vec<_>>>()
                .map_err(InvocationError::NotStarted)?;
            Some(members)
        };
        Ok(Observed {
            output,
            receipt: observed.receipt,
        })
    }

    /// Pins the owner in the Repository Cell before its directory name becomes ready.
    pub async fn ensure_owner(
        &self,
        identity: MutationIdentity,
        owner: &str,
    ) -> Result<Committed<()>, InvocationError<Vec<SqlResultSet>>> {
        validate_component(owner).map_err(InvocationError::NotStarted)?;
        let committed = self.sql.batch(identity, SqlBatch {
            statements: vec![SqlStatement {
                sql: "INSERT INTO repository_identity (singleton, owner) VALUES (1, ?1) ON CONFLICT(singleton) DO NOTHING".into(),
                parameters: vec![SqlValue::Text(owner.into())],
            }],
        }).await?;
        let observed = self
            .sql
            .query(
                Some(committed.receipt),
                SqlBatch {
                    statements: vec![SqlStatement {
                        sql: "SELECT owner FROM repository_identity WHERE singleton = 1".into(),
                        parameters: Vec::new(),
                    }],
                },
            )
            .await?;
        if observed
            .output
            .first()
            .and_then(|set| set.rows.first())
            .map(Vec::as_slice)
            != Some([SqlValue::Text(owner.into())].as_slice())
        {
            return Err(InvocationError::InvalidPublishedResult {
                receipt: committed.receipt,
                source: Box::new(Error::Command("repository owner differs from directory")),
            });
        }
        Ok(Committed {
            output: (),
            receipt: committed.receipt,
        })
    }

    /// Reads the role granted to one account by this Repository Cell.
    pub async fn access_level(
        &self,
        account: &str,
        minimum: Option<Receipt>,
    ) -> Result<Observed<Option<TokenScope>>, InvocationError<Vec<SqlResultSet>>> {
        validate_component(account).map_err(InvocationError::NotStarted)?;
        let observed = self
            .sql
            .query(
                minimum,
                SqlBatch {
                    statements: vec![access_statement(account)],
                },
            )
            .await?;
        let level = decode_access(&observed.output).map_err(InvocationError::NotStarted)?;
        Ok(Observed {
            output: level,
            receipt: observed.receipt,
        })
    }

    /// Grants or replaces a collaborator role only when the actor is the owner.
    pub async fn grant_member(
        &self,
        identity: MutationIdentity,
        actor: &str,
        account: &str,
        role: TokenScope,
    ) -> Result<Committed<bool>, InvocationError<Vec<SqlResultSet>>> {
        validate_component(actor).map_err(InvocationError::NotStarted)?;
        validate_component(account).map_err(InvocationError::NotStarted)?;
        if role == TokenScope::Admin {
            return Err(InvocationError::NotStarted(Error::Command(
                "only the repository owner has admin access",
            )));
        }
        let committed = self.sql.batch(identity, SqlBatch {
            statements: vec![SqlStatement {
                sql: "INSERT INTO repository_members (account, role) SELECT ?2, ?3 WHERE EXISTS (SELECT 1 FROM repository_identity WHERE owner = ?1) AND NOT EXISTS (SELECT 1 FROM repository_identity WHERE owner = ?2) ON CONFLICT(account) DO UPDATE SET role = excluded.role".into(),
                parameters: vec![
                    SqlValue::Text(actor.into()),
                    SqlValue::Text(account.into()),
                    SqlValue::Text(role.as_str().into()),
                ],
            }],
        }).await?;
        let changed = committed
            .output
            .first()
            .is_some_and(|set| set.rows_affected == 1);
        if changed
            && self
                .access_level(account, Some(committed.receipt))
                .await?
                .output
                != Some(role)
        {
            return Err(InvocationError::InvalidPublishedResult {
                receipt: committed.receipt,
                source: Box::new(Error::Command("collaborator role did not publish")),
            });
        }
        Ok(Committed {
            output: changed,
            receipt: committed.receipt,
        })
    }

    /// Revokes a collaborator role only when the actor is the owner.
    pub async fn revoke_member(
        &self,
        identity: MutationIdentity,
        actor: &str,
        account: &str,
    ) -> Result<Committed<bool>, InvocationError<Vec<SqlResultSet>>> {
        validate_component(actor).map_err(InvocationError::NotStarted)?;
        validate_component(account).map_err(InvocationError::NotStarted)?;
        let committed = self.sql.batch(identity, SqlBatch {
            statements: vec![
                SqlStatement {
                    sql: "DELETE FROM repository_members WHERE account = ?2 AND EXISTS (SELECT 1 FROM repository_identity WHERE owner = ?1)".into(),
                    parameters: vec![SqlValue::Text(actor.into()), SqlValue::Text(account.into())],
                },
                SqlStatement {
                    sql: "SELECT 1 FROM repository_identity WHERE owner = ?1".into(),
                    parameters: vec![SqlValue::Text(actor.into())],
                },
            ],
        }).await?;
        let authorized = committed
            .output
            .get(1)
            .is_some_and(|set| !set.rows.is_empty());
        Ok(Committed {
            output: authorized,
            receipt: committed.receipt,
        })
    }
}

pub(crate) fn access_statement(account: &str) -> SqlStatement {
    SqlStatement {
        sql: ACCESS_QUERY.into(),
        parameters: vec![SqlValue::Text(account.into())],
    }
}

pub(crate) fn decode_access(sets: &[SqlResultSet]) -> cellule_runtime::Result<Option<TokenScope>> {
    let Some(row) = sets.first().and_then(|set| set.rows.first()) else {
        return Err(Error::Command("repository access query returned no result"));
    };
    match row.as_slice() {
        [SqlValue::Null] => Ok(None),
        [SqlValue::Text(role)] if role == "read" => Ok(Some(TokenScope::Read)),
        [SqlValue::Text(role)] if role == "write" => Ok(Some(TokenScope::Write)),
        [SqlValue::Text(role)] if role == "admin" => Ok(Some(TokenScope::Admin)),
        _ => Err(Error::Command("invalid repository access row")),
    }
}
