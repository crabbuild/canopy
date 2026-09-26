use super::*;

pub const TOKEN_PAGE_SIZE: usize = 32;

/// Credential and trusted site policy used for one atomic token-management operation.
pub struct TokenAuthority<'a> {
    pub actor_digest: [u8; 32],
    pub site_owner: &'a str,
    pub account: &'a str,
}

#[derive(Debug, PartialEq, Eq)]
pub struct TokenInfo {
    pub id: [u8; 16],
    pub scope: TokenScope,
    pub enabled: bool,
    pub created_at_ms: i64,
}

#[derive(Debug, PartialEq, Eq)]
pub enum TokenChange {
    Applied,
    NotFound,
    Conflict,
    LastAdmin,
}

// The caller supplies site policy, not an authenticated account assertion.
// Recheck the exact credential in the same SQL transaction as each mutation.
const AUTHORIZED: &str = "EXISTS (SELECT 1 FROM access_tokens actor JOIN accounts a ON a.name = actor.account WHERE actor.digest = ?1 AND actor.enabled = 1 AND actor.scope = 'admin' AND a.enabled = 1 AND (actor.account = ?2 OR actor.account = ?3)) AND EXISTS (SELECT 1 FROM accounts WHERE name = ?3 AND enabled = 1)";

impl TokenAuthority<'_> {
    fn parameters(&self) -> cellule_runtime::Result<Vec<SqlValue>> {
        validate_component(self.site_owner)?;
        validate_component(self.account)?;
        Ok(vec![
            SqlValue::Blob(self.actor_digest.to_vec()),
            SqlValue::Text(self.site_owner.into()),
            SqlValue::Text(self.account.into()),
        ])
    }
}

impl DirectoryCell {
    /// Lists active and revoked token metadata without credential digests or secrets.
    ///
    /// Returns `None` for an inaccessible or missing account. Continue after the
    /// last ID of a full page; revoked IDs remain reserved and visible to admins.
    pub async fn tokens(
        &self,
        authority: TokenAuthority<'_>,
        after: Option<[u8; 16]>,
    ) -> Result<Observed<Option<Vec<TokenInfo>>>, InvocationError<Vec<SqlResultSet>>> {
        let parameters = authority
            .parameters()
            .map_err(InvocationError::NotStarted)?;
        let mut page_parameters = parameters.clone();
        page_parameters.push(SqlValue::Blob(
            after.map_or_else(Vec::new, |id| id.to_vec()),
        ));
        let observed = self.sql.query(None, SqlBatch { statements: vec![
            SqlStatement { sql: format!("SELECT {AUTHORIZED}"), parameters },
            SqlStatement {
                sql: format!("SELECT id, scope, enabled, created_ms FROM access_tokens WHERE account = ?3 AND id > ?4 AND ({AUTHORIZED}) ORDER BY id LIMIT {TOKEN_PAGE_SIZE}"),
                parameters: page_parameters,
            },
        ] }).await?;
        let allowed = matches!(
            observed
                .output
                .first()
                .and_then(|set| set.rows.first())
                .map(Vec::as_slice),
            Some([SqlValue::Integer(1)])
        );
        let output = if allowed {
            let rows = observed
                .output
                .get(1)
                .ok_or_else(|| InvocationError::NotStarted(Error::Command("missing token page")))?;
            Some(
                rows.rows
                    .iter()
                    .map(|row| token_info(row))
                    .collect::<cellule_runtime::Result<Vec<_>>>()
                    .map_err(InvocationError::NotStarted)?,
            )
        } else {
            None
        };
        Ok(Observed {
            output,
            receipt: observed.receipt,
        })
    }

    /// Issues a credential once; exact active-record retries converge without reactivation.
    pub async fn issue_token(
        &self,
        identity: MutationIdentity,
        authority: TokenAuthority<'_>,
        id: [u8; 16],
        digest: [u8; 32],
        scope: TokenScope,
    ) -> Result<Committed<TokenChange>, InvocationError<Vec<SqlResultSet>>> {
        let mut parameters = authority
            .parameters()
            .map_err(InvocationError::NotStarted)?;
        parameters.extend([
            SqlValue::Blob(id.to_vec()),
            SqlValue::Blob(digest.to_vec()),
            SqlValue::Text(scope.as_str().into()),
        ]);
        let decision = format!(
            "CASE WHEN NOT ({AUTHORIZED}) THEN 'missing' WHEN EXISTS (SELECT 1 FROM access_tokens WHERE id = ?4 AND digest = ?5 AND account = ?3 AND scope = ?6 AND enabled = 1) THEN 'applied' WHEN EXISTS (SELECT 1 FROM access_tokens WHERE id = ?4 OR digest = ?5) THEN 'conflict' ELSE 'applied' END"
        );
        let mut insert_parameters = parameters.clone();
        insert_parameters.push(SqlValue::Integer(identity.issued_at_ms));
        let result = self.sql.batch(identity, SqlBatch { statements: vec![
            SqlStatement { sql: format!("SELECT {decision}"), parameters },
            SqlStatement {
                sql: format!("INSERT INTO access_tokens (id, digest, account, scope, enabled, created_ms) SELECT ?4, ?5, ?3, ?6, 1, ?7 WHERE ({decision}) = 'applied' ON CONFLICT DO NOTHING"),
                parameters: insert_parameters,
            },
        ] }).await?;
        changed(result)
    }

    /// Revokes one credential while retaining its identity and the site's last admin.
    pub async fn revoke_token(
        &self,
        identity: MutationIdentity,
        authority: TokenAuthority<'_>,
        id: [u8; 16],
    ) -> Result<Committed<TokenChange>, InvocationError<Vec<SqlResultSet>>> {
        let mut parameters = authority
            .parameters()
            .map_err(InvocationError::NotStarted)?;
        parameters.push(SqlValue::Blob(id.to_vec()));
        let decision = format!(
            "CASE WHEN NOT ({AUTHORIZED}) OR NOT EXISTS (SELECT 1 FROM access_tokens WHERE id = ?4 AND account = ?3) THEN 'missing' WHEN EXISTS (SELECT 1 FROM access_tokens WHERE id = ?4 AND enabled = 0) THEN 'applied' WHEN ?3 = ?2 AND EXISTS (SELECT 1 FROM access_tokens WHERE id = ?4 AND scope = 'admin') AND (SELECT count(*) FROM access_tokens WHERE account = ?3 AND scope = 'admin' AND enabled = 1) <= 1 THEN 'last_admin' ELSE 'applied' END"
        );
        // Record the decision before changing the actor's own token. Reading
        // authorization afterward would report failure for a successful self-revoke.
        let result = self.sql.batch(identity, SqlBatch { statements: vec![
            SqlStatement { sql: format!("SELECT {decision}"), parameters: parameters.clone() },
            SqlStatement {
                sql: format!("UPDATE access_tokens SET enabled = 0 WHERE id = ?4 AND account = ?3 AND enabled = 1 AND ({decision}) = 'applied'"),
                parameters,
            },
        ] }).await?;
        changed(result)
    }
}

fn changed(
    result: Committed<Vec<SqlResultSet>>,
) -> Result<Committed<TokenChange>, InvocationError<Vec<SqlResultSet>>> {
    let output = match result
        .output
        .first()
        .and_then(|set| set.rows.first())
        .map(Vec::as_slice)
    {
        Some([SqlValue::Text(value)]) => match value.as_str() {
            "applied" => TokenChange::Applied,
            "missing" => TokenChange::NotFound,
            "conflict" => TokenChange::Conflict,
            "last_admin" => TokenChange::LastAdmin,
            _ => {
                return Err(InvocationError::NotStarted(Error::Command(
                    "invalid token outcome",
                )));
            }
        },
        _ => {
            return Err(InvocationError::NotStarted(Error::Command(
                "missing token outcome",
            )));
        }
    };
    Ok(Committed {
        output,
        receipt: result.receipt,
    })
}

fn token_info(row: &[SqlValue]) -> cellule_runtime::Result<TokenInfo> {
    let [
        SqlValue::Blob(id),
        SqlValue::Text(scope),
        SqlValue::Integer(enabled),
        SqlValue::Integer(created_at_ms),
    ] = row
    else {
        return Err(Error::Command("invalid token record"));
    };
    let scope = TokenScope::parse(scope).ok_or(Error::Command("invalid token scope"))?;
    if ![0, 1].contains(enabled) || *created_at_ms < 0 {
        return Err(Error::Command("invalid token metadata"));
    }
    Ok(TokenInfo {
        id: id
            .as_slice()
            .try_into()
            .map_err(|_| Error::Command("invalid token ID"))?,
        scope,
        enabled: *enabled == 1,
        created_at_ms: *created_at_ms,
    })
}
