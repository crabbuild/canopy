use super::*;

#[derive(Debug, PartialEq, Eq)]
pub enum DisableAccountOutcome {
    Disabled,
    NotFound,
    Forbidden,
    SiteOwner,
}

impl DirectoryCell {
    /// Disables an account while reserving its name and preserving attributed data.
    ///
    /// Only the active site-owner admin credential may disable accounts. The
    /// site owner is protected; repeating a successful disable is idempotent.
    pub async fn disable_account(
        &self,
        identity: MutationIdentity,
        authority: TokenAuthority<'_>,
    ) -> Result<Committed<DisableAccountOutcome>, InvocationError<Vec<SqlResultSet>>> {
        validate_component(authority.site_owner).map_err(InvocationError::NotStarted)?;
        validate_component(authority.account).map_err(InvocationError::NotStarted)?;
        let parameters = vec![
            SqlValue::Blob(authority.actor_digest.to_vec()),
            SqlValue::Text(authority.site_owner.into()),
            SqlValue::Text(authority.account.into()),
        ];
        // Authenticate within the mutation: an HTTP precheck may precede token
        // revocation. Keeping account rows also prevents old grants/name reuse
        // from transferring a disabled identity's access to another person.
        let decision = "CASE WHEN NOT EXISTS (SELECT 1 FROM access_tokens t JOIN accounts a ON a.name = t.account WHERE t.digest = ?1 AND t.account = ?2 AND t.scope = 'admin' AND t.enabled = 1 AND a.enabled = 1) THEN 'forbidden' WHEN ?3 = ?2 THEN 'site_owner' WHEN NOT EXISTS (SELECT 1 FROM accounts WHERE name = ?3) THEN 'missing' ELSE 'disabled' END";
        let result = self.sql.batch(identity, SqlBatch { statements: vec![
            SqlStatement { sql: format!("SELECT {decision}"), parameters: parameters.clone() },
            SqlStatement {
                sql: format!("UPDATE accounts SET enabled = 0 WHERE name = ?3 AND enabled = 1 AND ({decision}) = 'disabled'"),
                parameters,
            },
        ] }).await?;
        let output = match result
            .output
            .first()
            .and_then(|set| set.rows.first())
            .map(Vec::as_slice)
        {
            Some([SqlValue::Text(value)]) => match value.as_str() {
                "disabled" => DisableAccountOutcome::Disabled,
                "missing" => DisableAccountOutcome::NotFound,
                "forbidden" => DisableAccountOutcome::Forbidden,
                "site_owner" => DisableAccountOutcome::SiteOwner,
                _ => {
                    return Err(InvocationError::NotStarted(Error::Command(
                        "invalid account disable outcome",
                    )));
                }
            },
            _ => {
                return Err(InvocationError::NotStarted(Error::Command(
                    "missing account disable outcome",
                )));
            }
        };
        Ok(Committed {
            output,
            receipt: result.receipt,
        })
    }
}
