use super::*;

pub const ACCOUNT_PAGE_SIZE: usize = 32;

#[derive(Debug, PartialEq, Eq)]
pub struct AccountInfo {
    pub name: String,
    pub enabled: bool,
}

#[derive(Debug, PartialEq, Eq)]
pub enum DisableAccountOutcome {
    Disabled,
    NotFound,
    Forbidden,
    SiteOwner,
}

impl DirectoryCell {
    /// Lists accounts, including disabled identities, for an active site administrator.
    pub async fn accounts(
        &self,
        actor_digest: [u8; 32],
        site_owner: &str,
        after: Option<&str>,
    ) -> Result<Observed<Option<Vec<AccountInfo>>>, InvocationError<Vec<SqlResultSet>>> {
        validate_component(site_owner).map_err(InvocationError::NotStarted)?;
        if let Some(after) = after {
            validate_component(after).map_err(InvocationError::NotStarted)?;
        }
        let authorized = "EXISTS (SELECT 1 FROM access_tokens t JOIN accounts a ON a.name = t.account WHERE t.digest = ?2 AND t.account = ?3 AND t.scope = 'admin' AND t.enabled = 1 AND (t.expires_ms IS NULL OR t.expires_ms > ?1) AND a.enabled = 1)";
        let parameters = vec![
            SqlValue::Blob(actor_digest.to_vec()),
            SqlValue::Text(site_owner.into()),
        ];
        let mut page = parameters.clone();
        page.push(SqlValue::Text(after.unwrap_or_default().into()));
        // Owner-time authorization and the page share one snapshot. A credential
        // revoked while this read waits cannot disclose the account directory.
        let result = self.credential_query(None, SqlBatch { statements: vec![
            SqlStatement { sql: format!("SELECT {authorized}"), parameters },
            SqlStatement { sql: format!("SELECT name, enabled FROM accounts WHERE name > ?4 AND ({authorized}) ORDER BY name LIMIT {ACCOUNT_PAGE_SIZE}"), parameters: page },
        ] }).await?;
        let allowed = matches!(
            result
                .output
                .first()
                .and_then(|set| set.rows.first())
                .map(Vec::as_slice),
            Some([SqlValue::Integer(1)])
        );
        let output = if allowed {
            let page = result.output.get(1).ok_or_else(|| {
                InvocationError::NotStarted(Error::Command("missing account page"))
            })?;
            Some(
                page.rows
                    .iter()
                    .map(|row| {
                        let [SqlValue::Text(name), SqlValue::Integer(enabled)] = row.as_slice()
                        else {
                            return Err(Error::Command("invalid account row"));
                        };
                        validate_component(name)?;
                        Ok(AccountInfo {
                            name: name.clone(),
                            enabled: *enabled == 1,
                        })
                    })
                    .collect::<crab_cell_runtime::Result<Vec<_>>>()
                    .map_err(InvocationError::NotStarted)?,
            )
        } else {
            None
        };
        Ok(Observed {
            output,
            receipt: result.receipt,
        })
    }

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
        let decision = "CASE WHEN NOT EXISTS (SELECT 1 FROM access_tokens t JOIN accounts a ON a.name = t.account WHERE t.digest = ?2 AND t.account = ?3 AND t.scope = 'admin' AND t.enabled = 1 AND (t.expires_ms IS NULL OR t.expires_ms > ?1) AND a.enabled = 1) THEN 'forbidden' WHEN ?4 = ?3 THEN 'site_owner' WHEN NOT EXISTS (SELECT 1 FROM accounts WHERE name = ?4) THEN 'missing' ELSE 'disabled' END";
        let result = self.credential_command(identity, SqlBatch { statements: vec![
            SqlStatement { sql: format!("SELECT {decision}"), parameters: parameters.clone() },
            SqlStatement {
                sql: format!("UPDATE accounts SET enabled = 0 WHERE name = ?4 AND enabled = 1 AND ({decision}) = 'disabled'"),
                parameters,
            },
            audit::record_change(Some(authority.actor_digest), "account.disabled", authority.account, None),
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
