use super::*;

/// Maximum account events returned by one history query.
pub const AUDIT_PAGE_SIZE: usize = 32;

/// A committed account or credential change without credential secrets or digests.
#[derive(Debug, PartialEq, Eq)]
pub struct AccountEvent {
    pub id: i64,
    pub occurred_at_ms: i64,
    pub actor: Option<String>,
    pub actor_token_id: Option<[u8; 16]>,
    pub action: String,
    pub account: String,
    pub token_id: Option<[u8; 16]>,
    pub scope: Option<TokenScope>,
    pub expires_at_ms: Option<i64>,
}

// Keep this immediately after the authoritative mutation in the same batch.
// changes() excludes rejected/no-op retries. Retained token rows preserve actor
// identity even when the preceding mutation revoked the actor's own credential.
pub(super) fn record_change(
    actor: Option<[u8; 32]>,
    action: &str,
    account: &str,
    token: Option<[u8; 16]>,
) -> SqlStatement {
    SqlStatement {
        sql: "INSERT INTO account_events (occurred_ms, actor, actor_token_id, action, account, token_id, scope, expires_ms) SELECT ?1, actor.account, actor.id, ?3, ?4, target.id, target.scope, target.expires_ms FROM (SELECT 1) LEFT JOIN access_tokens actor ON actor.digest = ?2 LEFT JOIN access_tokens target ON target.id = ?5 WHERE changes() = 1".into(),
        parameters: vec![
            actor.map_or(SqlValue::Null, |value| SqlValue::Blob(value.to_vec())),
            SqlValue::Text(action.into()),
            SqlValue::Text(account.into()),
            token.map_or(SqlValue::Null, |value| SqlValue::Blob(value.to_vec())),
        ],
    }
}

impl DirectoryCell {
    /// Reads recent account changes for an active site-owner admin credential.
    pub async fn account_events(
        &self,
        actor_digest: [u8; 32],
        site_owner: &str,
        before: Option<i64>,
    ) -> Result<Observed<Option<Vec<AccountEvent>>>, InvocationError<Vec<SqlResultSet>>> {
        validate_component(site_owner).map_err(InvocationError::NotStarted)?;
        if before.is_some_and(|value| value <= 0) {
            return Err(InvocationError::NotStarted(Error::Command(
                "invalid audit cursor",
            )));
        }
        let authorized = "EXISTS (SELECT 1 FROM access_tokens t JOIN accounts a ON a.name = t.account WHERE t.digest = ?2 AND t.account = ?3 AND t.scope = 'admin' AND t.enabled = 1 AND (t.expires_ms IS NULL OR t.expires_ms > ?1) AND a.enabled = 1)";
        let parameters = vec![
            SqlValue::Blob(actor_digest.to_vec()),
            SqlValue::Text(site_owner.into()),
        ];
        let mut page = parameters.clone();
        page.push(before.map_or(SqlValue::Null, SqlValue::Integer));
        let result = self.credential_query(None, SqlBatch { statements: vec![
            SqlStatement { sql: format!("SELECT {authorized}"), parameters },
            SqlStatement { sql: format!("SELECT id, occurred_ms, actor, actor_token_id, action, account, token_id, scope, expires_ms FROM account_events WHERE id <= coalesce(?4 - 1, 9223372036854775807) AND ({authorized}) ORDER BY id DESC LIMIT {AUDIT_PAGE_SIZE}"), parameters: page },
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
            let page = result
                .output
                .get(1)
                .ok_or_else(|| InvocationError::NotStarted(Error::Command("missing audit page")))?;
            Some(
                page.rows
                    .iter()
                    .map(|row| event(row))
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
}

fn event(row: &[SqlValue]) -> crab_cell_runtime::Result<AccountEvent> {
    let [
        SqlValue::Integer(id),
        SqlValue::Integer(occurred_at_ms),
        actor,
        actor_token,
        SqlValue::Text(action),
        SqlValue::Text(account),
        token,
        scope,
        expires,
    ] = row
    else {
        return Err(Error::Command("invalid audit record"));
    };
    let actor = match actor {
        SqlValue::Null => None,
        SqlValue::Text(value) => Some(value.clone()),
        _ => return Err(Error::Command("invalid audit actor")),
    };
    let scope = match scope {
        SqlValue::Null => None,
        SqlValue::Text(value) => {
            Some(TokenScope::parse(value).ok_or(Error::Command("invalid audit scope"))?)
        }
        _ => return Err(Error::Command("invalid audit scope")),
    };
    let expires_at_ms = match expires {
        SqlValue::Null => None,
        SqlValue::Integer(value) => Some(*value),
        _ => return Err(Error::Command("invalid audit expiry")),
    };
    Ok(AccountEvent {
        id: *id,
        occurred_at_ms: *occurred_at_ms,
        actor,
        actor_token_id: token_id(actor_token)?,
        action: action.clone(),
        account: account.clone(),
        token_id: token_id(token)?,
        scope,
        expires_at_ms,
    })
}

fn token_id(value: &SqlValue) -> crab_cell_runtime::Result<Option<[u8; 16]>> {
    match value {
        SqlValue::Null => Ok(None),
        SqlValue::Blob(bytes) => bytes
            .as_slice()
            .try_into()
            .map(Some)
            .map_err(|_| Error::Command("invalid audit token ID")),
        _ => Err(Error::Command("invalid audit token ID")),
    }
}
