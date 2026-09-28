use super::*;
use ssh_key::{HashAlg, PublicKey, public::KeyData};

pub const SSH_KEY_PAGE_SIZE: usize = 32;
const MAX_ACTIVE_KEYS: usize = 64;
const MAX_DAILY_KEYS: usize = 256;
const ISSUANCE_WINDOW_MS: i64 = 24 * 60 * 60 * 1000;

#[derive(Debug, thiserror::Error)]
pub enum SshKeyError {
    #[error("SSH public key must be one line of at most 8192 bytes")]
    Size,
    #[error("SSH public key encoding is invalid")]
    Encoding(#[from] ssh_key::Error),
    #[error("SSH key must be Ed25519, ECDSA, or RSA with 2048–8192 bits")]
    Algorithm,
}

/// A parsed SSH public key with comment-independent identity.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SshKey {
    fingerprint: [u8; 32],
    public_key: String,
}

impl SshKey {
    /// Parses a plain OpenSSH public key, excluding options and certificates.
    pub fn parse(value: &str) -> Result<Self, SshKeyError> {
        if value.len() > 8192 {
            return Err(SshKeyError::Size);
        }
        let value = value.trim_end_matches(['\r', '\n']);
        if value.chars().any(char::is_control) {
            return Err(SshKeyError::Size);
        }
        let key = PublicKey::from_openssh(value)?;
        match key.key_data() {
            KeyData::Ed25519(_) | KeyData::Ecdsa(_) => {}
            KeyData::Rsa(key) => {
                // Compute the actual bit length: ssh-key's key_size rounds up
                // to a whole byte and would admit a modulus below 2048 bits.
                let bytes = key.n().as_positive_bytes().ok_or(SshKeyError::Algorithm)?;
                let first = bytes.first().ok_or(SshKeyError::Algorithm)?;
                let bits = bytes.len() * 8 - first.leading_zeros() as usize;
                if !(2048..=8192).contains(&bits) {
                    return Err(SshKeyError::Algorithm);
                }
            }
            _ => return Err(SshKeyError::Algorithm),
        }
        let fingerprint = key
            .fingerprint(HashAlg::Sha256)
            .sha256()
            .ok_or(SshKeyError::Algorithm)?;
        let public_key = PublicKey::from(key.key_data().clone()).to_openssh()?;
        Ok(Self {
            fingerprint,
            public_key,
        })
    }

    #[must_use]
    pub fn public_key(&self) -> &str {
        &self.public_key
    }

    #[must_use]
    pub fn fingerprint(&self) -> String {
        ssh_key::Fingerprint::Sha256(self.fingerprint).to_string()
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct SshKeyInfo {
    pub id: [u8; 16],
    pub key: SshKey,
    pub scope: TokenScope,
    pub enabled: bool,
    pub created_at_ms: i64,
}

#[derive(Debug, PartialEq, Eq)]
pub struct SshIdentity {
    pub key_id: [u8; 16],
    pub account: String,
    pub scope: TokenScope,
}

#[derive(Debug, PartialEq, Eq)]
pub enum SshKeyChange {
    Applied,
    NotFound,
    Conflict,
    ActiveLimit,
    IssuanceLimit,
}

impl DirectoryCell {
    pub(crate) async fn push_signers(
        &self,
        account: &str,
    ) -> Result<Vec<SshKey>, InvocationError<Vec<SqlResultSet>>> {
        let result = self.sql.query(None, SqlBatch { statements: vec![SqlStatement {
            sql: "SELECT k.public_key FROM ssh_keys k JOIN accounts a ON a.name = k.account WHERE k.account = ?1 AND k.scope = 'write' AND k.enabled = 1 AND a.enabled = 1 ORDER BY k.id".into(),
            parameters: vec![SqlValue::Text(account.into())],
        }]}).await?;
        result
            .output
            .first()
            .ok_or_else(|| {
                InvocationError::NotStarted(Error::Command("missing push signer result"))
            })?
            .rows
            .iter()
            .map(|row| {
                let [SqlValue::Text(key)] = row.as_slice() else {
                    return Err(InvocationError::NotStarted(Error::Command(
                        "invalid push signer",
                    )));
                };
                SshKey::parse(key).map_err(|_| {
                    InvocationError::NotStarted(Error::Command("invalid stored push signer"))
                })
            })
            .collect()
    }

    /// Resolves an enabled key and account; the SSH transport must verify possession.
    pub async fn ssh_identity(
        &self,
        key: &SshKey,
        minimum: Option<Receipt>,
    ) -> Result<Observed<Option<SshIdentity>>, InvocationError<Vec<SqlResultSet>>> {
        let result = self.sql.query(minimum, SqlBatch { statements: vec![SqlStatement {
            sql: "SELECT k.id, k.account, k.scope FROM ssh_keys k JOIN accounts a ON a.name = k.account WHERE k.fingerprint = ?1 AND k.enabled = 1 AND a.enabled = 1".into(),
            parameters: vec![SqlValue::Blob(key.fingerprint.to_vec())],
        }] }).await?;
        let output = result
            .output
            .first()
            .and_then(|set| set.rows.first())
            .map(|row| {
                let [
                    SqlValue::Blob(id),
                    SqlValue::Text(account),
                    SqlValue::Text(scope),
                ] = row.as_slice()
                else {
                    return Err(Error::Command("invalid SSH identity"));
                };
                Ok(SshIdentity {
                    key_id: id
                        .as_slice()
                        .try_into()
                        .map_err(|_| Error::Command("invalid SSH key ID"))?,
                    account: account.clone(),
                    scope: key_scope(scope)?,
                })
            })
            .transpose()
            .map_err(InvocationError::NotStarted)?;
        Ok(Observed {
            output,
            receipt: result.receipt,
        })
    }

    /// Lists active and revoked key metadata for the account's or site's token admin.
    pub async fn ssh_keys(
        &self,
        authority: TokenAuthority<'_>,
        after: Option<[u8; 16]>,
    ) -> Result<Observed<Option<Vec<SshKeyInfo>>>, InvocationError<Vec<SqlResultSet>>> {
        let parameters = authority
            .parameters()
            .map_err(InvocationError::NotStarted)?;
        let mut page = parameters.clone();
        page.push(SqlValue::Blob(
            after.map_or_else(Vec::new, |id| id.to_vec()),
        ));
        let authorized = tokens::AUTHORIZED;
        let result = self.credential_query(None, SqlBatch { statements: vec![
            SqlStatement { sql: format!("SELECT {authorized}"), parameters },
            SqlStatement {
                sql: format!("SELECT id, fingerprint, public_key, scope, enabled, created_ms FROM ssh_keys WHERE account = ?4 AND id > ?5 AND ({authorized}) ORDER BY id LIMIT {SSH_KEY_PAGE_SIZE}"),
                parameters: page,
            },
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
                InvocationError::NotStarted(Error::Command("missing SSH key page"))
            })?;
            Some(
                page.rows
                    .iter()
                    .map(|row| key_info(row))
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

    /// Registers one key with immutable ownership; revoked key material cannot be reactivated.
    pub async fn register_ssh_key(
        &self,
        identity: MutationIdentity,
        authority: TokenAuthority<'_>,
        id: [u8; 16],
        key: &SshKey,
        scope: TokenScope,
    ) -> Result<Committed<SshKeyChange>, InvocationError<Vec<SqlResultSet>>> {
        if scope == TokenScope::Admin {
            return Err(InvocationError::NotStarted(Error::Command(
                "SSH keys cannot administer accounts",
            )));
        }
        let mut parameters = authority
            .parameters()
            .map_err(InvocationError::NotStarted)?;
        parameters.extend([
            SqlValue::Blob(id.to_vec()),
            SqlValue::Blob(key.fingerprint.to_vec()),
            SqlValue::Text(key.public_key.clone()),
            SqlValue::Text(scope.as_str().into()),
        ]);
        let active = format!(
            "(SELECT count(*) FROM (SELECT 1 FROM ssh_keys WHERE account = ?4 AND enabled = 1 LIMIT {MAX_ACTIVE_KEYS}))"
        );
        let recent = format!(
            "(SELECT count(*) FROM (SELECT 1 FROM ssh_keys WHERE account = ?4 AND created_ms > ?1 - {ISSUANCE_WINDOW_MS} LIMIT {MAX_DAILY_KEYS}))"
        );
        let authorized = tokens::AUTHORIZED;
        let decision = format!(
            "CASE WHEN NOT ({authorized}) THEN 'missing' WHEN EXISTS (SELECT 1 FROM ssh_keys WHERE id = ?5 AND fingerprint = ?6 AND account = ?4 AND scope = ?8 AND enabled = 1) THEN 'applied' WHEN EXISTS (SELECT 1 FROM ssh_keys WHERE id = ?5 OR fingerprint = ?6) THEN 'conflict' WHEN {active} >= {MAX_ACTIVE_KEYS} THEN 'active_limit' WHEN {recent} >= {MAX_DAILY_KEYS} THEN 'issuance_limit' ELSE 'applied' END"
        );
        let result = self.credential_command(identity, SqlBatch { statements: vec![
            SqlStatement { sql: format!("SELECT {decision}"), parameters: parameters.clone() },
            SqlStatement { sql: format!("INSERT INTO ssh_keys (id, fingerprint, account, public_key, scope, enabled, created_ms) SELECT ?5, ?6, ?4, ?7, ?8, 1, ?1 WHERE ({decision}) = 'applied' ON CONFLICT DO NOTHING"), parameters },
            record_change(&authority, "ssh_key.registered", id),
        ] }).await?;
        changed(result)
    }

    /// Revokes a key while preserving its ownership and audit identity.
    pub async fn revoke_ssh_key(
        &self,
        identity: MutationIdentity,
        authority: TokenAuthority<'_>,
        id: [u8; 16],
    ) -> Result<Committed<SshKeyChange>, InvocationError<Vec<SqlResultSet>>> {
        let mut parameters = authority
            .parameters()
            .map_err(InvocationError::NotStarted)?;
        parameters.push(SqlValue::Blob(id.to_vec()));
        let authorized = tokens::AUTHORIZED;
        let decision = format!(
            "CASE WHEN NOT ({authorized}) OR NOT EXISTS (SELECT 1 FROM ssh_keys WHERE id = ?5 AND account = ?4) THEN 'missing' ELSE 'applied' END"
        );
        let result = self.credential_command(identity, SqlBatch { statements: vec![
            SqlStatement { sql: format!("SELECT {decision}"), parameters: parameters.clone() },
            SqlStatement { sql: format!("UPDATE ssh_keys SET enabled = 0 WHERE id = ?5 AND account = ?4 AND enabled = 1 AND ({decision}) = 'applied'"), parameters },
            record_change(&authority, "ssh_key.revoked", id),
        ] }).await?;
        changed(result)
    }
}

fn record_change(authority: &TokenAuthority<'_>, action: &str, id: [u8; 16]) -> SqlStatement {
    // Share the mutation transaction: an audit failure must roll back the key.
    // changes() suppresses duplicate events for exact retries and repeated revokes.
    SqlStatement {
        sql: "INSERT INTO account_events (occurred_ms, actor, actor_token_id, action, account, ssh_key_id, scope) SELECT ?1, actor.account, actor.id, ?3, ?4, target.id, target.scope FROM access_tokens actor JOIN ssh_keys target ON target.id = ?5 WHERE actor.digest = ?2 AND changes() = 1".into(),
        parameters: vec![SqlValue::Blob(authority.actor_digest.to_vec()), SqlValue::Text(action.into()), SqlValue::Text(authority.account.into()), SqlValue::Blob(id.to_vec())],
    }
}

fn changed(
    result: Committed<Vec<SqlResultSet>>,
) -> Result<Committed<SshKeyChange>, InvocationError<Vec<SqlResultSet>>> {
    let output = match result
        .output
        .first()
        .and_then(|set| set.rows.first())
        .map(Vec::as_slice)
    {
        Some([SqlValue::Text(value)]) => match value.as_str() {
            "applied" => SshKeyChange::Applied,
            "missing" => SshKeyChange::NotFound,
            "conflict" => SshKeyChange::Conflict,
            "active_limit" => SshKeyChange::ActiveLimit,
            "issuance_limit" => SshKeyChange::IssuanceLimit,
            _ => {
                return Err(InvocationError::NotStarted(Error::Command(
                    "invalid SSH key outcome",
                )));
            }
        },
        _ => {
            return Err(InvocationError::NotStarted(Error::Command(
                "missing SSH key outcome",
            )));
        }
    };
    Ok(Committed {
        output,
        receipt: result.receipt,
    })
}

fn key_scope(scope: &str) -> crab_cell_runtime::Result<TokenScope> {
    match TokenScope::parse(scope) {
        Some(scope @ (TokenScope::Read | TokenScope::Write)) => Ok(scope),
        _ => Err(Error::Command("invalid SSH key scope")),
    }
}

fn key_info(row: &[SqlValue]) -> crab_cell_runtime::Result<SshKeyInfo> {
    let [
        SqlValue::Blob(id),
        SqlValue::Blob(fingerprint),
        SqlValue::Text(public_key),
        SqlValue::Text(scope),
        SqlValue::Integer(enabled),
        SqlValue::Integer(created_at_ms),
    ] = row
    else {
        return Err(Error::Command("invalid SSH key record"));
    };
    Ok(SshKeyInfo {
        id: id
            .as_slice()
            .try_into()
            .map_err(|_| Error::Command("invalid SSH key ID"))?,
        key: SshKey {
            fingerprint: fingerprint
                .as_slice()
                .try_into()
                .map_err(|_| Error::Command("invalid SSH key fingerprint"))?,
            public_key: public_key.clone(),
        },
        scope: key_scope(scope)?,
        enabled: *enabled == 1,
        created_at_ms: *created_at_ms,
    })
}
