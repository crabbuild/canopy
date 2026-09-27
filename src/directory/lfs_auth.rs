use super::*;

pub(crate) const LFS_AUTH_SECONDS: u32 = 300;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum LfsOperation {
    Download,
    Upload,
}

impl LfsOperation {
    pub(crate) fn parse(value: &str) -> Option<Self> {
        match value {
            "download" => Some(Self::Download),
            "upload" => Some(Self::Upload),
            _ => None,
        }
    }

    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Download => "download",
            Self::Upload => "upload",
        }
    }

    pub(crate) fn scope(self) -> TokenScope {
        match self {
            Self::Download => TokenScope::Read,
            Self::Upload => TokenScope::Write,
        }
    }
}

pub(crate) struct LfsGrant {
    pub(crate) principal: Principal,
    pub(crate) operation: LfsOperation,
    pub(crate) repository: [u8; 16],
}

impl DirectoryCell {
    pub(crate) async fn issue_lfs_grant(
        &self,
        identity: MutationIdentity,
        key_id: [u8; 16],
        repository: [u8; 16],
        id: [u8; 16],
        digest: [u8; 32],
        operation: LfsOperation,
    ) -> Result<bool, InvocationError<Vec<SqlResultSet>>> {
        // Recheck the parent credential at owner time. A revoked SSH session
        // cannot mint a grant after its earlier exec authorization succeeds.
        let result = self.credential_command(identity, SqlBatch { statements: vec![
            SqlStatement {
                sql: "DELETE FROM lfs_grants WHERE digest IN (SELECT digest FROM lfs_grants WHERE expires_ms <= ?1 ORDER BY expires_ms LIMIT 256)".into(),
                parameters: vec![],
            },
            SqlStatement {
                sql: format!("INSERT INTO lfs_grants (digest, id, key_id, repository_id, operation, expires_ms) SELECT ?2, ?3, k.id, r.repository_id, ?6, ?1 + {} FROM ssh_keys k JOIN accounts a ON a.name = k.account JOIN repositories r ON r.repository_id = ?5 AND r.state = 'ready' WHERE k.id = ?4 AND k.enabled = 1 AND a.enabled = 1 AND (?6 = 'download' OR k.scope = 'write')", u64::from(LFS_AUTH_SECONDS) * 1000),
                parameters: vec![SqlValue::Blob(digest.to_vec()), SqlValue::Blob(id.to_vec()), SqlValue::Blob(key_id.to_vec()), SqlValue::Blob(repository.to_vec()), SqlValue::Text(operation.as_str().into())],
            },
        ] }).await?;
        Ok(result
            .output
            .get(1)
            .is_some_and(|set| set.rows_affected == 1))
    }

    pub(crate) async fn authenticate_lfs_grant(
        &self,
        digest: [u8; 32],
        owner: &str,
        repository: &str,
    ) -> Result<Option<LfsGrant>, InvocationError<Vec<SqlResultSet>>> {
        let result = self.credential_query(None, SqlBatch { statements: vec![SqlStatement {
            sql: "SELECT g.id, k.account, g.operation, g.repository_id FROM lfs_grants g JOIN ssh_keys k ON k.id = g.key_id JOIN accounts a ON a.name = k.account JOIN repositories r ON r.repository_id = g.repository_id WHERE g.digest = ?2 AND g.expires_ms > ?1 AND k.enabled = 1 AND a.enabled = 1 AND (g.operation = 'download' OR k.scope = 'write') AND r.owner = ?3 AND r.name = ?4 AND r.state = 'ready'".into(),
            parameters: vec![SqlValue::Blob(digest.to_vec()), SqlValue::Text(owner.into()), SqlValue::Text(repository.into())],
        }] }).await?;
        result
            .output
            .first()
            .and_then(|set| set.rows.first())
            .map(|row| {
                let [
                    SqlValue::Blob(id),
                    SqlValue::Text(account),
                    SqlValue::Text(operation),
                    SqlValue::Blob(repository),
                ] = row.as_slice()
                else {
                    return Err(InvocationError::NotStarted(Error::Command(
                        "invalid LFS grant",
                    )));
                };
                let operation = LfsOperation::parse(operation).ok_or(
                    InvocationError::NotStarted(Error::Command("invalid LFS grant operation")),
                )?;
                let token_id = id.as_slice().try_into().map_err(|_| {
                    InvocationError::NotStarted(Error::Command("invalid LFS grant ID"))
                })?;
                let repository = repository.as_slice().try_into().map_err(|_| {
                    InvocationError::NotStarted(Error::Command("invalid LFS grant repository"))
                })?;
                Ok(LfsGrant {
                    principal: Principal {
                        token_id,
                        account: account.clone(),
                        scope: operation.scope(),
                    },
                    operation,
                    repository,
                })
            })
            .transpose()
    }
}
