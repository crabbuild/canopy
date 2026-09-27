use super::*;
use crate::directory::{SshKey, SshKeyChange, SshKeyInfo, TokenAuthority, TokenChange, TokenInfo};

impl RepositoryManager {
    pub(crate) async fn account_events(
        &self,
        actor_digest: [u8; 32],
        before: Option<i64>,
    ) -> Result<Option<Vec<directory::AccountEvent>>, ServerError> {
        Ok(self
            .directory
            .account_events(actor_digest, &self.owner, before)
            .await?
            .output)
    }

    pub(crate) async fn accounts(
        &self,
        actor_digest: [u8; 32],
        after: Option<&str>,
    ) -> Result<Option<Vec<directory::AccountInfo>>, ServerError> {
        Ok(self
            .directory
            .accounts(actor_digest, &self.owner, after)
            .await?
            .output)
    }

    pub(crate) async fn tokens(
        &self,
        actor_digest: [u8; 32],
        account: &str,
        after: Option<[u8; 16]>,
    ) -> Result<Option<Vec<TokenInfo>>, ServerError> {
        let authority = self.token_authority(actor_digest, account);
        Ok(self.directory.tokens(authority, after).await?.output)
    }

    pub(crate) async fn issue_token(
        &self,
        actor_digest: [u8; 32],
        account: &str,
        id: [u8; 16],
        digest: [u8; 32],
        scope: TokenScope,
        expires_at_ms: Option<i64>,
    ) -> Result<TokenChange, ServerError> {
        let authority = self.token_authority(actor_digest, account);
        Ok(self
            .directory
            .issue_token(
                mutation_identity()?,
                authority,
                id,
                digest,
                scope,
                expires_at_ms,
            )
            .await?
            .output)
    }

    pub(crate) async fn revoke_token(
        &self,
        actor_digest: [u8; 32],
        account: &str,
        id: [u8; 16],
    ) -> Result<TokenChange, ServerError> {
        let authority = self.token_authority(actor_digest, account);
        Ok(self
            .directory
            .revoke_token(mutation_identity()?, authority, id)
            .await?
            .output)
    }

    pub(crate) async fn ssh_keys(
        &self,
        actor_digest: [u8; 32],
        account: &str,
        after: Option<[u8; 16]>,
    ) -> Result<Option<Vec<SshKeyInfo>>, ServerError> {
        Ok(self
            .directory
            .ssh_keys(self.token_authority(actor_digest, account), after)
            .await?
            .output)
    }

    pub(crate) async fn register_ssh_key(
        &self,
        actor_digest: [u8; 32],
        account: &str,
        id: [u8; 16],
        key: &SshKey,
        scope: TokenScope,
    ) -> Result<SshKeyChange, ServerError> {
        Ok(self
            .directory
            .register_ssh_key(
                mutation_identity()?,
                self.token_authority(actor_digest, account),
                id,
                key,
                scope,
            )
            .await?
            .output)
    }

    pub(crate) async fn revoke_ssh_key(
        &self,
        actor_digest: [u8; 32],
        account: &str,
        id: [u8; 16],
    ) -> Result<SshKeyChange, ServerError> {
        Ok(self
            .directory
            .revoke_ssh_key(
                mutation_identity()?,
                self.token_authority(actor_digest, account),
                id,
            )
            .await?
            .output)
    }

    fn token_authority<'a>(
        &'a self,
        actor_digest: [u8; 32],
        account: &'a str,
    ) -> TokenAuthority<'a> {
        TokenAuthority {
            actor_digest,
            site_owner: &self.owner,
            account,
        }
    }
}
