use super::*;
use crate::directory::{TokenAuthority, TokenChange, TokenInfo};

impl RepositoryManager {
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
