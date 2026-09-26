use super::*;
use crate::{DefaultBranch, directory::REPOSITORY_PAGE_SIZE};

pub(crate) struct RepositoryDetails {
    pub entry: RepositoryEntry,
    pub role: TokenScope,
    pub head: DefaultBranch,
}

impl RepositoryManager {
    pub(crate) async fn inspect(
        self: &Arc<Self>,
        actor: &str,
        name: &str,
    ) -> Result<Option<RepositoryDetails>, ServerError> {
        let Some(entry) = self
            .directory
            .lookup_candidate(actor, &self.owner, name)
            .await?
            .output
        else {
            return Ok(None);
        };
        let route = self.load(entry.clone()).await?;
        let Some(role) = route.repository.access_level(actor, None).await?.output else {
            return Ok(None);
        };
        let head = route.repository.default_branch(None).await?.output;
        Ok(Some(RepositoryDetails { entry, role, head }))
    }

    pub(crate) async fn list(
        self: &Arc<Self>,
        actor: &str,
        after: Option<[u8; 16]>,
    ) -> Result<(Vec<RepositoryEntry>, Option<String>), ServerError> {
        let candidates = self.directory.list_candidates(actor, after).await?.output;
        // Advance by candidates, including revoked grants. Otherwise a page of
        // revoked entries could trap pagination or require an unbounded scan.
        let full = candidates.len() == REPOSITORY_PAGE_SIZE;
        let mut scanned = None;
        let mut entries = Vec::new();
        for entry in candidates {
            if entry.owner == actor {
                scanned = Some(entry.repository_id);
                entries.push(entry);
                continue;
            }
            let route = match self.load(entry.clone()).await {
                Ok(route) => route,
                // Movement admission can run out partway through a cold page.
                // Resume after the last checked candidate instead of rescanning it.
                Err(ServerError::Runtime(Error::Capacity(_))) if scanned.is_some() => {
                    return Ok((
                        entries,
                        scanned.map(|id| uuid::Uuid::from_bytes(id).to_string()),
                    ));
                }
                Err(error) => return Err(error),
            };
            let id = entry.repository_id;
            if route
                .repository
                .access_level(actor, None)
                .await?
                .output
                .is_some()
            {
                entries.push(entry);
            }
            scanned = Some(id);
        }
        let next = if full {
            scanned.map(|id| uuid::Uuid::from_bytes(id).to_string())
        } else {
            None
        };
        Ok((entries, next))
    }
}
