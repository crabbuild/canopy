use super::*;
use crate::{
    DefaultBranch, ReadIdentity, RepositoryCell, RepositoryVisibility, Visibility,
    directory::REPOSITORY_PAGE_SIZE,
};

pub(crate) struct RepositoryDetails {
    pub entry: RepositoryEntry,
    pub role: TokenScope,
    pub head: DefaultBranch,
    pub visibility: RepositoryVisibility,
}

impl RepositoryManager {
    pub(crate) async fn set_visibility(
        &self,
        repository: &RepositoryCell,
        actor: &str,
        generation: i64,
        visibility: Visibility,
    ) -> Result<bool, ServerError> {
        // Publish the discovery hint first. Failed/stale visibility writes leave
        // only an invisible candidate, and later privacy changes cannot race a
        // deletion of a newer public listing. Every listing rechecks Cell access.
        if visibility == Visibility::Public
            && !self
                .directory
                .remember_public(mutation_identity()?, actor, repository.repository_id())
                .await?
                .output
        {
            return Ok(false);
        }
        Ok(repository
            .set_visibility(mutation_identity()?, actor, generation, visibility)
            .await?
            .output)
    }

    pub(crate) async fn inspect(
        self: &Arc<Self>,
        actor: ReadIdentity<'_>,
        name: &str,
    ) -> Result<Option<RepositoryDetails>, ServerError> {
        let started = Instant::now();
        let lookup = self
            .directory
            .lookup_candidate(actor, &self.owner, name)
            .await;
        tracing::debug!(
            stage = "directory_lookup",
            elapsed_seconds = started.elapsed().as_secs_f64(),
            succeeded = lookup.is_ok(),
            "repository request stage completed"
        );
        let Some(entry) = lookup?.output else {
            return Ok(None);
        };
        let repository = entry.repository_id;
        let started = Instant::now();
        let result: Result<Option<RepositoryDetails>, ServerError> = async {
            let route = self.load(entry.clone()).await?;
            let Some(role) = route.repository.access_level(actor, None).await?.output else {
                return Ok(None);
            };
            let head = route.repository.default_branch(None).await?.output;
            let visibility = route.repository.visibility().await?.output;
            Ok(Some(RepositoryDetails {
                entry,
                role,
                head,
                visibility,
            }))
        }
        .await;
        tracing::debug!(
            stage = "repository_metadata",
            repository = %hex::encode(repository),
            elapsed_seconds = started.elapsed().as_secs_f64(),
            succeeded = result.is_ok(),
            "repository request stage completed"
        );
        result
    }

    pub(crate) async fn list(
        self: &Arc<Self>,
        actor: ReadIdentity<'_>,
        after: Option<[u8; 16]>,
    ) -> Result<(Vec<RepositoryEntry>, Option<String>), ServerError> {
        let candidates = self.directory.list_candidates(actor, after).await?.output;
        // Advance by candidates, including revoked grants. Otherwise a page of
        // revoked entries could trap pagination or require an unbounded scan.
        let full = candidates.len() == REPOSITORY_PAGE_SIZE;
        let mut scanned = None;
        let mut entries = Vec::new();
        for entry in candidates {
            if matches!(actor, ReadIdentity::Account(account) if entry.owner == account) {
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
