use super::*;
use crate::pulls::candidates::{
    RebaseUnavailable,
    rebase::{Commit, MAX_COMMIT_BYTES, MAX_COMMITS},
};

pub(super) async fn prepare(
    repository: &RepositoryCell,
    backend: &GitHttpBackend,
    candidate: &MergeCandidate,
    common: &str,
) -> Result<CandidateResult, GatewayError> {
    let unavailable = |reason| Ok(CandidateResult::RebaseUnavailable { reason });
    let revision = &candidate.request.revision;
    let range = format!("{}..{}", revision.base_oid, revision.source_oid);
    let limit = format!("--max-count={}", MAX_COMMITS + 1);
    let listed = run(
        backend,
        &["rev-list", "--reverse", "--topo-order", &limit, &range],
        b"",
        &[],
    )
    .await?;
    if !listed.status.success() {
        return Err(listed.error());
    }
    let text = std::str::from_utf8(&listed.stdout).map_err(|_| GatewayError::MalformedCache)?;
    let commits: Vec<_> = text.lines().collect();
    if commits.is_empty() {
        return unavailable(RebaseUnavailable::NoCommits);
    }
    if commits.len() > MAX_COMMITS {
        return unavailable(RebaseUnavailable::Limit);
    }
    let mut originals = Vec::with_capacity(commits.len());
    let mut parent = common;
    for commit in &commits {
        parse_oid(commit)?;
        let size = run(backend, &["cat-file", "-s", commit], b"", &[]).await?;
        if !size.status.success() {
            return Err(size.error());
        }
        let size: usize = std::str::from_utf8(&size.stdout)
            .ok()
            .and_then(|text| text.trim().parse().ok())
            .ok_or(GatewayError::MalformedCache)?;
        if size > MAX_COMMIT_BYTES {
            return unavailable(RebaseUnavailable::Limit);
        }
        let original = run(backend, &["cat-file", "commit", commit], b"", &[]).await?;
        if !original.status.success() {
            return Err(original.error());
        }
        let Some(parsed) = Commit::parse(&original.stdout) else {
            // Detect topology from Git rather than treating arbitrary malformed
            // headers as a merge. Neither case is silently flattened or dropped.
            let parents = run(
                backend,
                &["rev-list", "--parents", "-n", "1", commit],
                b"",
                &[],
            )
            .await?;
            if !parents.status.success() {
                return Err(parents.error());
            }
            let count = parents
                .stdout
                .split(|byte| byte.is_ascii_whitespace())
                .filter(|part| !part.is_empty())
                .count();
            return unavailable(if count > 2 {
                RebaseUnavailable::MergeHistory
            } else {
                RebaseUnavailable::CommitFormat
            });
        };
        if parsed.parent != parent {
            return unavailable(RebaseUnavailable::MergeHistory);
        }
        parent = commit;
        originals.push(original.stdout);
    }
    if parent != revision.source_oid {
        return Err(GatewayError::MalformedCache);
    }
    repository
        .prepare_ancestry(parse_oid(common)?, parse_oid(&revision.base_oid)?)
        .await
        .map_err(|error| GatewayError::Cell(Box::new(error)))?;
    let mut current = revision.base_oid.clone();
    let mut tree_oid = String::new();
    for (source, bytes) in commits.into_iter().zip(originals) {
        let original = Commit::parse(&bytes).ok_or(GatewayError::MalformedCache)?;
        // Replaying one change uses its original parent as the explicit base;
        // recomputing a merge base would replay the entire branch instead.
        let (tree, conflict) = merge_tree(backend, &current, source, Some(original.parent)).await?;
        if let Some(conflict) = conflict {
            return Ok(conflict);
        }
        let body = original.rewrite(candidate, &tree, &current);
        if body.len() > MAX_COMMIT_BYTES {
            return unavailable(RebaseUnavailable::Limit);
        }
        let written = run(
            backend,
            &["hash-object", "-t", "commit", "-w", "--stdin"],
            &body,
            &[],
        )
        .await?;
        if !written.status.success() {
            return Err(written.error());
        }
        current = output_oid(&written.stdout)?;
        tree_oid = tree;
    }
    Ok(CandidateResult::Ready {
        oid: current,
        tree_oid,
    })
}
