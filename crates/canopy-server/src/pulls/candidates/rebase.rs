//! Exact commit rewriting and bounded certification for linear rebases.

use super::*;

pub(crate) const MAX_COMMITS: usize = 128;
pub(crate) const MAX_COMMIT_BYTES: usize = 64 * 1024;

pub(crate) struct Commit<'a> {
    pub(crate) tree: &'a str,
    pub(crate) parent: &'a str,
    headers: Vec<&'a [u8]>,
    message: &'a [u8],
}
impl<'a> Commit<'a> {
    pub(crate) fn parse(body: &'a [u8]) -> Option<Self> {
        if body.len() > MAX_COMMIT_BYTES || body.contains(&0) {
            return None;
        }
        let split = body.windows(2).position(|bytes| bytes == b"\n\n")?;
        let mut headers = Vec::new();
        let mut start = 0;
        for end in 0..=split {
            if body[end] == b'\n' && body.get(end + 1) != Some(&b' ') {
                headers.push(&body[start..end]);
                start = end + 1;
            }
        }
        let field = |prefix: &[u8]| {
            let mut matches = headers
                .iter()
                .filter_map(|header: &&[u8]| header.strip_prefix(prefix));
            let value = matches.next()?;
            matches.next().is_none().then_some(value)
        };
        let tree = std::str::from_utf8(field(b"tree ")?).ok()?;
        let parent = std::str::from_utf8(field(b"parent ")?).ok()?;
        parse_oid(tree)?;
        parse_oid(parent)?;
        field(b"author ")?;
        field(b"committer ")?;
        Some(Self {
            tree,
            parent,
            headers,
            message: &body[split + 2..],
        })
    }

    pub(crate) fn rewrite(&self, candidate: &MergeCandidate, tree: &str, parent: &str) -> Vec<u8> {
        let actor = &candidate.actor;
        let time = candidate.created_at_ms / 1000;
        let mut body = Vec::new();
        for header in &self.headers {
            let replacement;
            let bytes = if header.starts_with(b"tree ") {
                replacement = format!("tree {tree}");
                replacement.as_bytes()
            } else if header.starts_with(b"parent ") {
                replacement = format!("parent {parent}");
                replacement.as_bytes()
            } else if header.starts_with(b"committer ") {
                replacement =
                    format!("committer {actor} <{actor}@users.canopy.invalid> {time} +0000");
                replacement.as_bytes()
            } else if header.starts_with(b"gpgsig ")
                || header.starts_with(b"gpgsig-sha256 ")
                || header.starts_with(b"mergetag ")
            {
                // These signatures certify the original bytes or ancestry;
                // retaining them after replay would misrepresent authenticity.
                continue;
            } else {
                header
            };
            body.extend_from_slice(bytes);
            body.push(b'\n');
        }
        body.push(b'\n');
        body.extend_from_slice(self.message);
        body
    }
}

pub(super) fn certified(
    context: &CommandContext<'_, '_>,
    candidate: &MergeCandidate,
    tip: &str,
    tree: &str,
) -> cellule_runtime::Result<bool> {
    let mut source = candidate.request.revision.source_oid.clone();
    let mut current = tip.to_owned();
    let base = &candidate.request.revision.base_oid;
    for index in 0..MAX_COMMITS {
        let Some(original) = body(context, &source)? else {
            return Ok(false);
        };
        let Some(rewritten) = body(context, &current)? else {
            return Ok(false);
        };
        let Some(original_commit) = Commit::parse(&original) else {
            return Ok(false);
        };
        let Some(rewritten_commit) = Commit::parse(&rewritten) else {
            return Ok(false);
        };
        if (index == 0 && rewritten_commit.tree != tree)
            || original_commit.rewrite(candidate, rewritten_commit.tree, rewritten_commit.parent)
                != rewritten
        {
            return Ok(false);
        }
        // Native Git owns replay selection and merge semantics. The transaction
        // binds every rewritten commit to the contiguous original chain and a
        // certified common ancestor, so a worker cannot omit an intermediate commit.
        source = original_commit.parent.to_owned();
        current = rewritten_commit.parent.to_owned();
        if &current == base {
            if &source == base {
                return Ok(true);
            }
            let rows = context.sql(&SqlBatch {
                statements: vec![SqlStatement {
                    sql: "SELECT 1 FROM commit_ancestry WHERE ancestor = ?1 AND descendant = ?2"
                        .into(),
                    parameters: vec![
                        SqlValue::Blob(oid(&source)?.to_vec()),
                        SqlValue::Blob(oid(base)?.to_vec()),
                    ],
                }],
            })?;
            return Ok(rows.first().is_some_and(|set| !set.rows.is_empty()));
        }
    }
    Ok(false)
}

fn body(
    context: &CommandContext<'_, '_>,
    commit: &str,
) -> cellule_runtime::Result<Option<Vec<u8>>> {
    let rows = context.sql(&SqlBatch { statements: vec![SqlStatement {
        sql: "SELECT o.body FROM objects o JOIN object_closure c ON c.oid = o.oid WHERE o.oid = ?1 AND o.kind = 'commit' AND o.storage = 'inline' AND o.size <= ?2".into(),
        parameters: vec![SqlValue::Blob(oid(commit)?.to_vec()), SqlValue::Integer(MAX_COMMIT_BYTES as i64)],
    }] })?;
    match rows
        .first()
        .and_then(|set| set.rows.first())
        .map(Vec::as_slice)
    {
        Some([SqlValue::Blob(bytes)])
            if crate::object_id(oid(commit)?.format(), ObjectKind::Commit, bytes)
                == oid(commit)? =>
        {
            Ok(Some(bytes.clone()))
        }
        _ => Ok(None),
    }
}
