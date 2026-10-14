//! Exact commit rewriting and bounded certification for linear rebases.

use super::*;

pub(crate) const MAX_COMMITS: usize = 128;
pub(crate) const MAX_COMMIT_BYTES: usize = 64 * 1024;

pub(crate) struct Commit<'a> {
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
            parent,
            headers,
            message: &body[split + 2..],
        })
    }

    pub(super) fn metadata(&self) -> Vec<u8> {
        let mut hash = blake3::Hasher::new();
        let mut committer = [0; 32];
        let mut signed = false;
        for header in &self.headers {
            let normalized: &[u8] = if header.starts_with(b"tree ") {
                b"tree"
            } else if header.starts_with(b"parent ") {
                b"parent"
            } else if header.starts_with(b"committer ") {
                committer = *blake3::hash(header).as_bytes();
                b"committer"
            } else if header.starts_with(b"gpgsig ")
                || header.starts_with(b"gpgsig-sha256 ")
                || header.starts_with(b"mergetag ")
            {
                signed = true;
                continue;
            } else {
                header
            };
            hash.update(&(normalized.len() as u64).to_le_bytes());
            hash.update(normalized);
        }
        hash.update(&(self.message.len() as u64).to_le_bytes());
        hash.update(self.message);
        let mut metadata = hash.finalize().as_bytes().to_vec();
        metadata.extend_from_slice(&committer);
        metadata.push(u8::from(signed));
        metadata
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
        let Some(original_commit) = projection(context, &source)? else {
            return Ok(false);
        };
        let Some(rewritten_commit) = projection(context, &current)? else {
            return Ok(false);
        };
        let expected_committer = format!(
            "committer {} <{}@users.canopy.invalid> {} +0000",
            candidate.actor,
            candidate.actor,
            candidate.created_at_ms / 1000
        );
        if (index == 0 && rewritten_commit.tree != tree)
            || original_commit.metadata[..32] != rewritten_commit.metadata[..32]
            || rewritten_commit.metadata[32..64]
                != blake3::hash(expected_committer.as_bytes()).as_bytes()[..]
            || rewritten_commit.metadata[64] != 0
        {
            return Ok(false);
        }
        // Native Git owns replay selection and merge semantics. The transaction
        // binds every rewritten commit to the contiguous original chain and a
        // certified common ancestor, so a worker cannot omit an intermediate commit.
        source = original_commit.parent;
        current = rewritten_commit.parent;
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

struct Projection {
    tree: String,
    parent: String,
    metadata: Vec<u8>,
}
fn projection(
    context: &CommandContext<'_, '_>,
    commit: &str,
) -> cellule_runtime::Result<Option<Projection>> {
    let id = oid(commit)?;
    let rows = context.sql(&SqlBatch { statements: vec![SqlStatement {
        sql: "SELECT o.rebase_metadata FROM objects o JOIN object_closure c ON c.oid = o.oid WHERE o.oid = ?1 AND o.kind = 'commit' AND o.size <= ?2".into(), parameters: vec![SqlValue::Blob(id.to_vec()), SqlValue::Integer(MAX_COMMIT_BYTES as i64)],
    }]})?;
    let Some([SqlValue::Blob(metadata)]) = rows
        .first()
        .and_then(|set| set.rows.first())
        .map(Vec::as_slice)
    else {
        return Ok(None);
    };
    if metadata.len() != 65 {
        return Err(Error::Command("invalid rebase metadata"));
    }
    let rows = context.sql(&SqlBatch { statements: vec![SqlStatement {
        sql: "SELECT child.oid, child.kind FROM object_edges e JOIN objects child ON child.oid = e.child WHERE e.parent = ?1 ORDER BY child.kind, child.oid".into(), parameters: vec![SqlValue::Blob(id.to_vec())],
    }]})?;
    let mut tree = None;
    let mut parent = None;
    for row in &rows
        .first()
        .ok_or(Error::Command("missing rebase edges"))?
        .rows
    {
        let [SqlValue::Blob(oid), SqlValue::Text(kind)] = row.as_slice() else {
            return Err(Error::Command("invalid rebase edge"));
        };
        let value = hex::encode(oid);
        match kind.as_str() {
            "tree" if tree.is_none() => tree = Some(value),
            "commit" if parent.is_none() => parent = Some(value),
            _ => return Ok(None),
        }
    }
    Ok(tree.zip(parent).map(|(tree, parent)| Projection {
        tree,
        parent,
        metadata: metadata.clone(),
    }))
}
