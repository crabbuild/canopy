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
