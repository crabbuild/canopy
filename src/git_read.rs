//! Bounded repository browsing and PR comparison over verified Cell Git objects.

use crate::ReadIdentity;

pub(crate) mod browse;
mod graph;
pub(crate) mod patch;
mod trees;

use crate::{
    ObjectKind, RepositoryCell,
    pulls::{PullRevision, parse_oid},
};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use cellule_runtime::{InvocationError, SqlBatch, SqlResultSet, SqlStatement, SqlValue};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, sync::Arc};
use tokio::sync::OwnedSemaphorePermit;

type Oid = [u8; 20];
const FILE_PAGE: usize = 32;
const MAX_PATH: usize = 4096;
const PREVIEW_BYTES: u64 = 256 * 1024;
const MAX_OBJECT_BYTES: u64 = 8 * 1024 * 1024;
const MAX_TREE_BYTES: u64 = 64 * 1024 * 1024;
const MAX_TREE_ENTRIES: usize = 250_000;
const MAX_CHANGED_FILES: usize = 10_000;
const MAX_DEPTH: usize = 128;

#[derive(Debug, thiserror::Error)]
pub(crate) enum ReadError {
    #[error("Git view is unavailable")]
    Missing,
    #[error("pull request revision changed")]
    Changed,
    #[error("Git history has no common ancestor")]
    Unrelated,
    #[error("Git history has multiple best common ancestors")]
    Ambiguous,
    #[error("Git view exceeds its traversal or output limit")]
    TooLarge,
    #[error("invalid comparison input")]
    Invalid,
    #[error("Git view encountered malformed data")]
    Malformed,
    #[error("Git Cell read failed")]
    Cell(#[from] InvocationError<Vec<SqlResultSet>>),
    #[error("Git read worker failed")]
    Task(#[from] tokio::task::JoinError),
}
#[derive(Clone, Copy, PartialEq, Eq)]
struct Node {
    mode: u32,
    oid: Oid,
}
impl Node {
    fn is_tree(self) -> bool {
        self.mode & 0o170000 == 0o040000
    }
}
#[derive(Serialize)]
pub(crate) struct Entry {
    mode: String,
    oid: String,
}
impl From<Node> for Entry {
    fn from(node: Node) -> Self {
        Self {
            mode: format!("{:06o}", node.mode),
            oid: hex::encode(node.oid),
        }
    }
}
#[derive(Serialize)]
pub(crate) struct FileChange {
    path_base64: String,
    path: Option<String>,
    before: Option<Entry>,
    after: Option<Entry>,
}
#[derive(Clone, Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase", deny_unknown_fields)]
pub(crate) enum ComparisonTarget {
    Current { revision: PullRevision },
    Review { number: i64 },
    Thread { number: i64 },
    // An empty struct rejects extra keys; serde's internally tagged unit
    // variant would silently discard a caller-supplied revision.
    Merged {},
}
#[derive(Serialize)]
pub(crate) struct Comparison {
    revision: PullRevision,
    merge_base: String,
    files: Vec<FileChange>,
    next_after: Option<String>,
}
#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum Side {
    Before,
    After,
}
#[derive(Serialize)]
pub(crate) struct FilePreview {
    revision: PullRevision,
    merge_base: String,
    path_base64: String,
    path: Option<String>,
    entry: Entry,
    size: Option<u64>,
    content_status: &'static str,
    content_base64: Option<String>,
}

pub(crate) struct Reader {
    repository: Arc<RepositoryCell>,
    admission: Arc<OwnedSemaphorePermit>,
    bytes: u64,
    entries: usize,
}
impl Reader {
    pub(crate) fn new(
        repository: Arc<RepositoryCell>,
        admission: Arc<OwnedSemaphorePermit>,
    ) -> Self {
        Self {
            repository,
            admission,
            bytes: 0,
            entries: 0,
        }
    }
    async fn authorize<'a>(
        &self,
        actor: impl Into<ReadIdentity<'a>>,
        number: i64,
        target: &ComparisonTarget,
    ) -> Result<PullRevision, ReadError> {
        let actor = actor.into();
        if matches!(target, ComparisonTarget::Review { number } | ComparisonTarget::Thread { number } if *number < 1)
        {
            return Err(ReadError::Invalid);
        }
        let revision = match target {
            ComparisonTarget::Thread { number: thread } => self
                .repository
                .thread_revision(actor, number, *thread)
                .await?
                .ok_or(ReadError::Missing)?,
            ComparisonTarget::Review { number: review } => self
                .repository
                .reviewed_revision(actor, number, *review)
                .await?
                .ok_or(ReadError::Missing)?,
            ComparisonTarget::Current { revision } => {
                if revision.pull_version < 1
                    || revision.source_version < 1
                    || revision.base_version < 1
                {
                    return Err(ReadError::Invalid);
                }
                oid(&revision.source_oid)?;
                oid(&revision.base_oid)?;
                let pull = self
                    .repository
                    .pull(actor, number)
                    .await?
                    .output
                    .ok_or(ReadError::Missing)?
                    .summary;
                if pull.version != revision.pull_version
                    || pull.source.oid.as_deref() != Some(&revision.source_oid)
                    || pull.source.version != revision.source_version
                    || pull.base.oid.as_deref() != Some(&revision.base_oid)
                    || pull.base.version != revision.base_version
                {
                    return Err(ReadError::Changed);
                }
                revision.clone()
            }
            ComparisonTarget::Merged {} => {
                self.repository
                    .pull(actor, number)
                    .await?
                    .output
                    .and_then(|pull| pull.merge)
                    .ok_or(ReadError::Missing)?
                    .revision
            }
        };
        Ok(revision)
    }
    async fn roots(&mut self, base: Oid, source: Oid) -> Result<(Oid, Oid, Oid), ReadError> {
        let merge_base = self.merge_base(base, source).await?;
        let before = self.commit_tree(merge_base).await?;
        let after = self.commit_tree(source).await?;
        Ok((merge_base, before, after))
    }
    pub(crate) async fn files<'a>(
        mut self,
        actor: impl Into<ReadIdentity<'a>>,
        number: i64,
        target: ComparisonTarget,
        after: Option<&str>,
    ) -> Result<Comparison, ReadError> {
        let actor = actor.into();
        let cursor = after.map(path).transpose()?;
        let revision = self.authorize(actor, number, &target).await?;
        let (base, source) = (oid(&revision.base_oid)?, oid(&revision.source_oid)?);
        let (merge_base, before, after) = self.roots(base, source).await?;
        let changes = self.changes(before, after).await?;
        let mut remaining = changes
            .into_iter()
            .filter(|(path, _)| cursor.as_ref().is_none_or(|cursor| path > cursor));
        let page: Vec<_> = remaining.by_ref().take(FILE_PAGE).collect();
        let next_after = if remaining.next().is_some() {
            page.last().map(|(path, _)| URL_SAFE_NO_PAD.encode(path))
        } else {
            None
        };
        let files = page
            .into_iter()
            .map(|(path, (before, after))| FileChange {
                path_base64: URL_SAFE_NO_PAD.encode(&path),
                path: String::from_utf8(path).ok(),
                before: before.map(Entry::from),
                after: after.map(Entry::from),
            })
            .collect();
        // Historical roots are immutable, but access is always current. Live
        // views also reject tip movement before returning their result.
        self.authorize(actor, number, &target).await?;
        Ok(Comparison {
            revision,
            merge_base: hex::encode(merge_base),
            files,
            next_after,
        })
    }
    pub(crate) async fn file<'a>(
        mut self,
        actor: impl Into<ReadIdentity<'a>>,
        number: i64,
        target: ComparisonTarget,
        encoded_path: &str,
        side: Side,
    ) -> Result<FilePreview, ReadError> {
        let actor = actor.into();
        let path = path(encoded_path)?;
        let revision = self.authorize(actor, number, &target).await?;
        let (base, source) = (oid(&revision.base_oid)?, oid(&revision.source_oid)?);
        let (merge_base, before, after) = self.roots(base, source).await?;
        let root = match side {
            Side::Before => before,
            Side::After => after,
        };
        let entry = self
            .resolve(root, &path)
            .await?
            .filter(|entry| !entry.is_tree())
            .ok_or(ReadError::Missing)?;
        let (size, content_status, content_base64) = if entry.mode & 0o170000 == 0o160000 {
            (None, "gitlink", None)
        } else {
            let size = self.size(entry.oid, ObjectKind::Blob).await?;
            if size > PREVIEW_BYTES {
                (Some(size), "too_large", None)
            } else {
                let body = self.body(entry.oid, ObjectKind::Blob).await?;
                (Some(size), "included", Some(URL_SAFE_NO_PAD.encode(body)))
            }
        };
        self.authorize(actor, number, &target).await?;
        Ok(FilePreview {
            revision,
            merge_base: hex::encode(merge_base),
            path_base64: URL_SAFE_NO_PAD.encode(&path),
            path: String::from_utf8(path).ok(),
            entry: entry.into(),
            size,
            content_status,
            content_base64,
        })
    }
    async fn size(&self, oid: Oid, kind: ObjectKind) -> Result<u64, ReadError> {
        let result = self
            .repository
            .sql
            .query(
                None,
                SqlBatch {
                    statements: vec![SqlStatement {
                        sql: "SELECT kind, size FROM objects WHERE oid = ?1".into(),
                        parameters: vec![SqlValue::Blob(oid.to_vec())],
                    }],
                },
            )
            .await?;
        let Some([SqlValue::Text(stored_kind), SqlValue::Integer(size)]) = result
            .output
            .first()
            .and_then(|set| set.rows.first())
            .map(Vec::as_slice)
        else {
            return Err(ReadError::Malformed);
        };
        let expected = match kind {
            ObjectKind::Blob => "blob",
            ObjectKind::Tree => "tree",
            ObjectKind::Commit => "commit",
            ObjectKind::Tag => "tag",
        };
        if stored_kind != expected {
            return Err(ReadError::Malformed);
        }
        u64::try_from(*size).map_err(|_| ReadError::Malformed)
    }
    async fn body(&mut self, oid: Oid, kind: ObjectKind) -> Result<Vec<u8>, ReadError> {
        let size = self.size(oid, kind).await?;
        if size > MAX_OBJECT_BYTES || size > MAX_TREE_BYTES.saturating_sub(self.bytes) {
            return Err(ReadError::TooLarge);
        }
        self.bytes += size;
        let (stored_kind, body) = self
            .repository
            .object(oid, None)
            .await?
            .output
            .ok_or(ReadError::Malformed)?;
        if stored_kind != kind || body.len() as u64 != size {
            return Err(ReadError::Malformed);
        }
        Ok(body)
    }
    async fn commit_tree(&mut self, oid: Oid) -> Result<Oid, ReadError> {
        let body = self.body(oid, ObjectKind::Commit).await?;
        let line = body
            .split(|byte| *byte == b'\n')
            .next()
            .ok_or(ReadError::Malformed)?;
        let tree = line.strip_prefix(b"tree ").ok_or(ReadError::Malformed)?;
        let tree = std::str::from_utf8(tree).map_err(|_| ReadError::Malformed)?;
        oid_from_bytes(tree).ok_or(ReadError::Malformed)
    }
}
fn oid_from_bytes(value: &str) -> Option<Oid> {
    parse_oid(value)?.try_into().ok()
}
fn oid(value: &str) -> Result<Oid, ReadError> {
    oid_from_bytes(value).ok_or(ReadError::Invalid)
}
fn path(value: &str) -> Result<Vec<u8>, ReadError> {
    if value.len() > MAX_PATH.div_ceil(3) * 4 {
        return Err(ReadError::Invalid);
    }
    let bytes = URL_SAFE_NO_PAD
        .decode(value)
        .map_err(|_| ReadError::Invalid)?;
    if bytes.is_empty()
        || bytes.len() > MAX_PATH
        || bytes.contains(&0)
        || bytes
            .split(|byte| *byte == b'/')
            .any(|part| part.is_empty() || part == b"." || part == b"..")
        || URL_SAFE_NO_PAD.encode(&bytes) != value
    {
        return Err(ReadError::Invalid);
    }
    Ok(bytes)
}
