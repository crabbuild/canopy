use super::*;
use crate::ReadIdentity;
use crate::refs::{REF_PAGE_SIZE, RefReadError, valid_ref_name};

const PAGE: usize = 32;

#[derive(Serialize)]
pub(crate) struct Commit {
    oid: String,
    tree_oid: String,
    parents: Vec<String>,
    author_base64: Option<String>,
    committer_base64: Option<String>,
    message_base64: String,
    message_truncated: bool,
}
#[derive(Serialize)]
pub(crate) struct DirectoryEntry {
    name_base64: String,
    name: Option<String>,
    path_base64: String,
    kind: &'static str,
    #[serde(flatten)]
    entry: Entry,
}
#[derive(Serialize)]
pub(crate) struct Directory {
    commit: Commit,
    path_base64: String,
    tree_oid: String,
    entries: Vec<DirectoryEntry>,
    next_after: Option<String>,
}
#[derive(Serialize)]
pub(crate) struct File {
    commit: String,
    path_base64: String,
    path: Option<String>,
    #[serde(flatten)]
    entry: Entry,
    size: Option<u64>,
    content_status: &'static str,
    content_base64: Option<String>,
}
#[derive(Serialize)]
pub(crate) struct History {
    commits: Vec<Commit>,
    next_commit: Option<String>,
}

impl Reader {
    async fn member<'a>(&self, actor: impl Into<ReadIdentity<'a>>) -> Result<(), ReadError> {
        let actor = actor.into();
        self.repository
            .access_level(actor, None)
            .await?
            .output
            .ok_or(ReadError::Missing)?;
        Ok(())
    }
    pub(crate) async fn browser_refs<'a>(
        &self,
        actor: impl Into<ReadIdentity<'a>>,
        after: &str,
        generation: Option<i64>,
    ) -> Result<serde_json::Value, ReadError> {
        let actor = actor.into();
        if (!after.is_empty() && (!valid_ref_name(after) || generation.is_none()))
            || generation.is_some_and(|n| n < 0)
        {
            return Err(ReadError::Invalid);
        }
        self.member(actor).await?;
        let page = self
            .repository
            .refs_page(after, generation)
            .await
            .map_err(|error| match error {
                RefReadError::Changed => ReadError::Changed,
                RefReadError::Cell(error) => ReadError::Cell(error),
            })?
            .output;
        let next_after = (page.refs.len() == REF_PAGE_SIZE)
            .then(|| page.refs.last().map(|(name, _)| name.clone()))
            .flatten();
        let entries: Vec<_> = page.refs.into_iter().filter_map(|(name, state)| state.oid.map(|oid| serde_json::json!({"name":name,"oid":hex::encode(oid),"version":state.version}))).collect();
        self.member(actor).await?;
        Ok(
            serde_json::json!({"generation":page.generation,"default_branch":page.default_branch,"entries":entries,"next_after":next_after}),
        )
    }
    pub(crate) async fn browser_ref<'a>(
        &self,
        actor: impl Into<ReadIdentity<'a>>,
        reference: Option<&str>,
    ) -> Result<serde_json::Value, ReadError> {
        let actor = actor.into();
        if reference.is_some_and(|name| !valid_ref_name(name)) {
            return Err(ReadError::Invalid);
        }
        self.member(actor).await?;
        // Resolve default HEAD and its tip in one observation. The returned OID
        // pins subsequent reads even if a writer moves the ref during browsing.
        let result = self.repository.sql.query(None, SqlBatch {statements: vec![SqlStatement {
            sql: "SELECT g.generation, coalesce(?1,g.default_branch), r.oid, r.version FROM ref_generation g LEFT JOIN refs r ON r.name = coalesce(?1,g.default_branch) WHERE g.singleton = 1".into(),
            parameters: vec![reference.map_or(SqlValue::Null, |value| SqlValue::Text(value.into()))],
        }]}).await?;
        let row = result
            .output
            .first()
            .and_then(|set| set.rows.first())
            .ok_or(ReadError::Malformed)?;
        let [
            SqlValue::Integer(generation),
            SqlValue::Text(name),
            value,
            version,
        ] = row.as_slice()
        else {
            return Err(ReadError::Malformed);
        };
        let oid = match value {
            SqlValue::Null => None,
            SqlValue::Blob(bytes) if matches!(bytes.len(), 20 | 32) => Some(hex::encode(bytes)),
            _ => return Err(ReadError::Malformed),
        };
        let version = match version {
            SqlValue::Null => None,
            SqlValue::Integer(n) => Some(*n),
            _ => return Err(ReadError::Malformed),
        };
        self.member(actor).await?;
        Ok(
            serde_json::json!({"reference":name,"oid":oid,"version":version,"generation":generation}),
        )
    }
    async fn commit(&mut self, mut target: Oid) -> Result<Commit, ReadError> {
        // Only certified objects are browseable. Staged push bytes do not become
        // visible through a guessed OID before their complete graph is verified.
        for _ in 0..16 {
            let rows = self.repository.sql.query(None,SqlBatch {statements:vec![SqlStatement {
                sql:"SELECT o.kind FROM objects o JOIN object_closure c ON c.oid = o.oid WHERE o.oid = ?1".into(), parameters:vec![SqlValue::Blob(target.to_vec())],
            }]}).await?;
            let Some([SqlValue::Text(kind)]) = rows
                .output
                .first()
                .and_then(|s| s.rows.first())
                .map(Vec::as_slice)
            else {
                return Err(ReadError::Missing);
            };
            match kind.as_str() {
                "commit" => {
                    let body = self.body(target, ObjectKind::Commit).await?;
                    let admission = Arc::clone(&self.admission);
                    return tokio::task::spawn_blocking(move || {
                        let _admission = admission;
                        parse_commit(target, &body)
                    })
                    .await?;
                }
                "tag" => {
                    let body = self.body(target, ObjectKind::Tag).await?;
                    let first = body
                        .split(|b| *b == b'\n')
                        .next()
                        .ok_or(ReadError::Malformed)?;
                    target = std::str::from_utf8(
                        first.strip_prefix(b"object ").ok_or(ReadError::Malformed)?,
                    )
                    .ok()
                    .and_then(oid_from_bytes)
                    .ok_or(ReadError::Malformed)?;
                }
                _ => return Err(ReadError::Missing),
            }
        }
        Err(ReadError::TooLarge)
    }
    pub(crate) async fn browser_tree<'a>(
        mut self,
        actor: impl Into<ReadIdentity<'a>>,
        revision: &str,
        encoded_path: &str,
        after: Option<&str>,
    ) -> Result<Directory, ReadError> {
        let actor = actor.into();
        self.member(actor).await?;
        let path = if encoded_path.is_empty() {
            Vec::new()
        } else {
            path(encoded_path)?
        };
        let cursor = after.map(path_component).transpose()?;
        let commit = self.commit(oid(revision)?).await?;
        let entry = self
            .resolve(oid(&commit.tree_oid)?, &path)
            .await?
            .filter(|entry| entry.is_tree())
            .ok_or(ReadError::Missing)?;
        let tree = self.tree(entry.oid).await?;
        let mut remaining = tree
            .into_iter()
            .filter(|(name, _)| cursor.as_ref().is_none_or(|cursor| name > cursor));
        let page: Vec<_> = remaining.by_ref().take(PAGE).collect();
        let next_after = remaining
            .next()
            .and_then(|_| page.last().map(|(name, _)| URL_SAFE_NO_PAD.encode(name)));
        let mut entries = Vec::with_capacity(page.len());
        for (name, node) in page {
            let full = super::trees::join(&path, &name)?;
            entries.push(DirectoryEntry {
                name_base64: URL_SAFE_NO_PAD.encode(&name),
                name: String::from_utf8(name).ok(),
                path_base64: URL_SAFE_NO_PAD.encode(full),
                kind: if node.is_tree() {
                    "tree"
                } else if node.mode == 0o160000 {
                    "gitlink"
                } else if node.mode == 0o120000 {
                    "symlink"
                } else {
                    "file"
                },
                entry: node.into(),
            });
        }
        self.member(actor).await?;
        Ok(Directory {
            commit,
            path_base64: encoded_path.into(),
            tree_oid: hex::encode(entry.oid),
            entries,
            next_after,
        })
    }
    pub(crate) async fn browser_file<'a>(
        mut self,
        actor: impl Into<ReadIdentity<'a>>,
        revision: &str,
        encoded_path: &str,
    ) -> Result<File, ReadError> {
        let actor = actor.into();
        self.member(actor).await?;
        let path = path(encoded_path)?;
        let commit = self.commit(oid(revision)?).await?;
        let entry = self
            .resolve(oid(&commit.tree_oid)?, &path)
            .await?
            .filter(|entry| !entry.is_tree())
            .ok_or(ReadError::Missing)?;
        let (size, content_status, content_base64) = if entry.mode == 0o160000 {
            (None, "gitlink", None)
        } else {
            let size = self.size(entry.oid, ObjectKind::Blob).await?;
            if size > PREVIEW_BYTES {
                (Some(size), "too_large", None)
            } else {
                (
                    Some(size),
                    "included",
                    Some(URL_SAFE_NO_PAD.encode(self.body(entry.oid, ObjectKind::Blob).await?)),
                )
            }
        };
        self.member(actor).await?;
        Ok(File {
            commit: commit.oid,
            path_base64: encoded_path.into(),
            path: String::from_utf8(path).ok(),
            entry: entry.into(),
            size,
            content_status,
            content_base64,
        })
    }
    pub(crate) async fn browser_history<'a>(
        mut self,
        actor: impl Into<ReadIdentity<'a>>,
        revision: &str,
    ) -> Result<History, ReadError> {
        let actor = actor.into();
        self.member(actor).await?;
        let mut target = Some(oid(revision)?);
        let mut commits = Vec::new();
        while let Some(current) = target {
            if commits.len() == PAGE {
                break;
            }
            let commit = self.commit(current).await?;
            target = commit
                .parents
                .first()
                .map(|parent| oid(parent))
                .transpose()?;
            commits.push(commit);
        }
        self.member(actor).await?;
        Ok(History {
            commits,
            next_commit: target.map(hex::encode),
        })
    }
}
fn path_component(value: &str) -> Result<Vec<u8>, ReadError> {
    let decoded = path(value)?;
    if decoded.contains(&b'/') {
        return Err(ReadError::Invalid);
    }
    Ok(decoded)
}
fn parse_commit(id: Oid, body: &[u8]) -> Result<Commit, ReadError> {
    let split = body
        .windows(2)
        .position(|w| w == b"\n\n")
        .ok_or(ReadError::Malformed)?;
    let mut lines = body[..split].split(|b| *b == b'\n').peekable();
    let tree = lines
        .next()
        .and_then(|line| line.strip_prefix(b"tree "))
        .and_then(|value| std::str::from_utf8(value).ok())
        .and_then(oid_from_bytes)
        .filter(|oid| oid.format() == id.format())
        .ok_or(ReadError::Malformed)?;
    let mut parents = Vec::new();
    // Git recognizes parent edges only immediately after the first tree line.
    // Later headers and signature/message contents must not invent ancestry.
    while let Some(value) = lines.peek().and_then(|line| line.strip_prefix(b"parent ")) {
        if parents.len() == 2048 {
            return Err(ReadError::TooLarge);
        }
        parents.push(hex::encode(
            std::str::from_utf8(value)
                .ok()
                .and_then(oid_from_bytes)
                .filter(|parent| parent.format() == id.format())
                .ok_or(ReadError::Malformed)?,
        ));
        lines.next();
    }
    let mut author = None;
    let mut committer = None;
    for line in lines {
        if let Some(value) = line.strip_prefix(b"author ").filter(|_| author.is_none()) {
            if value.len() > 4096 {
                return Err(ReadError::TooLarge);
            }
            author = Some(URL_SAFE_NO_PAD.encode(value));
        } else if let Some(value) = line
            .strip_prefix(b"committer ")
            .filter(|_| committer.is_none())
        {
            if value.len() > 4096 {
                return Err(ReadError::TooLarge);
            }
            committer = Some(URL_SAFE_NO_PAD.encode(value));
        }
    }
    let message = &body[split + 2..];
    Ok(Commit {
        oid: hex::encode(id),
        tree_oid: hex::encode(tree),
        parents,
        author_base64: author,
        committer_base64: committer,
        message_base64: URL_SAFE_NO_PAD.encode(&message[..message.len().min(4096)]),
        message_truncated: message.len() > 4096,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn commit_graph_headers_stop_before_identity_and_signature_headers() {
        let tree = hex::encode([1; 20]);
        let parent = hex::encode([2; 20]);
        let later = hex::encode([3; 20]);
        let body = format!(
            "tree {tree}\nparent {parent}\nauthor First <a@b> 1 +0000\nauthor Later <c@d> 2 +0000\nparent {later}\ntree {later}\ngpgsig signed\n parent {later}\ncommitter First <a@b> 1 +0000\n\nparent {later}\n"
        );
        let commit = parse_commit(crate::ObjectId::Sha1([4; 20]), body.as_bytes()).unwrap();
        assert_eq!((commit.tree_oid, commit.parents), (tree, vec![parent]));
        assert_eq!(
            URL_SAFE_NO_PAD
                .decode(commit.author_base64.unwrap())
                .unwrap(),
            b"First <a@b> 1 +0000"
        );
    }
}
