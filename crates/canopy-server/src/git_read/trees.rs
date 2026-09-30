use super::*;
type Changes = BTreeMap<Vec<u8>, (Option<Node>, Option<Node>)>;

impl Reader {
    pub(super) async fn tree(&mut self, oid: Oid) -> Result<BTreeMap<Vec<u8>, Node>, ReadError> {
        let body = self.body(oid, ObjectKind::Tree).await?;
        let remaining = MAX_TREE_ENTRIES.saturating_sub(self.entries);
        let admission = Arc::clone(&self.admission);
        let entries = tokio::task::spawn_blocking(move || {
            let _admission = admission;
            parse_tree(oid.format(), &body, remaining)
        })
        .await??;
        self.entries += entries.len();
        Ok(entries)
    }
    pub(super) async fn changes(&mut self, before: Oid, after: Oid) -> Result<Changes, ReadError> {
        let mut changes = Changes::new();
        let mut pending = vec![(
            Vec::new(),
            Some(Node {
                mode: 0o040000,
                oid: before,
            }),
            Some(Node {
                mode: 0o040000,
                oid: after,
            }),
            0,
        )];
        let mut queued_path_bytes = 0;
        while let Some((path, before, after, depth)) = pending.pop() {
            queued_path_bytes -= path.len();
            if before == after {
                continue;
            }
            if depth > MAX_DEPTH || path.len() > MAX_PATH {
                return Err(ReadError::TooLarge);
            }
            let before_tree = before.is_some_and(Node::is_tree);
            let after_tree = after.is_some_and(Node::is_tree);
            if !before_tree && !after_tree {
                changes.insert(path, (before, after));
            } else {
                if before.is_some() && !before_tree {
                    changes.insert(path.clone(), (before, None));
                }
                if after.is_some() && !after_tree {
                    changes.insert(path.clone(), (None, after));
                }
                let old = if let Some(node) = before.filter(|n| n.is_tree()) {
                    self.tree(node.oid).await?
                } else {
                    BTreeMap::new()
                };
                let mut new = if let Some(node) = after.filter(|n| n.is_tree()) {
                    self.tree(node.oid).await?
                } else {
                    BTreeMap::new()
                };
                for (name, old) in old {
                    let after = new.remove(&name);
                    let child = join(&path, &name)?;
                    queued_path_bytes += child.len();
                    if queued_path_bytes > 8 * 1024 * 1024 {
                        return Err(ReadError::TooLarge);
                    }
                    pending.push((child, Some(old), after, depth + 1));
                }
                for (name, new) in new {
                    let child = join(&path, &name)?;
                    queued_path_bytes += child.len();
                    if queued_path_bytes > 8 * 1024 * 1024 {
                        return Err(ReadError::TooLarge);
                    }
                    pending.push((child, None, Some(new), depth + 1));
                }
            }
            if changes.len() > MAX_CHANGED_FILES {
                return Err(ReadError::TooLarge);
            }
        }
        Ok(changes)
    }
    pub(super) async fn resolve(
        &mut self,
        mut root: Oid,
        path: &[u8],
    ) -> Result<Option<Node>, ReadError> {
        if path.is_empty() {
            return Ok(Some(Node {
                mode: 0o040000,
                oid: root,
            }));
        }
        let mut components = path.split(|byte| *byte == b'/').peekable();
        let mut depth = 0;
        while let Some(component) = components.next() {
            depth += 1;
            if depth > MAX_DEPTH {
                return Err(ReadError::TooLarge);
            }
            let entries = self.tree(root).await?;
            let Some(entry) = entries.get(component).copied() else {
                return Ok(None);
            };
            if components.peek().is_none() {
                return Ok(Some(entry));
            }
            if !entry.is_tree() {
                return Ok(None);
            }
            root = entry.oid;
        }
        Ok(None)
    }
}
pub(super) fn join(prefix: &[u8], name: &[u8]) -> Result<Vec<u8>, ReadError> {
    if prefix.len() + name.len() + usize::from(!prefix.is_empty()) > MAX_PATH {
        return Err(ReadError::TooLarge);
    }
    let mut path = Vec::with_capacity(prefix.len() + name.len() + 1);
    path.extend_from_slice(prefix);
    if !prefix.is_empty() {
        path.push(b'/');
    }
    path.extend_from_slice(name);
    Ok(path)
}
fn parse_tree(
    format: crate::ObjectFormat,
    mut body: &[u8],
    limit: usize,
) -> Result<BTreeMap<Vec<u8>, Node>, ReadError> {
    let mut entries = BTreeMap::new();
    while !body.is_empty() {
        if entries.len() == limit {
            return Err(ReadError::TooLarge);
        }
        let space = body
            .iter()
            .position(|byte| *byte == b' ')
            .ok_or(ReadError::Malformed)?;
        if space == 0
            || !body[..space]
                .iter()
                .all(|byte| (b'0'..=b'7').contains(byte))
        {
            return Err(ReadError::Malformed);
        }
        let mode = std::str::from_utf8(&body[..space])
            .ok()
            .and_then(|s| u32::from_str_radix(s, 8).ok())
            .ok_or(ReadError::Malformed)?;
        if mode > 0o177777 || !matches!(mode & 0o170000, 0o040000 | 0o100000 | 0o120000 | 0o160000)
        {
            return Err(ReadError::Malformed);
        }
        // Git tree walking canonicalizes permission bits: only the owner's
        // executable bit distinguishes regular-file modes in a diff.
        let mode = match mode & 0o170000 {
            0o100000 if mode & 0o100 != 0 => 0o100755,
            0o100000 => 0o100644,
            kind => kind,
        };
        let rest = &body[space + 1..];
        let nul = rest
            .iter()
            .position(|byte| *byte == 0)
            .ok_or(ReadError::Malformed)?;
        let name = &rest[..nul];
        if name.is_empty() || name == b"." || name == b".." || name.contains(&b'/') {
            return Err(ReadError::Malformed);
        }
        if name.len() > MAX_PATH {
            return Err(ReadError::TooLarge);
        }
        let oid: Oid = rest
            .get(nul + 1..nul + 1 + format.bytes())
            .ok_or(ReadError::Malformed)?
            .try_into()
            .map_err(|_| ReadError::Malformed)?;
        if oid.is_zero() || entries.insert(name.to_vec(), Node { mode, oid }).is_some() {
            return Err(ReadError::Malformed);
        }
        body = &rest[nul + 1 + format.bytes()..];
    }
    Ok(entries)
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn tree_modes_match_git_canonical_permissions() {
        for (stored, expected) in [
            ("100664", 0o100644),
            ("100775", 0o100755),
            ("120777", 0o120000),
            ("40755", 0o040000),
            ("160755", 0o160000),
        ] {
            let mut bytes = format!("{stored} entry\0").into_bytes();
            bytes.extend_from_slice(&[1; 20]);
            assert_eq!(
                parse_tree(crate::ObjectFormat::Sha1, &bytes, 1).unwrap()[b"entry".as_slice()].mode,
                expected
            );
        }
    }
    #[test]
    fn tree_names_remain_bytes_and_duplicate_or_oversized_entries_fail() {
        let mut bytes = b"100644 \xff\0".to_vec();
        bytes.extend_from_slice(&[1; 20]);
        assert!(
            parse_tree(crate::ObjectFormat::Sha1, &bytes, 1)
                .unwrap()
                .contains_key(b"\xff".as_slice())
        );
        assert!(matches!(
            parse_tree(crate::ObjectFormat::Sha1, &bytes, 0),
            Err(ReadError::TooLarge)
        ));
        bytes.extend(bytes.clone());
        assert!(matches!(
            parse_tree(crate::ObjectFormat::Sha1, &bytes, 2),
            Err(ReadError::Malformed)
        ));
    }
}
