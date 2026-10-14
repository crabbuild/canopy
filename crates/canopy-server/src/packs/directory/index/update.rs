use super::*;

impl<R: IndexRecord> RangeIndex<R> {
    async fn path(
        &self,
        root: NodeRef<R>,
        oid: R::Key,
    ) -> Result<(Vec<(Arc<Node<R>>, usize)>, Arc<Node<R>>), IndexError> {
        let mut path = Vec::new();
        let mut reference = root;
        loop {
            let node = self.load(reference).await?;
            match &node.contents {
                Contents::Runs(_) => return Ok((path, node)),
                Contents::Children(children) => {
                    let at = children
                        .partition_point(|child| child.last_key < oid)
                        .min(children.len() - 1);
                    reference = children[at].clone();
                    path.push((node, at));
                }
            }
        }
    }
    async fn persist_split(
        &self,
        operation: [u8; 16],
        height: u8,
        contents: Contents<R>,
    ) -> Result<Vec<NodeRef<R>>, IndexError> {
        let mut pending = vec![contents];
        let mut references = Vec::new();
        while let Some(mut contents) = pending.pop() {
            if let Some(second) = contents.split() {
                pending.push(second);
                pending.push(contents);
                continue;
            }
            let node = Arc::new(Node {
                repository: self.repository(),
                operation,
                format: self.format,
                height,
                contents,
            });
            match node.encode() {
                Ok(bytes) => references.push(self.persist_encoded(node, bytes).await?),
                Err(IndexError::Codec(CodecError::Limit)) => {
                    let header = node.header_size()?;
                    let node = Arc::try_unwrap(node).map_err(|_| IndexError::Integrity)?;
                    pending.extend(node.contents.byte_parts(header)?.into_iter().rev());
                }
                Err(error) => return Err(error),
            }
        }
        Ok(references)
    }
    /// Replace one exact same-key incarnation in one path copy. This is a
    /// structural CAS; current authority and root publication are separate.
    pub async fn replace(
        &self,
        root: NodeRef<R>,
        operation: [u8; 16],
        expected: R,
        replacement: R,
    ) -> Result<NodeRef<R>, IndexError> {
        expected.validate_record(self.repository(), self.format)?;
        replacement.validate_record(self.repository(), self.format)?;
        if expected.first_key() != replacement.first_key()
            || expected.last_key() != replacement.last_key()
        {
            return Err(IndexError::RangeOverlap);
        }
        let (path, leaf) = self.path(root.clone(), expected.first_key()).await?;
        let Contents::Runs(mut runs) = leaf.contents.clone() else {
            return Err(IndexError::Integrity);
        };
        let at = runs.partition_point(|run| run.first_key() < expected.first_key());
        if runs.get(at) != Some(&expected) {
            return Err(IndexError::Stale);
        }
        if expected == replacement {
            return Ok(root);
        }
        runs[at] = replacement;
        let mut changed = self
            .persist_split(operation, 0, Contents::Runs(runs))
            .await?;
        for (parent, at) in path.into_iter().rev() {
            let Contents::Children(mut children) = parent.contents.clone() else {
                return Err(IndexError::Integrity);
            };
            children.splice(at..=at, changed);
            changed = self
                .persist_split(operation, parent.height, Contents::Children(children))
                .await?;
        }
        if changed.len() == 1 {
            return Ok(changed.remove(0));
        }
        let height = root
            .height
            .checked_add(1)
            .filter(|height| *height <= R::MAX_HEIGHT)
            .ok_or(IndexError::Limit)?;
        self.persist(operation, height, Contents::Children(changed))
            .await
    }
    /// Structural path-copy insertion. A trusted compaction/publication verifier
    /// still certifies the run's headers, dependencies and artifact existence.
    pub async fn insert(
        &self,
        root: Option<NodeRef<R>>,
        operation: [u8; 16],
        run: R,
    ) -> Result<NodeRef<R>, IndexError> {
        run.validate_record(self.store.repository(), self.format)?;
        if let Some(existing) = self.successor(root.clone(), run.first_key()).await? {
            if existing == run {
                return root.ok_or(IndexError::Integrity);
            }
            if existing.first_key() <= run.last_key() {
                return Err(IndexError::RangeOverlap);
            }
        }
        let Some(root) = root else {
            return self.persist(operation, 0, Contents::Runs(vec![run])).await;
        };
        let (path, leaf) = self.path(root.clone(), run.first_key()).await?;
        let Contents::Runs(mut runs) = leaf.contents.clone() else {
            return Err(IndexError::Integrity);
        };
        let at = runs.partition_point(|existing| existing.first_key() < run.first_key());
        runs.insert(at, run);
        let mut changed = self
            .persist_split(operation, 0, Contents::Runs(runs))
            .await?;
        for (parent, at) in path.into_iter().rev() {
            let Contents::Children(mut children) = parent.contents.clone() else {
                return Err(IndexError::Integrity);
            };
            children.splice(at..=at, changed);
            changed = self
                .persist_split(operation, parent.height, Contents::Children(children))
                .await?;
        }
        if changed.len() == 1 {
            return Ok(changed[0].clone());
        }
        let height = root
            .height
            .checked_add(1)
            .filter(|height| *height <= R::MAX_HEIGHT)
            .ok_or(IndexError::Limit)?;
        self.persist(operation, height, Contents::Children(changed))
            .await
    }
    /// Remove exactly the expected incarnation. Old roots remain immutable and
    /// usable for retained readers until the service releases their generation.
    pub async fn remove(
        &self,
        root: Option<NodeRef<R>>,
        operation: [u8; 16],
        expected: R,
    ) -> Result<Option<NodeRef<R>>, IndexError> {
        expected.validate_record(self.store.repository(), self.format)?;
        let root = root.ok_or(IndexError::Stale)?;
        let (path, leaf) = self.path(root, expected.first_key()).await?;
        let Contents::Runs(mut runs) = leaf.contents.clone() else {
            return Err(IndexError::Integrity);
        };
        let at = runs.partition_point(|run| run.first_key() < expected.first_key());
        if runs.get(at) != Some(&expected) {
            return Err(IndexError::Stale);
        }
        runs.remove(at);
        let mut changed = if runs.is_empty() {
            None
        } else {
            Some(self.persist(operation, 0, Contents::Runs(runs)).await?)
        };
        let depth = path.len();
        for (n, (parent, at)) in path.into_iter().rev().enumerate() {
            let Contents::Children(mut children) = parent.contents.clone() else {
                return Err(IndexError::Integrity);
            };
            children.splice(at..=at, changed);
            changed = if children.is_empty() {
                None
            } else if n + 1 == depth && children.len() == 1 {
                Some(children[0].clone())
            } else {
                Some(
                    self.persist(operation, parent.height, Contents::Children(children))
                        .await?,
                )
            };
        }
        // A retained unary subtree may become the root after removal. Collapse
        // it without changing any run or rewriting all remaining descriptors.
        while let Some(reference) = changed.clone() {
            if reference.height == 0 {
                break;
            }
            let node = self.load(reference).await?;
            let Contents::Children(children) = &node.contents else {
                return Err(IndexError::Integrity);
            };
            if children.len() != 1 {
                break;
            }
            changed = Some(children[0].clone());
        }
        Ok(changed)
    }
}
