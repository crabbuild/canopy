use super::*;

/// Forward cursor retaining only one bounded-height path. A canceled/failed
/// `next` poisons the cursor; restart from a known last returned OID instead.
pub struct RangeCursor<'a, R: IndexRecord = StoredRun> {
    index: &'a RangeIndex<R>,
    root: Option<NodeRef<R>>,
    after: Option<R::Key>,
    path: Vec<(Arc<Node<R>>, usize)>,
    leaf: Option<Arc<Node<R>>>,
    position: usize,
    initialized: bool,
    poisoned: bool,
}
impl<R: IndexRecord> RangeIndex<R> {
    pub fn cursor(
        &self,
        root: Option<NodeRef<R>>,
        after: Option<R::Key>,
    ) -> Result<RangeCursor<'_, R>, IndexError> {
        if let Some(root) = root {
            root.validate(self.format)?;
        }
        if after.is_some_and(|oid| !oid.valid(self.format)) {
            return Err(IndexError::Integrity);
        }
        Ok(RangeCursor {
            index: self,
            root,
            after,
            path: Vec::new(),
            leaf: None,
            position: 0,
            initialized: false,
            poisoned: false,
        })
    }
}
impl<R: IndexRecord> RangeCursor<'_, R> {
    async fn descend(&mut self, mut reference: NodeRef<R>, seek: bool) -> Result<(), IndexError> {
        loop {
            let node = self.index.load(reference).await?;
            match &node.contents {
                Contents::Runs(runs) => {
                    self.position = if seek {
                        self.after
                            .map_or(0, |oid| runs.partition_point(|run| run.first_key() <= oid))
                    } else {
                        0
                    };
                    self.leaf = Some(node);
                    return Ok(());
                }
                Contents::Children(children) => {
                    let at = if seek {
                        self.after.map_or(0, |oid| {
                            children
                                .partition_point(|child| child.last_key < oid)
                                .min(children.len() - 1)
                        })
                    } else {
                        0
                    };
                    reference = children[at];
                    self.path.push((node, at));
                }
            }
        }
    }
    async fn advance_leaf(&mut self) -> Result<bool, IndexError> {
        while let Some((node, at)) = self.path.pop() {
            let Contents::Children(children) = &node.contents else {
                return Err(IndexError::Integrity);
            };
            if let Some(reference) = children.get(at + 1) {
                let reference = *reference;
                self.path.push((node, at + 1));
                self.descend(reference, false).await?;
                return Ok(true);
            }
        }
        self.leaf = None;
        Ok(false)
    }
    async fn next_inner(&mut self) -> Result<Option<R>, IndexError> {
        if !self.initialized {
            self.initialized = true;
            if let Some(root) = self.root
                && self.after.is_none_or(|oid| oid < root.last_key)
            {
                self.descend(root, true).await?;
            }
        }
        loop {
            let Some(leaf) = &self.leaf else {
                return Ok(None);
            };
            let Contents::Runs(runs) = &leaf.contents else {
                return Err(IndexError::Integrity);
            };
            if let Some(run) = runs.get(self.position) {
                self.position += 1;
                return Ok(Some(*run));
            }
            if !self.advance_leaf().await? {
                return Ok(None);
            }
        }
    }
    pub async fn next(&mut self) -> Result<Option<R>, IndexError> {
        if self.poisoned {
            return Err(IndexError::Integrity);
        }
        self.poisoned = true;
        let result = self.next_inner().await;
        if result.is_ok() {
            self.poisoned = false;
        }
        result
    }
}
