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
    positive_only: bool,
}
impl<R: IndexRecord> RangeIndex<R> {
    pub fn cursor(
        &self,
        root: Option<NodeRef<R>>,
        after: Option<R::Key>,
    ) -> Result<RangeCursor<'_, R>, IndexError> {
        self.make_cursor(root, after, false)
    }
    /// Skip zero-weight subtrees and records, while preserving ordered seek.
    pub fn positive_cursor(
        &self,
        root: Option<NodeRef<R>>,
        after: Option<R::Key>,
    ) -> Result<RangeCursor<'_, R>, IndexError> {
        self.make_cursor(root, after, true)
    }
    fn make_cursor(
        &self,
        root: Option<NodeRef<R>>,
        after: Option<R::Key>,
        positive_only: bool,
    ) -> Result<RangeCursor<'_, R>, IndexError> {
        if let Some(root) = &root {
            root.validate(self.format)?;
        }
        if after.as_ref().is_some_and(|oid| !oid.valid(self.format)) {
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
            positive_only,
        })
    }
}
impl<R: IndexRecord> RangeCursor<'_, R> {
    async fn descend(&mut self, mut reference: NodeRef<R>, seek: bool) -> Result<(), IndexError> {
        loop {
            let weight = reference.object_count;
            let node = self.index.load(reference).await?;
            if self.positive_only && weight == 0 {
                self.leaf = None;
                return Ok(());
            }
            match &node.contents {
                Contents::Runs(runs) => {
                    self.position = if seek {
                        self.after
                            .as_ref()
                            .map_or(0, |oid| runs.partition_point(|run| run.first_key() <= *oid))
                    } else {
                        0
                    };
                    self.leaf = Some(node);
                    return Ok(());
                }
                Contents::Children(children) => {
                    let at = if seek {
                        self.after.as_ref().map_or(0, |oid| {
                            children
                                .partition_point(|child| child.last_key < *oid)
                                .min(children.len() - 1)
                        })
                    } else {
                        0
                    };
                    let Some(at) = (at..children.len())
                        .find(|at| !self.positive_only || children[*at].object_count != 0)
                    else {
                        self.leaf = None;
                        return Ok(());
                    };
                    reference = children[at].clone();
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
            if let Some(next) = (at + 1..children.len())
                .find(|at| !self.positive_only || children[*at].object_count != 0)
            {
                let reference = children[next].clone();
                self.path.push((node, next));
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
            if let Some(root) = self.root.clone()
                && self.after.as_ref().is_none_or(|oid| *oid < root.last_key)
            {
                self.descend(root, true).await?;
            }
        }
        loop {
            let Some(leaf) = &self.leaf else {
                // A seek may exhaust a positive subtree after its final live
                // child. Its ancestors can still have later live siblings.
                if self.advance_leaf().await? {
                    continue;
                }
                return Ok(None);
            };
            let Contents::Runs(runs) = &leaf.contents else {
                return Err(IndexError::Integrity);
            };
            if let Some(run) = runs.get(self.position) {
                self.position += 1;
                if !self.positive_only || run.object_count() != 0 {
                    return Ok(Some(run.clone()));
                }
                continue;
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
