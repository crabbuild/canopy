//! Ordered additions/replacements between retained immutable roots. Equal
//! authenticated subtrees are skipped; no historical key set is materialized.
use super::*;

#[derive(Clone)]
enum Item<R: IndexRecord> {
    Node(NodeRef<R>),
    Record(R),
}
impl<R: IndexRecord> Item<R> {
    fn first(&self) -> R::Key {
        match self {
            Self::Node(n) => n.first_key.clone(),
            Self::Record(r) => r.first_key(),
        }
    }
    fn last(&self) -> R::Key {
        match self {
            Self::Node(n) => n.last_key.clone(),
            Self::Record(r) => r.last_key(),
        }
    }
}

/// Differences are defined by record first key and full record equality.
/// Deletions are omitted. A canceled/failed page poisons the cursor; restart
/// with the same root pair and the last *returned* record's first key.
pub struct RangeChanges<'a, R: IndexRecord> {
    index: &'a RangeIndex<R>,
    before: Vec<Item<R>>,
    after: Vec<Item<R>>,
    resume: Option<R::Key>,
    pending: Option<R>,
    initialized: bool,
    poisoned: bool,
}
impl<R: IndexRecord> RangeIndex<R> {
    pub fn changes(
        &self,
        before: Option<NodeRef<R>>,
        after: Option<NodeRef<R>>,
        resume: Option<R::Key>,
    ) -> Result<RangeChanges<'_, R>, IndexError> {
        for root in [&before, &after].into_iter().flatten() {
            root.validate(self.format)?;
        }
        if resume.as_ref().is_some_and(|key| !key.valid(self.format)) {
            return Err(IndexError::Integrity);
        }
        Ok(RangeChanges {
            index: self,
            before: before.into_iter().map(Item::Node).collect(),
            after: after.into_iter().map(Item::Node).collect(),
            resume,
            pending: None,
            initialized: false,
            poisoned: false,
        })
    }
}
impl<R: IndexRecord> RangeChanges<'_, R> {
    async fn expand(index: &RangeIndex<R>, stack: &mut Vec<Item<R>>) -> Result<(), IndexError> {
        let Some(Item::Node(reference)) = stack.pop() else {
            return Err(IndexError::Integrity);
        };
        let node = index.load(reference).await?;
        match &node.contents {
            Contents::Children(v) => stack.extend(v.iter().rev().cloned().map(Item::Node)),
            Contents::Runs(v) => stack.extend(v.iter().rev().cloned().map(Item::Record)),
        }
        // At most one path's unvisited siblings per level, including its leaf.
        if stack.len() > (usize::from(R::MAX_HEIGHT) + 1) * R::FANOUT {
            return Err(IndexError::Limit);
        }
        Ok(())
    }
    async fn next_inner(&mut self) -> Result<Option<R>, IndexError> {
        if !self.initialized {
            self.initialized = true;
            // Authenticate root context even when the entire pair is equal.
            for stack in [&self.before, &self.after] {
                if let Some(Item::Node(root)) = stack.last() {
                    self.index.validate_root(root.clone()).await?;
                }
            }
        }
        if let Some(record) = self.pending.take() {
            return Ok(Some(record));
        }
        loop {
            let Some(new) = self.after.last() else {
                return Ok(None);
            };
            if self.resume.as_ref().is_some_and(|key| new.last() <= *key) {
                self.after.pop();
                continue;
            }
            if let Some(old) = self.before.last() {
                if let (Item::Node(a), Item::Node(b)) = (old, new)
                    && a == b
                {
                    self.before.pop();
                    self.after.pop();
                    continue;
                }
                if old.last() < new.first() {
                    self.before.pop();
                    continue;
                }
                if old.first() <= new.last() {
                    match (old, new) {
                        (Item::Node(a), Item::Node(b)) if a.height >= b.height => {
                            Self::expand(self.index, &mut self.before).await?;
                            continue;
                        }
                        (_, Item::Node(_)) => {
                            Self::expand(self.index, &mut self.after).await?;
                            continue;
                        }
                        (Item::Node(_), _) => {
                            Self::expand(self.index, &mut self.before).await?;
                            continue;
                        }
                        (Item::Record(a), Item::Record(b)) => {
                            // Range overlap is allowed between generations. Matching
                            // is by first key, rather than by enclosing interval.
                            if a.first_key() < b.first_key() {
                                self.before.pop();
                                continue;
                            }
                            if a.first_key() == b.first_key() {
                                let equal = a == b;
                                self.before.pop();
                                if equal {
                                    self.after.pop();
                                    continue;
                                }
                            }
                        }
                    }
                }
            }
            match self.after.last() {
                Some(Item::Node(_)) => Self::expand(self.index, &mut self.after).await?,
                Some(Item::Record(_)) => {
                    let Some(Item::Record(record)) = self.after.pop() else {
                        return Err(IndexError::Integrity);
                    };
                    if self
                        .resume
                        .as_ref()
                        .is_none_or(|key| record.first_key() > *key)
                    {
                        return Ok(Some(record));
                    }
                }
                None => return Ok(None),
            }
        }
    }
    /// Bound both record count and encoded descriptor bytes. Native input bytes
    /// are separately admitted before downloads. Never advance over a record
    /// excluded by the byte limit, including when a short page is returned.
    pub async fn page(&mut self, count: usize, bytes: usize) -> Result<Vec<R>, IndexError> {
        if self.poisoned {
            return Err(IndexError::Integrity);
        }
        if count == 0 || count > R::FANOUT || bytes == 0 || bytes > R::NODE_BYTES as usize {
            return Err(IndexError::Limit);
        }
        self.poisoned = true;
        let result = self.page_inner(count, bytes).await;
        if result.is_ok() {
            self.poisoned = false;
        }
        result
    }
    async fn page_inner(&mut self, count: usize, bytes: usize) -> Result<Vec<R>, IndexError> {
        let mut page = Vec::with_capacity(count);
        let mut used = 0;
        while page.len() < count {
            let Some(record) = self.next_inner().await? else {
                break;
            };
            let mut encoder = BoundedEncoder::new(R::NODE_BYTES)?;
            record.encode_record(&mut encoder)?;
            let size = encoder.finish().len();
            if size > bytes - used {
                if page.is_empty() {
                    return Err(IndexError::Limit);
                }
                self.pending = Some(record);
                break;
            }
            used += size;
            page.push(record);
        }
        Ok(page)
    }
}
