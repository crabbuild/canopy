//! Conditional sorted upserts. Only affected paths are loaded; unchanged
//! authenticated child references join the shared streaming builder directly.
use super::*;
use bulk::Builder;

struct Changes<I, R: IndexRecord> {
    input: I,
    pending: Option<R>,
    last: Option<R::Key>,
    done: bool,
    repository: [u8; 16],
    format: ObjectFormat,
}
impl<I: Iterator<Item = Result<R, IndexError>>, R: IndexRecord> Changes<I, R> {
    fn peek(&mut self) -> Result<Option<&R>, IndexError> {
        if self.pending.is_none() && !self.done {
            if let Some(record) = self.input.next() {
                let record = record?;
                record.validate_record(self.repository, self.format)?;
                if self
                    .last
                    .as_ref()
                    .is_some_and(|last| *last >= record.first_key())
                {
                    return Err(IndexError::RangeOverlap);
                }
                self.last = Some(record.last_key());
                self.pending = Some(record);
            } else {
                self.done = true;
            }
        }
        Ok(self.pending.as_ref())
    }
    fn within(&mut self, upper: Option<&R::Key>) -> Result<bool, IndexError> {
        Ok(self
            .peek()?
            .is_some_and(|record| upper.is_none_or(|upper| record.first_key() <= *upper)))
    }
    fn pop(&mut self) -> Result<R, IndexError> {
        self.peek()?;
        self.pending.take().ok_or(IndexError::Integrity)
    }
}
impl<R: IndexRecord> RangeIndex<R> {
    /// Upsert sorted exact-key/range records through one streaming rewrite.
    /// Ref callers validate every expectation/namespace against this immutable
    /// base first. This structural API confers no publication authority.
    pub async fn upsert_sorted<I>(
        &self,
        root: Option<NodeRef<R>>,
        operation: [u8; 16],
        records: I,
    ) -> Result<Option<NodeRef<R>>, IndexError>
    where
        I: IntoIterator<Item = Result<R, IndexError>>,
        I::IntoIter: Send,
    {
        let Some(root) = root else {
            return self.build_sorted(operation, records).await;
        };
        self.validate_root(root.clone()).await?;
        let mut changes = Changes {
            input: records.into_iter(),
            pending: None,
            last: None,
            done: false,
            repository: self.repository(),
            format: self.format,
        };
        if changes.peek()?.is_none() {
            return Ok(Some(root));
        }
        let mut builder = Builder::new(self, operation)?;
        self.rewrite_walk(root, None, &mut changes, &mut builder)
            .await?;
        if changes.peek()?.is_some() {
            return Err(IndexError::Integrity);
        }
        builder.finish().await
    }
    async fn rewrite_walk<I: Iterator<Item = Result<R, IndexError>> + Send>(
        &self,
        root: NodeRef<R>,
        upper: Option<R::Key>,
        changes: &mut Changes<I, R>,
        builder: &mut Builder<'_, R>,
    ) -> Result<(), IndexError> {
        if !changes.within(upper.as_ref())? {
            return builder.subtree(root).await;
        }
        let node = self.load(root).await?;
        match &node.contents {
            Contents::Runs(records) => {
                for old in records {
                    while changes.within(upper.as_ref())?
                        && changes
                            .peek()?
                            .is_some_and(|new| new.first_key() < old.first_key())
                    {
                        builder.record(changes.pop()?).await?;
                    }
                    if changes.within(upper.as_ref())?
                        && changes
                            .peek()?
                            .is_some_and(|new| new.first_key() == old.first_key())
                    {
                        let new = changes.pop()?;
                        if new.last_key() != old.last_key() {
                            return Err(IndexError::RangeOverlap);
                        }
                        builder.record(new).await?;
                    } else {
                        builder.record(old.clone()).await?;
                    }
                }
                while changes.within(upper.as_ref())? {
                    builder.record(changes.pop()?).await?;
                }
            }
            Contents::Children(children) => {
                for (at, child) in children.iter().enumerate() {
                    // Route gaps to the next child; the last child inherits the
                    // enclosing interval so insertions can extend its old fence.
                    let bound = if at + 1 == children.len() {
                        upper.clone()
                    } else {
                        Some(child.last_key.clone())
                    };
                    Box::pin(self.rewrite_walk(child.clone(), bound, changes, builder)).await?;
                }
            }
        }
        Ok(())
    }
}
