//! Streaming ordered construction. One completed block is held behind the
//! active block at each level so small final tails can be balanced before reuse.
use super::*;

struct Level<R: IndexRecord> {
    pending: Option<Vec<NodeRef<R>>>,
    refs: Vec<NodeRef<R>>,
    bytes: usize,
}
impl<R: IndexRecord> Level<R> {
    fn empty(&self) -> bool {
        self.pending.is_none() && self.refs.is_empty()
    }
}
/// Drain at most two valid blocks, balancing a final underfilled tail by both
/// encoded bytes and fanout. The same helper handles leaves and child groups.
fn groups<T>(
    pending: Option<Vec<T>>,
    mut active: Vec<T>,
    header: usize,
    limit: u32,
    fanout: usize,
    encode: impl Fn(&T, &mut BoundedEncoder) -> Result<(), CodecError>,
) -> Result<Vec<Vec<T>>, IndexError> {
    let Some(mut pending) = pending else {
        return Ok(if active.is_empty() {
            Vec::new()
        } else {
            vec![active]
        });
    };
    if active.is_empty() {
        return Ok(vec![pending]);
    }
    let capacity = (limit as usize)
        .checked_sub(header)
        .ok_or(IndexError::Limit)?;
    let size = |item: &T| -> Result<usize, IndexError> {
        let mut e = BoundedEncoder::new(limit)?;
        encode(item, &mut e)?;
        Ok(e.finish().len())
    };
    let active_bytes = active
        .iter()
        .map(&size)
        .try_fold(0usize, |sum, n| Ok::<_, IndexError>(sum + n?))?;
    if active.len() * 2 >= fanout || active_bytes * 2 >= capacity {
        return Ok(vec![pending, active]);
    }
    pending.append(&mut active);
    let sizes = pending.iter().map(size).collect::<Result<Vec<_>, _>>()?;
    let total: usize = sizes.iter().sum();
    let mut prefix = 0;
    let mut best = None;
    for (at, size) in sizes.iter().enumerate().take(sizes.len() - 1) {
        prefix += size;
        let left = at + 1;
        let right = sizes.len() - left;
        if left > fanout || right > fanout || prefix > capacity || total - prefix > capacity {
            continue;
        }
        let left_load = (left as u64 * capacity as u64).max(prefix as u64 * fanout as u64);
        let right_load =
            (right as u64 * capacity as u64).max((total - prefix) as u64 * fanout as u64);
        let difference = left_load.abs_diff(right_load);
        if best.is_none_or(|(_, score)| difference < score) {
            best = Some((left, difference));
        }
    }
    let (at, _) = best.ok_or(IndexError::Limit)?;
    let second = pending.split_off(at);
    Ok(vec![pending, second])
}
pub(super) struct Builder<'a, R: IndexRecord> {
    index: &'a RangeIndex<R>,
    operation: [u8; 16],
    header: usize,
    levels: Vec<Level<R>>,
    pending_leaf: Option<Vec<R>>,
    leaf: Vec<R>,
    used: usize,
    last: Option<R::Key>,
}
impl<'a, R: IndexRecord> Builder<'a, R> {
    pub(super) fn new(index: &'a RangeIndex<R>, operation: [u8; 16]) -> Result<Self, IndexError> {
        let header = Node::<R> {
            repository: index.repository(),
            operation,
            format: index.format,
            height: 0,
            contents: Contents::Runs(Vec::new()),
        }
        .header_size()?;
        Ok(Self {
            index,
            operation,
            header,
            levels: Vec::new(),
            pending_leaf: None,
            leaf: Vec::new(),
            used: header,
            last: None,
        })
    }
    async fn emit_leaf(&mut self, records: Vec<R>) -> Result<(), IndexError> {
        let reference = self
            .index
            .persist(self.operation, 0, Contents::Runs(records))
            .await?;
        self.index
            .carry(self.operation, self.header, &mut self.levels, reference)
            .await
    }
    async fn flush_leaf(&mut self) -> Result<(), IndexError> {
        let groups = groups(
            self.pending_leaf.take(),
            std::mem::take(&mut self.leaf),
            self.header,
            R::NODE_BYTES,
            R::FANOUT,
            |record, e| record.encode_record(e),
        )?;
        self.used = self.header;
        for group in groups {
            self.emit_leaf(group).await?;
        }
        Ok(())
    }
    fn drain_level(&mut self, height: usize) -> Result<Vec<Vec<NodeRef<R>>>, IndexError> {
        let Some(level) = self.levels.get_mut(height) else {
            return Ok(Vec::new());
        };
        let groups = groups(
            level.pending.take(),
            std::mem::take(&mut level.refs),
            self.header,
            R::NODE_BYTES,
            R::FANOUT,
            |reference, e| codec::reference(e, reference.clone()),
        )?;
        level.bytes = self.header;
        Ok(groups)
    }
    pub(super) async fn record(&mut self, record: R) -> Result<(), IndexError> {
        record.validate_record(self.index.repository(), self.index.format)?;
        if self
            .last
            .as_ref()
            .is_some_and(|last| *last >= record.first_key())
        {
            return Err(IndexError::RangeOverlap);
        }
        let mut e = BoundedEncoder::new(R::NODE_BYTES)?;
        record.encode_record(&mut e)?;
        let size = e.finish().len();
        if self.header + size > R::NODE_BYTES as usize {
            return Err(IndexError::Limit);
        }
        if !self.leaf.is_empty()
            && (self.leaf.len() == R::FANOUT || self.used + size > R::NODE_BYTES as usize)
        {
            let full = std::mem::take(&mut self.leaf);
            if let Some(previous) = self.pending_leaf.replace(full) {
                self.emit_leaf(previous).await?;
            }
            self.used = self.header;
        }
        self.last = Some(record.last_key());
        self.leaf.push(record);
        self.used += size;
        Ok(())
    }
    /// Caller authenticated the root or its containing parent. Reuse itself
    /// does not issue a new descendant/existence certificate.
    pub(super) async fn subtree(&mut self, reference: NodeRef<R>) -> Result<(), IndexError> {
        reference.validate(self.index.format)?;
        if self
            .last
            .as_ref()
            .is_some_and(|last| *last >= reference.first_key)
        {
            return Err(IndexError::RangeOverlap);
        }
        self.flush_leaf().await?;
        // Lower groups are the later suffix of emitted content. Lift them
        // first so a reused higher subtree cannot precede that suffix.
        for height in 0..usize::from(reference.height) {
            for children in self.drain_level(height)? {
                let parent = self
                    .index
                    .persist(
                        self.operation,
                        (height + 1) as u8,
                        Contents::Children(children),
                    )
                    .await?;
                self.index
                    .carry(self.operation, self.header, &mut self.levels, parent)
                    .await?;
            }
        }
        self.last = Some(reference.last_key.clone());
        self.index
            .carry(self.operation, self.header, &mut self.levels, reference)
            .await
    }
    pub(super) async fn finish(mut self) -> Result<Option<NodeRef<R>>, IndexError> {
        self.flush_leaf().await?;
        let mut height = 0;
        while height < self.levels.len() {
            let mut groups = self.drain_level(height)?;
            if groups.len() == 1 && groups[0].len() == 1 && self.levels.iter().all(Level::empty) {
                return Ok(groups.pop().and_then(|mut children| children.pop()));
            }
            for children in groups {
                let parent_height = u8::try_from(height + 1).map_err(|_| IndexError::Limit)?;
                if parent_height > R::MAX_HEIGHT {
                    return Err(IndexError::Limit);
                }
                let reference = self
                    .index
                    .persist(self.operation, parent_height, Contents::Children(children))
                    .await?;
                self.index
                    .carry(self.operation, self.header, &mut self.levels, reference)
                    .await?;
            }
            height += 1;
        }
        Ok(None)
    }
}
impl<R: IndexRecord> RangeIndex<R> {
    async fn carry(
        &self,
        operation: [u8; 16],
        header: usize,
        levels: &mut Vec<Level<R>>,
        mut reference: NodeRef<R>,
    ) -> Result<(), IndexError> {
        loop {
            let height = reference.height as usize;
            if height > R::MAX_HEIGHT as usize {
                return Err(IndexError::Limit);
            }
            while levels.len() <= height {
                levels.push(Level {
                    pending: None,
                    refs: Vec::new(),
                    bytes: header,
                });
            }
            let mut e = BoundedEncoder::new(R::NODE_BYTES)?;
            codec::reference(&mut e, reference.clone())?;
            let size = e.finish().len();
            if header + size > R::NODE_BYTES as usize {
                return Err(IndexError::Limit);
            }
            let level = &mut levels[height];
            if !level.refs.is_empty()
                && (level.refs.len() == R::FANOUT || level.bytes + size > R::NODE_BYTES as usize)
            {
                let full = std::mem::take(&mut level.refs);
                let previous = level.pending.replace(full);
                level.refs.push(reference);
                level.bytes = header + size;
                let Some(children) = previous else {
                    return Ok(());
                };
                let parent_height = u8::try_from(height + 1).map_err(|_| IndexError::Limit)?;
                if parent_height > R::MAX_HEIGHT {
                    return Err(IndexError::Limit);
                }
                reference = self
                    .persist(operation, parent_height, Contents::Children(children))
                    .await?;
            } else {
                level.refs.push(reference);
                level.bytes += size;
                return Ok(());
            }
        }
    }
    pub async fn build_sorted<I>(
        &self,
        operation: [u8; 16],
        records: I,
    ) -> Result<Option<NodeRef<R>>, IndexError>
    where
        I: IntoIterator<Item = Result<R, IndexError>>,
        I::IntoIter: Send,
    {
        let mut builder = Builder::new(self, operation)?;
        for record in records {
            builder.record(record?).await?;
        }
        builder.finish().await
    }
}
