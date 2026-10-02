//! Streaming sorted construction: one leaf and one bounded group per height,
//! avoiding a path upload for every member of a large initial inventory.
use super::*;

struct Level<R: IndexRecord> {
    refs: Vec<NodeRef<R>>,
    bytes: usize,
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
                let children = std::mem::take(&mut level.refs);
                level.refs.push(reference);
                level.bytes = header + size;
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
        let header = Node::<R> {
            repository: self.repository(),
            operation,
            format: self.format,
            height: 0,
            contents: Contents::Runs(Vec::new()),
        }
        .header_size()?;
        let mut levels = Vec::new();
        let mut leaf = Vec::new();
        let mut used = header;
        let mut last = None;
        for record in records {
            let record = record?;
            record.validate_record(self.repository(), self.format)?;
            if last
                .as_ref()
                .is_some_and(|last| *last >= record.first_key())
            {
                return Err(IndexError::RangeOverlap);
            }
            last = Some(record.last_key());
            let mut e = BoundedEncoder::new(R::NODE_BYTES)?;
            record.encode_record(&mut e)?;
            let size = e.finish().len();
            if header + size > R::NODE_BYTES as usize {
                return Err(IndexError::Limit);
            }
            if !leaf.is_empty() && (leaf.len() == R::FANOUT || used + size > R::NODE_BYTES as usize)
            {
                let reference = self
                    .persist(operation, 0, Contents::Runs(std::mem::take(&mut leaf)))
                    .await?;
                self.carry(operation, header, &mut levels, reference)
                    .await?;
                used = header;
            }
            leaf.push(record);
            used += size;
        }
        if !leaf.is_empty() {
            let reference = self.persist(operation, 0, Contents::Runs(leaf)).await?;
            self.carry(operation, header, &mut levels, reference)
                .await?;
        }
        let mut height = 0;
        while height < levels.len() {
            let children = std::mem::take(&mut levels[height].refs);
            levels[height].bytes = header;
            if !children.is_empty() {
                if children.len() == 1 && levels.iter().all(|level| level.refs.is_empty()) {
                    return Ok(children.into_iter().next());
                }
                let parent_height = u8::try_from(height + 1).map_err(|_| IndexError::Limit)?;
                if parent_height > R::MAX_HEIGHT {
                    return Err(IndexError::Limit);
                }
                let reference = self
                    .persist(operation, parent_height, Contents::Children(children))
                    .await?;
                self.carry(operation, header, &mut levels, reference)
                    .await?;
            }
            height += 1;
        }
        Ok(None)
    }
}
