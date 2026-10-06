//! Typed physical-artifact traversal with bounded depth and shared-node reuse.
use super::*;

pub(crate) type WalkResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[async_trait::async_trait]
pub(crate) trait ArtifactVisitor: Send {
    /// False only after this exact key/descriptor was successfully retained.
    /// Callers use admitted disk to bound physical-artifact deduplication.
    async fn artifact(
        &mut self,
        key: ArtifactKey,
        descriptor: ArtifactDescriptor,
    ) -> WalkResult<bool>;
}

#[async_trait::async_trait]
pub(crate) trait IndexVisitor<R: IndexRecord>: ArtifactVisitor {
    async fn record(&mut self, record: &R) -> WalkResult<()>;
}

impl<R: IndexRecord> RangeIndex<R> {
    pub(crate) async fn visit<V: IndexVisitor<R> + ?Sized>(
        &self,
        root: NodeRef<R>,
        visitor: &mut V,
    ) -> WalkResult<()> {
        root.validate(self.format)?;
        let limit = (R::FANOUT - 1) * (usize::from(R::MAX_HEIGHT) + 1) + 1;
        let mut pending = vec![root];
        while let Some(reference) = pending.pop() {
            // Validate this reference's complete bounds even if its physical
            // node was previously copied through another generation/root.
            let node = self.load(reference.clone()).await?;
            if !visitor
                .artifact(reference.key(), reference.artifact)
                .await?
            {
                continue;
            }
            match &node.contents {
                Contents::Runs(records) => {
                    for record in records {
                        visitor.record(record).await?;
                    }
                }
                Contents::Children(children) => {
                    if pending
                        .len()
                        .checked_add(children.len())
                        .is_none_or(|n| n > limit)
                    {
                        return Err(IndexError::Limit.into());
                    }
                    pending.extend(children.iter().rev().cloned());
                }
            }
        }
        Ok(())
    }
}
