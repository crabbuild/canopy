use super::*;

/// Constructed only from a complete physical witness. Invalid or interrupted
/// partitions cannot finish; retained state is independent of shard count.
pub struct PhysicalPartition {
    native: NativePackDescriptor,
    expected_count: u32,
    expected_digest: [u8; 32],
    next: u32,
    count: u32,
    chain: [u8; 32],
    last: Option<ObjectId>,
    failed: bool,
}
impl PhysicalPartition {
    pub(super) fn new(
        native: NativePackDescriptor,
        expected_count: u32,
        expected_digest: [u8; 32],
    ) -> Self {
        Self {
            native,
            expected_count,
            expected_digest,
            next: 0,
            count: 0,
            chain: seed(native),
            last: None,
            failed: false,
        }
    }
    pub fn add(&mut self, segment: SegmentDescriptor) -> Result<(), PhysicalError> {
        if self.failed {
            return Err(PhysicalError::Integrity);
        }
        self.failed = true;
        let identity = segment.identity;
        if identity != shard_identity(self.native, self.next, identity.object_count)
            || identity.object_count == 0
            || segment.first_oid.format() != self.native.format
            || segment.last_oid.format() != self.native.format
            || segment.first_oid.is_zero()
            || segment.first_oid > segment.last_oid
            || self.last.is_some_and(|oid| oid >= segment.first_oid)
        {
            return Err(PhysicalError::Integrity);
        }
        self.next = self
            .next
            .checked_add(identity.object_count)
            .filter(|end| *end <= self.native.object_count)
            .ok_or(PhysicalError::Integrity)?;
        self.chain = fold_shard(self.chain, self.count, segment);
        self.count = self.count.checked_add(1).ok_or(PhysicalError::Integrity)?;
        self.last = Some(segment.last_oid);
        self.failed = false;
        Ok(())
    }
    pub fn finish(self) -> Result<(), PhysicalError> {
        if self.failed
            || self.next != self.native.object_count
            || self.count != self.expected_count
            || self.chain != self.expected_digest
        {
            return Err(PhysicalError::Integrity);
        }
        Ok(())
    }
}
