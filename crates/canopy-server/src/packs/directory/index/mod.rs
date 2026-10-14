//! Shared persistent bounded-fanout tree for nonoverlapping directory ranges
//! and metadata source incarnation keys. Leaf types have distinct codec domains.
//! Every path update writes only changed nodes. A selected root never requires
//! walking earlier generations. Node integrity is not a publication certificate.

use super::*;
use cellule_runtime::codec::{BoundedDecoder, BoundedEncoder, CodecError};
use std::{
    collections::VecDeque,
    sync::atomic::{AtomicU64, Ordering},
};

pub(in crate::packs) mod codec;
pub(in crate::packs) mod record;
pub use record::{IndexKey, IndexRecord};
mod bulk;
mod changes;
mod cursor;
mod rewrite;
mod update;
mod visit;
pub use changes::RangeChanges;
pub use cursor::RangeCursor;
pub(crate) use visit::{ArtifactVisitor, IndexVisitor, WalkResult};

pub const FANOUT: usize = 128;
pub const NODE_BYTES: u32 = 64 << 10;
pub const MAX_HEIGHT: u8 = 7;
const CACHE_NODES: usize = 64;
pub const MAX_RANGE_RECORDS: usize = 4096;

#[derive(Debug, thiserror::Error)]
pub enum IndexError {
    #[error("directory metadata failed")]
    Metadata(#[from] MetadataError),
    #[error("catalog node transfer failed")]
    Artifact(#[from] canopy_object_storage::artifact::ArtifactError),
    #[error("catalog node encoding failed")]
    Codec(#[from] CodecError),
    #[error("catalog node or reference is invalid")]
    Integrity,
    #[error("catalog record keys or ranges overlap")]
    RangeOverlap,
    #[error("the expected catalog record is absent or changed")]
    Stale,
    #[error("range index exceeds its bounded fanout, height or byte limit")]
    Limit,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeRef<R: IndexRecord = StoredRun> {
    pub operation: [u8; 16],
    pub artifact: ArtifactDescriptor,
    pub height: u8,
    pub first_key: R::Key,
    pub last_key: R::Key,
    pub record_count: u64,
    /// Sum of record weights: represented objects for object/source records,
    /// live refs for ref-state records. Object counts include overlaps between
    /// shards or logical projections; never a unique-object coverage proof.
    pub object_count: u64,
}
impl<R: IndexRecord> Copy for NodeRef<R> where R::Key: Copy {}
impl<R: IndexRecord> NodeRef<R> {
    pub fn validate(&self, format: ObjectFormat) -> Result<(), IndexError> {
        if self.height > R::MAX_HEIGHT
            || !self.first_key.valid(format)
            || !self.last_key.valid(format)
            || self.first_key > self.last_key
            || self.record_count == 0
            || !R::valid_counts(self.record_count, self.object_count)
            || self.artifact.size == 0
            || self.artifact.size > u64::from(R::NODE_BYTES)
        {
            return Err(IndexError::Integrity);
        }
        Ok(())
    }
    fn key(&self) -> ArtifactKey {
        ArtifactKey {
            operation: self.operation,
            binding_digest: self.artifact.digest,
            kind: ArtifactKind::CatalogNode,
        }
    }
}

#[derive(Clone)]
enum Contents<R: IndexRecord = StoredRun> {
    Runs(Vec<R>),
    Children(Vec<NodeRef<R>>),
}
impl<R: IndexRecord> Contents<R> {
    fn len(&self) -> usize {
        match self {
            Self::Runs(v) => v.len(),
            Self::Children(v) => v.len(),
        }
    }
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    fn first(&self) -> Option<R::Key> {
        match self {
            Self::Runs(v) => v.first().map(|r| r.first_key()),
            Self::Children(v) => v.first().map(|r| r.first_key.clone()),
        }
    }
    fn last(&self) -> Option<R::Key> {
        match self {
            Self::Runs(v) => v.last().map(|r| r.last_key()),
            Self::Children(v) => v.last().map(|r| r.last_key.clone()),
        }
    }
    fn split(&mut self) -> Option<Self> {
        if self.len() <= R::FANOUT {
            return None;
        }
        let at = self.len() / 2;
        Some(match self {
            Self::Runs(v) => Self::Runs(v.split_off(at)),
            Self::Children(v) => Self::Children(v.split_off(at)),
        })
    }
    fn byte_parts(self, header: usize) -> Result<Vec<Self>, IndexError> {
        fn chunks<T>(
            items: Vec<T>,
            header: usize,
            limit: u32,
            fanout: usize,
            encode: impl Fn(&T, &mut BoundedEncoder) -> Result<(), CodecError>,
        ) -> Result<Vec<Vec<T>>, IndexError> {
            let capacity = (limit as usize)
                .checked_sub(header)
                .ok_or(IndexError::Limit)?;
            let mut groups = Vec::new();
            let mut group = Vec::new();
            let mut used = 0;
            for item in items {
                let mut e = BoundedEncoder::new(limit)?;
                encode(&item, &mut e)?;
                let size = e.finish().len();
                if size > capacity {
                    return Err(IndexError::Limit);
                }
                if !group.is_empty() && (used + size > capacity || group.len() == fanout) {
                    groups.push(std::mem::take(&mut group));
                    used = 0;
                }
                group.push(item);
                used += size;
            }
            if !group.is_empty() {
                groups.push(group);
            }
            if groups.len() < 2 {
                return Err(IndexError::Limit);
            }
            Ok(groups)
        }
        match self {
            Self::Runs(v) => Ok(chunks(v, header, R::NODE_BYTES, R::FANOUT, |r, e| {
                r.encode_record(e)
            })?
            .into_iter()
            .map(Self::Runs)
            .collect()),
            Self::Children(v) => Ok(chunks(v, header, R::NODE_BYTES, R::FANOUT, |r, e| {
                codec::reference(e, r.clone())
            })?
            .into_iter()
            .map(Self::Children)
            .collect()),
        }
    }
}
#[derive(Clone)]
struct Node<R: IndexRecord = StoredRun> {
    repository: [u8; 16],
    operation: [u8; 16],
    format: ObjectFormat,
    height: u8,
    contents: Contents<R>,
}
impl<R: IndexRecord> Node<R> {
    fn validate(&self) -> Result<(), IndexError> {
        if self.contents.is_empty()
            || self.contents.len() > R::FANOUT
            || self.height > R::MAX_HEIGHT
        {
            return Err(IndexError::Limit);
        }
        let mut previous = None;
        match &self.contents {
            Contents::Runs(runs) => {
                if self.height != 0 {
                    return Err(IndexError::Integrity);
                }
                for stored in runs {
                    stored.validate_record(self.repository, self.format)?;
                    if previous
                        .as_ref()
                        .is_some_and(|last| *last >= stored.first_key())
                    {
                        return Err(IndexError::Integrity);
                    }
                    previous = Some(stored.last_key());
                }
            }
            Contents::Children(children) => {
                if self.height == 0 {
                    return Err(IndexError::Integrity);
                }
                for child in children {
                    child.validate(self.format)?;
                    if child.height + 1 != self.height
                        || previous
                            .as_ref()
                            .is_some_and(|last| *last >= child.first_key)
                    {
                        return Err(IndexError::Integrity);
                    }
                    previous = Some(child.last_key.clone());
                }
            }
        }
        let (records, weight) = self.counts()?;
        if !R::valid_counts(records, weight) {
            return Err(IndexError::Limit);
        }
        Ok(())
    }
    fn counts(&self) -> Result<(u64, u64), IndexError> {
        let mut count = (0_u64, 0_u64);
        let mut add = |runs: u64, objects: u64| -> Result<(), IndexError> {
            count.0 = count.0.checked_add(runs).ok_or(IndexError::Limit)?;
            count.1 = count
                .1
                .checked_add(objects)
                .filter(|n| *n <= i64::MAX as u64)
                .ok_or(IndexError::Limit)?;
            Ok(())
        };
        match &self.contents {
            Contents::Runs(runs) => {
                for run in runs {
                    add(1, run.object_count())?;
                }
            }
            Contents::Children(children) => {
                for child in children {
                    add(child.record_count, child.object_count)?;
                }
            }
        }
        Ok(count)
    }
    fn reference(&self, artifact: ArtifactDescriptor) -> Result<NodeRef<R>, IndexError> {
        self.validate()?;
        let (record_count, object_count) = self.counts()?;
        Ok(NodeRef {
            operation: self.operation,
            artifact,
            height: self.height,
            first_key: self.contents.first().ok_or(IndexError::Integrity)?,
            last_key: self.contents.last().ok_or(IndexError::Integrity)?,
            record_count,
            object_count,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReadStats {
    pub loaded_nodes: u64,
    pub cache_hits: u64,
}
type CachedNode<R> = (NodeRef<R>, Arc<Node<R>>);
/// One repository/format's index client with a fixed-size node cache. Reader
/// admission and a retained generation pin are supplied by the service layer.
pub struct RangeIndex<R: IndexRecord = StoredRun> {
    store: Arc<ArtifactStore>,
    format: ObjectFormat,
    cache: Mutex<VecDeque<CachedNode<R>>>,
    loaded: AtomicU64,
    hits: AtomicU64,
}
impl<R: IndexRecord> RangeIndex<R> {
    pub fn new(store: Arc<ArtifactStore>, format: ObjectFormat) -> Self {
        Self {
            store,
            format,
            cache: Mutex::new(VecDeque::new()),
            loaded: AtomicU64::new(0),
            hits: AtomicU64::new(0),
        }
    }
    pub fn repository(&self) -> [u8; 16] {
        self.store.repository()
    }
    pub fn format(&self) -> ObjectFormat {
        self.format
    }
    pub fn stats(&self) -> ReadStats {
        ReadStats {
            loaded_nodes: self.loaded.load(Ordering::Relaxed),
            cache_hits: self.hits.load(Ordering::Relaxed),
        }
    }
    pub fn clear_cache(&self) -> Result<(), IndexError> {
        self.cache
            .lock()
            .map_err(|_| IndexError::Integrity)?
            .clear();
        Ok(())
    }
    /// Authenticates the selected root node and its exact summary/context.
    /// Descendant existence, inventory and closure remain verifier obligations.
    pub async fn validate_root(&self, root: NodeRef<R>) -> Result<(), IndexError> {
        self.load(root).await?;
        Ok(())
    }
    fn cache(&self, reference: NodeRef<R>, node: Arc<Node<R>>) -> Result<(), IndexError> {
        let mut cache = self.cache.lock().map_err(|_| IndexError::Integrity)?;
        if let Some(at) = cache.iter().position(|(cached, _)| cached == &reference) {
            cache.remove(at);
        }
        if cache.len() == CACHE_NODES {
            cache.pop_front();
        }
        cache.push_back((reference, node));
        Ok(())
    }
    async fn load(&self, reference: NodeRef<R>) -> Result<Arc<Node<R>>, IndexError> {
        reference.validate(self.format)?;
        {
            let mut cache = self.cache.lock().map_err(|_| IndexError::Integrity)?;
            if let Some(at) = cache.iter().position(|(cached, _)| cached == &reference) {
                let value = cache.remove(at).ok_or(IndexError::Integrity)?;
                let node = Arc::clone(&value.1);
                cache.push_back(value);
                self.hits.fetch_add(1, Ordering::Relaxed);
                return Ok(node);
            }
        }
        let mut read = self.store.read(reference.key(), reference.artifact).await?;
        let bytes = read.next().await?.ok_or(IndexError::Integrity)?;
        if read.next().await?.is_some() {
            return Err(IndexError::Integrity);
        }
        let node = Arc::new(Node::<R>::decode(&bytes)?);
        if node.repository != self.store.repository()
            || node.format != self.format
            || node.reference(reference.artifact)? != reference
        {
            return Err(IndexError::Integrity);
        }
        self.loaded.fetch_add(1, Ordering::Relaxed);
        self.cache(reference, Arc::clone(&node))?;
        Ok(node)
    }
    async fn persist(
        &self,
        operation: [u8; 16],
        height: u8,
        contents: Contents<R>,
    ) -> Result<NodeRef<R>, IndexError> {
        let node = Arc::new(Node {
            repository: self.store.repository(),
            operation,
            format: self.format,
            height,
            contents,
        });
        let bytes = node.encode()?;
        self.persist_encoded(node, bytes).await
    }
    async fn persist_encoded(
        &self,
        node: Arc<Node<R>>,
        bytes: Vec<u8>,
    ) -> Result<NodeRef<R>, IndexError> {
        let digest = *blake3::hash(&bytes).as_bytes();
        let key = ArtifactKey {
            operation: node.operation,
            binding_digest: digest,
            kind: ArtifactKind::CatalogNode,
        };
        let artifact = self
            .store
            .put(key, bytes.len() as u64, digest, &mut bytes.as_slice())
            .await?;
        let reference = node.reference(artifact)?;
        self.cache(reference.clone(), node)?;
        Ok(reference)
    }
    /// First run whose last OID is at least `oid`. It may start after `oid`, so
    /// callers use this to detect overlap even when both endpoints lie in gaps.
    pub async fn successor(
        &self,
        root: Option<NodeRef<R>>,
        oid: R::Key,
    ) -> Result<Option<R>, IndexError> {
        if !oid.valid(self.format) {
            return Ok(None);
        }
        let Some(mut reference) = root else {
            return Ok(None);
        };
        reference.validate(self.format)?;
        if oid > reference.last_key {
            return Ok(None);
        }
        loop {
            let node = self.load(reference).await?;
            match &node.contents {
                Contents::Runs(runs) => {
                    return Ok(runs
                        .get(runs.partition_point(|run| run.last_key() < oid))
                        .cloned());
                }
                Contents::Children(children) => {
                    let at = children.partition_point(|child| child.last_key < oid);
                    reference = children.get(at).ok_or(IndexError::Integrity)?.clone();
                }
            }
        }
    }
    /// All records intersecting an inclusive interval, including an enclosing
    /// record whose first key precedes the interval. Fail rather than truncate
    /// when the caller's bounded selection budget is exceeded. Seeking reads
    /// one path; subsequent cursor work is proportional to intersecting leaves.
    pub async fn overlapping(
        &self,
        root: Option<NodeRef<R>>,
        first: R::Key,
        last: R::Key,
        limit: usize,
    ) -> Result<Vec<R>, IndexError> {
        if !first.valid(self.format) || !last.valid(self.format) || first > last {
            return Err(IndexError::Integrity);
        }
        if limit > MAX_RANGE_RECORDS {
            return Err(IndexError::Limit);
        }
        let mut selected = Vec::new();
        let Some(start) = self
            .successor(root.clone(), first)
            .await?
            .filter(|run| run.first_key() <= last)
        else {
            return Ok(selected);
        };
        if limit == 0 {
            return Err(IndexError::Limit);
        }
        let after = start.first_key();
        selected.push(start);
        let mut cursor = self.cursor(root, Some(after))?;
        while let Some(run) = cursor.next().await? {
            if run.first_key() > last {
                break;
            }
            if selected.len() == limit {
                return Err(IndexError::Limit);
            }
            selected.push(run);
        }
        Ok(selected)
    }
    pub async fn find(
        &self,
        root: Option<NodeRef<R>>,
        oid: R::Key,
    ) -> Result<Option<R>, IndexError> {
        Ok(self
            .successor(root, oid.clone())
            .await?
            .filter(|run| run.first_key() <= oid))
    }
}

#[cfg(test)]
mod tests;
