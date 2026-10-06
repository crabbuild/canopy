//! Selected authenticated packed extents in a private native decoder workspace.
use super::*;
use crate::{
    git_cache::GitCache,
    git_objects::{ObjectReadError, ReadOwner},
    native_resources::{NativeScope, NativeWork},
    packs::{metadata::MetadataError, sources::NativePackDescriptor},
};
use bytes::Bytes;
use canopy_object_storage::{artifact::ArtifactRanges, external::PART_BYTES};
use rusqlite::{Connection, params};
use std::sync::Mutex;
use tokio::sync::Mutex as AsyncMutex;

const MAX_DELTA_DEPTH: usize = 128;
struct Offsets {
    // Connection closes before the cache can remove the admitted SQLite file.
    db: Mutex<Connection>,
    _cache: Arc<GitCache>,
}
pub(super) struct SparsePack {
    pub(super) cache: Arc<GitCache>,
    index: Arc<crate::git_format::pack_index::PackIndex>,
    offsets: Arc<Offsets>,
    descriptor: NativePackDescriptor,
    payload: ArtifactRanges,
    native: NativeScope,
    preparing: AsyncMutex<()>,
    // One bounded provider part, independent of pack/object/repository size.
    part: Mutex<Option<(u64, Bytes)>>,
}
impl SparsePack {
    pub(super) fn index_budget(p: NativePackDescriptor) -> Result<u64, NativeReadError> {
        p.index
            .size
            .checked_add(
                u64::from(p.object_count)
                    .checked_mul(64)
                    .ok_or(NativeReadError::Capacity)?,
            )
            .and_then(|n| n.checked_add(128 * 1024 + 12288))
            .ok_or(NativeReadError::Capacity)
    }
    pub(super) async fn new(
        cache: Arc<GitCache>,
        store: &ArtifactStore,
        descriptor: NativePackDescriptor,
        native: NativeScope,
        owner: ReadOwner,
        cleanup: ReadOwner,
    ) -> Result<Self, NativeReadError> {
        let index = Arc::new(
            cache
                .native_index_owned(store, descriptor, owner.clone())
                .await?,
        );
        let keep = owner.clone();
        let held = cache.clone();
        let scan = index.clone();
        let claim = native
            .try_admit(NativeWork::Read)
            .map_err(ObjectReadError::from)?;
        let tail = descriptor
            .pack
            .size
            .checked_sub(descriptor.git_checksum.len() as u64)
            .ok_or(MetadataError::Integrity)?;
        let offsets = tokio::task::spawn_blocking(move || {
            let (_owner, _claim) = (keep, claim);
            held.reserve_native_spool(u64::from(scan.len()).checked_mul(64).and_then(|n| n.checked_add(128 * 1024)).ok_or(MetadataError::Limit)?)?;
            let mut db = Connection::open(held.root().join("native-offsets.sqlite"))?;
            db.execute_batch("PRAGMA page_size=4096; PRAGMA journal_mode=OFF; PRAGMA synchronous=OFF; PRAGMA cache_size=-256; PRAGMA mmap_size=0; PRAGMA temp_store=MEMORY; CREATE TABLE extents(offset BLOB PRIMARY KEY,ready INTEGER NOT NULL DEFAULT 0) WITHOUT ROWID;")?;
            let tx = db.transaction()?;
            {
                let mut insert = tx.prepare("INSERT INTO extents(offset) VALUES(?1)")?;
                for offset in scan.offsets() {
                    let offset = offset?;
                    if offset >= tail { return Err(MetadataError::Integrity); }
                    insert.execute([offset.to_be_bytes().as_slice()])?;
                }
            }
            tx.commit()?;
            Ok::<_, MetadataError>(Arc::new(Offsets { db: Mutex::new(db), _cache: held }))
        }).await??;
        cache.sparse_native_owned(descriptor, owner.clone()).await?;
        let payload = store
            .ranges_owned(
                descriptor.key(ArtifactKind::Pack)?,
                descriptor.pack,
                cleanup,
            )
            .await
            .map_err(MetadataError::from)?;
        Ok(Self {
            cache,
            index,
            offsets,
            descriptor,
            payload,
            native,
            preparing: AsyncMutex::new(()),
            part: Mutex::new(None),
        })
    }
    async fn find(&self, oid: ObjectId, owner: ReadOwner) -> Result<u64, NativeReadError> {
        let index = self.index.clone();
        let claim = self
            .native
            .try_admit(NativeWork::Read)
            .map_err(ObjectReadError::from)?;
        Ok(tokio::task::spawn_blocking(move || {
            let (_owner, _claim) = (owner, claim);
            index
                .find(oid)?
                .map(|entry| entry.offset)
                .ok_or(MetadataError::Integrity)
        })
        .await??)
    }
    async fn span(&self, offset: u64, owner: ReadOwner) -> Result<(bool, u64), NativeReadError> {
        let spool = self.offsets.clone();
        let tail = self.descriptor.pack.size - self.descriptor.git_checksum.len() as u64;
        let claim = self
            .native
            .try_admit(NativeWork::Read)
            .map_err(ObjectReadError::from)?;
        Ok(tokio::task::spawn_blocking(move || {
            let (_owner, _claim) = (owner, claim);
            let db = spool.db.lock().map_err(|_| MetadataError::Integrity)?;
            let (ready, end): (bool, Vec<u8>) = db.query_row("SELECT ready,coalesce((SELECT min(offset) FROM extents WHERE offset>?1),?2) FROM extents WHERE offset=?1", params![offset.to_be_bytes().as_slice(), tail.to_be_bytes().as_slice()], |row| Ok((row.get(0)?, row.get(1)?)))?;
            let end = u64::from_be_bytes(end.try_into().map_err(|_| MetadataError::Integrity)?);
            if end <= offset || end > tail { return Err(MetadataError::Integrity); }
            Ok::<_, MetadataError>((ready, end))
        }).await??)
    }
    async fn part(&self, index: u64, owner: ReadOwner) -> Result<Bytes, NativeReadError> {
        if let Some((saved, bytes)) = &*self.part.lock().map_err(|_| MetadataError::Integrity)?
            && *saved == index
        {
            return Ok(bytes.clone());
        }
        let bytes = self
            .payload
            .part_owned(index, owner)
            .await
            .map_err(MetadataError::from)?;
        *self.part.lock().map_err(|_| MetadataError::Integrity)? = Some((index, bytes.clone()));
        Ok(bytes)
    }
    async fn prefix(
        &self,
        offset: u64,
        end: u64,
        owner: ReadOwner,
    ) -> Result<Vec<u8>, NativeReadError> {
        let end = end.min(offset.checked_add(64).ok_or(MetadataError::Integrity)?);
        let mut bytes = Vec::with_capacity((end - offset) as usize);
        let mut at = offset;
        while at < end {
            let part = self.part(at / PART_BYTES as u64, owner.clone()).await?;
            let start = (at % PART_BYTES as u64) as usize;
            let count = (end - at).min(
                part.len()
                    .checked_sub(start)
                    .ok_or(MetadataError::Integrity)? as u64,
            ) as usize;
            if count == 0 {
                return Err(MetadataError::Integrity.into());
            }
            bytes.extend_from_slice(&part[start..start + count]);
            at += count as u64;
        }
        Ok(bytes)
    }
    pub(super) async fn prepare(
        &self,
        oid: ObjectId,
        owner: ReadOwner,
    ) -> Result<(), NativeReadError> {
        let _serial = self.preparing.lock().await;
        let mut offset = self.find(oid, owner.clone()).await?;
        let mut prepared = Vec::with_capacity(MAX_DELTA_DEPTH);
        loop {
            let (ready, end) = self.span(offset, owner.clone()).await?;
            if ready {
                break;
            }
            if prepared.len() == MAX_DELTA_DEPTH {
                return Err(NativeReadError::Capacity);
            }
            let prefix = self.prefix(offset, end, owner.clone()).await?;
            let base = base(&prefix, offset, self.descriptor.git_checksum.format())?;
            let next = match base {
                Base::None => None,
                Base::Offset(value) => Some(value),
                Base::Object(value) => Some(self.find(value, owner.clone()).await?),
            };
            if next.is_some_and(|next| next == offset || prepared.contains(&next)) {
                return Err(MetadataError::Integrity.into());
            }
            let mut at = offset;
            while at < end {
                let part = self.part(at / PART_BYTES as u64, owner.clone()).await?;
                let start = (at % PART_BYTES as u64) as usize;
                let count = (end - at).min(
                    part.len()
                        .checked_sub(start)
                        .ok_or(MetadataError::Integrity)? as u64,
                ) as usize;
                if count == 0 {
                    return Err(MetadataError::Integrity.into());
                }
                self.cache
                    .sparse_payload_owned(
                        self.descriptor,
                        at,
                        part.slice(start..start + count),
                        owner.clone(),
                    )
                    .await?;
                at += count as u64;
            }
            prepared.push(offset);
            let Some(next) = next else {
                break;
            };
            offset = next;
        }
        let spool = self.offsets.clone();
        let claim = self
            .native
            .try_admit(NativeWork::Read)
            .map_err(ObjectReadError::from)?;
        tokio::task::spawn_blocking(move || {
            let (_owner, _claim) = (owner, claim);
            let mut db = spool.db.lock().map_err(|_| MetadataError::Integrity)?;
            let tx = db.transaction()?;
            for offset in prepared {
                tx.execute(
                    "UPDATE extents SET ready=1 WHERE offset=?1",
                    [offset.to_be_bytes().as_slice()],
                )?;
            }
            tx.commit()?;
            Ok::<_, MetadataError>(())
        })
        .await??;
        Ok(())
    }
}
enum Base {
    None,
    Offset(u64),
    Object(ObjectId),
}
fn base(bytes: &[u8], offset: u64, format: ObjectFormat) -> Result<Base, MetadataError> {
    let first = *bytes.first().ok_or(MetadataError::Integrity)?;
    let kind = (first >> 4) & 7;
    let mut at = 1;
    let mut size = u64::from(first & 15);
    let mut shift = 4;
    let mut previous = first;
    while previous & 128 != 0 {
        previous = *bytes.get(at).ok_or(MetadataError::Integrity)?;
        at += 1;
        let value = u64::from(previous & 127)
            .checked_mul(1_u64.checked_shl(shift).ok_or(MetadataError::Integrity)?)
            .ok_or(MetadataError::Integrity)?;
        size = size.checked_add(value).ok_or(MetadataError::Integrity)?;
        shift += 7;
    }
    let _size = size; // Native Git validates the complete encoded stream.
    match kind {
        1..=4 => Ok(Base::None),
        6 => {
            let mut byte = *bytes.get(at).ok_or(MetadataError::Integrity)?;
            at += 1;
            let mut distance = u64::from(byte & 127);
            while byte & 128 != 0 {
                byte = *bytes.get(at).ok_or(MetadataError::Integrity)?;
                at += 1;
                distance = distance
                    .checked_add(1)
                    .and_then(|n| n.checked_mul(128))
                    .and_then(|n| n.checked_add(u64::from(byte & 127)))
                    .ok_or(MetadataError::Integrity)?;
            }
            let base = offset
                .checked_sub(distance)
                .filter(|base| *base >= 12 && *base < offset)
                .ok_or(MetadataError::Integrity)?;
            Ok(Base::Offset(base))
        }
        7 => Ok(Base::Object(
            ObjectId::try_from(
                bytes
                    .get(at..at + format.bytes())
                    .ok_or(MetadataError::Integrity)?,
            )
            .map_err(|_| MetadataError::Integrity)?,
        )),
        _ => Err(MetadataError::Integrity),
    }
}
