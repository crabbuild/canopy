//! One resident metadata shard; ordered descriptors live on admitted disk.
//! This private replay is not an input checkpoint or a publication certificate.
use super::*;
use crate::packs::{
    directory::index::IndexRecord,
    metadata::{AdmittedFile, StoredSegment},
    publication::StagingContext,
    sources::SourceRecord,
};
use cellule_runtime::codec::{BoundedDecoder, BoundedEncoder};
use std::{
    io::{Read, Seek, SeekFrom, Write},
    sync::Mutex,
};

const RECORD_BYTES: u32 = 512;

#[derive(Clone, Copy, Debug)]
pub struct NativeMetadataLimits {
    pub max_shard_objects: u32,
    pub max_descriptor_bytes: u64,
}
impl Default for NativeMetadataLimits {
    fn default() -> Self {
        Self {
            max_shard_objects: 8192,
            max_descriptor_bytes: 64 << 20,
        }
    }
}

/// Available only after every ordinal has passed isolated physical inspection
/// and all metadata uploads have completed. No local metadata shard or worker
/// activity remains in the result, allowing Creating to drain before Bind.
pub struct StagedNativeMetadata {
    pub(in crate::packs) witness: PhysicalPackWitness,
    pub(in crate::packs) replay: DescriptorReplay,
}
impl StagedNativeMetadata {
    #[cfg(test)]
    pub(in crate::packs) fn truncate_replay_for_test(&self) -> Result<(), PhysicalError> {
        self.replay
            .spool
            .lock()
            .map_err(|_| PhysicalError::Integrity)?
            .file
            .file()
            .as_file()
            .set_len(self.replay.bytes - 1)?;
        Ok(())
    }
    #[cfg(test)]
    pub(in crate::packs) fn first_metadata_for_test(&self) -> Result<StoredSegment, PhysicalError> {
        self.replay
            .spool
            .lock()
            .map_err(|_| PhysicalError::Integrity)?
            .read(self.native(), 0, self.replay.bytes)?
            .map(|(stored, _)| stored)
            .ok_or(PhysicalError::Integrity)
    }
    pub fn native(&self) -> NativePackDescriptor {
        self.witness.native()
    }
    pub fn shard_count(&self) -> u32 {
        self.witness.shard_count()
    }
    pub fn descriptor_bytes(&self) -> u64 {
        self.replay.bytes
    }
}

impl PhysicalVerifier {
    pub async fn stage_metadata(
        mut self,
        limits: NativeMetadataLimits,
    ) -> Result<StagedNativeMetadata, PhysicalError> {
        if limits.max_shard_objects == 0
            || limits.max_descriptor_bytes == 0
            || limits.max_descriptor_bytes > MAX_ARTIFACT_BYTES
            || self.next_ordinal != 0
            || self.failed
        {
            return Err(PhysicalError::Limit);
        }
        let context = self.context.clone().ok_or(PhysicalError::Integrity)?;
        context.ensure_live()?;
        let root = self.root.clone();
        let budget = self.budget.clone();
        let activity = context.physical_owner();
        let mut spool = tokio::task::spawn_blocking(move || {
            let _activity = activity;
            let workspace = Arc::new(
                tempfile::Builder::new()
                    .prefix("canopy-native-metadata-")
                    .tempdir_in(root)?,
            );
            let mut file = AdmittedFile::new(
                tempfile::NamedTempFile::new_in(workspace.path())?,
                budget.try_reserve(0).map_err(MetadataError::from)?,
            );
            file.retain_workspace(workspace);
            Ok::<_, PhysicalError>(DescriptorSpool { file, bytes: 0 })
        })
        .await??;
        while self.next_ordinal < self.descriptor.object_count {
            let count =
                (self.descriptor.object_count - self.next_ordinal).min(limits.max_shard_objects);
            let segment = self.inspect_next_shard(count).await?;
            context.ensure_live()?;
            let metadata = segment
                .upload_owned(&self.store, context.physical_owner())
                .await?;
            context.ensure_live()?;
            let record = SourceRecord {
                metadata,
                pack: self.descriptor.pack,
                index: self.descriptor.index,
                pack_object_count: self.descriptor.object_count,
            };
            record.validate(self.descriptor.repository, self.descriptor.format)?;
            let activity = context.physical_owner();
            spool = tokio::task::spawn_blocking(move || {
                let _activity = activity;
                spool.append(record, limits.max_descriptor_bytes)?;
                Ok::<_, PhysicalError>(spool)
            })
            .await??;
            // Upload consumes the last shard pin. The next shard never coexists
            // with earlier metadata files; only fixed-size descriptors survive.
        }
        let witness = self.finish().await?;
        let activity = context.physical_owner();
        let replay = tokio::task::spawn_blocking(move || {
            let _activity = activity;
            spool.file.file().as_file().sync_all()?;
            if spool.file.file().as_file().metadata()?.len() != spool.bytes {
                return Err(PhysicalError::Integrity);
            }
            Ok::<_, PhysicalError>(DescriptorReplay {
                bytes: spool.bytes,
                spool: Arc::new(Mutex::new(spool)),
            })
        })
        .await??;
        context.ensure_live()?;
        Ok(StagedNativeMetadata { witness, replay })
    }
}

struct DescriptorSpool {
    file: AdmittedFile,
    bytes: u64,
}
impl DescriptorSpool {
    fn append(&mut self, record: SourceRecord, maximum: u64) -> Result<(), PhysicalError> {
        let mut encoder = BoundedEncoder::new(RECORD_BYTES).map_err(IndexError::from)?;
        record
            .encode_record(&mut encoder)
            .map_err(IndexError::from)?;
        let bytes = encoder.finish();
        let end = self
            .bytes
            .checked_add(4 + bytes.len() as u64)
            .filter(|end| *end <= maximum)
            .ok_or(PhysicalError::Limit)?;
        // Admit before appending. A partial failed append cannot escape the
        // consuming factory and keeps its file charged through cleanup.
        self.file
            .reservation()
            .resize(end)
            .map_err(MetadataError::from)?;
        self.file
            .file_mut()
            .write_all(&(bytes.len() as u32).to_be_bytes())?;
        self.file.file_mut().write_all(&bytes)?;
        self.bytes = end;
        Ok(())
    }
    fn read(
        &mut self,
        native: NativePackDescriptor,
        offset: u64,
        size: u64,
    ) -> Result<Option<(StoredSegment, u64)>, PhysicalError> {
        if self.file.file().as_file().metadata()?.len() != size || offset > size {
            return Err(PhysicalError::Integrity);
        }
        if offset == size {
            return Ok(None);
        }
        let file = self.file.file_mut();
        file.seek(SeekFrom::Start(offset))?;
        let mut length = [0; 4];
        file.read_exact(&mut length)?;
        let length = u32::from_be_bytes(length);
        let end = offset
            .checked_add(4 + u64::from(length))
            .filter(|end| *end <= size && length != 0 && length <= RECORD_BYTES)
            .ok_or(PhysicalError::Integrity)?;
        let mut bytes = vec![0; length as usize];
        file.read_exact(&mut bytes)?;
        let mut decoder = BoundedDecoder::new(&bytes, RECORD_BYTES).map_err(IndexError::from)?;
        let record = SourceRecord::decode_record(&mut decoder, native.repository, native.format)
            .map_err(IndexError::from)?;
        decoder.finish().map_err(IndexError::from)?;
        record.validate(native.repository, native.format)?;
        if record.native() != native {
            return Err(PhysicalError::Integrity);
        }
        Ok(Some((record.metadata, end)))
    }
}

pub(in crate::packs) struct DescriptorReplay {
    spool: Arc<Mutex<DescriptorSpool>>,
    bytes: u64,
}
impl DescriptorReplay {
    pub(in crate::packs) async fn next(
        &self,
        context: &StagingContext,
        native: NativePackDescriptor,
        offset: u64,
    ) -> Result<Option<(StoredSegment, u64)>, PhysicalError> {
        context.ensure_live()?;
        let spool = self.spool.clone();
        let activity = context.physical_owner();
        let size = self.bytes;
        let record = tokio::task::spawn_blocking(move || {
            let _activity = activity;
            let mut spool = spool.lock().map_err(|_| PhysicalError::Integrity)?;
            spool.read(native, offset, size)
        })
        .await??;
        context.ensure_live()?;
        Ok(record)
    }
}

#[cfg(test)]
mod tests;
