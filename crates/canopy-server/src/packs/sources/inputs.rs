//! Creating input inventory reuses the persistent range index, independently
//! of canonical source shards. Membership is custody, never publication proof.
use super::*;
use cellule_runtime::codec::{BoundedDecoder, BoundedEncoder, CodecError};
use index::{
    codec::{artifact, fixed, read_artifact},
    record::sealed,
};

impl sealed::Record for NativePackDescriptor {}
impl IndexRecord for NativePackDescriptor {
    type Key = SegmentKey;
    const FANOUT: usize = SOURCE_FANOUT;
    const DOMAIN: &'static [u8] = b"canopy.native-input-index.v1\0";
    fn first_key(&self) -> SegmentKey {
        SegmentKey {
            operation: self.operation,
            digest: self.pack.digest,
        }
    }
    fn last_key(&self) -> SegmentKey {
        self.first_key()
    }
    fn object_count(&self) -> u64 {
        u64::from(self.object_count)
    }
    fn validate_record(
        &self,
        repository: [u8; 16],
        format: ObjectFormat,
    ) -> Result<(), IndexError> {
        self.validate(repository, format)?;
        if self.pack.manifest_digest == [0; 32] || self.index.manifest_digest == [0; 32] {
            return Err(IndexError::Integrity);
        }
        Ok(())
    }
    fn encode_record(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        e.write_bytes(&self.operation)?;
        self.git_checksum.encode(e)?;
        e.write_u32(self.object_count)?;
        artifact(e, self.pack)?;
        artifact(e, self.index)
    }
    fn decode_record(
        d: &mut BoundedDecoder<'_>,
        repository: [u8; 16],
        format: ObjectFormat,
    ) -> Result<Self, CodecError> {
        Ok(Self {
            repository,
            operation: fixed(d)?,
            format,
            git_checksum: ObjectId::decode(d, format)?,
            object_count: d.read_u32()?,
            pack: read_artifact(d)?,
            index: read_artifact(d)?,
        })
    }
}
