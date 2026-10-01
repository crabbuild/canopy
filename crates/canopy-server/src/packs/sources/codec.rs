use super::*;
use cellule_runtime::codec::{BoundedDecoder, BoundedEncoder, CodecError};
use index::{
    codec::{artifact, fixed, read_artifact},
    record::sealed,
};

impl sealed::Key for SegmentKey {}
impl IndexKey for SegmentKey {
    fn valid(self, _format: ObjectFormat) -> bool {
        true
    }
    fn encode(self, encoder: &mut BoundedEncoder) -> Result<(), CodecError> {
        let mut bytes = [0; 48];
        bytes[..16].copy_from_slice(&self.operation);
        bytes[16..].copy_from_slice(&self.digest);
        encoder.write_bytes(&bytes)
    }
    fn decode(decoder: &mut BoundedDecoder<'_>, _format: ObjectFormat) -> Result<Self, CodecError> {
        let bytes: [u8; 48] = fixed(decoder)?;
        Ok(Self {
            operation: bytes[..16]
                .try_into()
                .map_err(|_| CodecError::Invalid("source operation"))?,
            digest: bytes[16..]
                .try_into()
                .map_err(|_| CodecError::Invalid("source digest"))?,
        })
    }
}
impl sealed::Record for SourceRecord {}
impl IndexRecord for SourceRecord {
    type Key = SegmentKey;
    const FANOUT: usize = SOURCE_FANOUT;
    const DOMAIN: &'static [u8] = b"canopy.source-index.v1\0";
    fn first_key(self) -> SegmentKey {
        self.key()
    }
    fn last_key(self) -> SegmentKey {
        self.key()
    }
    fn object_count(self) -> u64 {
        u64::from(self.metadata.segment.identity.object_count)
    }
    fn validate_record(self, repository: [u8; 16], format: ObjectFormat) -> Result<(), IndexError> {
        self.validate(repository, format)
    }
    fn encode_record(self, encoder: &mut BoundedEncoder) -> Result<(), CodecError> {
        let segment = self.metadata.segment;
        let identity = segment.identity;
        encoder.write_bytes(&identity.operation)?;
        encoder.write_bytes(&identity.git_checksum)?;
        encoder.write_u32(identity.first_ordinal)?;
        encoder.write_u32(identity.object_count)?;
        encoder.write_u64(segment.edge_count)?;
        encoder.write_bytes(&segment.inventory_digest)?;
        segment.first_oid.encode(encoder)?;
        segment.last_oid.encode(encoder)?;
        artifact(encoder, self.metadata.artifact)?;
        artifact(encoder, self.pack)?;
        artifact(encoder, self.index)?;
        encoder.write_u32(self.pack_object_count)
    }
    fn decode_record(
        decoder: &mut BoundedDecoder<'_>,
        repository: [u8; 16],
        format: ObjectFormat,
    ) -> Result<Self, CodecError> {
        let operation = fixed(decoder)?;
        let git_checksum = ObjectId::decode(decoder, format)?;
        let first_ordinal = decoder.read_u32()?;
        let object_count = decoder.read_u32()?;
        let edge_count = decoder.read_u64()?;
        let inventory_digest = fixed(decoder)?;
        let first_oid = ObjectId::decode(decoder, format)?;
        let last_oid = ObjectId::decode(decoder, format)?;
        let metadata = read_artifact(decoder)?;
        let pack = read_artifact(decoder)?;
        let index = read_artifact(decoder)?;
        Ok(Self {
            metadata: StoredSegment {
                segment: SegmentDescriptor {
                    identity: SegmentIdentity {
                        repository,
                        operation,
                        format,
                        pack_digest: pack.digest,
                        git_checksum,
                        first_ordinal,
                        object_count,
                    },
                    edge_count,
                    inventory_digest,
                    first_oid,
                    last_oid,
                    size: metadata.size,
                    digest: metadata.digest,
                },
                artifact: metadata,
            },
            pack,
            index,
            pack_object_count: decoder.read_u32()?,
        })
    }
}
