use super::*;

pub(in crate::packs) mod sealed {
    pub trait Key {}
    pub trait Record {}
}

/// Persisted key types are sealed: their byte order and validation are part of
/// the catalog contract. Source incarnation keys are not fabricated Git OIDs.
pub trait IndexKey: Copy + Ord + std::fmt::Debug + Send + Sync + 'static + sealed::Key {
    fn valid(self, format: ObjectFormat) -> bool;
    fn encode(self, encoder: &mut BoundedEncoder) -> Result<(), CodecError>;
    fn decode(decoder: &mut BoundedDecoder<'_>, format: ObjectFormat) -> Result<Self, CodecError>;
}

/// One immutable leaf descriptor in the shared path-copy range tree.
pub trait IndexRecord:
    Copy + Eq + std::fmt::Debug + Send + Sync + 'static + sealed::Record
{
    type Key: IndexKey;
    const FANOUT: usize;
    const DOMAIN: &'static [u8];
    fn first_key(self) -> Self::Key;
    fn last_key(self) -> Self::Key;
    fn object_count(self) -> u64;
    fn validate_record(self, repository: [u8; 16], format: ObjectFormat) -> Result<(), IndexError>;
    fn encode_record(self, encoder: &mut BoundedEncoder) -> Result<(), CodecError>;
    fn decode_record(
        decoder: &mut BoundedDecoder<'_>,
        repository: [u8; 16],
        format: ObjectFormat,
    ) -> Result<Self, CodecError>;
}

impl sealed::Key for ObjectId {}
impl IndexKey for ObjectId {
    fn valid(self, format: ObjectFormat) -> bool {
        self.format() == format && !self.is_zero()
    }
    fn encode(self, encoder: &mut BoundedEncoder) -> Result<(), CodecError> {
        encoder.write_bytes(&self)
    }
    fn decode(decoder: &mut BoundedDecoder<'_>, format: ObjectFormat) -> Result<Self, CodecError> {
        let oid = ObjectId::try_from(decoder.read_bytes()?)
            .map_err(|_| CodecError::Invalid("invalid catalog OID"))?;
        if !oid.valid(format) {
            return Err(CodecError::Invalid("invalid catalog OID format or zero ID"));
        }
        Ok(oid)
    }
}
impl sealed::Record for StoredRun {}
impl IndexRecord for StoredRun {
    type Key = ObjectId;
    const FANOUT: usize = FANOUT;
    const DOMAIN: &'static [u8] = b"canopy.range-index.v2\0";
    fn first_key(self) -> ObjectId {
        self.coverage.first_oid
    }
    fn last_key(self) -> ObjectId {
        self.coverage.last_oid
    }
    fn object_count(self) -> u64 {
        self.coverage.object_count
    }
    fn validate_record(self, repository: [u8; 16], format: ObjectFormat) -> Result<(), IndexError> {
        self.validate()?;
        if self.run.repository != repository || self.run.format != format {
            return Err(IndexError::Integrity);
        }
        Ok(())
    }
    fn encode_record(self, encoder: &mut BoundedEncoder) -> Result<(), CodecError> {
        codec::write_run(encoder, self)
    }
    fn decode_record(
        decoder: &mut BoundedDecoder<'_>,
        repository: [u8; 16],
        format: ObjectFormat,
    ) -> Result<Self, CodecError> {
        codec::read_run(decoder, repository, format)
    }
}
