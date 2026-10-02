use super::*;
use crate::packs::directory::index::{IndexKey, IndexRecord, record::sealed};

/// A byte-ordered UTF-8 seek coordinate. Leaves separately enforce Git ref
/// syntax, so prefix bounds such as `refs/heads/topic/` are valid coordinates.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct RefNameKey(Arc<str>);
impl RefNameKey {
    pub fn new(name: &str) -> Result<Self, CodecError> {
        if name.len() > MAX_NAME_BYTES || name.contains('\0') {
            return Err(CodecError::Invalid("ref key size or NUL"));
        }
        Ok(Self(Arc::from(name)))
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
impl sealed::Key for RefNameKey {}
impl IndexKey for RefNameKey {
    fn valid(&self, _: ObjectFormat) -> bool {
        self.0.len() <= MAX_NAME_BYTES && !self.0.contains('\0')
    }
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        e.write_text(&self.0)
    }
    fn decode(d: &mut BoundedDecoder<'_>, _: ObjectFormat) -> Result<Self, CodecError> {
        Self::new(d.read_text()?)
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RefStateRecord {
    pub(super) name: RefNameKey,
    pub(super) state: RefExpectation,
}
impl RefStateRecord {
    pub fn new(
        name: &str,
        state: RefExpectation,
        format: ObjectFormat,
    ) -> Result<Self, IndexError> {
        let record = Self {
            name: RefNameKey::new(name)?,
            state,
        };
        record.validate_record([0; 16], format)?;
        Ok(record)
    }
    pub fn name(&self) -> &str {
        self.name.as_str()
    }
    pub fn state(&self) -> &RefExpectation {
        &self.state
    }
}
impl sealed::Record for RefStateRecord {}
impl IndexRecord for RefStateRecord {
    type Key = RefNameKey;
    const FANOUT: usize = 128;
    const DOMAIN: &'static [u8] = b"canopy.ref-state-index.v1\0";
    const NODE_BYTES: u32 = 512 << 10;
    const MAX_HEIGHT: u8 = 16;
    fn valid_counts(records: u64, live: u64) -> bool {
        live <= records && records <= i64::MAX as u64
    }
    fn first_key(&self) -> RefNameKey {
        self.name.clone()
    }
    fn last_key(&self) -> RefNameKey {
        self.name.clone()
    }
    fn object_count(&self) -> u64 {
        u64::from(self.state.oid.is_some())
    }
    fn validate_record(&self, _: [u8; 16], format: ObjectFormat) -> Result<(), IndexError> {
        if !self.name.valid(format)
            || !crate::refs::valid_ref_name(self.name())
            || self.state.version <= 0
            || self
                .state
                .oid
                .is_some_and(|oid| oid.is_zero() || oid.format() != format)
        {
            return Err(IndexError::Integrity);
        }
        Ok(())
    }
    fn encode_record(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        self.name.encode(e)?;
        e.write_i64(self.state.version)?;
        e.write_bool(self.state.oid.is_some())?;
        if let Some(oid) = self.state.oid {
            e.write_bytes(&oid)?;
        }
        Ok(())
    }
    fn decode_record(
        d: &mut BoundedDecoder<'_>,
        repository: [u8; 16],
        format: ObjectFormat,
    ) -> Result<Self, CodecError> {
        let name = RefNameKey::decode(d, format)?;
        let version = d.read_i64()?;
        let oid = if d.read_bool()? {
            Some(
                crate::ObjectId::try_from(d.read_bytes()?)
                    .map_err(|_| CodecError::Invalid("ref object ID"))?,
            )
        } else {
            None
        };
        let record = Self {
            name,
            state: RefExpectation { oid, version },
        };
        record
            .validate_record(repository, format)
            .map_err(|_| CodecError::Invalid("ref state"))?;
        Ok(record)
    }
}
