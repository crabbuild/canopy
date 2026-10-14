use super::super::directory::index::codec::{
    artifact, fixed, read_artifact, read_reference, reference,
};
use super::*;
use cellule_runtime::codec::{BoundedDecoder, BoundedEncoder};

impl CatalogSnapshot {
    pub(super) fn encode(self, operation: [u8; 16]) -> Result<Vec<u8>, IndexError> {
        self.validate()?;
        let mut encoder = BoundedEncoder::new(CATALOG_BYTES)?;
        encoder.write_bytes(b"canopy.catalog-root.v1\0")?;
        encoder.write_bytes(&self.directory.repository)?;
        encoder.write_bytes(&operation)?;
        encoder.write_u8(self.directory.format.bytes() as u8)?;
        encoder.write_bytes(&self.directory.operation)?;
        artifact(&mut encoder, self.directory.artifact)?;
        encoder.write_bool(self.sources.is_some())?;
        if let Some(root) = self.sources {
            reference(&mut encoder, root)?;
        }
        Ok(encoder.finish())
    }
    pub(super) fn decode(bytes: &[u8]) -> Result<(Self, [u8; 16]), IndexError> {
        let mut decoder = BoundedDecoder::new(bytes, CATALOG_BYTES)?;
        if decoder.read_bytes()? != b"canopy.catalog-root.v1\0" {
            return Err(IndexError::Integrity);
        }
        let repository = fixed(&mut decoder)?;
        let operation = fixed(&mut decoder)?;
        let format = match decoder.read_u8()? {
            20 => ObjectFormat::Sha1,
            32 => ObjectFormat::Sha256,
            _ => return Err(IndexError::Integrity),
        };
        let directory = StoredSnapshot {
            repository,
            format,
            operation: fixed(&mut decoder)?,
            artifact: read_artifact(&mut decoder)?,
        };
        let sources = if decoder.read_bool()? {
            Some(read_reference(&mut decoder, format)?)
        } else {
            None
        };
        decoder.finish()?;
        let snapshot = Self { directory, sources };
        snapshot.validate()?;
        Ok((snapshot, operation))
    }
}

// Reuse the same descriptor and artifact encoding for authoritative Cell facts.
// This record is distinct from the catalog artifact's two-root payload.
impl cellule_runtime::codec::WireValue for StoredCatalog {
    fn encode(
        &self,
        encoder: &mut BoundedEncoder,
    ) -> Result<(), cellule_runtime::codec::CodecError> {
        self.validate().map_err(|_| {
            cellule_runtime::codec::CodecError::Invalid("invalid catalog reference")
        })?;
        encoder.write_bytes(b"canopy.catalog-ref.v1\0")?;
        encoder.write_bytes(&self.repository)?;
        encoder.write_bytes(&self.operation)?;
        encoder.write_u8(self.format.bytes() as u8)?;
        artifact(encoder, self.artifact)
    }
    fn decode(
        decoder: &mut BoundedDecoder<'_>,
    ) -> Result<Self, cellule_runtime::codec::CodecError> {
        use cellule_runtime::codec::CodecError;
        if decoder.read_bytes()? != b"canopy.catalog-ref.v1\0" {
            return Err(CodecError::Invalid("invalid catalog reference domain"));
        }
        let repository = fixed(decoder)?;
        let operation = fixed(decoder)?;
        let format = match decoder.read_u8()? {
            20 => ObjectFormat::Sha1,
            32 => ObjectFormat::Sha256,
            _ => return Err(CodecError::Invalid("invalid catalog reference format")),
        };
        let value = Self {
            repository,
            operation,
            format,
            artifact: read_artifact(decoder)?,
        };
        value
            .validate()
            .map_err(|_| CodecError::Invalid("invalid catalog reference"))?;
        Ok(value)
    }
}
