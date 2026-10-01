use super::*;

pub(in crate::packs) fn fixed<const N: usize>(
    decoder: &mut BoundedDecoder<'_>,
) -> Result<[u8; N], CodecError> {
    decoder
        .read_bytes()?
        .try_into()
        .map_err(|_| CodecError::Invalid("invalid fixed-width directory field"))
}
fn oid(decoder: &mut BoundedDecoder<'_>, format: ObjectFormat) -> Result<ObjectId, CodecError> {
    let oid = ObjectId::try_from(decoder.read_bytes()?)
        .map_err(|_| CodecError::Invalid("invalid directory OID"))?;
    if oid.format() != format {
        return Err(CodecError::Invalid("directory OID format mismatch"));
    }
    Ok(oid)
}
pub(in crate::packs) fn artifact(
    encoder: &mut BoundedEncoder,
    value: ArtifactDescriptor,
) -> Result<(), CodecError> {
    encoder.write_u64(value.size)?;
    encoder.write_bytes(&value.digest)?;
    encoder.write_bytes(&value.manifest_digest)
}
pub(in crate::packs) fn read_artifact(
    decoder: &mut BoundedDecoder<'_>,
) -> Result<ArtifactDescriptor, CodecError> {
    Ok(ArtifactDescriptor {
        size: decoder.read_u64()?,
        digest: fixed(decoder)?,
        manifest_digest: fixed(decoder)?,
    })
}
pub(in crate::packs) fn reference<R: IndexRecord>(
    encoder: &mut BoundedEncoder,
    value: NodeRef<R>,
) -> Result<(), CodecError> {
    encoder.write_bytes(&value.operation)?;
    artifact(encoder, value.artifact)?;
    encoder.write_u8(value.height)?;
    value.first_key.encode(encoder)?;
    value.last_key.encode(encoder)?;
    encoder.write_u64(value.record_count)?;
    encoder.write_u64(value.object_count)
}
pub(in crate::packs) fn read_reference<R: IndexRecord>(
    decoder: &mut BoundedDecoder<'_>,
    format: ObjectFormat,
) -> Result<NodeRef<R>, CodecError> {
    Ok(NodeRef {
        operation: fixed(decoder)?,
        artifact: read_artifact(decoder)?,
        height: decoder.read_u8()?,
        first_key: R::Key::decode(decoder, format)?,
        last_key: R::Key::decode(decoder, format)?,
        record_count: decoder.read_u64()?,
        object_count: decoder.read_u64()?,
    })
}
pub(in crate::packs) fn write_run(
    encoder: &mut BoundedEncoder,
    stored: StoredRun,
) -> Result<(), CodecError> {
    let run = stored.run;
    encoder.write_bytes(&run.operation)?;
    encoder.write_u64(run.object_count)?;
    encoder.write_bytes(&run.first_oid)?;
    encoder.write_bytes(&run.last_oid)?;
    encoder.write_bytes(&run.inventory_digest)?;
    artifact(encoder, stored.artifact)
}
pub(in crate::packs) fn read_run(
    decoder: &mut BoundedDecoder<'_>,
    repository: [u8; 16],
    format: ObjectFormat,
) -> Result<StoredRun, CodecError> {
    let operation = fixed(decoder)?;
    let object_count = decoder.read_u64()?;
    let first_oid = oid(decoder, format)?;
    let last_oid = oid(decoder, format)?;
    let inventory_digest = fixed(decoder)?;
    let artifact = read_artifact(decoder)?;
    Ok(StoredRun {
        run: RunDescriptor {
            repository,
            operation,
            format,
            object_count,
            first_oid,
            last_oid,
            inventory_digest,
            size: artifact.size,
            digest: artifact.digest,
        },
        artifact,
    })
}
impl<R: IndexRecord> Node<R> {
    pub(super) fn encode(&self) -> Result<Vec<u8>, IndexError> {
        self.validate()?;
        let mut encoder = BoundedEncoder::new(NODE_BYTES)?;
        encoder.write_bytes(R::DOMAIN)?;
        encoder.write_bytes(&self.repository)?;
        encoder.write_bytes(&self.operation)?;
        encoder.write_u8(self.format.bytes() as u8)?;
        encoder.write_u8(self.height)?;
        encoder.write_count(self.contents.len())?;
        match &self.contents {
            Contents::Runs(runs) => {
                for stored in runs {
                    stored.encode_record(&mut encoder)?;
                }
            }
            Contents::Children(children) => {
                for child in children {
                    reference(&mut encoder, *child)?;
                }
            }
        }
        Ok(encoder.finish())
    }
    pub(super) fn decode(bytes: &[u8]) -> Result<Self, IndexError> {
        let mut decoder = BoundedDecoder::new(bytes, NODE_BYTES)?;
        if decoder.read_bytes()? != R::DOMAIN {
            return Err(IndexError::Integrity);
        }
        let repository = fixed(&mut decoder)?;
        let operation = fixed(&mut decoder)?;
        let format = match decoder.read_u8()? {
            20 => ObjectFormat::Sha1,
            32 => ObjectFormat::Sha256,
            _ => return Err(IndexError::Integrity),
        };
        let height = decoder.read_u8()?;
        let count = decoder.read_count()?;
        if !(1..=R::FANOUT).contains(&count) || height > MAX_HEIGHT {
            return Err(IndexError::Limit);
        }
        let contents = if height == 0 {
            let mut runs = Vec::with_capacity(count);
            for _ in 0..count {
                runs.push(R::decode_record(&mut decoder, repository, format)?);
            }
            Contents::Runs(runs)
        } else {
            let mut children = Vec::with_capacity(count);
            for _ in 0..count {
                children.push(read_reference(&mut decoder, format)?);
            }
            Contents::Children(children)
        };
        decoder.finish()?;
        let node = Self {
            repository,
            operation,
            format,
            height,
            contents,
        };
        node.validate()?;
        Ok(node)
    }
}
