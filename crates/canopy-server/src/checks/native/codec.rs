use super::*;
fn fixed<const N: usize>(d: &mut BoundedDecoder<'_>) -> Result<[u8; N], CodecError> {
    d.read_bytes()?.try_into().map_err(|_| invalid())
}
fn invalid() -> CodecError {
    CodecError::Invalid("invalid native check input")
}
impl WireValue for CommitSelection {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        validate_repository_id(self.repository).map_err(|_| invalid())?;
        if self
            .actor
            .as_deref()
            .is_some_and(|a| validate_component(a).is_err())
        {
            return Err(invalid());
        }
        e.write_bytes(&self.repository)?;
        self.actor.encode(e)?;
        e.write_bytes(self.oid.as_ref())?;
        self.membership.encode(e)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        let value = Self {
            repository: fixed(d)?,
            actor: Option::<String>::decode(d)?,
            oid: ObjectId::try_from(d.read_bytes()?).map_err(|_| invalid())?,
            membership: Option::<CommitMembership>::decode(d)?,
        };
        value.encode(&mut BoundedEncoder::new(4096)?)?;
        Ok(value)
    }
}
impl WireValue for CommitPage {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        if self
            .after
            .as_deref()
            .is_some_and(|a| validate_component(a).is_err())
        {
            return Err(invalid());
        }
        self.selection.encode(e)?;
        self.after.encode(e)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        let value = Self {
            selection: CommitSelection::decode(d)?,
            after: Option::<String>::decode(d)?,
        };
        value.encode(&mut BoundedEncoder::new(4096)?)?;
        Ok(value)
    }
}
impl WireValue for CheckStart {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        if self.selection.actor.is_none()
            || validate_repository_id(self.id).is_err()
            || validate_component(&self.context).is_err()
            || self.context_version < 1
        {
            return Err(invalid());
        }
        self.selection.encode(e)?;
        e.write_bytes(&self.id)?;
        e.write_text(&self.context)?;
        e.write_i64(self.context_version)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        let value = Self {
            selection: CommitSelection::decode(d)?,
            id: fixed(d)?,
            context: d.read_text()?.into(),
            context_version: d.read_i64()?,
        };
        value.encode(&mut BoundedEncoder::new(4096)?)?;
        Ok(value)
    }
}
impl WireValue for CheckChange {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        e.write_u8(match self {
            Self::Applied => 0,
            Self::NotFound => 1,
            Self::Forbidden => 2,
            Self::Conflict => 3,
        })
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        match d.read_u8()? {
            0 => Ok(Self::Applied),
            1 => Ok(Self::NotFound),
            2 => Ok(Self::Forbidden),
            3 => Ok(Self::Conflict),
            _ => Err(invalid()),
        }
    }
}
