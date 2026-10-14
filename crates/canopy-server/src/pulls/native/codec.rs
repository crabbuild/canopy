use super::*;
fn invalid() -> CodecError {
    CodecError::Invalid("invalid native pull input")
}
fn fixed<const N: usize>(d: &mut BoundedDecoder<'_>) -> Result<[u8; N], CodecError> {
    d.read_bytes()?.try_into().map_err(|_| invalid())
}
fn digest(value: &impl WireValue) -> Result<[u8; 32], CodecError> {
    let mut e = BoundedEncoder::new(INPUT_BYTES)?;
    value.encode(&mut e)?;
    Ok(*blake3::hash(&e.finish()).as_bytes())
}
impl CreateData {
    pub(crate) fn digest(&self) -> Result<[u8; 32], CodecError> {
        digest(self)
    }
}
impl ReviewData {
    pub(crate) fn digest(&self) -> Result<[u8; 32], CodecError> {
        digest(self)
    }
}
impl ReadData {
    pub(crate) fn digest(&self) -> Result<[u8; 32], CodecError> {
        digest(self)
    }
}
impl WireValue for CreateData {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        if validate_repository_id(self.id).is_err()
            || !valid_new(&self.view())
            || [self.source_ref.len(), self.base_ref.len()]
                .into_iter()
                .any(|n| n > crate::packs::ref_state::MAX_NAME_BYTES)
        {
            return Err(invalid());
        }
        e.write_u8(51)?;
        e.write_bytes(&self.id)?;
        e.write_text(&self.title)?;
        e.write_text(&self.body)?;
        e.write_bool(self.draft)?;
        e.write_text(&self.source_ref)?;
        e.write_text(&self.source_oid)?;
        e.write_text(&self.base_ref)?;
        e.write_text(&self.base_oid)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        if d.read_u8()? != 51 {
            return Err(invalid());
        }
        let v = Self {
            id: fixed(d)?,
            title: d.read_text()?.into(),
            body: d.read_text()?.into(),
            draft: d.read_bool()?,
            source_ref: d.read_text()?.into(),
            source_oid: d.read_text()?.into(),
            base_ref: d.read_text()?.into(),
            base_oid: d.read_text()?.into(),
        };
        v.digest()?;
        Ok(v)
    }
}
impl WireValue for PullRevision {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        e.write_i64(self.pull_version)?;
        e.write_text(&self.source_oid)?;
        e.write_i64(self.source_version)?;
        e.write_text(&self.base_oid)?;
        e.write_i64(self.base_version)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        Ok(Self {
            pull_version: d.read_i64()?,
            source_oid: d.read_text()?.into(),
            source_version: d.read_i64()?,
            base_oid: d.read_text()?.into(),
            base_version: d.read_i64()?,
        })
    }
}
impl WireValue for ReviewData {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        if self.number < 1
            || validate_repository_id(self.id).is_err()
            || !valid_review(&self.view())
        {
            return Err(invalid());
        }
        e.write_u8(53)?;
        e.write_i64(self.number)?;
        e.write_bytes(&self.id)?;
        self.revision.encode(e)?;
        e.write_u8(match self.kind {
            ReviewKind::Comment => 0,
            ReviewKind::Approve => 1,
            ReviewKind::RequestChanges => 2,
        })?;
        e.write_text(&self.body)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        if d.read_u8()? != 53 {
            return Err(invalid());
        }
        let number = d.read_i64()?;
        let id = fixed(d)?;
        let revision = PullRevision::decode(d)?;
        let kind = match d.read_u8()? {
            0 => ReviewKind::Comment,
            1 => ReviewKind::Approve,
            2 => ReviewKind::RequestChanges,
            _ => return Err(invalid()),
        };
        let v = Self {
            number,
            id,
            revision,
            kind,
            body: d.read_text()?.into(),
        };
        v.digest()?;
        Ok(v)
    }
}
impl WireValue for PullState {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        e.write_u8(match self {
            Self::Open => 0,
            Self::Closed => 1,
            Self::Merged => 2,
        })
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        match d.read_u8()? {
            0 => Ok(Self::Open),
            1 => Ok(Self::Closed),
            2 => Ok(Self::Merged),
            _ => Err(invalid()),
        }
    }
}
impl WireValue for ReadData {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        e.write_u8(52)?;
        match &self.kind {
            ReadKind::Page { after, state } => {
                if *after < 0 {
                    return Err(invalid());
                }
                e.write_u8(0)?;
                e.write_i64(*after)?;
                state.encode(e)?;
            }
            ReadKind::Detail(number) => {
                if *number < 1 {
                    return Err(invalid());
                }
                e.write_u8(1)?;
                e.write_i64(*number)?;
            }
            ReadKind::ReviewPolicy(number) => {
                if *number < 1 {
                    return Err(invalid());
                }
                e.write_u8(3)?;
                e.write_i64(*number)?;
            }
            ReadKind::Reviews { number, after } => {
                if *number < 1 || *after < 0 {
                    return Err(invalid());
                }
                e.write_u8(2)?;
                e.write_i64(*number)?;
                e.write_i64(*after)?;
            }
        }
        e.write_bytes(&self.metadata)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        if d.read_u8()? != 52 {
            return Err(invalid());
        }
        let kind = match d.read_u8()? {
            0 => ReadKind::Page {
                after: d.read_i64()?,
                state: Option::<PullState>::decode(d)?,
            },
            1 => ReadKind::Detail(d.read_i64()?),
            3 => ReadKind::ReviewPolicy(d.read_i64()?),
            2 => ReadKind::Reviews {
                number: d.read_i64()?,
                after: d.read_i64()?,
            },
            _ => return Err(invalid()),
        };
        let v = Self {
            kind,
            metadata: fixed(d)?,
        };
        v.digest()?;
        Ok(v)
    }
}
impl WireValue for CreateRequest {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        if self.selection.actor.is_none() {
            return Err(invalid());
        }
        self.selection.encode(e)?;
        self.data.encode(e)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        let v = Self {
            selection: RefSelection::decode(d)?,
            data: CreateData::decode(d)?,
        };
        if v.selection.actor.is_none() {
            return Err(invalid());
        }
        Ok(v)
    }
}
impl WireValue for ReviewRequest {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        if self.selection.actor.is_none() {
            return Err(invalid());
        }
        self.selection.encode(e)?;
        self.data.encode(e)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        let v = Self {
            selection: RefSelection::decode(d)?,
            data: ReviewData::decode(d)?,
        };
        if v.selection.actor.is_none() {
            return Err(invalid());
        }
        Ok(v)
    }
}
impl WireValue for ReadRequest {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        self.selection.encode(e)?;
        self.data.encode(e)
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        Ok(Self {
            selection: RefSelection::decode(d)?,
            data: ReadData::decode(d)?,
        })
    }
}
impl WireValue for ReadReply {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        match self {
            Self::Changed => e.write_u8(0),
            Self::Rows(rows) => {
                e.write_u8(1)?;
                rows.encode(e)
            }
        }
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        match d.read_u8()? {
            0 => Ok(Self::Changed),
            1 => Ok(Self::Rows(Option::<Vec<SqlResultSet>>::decode(d)?)),
            _ => Err(invalid()),
        }
    }
}
impl WireValue for PullChange {
    fn encode(&self, e: &mut BoundedEncoder) -> Result<(), CodecError> {
        match self {
            Self::Applied(n) if *n > 0 => {
                e.write_u8(0)?;
                e.write_i64(*n)
            }
            Self::NotFound => e.write_u8(1),
            Self::Forbidden => e.write_u8(2),
            Self::Conflict => e.write_u8(3),
            _ => Err(invalid()),
        }
    }
    fn decode(d: &mut BoundedDecoder<'_>) -> Result<Self, CodecError> {
        match d.read_u8()? {
            0 => {
                let n = d.read_i64()?;
                if n < 1 {
                    return Err(invalid());
                }
                Ok(Self::Applied(n))
            }
            1 => Ok(Self::NotFound),
            2 => Ok(Self::Forbidden),
            3 => Ok(Self::Conflict),
            _ => Err(invalid()),
        }
    }
}
