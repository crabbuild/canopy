//! Git object formats and canonical object identifiers.

use sha1::{Digest, Sha1};
use sha2::Sha256;
use std::ops::Deref;

use crate::ObjectKind;

/// Immutable hash format selected when a repository is created.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ObjectFormat {
    #[default]
    Sha1,
    Sha256,
}

impl ObjectFormat {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Sha1 => "sha1",
            Self::Sha256 => "sha256",
        }
    }

    pub const fn bytes(self) -> usize {
        match self {
            Self::Sha1 => 20,
            Self::Sha256 => 32,
        }
    }

    pub(crate) fn parse(value: &str) -> Option<Self> {
        match value {
            "sha1" => Some(Self::Sha1),
            "sha256" => Some(Self::Sha256),
            _ => None,
        }
    }

    pub const fn zero(self) -> ObjectId {
        match self {
            Self::Sha1 => ObjectId::Sha1([0; 20]),
            Self::Sha256 => ObjectId::Sha256([0; 32]),
        }
    }
}

/// A Git object ID carrying its hash format without heap allocation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ObjectId {
    Sha1([u8; 20]),
    Sha256([u8; 32]),
}

#[derive(Debug, thiserror::Error)]
#[error("Git object ID must contain 20 or 32 bytes, or 40 or 64 hexadecimal digits")]
pub struct ObjectIdError;

impl ObjectId {
    pub const fn format(self) -> ObjectFormat {
        match self {
            Self::Sha1(_) => ObjectFormat::Sha1,
            Self::Sha256(_) => ObjectFormat::Sha256,
        }
    }

    pub fn from_hex(text: impl AsRef<[u8]>) -> Result<Self, ObjectIdError> {
        let text = text.as_ref();
        match text.len() {
            40 => {
                let mut bytes = [0; 20];
                hex::decode_to_slice(text, &mut bytes).map_err(|_| ObjectIdError)?;
                Ok(Self::Sha1(bytes))
            }
            64 => {
                let mut bytes = [0; 32];
                hex::decode_to_slice(text, &mut bytes).map_err(|_| ObjectIdError)?;
                Ok(Self::Sha256(bytes))
            }
            _ => Err(ObjectIdError),
        }
    }

    pub fn is_zero(self) -> bool {
        self.iter().all(|byte| *byte == 0)
    }
}

impl Deref for ObjectId {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}
impl AsRef<[u8]> for ObjectId {
    fn as_ref(&self) -> &[u8] {
        match self {
            Self::Sha1(bytes) => bytes,
            Self::Sha256(bytes) => bytes,
        }
    }
}
impl TryFrom<&[u8]> for ObjectId {
    type Error = ObjectIdError;
    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        match bytes.len() {
            20 => bytes.try_into().map(Self::Sha1).map_err(|_| ObjectIdError),
            32 => bytes
                .try_into()
                .map(Self::Sha256)
                .map_err(|_| ObjectIdError),
            _ => Err(ObjectIdError),
        }
    }
}

impl TryFrom<Vec<u8>> for ObjectId {
    type Error = ObjectIdError;
    fn try_from(bytes: Vec<u8>) -> Result<Self, Self::Error> {
        Self::try_from(bytes.as_slice())
    }
}

pub(crate) enum ObjectHasher {
    Sha1(Sha1),
    Sha256(Sha256),
}
impl ObjectHasher {
    pub(crate) fn new(format: ObjectFormat, kind: ObjectKind, size: u64) -> Self {
        let mut hash = match format {
            ObjectFormat::Sha1 => Self::Sha1(Sha1::new()),
            ObjectFormat::Sha256 => Self::Sha256(Sha256::new()),
        };
        hash.update(format!("{} {size}\0", kind.git_name()).as_bytes());
        hash
    }
    pub(crate) fn update(&mut self, bytes: &[u8]) {
        match self {
            Self::Sha1(hash) => hash.update(bytes),
            Self::Sha256(hash) => hash.update(bytes),
        }
    }
    pub(crate) fn finalize(self) -> ObjectId {
        match self {
            Self::Sha1(hash) => ObjectId::Sha1(hash.finalize().into()),
            Self::Sha256(hash) => ObjectId::Sha256(hash.finalize().into()),
        }
    }
}

/// Computes the canonical Git ID for an object's format, type and bytes.
#[must_use]
pub fn object_id(format: ObjectFormat, kind: ObjectKind, body: &[u8]) -> ObjectId {
    let mut hash = ObjectHasher::new(format, kind, body.len() as u64);
    hash.update(body);
    hash.finalize()
}
