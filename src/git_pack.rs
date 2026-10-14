//! Verified native Git packs with bounded delta decoding.

use std::{fs::File, io::Read, path::Path};

use crate::{ObjectFormat, ObjectId, ObjectKind, object_id};
use serde::{Deserialize, Serialize};
use sha1::Digest as _;

#[derive(Debug, thiserror::Error)]
pub enum PackError {
    #[error("Git pack object-store operation failed")]
    Store(#[from] object_store::Error),
    #[error("Git pack disk admission failed")]
    Budget(#[from] cellule_ltx::LtxError),
    #[error("Git pack worker failed")]
    Task(#[from] tokio::task::JoinError),
    #[error("Git pack I/O failed")]
    Io(#[from] std::io::Error),
    #[error("Git pack decoding failed")]
    Decode(#[source] Box<dyn std::error::Error + Send + Sync>),
    #[error("Git pack integrity or object identity differs")]
    Integrity,
    #[error("Git pack read exceeds its allocation or output bound")]
    Limit,
    #[error("Git pack allocation failed")]
    Allocation(#[from] std::collections::TryReserveError),
}

impl PackError {
    pub(crate) fn is_admission(&self) -> bool {
        match self {
            Self::Budget(_) => true,
            Self::Io(error) => error
                .get_ref()
                .is_some_and(|source| source.is::<cellule_ltx::LtxError>()),
            Self::Decode(error) => error
                .downcast_ref::<crate::git_cache::CacheError>()
                .is_some_and(crate::git_cache::CacheError::is_admission),
            _ => false,
        }
    }
}

mod build;
mod store;
pub use store::{OwnedPack, PackStore};

/// Exact immutable pack/index bytes selected by repository metadata.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PackIdentity {
    pub format: ObjectFormat,
    pub pack_digest: [u8; 32],
    pub pack_bytes: u64,
    pub index_digest: [u8; 32],
    pub index_bytes: u64,
}

/// Object metadata obtained without expanding its complete delta chain.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PackObjectHeader {
    pub oid: ObjectId,
    pub kind: ObjectKind,
    pub size: u64,
    pub offset: u64,
    pub delta_depth: u32,
}

/// Private, immutable local pack owned by the repository's disk cache.
///
/// The owner must retain the files and their disk reservation until this value
/// and all readers close. Native Git never modifies these files in place.
pub struct GitPack {
    identity: PackIdentity,
    data: gix_pack::data::File,
    index: gix_pack::index::File<Box<[u8]>>,
}

impl GitPack {
    /// Opens an exact content-addressed pair. Index and delta allocations have
    /// explicit caller budgets; the pack body itself is read-only memory mapped.
    pub fn open(
        path: &Path,
        expected: PackIdentity,
        max_index_bytes: usize,
        max_decode_bytes: usize,
    ) -> Result<Self, PackError> {
        if expected.index_bytes > max_index_bytes as u64 || max_decode_bytes == 0 {
            return Err(PackError::Limit);
        }
        let index_path = path.with_extension("idx");
        let mut file = File::open(&index_path)?;
        if file.metadata()?.len() != expected.index_bytes {
            return Err(PackError::Integrity);
        }
        let index_length = usize::try_from(expected.index_bytes).map_err(|_| PackError::Limit)?;
        let mut index_bytes = Vec::new();
        index_bytes.try_reserve_exact(index_length)?;
        index_bytes.resize(index_length, 0);
        file.read_exact(&mut index_bytes)?;
        if file.read(&mut [0; 1])? != 0 {
            return Err(PackError::Integrity);
        }
        if index_bytes.len() as u64 != expected.index_bytes
            || *blake3::hash(&index_bytes).as_bytes() != expected.index_digest
        {
            return Err(PackError::Integrity);
        }
        verify_trailer(&index_bytes, expected.format)?;
        let index = gix_pack::index::File::from_data(
            index_bytes.into_boxed_slice(),
            index_path,
            hash_kind(expected.format),
        )
        .map_err(decode_error)?;
        let data = gix_pack::data::File::at(path, hash_kind(expected.format))
            .map_err(decode_error)?
            .with_alloc_limit_bytes(Some(max_decode_bytes));
        let bytes = data
            .entry_slice(0..data.data_len() as u64)
            .ok_or(PackError::Integrity)?;
        if bytes.len() as u64 != expected.pack_bytes
            || *blake3::hash(bytes).as_bytes() != expected.pack_digest
            || data.checksum() != index.pack_checksum()
            || data.num_objects() != index.num_objects()
        {
            return Err(PackError::Integrity);
        }
        verify_trailer(bytes, expected.format)?;
        Ok(Self {
            identity: expected,
            data,
            index,
        })
    }

    pub fn identity(&self) -> &PackIdentity {
        &self.identity
    }

    pub fn object_ids(&self) -> impl Iterator<Item = Result<ObjectId, PackError>> + '_ {
        self.index.iter().map(|entry| {
            entry
                .oid
                .as_bytes()
                .try_into()
                .map_err(|_| PackError::Integrity)
        })
    }

    /// Tests index membership without allocating or expanding a delta chain.
    pub fn contains(&self, oid: ObjectId) -> Result<bool, PackError> {
        Ok(self.offset(oid)?.is_some())
    }

    pub fn header(&self, oid: ObjectId) -> Result<Option<PackObjectHeader>, PackError> {
        let Some(offset) = self.offset(oid)? else {
            return Ok(None);
        };
        let header = self
            .data
            .decode_header(
                self.data.entry(offset).map_err(decode_error)?,
                &mut gix_zlib::Inflate::default(),
                &|oid| {
                    self.index
                        .lookup(oid)
                        .map(|index| self.data.entry(self.index.pack_offset_at_index(index)))
                        .transpose()
                        .map(|entry| {
                            entry.map(gix_pack::data::decode::header::ResolvedBase::InPack)
                        })
                },
            )
            .map_err(decode_error)?;
        Ok(Some(PackObjectHeader {
            oid,
            kind: parse_kind(header.kind.as_bytes())?,
            size: header.object_size,
            offset,
            delta_depth: header.num_deltas,
        }))
    }

    /// Decodes a self-contained native pack entry and checks its canonical Git
    /// object ID. Ref deltas may resolve only within this same verified pack.
    pub fn object(
        &self,
        oid: ObjectId,
        max_output_bytes: usize,
    ) -> Result<Option<(ObjectKind, Vec<u8>)>, PackError> {
        let Some(header) = self.header(oid)? else {
            return Ok(None);
        };
        if header.size > max_output_bytes as u64 {
            return Err(PackError::Limit);
        }
        let mut body = Vec::new();
        let outcome = self
            .data
            .decode_entry(
                self.data.entry(header.offset).map_err(decode_error)?,
                &mut body,
                &mut gix_zlib::Inflate::default(),
                &|oid, _| {
                    self.index
                        .lookup(oid)
                        .map(|index| self.data.entry(self.index.pack_offset_at_index(index)))
                        .transpose()
                        .map(|entry| entry.map(gix_pack::data::decode::entry::ResolvedBase::InPack))
                },
                &mut gix_pack::cache::Never,
            )
            .map_err(decode_error)?;
        let kind = parse_kind(outcome.kind.as_bytes())?;
        if kind != header.kind
            || body.len() as u64 != header.size
            || object_id(oid.format(), kind, &body) != oid
        {
            return Err(PackError::Integrity);
        }
        Ok(Some((kind, body)))
    }

    fn offset(&self, oid: ObjectId) -> Result<Option<u64>, PackError> {
        if oid.format() != self.identity.format {
            return Err(PackError::Integrity);
        }
        let oid = gix_hash::oid::try_from_bytes(&oid).map_err(decode_error)?;
        Ok(self
            .index
            .lookup(oid)
            .map(|index| self.index.pack_offset_at_index(index)))
    }
}

fn hash_kind(format: ObjectFormat) -> gix_hash::Kind {
    match format {
        ObjectFormat::Sha1 => gix_hash::Kind::Sha1,
        ObjectFormat::Sha256 => gix_hash::Kind::Sha256,
    }
}

fn parse_kind(kind: &[u8]) -> Result<ObjectKind, PackError> {
    match kind {
        b"blob" => Ok(ObjectKind::Blob),
        b"tree" => Ok(ObjectKind::Tree),
        b"commit" => Ok(ObjectKind::Commit),
        b"tag" => Ok(ObjectKind::Tag),
        _ => Err(PackError::Integrity),
    }
}

fn verify_trailer(bytes: &[u8], format: ObjectFormat) -> Result<(), PackError> {
    let length = bytes
        .len()
        .checked_sub(format.bytes())
        .ok_or(PackError::Integrity)?;
    let digest = match format {
        ObjectFormat::Sha1 => sha1::Sha1::digest(&bytes[..length]).to_vec(),
        ObjectFormat::Sha256 => sha2::Sha256::digest(&bytes[..length]).to_vec(),
    };
    if digest != bytes[length..] {
        return Err(PackError::Integrity);
    }
    Ok(())
}

fn decode_error(error: impl std::error::Error + Send + Sync + 'static) -> PackError {
    PackError::Decode(Box::new(error))
}

#[cfg(test)]
mod tests;
