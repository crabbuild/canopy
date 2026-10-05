use super::super::metadata::{MetadataError, file_digest};
use super::*;
use crate::git_format::{ObjectHasher, pack_index::PackIndex};
use std::{fs::File, io::Read, path::Path};

/// The shared artifact context of every shard in one physical pack. This is
/// available before canonical inspection creates metadata. Descriptors alone
/// prove neither native validity, decoded bodies nor graph closure.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NativePackDescriptor {
    pub repository: [u8; 16],
    pub operation: [u8; 16],
    pub format: ObjectFormat,
    pub git_checksum: ObjectId,
    pub object_count: u32,
    pub pack: ArtifactDescriptor,
    pub index: ArtifactDescriptor,
}
impl NativePackDescriptor {
    pub fn validate(self, repository: [u8; 16], format: ObjectFormat) -> Result<(), IndexError> {
        let width = format.bytes() as u64;
        let index_min = 8 + 256 * 4 + u64::from(self.object_count) * (width + 8) + 2 * width;
        let index_max = index_min + u64::from(self.object_count) * 8;
        if self.repository != repository
            || self.format != format
            || self.git_checksum.format() != format
            || self.git_checksum.is_zero()
            || self.object_count == 0
            || self.pack.size < 12 + width
            || self.index.size < index_min
            || self.index.size > index_max
            || !(self.index.size - index_min).is_multiple_of(8)
            || [self.pack, self.index]
                .iter()
                .any(|artifact| artifact.size > canopy_object_storage::external::MAX_ARTIFACT_BYTES)
        {
            return Err(IndexError::Integrity);
        }
        Ok(())
    }
    pub fn key(self, kind: ArtifactKind) -> Result<ArtifactKey, IndexError> {
        if !matches!(kind, ArtifactKind::Pack | ArtifactKind::Index) {
            return Err(IndexError::Integrity);
        }
        Ok(ArtifactKey {
            operation: self.operation,
            binding_digest: self.pack.digest,
            kind,
        })
    }
    /// Bounded blocking artifact/header/checksum verification. Callers retain
    /// immutable admitted files. Native decoding and delta independence remain
    /// unproved until the isolated physical verifier completes.
    pub fn verify_files(
        self,
        pack_path: &Path,
        index_path: &Path,
    ) -> Result<VerifiedPackBinding, IndexError> {
        self.validate(self.repository, self.format)?;
        if file_digest(index_path, self.index.size)? != self.index.digest {
            return Err(IndexError::Integrity);
        }
        let index = PackIndex::open(index_path, self.format).map_err(MetadataError::from)?;
        if index.len() != self.object_count || index.pack_checksum() != self.git_checksum {
            return Err(IndexError::Integrity);
        }
        if pack_digest(
            pack_path,
            self.pack.size,
            self.git_checksum,
            self.object_count,
        )? != self.pack.digest
        {
            return Err(IndexError::Integrity);
        }
        Ok(VerifiedPackBinding {
            binding: self,
            index,
        })
    }
    /// Local precursor only: manifest digests are filled by authenticated
    /// upload before this descriptor escapes the capture service.
    /// A native receive of refs pointing to existing objects can produce an
    /// empty pack. Verify its index/header/trailer before excluding it from the
    /// incoming object inventory; zero-object sources remain forbidden.
    pub(crate) fn is_empty_pair(
        format: ObjectFormat,
        pack: &Path,
        index: &Path,
    ) -> Result<bool, IndexError> {
        let index = PackIndex::open(index, format).map_err(MetadataError::from)?;
        if !index.is_empty() {
            return Ok(false);
        }
        let size = std::fs::metadata(pack).map_err(MetadataError::from)?.len();
        if size != 12 + format.bytes() as u64 {
            return Err(IndexError::Integrity);
        }
        pack_digest(pack, size, index.pack_checksum(), 0)?;
        Ok(true)
    }
    pub(crate) fn inspect_files(
        repository: [u8; 16],
        operation: [u8; 16],
        format: ObjectFormat,
        pack_path: &Path,
        index_path: &Path,
    ) -> Result<Self, IndexError> {
        let pack_size = std::fs::metadata(pack_path)
            .map_err(MetadataError::from)?
            .len();
        let index_size = std::fs::metadata(index_path)
            .map_err(MetadataError::from)?
            .len();
        let index = PackIndex::open(index_path, format).map_err(MetadataError::from)?;
        let mut descriptor = Self {
            repository,
            operation,
            format,
            git_checksum: index.pack_checksum(),
            object_count: index.len(),
            pack: ArtifactDescriptor {
                size: pack_size,
                digest: [0; 32],
                manifest_digest: [0; 32],
            },
            index: ArtifactDescriptor {
                size: index_size,
                digest: [0; 32],
                manifest_digest: [0; 32],
            },
        };
        descriptor.validate(repository, format)?;
        descriptor.index.digest = file_digest(index_path, index_size)?;
        descriptor.pack.digest = pack_digest(
            pack_path,
            pack_size,
            descriptor.git_checksum,
            descriptor.object_count,
        )?;
        Ok(descriptor)
    }
}

fn pack_digest(
    pack_path: &Path,
    size: u64,
    git_checksum: ObjectId,
    object_count: u32,
) -> Result<[u8; 32], IndexError> {
    let format = git_checksum.format();
    let mut file = File::open(pack_path).map_err(MetadataError::from)?;
    if file.metadata().map_err(MetadataError::from)?.len() != size {
        return Err(IndexError::Integrity);
    }
    let mut header = [0; 12];
    file.read_exact(&mut header).map_err(MetadataError::from)?;
    let version = u32::from_be_bytes(header[4..8].try_into().map_err(|_| IndexError::Integrity)?);
    let count = u32::from_be_bytes(header[8..].try_into().map_err(|_| IndexError::Integrity)?);
    if &header[..4] != b"PACK" || !matches!(version, 2 | 3) || count != object_count {
        return Err(IndexError::Integrity);
    }
    let mut whole = blake3::Hasher::new();
    let mut native = ObjectHasher::raw(format);
    whole.update(&header);
    native.update(&header);
    let mut remaining = size - 12 - format.bytes() as u64;
    let mut buffer = [0; 64 << 10];
    while remaining > 0 {
        let length = remaining.min(buffer.len() as u64) as usize;
        file.read_exact(&mut buffer[..length])
            .map_err(MetadataError::from)?;
        whole.update(&buffer[..length]);
        native.update(&buffer[..length]);
        remaining -= length as u64;
    }
    let mut trailer = [0; 32];
    let trailer = &mut trailer[..format.bytes()];
    file.read_exact(trailer).map_err(MetadataError::from)?;
    whole.update(trailer);
    if trailer != git_checksum.as_ref()
        || native.finalize() != git_checksum
        || file.read(&mut buffer[..1]).map_err(MetadataError::from)? != 0
    {
        return Err(IndexError::Integrity);
    }
    Ok(*whole.finalize().as_bytes())
}
