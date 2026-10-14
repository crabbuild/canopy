//! Checked, file-backed Git v2 pack indexes with bounded-memory lookup.
//!
//! The caller must keep an index immutable for this handle's lifetime. The index
//! checksum verifies its bytes, not the pack's decoded objects: admission still
//! requires native pack verification and canonical object verification.

use crate::{ObjectFormat, ObjectHasher, ObjectId};
use std::{
    fs::File,
    io::{self, Read, Seek, SeekFrom},
    path::Path,
};

const HEADER: u64 = 8 + 256 * 4;
const PAGE: usize = 64 * 1024;

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

/// Immutable native index. Heap size is independent of the number of objects.
pub struct PackIndex {
    file: File,
    format: ObjectFormat,
    fanout: [u32; 256],
    count: u32,
    offsets: u64,
    large_offsets: u64,
    large_count: u64,
    checksum: ObjectId,
}

/// An indexed object; offsets/CRCs are native hints, never publication authority.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct IndexEntry {
    pub oid: ObjectId,
    pub offset: u64,
    pub crc32: u32,
}

impl PackIndex {
    /// Validate signature, layout, native digest, sorted IDs, fanout and offsets.
    /// This intentionally rejects unsupported versions rather than guessing.
    pub fn open(path: impl AsRef<Path>, format: ObjectFormat) -> io::Result<Self> {
        let mut file = File::open(path)?;
        let length = file.metadata()?.len();
        let width = format.bytes() as u64;
        if length < HEADER + 2 * width {
            return Err(invalid("truncated Git pack index"));
        }
        let mut header = [0; HEADER as usize];
        file.read_exact(&mut header)?;
        if header[..8] != *b"\xfftOc\0\0\0\x02" {
            return Err(invalid("unsupported Git pack index version"));
        }
        let mut fanout = [0; 256];
        let mut previous = 0;
        for (bucket, value) in fanout.iter_mut().enumerate() {
            let start = 8 + bucket * 4;
            *value = u32::from_be_bytes(header[start..start + 4].try_into().unwrap());
            if *value < previous {
                return Err(invalid("nonmonotonic Git index fanout"));
            }
            previous = *value;
        }
        let count = fanout[255];
        let offsets = HEADER + u64::from(count) * (width + 4);
        let large_offsets = offsets + u64::from(count) * 4;
        let payload_end = length - 2 * width;
        if large_offsets > payload_end || !(payload_end - large_offsets).is_multiple_of(8) {
            return Err(invalid("invalid Git pack index length"));
        }
        let large_count = (payload_end - large_offsets) / 8;
        if large_count > u64::from(count) {
            return Err(invalid("excess large Git offsets"));
        }
        file.seek(SeekFrom::Start(length - 2 * width))?;
        let mut trailer = [0; 64];
        file.read_exact(&mut trailer[..(2 * width) as usize])?;
        let checksum = ObjectId::try_from(&trailer[..width as usize])
            .map_err(|_| invalid("invalid native pack checksum"))?;
        let expected_index = ObjectId::try_from(&trailer[width as usize..(2 * width) as usize])
            .map_err(|_| invalid("invalid native index checksum"))?;
        file.seek(SeekFrom::Start(0))?;
        let mut hash = ObjectHasher::raw(format);
        let mut remaining = length - width;
        let mut buffer = [0; PAGE];
        while remaining > 0 {
            let size = remaining.min(PAGE as u64) as usize;
            file.read_exact(&mut buffer[..size])?;
            hash.update(&buffer[..size]);
            remaining -= size as u64;
        }
        if hash.finalize() != expected_index {
            return Err(invalid("Git pack index checksum mismatch"));
        }
        let index = Self {
            file,
            format,
            fanout,
            count,
            offsets,
            large_offsets,
            large_count,
            checksum,
        };
        let mut actual = [0_u32; 256];
        let mut previous = None;
        for oid in index.ids() {
            let oid = oid?;
            if previous.is_some_and(|previous| previous >= oid) {
                return Err(invalid("Git index object IDs must be unique and sorted"));
            }
            actual[oid[0] as usize] += 1;
            previous = Some(oid);
        }
        let mut total = 0;
        for (count, expected) in actual.iter().zip(index.fanout) {
            total += count;
            if total != expected {
                return Err(invalid("Git index fanout does not match object IDs"));
            }
        }
        // Validate 32-bit offsets sequentially. Large offset references are read
        // by position; no array proportional to the object count is allocated.
        let mut at = index.offsets;
        let mut remaining = u64::from(index.count) * 4;
        let mut references = 0_u64;
        while remaining > 0 {
            let size = remaining.min(PAGE as u64) as usize;
            read_at(&index.file, &mut buffer[..size], at)?;
            for encoded in buffer[..size].as_chunks::<4>().0 {
                let raw = u32::from_be_bytes(*encoded);
                index.offset(raw)?;
                references += u64::from(raw & 0x8000_0000 != 0);
            }
            at += size as u64;
            remaining -= size as u64;
        }
        if references != large_count {
            return Err(invalid("unused large Git offsets"));
        }
        Ok(index)
    }

    pub fn len(&self) -> u32 {
        self.count
    }
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }
    pub fn format(&self) -> ObjectFormat {
        self.format
    }
    pub fn pack_checksum(&self) -> ObjectId {
        self.checksum
    }

    /// Iterate native hash order with one 64 KiB page, including IO errors.
    pub fn ids(&self) -> IndexIds<'_> {
        self.ids_at(0)
    }

    /// Scan native offsets in hash order using one fixed page. Callers may
    /// build an admitted disk index of packed extents without retaining an
    /// object-count-sized heap vector or performing a hash lookup per entry.
    pub fn offsets(&self) -> IndexOffsets<'_> {
        IndexOffsets {
            index: self,
            next: 0,
            buffer: Box::new([0; PAGE]),
            start: 0,
            end: 0,
            failed: false,
        }
    }

    /// Start a bounded sequential read at a checked native ordinal. Immutable
    /// metadata shards use this to cover contiguous ranges without rescanning
    /// earlier index entries or materializing all IDs.
    pub fn ids_from(&self, ordinal: u32) -> io::Result<IndexIds<'_>> {
        if ordinal > self.count {
            return Err(invalid("Git index ordinal out of range"));
        }
        Ok(self.ids_at(ordinal))
    }

    fn ids_at(&self, ordinal: u32) -> IndexIds<'_> {
        IndexIds {
            index: self,
            next: ordinal,
            buffer: Box::new([0; PAGE]),
            start: 0,
            end: 0,
            failed: false,
        }
    }

    /// Check native membership without reading offsets or CRCs.
    pub fn contains(&self, oid: ObjectId) -> io::Result<bool> {
        Ok(self.position(oid)?.is_some())
    }

    /// Fanout narrows binary search without constructing a heap OID inventory.
    pub fn find(&self, oid: ObjectId) -> io::Result<Option<IndexEntry>> {
        let Some(position) = self.position(oid)? else {
            return Ok(None);
        };
        let mut encoded = [0; 4];
        read_at(
            &self.file,
            &mut encoded,
            self.offsets + u64::from(position) * 4,
        )?;
        let offset = self.offset(u32::from_be_bytes(encoded))?;
        read_at(
            &self.file,
            &mut encoded,
            HEADER + u64::from(self.count) * self.format.bytes() as u64 + u64::from(position) * 4,
        )?;
        Ok(Some(IndexEntry {
            oid,
            offset,
            crc32: u32::from_be_bytes(encoded),
        }))
    }

    fn position(&self, oid: ObjectId) -> io::Result<Option<u32>> {
        if oid.format() != self.format {
            return Ok(None);
        }
        let bucket = oid[0] as usize;
        let mut low = if bucket == 0 {
            0
        } else {
            self.fanout[bucket - 1]
        };
        let mut high = self.fanout[bucket];
        let mut candidate = [0; 32];
        let width = self.format.bytes();
        while low < high {
            let mid = low + (high - low) / 2;
            read_at(
                &self.file,
                &mut candidate[..width],
                HEADER + u64::from(mid) * width as u64,
            )?;
            match candidate[..width].cmp(oid.as_ref()) {
                std::cmp::Ordering::Less => low = mid + 1,
                std::cmp::Ordering::Greater => high = mid,
                std::cmp::Ordering::Equal => return Ok(Some(mid)),
            }
        }
        Ok(None)
    }

    fn offset(&self, raw: u32) -> io::Result<u64> {
        let offset = if raw & 0x8000_0000 == 0 {
            u64::from(raw)
        } else {
            let ordinal = u64::from(raw & 0x7fff_ffff);
            if ordinal >= self.large_count {
                return Err(invalid("Git large offset out of range"));
            }
            let mut bytes = [0; 8];
            read_at(&self.file, &mut bytes, self.large_offsets + ordinal * 8)?;
            u64::from_be_bytes(bytes)
        };
        if offset < 12 {
            return Err(invalid("Git object offset precedes pack data"));
        }
        Ok(offset)
    }
}

/// Bounded sequential index reader. An IO error ends the iterator.
pub struct IndexIds<'a> {
    index: &'a PackIndex,
    next: u32,
    buffer: Box<[u8; PAGE]>,
    start: usize,
    end: usize,
    failed: bool,
}

pub struct IndexOffsets<'a> {
    index: &'a PackIndex,
    next: u32,
    buffer: Box<[u8; PAGE]>,
    start: usize,
    end: usize,
    failed: bool,
}
impl Iterator for IndexOffsets<'_> {
    type Item = io::Result<u64>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.failed || self.next == self.index.count {
            return None;
        }
        if self.start == self.end {
            let records = (self.index.count - self.next).min((PAGE / 4) as u32) as usize;
            self.end = records * 4;
            self.start = 0;
            if let Err(error) = read_at(
                &self.index.file,
                &mut self.buffer[..self.end],
                self.index.offsets + u64::from(self.next) * 4,
            ) {
                self.failed = true;
                return Some(Err(error));
            }
        }
        let mut encoded = [0; 4];
        encoded.copy_from_slice(&self.buffer[self.start..self.start + 4]);
        self.start += 4;
        self.next += 1;
        let result = self.index.offset(u32::from_be_bytes(encoded));
        if result.is_err() {
            self.failed = true;
        }
        Some(result)
    }
}
impl Iterator for IndexIds<'_> {
    type Item = io::Result<ObjectId>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.failed || self.next == self.index.count {
            return None;
        }
        let width = self.index.format.bytes();
        if self.start == self.end {
            let records = (self.index.count - self.next).min((PAGE / width) as u32) as usize;
            self.end = records * width;
            self.start = 0;
            if let Err(error) = read_at(
                &self.index.file,
                &mut self.buffer[..self.end],
                HEADER + u64::from(self.next) * width as u64,
            ) {
                self.failed = true;
                return Some(Err(error));
            }
        }
        let oid = ObjectId::try_from(&self.buffer[self.start..self.start + width])
            .map_err(|_| invalid("invalid Git index object ID"));
        self.start += width;
        self.next += 1;
        Some(oid)
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = if self.failed {
            0
        } else {
            (self.index.count - self.next) as usize
        };
        (0, Some(remaining))
    }
}

#[cfg(unix)]
fn read_at(file: &File, bytes: &mut [u8], offset: u64) -> io::Result<()> {
    std::os::unix::fs::FileExt::read_exact_at(file, bytes, offset)
}
#[cfg(windows)]
fn read_at(file: &File, mut bytes: &mut [u8], mut offset: u64) -> io::Result<()> {
    use std::os::windows::fs::FileExt;
    while !bytes.is_empty() {
        match file.seek_read(bytes, offset) {
            Ok(0) => return Err(io::ErrorKind::UnexpectedEof.into()),
            Ok(n) => {
                offset += n as u64;
                bytes = &mut bytes[n..];
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
