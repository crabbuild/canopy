use super::*;
use std::{
    io::Write,
    process::{Command, Stdio},
};

fn checksum(bytes: &[u8], format: ObjectFormat) -> ObjectId {
    let mut hash = ObjectHasher::raw(format);
    hash.update(bytes);
    hash.finalize()
}
fn fixture(format: ObjectFormat, count: usize) -> Vec<u8> {
    let width = format.bytes();
    let mut bytes = b"\xfftOc\0\0\0\x02".to_vec();
    let mut ids = Vec::new();
    for n in 0..count {
        let mut oid = vec![0; width];
        oid[..4].copy_from_slice(&(n as u32).to_be_bytes());
        ids.push(oid);
    }
    for bucket in 0..256 {
        let total = ids
            .iter()
            .filter(|oid| usize::from(oid[0]) <= bucket)
            .count() as u32;
        bytes.extend_from_slice(&total.to_be_bytes());
    }
    for oid in &ids {
        bytes.extend_from_slice(oid);
    }
    for _ in &ids {
        bytes.extend_from_slice(&0_u32.to_be_bytes());
    }
    for n in 0..count {
        bytes.extend_from_slice(&(12_u32 + n as u32).to_be_bytes());
    }
    bytes.extend_from_slice(&vec![7; width]);
    bytes.extend_from_slice(checksum(&bytes, format).as_ref());
    bytes
}
fn rehash(bytes: &mut [u8], format: ObjectFormat) {
    let last = bytes.len() - format.bytes();
    let digest = checksum(&bytes[..last], format);
    bytes[last..].copy_from_slice(digest.as_ref());
}
fn open(bytes: &[u8], format: ObjectFormat) -> io::Result<PackIndex> {
    let mut file = tempfile::NamedTempFile::new()?;
    file.write_all(bytes)?;
    PackIndex::open(file.path(), format)
}

#[test]
fn validates_empty_and_multi_page_indexes_for_both_formats() -> io::Result<()> {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let empty = open(&fixture(format, 0), format)?;
        assert!(empty.is_empty());
        let index = open(&fixture(format, 10_000), format)?;
        assert_eq!(index.len(), 10_000);
        assert_eq!(
            index.pack_checksum(),
            ObjectId::try_from(vec![7; format.bytes()]).unwrap()
        );
        for (n, oid) in index.ids().enumerate() {
            let oid = oid?;
            assert_eq!(u32::from_be_bytes(oid[..4].try_into().unwrap()), n as u32);
            assert_eq!(index.find(oid)?.unwrap().offset, 12 + n as u64);
        }
        // Shards start at a native ordinal, including positions crossing the
        // iterator's buffer boundary. The end is a valid empty iterator.
        for start in [0, 1, 2_049, 4_097, 9_999, 10_000] {
            let mut count = 0;
            for (n, oid) in index.ids_from(start)?.enumerate() {
                let oid = oid?;
                assert_eq!(
                    u32::from_be_bytes(oid[..4].try_into().unwrap()),
                    start + n as u32
                );
                count += 1;
            }
            assert_eq!(count, 10_000 - start);
        }
        assert!(index.ids_from(10_001).is_err());
        assert!(empty.ids_from(1).is_err());
        assert!(index.find(format.zero())?.is_some());
        let missing = ObjectId::try_from(vec![255; format.bytes()]).unwrap();
        assert!(index.find(missing)?.is_none());
        assert!(
            index
                .find(if format == ObjectFormat::Sha1 {
                    ObjectFormat::Sha256.zero()
                } else {
                    ObjectFormat::Sha1.zero()
                })?
                .is_none()
        );
    }
    Ok(())
}

#[test]
fn rejects_truncation_and_checksum_tampering() {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let bytes = fixture(format, 3);
        for length in [0, 8, 1031, 1032, bytes.len() - 1] {
            assert!(open(&bytes[..length], format).is_err());
        }
        let mut altered = bytes.clone();
        altered[HEADER as usize] ^= 1;
        assert!(open(&altered, format).is_err());
        let mut altered = bytes;
        let last = altered.len() - 1;
        altered[last] ^= 1;
        assert!(open(&altered, format).is_err());
    }
}

#[test]
fn rejects_structural_corruption_even_with_recomputed_checksum() {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let original = fixture(format, 3);
        for field in [
            4,
            8,
            HEADER as usize,
            HEADER as usize + format.bytes(),
            HEADER as usize + 3 * (format.bytes() + 4),
        ] {
            let mut bytes = original.clone();
            match field {
                4 => bytes[7] = 3,                                       // unsupported version
                8 => bytes[8..12].copy_from_slice(&2_u32.to_be_bytes()), // wrong fanout
                n if n == HEADER as usize => bytes[n] = 255,             // unsorted IDs
                n if n == HEADER as usize + format.bytes() => {
                    // duplicate IDs
                    let first = bytes[HEADER as usize..HEADER as usize + format.bytes()].to_vec();
                    bytes[n..n + format.bytes()].copy_from_slice(&first);
                }
                n => bytes[n..n + 4].copy_from_slice(&0_u32.to_be_bytes()), // invalid offset
            }
            rehash(&mut bytes, format);
            assert!(open(&bytes, format).is_err(), "accepted mutation {field}");
        }
        let mut bytes = original;
        let end = bytes.len() - 2 * format.bytes();
        bytes.splice(end..end, [0; 8]); // unreferenced offset
        rehash(&mut bytes, format);
        assert!(open(&bytes, format).is_err());
    }
}

#[test]
fn supports_large_offsets_and_rejects_out_of_range_references() -> io::Result<()> {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let mut bytes = fixture(format, 1);
        let offsets = HEADER as usize + format.bytes() + 4;
        bytes[offsets..offsets + 4].copy_from_slice(&0x8000_0000_u32.to_be_bytes());
        let end = bytes.len() - 2 * format.bytes();
        bytes.splice(end..end, 0x1_0000_0010_u64.to_be_bytes());
        rehash(&mut bytes, format);
        let index = open(&bytes, format)?;
        assert_eq!(index.find(format.zero())?.unwrap().offset, 0x1_0000_0010);
        bytes[offsets..offsets + 4].copy_from_slice(&0x8000_0001_u32.to_be_bytes());
        rehash(&mut bytes, format);
        assert!(open(&bytes, format).is_err());
    }
    Ok(())
}

#[test]
fn lookup_is_safe_under_concurrent_reads() -> io::Result<()> {
    let index = std::sync::Arc::new(open(
        &fixture(ObjectFormat::Sha256, 10_000),
        ObjectFormat::Sha256,
    )?);
    let threads: Vec<_> = (0..4)
        .map(|_| {
            let index = std::sync::Arc::clone(&index);
            std::thread::spawn(move || {
                for oid in index.ids() {
                    let oid = oid.unwrap();
                    assert!(index.find(oid).unwrap().is_some());
                }
            })
        })
        .collect();
    for thread in threads {
        thread.join().unwrap();
    }
    Ok(())
}

#[test]
fn reads_stock_git_indexes_and_agrees_with_show_index() -> Result<(), Box<dyn std::error::Error>> {
    fn git(root: &Path, args: &[&str], input: &[u8]) -> io::Result<Vec<u8>> {
        let mut child = Command::new("git")
            .arg("-C")
            .arg(root)
            .args(args)
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env(
                "GIT_CONFIG_GLOBAL",
                if cfg!(windows) { "NUL" } else { "/dev/null" },
            )
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;
        child.stdin.take().unwrap().write_all(input)?;
        let output = child.wait_with_output()?;
        if !output.status.success() {
            return Err(io::Error::other(
                String::from_utf8_lossy(&output.stderr).into_owned(),
            ));
        }
        Ok(output.stdout)
    }
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let root = tempfile::TempDir::new()?;
        git(
            root.path(),
            &[
                "init",
                "--bare",
                &format!("--object-format={}", format.as_str()),
            ],
            b"",
        )?;
        let mut ids = Vec::new();
        for n in 0..32 {
            ids.extend_from_slice(&git(
                root.path(),
                &["hash-object", "-w", "--stdin"],
                format!("body {n}\n").as_bytes(),
            )?);
        }
        let hash = git(
            root.path(),
            &["pack-objects", "--index-version=2", "objects/pack/pack"],
            &ids,
        )?;
        let name = String::from_utf8(hash)?.trim().to_owned();
        let path = root.path().join(format!("objects/pack/pack-{name}.idx"));
        let index = PackIndex::open(&path, format)?;
        assert_eq!(index.pack_checksum(), ObjectId::from_hex(&name)?);
        let native = git(root.path(), &["show-index"], &std::fs::read(&path)?)?;
        for row in String::from_utf8(native)?.lines() {
            let mut fields = row.split_whitespace();
            let offset: u64 = fields.next().unwrap().parse()?;
            let oid = ObjectId::from_hex(fields.next().unwrap())?;
            let crc = u32::from_str_radix(fields.next().unwrap().trim_matches(['(', ')']), 16)?;
            assert_eq!(
                index.find(oid)?,
                Some(IndexEntry {
                    oid,
                    offset,
                    crc32: crc
                })
            );
        }
    }
    Ok(())
}
