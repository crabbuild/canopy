use super::*;
use crate::packs::sources::tests::source;
type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

fn spool(budget: &DiskBudget) -> Result<DescriptorSpool> {
    Ok(DescriptorSpool {
        file: AdmittedFile::new(tempfile::NamedTempFile::new()?, budget.try_reserve(0)?),
        bytes: 0,
    })
}

#[test]
fn descriptor_replay_reuses_source_codec_and_reserves_before_each_append() -> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let budget = DiskBudget::new(8 << 20);
        let mut spool = spool(&budget)?;
        let record = source(1, format);
        // Digest order is deliberately irrelevant to this sequential replay.
        // Each frame preserves the source codec, including artifact manifests.
        for ordinal in 0..5000 {
            let mut next = record;
            next.metadata.segment.identity.first_ordinal = ordinal;
            next.pack_object_count = 5001;
            next.index.size =
                8 + 256 * 4 + 5001 * (format.bytes() as u64 + 8) + 2 * format.bytes() as u64;
            spool.append(next, 8 << 20)?;
            assert_eq!(budget.used(), spool.bytes);
            assert_eq!(spool.file.file().as_file().metadata()?.len(), spool.bytes);
        }
        let size = spool.bytes;
        let mut native = record.native();
        native.object_count = 5001;
        native.index.size =
            8 + 256 * 4 + 5001 * (format.bytes() as u64 + 8) + 2 * format.bytes() as u64;
        let mut offset = 0;
        for ordinal in 0..5000 {
            let (metadata, end) = spool.read(native, offset, size)?.ok_or("record")?;
            assert_eq!(metadata.segment.identity.first_ordinal, ordinal);
            assert_eq!(metadata.artifact, record.metadata.artifact);
            offset = end;
        }
        assert_eq!(offset, size);
        assert!(spool.read(native, offset, size)?.is_none());
        let path = spool.file.file().path().to_owned();
        drop(spool);
        assert_eq!(budget.used(), 0);
        assert!(!path.exists());
    }
    Ok(())
}

#[test]
fn descriptor_replay_rejects_denied_growth_truncation_oversized_frames_and_foreign_binding()
-> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let budget = DiskBudget::new(512);
        let mut spool = spool(&budget)?;
        let record = source(1, format);
        spool.append(record, 1024)?;
        let charged = budget.used();
        let size = spool.bytes;
        assert!(matches!(
            spool.append(record, 1024),
            Err(PhysicalError::Metadata(MetadataError::Budget(_)))
        ));
        assert_eq!(budget.used(), charged);
        assert_eq!(spool.bytes, size);
        assert_eq!(spool.file.file().as_file().metadata()?.len(), size);
        assert!(matches!(
            spool.append(record, size),
            Err(PhysicalError::Limit)
        ));
        let mut foreign = record.native();
        foreign.pack.manifest_digest[0] ^= 1;
        assert!(spool.read(foreign, 0, size).is_err());
        assert!(spool.read(record.native(), size + 1, size).is_err());
        spool.file.file_mut().seek(SeekFrom::Start(0))?;
        spool.file.file_mut().write_all(&513u32.to_be_bytes())?;
        assert!(spool.read(record.native(), 0, size).is_err());
        spool.file.file().as_file().set_len(size - 1)?;
        assert!(spool.read(record.native(), 0, size).is_err());
        drop(spool);
        assert_eq!(budget.used(), 0);
    }
    Ok(())
}
