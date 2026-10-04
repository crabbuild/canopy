use super::*;
use crate::packs::verification::physical::tests::{Prepared, prepared_for_store};
use object_store::memory::InMemory;
type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

async fn packs(format: ObjectFormat, counts: &[usize]) -> Result<Vec<Prepared>> {
    let provider = Arc::new(InMemory::new());
    let store = Arc::new(ArtifactStore::new(provider.clone(), [1; 16]));
    let mut inputs = Vec::new();
    for (n, count) in counts.iter().enumerate() {
        let mut operation = *b"CANOPY0100000000";
        operation[8..].copy_from_slice(&(n as u64 + 700).to_be_bytes());
        inputs.push(
            prepared_for_store(format, *count, operation, provider.clone(), store.clone()).await?,
        );
    }
    Ok(inputs)
}
fn files(pack: &Prepared, budget: DiskBudget) -> Result<NativeFiles> {
    Ok(NativeFiles::new(
        Arc::new(tempfile::TempDir::new()?),
        budget,
        pack.store.clone(),
        pack.descriptor.format,
        crate::native_resources::NativeResources::default()
            .scope(crate::native_resources::NativeClass::Foreground),
    ))
}
#[tokio::test]
async fn native_pack_cache_evicts_idle_files_for_disk_pressure_and_reuses_verified_downloads()
-> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let inputs = packs(format, &[300, 301]).await?;
        let size = inputs
            .iter()
            .map(|p| p.descriptor.pack.size + p.descriptor.index.size)
            .max()
            .ok_or("pair")?;
        let budget = DiskBudget::new(size + 4096);
        let files = files(&inputs[0], budget.clone())?;
        drop(files.load(inputs[0].descriptor, Arc::new(())).await?);
        drop(files.load(inputs[0].descriptor, Arc::new(())).await?);
        assert_eq!(files.stats()?.downloaded_files, 1);
        drop(files.load(inputs[1].descriptor, Arc::new(())).await?);
        assert_eq!(files.stats()?.downloaded_files, 2);
        assert_eq!(files.stats()?.cached_files, 1);
        assert_eq!(files.stats()?.open_files, 1);
        drop(files);
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while budget.used() != 0 {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await?;
    }
    Ok(())
}
#[tokio::test]
async fn native_pack_slots_include_borrowed_evicted_files_and_refuse_capacity_without_deadlock()
-> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let inputs = packs(format, &[12, 13, 14, 15, 16, 17, 18, 19, 20]).await?;
        let files = files(&inputs[0], DiskBudget::new(64 << 20))?;
        let mut borrowed = Vec::new();
        for pack in inputs.iter().take(OPEN_PACKS) {
            borrowed.push(files.load(pack.descriptor, Arc::new(())).await?);
        }
        assert_eq!(files.stats()?.open_files, OPEN_PACKS);
        assert_eq!(files.stats()?.cached_files, CACHED_PACKS);
        assert!(matches!(
            files.load(inputs[8].descriptor, Arc::new(())).await,
            Err(NativeReadError::Capacity)
        ));
        borrowed.clear();
        let last = files.load(inputs[8].descriptor, Arc::new(())).await?;
        assert_eq!(files.stats()?.downloaded_files, 9);
        drop(last);
    }
    Ok(())
}
#[tokio::test]
async fn native_pack_failure_never_enters_cache_and_file_slot_follows_native_cache_ownership()
-> Result {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let inputs = packs(format, &[16]).await?;
        let descriptor = inputs[0].descriptor;
        let files = files(&inputs[0], DiskBudget::new(64 << 20))?;
        // Authenticated artifact bytes are intact; a false Git checksum must
        // still fail pack/index binding before the cache remembers a file.
        let mut wrong = descriptor;
        wrong.git_checksum = match format {
            ObjectFormat::Sha1 => crate::ObjectId::Sha1([9; 20]),
            ObjectFormat::Sha256 => crate::ObjectId::Sha256([9; 32]),
        };
        assert!(files.load(wrong, Arc::new(())).await.is_err());
        assert_eq!(files.stats()?.open_files, 0);
        assert_eq!(files.stats()?.downloaded_files, 0);
        let file = files.load(descriptor, Arc::new(())).await?;
        let mut reader =
            GitObjects::batch_owned(&file.cache.git_dir(), &files.native, file.cache.clone())?;
        let expected = inputs[0].fixture.objects.values().next().ok_or("object")?.0;
        reader.read_verified(expected, 1 << 20).await?;
        files.cache.lock().map_err(|_| "cache")?.clear();
        drop(file);
        assert_eq!(files.stats()?.cached_files, 0);
        assert_eq!(files.stats()?.open_files, 1);
        reader.finish().await?;
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while files.stats().expect("stats").open_files != 0 {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await?;
    }
    Ok(())
}
