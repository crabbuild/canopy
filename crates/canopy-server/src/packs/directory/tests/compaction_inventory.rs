use super::*;

#[tokio::test]
async fn compaction_copy_checks_each_full_input_inventory_and_poisons_partial_merges() -> Result {
    let fixture = fixture(ObjectFormat::Sha256, 40).await?;
    let budget = DiskBudget::new(128 << 20);
    let source = segment(&fixture, budget.clone(), [1; 16])?;
    for fault in 0..3 {
        let mut writer = directory(&fixture, budget.clone())?;
        writer.add_segment(&source)?;
        let mut run = writer.seal()?;
        match fault {
            0 => run.descriptor.object_count += 1,
            1 => run.descriptor.inventory_digest[0] ^= 1,
            _ => run.descriptor.first_oid = ObjectId::Sha256([1; 32]),
        }
        assert!(matches!(
            run.verify_inventory(),
            Err(MetadataError::Integrity)
        ));
        let mut merged = directory(&fixture, budget.clone())?;
        assert!(matches!(
            merged.add_run(&run),
            Err(MetadataError::Integrity)
        ));
        assert!(matches!(
            merged.add_run(&run),
            Err(MetadataError::Integrity)
        ));
        assert!(matches!(merged.seal(), Err(MetadataError::Integrity)));
    }
    drop(source);
    assert_eq!(budget.used(), 0);
    Ok(())
}
