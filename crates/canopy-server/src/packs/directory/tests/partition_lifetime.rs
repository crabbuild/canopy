use super::*;

#[tokio::test]
async fn partition_output_retains_workspace_after_input_and_stream_drop() -> Result {
    let fixture = fixture(ObjectFormat::Sha256, 80).await?;
    let budget = DiskBudget::new(128 << 20);
    let source = segment(&fixture, budget.clone(), [1; 16])?;
    let root = tempfile::TempDir::new()?;
    let workspace = Arc::new(tempfile::TempDir::new_in(root.path())?);
    let path = workspace.path().to_owned();
    let mut writer = DirectoryBuilder::new(
        workspace.path(),
        budget.clone(),
        fixture.identity.repository,
        [80; 16],
        fixture.identity.format,
        limits(),
    )?;
    writer.retain_workspace(workspace);
    writer.add_segment(&source)?;
    let input = Arc::new(writer.seal()?);
    drop(source);
    let mut stream = DirectoryPartitioner::new(
        input,
        budget.clone(),
        MetadataLimits {
            max_file_bytes: 16 << 10,
            cache_kib: 16,
        },
    )?;
    let output = stream.next_run()?.ok_or("output")?;
    let entry = output.entries_after(None)?[0];
    drop(stream);
    assert!(path.exists() && output.path().exists());
    assert_eq!(budget.used(), output.descriptor().size);
    assert_eq!(output.find(entry.header.object.oid)?, Some(entry));
    drop(output);
    assert_eq!(budget.used(), 0);
    assert!(!path.exists());
    assert_eq!(std::fs::read_dir(root.path())?.count(), 0);
    Ok(())
}
