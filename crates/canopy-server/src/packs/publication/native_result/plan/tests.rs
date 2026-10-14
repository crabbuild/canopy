use super::*;
use std::io::Write;

fn framed(bytes: Vec<u8>, output: &mut Vec<u8>) {
    output.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
    output.extend(bytes);
}
fn header(count: usize) -> Vec<u8> {
    let mut e = BoundedEncoder::new(FRAME_BYTES).unwrap();
    e.write_bytes(DOMAIN).unwrap();
    e.write_text("owner").unwrap();
    e.write_count(count).unwrap();
    e.finish()
}
fn chunk(actor: &str, count: usize) -> Vec<u8> {
    let plan = crate::PushPlan {
        actor: actor.into(),
        updates: (0..count)
            .map(|n| crate::RefUpdate {
                name: format!("refs/heads/{n}"),
                expected: None,
                new_oid: Some(crate::ObjectId::try_from(&[1; 32][..]).unwrap()),
            })
            .collect(),
    };
    let mut e = BoundedEncoder::new(FRAME_BYTES).unwrap();
    plan.encode(&mut e).unwrap();
    e.finish()
}
#[tokio::test]
async fn authenticated_plan_recovery_rejects_malformed_frames_and_releases_disk()
-> Result<(), Box<dyn std::error::Error>> {
    let store = ArtifactStore::new(
        std::sync::Arc::new(object_store::memory::InMemory::new()),
        [1; 16],
    );
    let operation = *b"CANOPY01\0\0\0\0\0\0\0\x01";
    let directory = tempfile::TempDir::new()?;
    let budget = DiskBudget::new(1 << 20);
    let mut valid = Vec::new();
    framed(header(33), &mut valid);
    framed(chunk("owner", 32), &mut valid);
    framed(chunk("owner", 1), &mut valid);
    let mut cases = vec![vec![0; 4], ((FRAME_BYTES + 1).to_be_bytes()).to_vec()];
    let mut trailing = valid.clone();
    trailing.push(1);
    cases.push(trailing);
    cases.push(valid[..valid.len() - 1].to_vec());
    for (count, actor, chunk_count) in [
        (0, "owner", 1),
        (crate::refs::MAX_UPDATES + 1, "owner", 1),
        (33, "owner", 1),
        (1, "other", 1),
        (1, "owner", 2),
    ] {
        let mut bytes = Vec::new();
        framed(header(count), &mut bytes);
        framed(chunk(actor, chunk_count), &mut bytes);
        cases.push(bytes);
    }
    for bytes in cases {
        let digest = *blake3::hash(&bytes).as_bytes();
        let descriptor = retain_body(&store, operation, bytes).await?;
        assert_eq!(descriptor.digest, digest);
        assert!(
            reopen(&store, operation, descriptor, directory.path(), &budget)
                .await
                .is_err()
        );
        assert_eq!(budget.used(), 0);
    }
    let descriptor = retain_body(&store, operation, valid).await?;
    let recovered = reopen(&store, operation, descriptor, directory.path(), &budget).await?;
    assert_eq!(recovered.actor, "owner");
    assert_eq!(recovered.updates.len(), 33);
    assert_eq!(budget.used(), 0);
    // Disk rejection happens before a completion can escape and leaves no spool.
    assert!(
        reopen(
            &store,
            operation,
            descriptor,
            directory.path(),
            &DiskBudget::new(1)
        )
        .await
        .is_err()
    );
    assert_eq!(std::fs::read_dir(directory.path())?.count(), 0);
    // Also exercise a short length prefix independently of artifact integrity.
    let mut file = tempfile::tempfile()?;
    file.write_all(&[0, 0])?;
    std::io::Seek::rewind(&mut file)?;
    assert!(read(&mut file).is_err());
    Ok(())
}
