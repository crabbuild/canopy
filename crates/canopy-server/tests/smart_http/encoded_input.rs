use super::*;
use std::io::Write;

fn gzip(bytes: &[u8]) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(bytes)?;
    Ok(encoder.finish()?)
}

pub async fn reject_corruption_and_expansion(
    repository: &RepositoryCell,
    budget: &DiskBudget,
    client: &reqwest::Client,
    url: &str,
    push: &[u8],
) -> Result<(), Box<dyn std::error::Error>> {
    let retained = budget.used();
    let valid = gzip(push)?;
    let mut checksum = valid.clone();
    let crc = checksum.len() - 8;
    checksum[crc] ^= 1;
    let mut trailing = valid.clone();
    trailing.extend(b"invalid trailing bytes");
    for wire in [checksum, valid[..valid.len() - 1].to_vec(), trailing] {
        let id = uuid::Uuid::new_v4().to_string();
        for _ in 0..2 {
            let response = client
                .post(format!("{url}/git-receive-pack"))
                .bearer_auth("local-test-token")
                .header("Content-Type", "application/x-git-receive-pack-request")
                .header("Content-Encoding", "gzip")
                .header("Idempotency-Key", &id)
                .body(wire.clone())
                .send()
                .await?;
            assert_eq!(response.status(), reqwest::StatusCode::BAD_REQUEST);
            assert_eq!(response.text().await?, "Invalid Git gzip stream");
            assert_eq!(budget.used(), retained);
        }
        assert!(
            repository
                .ref_state("refs/heads/quota", None)
                .await?
                .output
                .is_none()
        );
        assert!(repository.object_page(None).await?.output.is_empty());
    }
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    for _ in 0..1024 {
        encoder.write_all(&[b'x'; 64 * 1024])?;
    }
    encoder.write_all(b"x")?;
    let response = client
        .post(format!("{url}/git-upload-pack"))
        .bearer_auth("local-test-token")
        .header("Content-Type", "application/x-git-upload-pack-request")
        .header("Content-Encoding", "gzip")
        .body(encoder.finish()?)
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(budget.used(), retained);

    // A valid decoded request above Git CGI's default 10 MiB buffer remains
    // below Canopy's 64 MiB limit. No ref matches these long prefixes.
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    encoder.write_all(b"0014command=ls-refs\n0001")?;
    let prefix = format!("ref-prefix {}\n", "z".repeat(60_000));
    let packet = format!("{:04x}{prefix}", prefix.len() + 4);
    for _ in 0..190 {
        encoder.write_all(packet.as_bytes())?;
    }
    encoder.write_all(b"0000")?;
    assert!(packet.len() * 190 > 10 * 1024 * 1024);
    let response = client
        .post(format!("{url}/git-upload-pack"))
        .bearer_auth("local-test-token")
        .header("Content-Type", "application/x-git-upload-pack-request")
        .header("Content-Encoding", "gzip")
        .header("Git-Protocol", "version=2")
        .body(encoder.finish()?)
        .send()
        .await?
        .error_for_status()?
        .bytes()
        .await?;
    assert_eq!(response, b"0000".as_slice());
    Ok(())
}

pub async fn delete_with_admission_retry(
    repository: &RepositoryCell,
    budget: &DiskBudget,
    client: &reqwest::Client,
    url: &str,
    commit: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let command = format!(
        "{commit} {} refs/heads/quota\0report-status delete-refs\n",
        "0".repeat(40)
    );
    let wire = gzip(format!("{:04x}{command}0000", command.len() + 4).as_bytes())?;
    let id = uuid::Uuid::new_v4().to_string();
    let request = || {
        client
            .post(format!("{url}/git-receive-pack"))
            .bearer_auth("local-test-token")
            .header("Content-Type", "application/x-git-receive-pack-request")
            .header("Content-Encoding", "gzip")
            .header("Idempotency-Key", &id)
            .body(wire.clone())
    };
    let warm = budget.used();
    let generation = repository.refs_page("", None).await?.output.generation;
    let occupied = budget.try_reserve(budget.capacity() - warm - wire.len() as u64 - 1)?;
    let response = request().send().await?;
    assert_eq!(response.status(), reqwest::StatusCode::INSUFFICIENT_STORAGE);
    assert_eq!(budget.used(), occupied.bytes() + warm);
    assert_eq!(
        repository.refs_page("", None).await?.output.generation,
        generation
    );
    drop(occupied);
    let response = request().send().await?.error_for_status()?.bytes().await?;
    assert!(
        response
            .windows(b"ok refs/heads/quota".len())
            .any(|part| part == b"ok refs/heads/quota")
    );
    assert!(
        repository
            .ref_state("refs/heads/quota", None)
            .await?
            .output
            .is_some_and(|state| state.oid.is_none())
    );
    // Admit only the encoded spool: any repeated gzip expansion must fail.
    // Completed replay must resolve the original response before decoding.
    let occupied = budget.try_reserve(budget.capacity() - warm - wire.len() as u64)?;
    let replay = request().send().await?.error_for_status()?.bytes().await?;
    assert_eq!(replay, response);
    assert_eq!(budget.used(), occupied.bytes() + warm);
    drop(occupied);
    // Changing only gzip metadata preserves decoded commands, but is a
    // different encoded request and cannot reuse the completed operation ID.
    let mut changed = wire.clone();
    changed[4..8].copy_from_slice(&1u32.to_le_bytes());
    let conflict = client
        .post(format!("{url}/git-receive-pack"))
        .bearer_auth("local-test-token")
        .header("Content-Type", "application/x-git-receive-pack-request")
        .header("Content-Encoding", "gzip")
        .header("Idempotency-Key", &id)
        .body(changed)
        .send()
        .await?;
    assert_eq!(conflict.status(), reqwest::StatusCode::CONFLICT);
    assert_eq!(
        repository.refs_page("", None).await?.output.generation,
        generation + 1
    );
    assert_eq!(budget.used(), warm);
    Ok(())
}
