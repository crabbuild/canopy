use super::*;

pub async fn verify(
    root: &Path,
    repository: &RepositoryCell,
    budget: &DiskBudget,
    client: &reqwest::Client,
    url: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let source = root.join("quota-source");
    run_git(
        None,
        &["init", "-b", "main", source.to_str().ok_or("invalid path")?],
    )
    .await?;
    run_git(Some(&source), &["config", "user.name", "Canopy Test"]).await?;
    run_git(
        Some(&source),
        &["config", "user.email", "canopy@example.invalid"],
    )
    .await?;
    // A small delta pack expands into many loose objects below Git's unpack limit.
    // This distinguishes native cache admission from request-spool admission.
    let mut common = vec![0; 8192];
    blake3::Hasher::new().finalize_xof().fill(&mut common);
    for index in 0..40 {
        let mut body = common.clone();
        body.extend_from_slice(format!("file {index}\n").as_bytes());
        tokio::fs::write(source.join(format!("file-{index:02}")), body).await?;
    }
    run_git(Some(&source), &["add", "."]).await?;
    run_git(Some(&source), &["commit", "-m", "Cache admission fixture"]).await?;
    let commit = run_git(Some(&source), &["rev-parse", "HEAD"]).await?;
    let commit = std::str::from_utf8(&commit)?.trim();
    let pack = run_git(
        Some(&source),
        &["pack-objects", "--stdout", "--all", "--window=50"],
    )
    .await?;
    assert!(
        pack.len() < common.len() * 4,
        "fixture requires delta compression"
    );
    let command = format!(
        "{} {commit} refs/heads/quota\0report-status\n",
        "0".repeat(40)
    );
    let mut body = format!("{:04x}{command}0000", command.len() + 4).into_bytes();
    body.extend_from_slice(&pack);
    encoded_input::reject_corruption_and_expansion(repository, budget, client, url, &body).await?;
    let id = uuid::Uuid::new_v4().to_string();
    let request = || {
        client
            .post(format!("{url}/git-receive-pack"))
            .bearer_auth("local-test-token")
            .header("Content-Type", "application/x-git-receive-pack-request")
            .header("Idempotency-Key", &id)
            .body(body.clone())
    };
    let before = repository.refs_page("", None).await?.output.generation;
    let occupied = budget.try_reserve(budget.capacity() - body.len() as u64 - 4096)?;
    let response = request().send().await?;
    assert_eq!(response.status(), reqwest::StatusCode::INSUFFICIENT_STORAGE);
    assert_eq!(response.text().await?, "Git cache disk budget exhausted");
    assert!(
        repository
            .ref_state("refs/heads/quota", None)
            .await?
            .output
            .is_none()
    );
    assert!(repository.object_page(None).await?.output.is_empty());
    assert_eq!(budget.used(), occupied.bytes());
    assert_eq!(
        repository.refs_page("", None).await?.output.generation,
        before
    );
    drop(occupied);

    let response = request().send().await?.error_for_status()?.bytes().await?;
    assert!(
        response
            .windows(b"ok refs/heads/quota".len())
            .any(|part| part == b"ok refs/heads/quota")
    );
    let expected: [u8; 20] = hex::decode(commit)?.try_into().map_err(|_| "invalid OID")?;
    assert_eq!(
        repository
            .ref_state("refs/heads/quota", None)
            .await?
            .output
            .and_then(|state| state.oid),
        Some(expected)
    );
    assert_eq!(
        repository.refs_page("", None).await?.output.generation,
        before + 1
    );
    let replay = request()
        .header("Content-Encoding", "identity")
        .send()
        .await?
        .error_for_status()?
        .bytes()
        .await?;
    assert_eq!(replay, response);
    assert_eq!(
        repository.refs_page("", None).await?.output.generation,
        before + 1
    );
    assert_eq!(
        request()
            .header("Content-Encoding", "gzip")
            .send()
            .await?
            .status(),
        reqwest::StatusCode::CONFLICT
    );

    // This request has no uploaded pack. Its cache must hydrate from SQLite,
    // fail admission, clean up, then rebuild successfully once capacity is freed.
    let occupied = budget.try_reserve(budget.capacity() - 512)?;
    let advertisement = || {
        client
            .get(format!("{url}/info/refs?service=git-upload-pack"))
            .bearer_auth("local-test-token")
    };
    let response = advertisement().send().await?;
    assert_eq!(response.status(), reqwest::StatusCode::INSUFFICIENT_STORAGE);
    assert_eq!(response.text().await?, "Git cache disk budget exhausted");
    assert_eq!(budget.used(), occupied.bytes());
    drop(occupied);
    advertisement()
        .send()
        .await?
        .error_for_status()?
        .bytes()
        .await?;
    assert!(budget.used() > common.len() as u64 * 40);
    encoded_input::delete_with_admission_retry(repository, budget, client, url, commit).await?;
    assert_eq!(budget.used(), 0);
    Ok(())
}
