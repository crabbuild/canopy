use super::*;

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

pub async fn verify(root: &Path, url: &str) -> Result {
    let fixture = tempfile::TempDir::new_in(root)?;
    let root = fixture.path();
    let source = root.join("native-source");
    let clone = root.join("native-clone");
    run_git(
        None,
        &["init", "-b", "pack-policy", source.to_str().ok_or("path")?],
    )
    .await?;
    run_git(Some(&source), &["config", "user.name", "Canopy Test"]).await?;
    run_git(
        Some(&source),
        &["config", "user.email", "canopy@example.invalid"],
    )
    .await?;
    let mut small = b"small text line\n".repeat(32768);
    let mut large = b"large text line\n".repeat(640000);
    let mut small_oids = Vec::new();
    let mut large_oids = Vec::new();
    for generation in 0..2 {
        small[0] = b'A' + generation;
        large[0] = b'A' + generation;
        tokio::fs::write(source.join("small.txt"), &small).await?;
        tokio::fs::write(source.join("large.txt"), &large).await?;
        run_git(Some(&source), &["add", "."]).await?;
        run_git(Some(&source), &["commit", "-m", "Related objects"]).await?;
        for (name, ids) in [
            ("HEAD:small.txt", &mut small_oids),
            ("HEAD:large.txt", &mut large_oids),
        ] {
            ids.push(
                String::from_utf8(run_git(Some(&source), &["rev-parse", name]).await?)?
                    .trim()
                    .to_owned(),
            );
        }
    }
    run_git(
        Some(&source),
        &[
            "-c",
            "http.extraHeader=Authorization: Bearer local-test-token",
            "push",
            url,
            "pack-policy",
        ],
    )
    .await?;
    run_git(
        None,
        &[
            "-c",
            "fetch.unpackLimit=1",
            "-c",
            "http.extraHeader=Authorization: Bearer local-test-token",
            "clone",
            "--single-branch",
            "--branch",
            "pack-policy",
            url,
            clone.to_str().ok_or("path")?,
        ],
    )
    .await?;
    assert_eq!(tokio::fs::read(clone.join("large.txt")).await?, large);
    assert_eq!(tokio::fs::read(clone.join("small.txt")).await?, small);
    run_git(Some(&clone), &["fsck", "--strict", "--full"]).await?;
    let index = std::fs::read_dir(clone.join(".git/objects/pack"))?
        .collect::<std::io::Result<Vec<_>>>()?
        .into_iter()
        .map(|entry| entry.path())
        .find(|path| path.extension().is_some_and(|extension| extension == "idx"))
        .ok_or("clone pack index")?;
    let listing = String::from_utf8(
        run_git(
            Some(&clone),
            &["verify-pack", "-v", index.to_str().ok_or("path")?],
        )
        .await?,
    )?;
    // Inspect the actual served pack: large similar blobs are whole objects,
    // while ordinary source-size blobs retain delta compression.
    for oid in large_oids {
        let fields: Vec<_> = listing
            .lines()
            .find(|line| line.starts_with(&oid))
            .ok_or("large object in pack")?
            .split_whitespace()
            .collect();
        assert_eq!(fields.len(), 5, "large object must not be a delta");
    }
    assert!(
        listing.lines().any(|line| {
            let fields: Vec<_> = line.split_whitespace().collect();
            fields.len() == 7 && small_oids.iter().any(|oid| fields[0] == oid)
        }),
        "ordinary blobs still use deltas"
    );
    Ok(())
}
