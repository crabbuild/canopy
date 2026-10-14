use super::*;
use std::path::PathBuf;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

pub(super) async fn git(directory: &Path, args: &[&str]) -> TestResult<Vec<u8>> {
    let output = crate::native_git::command(directory)?
        .args(args)
        .output()
        .await?;
    if !output.status.success() {
        return Err(String::from_utf8_lossy(&output.stderr).into_owned().into());
    }
    Ok(output.stdout)
}

pub(super) async fn fixture(
    format: ObjectFormat,
) -> TestResult<(tempfile::TempDir, PathBuf, PackIdentity)> {
    let directory = tempfile::TempDir::new()?;
    git(
        directory.path(),
        &[
            "init",
            "-b",
            "main",
            &format!("--object-format={}", format.as_str()),
        ],
    )
    .await?;
    git(
        directory.path(),
        &["config", "user.name", "Pack qualification"],
    )
    .await?;
    git(
        directory.path(),
        &["config", "user.email", "pack@example.invalid"],
    )
    .await?;
    for index in 0..12 {
        let mut body = b"immutable native pack and bounded delta decoding\n".repeat(2048);
        body[1000 + index] = index as u8;
        std::fs::write(directory.path().join("file.txt"), body)?;
        git(directory.path(), &["add", "file.txt"]).await?;
        git(
            directory.path(),
            &["commit", "-m", &format!("Revision {index}")],
        )
        .await?;
    }
    git(
        directory.path(),
        &["tag", "-a", "release", "-m", "Native annotated tag"],
    )
    .await?;
    let pack_path = directory.path().join("objects.pack");
    std::fs::write(
        &pack_path,
        git(
            directory.path(),
            &["pack-objects", "--all", "--stdout", "--depth=20"],
        )
        .await?,
    )?;
    git(
        directory.path(),
        &[
            "index-pack",
            "--strict",
            pack_path.to_str().ok_or("invalid test path")?,
        ],
    )
    .await?;
    let pack = std::fs::read(&pack_path)?;
    let index = std::fs::read(pack_path.with_extension("idx"))?;
    let identity = PackIdentity {
        format,
        pack_digest: *blake3::hash(&pack).as_bytes(),
        pack_bytes: pack.len() as u64,
        index_digest: *blake3::hash(&index).as_bytes(),
        index_bytes: index.len() as u64,
    };
    Ok((directory, pack_path, identity))
}

#[tokio::test]
async fn native_sha1_and_sha256_delta_packs_return_every_exact_git_object() -> TestResult {
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        let (directory, path, identity) = fixture(format).await?;
        let pack = GitPack::open(&path, identity.clone(), 1024 * 1024, 1024 * 1024)?;
        assert_eq!(pack.identity(), &identity);
        let mut deltas = 0;
        let objects: Vec<_> = pack.object_ids().collect::<Result<_, _>>()?;
        assert_eq!(objects.len(), 37);
        for oid in objects {
            let header = pack.header(oid)?.ok_or("missing pack header")?;
            deltas += usize::from(header.delta_depth > 0);
            let (kind, bytes) = pack
                .object(oid, 1024 * 1024)?
                .ok_or("missing pack object")?;
            let expected = git(
                directory.path(),
                &["cat-file", kind.git_name(), &hex::encode(oid)],
            )
            .await?;
            assert_eq!(bytes, expected);
            assert_eq!(header.size, bytes.len() as u64);
            assert_eq!(header.kind, kind);
        }
        assert!(deltas > 0, "the fixture must exercise real delta chains");
        assert!(pack.object(format.zero(), 1024)?.is_none());
        if format == ObjectFormat::Sha256 {
            assert!(matches!(
                pack.header(ObjectId::Sha1([1; 20])),
                Err(PackError::Integrity)
            ));
        }
    }
    Ok(())
}

#[tokio::test]
async fn identity_native_checksums_and_allocation_bounds_fail_closed() -> TestResult {
    let (directory, path, identity) = fixture(ObjectFormat::Sha1).await?;
    assert!(matches!(
        GitPack::open(&path, identity.clone(), 1, 1024),
        Err(PackError::Limit)
    ));
    let pack = GitPack::open(&path, identity.clone(), 1024 * 1024, 1024 * 1024)?;
    let oid = ObjectId::from_hex(
        git(directory.path(), &["rev-parse", "HEAD:file.txt"])
            .await?
            .strip_suffix(b"\n")
            .ok_or("missing newline")?,
    )?;
    assert!(matches!(pack.object(oid, 1), Err(PackError::Limit)));
    let bounded = GitPack::open(&path, identity.clone(), 1024 * 1024, 1)?;
    assert!(bounded.object(oid, 1024 * 1024).is_err());
    let mut wrong = identity.clone();
    wrong.pack_digest[0] ^= 1;
    assert!(matches!(
        GitPack::open(&path, wrong, 1024 * 1024, 1024),
        Err(PackError::Integrity)
    ));
    // Even a matching outer digest cannot hide a broken native pack checksum.
    let mut bytes = std::fs::read(&path)?;
    bytes[12] ^= 1;
    std::fs::write(directory.path().join("corrupt.pack"), &bytes)?;
    std::fs::copy(
        path.with_extension("idx"),
        directory.path().join("corrupt.idx"),
    )?;
    let mut corrupt = identity;
    corrupt.pack_digest = *blake3::hash(&bytes).as_bytes();
    assert!(matches!(
        GitPack::open(
            &directory.path().join("corrupt.pack"),
            corrupt,
            1024 * 1024,
            1024
        ),
        Err(PackError::Integrity)
    ));
    Ok(())
}
