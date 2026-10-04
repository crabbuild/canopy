use super::*;
use bytes::Bytes;
use object_store::aws::{AmazonS3Builder, S3CopyIfNotExists};
use sha2::{Digest as _, Sha256};
use std::{
    pin::Pin,
    task::{Context, Poll},
};

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;
const AUTH: &str = "http.extraHeader=Authorization: Bearer local-test-token";
const PART: usize = 8 * 1024 * 1024;

struct Repeated {
    bytes: Bytes,
    remaining: usize,
}
impl http_body::Body for Repeated {
    type Data = Bytes;
    type Error = std::io::Error;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<std::io::Result<http_body::Frame<Bytes>>>> {
        if self.remaining == 0 {
            return Poll::Ready(None);
        }
        self.remaining -= 1;
        Poll::Ready(Some(Ok(http_body::Frame::data(self.bytes.clone()))))
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "large transfer qualification; run on a dedicated disk with at least 40 GiB free"]
async fn push_and_database_exceed_512_mib_and_lfs_exceeds_5_gib_after_restore() -> Result {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN)
        .try_init();
    let workspace = tempfile::TempDir::new()?;
    let store: Arc<dyn ObjectStore> = Arc::new(
        AmazonS3Builder::new()
            .with_endpoint(std::env::var("CANOPY_TEST_S3_ENDPOINT")?)
            .with_bucket_name(std::env::var("CANOPY_TEST_S3_BUCKET")?)
            .with_region("us-east-1")
            .with_access_key_id("canopy-test-access")
            .with_secret_access_key("canopy-test-secret")
            .with_allow_http(true)
            .with_copy_if_not_exists(S3CopyIfNotExists::Multipart)
            .build()?,
    );
    let address = available_address().await?;
    let mut first = config(address, workspace.path().join("first"));
    first.local_disk_limit_bytes = 8 << 30;
    let server = CanopyServer::start(first, store.clone()).await?;
    let url = create_repository(address, "size").await?;
    let source = workspace.path().join("source");
    run_git(None, &["init", "-b", "main", path_str(&source)?]).await?;
    for (name, value) in [
        ("user.name", "Test"),
        ("user.email", "test@example.invalid"),
        ("commit.gpgsign", "false"),
    ] {
        run_git(Some(&source), &["config", name, value]).await?;
    }
    // Each incompressible blob stays in SQLite. The combined pack and database
    // both exceed their former caps, so externalization cannot conceal a quota.
    for index in 0_u64..900 {
        let mut bytes = vec![0; 640 * 1024];
        blake3::Hasher::new()
            .update(&index.to_le_bytes())
            .finalize_xof()
            .fill(&mut bytes);
        tokio::fs::write(source.join(format!("file-{index:04}")), bytes).await?;
    }
    let bytes = Bytes::from(vec![0; PART]);
    let parts = 641; // 5 GiB plus one transfer part.
    // Sparse zeros reduce fixture scratch usage; both remote bodies still transfer
    // every logical byte and are verified after fresh-disk recovery.
    tokio::fs::File::create(source.join("large-blob"))
        .await?
        .set_len((PART * parts) as u64)
        .await?;
    run_git(Some(&source), &["add", "."]).await?;
    run_git(Some(&source), &["commit", "-m", "Large SQLite repository"]).await?;
    run_git(Some(&source), &["repack", "-ad"]).await?;
    let mut packs = tokio::fs::read_dir(source.join(".git/objects/pack")).await?;
    let mut pack_bytes = 0;
    while let Some(entry) = packs.next_entry().await? {
        if entry
            .path()
            .extension()
            .is_some_and(|extension| extension == "pack")
        {
            pack_bytes += entry.metadata().await?.len();
        }
    }
    assert!(pack_bytes > 512 * 1024 * 1024);
    eprintln!("source pack: {pack_bytes} bytes");
    run_git(Some(&source), &["-c", AUTH, "push", &url, "main"]).await?;
    let expected = run_git(Some(&source), &["rev-parse", "HEAD"]).await?;
    tokio::fs::remove_dir_all(&source).await?;
    let database_root = workspace.path().join("first/canopy-pack-v1");
    let database_bytes = tokio::task::spawn_blocking(
        move || -> std::result::Result<u64, Box<dyn std::error::Error + Send + Sync>> {
            for entry in std::fs::read_dir(database_root)? {
                let path = entry?.path().join("repository.sqlite");
                if !path.is_file() {
                    continue;
                }
                let database = cellule_ltx::rusqlite::Connection::open_with_flags(
                    path,
                    cellule_ltx::rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
                )?;
                let pages: u64 = database.query_row("PRAGMA page_count", [], |row| row.get(0))?;
                let page_size: u64 =
                    database.query_row("PRAGMA page_size", [], |row| row.get(0))?;
                return Ok(pages * page_size);
            }
            Err("repository database was not found".into())
        },
    )
    .await?
    .map_err(|error| error as Box<dyn std::error::Error>)?;
    assert!(database_bytes > 512 * 1024 * 1024);
    eprintln!("large push published; SQLite database: {database_bytes} bytes");

    // Independent SHA-256 reference (Python hashlib): 641 blocks of 8 MiB zeros.
    let oid = "d10a354ad1d4a3bce953fe5678594a5158a8e27dad21b98fec2e6cd6db042778";
    let size = (PART * parts) as u64;
    let client = reqwest::Client::new();
    let batch: serde_json::Value = client
        .post(format!("{url}/info/lfs/objects/batch"))
        .bearer_auth("local-test-token")
        .json(&serde_json::json!({"operation":"upload","objects":[{"oid":oid,"size":size}]}))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert!(
        batch["objects"][0]["actions"]["upload"]["href"].is_string(),
        "{batch}"
    );
    client
        .put(format!("{url}/info/lfs/objects/{oid}"))
        .bearer_auth("local-test-token")
        .header("Content-Length", size)
        .body(reqwest::Body::wrap(Repeated {
            bytes,
            remaining: parts,
        }))
        .send()
        .await?
        .error_for_status()?;
    eprintln!("large LFS upload published");
    server.shutdown().await?;

    let address = available_address().await?;
    let mut restored = config(address, workspace.path().join("restored"));
    restored.local_disk_limit_bytes = 8 << 30;
    let server = CanopyServer::start(restored, store).await?;
    let url = format!("http://{address}/canopy/size.git");
    let clone = workspace.path().join("clone");
    run_git(None, &["-c", AUTH, "clone", &url, path_str(&clone)?]).await?;
    assert_eq!(
        run_git(Some(&clone), &["rev-parse", "HEAD"]).await?,
        expected
    );
    run_git(Some(&clone), &["fsck", "--strict", "--full"]).await?;
    eprintln!("fresh-server clone and strict fsck passed");
    let restored_blob = clone.join("large-blob");
    let restored_hash = tokio::task::spawn_blocking(move || {
        use std::io::Read;
        let mut file = std::fs::File::open(restored_blob)?;
        let mut buffer = vec![0; PART];
        let mut hash = Sha256::new();
        loop {
            let count = file.read(&mut buffer)?;
            if count == 0 {
                break;
            }
            hash.update(&buffer[..count]);
        }
        Ok::<_, std::io::Error>(hex::encode(hash.finalize()))
    })
    .await??;
    assert_eq!(restored_hash, oid);
    eprintln!("restored Git blob hash passed; downloading LFS body");
    let mut response = client
        .get(format!("{url}/info/lfs/objects/{oid}"))
        .bearer_auth("local-test-token")
        .send()
        .await?
        .error_for_status()?;
    assert_eq!(response.content_length(), Some(size));
    let mut actual = 0;
    let mut hash = Sha256::new();
    while let Some(bytes) = response.chunk().await? {
        actual += bytes.len() as u64;
        hash.update(bytes);
    }
    assert_eq!(actual, size);
    assert_eq!(hex::encode(hash.finalize()), oid);
    eprintln!("restored LFS download size and hash passed");
    server.shutdown().await?;
    Ok(())
}
