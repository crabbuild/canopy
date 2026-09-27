//! Real-store qualification of idle active versus released repository Cells.

#[path = "idle_density/store.rs"]
mod store;

use canopy_server::server::{CanopyServer, ServerConfig};
use cellule_runtime::{ApplicationId, Digest, NodeId, TenantId};
use ed25519_dalek::SigningKey;
use object_store::ObjectStore;
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use std::{
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};
use store::MeasuredStore;
use uuid::Uuid;

type Result<T = ()> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
enum Error {
    #[error("usage: benchmark_idle <s3-url> <new-work-dir> <counts-csv> <window-seconds>=10")]
    Usage,
    #[error("benchmark invariant failed: {0}")]
    Invalid(&'static str),
    #[error("benchmark I/O failed")]
    Io(#[from] std::io::Error),
    #[error("benchmark HTTP operation failed")]
    Http(#[from] reqwest::Error),
    #[error("benchmark configuration URL failed")]
    Url(#[from] url::ParseError),
    #[error("benchmark object store failed")]
    Store(#[from] object_store::Error),
    #[error("Canopy server failed")]
    Server(#[from] canopy_server::server::ServerError),
    #[error("benchmark report failed")]
    Json(#[from] serde_json::Error),
}

struct Fixture {
    directory: PathBuf,
    tenant: TenantId,
    application: ApplicationId,
    prefix: object_store::path::Path,
    key: SigningKey,
    token: String,
    active_limit: usize,
}

impl Fixture {
    fn config(&self, name: &str) -> ServerConfig {
        ServerConfig {
            tenant: self.tenant,
            application: self.application,
            node: NodeId::from_bytes(Uuid::new_v4().into_bytes()),
            fleet: Digest::from_bytes([11; 32]),
            image: Digest::from_bytes([22; 32]),
            signing_key: self.key.clone(),
            owner: "canopy".into(),
            token: self.token.clone(),
            public_url: "http://127.0.0.1".into(),
            peer_endpoint: "https://idle.example.invalid".into(),
            peer_ca_pem: None,
            listen: ([127, 0, 0, 1], 0).into(),
            data_dir: self.directory.join(name),
            store_prefix: self.prefix.clone(),
            local_disk_limit_bytes: 1536 * 1024 * 1024,
            max_active_repositories: self.active_limit,
        }
    }
}

#[tokio::main]
async fn main() -> std::process::ExitCode {
    let (writer, _guard) = tracing_appender::non_blocking::NonBlockingBuilder::default()
        .buffered_lines_limit(256)
        .lossy(true)
        .finish(std::io::stderr());
    tracing_subscriber::fmt()
        .with_writer(writer)
        .with_env_filter("info")
        .init();
    match run().await {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            std::process::ExitCode::FAILURE
        }
    }
}

async fn run() -> Result {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args.len() != 4 {
        return Err(Error::Usage);
    }
    let url = url::Url::parse(&args[0])?;
    if url.scheme() != "s3"
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
    {
        return Err(Error::Usage);
    }
    let counts: Vec<usize> = args[2]
        .split(',')
        .map(str::parse)
        .collect::<std::result::Result<_, _>>()
        .map_err(|_| Error::Usage)?;
    let seconds: u64 = args[3].parse().map_err(|_| Error::Usage)?;
    if counts.is_empty()
        || counts.windows(2).any(|v| v[0] >= v[1])
        || counts.iter().any(|v| *v > 9999)
        || !(10..=60).contains(&seconds)
    {
        return Err(Error::Usage);
    }
    let active_limit = counts.last().copied().ok_or(Error::Usage)?.max(1);
    let directory = PathBuf::from(&args[1]);
    std::fs::create_dir(&directory)?;
    let options = std::env::vars()
        .flat_map(|(key, value)| {
            [
                (key.clone(), value.clone()),
                (key.to_ascii_lowercase(), value),
            ]
        })
        .chain(std::iter::once((
            "aws_copy_if_not_exists".into(),
            object_store::aws::S3CopyIfNotExists::Multipart.to_string(),
        )));
    let (inner, prefix) = object_store::parse_url_opts(&url, options)?;
    let measured = Arc::new(MeasuredStore::new(Arc::from(inner)));
    let mut key = [0; 32];
    key[..16].copy_from_slice(Uuid::new_v4().as_bytes());
    key[16..].copy_from_slice(Uuid::new_v4().as_bytes());
    let fixture = Fixture {
        directory,
        tenant: TenantId::from_bytes(Uuid::new_v4().into_bytes()),
        application: ApplicationId::from_bytes(Uuid::new_v4().into_bytes()),
        prefix: prefix.join(Uuid::new_v4().to_string()),
        key: SigningKey::from_bytes(&key),
        token: format!("cnp_{}", hex::encode(Sha256::digest(key))),
        active_limit,
    };
    let mut report = json!({"passed":false,"cell_capabilities":"SQL only","requested_counts":counts,"window_seconds":seconds,
        "binary_sha256":hex::encode(Sha256::digest(std::fs::read(std::env::current_exe()?)?)),"store_prefix":fixture.prefix.to_string(),
        "counting_boundary":"object_store API, not provider HTTP attempts; list/delete/multipart parts are not counted",
        "windows":[],"seeded_repositories":[],"shutdown_passed":false});
    let result = qualify(&fixture, &measured, &counts, seconds, &mut report).await;
    report["passed"] = json!(result.is_ok());
    if let Err(error) = &result {
        report["error"] = json!(error.to_string());
    }
    save(&fixture, &report)?;
    result
}

fn save(fixture: &Fixture, report: &Value) -> Result {
    std::fs::write(
        fixture.directory.join("report.json"),
        serde_json::to_vec_pretty(report)?,
    )?;
    Ok(())
}

async fn qualify(
    fixture: &Fixture,
    measured: &Arc<MeasuredStore>,
    counts: &[usize],
    seconds: u64,
    report: &mut Value,
) -> Result {
    let backend: Arc<dyn ObjectStore> = measured.clone();
    let server = CanopyServer::start(fixture.config("active"), backend.clone()).await?;
    let base = format!("http://{}", server.local_addr());
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()?;
    let mut seeded = 0;
    let result = async {
        for &count in counts {
            while seeded < count {
                let name = format!("idle-{seeded:05}");
                let entry: Value = client
                    .post(format!("{base}/api/repositories"))
                    .bearer_auth(&fixture.token)
                    .json(&json!({"name":name}))
                    .send()
                    .await?
                    .error_for_status()?
                    .json()
                    .await?;
                if entry["name"] != name || entry["repository_id"].as_str().is_none() {
                    return Err(Error::Invalid("seed identity"));
                }
                report["seeded_repositories"]
                    .as_array_mut()
                    .ok_or(Error::Invalid("report array"))?
                    .push(json!({"name":name,"repository_id":entry["repository_id"]}));
                seeded += 1;
                if seeded % 100 == 0 {
                    println!("seeded {seeded}");
                    save(fixture, report)?;
                }
            }
            window(
                fixture,
                measured,
                count + 1,
                seconds,
                format!("{count} active repositories"),
                report,
            )
            .await?;
        }
        Ok(())
    }
    .await;
    let shutdown = server.shutdown().await;
    result?;
    shutdown?;
    let cold = CanopyServer::start(fixture.config("released"), backend).await?;
    let cold_base = format!("http://{}", cold.local_addr());
    let result = async {
        window(
            fixture,
            measured,
            1,
            seconds,
            format!("{seeded} released repositories"),
            report,
        )
        .await?;
        if seeded > 0 {
            for index in [0, seeded / 2, seeded - 1] {
                let name = format!("idle-{index:05}");
                let recovered: Value = client
                    .get(format!("{cold_base}/api/repositories/{name}"))
                    .bearer_auth(&fixture.token)
                    .send()
                    .await?
                    .error_for_status()?
                    .json()
                    .await?;
                if recovered["repository_id"]
                    != report["seeded_repositories"][index]["repository_id"]
                {
                    return Err(Error::Invalid("restored repository identity"));
                }
            }
        }
        Ok(())
    }
    .await;
    let shutdown = cold.shutdown().await;
    result?;
    shutdown?;
    report["shutdown_passed"] = json!(true);
    Ok(())
}

async fn window(
    fixture: &Fixture,
    measured: &MeasuredStore,
    expected: usize,
    seconds: u64,
    name: String,
    report: &mut Value,
) -> Result {
    // Allow foreground publication to settle before measuring autonomous work.
    tokio::time::sleep(Duration::from_secs(4)).await;
    let (before, unfinished_before) = measured.begin().await;
    let started = Instant::now();
    println!("measuring {name} for {seconds}s");
    tokio::time::sleep(Duration::from_secs(seconds)).await;
    let (counts, after, unfinished_after) = measured.end().await;
    let counts = counts.ok_or(Error::Invalid("measurement missing"))?;
    let elapsed = started.elapsed().as_secs_f64();
    let valid = before == expected
        && after == expected
        && counts.put_failed == 0
        && counts.updated_cells.len() == expected
        && counts.root_changes == 0;
    report["windows"].as_array_mut().ok_or(Error::Invalid("report windows"))?.push(json!({"name":name,"elapsed_seconds":elapsed,
        "serving_cells_before":before,"serving_cells_after":after,"unfinished_puts_before":unfinished_before,"unfinished_puts_after":unfinished_after,
        "control_updates_per_second":counts.conditional_control_updates as f64/elapsed,"counts":counts,"valid":valid}));
    save(fixture, report)?;
    println!(
        "finished {name}: {:.3} control updates/s",
        counts.conditional_control_updates as f64 / elapsed
    );
    if !valid {
        return Err(Error::Invalid(
            "idle window ownership, renewal or publication",
        ));
    }
    Ok(())
}
