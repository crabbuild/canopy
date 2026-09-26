use std::{net::SocketAddr, path::PathBuf};

use canopy_server::{
    CanopyApplication, build_descriptor,
    deployment::{Deployment, RecoveryConfig},
    server::{CanopyServer, ServerConfig, ServerError},
};
use cellule_app::CellApplication;
use cellule_runtime::{ApplicationId, ApplicationIdentity, Digest, NodeId, RequestId, TenantId};
use cellule_store::{StorageError, Store, provider_store::build_url_object_store};
use ed25519_dalek::SigningKey;
use serde::Deserialize;
use thiserror::Error;
use uuid::Uuid;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FileConfig {
    storage_url: String,
    tenant_id: String,
    application_id: String,
    node_id: String,
    fleet_digest: String,
    image_digest: String,
    owner: String,
    public_url: String,
    peer_endpoint: String,
    peer_ca_certificate: Option<PathBuf>,
    listen: String,
    data_dir: PathBuf,
    local_disk_limit_bytes: u64,
}

#[derive(Debug, Error)]
enum StartupError {
    #[error(
        "usage: canopy <config.json> | canopy maintenance <config.json> status | canopy maintenance <config.json> begin|recover|end <operation-uuid>"
    )]
    Usage,
    #[error("cannot read configuration")]
    ConfigIo(#[from] std::io::Error),
    #[error("configuration JSON is invalid")]
    ConfigJson(#[from] serde_json::Error),
    #[error("required secret environment variable is missing: {0}")]
    MissingSecret(&'static str),
    #[error("configuration UUID is invalid")]
    Uuid(#[from] uuid::Error),
    #[error("configuration digest or signing key is invalid")]
    Hex(#[from] hex::FromHexError),
    #[error("configuration address is invalid")]
    Address(#[from] std::net::AddrParseError),
    #[error("configuration value has the wrong byte length")]
    Length,
    #[error("object-store configuration failed")]
    Storage(#[from] StorageError),
    #[error("deployment administration failed")]
    Deployment(#[from] cellule_runtime::Error),
    #[error("Canopy service failed")]
    Server(#[from] ServerError),
}

#[tokio::main]
async fn main() -> Result<(), StartupError> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let mut args = std::env::args_os();
    let _ = args.next();
    let Some(first) = args.next() else {
        return Err(StartupError::Usage);
    };
    if first == "maintenance" {
        let path = args.next().ok_or(StartupError::Usage)?;
        let action = args.next().ok_or(StartupError::Usage)?;
        let operation = args.next();
        if args.next().is_some() {
            return Err(StartupError::Usage);
        }
        let file: FileConfig = serde_json::from_slice(&std::fs::read(path)?)?;
        return maintenance(file, action, operation).await;
    }
    if args.next().is_some() {
        return Err(StartupError::Usage);
    }
    let file: FileConfig = serde_json::from_slice(&std::fs::read(first)?)?;
    let token = std::env::var("CANOPY_GIT_TOKEN")
        .map_err(|_| StartupError::MissingSecret("CANOPY_GIT_TOKEN"))?;
    if token.is_empty() {
        return Err(StartupError::MissingSecret("CANOPY_GIT_TOKEN"));
    }
    let signing_key = signing_key()?;
    let provider = build_url_object_store(&file.storage_url)?;
    let config = ServerConfig {
        tenant: TenantId::from_bytes(Uuid::parse_str(&file.tenant_id)?.into_bytes()),
        application: ApplicationId::from_bytes(Uuid::parse_str(&file.application_id)?.into_bytes()),
        node: NodeId::from_bytes(Uuid::parse_str(&file.node_id)?.into_bytes()),
        fleet: Digest::from_bytes(decode_fixed(&file.fleet_digest)?),
        image: Digest::from_bytes(decode_fixed(&file.image_digest)?),
        signing_key,
        owner: file.owner,
        token,
        public_url: file.public_url,
        peer_endpoint: file.peer_endpoint,
        peer_ca_pem: file.peer_ca_certificate.map(std::fs::read).transpose()?,
        listen: file.listen.parse::<SocketAddr>()?,
        data_dir: file.data_dir,
        store_prefix: provider.prefix().clone(),
        local_disk_limit_bytes: file.local_disk_limit_bytes,
    };
    let server = CanopyServer::start(config, provider.store_arc()).await?;
    tracing::info!(address = %server.local_addr(), "Canopy is ready");
    server.serve_until(shutdown_signal()).await?;
    Ok(())
}

#[cfg(unix)]
async fn shutdown_signal() -> Result<(), std::io::Error> {
    use tokio::signal::unix::{SignalKind, signal};

    let mut terminate = signal(SignalKind::terminate())?;
    tokio::select! {
        result = tokio::signal::ctrl_c() => result,
        _ = terminate.recv() => Ok(()),
    }
}

#[cfg(not(unix))]
async fn shutdown_signal() -> Result<(), std::io::Error> {
    tokio::signal::ctrl_c().await
}

fn decode_fixed(value: &str) -> Result<[u8; 32], StartupError> {
    hex::decode(value)?
        .try_into()
        .map_err(|_| StartupError::Length)
}

fn signing_key() -> Result<SigningKey, StartupError> {
    let value = std::env::var("CANOPY_NODE_SIGNING_KEY_HEX")
        .map_err(|_| StartupError::MissingSecret("CANOPY_NODE_SIGNING_KEY_HEX"))?;
    Ok(SigningKey::from_bytes(&decode_fixed(&value)?))
}

async fn maintenance(
    file: FileConfig,
    action: std::ffi::OsString,
    operation: Option<std::ffi::OsString>,
) -> Result<(), StartupError> {
    let operation = match (action.to_str(), operation) {
        (Some("status"), None) => None,
        (Some("begin" | "recover" | "end"), Some(value)) => Some(RequestId::from_bytes(
            Uuid::parse_str(value.to_str().ok_or(StartupError::Usage)?)?.into_bytes(),
        )),
        _ => return Err(StartupError::Usage),
    };
    let application = CanopyApplication::compile(build_descriptor(
        include_bytes!("../Cargo.lock"),
        env!("CARGO_PKG_VERSION"),
    ))?;
    let provider = build_url_object_store(&file.storage_url)?;
    let deployment = Deployment::new(
        Store::new(provider.store_arc()),
        provider.prefix().clone(),
        ApplicationIdentity::new(
            TenantId::from_bytes(Uuid::parse_str(&file.tenant_id)?.into_bytes()),
            ApplicationId::from_bytes(Uuid::parse_str(&file.application_id)?.into_bytes()),
        ),
        Digest::from_bytes(decode_fixed(&file.fleet_digest)?),
        Digest::from_bytes(decode_fixed(&file.image_digest)?),
        application.registry(),
    )?;
    let now = || -> Result<i64, StartupError> {
        let elapsed = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|_| StartupError::Server(ServerError::Clock))?;
        i64::try_from(elapsed.as_millis()).map_err(|_| StartupError::Server(ServerError::Clock))
    };
    match (action.to_str(), operation) {
        (Some("begin"), Some(operation)) => {
            deployment.begin_maintenance(operation).await?;
        }
        (Some("recover"), Some(operation)) => {
            let config = RecoveryConfig {
                node: NodeId::from_bytes(Uuid::parse_str(&file.node_id)?.into_bytes()),
                signing_key: signing_key()?,
                endpoint: file.peer_endpoint,
                data_dir: file.data_dir,
                local_disk_limit_bytes: file.local_disk_limit_bytes,
            };
            deployment.recover_maintenance(operation, config).await?;
        }
        (Some("end"), Some(operation)) => {
            deployment.end_maintenance(operation, now()?).await?;
        }
        (Some("status"), None) => {}
        _ => return Err(StartupError::Usage),
    }
    println!(
        "{}",
        serde_json::to_string(&deployment.status(now()?).await?)?
    );
    Ok(())
}
