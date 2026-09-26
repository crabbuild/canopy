use std::{net::SocketAddr, path::PathBuf};

use canopy_server::server::{ServerError, SingleRepositoryConfig, SingleRepositoryServer};
use cellule_runtime::{ApplicationId, Digest, NodeId, TenantId};
use cellule_store::{StorageError, provider_store::build_url_object_store};
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
    repository_id: String,
    node_id: String,
    fleet_digest: String,
    image_digest: String,
    owner: String,
    public_url: String,
    peer_endpoint: String,
    listen: String,
    data_dir: PathBuf,
    local_disk_limit_bytes: u64,
}

#[derive(Debug, Error)]
enum StartupError {
    #[error("usage: canopy <config.json>")]
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
    #[error("Canopy service failed")]
    Server(#[from] ServerError),
}

#[tokio::main]
async fn main() -> Result<(), StartupError> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let mut args = std::env::args_os();
    let _ = args.next();
    let Some(path) = args.next() else {
        return Err(StartupError::Usage);
    };
    if args.next().is_some() {
        return Err(StartupError::Usage);
    }
    let file: FileConfig = serde_json::from_slice(&std::fs::read(path)?)?;
    let token = std::env::var("CANOPY_GIT_TOKEN")
        .map_err(|_| StartupError::MissingSecret("CANOPY_GIT_TOKEN"))?;
    if token.is_empty() {
        return Err(StartupError::MissingSecret("CANOPY_GIT_TOKEN"));
    }
    let signing_key = std::env::var("CANOPY_NODE_SIGNING_KEY_HEX")
        .map_err(|_| StartupError::MissingSecret("CANOPY_NODE_SIGNING_KEY_HEX"))?;
    let signing_key = SigningKey::from_bytes(&decode_fixed(&signing_key)?);
    let provider = build_url_object_store(&file.storage_url)?;
    let config = SingleRepositoryConfig {
        tenant: TenantId::from_bytes(Uuid::parse_str(&file.tenant_id)?.into_bytes()),
        application: ApplicationId::from_bytes(Uuid::parse_str(&file.application_id)?.into_bytes()),
        repository: Uuid::parse_str(&file.repository_id)?.into_bytes(),
        node: NodeId::from_bytes(Uuid::parse_str(&file.node_id)?.into_bytes()),
        fleet: Digest::from_bytes(decode_fixed(&file.fleet_digest)?),
        image: Digest::from_bytes(decode_fixed(&file.image_digest)?),
        signing_key,
        owner: file.owner,
        token,
        public_url: file.public_url,
        peer_endpoint: file.peer_endpoint,
        listen: file.listen.parse::<SocketAddr>()?,
        data_dir: file.data_dir,
        store_prefix: provider.prefix().clone(),
        local_disk_limit_bytes: file.local_disk_limit_bytes,
    };
    let server = SingleRepositoryServer::start(config, provider.store_arc()).await?;
    tracing::info!(address = %server.local_addr(), "Canopy is ready");
    shutdown_signal().await?;
    server.shutdown().await?;
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
