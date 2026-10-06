//! Production resident fixture shared by standalone transport qualifications.
use canopy_server::server::{CanopyServer, ServerConfig};
use cellule_runtime::{ApplicationId, Digest, TenantId, identity::NodeId};
use ed25519_dalek::SigningKey;
use object_store::{ObjectStore, path::Path as StorePath};
use std::{path::Path, sync::Arc};
use tokio::{net::TcpListener, process::Command};
pub type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;
pub const TOKEN: &str = "local-test-token";

pub async fn start(directory: &Path, store: Arc<dyn ObjectStore>) -> Result<CanopyServer> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let config = ServerConfig {
        tenant: TenantId::from_bytes([31; 16]),
        application: ApplicationId::from_bytes([32; 16]),
        node: NodeId::from_bytes(uuid::Uuid::new_v4().into_bytes()),
        fleet: Digest::from_bytes([35; 32]),
        image: Digest::from_bytes([36; 32]),
        signing_key: SigningKey::from_bytes(&[37; 32]),
        owner: "canopy".into(),
        token: TOKEN.into(),
        public_url: format!("http://{address}"),
        peer_endpoint: "https://native-fixture.invalid".into(),
        peer_ca_pem: None,
        listen: address,
        ssh: None,
        data_dir: directory.to_owned(),
        store_prefix: StorePath::from("native-standalone-fixture"),
        native_limits: canopy_server::native_resources::NativeLimits::default(),
        local_disk_limit_bytes: 1 << 30,
        max_active_repositories: 3,
    };
    Ok(CanopyServer::start_with_listener(config, store, listener).await?)
}
pub async fn create(server: &CanopyServer, name: &str, format: &str) -> Result<String> {
    let body: serde_json::Value = reqwest::Client::new()
        .post(format!("http://{}/api/repositories", server.local_addr()))
        .bearer_auth(TOKEN)
        .json(&serde_json::json!({"name":name,"object_format":format}))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    Ok(body["clone_url"]
        .as_str()
        .ok_or("clone URL missing")?
        .into())
}
pub fn path(path: &Path) -> Result<&str> {
    Ok(path.to_str().ok_or("non-UTF8 fixture path")?)
}
pub fn command(cwd: Option<&Path>, arguments: &[&str]) -> Command {
    let mut command = Command::new("git");
    command
        .args([
            "-c",
            "credential.helper=",
            "-c",
            "http.extraHeader=Authorization: Bearer local-test-token",
        ])
        .env("GIT_TERMINAL_PROMPT", "0");
    if let Some(cwd) = cwd {
        command.current_dir(cwd);
    }
    command.args(arguments);
    command
}
pub async fn git(cwd: Option<&Path>, arguments: &[&str]) -> Result<Vec<u8>> {
    let output = command(cwd, arguments).output().await?;
    if !output.status.success() {
        return Err(format!(
            "git {} failed: {}",
            arguments.join(" "),
            String::from_utf8_lossy(&output.stderr)
        )
        .into());
    }
    Ok(output.stdout)
}
