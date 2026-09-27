//! SSH Git ingress with Directory keys and shared repository admission.

use crate::{
    ReadIdentity,
    directory::{SshKey, TokenScope, validate_component},
    server::{RepositoryManager, ServerError},
};
use russh::{
    Channel, ChannelId, MethodKind, MethodSet,
    server::{self, Auth, Session},
};
use ssh_key::{PrivateKey, PublicKey};
use std::{collections::HashMap, net::SocketAddr, sync::Arc, time::Duration};
use tokio::{
    net::TcpListener,
    sync::{OwnedSemaphorePermit, Semaphore},
};
use tokio_util::{sync::CancellationToken, task::TaskTracker};

/// SSH bind address and stable, decrypted host identity.
pub struct SshConfig {
    pub listen: SocketAddr,
    pub host_key: PrivateKey,
}

#[derive(Debug, thiserror::Error)]
enum SshError {
    #[error("SSH transport failed")]
    Transport(#[from] russh::Error),
    #[error("SSH public key is invalid")]
    Key(#[from] ssh_key::Error),
    #[error("SSH repository operation failed")]
    Server(#[from] ServerError),
    #[error("SSH Git transfer failed")]
    Git(#[from] crate::git_gateway::GatewayError),
    #[error("SSH transfer admission failed")]
    Admission(#[from] crab_cell_runtime::Error),
    #[error("{0}")]
    Rejected(&'static str),
}

pub(crate) async fn serve(
    config: SshConfig,
    listener: TcpListener,
    manager: Arc<RepositoryManager>,
    tasks: TaskTracker,
    stop: CancellationToken,
) -> std::io::Result<()> {
    let mut methods = MethodSet::empty();
    methods.push(MethodKind::PublicKey);
    let config = Arc::new(server::Config {
        keys: vec![config.host_key],
        methods,
        maximum_packet_size: 32 * 1024,
        window_size: 256 * 1024,
        channel_buffer_size: 4,
        event_buffer_size: 8,
        max_auth_attempts: 6,
        inactivity_timeout: Some(Duration::from_secs(120)),
        auth_rejection_time: Duration::from_millis(250),
        nodelay: true,
        ..Default::default()
    });
    let connections = Arc::new(Semaphore::new(64));
    loop {
        let (socket, _) = tokio::select! {
            () = stop.cancelled() => return Ok(()),
            accepted = listener.accept() => accepted?,
        };
        let Ok(permit) = Arc::clone(&connections).try_acquire_owned() else {
            continue;
        };
        socket.set_nodelay(true)?;
        let config = Arc::clone(&config);
        let manager = Arc::clone(&manager);
        let stop = stop.clone();
        let tracked = tasks.clone();
        tasks.spawn(async move {
            let _permit = permit;
            let handler = Connection {
                manager, tasks: tracked, key: None, channels: HashMap::new(), opened_channels: 0,
                slots: Arc::new(Semaphore::new(4)), stop: stop.child_token(),
            };
            let session = tokio::select! {
                () = stop.cancelled() => return,
                result = tokio::time::timeout(Duration::from_secs(30), server::run_stream(config, socket, handler)) => {
                    match result {
                        Ok(Ok(session)) => session,
                        Ok(Err(error)) => { tracing::debug!(error = %error, "SSH handshake failed"); return; }
                        Err(_) => return,
                    }
                }
            };
            let handle = session.handle();
            tokio::pin!(session);
            let result = tokio::select! {
                result = &mut session => result,
                () = stop.cancelled() => {
                    let _ = handle.disconnect(russh::Disconnect::ByApplication, "Canopy is draining".into(), "".into()).await;
                    session.await
                }
            };
            if let Err(error) = result { tracing::debug!(error = %error, "SSH connection ended"); }
        });
    }
}

struct Connection {
    manager: Arc<RepositoryManager>,
    tasks: TaskTracker,
    key: Option<SshKey>,
    channels: HashMap<ChannelId, ChannelState>,
    opened_channels: usize,
    slots: Arc<Semaphore>,
    stop: CancellationToken,
}
struct ChannelState {
    channel: Option<Channel<server::Msg>>,
    protocol_v2: bool,
    stop: CancellationToken,
    slot: Arc<OwnedSemaphorePermit>,
}
impl Drop for Connection {
    fn drop(&mut self) {
        self.stop.cancel();
    }
}

impl Connection {
    async fn registered_key(
        &self,
        user: &str,
        public: &PublicKey,
    ) -> Result<Option<SshKey>, SshError> {
        if user != "git" || self.stop.is_cancelled() || !(self.manager.ready)() {
            return Ok(None);
        }
        let Ok(key) = SshKey::parse(&public.to_openssh()?) else {
            return Ok(None);
        };
        Ok(self.manager.ssh_identity(&key).await?.map(|_| key))
    }
}

impl server::Handler for Connection {
    type Error = SshError;

    async fn auth_publickey_offered(
        &mut self,
        user: &str,
        key: &PublicKey,
    ) -> Result<Auth, SshError> {
        Ok(if self.registered_key(user, key).await?.is_some() {
            Auth::Accept
        } else {
            Auth::reject()
        })
    }
    async fn auth_publickey(&mut self, user: &str, key: &PublicKey) -> Result<Auth, SshError> {
        // Russh calls this only after verifying the signature. An offered key
        // never establishes identity, and revocation is checked again at exec.
        self.key = self.registered_key(user, key).await?;
        Ok(if self.key.is_some() {
            Auth::Accept
        } else {
            Auth::reject()
        })
    }
    async fn channel_open_session(
        &mut self,
        channel: Channel<server::Msg>,
        reply: server::ChannelOpenHandle,
        session: &mut Session,
    ) -> Result<(), SshError> {
        if self.key.is_none() || self.stop.is_cancelled() {
            return Ok(());
        }
        // Russh retains sender bookkeeping after locally initiated channel
        // closes. Bound lifetime channel churn as well as concurrent work;
        // clients can authenticate a new connection after reaching this cap.
        if self.opened_channels == 4096 {
            session.disconnect(
                russh::Disconnect::ByApplication,
                "SSH connection channel limit reached",
                "",
            )?;
            return Ok(());
        }
        // Russh removes a locally closed channel before receiving the peer's
        // close reply, so that reply need not invoke channel_close. Reclaim
        // completed commands here as well as explicitly closed peer channels.
        self.channels.retain(|_, state| !state.stop.is_cancelled());
        let Ok(slot) = Arc::clone(&self.slots).try_acquire_owned() else {
            return Ok(());
        };
        self.opened_channels += 1;
        self.channels.insert(
            channel.id(),
            ChannelState {
                channel: Some(channel),
                protocol_v2: false,
                stop: self.stop.child_token(),
                slot: Arc::new(slot),
            },
        );
        reply.accept().await;
        Ok(())
    }
    async fn env_request(
        &mut self,
        id: ChannelId,
        name: &str,
        value: &str,
        session: &mut Session,
    ) -> Result<(), SshError> {
        if let Some(state) = self.channels.get_mut(&id)
            && state.channel.is_some()
            && name == "GIT_PROTOCOL"
            && matches!(value, "version=0" | "version=1" | "version=2")
        {
            state.protocol_v2 = value == "version=2";
            session.channel_success(id)?;
        } else {
            session.channel_failure(id)?;
        }
        Ok(())
    }

    async fn data(
        &mut self,
        id: ChannelId,
        _data: &[u8],
        session: &mut Session,
    ) -> Result<(), SshError> {
        // Before exec there is no reader for the bounded channel queue. Reject
        // premature input on its first packet so it cannot stall the session
        // loop and prevent shutdown or unrelated channels from progressing.
        if self
            .channels
            .get(&id)
            .is_some_and(|state| state.channel.is_some())
        {
            self.channels.remove(&id);
            session.close(id)?;
        }
        Ok(())
    }

    async fn extended_data(
        &mut self,
        id: ChannelId,
        _ext: u32,
        data: &[u8],
        session: &mut Session,
    ) -> Result<(), SshError> {
        self.data(id, data, session).await
    }
    async fn exec_request(
        &mut self,
        id: ChannelId,
        data: &[u8],
        session: &mut Session,
    ) -> Result<(), SshError> {
        let Some(command) = GitCommand::parse(data) else {
            session.channel_failure(id)?;
            return Ok(());
        };
        let Some(key) = self.key.clone() else {
            session.channel_failure(id)?;
            return Ok(());
        };
        let Some(state) = self.channels.get_mut(&id) else {
            session.channel_failure(id)?;
            return Ok(());
        };
        let Some(channel) = state.channel.take() else {
            session.channel_failure(id)?;
            return Ok(());
        };
        session.channel_success(id)?;
        let handle = session.handle();
        let manager = Arc::clone(&self.manager);
        let slot = Arc::clone(&state.slot);
        let stop = state.stop.clone();
        let protocol_v2 = state.protocol_v2;
        self.tasks.spawn(async move {
            let _slot = slot;
            // Retain the stream's write half until exit status is queued. Russh's
            // stream drop closes the channel; early drop would hide failure status.
            let (reader, mut writer) = tokio::io::split(channel.into_stream());
            let run = async {
                if stop.is_cancelled() || !(manager.ready)() {
                    return Err(SshError::Rejected("Canopy is not ready"));
                }
                let principal = manager.ssh_identity(&key).await?
                    .ok_or(SshError::Rejected("SSH key is revoked or unavailable"))?;
                if command.push && principal.scope < TokenScope::Write {
                    return Err(SshError::Rejected("SSH key is read-only"));
                }
                let actor = ReadIdentity::Account(&principal.account);
                let admission = manager.transfer_permit(actor).await?;
                let route = manager.resolve(actor, &command.owner, &command.repository).await?
                    .ok_or(SshError::Rejected("Repository is unavailable"))?;
                if command.push {
                    // Once a push is admitted, disconnection cannot abandon its
                    // durable outcome. TaskTracker drains it before Cell shutdown.
                    route.gateway.ssh_push(reader, &mut writer, &principal.account, admission).await?;
                } else {
                    tokio::select! {
                        () = stop.cancelled() => return Err(SshError::Rejected("SSH fetch cancelled")),
                        result = tokio::time::timeout(Duration::from_secs(120), route.gateway.ssh_fetch(reader, &mut writer, &principal.account, protocol_v2, admission)) => {
                            result.map_err(|_| SshError::Rejected("Git transfer timed out"))??;
                        }
                    }
                }
                Ok::<_, SshError>(())
            }.await;
            let code = if let Err(error) = run {
                let message = match &error {
                    SshError::Rejected(message) => *message,
                    SshError::Git(crate::git_gateway::GatewayError::Unauthorized) => "Repository access denied",
                    SshError::Git(crate::git_gateway::GatewayError::UnreachableWant) => "Requested object is not reachable from a current repository ref",
                    SshError::Admission(_) => "Canopy transfer capacity is busy; retry shortly",
                    _ => "Canopy Git operation failed; retry or contact the administrator",
                };
                tracing::warn!(error = ?error, "SSH Git command failed");
                let _ = handle.extended_data(id, 1, format!("{message}\n").into_bytes()).await;
                1
            } else { 0 };
            let _ = handle.exit_status_request(id, code).await;
            let _ = handle.eof(id).await;
            let _ = handle.close(id).await;
            stop.cancel();
        });
        Ok(())
    }
    async fn channel_close(
        &mut self,
        id: ChannelId,
        _session: &mut Session,
    ) -> Result<(), SshError> {
        if let Some(state) = self.channels.remove(&id) {
            state.stop.cancel();
        }
        Ok(())
    }
    async fn shell_request(
        &mut self,
        id: ChannelId,
        session: &mut Session,
    ) -> Result<(), SshError> {
        session.channel_failure(id)?;
        Ok(())
    }
    async fn subsystem_request(
        &mut self,
        id: ChannelId,
        _name: &str,
        session: &mut Session,
    ) -> Result<(), SshError> {
        session.channel_failure(id)?;
        Ok(())
    }
    async fn pty_request(
        &mut self,
        id: ChannelId,
        _term: &str,
        _cols: u32,
        _rows: u32,
        _width: u32,
        _height: u32,
        _modes: &[(russh::Pty, u32)],
        session: &mut Session,
    ) -> Result<(), SshError> {
        session.channel_failure(id)?;
        Ok(())
    }
}

struct GitCommand {
    push: bool,
    owner: String,
    repository: String,
}
impl GitCommand {
    fn parse(bytes: &[u8]) -> Option<Self> {
        if bytes.len() > 256 {
            return None;
        }
        let (service, path) = std::str::from_utf8(bytes).ok()?.split_once(' ')?;
        let push = match service {
            "git-upload-pack" => false,
            "git-receive-pack" => true,
            _ => return None,
        };
        let path = if let Some(path) = path.strip_prefix('\'') {
            path.strip_suffix('\'')?
        } else {
            path
        };
        let path = path.strip_prefix('/').unwrap_or(path);
        let (owner, repository) = path.split_once('/')?;
        let repository = repository.strip_suffix(".git").unwrap_or(repository);
        validate_component(owner).ok()?;
        validate_component(repository).ok()?;
        Some(Self {
            push,
            owner: owner.into(),
            repository: repository.into(),
        })
    }
}
