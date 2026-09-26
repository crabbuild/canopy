use super::*;

impl CanopyServer {
    /// Starts a node only after storage fencing, authority and Git ingress are ready.
    /// Cancelling startup requests cleanup after admitted initialization settles.
    pub async fn start(
        config: ServerConfig,
        store: Arc<dyn ObjectStore>,
    ) -> Result<Self, ServerError> {
        let (ready, receive_ready) = oneshot::channel();
        let (shutdown, receive_shutdown) = oneshot::channel();
        // The task owns startup, drain and the workspace together. Dropping any
        // caller future only closes a channel; it cannot abandon admitted work.
        let finished = tokio::spawn(async move {
            let server = match RunningServer::start(config, store).await {
                Ok(server) => server,
                Err(error) => {
                    let _ = ready.send(Err(error));
                    return Ok(());
                }
            };
            if ready.send(Ok(server.address)).is_ok() {
                let _ = receive_shutdown.await;
            }
            let result = server.shutdown().await;
            if let Err(error) = &result {
                tracing::error!(error = %error, "Canopy shutdown failed");
            }
            result
        });
        let address = match receive_ready.await {
            Ok(address) => address?,
            Err(_) => {
                finished.await??;
                return Err(ServerError::Repository(
                    "node supervisor ended before readiness",
                ));
            }
        };
        Ok(Self {
            address,
            shutdown,
            finished,
        })
    }

    #[must_use]
    pub const fn local_addr(&self) -> std::net::SocketAddr {
        self.address
    }

    /// Stops ingress, drains accepted work, and withdraws the node advertisement.
    /// Cancelling this wait does not cancel the supervised shutdown.
    pub async fn shutdown(self) -> Result<(), ServerError> {
        let _ = self.shutdown.send(());
        self.finished.await?
    }
}

impl RunningServer {
    async fn shutdown(self) -> Result<(), ServerError> {
        self.ingress_stop.cancel();
        let serving = self.serving.await;
        self.tasks.close();
        self.tasks.wait().await;
        let drained = self.node.shutdown().await;
        if drained.is_ok() {
            self.local.confirm_drained();
        }
        self.stop.cancel();
        let observed = self.advertisement.lock().await;
        let withdrawn = match unix_now_ms() {
            Ok(now_ms) => self.directory.withdraw(&observed, now_ms).await,
            Err(error) => return Err(error),
        };
        serving??;
        drained?;
        withdrawn?;
        Ok(())
    }
}
