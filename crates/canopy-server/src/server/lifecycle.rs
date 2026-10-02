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
            if ready.send(Ok((server.address, server.ssh_address))).is_ok() {
                tokio::select! {
                    _ = receive_shutdown => {},
                    () = server.release_stop.cancelled() => {},
                }
            }
            let result = server.shutdown().await;
            if let Err(error) = &result {
                tracing::error!(error = %error, "Canopy shutdown failed");
            }
            result
        });
        let (address, ssh_address) = match receive_ready.await {
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
            ssh_address,
            shutdown,
            finished,
        })
    }

    /// Runs until a stop signal or deployment-driven shutdown, preserving drain.
    pub async fn serve_until(
        self,
        signal: impl std::future::Future<Output = std::io::Result<()>>,
    ) -> Result<(), ServerError> {
        let Self {
            shutdown,
            mut finished,
            ..
        } = self;
        tokio::select! {
            result = &mut finished => result?,
            result = signal => {
                let _ = shutdown.send(());
                let drained = finished.await?;
                result?;
                drained
            }
        }
    }

    #[must_use]
    pub const fn local_addr(&self) -> std::net::SocketAddr {
        self.address
    }

    #[must_use]
    pub const fn ssh_addr(&self) -> Option<std::net::SocketAddr> {
        self.ssh_address
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
        self.native.close();
        self.maintenance_stop.cancel();
        self.ingress_stop.cancel();
        let serving = self.serving.await;
        let ssh_serving = if let Some(task) = self.ssh_serving {
            Some(task.await)
        } else {
            None
        };
        self.tasks.close();
        self.tasks.wait().await;
        // Detached native reapers and blocking verifiers outlive their request
        // observers. Keep Cell authority, heartbeat and workspace until every
        // admitted owner releases its claim. Uncertain drain stays pending.
        self.native.drain().await;
        let drained = self.node.shutdown().await;
        if drained.is_ok() {
            self.local.confirm_drained();
        }
        self.stop.cancel();
        let renewal = self.renewal.await;
        let observed = self.advertisement.lock().await;
        let withdrawn = match unix_now_ms() {
            Ok(now_ms) => self.directory.withdraw(&observed, now_ms).await,
            Err(error) => return Err(error),
        };
        serving??;
        if let Some(result) = ssh_serving {
            result??;
        }
        drained?;
        renewal??;
        withdrawn?;
        Ok(())
    }
}

#[cfg(test)]
mod tests;
