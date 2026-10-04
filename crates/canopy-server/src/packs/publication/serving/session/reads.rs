//! One admission and physical lifetime for private immutable read workers.
use super::*;
use std::future::Future;

impl ServingPin {
    pub(super) async fn read_owned<T, F, Work>(
        &self,
        actor: Option<String>,
        work: Work,
    ) -> Result<T, ServingReadError>
    where
        T: Send + 'static,
        F: Future<Output = Result<T, ServingReadError>> + Send,
        Work: FnOnce(Arc<Inner>, Instant) -> F + Send + 'static,
    {
        if self.inner.context.budget.inner.stop.is_cancelled() {
            return Err(ServingReadError::Inactive);
        }
        let scope = actor
            .as_deref()
            .map_or(ReadIdentity::Anonymous, ReadIdentity::Account);
        let permit = self
            .inner
            .context
            .budget
            .inner
            .admission
            .acquire(scope)
            .await?;
        let guard = {
            let mut state = self.inner.state.lock().expect("serving workers");
            if state.closed {
                return Err(ServingReadError::Inactive);
            }
            state.active += 1;
            Active(Arc::clone(&self.inner))
        };
        let inner = Arc::clone(&self.inner);
        self.inner
            .context
            .tasks()
            .spawn(async move {
                let (_permit, _guard) = (permit, guard);
                let (_, deadline) = inner.observe(actor.clone()).await?;
                if Instant::now() >= deadline {
                    return Err(ServingReadError::Inactive);
                }
                // Observer cancellation only detaches this task. Do not time out by
                // dropping provider work and misreporting that its roots drained.
                let output = work(inner.clone(), deadline).await?;
                inner.observe(actor).await?;
                if Instant::now() >= deadline {
                    return Err(ServingReadError::Inactive);
                }
                Ok(output)
            })
            .await?
    }
}
