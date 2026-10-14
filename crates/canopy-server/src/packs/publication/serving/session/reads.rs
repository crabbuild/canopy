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
        Work: FnOnce(Arc<Inner>, Instant, Arc<crate::AdmissionPermit>) -> F + Send + 'static,
    {
        self.read_session(actor, work, false).await
    }

    /// A renewable producer retains its borrow/admission and refreshes the exact
    /// lease before each bounded I/O step. Renewal cannot resurrect an expired
    /// pin; the final fresh observation must still prove current authority.
    pub(super) async fn read_session<T, F, Work>(
        &self,
        actor: Option<String>,
        work: Work,
        renewable: bool,
    ) -> Result<T, ServingReadError>
    where
        T: Send + 'static,
        F: Future<Output = Result<T, ServingReadError>> + Send,
        Work: FnOnce(Arc<Inner>, Instant, Arc<crate::AdmissionPermit>) -> F + Send + 'static,
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
                let (permit, _guard) = (Arc::new(permit), guard);
                let (_, deadline) = inner.observe(actor.clone()).await?;
                if Instant::now() >= deadline {
                    return Err(ServingReadError::Inactive);
                }
                // Observer cancellation only detaches this task. Do not time out by
                // dropping provider work and misreporting that its roots drained.
                let output = work(inner.clone(), deadline, permit.clone()).await?;
                let (_, current) = inner.observe(actor).await?;
                if Instant::now() >= current || !renewable && Instant::now() >= deadline {
                    return Err(ServingReadError::Inactive);
                }
                Ok(output)
            })
            .await?
    }
}
