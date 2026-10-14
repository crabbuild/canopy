//! An eviction owner excludes new work while releasing its exact read pins.
use super::*;

pub(super) struct Gate {
    owner: Arc<()>,
    readers: Box<[ServingToken]>,
    remaining: Vec<ServingToken>,
}

/// This controls scheduling only. Every release still needs its private drained
/// capability, current Admin/owner and exact receiver checks.
#[must_use]
pub struct ServingDrainAdmission {
    inner: Arc<Inner>,
    owner: Arc<()>,
}
impl Drop for ServingDrainAdmission {
    fn drop(&mut self) {
        let mut gate = self
            .inner
            .serving_drain
            .lock()
            .expect("serving drain admission");
        if gate
            .as_ref()
            .is_some_and(|gate| Arc::ptr_eq(&gate.owner, &self.owner))
        {
            gate.take();
        }
        drop(gate);
        self.inner.drained.notify_waiters();
        // Never change State.closed or abandon any accepted command here.
    }
}
impl Inner {
    pub(super) fn drain_allows(&self, ready: &ReadyPublication) -> bool {
        self.serving_drain
            .lock()
            .expect("serving drain admission")
            .as_ref()
            .is_none_or(|gate| {
                ready
                    .serving_release_token()
                    .is_some_and(|token| gate.readers.contains(&token))
            })
    }
}
impl PublicationCoordinator {
    /// Pause serving producers/borrows first and keep them paused until this
    /// guard is dropped or the coordinator is closed. Busy admission is refused
    /// without changing any existing command or closing the queue.
    pub async fn reserve_serving_drain(
        &self,
        readers: &[ServingToken],
    ) -> Result<Option<ServingDrainAdmission>, PublicationScheduleError> {
        if readers.len() > 16
            || readers.iter().any(|token| token.validate().is_err())
            || readers
                .iter()
                .enumerate()
                .any(|(i, id)| readers[..i].contains(id))
        {
            return Err(PublicationScheduleError::InvalidLimits);
        }
        for token in readers {
            if crate::repository_target(
                self.inner.target.tenant(),
                self.inner.target.application(),
                token.repository,
            )
            .map_err(|_| PublicationScheduleError::Foreign)?
                != self.inner.target
            {
                return Err(PublicationScheduleError::Foreign);
            }
        }
        let state = self.inner.state.lock().await;
        let mut gate = self
            .inner
            .serving_drain
            .lock()
            .expect("serving drain admission");
        if state.closed || state.worker || !state.jobs.is_empty() || gate.is_some() {
            return Ok(None);
        }
        let owner = Arc::new(());
        *gate = Some(Gate {
            owner: owner.clone(),
            readers: readers.into(),
            remaining: readers.to_vec(),
        });
        Ok(Some(ServingDrainAdmission {
            inner: self.inner.clone(),
            owner,
        }))
    }
}

impl Inner {
    pub(super) fn observe_serving_release(&self, token: ServingToken) {
        if let Some(gate) = self
            .serving_drain
            .lock()
            .expect("serving drain admission")
            .as_mut()
        {
            gate.remaining.retain(|pending| *pending != token);
        }
    }
}
impl ServingDrainAdmission {
    /// Close only after every selected exact root has an observed successful
    /// release and all admitted work has finished. A denial or uncertainty is
    /// never a completed release. Failure leaves the guard and queue unchanged.
    pub async fn close_if_drained(&self) -> bool {
        let mut state = self.inner.state.lock().await;
        let gate = self
            .inner
            .serving_drain
            .lock()
            .expect("serving drain admission");
        if !gate
            .as_ref()
            .is_some_and(|gate| Arc::ptr_eq(&gate.owner, &self.owner) && gate.remaining.is_empty())
            || state.worker
            || !state.jobs.is_empty()
        {
            return false;
        }
        state.closed = true;
        drop(gate);
        drop(state);
        self.inner.drained.notify_waiters();
        true
    }
}
