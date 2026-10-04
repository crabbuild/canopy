//! Certified object bodies with worker ownership through native physical drain.
use super::*;

/// Foreground copies are bounded independently of any object header. Streaming
/// larger objects is a separate producer contract, never an unbounded Vec.
const MAX_BODY_BYTES: usize = 64 << 20;
impl ServingPin {
    pub async fn body(
        &self,
        actor: Option<String>,
        oid: crate::ObjectId,
        limit: usize,
    ) -> Result<Option<Vec<u8>>, ServingReadError> {
        if oid.is_zero() || oid.format() != self.inner.lease.format {
            return Err(ServingReadError::Context);
        }
        if limit == 0 || limit > MAX_BODY_BYTES {
            return Err(ServingReadError::TooLarge);
        }
        self.read_owned(actor, move |inner, deadline| async move {
            let reader = inner.catalog().await?;
            let Some(object) = reader
                .lookup(oid, &*inner.context.files, &*inner.context.files)
                .await?
            else {
                return Ok(None);
            };
            if object.entry.header.object.size > limit as u64 {
                return Err(ServingReadError::TooLarge);
            }
            if Instant::now() >= deadline {
                return Err(ServingReadError::Inactive);
            }
            // This is a child of an already admitted worker. Closing refuses new
            // workers but must not invalidate native drain ownership of this one.
            let owner = inner.child();
            Ok(Some(inner.context.files.body(object, limit, owner).await?))
        })
        .await
    }
}
