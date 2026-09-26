//! Bounded, disk-accounted Git request bodies.

use std::{
    fs::File,
    future::poll_fn,
    io::{Read, Seek, Write},
    path::Path,
    pin::Pin,
    process::Stdio,
    sync::Arc,
    time::Duration,
};

use axum::body::Body;
use cellule_ltx::{DiskBudget, DiskReservation};
use futures_core::Stream;

pub(crate) const MAX_PUSH_BYTES: u64 = 512 * 1024 * 1024;
pub(crate) const MAX_FETCH_REQUEST_BYTES: u64 = 64 * 1024 * 1024;
const CHUNK_BYTES: usize = 64 * 1024;

#[derive(Debug, thiserror::Error)]
pub enum InputError {
    #[error("Git request exceeds the input limit")]
    TooLarge,
    #[error("Git request upload timed out")]
    Timeout,
    #[error("Git request body failed")]
    Body(#[from] axum::Error),
    #[error("Git request spool I/O failed")]
    Io(#[from] std::io::Error),
    #[error("Git request disk admission failed")]
    Budget(#[from] cellule_ltx::LtxError),
    #[error("Git request spool task failed")]
    Task(#[from] tokio::task::JoinError),
}

struct Spool {
    // Close the file before releasing its accounting. Blocking I/O jobs retain
    // this entire owner, so cancellation cannot release their reservation early.
    file: File,
    reservation: DiskReservation,
}

/// An immutable request spool, deleted when its last file handle closes.
pub struct GitInput {
    spool: Arc<Spool>,
    size: u64,
}

impl GitInput {
    /// Receives a body under a byte limit, a shared disk budget and a 120-second deadline.
    pub async fn receive(
        body: Body,
        directory: &Path,
        budget: &DiskBudget,
        limit: u64,
    ) -> Result<Self, InputError> {
        tokio::time::timeout(
            Duration::from_secs(120),
            Self::spool(body, directory, budget, limit),
        )
        .await
        .map_err(|_| InputError::Timeout)?
    }

    async fn spool(
        body: Body,
        directory: &Path,
        budget: &DiskBudget,
        limit: u64,
    ) -> Result<Self, InputError> {
        let spool = Arc::new(Spool {
            file: tempfile::tempfile_in(directory)?,
            reservation: budget.try_reserve(0)?,
        });
        let mut body = body.into_data_stream();
        let mut size = 0u64;
        while let Some(frame) = poll_fn(|cx| Pin::new(&mut body).poll_next(cx)).await {
            let frame = frame?;
            let next = size
                .checked_add(frame.len() as u64)
                .ok_or(InputError::TooLarge)?;
            if next > limit {
                return Err(InputError::TooLarge);
            }
            for start in (0..frame.len()).step_by(CHUNK_BYTES) {
                let bytes = frame.slice(start..frame.len().min(start + CHUNK_BYTES));
                spool.reservation.try_grow(bytes.len() as u64)?;
                let spool = Arc::clone(&spool);
                tokio::task::spawn_blocking(move || (&spool.file).write_all(&bytes)).await??;
            }
            size = next;
        }
        (&spool.file).rewind()?;
        Ok(Self { spool, size })
    }

    pub(crate) fn size(&self) -> u64 {
        self.size
    }

    pub(crate) fn stdin(&self) -> Result<Stdio, std::io::Error> {
        Ok(Stdio::from(self.spool.file.try_clone()?))
    }

    pub(crate) async fn digest(&self, mut hash: blake3::Hasher) -> Result<[u8; 32], InputError> {
        // Preserve the canonical length-prefixed HTTP digest without keeping the
        // body in memory. Only this pre-execution pass shares the file cursor.
        hash.update(&self.size.to_le_bytes());
        let spool = Arc::clone(&self.spool);
        Ok(tokio::task::spawn_blocking(move || {
            let mut file = &spool.file;
            file.rewind()?;
            let mut buffer = [0; CHUNK_BYTES];
            loop {
                let count = file.read(&mut buffer)?;
                if count == 0 {
                    break;
                }
                hash.update(&buffer[..count]);
            }
            file.rewind()?;
            Ok::<_, std::io::Error>(*hash.finalize().as_bytes())
        })
        .await??)
    }
}

#[cfg(test)]
#[path = "git_input/tests.rs"]
mod tests;
