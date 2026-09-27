//! Bounded, disk-accounted Git request bodies.

use std::{
    fs::File,
    future::poll_fn,
    io::{self, Read, Seek, SeekFrom, Write},
    path::Path,
    pin::Pin,
    process::Stdio,
    sync::Arc,
    time::Duration,
};

use crate::AdmissionPermit;
use axum::body::Body;
use crab_ltx::{DiskBudget, DiskReservation};
use futures_core::Stream;
use tokio_util::sync::CancellationToken;

pub(crate) const MAX_FETCH_REQUEST_BYTES: u64 = 64 * 1024 * 1024;
const CHUNK_BYTES: usize = 64 * 1024;

#[derive(Debug, thiserror::Error)]
pub enum InputError {
    #[error("Git request exceeds the input limit")]
    TooLarge,
    #[error("Git request upload timed out")]
    Timeout,
    #[error("Git receive-pack commands are malformed")]
    Commands,
    #[error("Git upload-pack commands are malformed")]
    Fetch,
    #[error("Git request body failed")]
    Body(#[from] axum::Error),
    #[error("Git request gzip stream is invalid")]
    Gzip(#[source] io::Error),
    #[error("Git request spool I/O failed")]
    Io(#[from] std::io::Error),
    #[error("Git request disk admission failed")]
    Budget(#[from] crab_ltx::CrabError),
    #[error("Git request spool task failed")]
    Task(#[from] tokio::task::JoinError),
}

struct Spool {
    // Close the file before releasing disk and transfer admission. Blocking I/O
    // jobs retain this owner through cancellation until their work exits.
    file: File,
    reservation: DiskReservation,
    admission: Option<Arc<AdmissionPermit>>,
}

/// An immutable request spool, deleted when its last file handle closes.
pub struct GitInput {
    spool: Arc<Spool>,
    size: u64,
}

impl GitInput {
    /// Receives a body with optional size bounds, shared disk admission and an idle timeout.
    pub async fn receive(
        body: Body,
        directory: &Path,
        budget: &DiskBudget,
        limit: Option<u64>,
        admission: Option<Arc<AdmissionPermit>>,
    ) -> Result<Self, InputError> {
        Self::spool(body, directory, budget, limit, admission).await
    }

    async fn spool(
        body: Body,
        directory: &Path,
        budget: &DiskBudget,
        limit: Option<u64>,
        admission: Option<Arc<AdmissionPermit>>,
    ) -> Result<Self, InputError> {
        let spool = Arc::new(Spool {
            file: tempfile::tempfile_in(directory)?,
            reservation: budget.try_reserve(0)?,
            admission,
        });
        let mut body = body.into_data_stream();
        let mut size = 0u64;
        while let Some(frame) = tokio::time::timeout(
            Duration::from_secs(120),
            poll_fn(|cx| Pin::new(&mut body).poll_next(cx)),
        )
        .await
        .map_err(|_| InputError::Timeout)?
        {
            let frame = frame?;
            let next = size
                .checked_add(frame.len() as u64)
                .ok_or(InputError::TooLarge)?;
            if limit.is_some_and(|limit| next > limit) {
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

    /// Validates and expands gzip under the same byte and disk limits as plain input.
    pub(crate) async fn decode_gzip(
        self,
        directory: &Path,
        budget: &DiskBudget,
        limit: Option<u64>,
    ) -> Result<Self, InputError> {
        let output = Arc::new(Spool {
            file: tempfile::tempfile_in(directory)?,
            reservation: budget.try_reserve(0)?,
            admission: self.spool.admission.clone(),
        });
        let cancelled = CancellationToken::new();
        let _cancel_on_drop = cancelled.clone().drop_guard();
        let task = tokio::task::spawn_blocking(move || {
            let mut file = &self.spool.file;
            file.rewind()?;
            let reader = DecodeReader {
                file,
                cancelled: &cancelled,
            };
            let mut decoder = flate2::read::MultiGzDecoder::new(reader);
            let mut buffer = [0; CHUNK_BYTES];
            let mut size = 0_u64;
            loop {
                if cancelled.is_cancelled() {
                    return Err(InputError::Timeout);
                }
                let count = decoder
                    .read(&mut buffer)
                    .map_err(|error| match error.kind() {
                        io::ErrorKind::InvalidInput
                        | io::ErrorKind::InvalidData
                        | io::ErrorKind::UnexpectedEof => InputError::Gzip(error),
                        io::ErrorKind::TimedOut if cancelled.is_cancelled() => InputError::Timeout,
                        _ => InputError::Io(error),
                    })?;
                if count == 0 {
                    break;
                }
                size = size
                    .checked_add(count as u64)
                    .filter(|size| limit.is_none_or(|limit| *size <= limit))
                    .ok_or(InputError::TooLarge)?;
                output.reservation.try_grow(count as u64)?;
                (&output.file).write_all(&buffer[..count])?;
            }
            (&output.file).rewind()?;
            Ok(Self {
                spool: output,
                size,
            })
        });
        // A blocking decoder cannot be aborted. Cancellation stops its bounded
        // read/write loop; the worker owns both files and charges until it exits.
        task.await.map_err(InputError::Task)?
    }

    pub(crate) async fn prefix(&self, limit: usize) -> Result<Vec<u8>, InputError> {
        let spool = Arc::clone(&self.spool);
        Ok(tokio::task::spawn_blocking(move || {
            let mut file = &spool.file;
            file.rewind()?;
            let mut bytes = Vec::new();
            (&mut file).take(limit as u64).read_to_end(&mut bytes)?;
            file.rewind()?;
            Ok::<_, std::io::Error>(bytes)
        })
        .await??)
    }

    pub(crate) async fn packet_prefix(&self, limit: usize) -> Result<Vec<u8>, InputError> {
        self.packet_group(0, limit).await
    }

    pub(crate) async fn packet_group(
        &self,
        offset: u64,
        limit: usize,
    ) -> Result<Vec<u8>, InputError> {
        let spool = Arc::clone(&self.spool);
        tokio::task::spawn_blocking(move || {
            let mut file = &spool.file;
            file.seek(SeekFrom::Start(offset))?;
            let result = (|| {
                let mut bytes = Vec::new();
                loop {
                    let mut header = [0; 4];
                    file.read_exact(&mut header).map_err(|error| {
                        if error.kind() == io::ErrorKind::UnexpectedEof {
                            InputError::Commands
                        } else {
                            InputError::Io(error)
                        }
                    })?;
                    if !header.iter().all(u8::is_ascii_hexdigit) {
                        return Err(InputError::Commands);
                    }
                    let length = std::str::from_utf8(&header)
                        .ok()
                        .and_then(|value| usize::from_str_radix(value, 16).ok())
                        .ok_or(InputError::Commands)?;
                    if length != 0 && !(5..=65520).contains(&length) {
                        return Err(InputError::Commands);
                    }
                    if bytes.len() + length.max(4) > limit {
                        return Err(InputError::TooLarge);
                    }
                    bytes.extend_from_slice(&header);
                    if length == 0 {
                        return Ok(bytes);
                    }
                    let start = bytes.len();
                    bytes.resize(start + length - 4, 0);
                    file.read_exact(&mut bytes[start..]).map_err(|error| {
                        if error.kind() == io::ErrorKind::UnexpectedEof {
                            InputError::Commands
                        } else {
                            InputError::Io(error)
                        }
                    })?;
                }
            })();
            // Native Git must read the original stream even when preflight
            // rejects it. Its report remains the authoritative wire encoding.
            file.rewind()?;
            result
        })
        .await?
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

struct DecodeReader<'a> {
    file: &'a File,
    cancelled: &'a CancellationToken,
}

impl Read for DecodeReader<'_> {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        // Header-only gzip members may consume input without yielding output.
        // Check cancellation at compressed reads as well as decoded writes.
        if self.cancelled.is_cancelled() {
            return Err(io::ErrorKind::TimedOut.into());
        }
        self.file.read(bytes)
    }
}

#[cfg(test)]
#[path = "git_input/tests.rs"]
mod tests;
