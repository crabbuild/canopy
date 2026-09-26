//! Incremental object enumeration and bounded reads from the disposable Git cache.

use std::{io, path::Path, process::Stdio, time::Duration};

use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt, BufReader},
    process::{Child, ChildStdin, ChildStdout, Command},
    time::timeout,
};
use tokio_util::task::AbortOnDropHandle;

use crate::{INLINE_OBJECT_LIMIT, ObjectKind, large_blob::MAX_EXTERNAL_BLOB_BYTES, object_id};

const IO_TIMEOUT: Duration = Duration::from_secs(120);
const HEADER_LIMIT: usize = 128;
const STDERR_LIMIT: usize = 64 * 1024;

#[derive(Debug, thiserror::Error)]
pub enum ObjectReadError {
    #[error("Git object process I/O failed")]
    Io(#[from] io::Error),
    #[error("Git object process failed: {0}")]
    Git(String),
    #[error("Git object stream is malformed or corrupt")]
    Malformed,
    #[error("Git object exceeds the current ingest limit")]
    TooLarge,
    #[error("Git object process timed out")]
    Timeout,
    #[error("Git object process task failed")]
    Task(#[from] tokio::task::JoinError),
}

struct Process {
    child: Child,
    output: BufReader<ChildStdout>,
    stderr: AbortOnDropHandle<Result<Vec<u8>, io::Error>>,
}

impl Process {
    fn start(git_dir: &Path, args: &[&str]) -> Result<(Self, ChildStdin), ObjectReadError> {
        let mut child = Command::new("git")
            .arg("--no-replace-objects")
            .arg("--git-dir")
            .arg(git_dir)
            .args(args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()?;
        let input = child.stdin.take().ok_or(ObjectReadError::Malformed)?;
        let output = child.stdout.take().ok_or(ObjectReadError::Malformed)?;
        let mut stderr = child.stderr.take().ok_or(ObjectReadError::Malformed)?;
        let stderr = AbortOnDropHandle::new(tokio::spawn(async move {
            let mut retained = Vec::new();
            let mut chunk = [0; 8192];
            loop {
                let size = stderr.read(&mut chunk).await?;
                if size == 0 {
                    return Ok(retained);
                }
                // Drain beyond the diagnostic limit so a full stderr pipe cannot stall Git.
                let keep = size.min(STDERR_LIMIT - retained.len());
                retained.extend_from_slice(&chunk[..keep]);
            }
        }));
        Ok((
            Self {
                child,
                output: BufReader::new(output),
                stderr,
            },
            input,
        ))
    }

    async fn finish(mut self) -> Result<(), ObjectReadError> {
        let status = self.child.wait().await?;
        let stderr = self.stderr.await??;
        if !status.success() {
            return Err(ObjectReadError::Git(format!(
                "{status}: {}",
                String::from_utf8_lossy(&stderr)
            )));
        }
        Ok(())
    }
}

pub(crate) struct GitObjects {
    walk: Process,
    revisions: AbortOnDropHandle<Result<(), io::Error>>,
    batch: Process,
    requests: ChildStdin,
}

impl GitObjects {
    pub(crate) fn start(
        git_dir: &Path,
        included: Vec<[u8; 20]>,
        excluded: Vec<[u8; 20]>,
    ) -> Result<Self, ObjectReadError> {
        let (walk, mut input) = Process::start(
            git_dir,
            &["rev-list", "--objects", "--no-object-names", "--stdin"],
        )?;
        // Ref lists can exceed argv limits; feed stdin concurrently with stdout consumption.
        let revisions = AbortOnDropHandle::new(tokio::spawn(async move {
            for (prefix, roots) in [("", included), ("^", excluded)] {
                for oid in roots {
                    input
                        .write_all(format!("{prefix}{}\n", hex::encode(oid)).as_bytes())
                        .await?;
                }
            }
            Ok(())
        }));
        let (batch, requests) = Process::start(git_dir, &["cat-file", "--batch"])?;
        Ok(Self {
            walk,
            revisions,
            batch,
            requests,
        })
    }

    pub(crate) async fn next(&mut self) -> Result<Option<[u8; 20]>, ObjectReadError> {
        timeout(IO_TIMEOUT, async {
            match header(&mut self.walk.output).await? {
                Some(line) => Ok(Some(parse_oid(&line)?)),
                None => Ok(None),
            }
        })
        .await
        .map_err(|_| ObjectReadError::Timeout)?
    }

    pub(crate) async fn read(
        &mut self,
        oid: [u8; 20],
    ) -> Result<(ObjectKind, Vec<u8>), ObjectReadError> {
        timeout(IO_TIMEOUT, async {
            self.requests
                .write_all(format!("{}\n", hex::encode(oid)).as_bytes())
                .await?;
            read_object(&mut self.batch.output, oid).await
        })
        .await
        .map_err(|_| ObjectReadError::Timeout)?
    }

    pub(crate) async fn finish(self) -> Result<(), ObjectReadError> {
        timeout(IO_TIMEOUT, async move {
            self.revisions.await??;
            self.walk.finish().await?;
            drop(self.requests);
            let mut batch = self.batch;
            if header(&mut batch.output).await?.is_some() {
                return Err(ObjectReadError::Malformed);
            }
            batch.finish().await
        })
        .await
        .map_err(|_| ObjectReadError::Timeout)?
    }
}

async fn header(reader: &mut (impl AsyncRead + Unpin)) -> Result<Option<Vec<u8>>, ObjectReadError> {
    let mut line = Vec::new();
    while line.len() < HEADER_LIMIT {
        match reader.read_u8().await {
            Ok(b'\n') => return Ok(Some(line)),
            Ok(byte) => line.push(byte),
            Err(error) if error.kind() == io::ErrorKind::UnexpectedEof && line.is_empty() => {
                return Ok(None);
            }
            Err(error) => return Err(error.into()),
        }
    }
    Err(ObjectReadError::Malformed)
}

fn parse_oid(bytes: &[u8]) -> Result<[u8; 20], ObjectReadError> {
    let mut oid = [0; 20];
    hex::decode_to_slice(bytes, &mut oid).map_err(|_| ObjectReadError::Malformed)?;
    Ok(oid)
}

async fn read_object(
    reader: &mut (impl AsyncRead + Unpin),
    expected: [u8; 20],
) -> Result<(ObjectKind, Vec<u8>), ObjectReadError> {
    let line = header(reader).await?.ok_or(ObjectReadError::Malformed)?;
    let mut fields = line.split(|byte| *byte == b' ');
    let (Some(oid), Some(kind), Some(size), None) =
        (fields.next(), fields.next(), fields.next(), fields.next())
    else {
        return Err(ObjectReadError::Malformed);
    };
    if parse_oid(oid)? != expected {
        return Err(ObjectReadError::Malformed);
    }
    let kind = match kind {
        b"blob" => ObjectKind::Blob,
        b"tree" => ObjectKind::Tree,
        b"commit" => ObjectKind::Commit,
        b"tag" => ObjectKind::Tag,
        _ => return Err(ObjectReadError::Malformed),
    };
    let size: usize = std::str::from_utf8(size)
        .map_err(|_| ObjectReadError::Malformed)?
        .parse()
        .map_err(|_| ObjectReadError::Malformed)?;
    // Check the declared size before allocating or reading the object's body.
    if size > MAX_EXTERNAL_BLOB_BYTES || (kind != ObjectKind::Blob && size > INLINE_OBJECT_LIMIT) {
        return Err(ObjectReadError::TooLarge);
    }
    let mut body = vec![0; size];
    reader.read_exact(&mut body).await?;
    if reader.read_u8().await? != b'\n' {
        return Err(ObjectReadError::Malformed);
    }
    tokio::task::spawn_blocking(move || {
        if object_id(kind, &body) != expected {
            return Err(ObjectReadError::Malformed);
        }
        Ok((kind, body))
    })
    .await?
}

#[cfg(test)]
mod tests;
