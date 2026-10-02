//! Incremental object enumeration and bounded reads from the disposable Git cache.

use std::{io, path::Path, process::Stdio, time::Duration};

use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt, BufReader},
    process::{ChildStdin, ChildStdout},
    time::timeout,
};
use tokio_util::task::AbortOnDropHandle;

use crate::{INLINE_OBJECT_LIMIT, ObjectKind, object_id};

const IO_TIMEOUT: Duration = Duration::from_secs(120);
const HEADER_LIMIT: usize = 128;
const STDERR_LIMIT: usize = 64 * 1024;

#[derive(Debug, thiserror::Error)]
pub enum ObjectReadError {
    #[error("Git object memory allocation failed")]
    Allocation(#[source] std::collections::TryReserveError),
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
    worker: crate::native_git::process::GitProcess<()>,
    output: BufReader<ChildStdout>,
    stderr: AbortOnDropHandle<Result<Vec<u8>, io::Error>>,
}

impl Process {
    fn start(git_dir: &Path, args: &[&str]) -> Result<(Self, ChildStdin), ObjectReadError> {
        let mut command = crate::native_git::command(git_dir)?;
        command
            .arg("--git-dir")
            .arg(git_dir)
            .args(args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut child = crate::native_git::process::GitProcess::spawn(command, ())?;
        let input = child.child.stdin.take().ok_or(ObjectReadError::Malformed)?;
        let output = child
            .child
            .stdout
            .take()
            .ok_or(ObjectReadError::Malformed)?;
        let mut stderr = child
            .child
            .stderr
            .take()
            .ok_or(ObjectReadError::Malformed)?;
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
                worker: child,
                output: BufReader::new(output),
                stderr,
            },
            input,
        ))
    }

    async fn finish(mut self) -> Result<(), ObjectReadError> {
        let status = self.worker.wait().await?;
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

pub(crate) struct GitObjectWalk {
    process: Process,
    revisions: AbortOnDropHandle<Result<(), io::Error>>,
    missing_only: bool,
}

impl GitObjectWalk {
    pub(crate) fn missing(
        git_dir: &Path,
        included: Vec<crate::ObjectId>,
        filter: Option<&str>,
    ) -> Result<Self, ObjectReadError> {
        Self::start(git_dir, included, Vec::new(), true, filter)
    }

    fn start(
        git_dir: &Path,
        included: Vec<crate::ObjectId>,
        excluded: Vec<crate::ObjectId>,
        missing_only: bool,
        filter: Option<&str>,
    ) -> Result<Self, ObjectReadError> {
        let filter = filter.map(|value| format!("--filter={value}"));
        let mut args = vec!["rev-list", "--objects", "--no-object-names", "--stdin"];
        if missing_only {
            args.push("--missing=print");
        }
        if let Some(filter) = &filter {
            args.push(filter);
        }
        let (process, mut input) = Process::start(git_dir, &args)?;
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
        Ok(Self {
            process,
            revisions,
            missing_only,
        })
    }

    pub(crate) async fn next(&mut self) -> Result<Option<crate::ObjectId>, ObjectReadError> {
        timeout(IO_TIMEOUT, async {
            while let Some(line) = header(&mut self.process.output).await? {
                if self.missing_only {
                    if let Some(oid) = line.strip_prefix(b"?") {
                        return Ok(Some(parse_oid(oid)?));
                    }
                    parse_oid(&line)?;
                } else {
                    return Ok(Some(parse_oid(&line)?));
                }
            }
            Ok(None)
        })
        .await
        .map_err(|_| ObjectReadError::Timeout)?
    }

    pub(crate) async fn finish(self) -> Result<(), ObjectReadError> {
        timeout(IO_TIMEOUT, async move {
            self.revisions.await??;
            self.process.finish().await
        })
        .await
        .map_err(|_| ObjectReadError::Timeout)?
    }
}

pub(crate) struct GitObjects {
    walk: Option<GitObjectWalk>,
    inventory: Option<std::vec::IntoIter<crate::ObjectId>>,
    batch: Process,
    requests: ChildStdin,
    // A canceled/failed streamed inspection leaves a partial native frame.
    // Never reuse that process for another object or a successful finish.
    inspection_failed: bool,
}

pub trait EdgeSink: Send {
    fn append(
        &mut self,
        parent: crate::ObjectId,
        edges: &[crate::packs::metadata::TypedEdge],
    ) -> impl std::future::Future<Output = Result<(), ObjectReadError>> + Send;
}

impl GitObjects {
    pub(crate) fn start(
        git_dir: &Path,
        included: Vec<crate::ObjectId>,
        excluded: Vec<crate::ObjectId>,
    ) -> Result<Self, ObjectReadError> {
        let walk = GitObjectWalk::start(git_dir, included, excluded, false, None)?;
        let (batch, requests) = Process::start(git_dir, &["cat-file", "--batch"])?;
        Ok(Self {
            walk: Some(walk),
            inventory: None,
            batch,
            requests,
            inspection_failed: false,
        })
    }

    pub(crate) fn packed(
        git_dir: &Path,
        ids: Vec<crate::ObjectId>,
    ) -> Result<Self, ObjectReadError> {
        let mut objects = Self::batch(git_dir)?;
        objects.inventory = Some(ids.into_iter());
        Ok(objects)
    }

    /// Persistent native reader with caller-owned bounded index iteration.
    /// Verification uses an isolated admitted object directory without alternates.
    pub(crate) fn batch(git_dir: &Path) -> Result<Self, ObjectReadError> {
        let (batch, requests) = Process::start(git_dir, &["cat-file", "--batch"])?;
        Ok(Self {
            walk: None,
            inventory: None,
            batch,
            requests,
            inspection_failed: false,
        })
    }

    /// Streams canonical hashing and typed structural extraction. Sink writes
    /// are private preparation; discard them if this returns an error or is
    /// canceled. Pack binding and graph closure remain verifier obligations.
    pub(crate) async fn inspect_graph(
        &mut self,
        oid: crate::ObjectId,
        sink: &mut impl EdgeSink,
    ) -> Result<crate::packs::metadata::CanonicalObject, ObjectReadError> {
        if self.inspection_failed {
            return Err(ObjectReadError::Malformed);
        }
        self.inspection_failed = true;
        let object = timeout(IO_TIMEOUT, async {
            self.requests
                .write_all(format!("{}\n", hex::encode(oid)).as_bytes())
                .await?;
            open_object(&mut self.batch.output, oid).await
        })
        .await
        .map_err(|_| ObjectReadError::Timeout)??;
        let canonical = object.inspect_graph(sink).await?;
        self.inspection_failed = false;
        Ok(canonical)
    }

    pub(crate) async fn next(&mut self) -> Result<Option<crate::ObjectId>, ObjectReadError> {
        if self.inspection_failed {
            return Err(ObjectReadError::Malformed);
        }
        if let Some(inventory) = &mut self.inventory {
            return Ok(inventory.next());
        }
        self.walk
            .as_mut()
            .ok_or(ObjectReadError::Malformed)?
            .next()
            .await
    }

    pub(crate) async fn read(
        &mut self,
        oid: crate::ObjectId,
    ) -> Result<GitObject<'_, BufReader<ChildStdout>>, ObjectReadError> {
        if self.inspection_failed {
            return Err(ObjectReadError::Malformed);
        }
        timeout(IO_TIMEOUT, async {
            self.requests
                .write_all(format!("{}\n", hex::encode(oid)).as_bytes())
                .await?;
            open_object(&mut self.batch.output, oid).await
        })
        .await
        .map_err(|_| ObjectReadError::Timeout)?
    }

    pub(crate) async fn finish(self) -> Result<(), ObjectReadError> {
        if self.inspection_failed {
            return Err(ObjectReadError::Malformed);
        }
        timeout(IO_TIMEOUT, async move {
            if let Some(walk) = self.walk {
                walk.finish().await?;
            }
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

fn parse_oid(bytes: &[u8]) -> Result<crate::ObjectId, ObjectReadError> {
    crate::ObjectId::from_hex(bytes).map_err(|_| ObjectReadError::Malformed)
}

pub(crate) struct GitObject<'a, R> {
    pub(crate) oid: crate::ObjectId,
    pub(crate) kind: ObjectKind,
    pub(crate) size: u64,
    pub(crate) reader: tokio::io::Take<&'a mut R>,
}

impl<R: AsyncRead + Unpin> GitObject<'_, R> {
    async fn inspect_graph(
        mut self,
        sink: &mut impl EdgeSink,
    ) -> Result<crate::packs::metadata::CanonicalObject, ObjectReadError> {
        use crate::{
            graph::stream::{CHUNK_BYTES, EdgeParser},
            packs::metadata::{CanonicalObject, PAGE_OBJECTS},
        };
        let mut canonical =
            crate::git_format::ObjectHasher::new(self.oid.format(), self.kind, self.size);
        let mut hash = blake3::Hasher::new();
        let mut parser = EdgeParser::new(self.oid.format(), self.kind);
        let mut buffer = vec![0; CHUNK_BYTES];
        // A bounded input chunk plus one crossing record bounds occurrences.
        // Repository size, wide trees and repeated parents do not grow this Vec.
        let max_edges = CHUNK_BYTES / (self.oid.len() + 4) + 1;
        let mut edges = Vec::with_capacity(max_edges);
        loop {
            let count = timeout(IO_TIMEOUT, self.reader.read(&mut buffer))
                .await
                .map_err(|_| ObjectReadError::Timeout)??;
            if count == 0 {
                break;
            }
            canonical.update(&buffer[..count]);
            hash.update(&buffer[..count]);
            edges.clear();
            parser
                .feed(&buffer[..count], |edge| edges.push(edge))
                .map_err(|_| ObjectReadError::Malformed)?;
            if edges.len() > max_edges {
                return Err(ObjectReadError::Malformed);
            }
            for batch in edges.chunks(PAGE_OBJECTS) {
                timeout(IO_TIMEOUT, sink.append(self.oid, batch))
                    .await
                    .map_err(|_| ObjectReadError::Timeout)??;
            }
        }
        parser.finish().map_err(|_| ObjectReadError::Malformed)?;
        let result = CanonicalObject {
            oid: self.oid,
            kind: self.kind,
            size: self.size,
            digest: *hash.finalize().as_bytes(),
        };
        self.finish().await?;
        if canonical.finalize() != result.oid {
            return Err(ObjectReadError::Malformed);
        }
        Ok(result)
    }
    /// Verify a packed body with constant memory, including oversized blobs.
    pub(crate) async fn fingerprint(mut self) -> Result<[u8; 32], ObjectReadError> {
        let expected = self.oid;
        let mut canonical =
            crate::git_format::ObjectHasher::new(expected.format(), self.kind, self.size);
        let mut hash = blake3::Hasher::new();
        let mut buffer = vec![0; 64 << 10];
        loop {
            let count = timeout(IO_TIMEOUT, self.reader.read(&mut buffer))
                .await
                .map_err(|_| ObjectReadError::Timeout)??;
            if count == 0 {
                break;
            }
            canonical.update(&buffer[..count]);
            hash.update(&buffer[..count]);
        }
        self.finish().await?;
        if canonical.finalize() != expected {
            return Err(ObjectReadError::Malformed);
        }
        Ok(*hash.finalize().as_bytes())
    }

    pub(crate) async fn verify(
        mut self,
        kind: ObjectKind,
        size: u64,
        digest: [u8; 32],
    ) -> Result<(), ObjectReadError> {
        if self.kind != kind || self.size != size {
            return Err(ObjectReadError::Malformed);
        }
        let expected = self.oid;
        let mut canonical = crate::git_format::ObjectHasher::new(expected.format(), kind, size);
        let mut hash = blake3::Hasher::new();
        let mut buffer = vec![0; 64 << 10];
        loop {
            let count = timeout(IO_TIMEOUT, self.reader.read(&mut buffer))
                .await
                .map_err(|_| ObjectReadError::Timeout)??;
            if count == 0 {
                break;
            }
            canonical.update(&buffer[..count]);
            hash.update(&buffer[..count]);
        }
        self.finish().await?;
        if canonical.finalize() != expected || hash.finalize().as_bytes() != &digest {
            return Err(ObjectReadError::Malformed);
        }
        Ok(())
    }

    pub(crate) async fn body(mut self) -> Result<(ObjectKind, Vec<u8>), ObjectReadError> {
        let limit = if self.kind == ObjectKind::Blob {
            INLINE_OBJECT_LIMIT
        } else {
            isize::MAX as usize
        };
        if self.size > limit as u64 {
            return Err(ObjectReadError::TooLarge);
        }
        timeout(IO_TIMEOUT, async {
            let mut body = Vec::new();
            body.try_reserve_exact(self.size as usize)
                .map_err(ObjectReadError::Allocation)?;
            body.resize(self.size as usize, 0);
            self.reader.read_exact(&mut body).await?;
            let kind = self.kind;
            let expected = self.oid;
            self.finish().await?;
            tokio::task::spawn_blocking(move || {
                if object_id(expected.format(), kind, &body) != expected {
                    return Err(ObjectReadError::Malformed);
                }
                Ok((kind, body))
            })
            .await?
        })
        .await
        .map_err(|_| ObjectReadError::Timeout)?
    }

    // cat-file separates bodies with a newline. The sized reader prevents an
    // external upload from consuming that separator or the following response.
    pub(crate) async fn finish(self) -> Result<(), ObjectReadError> {
        if self.reader.limit() != 0 {
            return Err(ObjectReadError::Malformed);
        }
        let separator = timeout(IO_TIMEOUT, self.reader.into_inner().read_u8())
            .await
            .map_err(|_| ObjectReadError::Timeout)??;
        if separator != b'\n' {
            return Err(ObjectReadError::Malformed);
        }
        Ok(())
    }
}

async fn open_object<R: AsyncRead + Unpin>(
    reader: &mut R,
    expected: crate::ObjectId,
) -> Result<GitObject<'_, R>, ObjectReadError> {
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
    let size: u64 = std::str::from_utf8(size)
        .map_err(|_| ObjectReadError::Malformed)?
        .parse()
        .map_err(|_| ObjectReadError::Malformed)?;
    let limit = if kind == ObjectKind::Blob {
        i64::MAX as u64
    } else {
        isize::MAX as usize as u64
    };
    if size > limit {
        return Err(ObjectReadError::TooLarge);
    }
    Ok(GitObject {
        oid: expected,
        kind,
        size,
        reader: reader.take(size),
    })
}

#[cfg(test)]
mod tests;
