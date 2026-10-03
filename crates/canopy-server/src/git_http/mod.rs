//! Git smart HTTP wire handling through Git's reference CGI implementation.

use std::{
    future::poll_fn,
    path::PathBuf,
    pin::Pin,
    process::Stdio,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

pub use crate::git_cache::CacheError;
mod capture;
use crate::{
    git_cache::GitCache,
    git_input::{GitInput, MAX_FETCH_REQUEST_BYTES},
};
use bytes::Bytes;
pub use capture::NativeCaptureError;
use cellule_ltx::DiskBudget;
use futures_core::Stream;
use tokio_util::task::AbortOnDropHandle;

use tokio::{
    io::{AsyncRead, AsyncReadExt, BufReader},
    process::Command,
    sync::{mpsc, oneshot},
};

const MAX_CGI_OUTPUT_BYTES: usize = 64 * 1024 * 1024;
const MAX_CGI_STDERR_BYTES: usize = 64 * 1024;
const CHUNK_BYTES: usize = 64 * 1024;
const MAX_CGI_HEADER_BYTES: usize = 64 * 1024;
// Pack traversal and delta search can legitimately run for minutes before
// producing output. Cancellation still kills the entire process group.
pub(crate) const WORKER_DEADLINE: Duration = Duration::from_secs(3600);

#[derive(Debug, thiserror::Error)]
pub enum GitHttpError {
    #[error("Git cache failed")]
    Cache(#[from] CacheError),
    #[error("Git process I/O failed")]
    Io(#[from] std::io::Error),
    #[error("Git process exited unsuccessfully: {status}: {stderr}")]
    GitExit {
        status: std::process::ExitStatus,
        stderr: String,
    },
    #[error("Git CGI output is malformed")]
    MalformedCgi,
    #[error("Git request or response exceeds the configured limit")]
    TooLarge,
    #[error("Git process timed out")]
    Timeout,
    #[error("Git URL is outside this repository")]
    InvalidPath,
    #[error("Git response stream was interrupted")]
    Interrupted,
    #[error("Git backend requires a decoded request body")]
    EncodedInput,
}

/// One bounded smart HTTP request. The gateway authenticates before constructing it.
pub struct GitHttpRequest<B = GitInput> {
    pub method: String,
    pub path_info: String,
    pub query: String,
    pub content_type: Option<String>,
    pub gzip: bool,
    pub protocol_v2: bool,
    pub body: B,
    pub authenticated: bool,
}

/// CGI response with a streamed or collected body.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GitHttpResponse<B = Vec<u8>> {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: B,
}

/// Disposable bare repository used only while serving Git wire requests.
#[derive(Clone)]
pub struct GitHttpBackend {
    pub(crate) cache: Arc<GitCache>,
    pub(crate) nonce_seed: Option<[u8; 32]>,
    pub(crate) signers: Option<PathBuf>,
}

impl GitHttpBackend {
    pub(crate) fn git_dir(&self) -> PathBuf {
        self.cache.git_dir()
    }

    /// Creates an owned bare cache beneath the scratch root using shared disk admission.
    pub async fn initialize(
        scratch_root: PathBuf,
        budget: DiskBudget,
        head: &str,
        object_format: crate::ObjectFormat,
        native: crate::native_resources::NativeScope,
    ) -> Result<Self, GitHttpError> {
        Ok(Self {
            cache: GitCache::create(scratch_root, budget, head, object_format, native).await?,
            nonce_seed: None,
            signers: None,
        })
    }

    pub(crate) fn with_nonce(mut self, seed: Option<[u8; 32]>) -> Self {
        self.nonce_seed = seed;
        self
    }

    pub(crate) fn with_signers(&self, path: PathBuf) -> Self {
        Self {
            cache: Arc::clone(&self.cache),
            nonce_seed: self.nonce_seed,
            signers: Some(path),
        }
    }

    /// Runs Git on decoded input and collects a bounded reply for durable push publication.
    pub async fn run(&self, request: GitHttpRequest) -> Result<GitHttpResponse, GitHttpError> {
        let response = self.stream(request, ()).await?;
        self.collect(response).await
    }

    /// Receives native pack/index inputs for staged catalog preparation. The
    /// caller must authenticate and admit the decoded request before invoking
    /// this API, then capture and verify inputs before durable publication.
    pub async fn run_native_receive(
        &self,
        request: GitHttpRequest,
    ) -> Result<GitHttpResponse, GitHttpError> {
        if request.method != "POST"
            || request.path_info != "/repo.git/git-receive-pack"
            || !request.query.is_empty()
        {
            return Err(GitHttpError::InvalidPath);
        }
        let mut command = self.transport_command()?;
        command.args(["-c", "receive.unpackLimit=0"]);
        let response = self.stream_command(request, (), command).await?;
        self.collect(response).await
    }

    async fn collect(
        &self,
        response: GitHttpResponse<GitBody>,
    ) -> Result<GitHttpResponse, GitHttpError> {
        let GitHttpResponse {
            status,
            headers,
            mut body,
        } = response;
        let mut bytes = Vec::new();
        while let Some(chunk) = poll_fn(|cx| Pin::new(&mut body).poll_next(cx)).await {
            let chunk = chunk?;
            if bytes.len() + chunk.len() > MAX_CGI_OUTPUT_BYTES {
                return Err(GitHttpError::TooLarge);
            }
            bytes.extend_from_slice(&chunk);
        }
        self.cache.reconcile().await?;
        Ok(GitHttpResponse {
            status,
            headers,
            body: bytes,
        })
    }

    pub(crate) fn transport_command(&self) -> Result<Command, GitHttpError> {
        let mut process = crate::native_git::command(&self.git_dir())?;
        // The cache's HEAD must not implicitly protect a branch by name.
        // Repository policy belongs in the Cell ref transaction.
        process
            .args([
                "-c",
                "receive.denyDeleteCurrent=ignore",
                "-c",
                "receive.autogc=false",
                "-c",
                "receive.advertisePushOptions=true",
                "-c",
                "uploadpack.allowFilter=true",
                // The gateway checks every want against certified Cell edges;
                // Git's reachable-want check alone does not fence blob wants.
                "-c",
                "uploadpack.allowReachableSHA1InWant=true",
                "-c",
                "uploadpackfilter.allow=false",
                "-c",
                "uploadpackfilter.blob:none.allow=true",
                "-c",
                "uploadpackfilter.blob:limit.allow=true",
                "-c",
                "uploadpackfilter.tree.allow=true",
                "-c",
                "uploadpackfilter.object:type.allow=true",
                "-c",
                "uploadpackfilter.combine.allow=true",
                // Stream large blobs and skip expensive delta search for them.
                // Scope this to transport: the threshold also changes text merge
                // behavior, so merge-tree must retain its ordinary Git semantics.
                "-c",
                "core.bigFileThreshold=8m",
            ])
            .arg("-c")
            .arg(format!(
                "core.hooksPath={}",
                self.git_dir().join("hooks").display()
            ));
        if let Some(seed) = self.nonce_seed {
            process
                .arg("-c")
                .arg(format!("receive.certNonceSeed={}", hex::encode(seed)));
            process.args(["-c", "receive.certNonceSlop=300"]);
        }
        if let Some(signers) = &self.signers {
            process.args(["-c", "gpg.format=ssh"]);
            process
                .arg("-c")
                .arg(format!("gpg.ssh.allowedSignersFile={}", signers.display()));
        }
        Ok(process)
    }

    /// Streams Git output while retaining the caller's disposable cache owner.
    pub(crate) async fn stream<T: Send + 'static>(
        &self,
        request: GitHttpRequest,
        keep_alive: T,
    ) -> Result<GitHttpResponse<GitBody>, GitHttpError> {
        self.stream_command(request, keep_alive, self.transport_command()?)
            .await
    }

    async fn stream_command<T: Send + 'static>(
        &self,
        request: GitHttpRequest,
        keep_alive: T,
        mut process: Command,
    ) -> Result<GitHttpResponse<GitBody>, GitHttpError> {
        if request.gzip {
            return Err(GitHttpError::EncodedInput);
        }
        if !request.path_info.starts_with("/repo.git/")
            || request.path_info.contains("..")
            || request.path_info.contains('\\')
        {
            return Err(GitHttpError::InvalidPath);
        }
        process
            .arg("http-backend")
            .env("GIT_PROJECT_ROOT", self.cache.root())
            .env("GIT_HTTP_EXPORT_ALL", "1")
            .env("REQUEST_METHOD", &request.method)
            .env("PATH_INFO", &request.path_info)
            .env("QUERY_STRING", &request.query)
            .env("CONTENT_LENGTH", request.body.size().to_string())
            .env("HTTP_CONTENT_ENCODING", "identity")
            // CGI buffers upload-pack requests even when stdin is a completed file.
            // Its ceiling must agree with admission after gzip has been decoded.
            .env(
                "GIT_HTTP_MAX_REQUEST_BUFFER",
                MAX_FETCH_REQUEST_BYTES.to_string(),
            )
            .env("SERVER_PROTOCOL", "HTTP/1.1")
            .stdin(request.body.stdin()?)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        if let Some(content_type) = &request.content_type {
            process.env("CONTENT_TYPE", content_type);
        }
        if request.protocol_v2 {
            process.env("GIT_PROTOCOL", "version=2");
        }
        if request.authenticated {
            process.env("REMOTE_USER", "canopy-gateway");
        }
        start_stream(
            process,
            (keep_alive, Arc::clone(&self.cache), request.body),
            WORKER_DEADLINE,
            self.cache
                .native
                .try_admit(crate::native_resources::NativeWork::Pack)?,
        )
        .await
    }
}

/// Backpressured Git output; dropping it cancels the subprocess and releases the cache owner.
pub(crate) struct GitBody {
    receiver: mpsc::Receiver<Output>,
    _task: AbortOnDropHandle<()>,
    finished: bool,
}

enum Output {
    Chunk(Bytes),
    End(Result<(), GitHttpError>),
}

impl Stream for GitBody {
    type Item = Result<Bytes, GitHttpError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.finished {
            return Poll::Ready(None);
        }
        match self.receiver.poll_recv(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Some(Output::Chunk(bytes))) => Poll::Ready(Some(Ok(bytes))),
            Poll::Ready(Some(Output::End(result))) => {
                self.finished = true;
                Poll::Ready(result.err().map(Err))
            }
            Poll::Ready(None) => {
                self.finished = true;
                Poll::Ready(Some(Err(GitHttpError::Interrupted)))
            }
        }
    }
}

async fn start_stream<T: Send + 'static>(
    command: Command,
    keep_alive: T,
    deadline: Duration,
    native: crate::native_resources::NativePermit,
) -> Result<GitHttpResponse<GitBody>, GitHttpError> {
    let mut process = GitProcess::spawn(command, keep_alive, native)?;
    let stdout = process
        .child
        .stdout
        .take()
        .ok_or(GitHttpError::MalformedCgi)?;
    let stderr = process
        .child
        .stderr
        .take()
        .ok_or(GitHttpError::MalformedCgi)?;
    let (head_sender, head_receiver) = oneshot::channel();
    let (sender, receiver) = mpsc::channel(4);
    let task = tokio::spawn(async move {
        let mut head_sender = Some(head_sender);
        let run = async {
            let read_stdout = async {
                let mut reader = BufReader::with_capacity(CHUNK_BYTES, stdout);
                let mut header = Vec::new();
                while !header.ends_with(b"\r\n\r\n") {
                    if header.len() == MAX_CGI_HEADER_BYTES {
                        return Err(GitHttpError::TooLarge);
                    }
                    header.push(reader.read_u8().await?);
                }
                let head = parse_headers(&header)?;
                head_sender
                    .take()
                    .ok_or(GitHttpError::Interrupted)?
                    .send((head.status, head.headers))
                    .map_err(|_| GitHttpError::Interrupted)?;
                let mut buffer = vec![0; CHUNK_BYTES];
                loop {
                    let count = reader.read(&mut buffer).await?;
                    if count == 0 {
                        break;
                    }
                    sender
                        .send(Output::Chunk(Bytes::copy_from_slice(&buffer[..count])))
                        .await
                        .map_err(|_| GitHttpError::Interrupted)?;
                }
                Ok::<_, GitHttpError>(())
            };
            let ((), stderr) =
                tokio::try_join!(read_stdout, read_bounded(stderr, MAX_CGI_STDERR_BYTES),)?;
            // Keep the group leader unreaped while descendants still own pipes;
            // cancellation can then signal its group without PID reuse ambiguity.
            let status = process.wait().await?;
            if !status.success() {
                return Err(GitHttpError::GitExit {
                    status,
                    stderr: String::from_utf8_lossy(&stderr).into_owned(),
                });
            }
            Ok(())
        };
        let result = tokio::time::timeout(deadline, run)
            .await
            .map_err(|_| GitHttpError::Timeout)
            .and_then(|result| result);
        if let Err(error) = &result {
            tracing::warn!(error = ?error, deadline_seconds = deadline.as_secs(), "Git response worker failed");
        }
        // Cleanup precedes any blocked delivery of the final error.
        drop(process);
        drop(head_sender);
        let _ = sender.send(Output::End(result)).await;
    });
    let mut body = GitBody {
        receiver,
        _task: AbortOnDropHandle::new(task),
        finished: false,
    };
    let (status, headers) = match head_receiver.await {
        Ok(head) => head,
        Err(_) => {
            return Err(
                match poll_fn(|cx| Pin::new(&mut body).poll_next(cx)).await {
                    Some(Err(error)) => error,
                    _ => GitHttpError::Interrupted,
                },
            );
        }
    };
    Ok(GitHttpResponse {
        status,
        headers,
        body,
    })
}

pub(crate) use crate::native_git::process::GitProcess;

pub(crate) async fn read_bounded<R: AsyncRead + Unpin>(
    reader: R,
    limit: usize,
) -> Result<Vec<u8>, GitHttpError> {
    let mut body = Vec::new();
    reader
        .take(u64::try_from(limit).map_err(|_| GitHttpError::TooLarge)? + 1)
        .read_to_end(&mut body)
        .await?;
    if body.len() > limit {
        return Err(GitHttpError::TooLarge);
    }
    Ok(body)
}

fn parse_headers(output: &[u8]) -> Result<GitHttpResponse<()>, GitHttpError> {
    let Some(separator) = output.windows(4).position(|window| window == b"\r\n\r\n") else {
        return Err(GitHttpError::MalformedCgi);
    };
    let header =
        std::str::from_utf8(&output[..separator]).map_err(|_| GitHttpError::MalformedCgi)?;
    let mut status = 200;
    let mut headers = Vec::new();
    for line in header.split("\r\n") {
        let Some((name, value)) = line.split_once(':') else {
            return Err(GitHttpError::MalformedCgi);
        };
        let value = value.trim().to_owned();
        if name.eq_ignore_ascii_case("Status") {
            status = value
                .split_whitespace()
                .next()
                .ok_or(GitHttpError::MalformedCgi)?
                .parse()
                .map_err(|_| GitHttpError::MalformedCgi)?;
        } else {
            headers.push((name.to_owned(), value));
        }
    }
    Ok(GitHttpResponse {
        status,
        headers,
        body: (),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncWriteExt;

    #[tokio::test]
    async fn subprocess_output_is_rejected_at_the_read_limit() -> Result<(), GitHttpError> {
        let (mut writer, reader) = tokio::io::duplex(16);
        writer.write_all(b"123456").await?;
        drop(writer);
        assert!(matches!(
            read_bounded(reader, 5).await,
            Err(GitHttpError::TooLarge)
        ));
        Ok(())
    }
}

#[cfg(all(test, unix))]
mod stream_tests;
