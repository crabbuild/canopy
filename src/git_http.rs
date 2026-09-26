//! Git smart HTTP wire handling through Git's reference CGI implementation.

use std::{path::PathBuf, process::Stdio, time::Duration};

use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    process::Command,
};

const MAX_CGI_OUTPUT_BYTES: usize = 64 * 1024 * 1024;
const MAX_CGI_INPUT_BYTES: usize = 64 * 1024 * 1024;
const MAX_CGI_STDERR_BYTES: usize = 64 * 1024;

#[derive(Debug, thiserror::Error)]
pub enum GitHttpError {
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
}

/// One bounded smart HTTP request. The gateway authenticates before constructing it.
pub struct GitHttpRequest {
    pub method: String,
    pub path_info: String,
    pub query: String,
    pub content_type: Option<String>,
    pub protocol_v2: bool,
    pub body: Vec<u8>,
    pub authenticated: bool,
}

/// CGI response; receive-pack results remain buffered until Cell publication.
pub struct GitHttpResponse {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

/// Disposable bare repository used only while serving Git wire requests.
pub struct GitHttpBackend {
    project_root: PathBuf,
}

impl GitHttpBackend {
    pub(crate) fn git_dir(&self) -> PathBuf {
        self.project_root.join("repo.git")
    }

    /// Creates a new empty bare cache at `<root>/repo.git`.
    pub async fn initialize(project_root: PathBuf) -> Result<Self, GitHttpError> {
        tokio::fs::create_dir_all(&project_root).await?;
        let output = Command::new("git")
            .arg("init")
            .arg("--bare")
            .arg(project_root.join("repo.git"))
            .output()
            .await?;
        if !output.status.success() {
            return Err(GitHttpError::GitExit {
                status: output.status,
                stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
            });
        }
        let output = Command::new("git")
            .arg("--git-dir")
            .arg(project_root.join("repo.git"))
            .arg("symbolic-ref")
            .arg("HEAD")
            .arg("refs/heads/main")
            .output()
            .await?;
        if !output.status.success() {
            return Err(GitHttpError::GitExit {
                status: output.status,
                stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
            });
        }
        Ok(Self { project_root })
    }

    /// Runs Git's smart HTTP backend with the repository cache as its project root.
    pub async fn run(&self, request: GitHttpRequest) -> Result<GitHttpResponse, GitHttpError> {
        if !request.path_info.starts_with("/repo.git/")
            || request.path_info.contains("..")
            || request.path_info.contains('\\')
        {
            return Err(GitHttpError::InvalidPath);
        }
        if request.body.len() > MAX_CGI_INPUT_BYTES {
            return Err(GitHttpError::TooLarge);
        }
        let mut process = Command::new("git");
        // The cache's synthetic HEAD must not protect a branch by name.
        // Repository policy belongs in the Cell ref transaction.
        process
            .args(["-c", "receive.denyDeleteCurrent=ignore"])
            .arg("http-backend")
            .env("GIT_PROJECT_ROOT", &self.project_root)
            .env("GIT_HTTP_EXPORT_ALL", "1")
            .env("REQUEST_METHOD", &request.method)
            .env("PATH_INFO", &request.path_info)
            .env("QUERY_STRING", &request.query)
            .env("CONTENT_LENGTH", request.body.len().to_string())
            .env("SERVER_PROTOCOL", "HTTP/1.1")
            .stdin(Stdio::piped())
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
        let mut child = process.spawn()?;
        let Some(mut stdin) = child.stdin.take() else {
            return Err(GitHttpError::MalformedCgi);
        };
        let Some(stdout) = child.stdout.take() else {
            return Err(GitHttpError::MalformedCgi);
        };
        let Some(stderr) = child.stderr.take() else {
            return Err(GitHttpError::MalformedCgi);
        };
        let run = async {
            let (_, stdout, stderr, status) = tokio::try_join!(
                async {
                    stdin
                        .write_all(&request.body)
                        .await
                        .map_err(GitHttpError::from)
                },
                read_bounded(stdout, MAX_CGI_OUTPUT_BYTES),
                read_bounded(stderr, MAX_CGI_STDERR_BYTES),
                async { child.wait().await.map_err(GitHttpError::from) },
            )?;
            Ok::<_, GitHttpError>((status, stdout, stderr))
        };
        let (status, stdout, stderr) = tokio::time::timeout(Duration::from_secs(120), run)
            .await
            .map_err(|_| GitHttpError::Timeout)??;
        if !status.success() {
            return Err(GitHttpError::GitExit {
                status,
                stderr: String::from_utf8_lossy(&stderr).into_owned(),
            });
        }
        parse_cgi(&stdout)
    }
}

async fn read_bounded<R: AsyncRead + Unpin>(
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

fn parse_cgi(output: &[u8]) -> Result<GitHttpResponse, GitHttpError> {
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
        body: output[separator + 4..].to_vec(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

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
