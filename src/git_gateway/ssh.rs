use super::*;
use crate::git_http::{GitProcess, read_bounded};
use std::{pin::Pin, process::Stdio};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio_util::io::ReaderStream;

impl GitGateway {
    pub(crate) async fn ssh_fetch<R, W>(
        &self,
        reader: R,
        mut writer: W,
        actor: &str,
        protocol_v2: bool,
        admission: Arc<AdmissionPermit>,
    ) -> Result<(), GatewayError>
    where
        R: AsyncRead + Unpin,
        W: AsyncWrite + Unpin,
    {
        if self.access_level(actor).await?.is_none() {
            return Err(GatewayError::Unauthorized);
        }
        // SSH negotiation retains one immutable ref snapshot for the channel.
        // Native Git may traverse immediately after wants; hydrate before spawn.
        let cached = self.fetch_cache(&BTreeSet::new(), true).await?;
        let mut command = cached.backend.transport_command()?;
        command
            .arg("upload-pack")
            .arg(cached.backend.git_dir())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        if protocol_v2 {
            command.env("GIT_PROTOCOL", "version=2");
        }
        let mut process = GitProcess::spawn(command, (Arc::clone(&cached), admission))?;
        let mut stdin = process
            .child
            .stdin
            .take()
            .ok_or(GitHttpError::Interrupted)?;
        let mut stdout = process
            .child
            .stdout
            .take()
            .ok_or(GitHttpError::Interrupted)?;
        let stderr = process
            .child
            .stderr
            .take()
            .ok_or(GitHttpError::Interrupted)?;
        let input = async {
            let result = async {
                let mut reader = reader;
                let mut remaining = MAX_FETCH_REQUEST_BYTES as usize;
                loop {
                    let Some(group) = packet_group(&mut reader, remaining).await? else {
                        return Ok::<_, GatewayError>(());
                    };
                    remaining -= group.len();
                    let request = fetch::FetchRequest::parse(&group)?;
                    self.validate_wants(&cached.snapshot, &request.wants)
                        .await?;
                    stdin.write_all(&group).await?;
                    stdin.flush().await?;
                    if !protocol_v2 {
                        // v0 wants end at the first flush. Remaining packets are
                        // negotiation haves/done; Git owns that state machine.
                        let copied =
                            tokio::io::copy(&mut reader.take(remaining as u64 + 1), &mut stdin)
                                .await?;
                        if copied > remaining as u64 {
                            return Err(InputError::TooLarge.into());
                        }
                        return Ok(());
                    }
                }
            }
            .await;
            drop(stdin);
            result
        };
        let output = async {
            let (copied, stderr) = tokio::try_join!(
                async {
                    tokio::io::copy(&mut stdout, &mut writer)
                        .await
                        .map_err(GitHttpError::from)
                },
                read_bounded(stderr, 64 * 1024)
            )?;
            let _ = copied;
            Ok::<_, GitHttpError>(stderr)
        };
        // Git can finish a fetch while the client leaves stdin open waiting for
        // EOF. Stop input when output closes; never wait for both directions.
        tokio::pin!(input, output);
        let stderr = tokio::select! {
            result = &mut output => result?,
            result = &mut input => { result?; output.await? }
        };
        let status = process.child.wait().await?;
        process.disarm();
        if !status.success() {
            return Err(GitHttpError::GitExit {
                status,
                stderr: String::from_utf8_lossy(&stderr).into_owned(),
            }
            .into());
        }
        Ok(())
    }

    pub(crate) async fn ssh_push<R, W>(
        &self,
        mut reader: R,
        mut writer: W,
        actor: &str,
        admission: Arc<AdmissionPermit>,
    ) -> Result<(), GatewayError>
    where
        R: AsyncRead + Unpin + Send + 'static,
        W: AsyncWrite + Unpin,
    {
        if self
            .access_level(actor)
            .await?
            .is_none_or(|scope| scope < TokenScope::Write)
        {
            return Err(GatewayError::Unauthorized);
        }
        let advertisement = self
            .handle(
                rpc("GET", Body::empty()),
                actor,
                None,
                Some(Arc::clone(&admission)),
            )
            .await?;
        if advertisement.status != 200 {
            return Err(GatewayError::Unauthorized);
        }
        // HTTP adds one service announcement and flush before native refs.
        // Strip only this envelope while streaming the advertisement.
        write_body(
            advertisement.body,
            &mut writer,
            b"001f# service=git-receive-pack\n0000",
        )
        .await?;
        let commands = tokio::time::timeout(
            std::time::Duration::from_secs(120),
            packet_group(&mut reader, 40 * 1024 * 1024),
        )
        .await
        .map_err(|_| InputError::Timeout)??;
        let Some(commands) = commands else {
            return Ok(());
        };
        if commands == b"0000" {
            return Ok(());
        }
        let needs_pack = has_new_objects(&commands)?;
        // Stock send-pack closes its write fd after pack-objects. Delete-only
        // pushes have no pack and await status without closing stdin.
        let body = if needs_pack {
            Body::from_stream(ReaderStream::new(
                std::io::Cursor::new(commands).chain(reader),
            ))
        } else {
            Body::from(commands)
        };
        let response = self
            .handle(rpc("POST", body), actor, None, Some(admission))
            .await?;
        if response.status != 200 {
            return Err(GitHttpError::Interrupted.into());
        }
        write_body(response.body, &mut writer, b"").await
    }
}

async fn write_body(
    body: Body,
    writer: &mut (impl AsyncWrite + Unpin),
    envelope: &[u8],
) -> Result<(), GatewayError> {
    tokio::time::timeout(std::time::Duration::from_secs(120), async {
        let mut body = body.into_data_stream();
        let mut skipped = 0;
        while let Some(chunk) =
            std::future::poll_fn(|cx| futures_core::Stream::poll_next(Pin::new(&mut body), cx))
                .await
        {
            let chunk = chunk.map_err(InputError::Body)?;
            let skip = (envelope.len() - skipped).min(chunk.len());
            if chunk[..skip] != envelope[skipped..skipped + skip] {
                return Err(GitHttpError::MalformedCgi.into());
            }
            skipped += skip;
            writer.write_all(&chunk[skip..]).await?;
        }
        if skipped != envelope.len() {
            return Err(GitHttpError::MalformedCgi.into());
        }
        writer.flush().await?;
        Ok(())
    })
    .await
    .map_err(|_| GitHttpError::Timeout)?
}

fn rpc(method: &str, body: Body) -> GitHttpRequest<Body> {
    let discovery = method == "GET";
    GitHttpRequest {
        method: method.into(),
        path_info: if discovery {
            "/repo.git/info/refs"
        } else {
            "/repo.git/git-receive-pack"
        }
        .into(),
        query: if discovery {
            "service=git-receive-pack"
        } else {
            ""
        }
        .into(),
        content_type: (!discovery).then(|| "application/x-git-receive-pack-request".into()),
        gzip: false,
        protocol_v2: false,
        body,
        authenticated: true,
    }
}

async fn packet_group(
    reader: &mut (impl AsyncRead + Unpin),
    limit: usize,
) -> Result<Option<Vec<u8>>, GatewayError> {
    let mut bytes = Vec::new();
    loop {
        let mut header = [0; 4];
        let read = reader.read(&mut header[..1]).await?;
        if read == 0 && bytes.is_empty() {
            return Ok(None);
        }
        if read == 0 {
            return Err(InputError::Fetch.into());
        }
        reader.read_exact(&mut header[1..]).await?;
        if !header.iter().all(u8::is_ascii_hexdigit) {
            return Err(InputError::Fetch.into());
        }
        let length = std::str::from_utf8(&header)
            .ok()
            .and_then(|s| usize::from_str_radix(s, 16).ok())
            .ok_or(InputError::Fetch)?;
        if !(length <= 1 || (5..=65520).contains(&length)) {
            return Err(InputError::Fetch.into());
        }
        let length = length.max(4);
        if bytes.len() + length > limit {
            return Err(InputError::TooLarge.into());
        }
        bytes.extend_from_slice(&header);
        let start = bytes.len();
        bytes.resize(start + length - 4, 0);
        reader.read_exact(&mut bytes[start..]).await?;
        if header == *b"0000" {
            return Ok(Some(bytes));
        }
    }
}

fn has_new_objects(mut bytes: &[u8]) -> Result<bool, InputError> {
    let mut has_new = false;
    while bytes != b"0000" {
        let length = std::str::from_utf8(bytes.get(..4).ok_or(InputError::Commands)?)
            .ok()
            .and_then(|s| usize::from_str_radix(s, 16).ok())
            .ok_or(InputError::Commands)?;
        let packet = bytes.get(4..length).ok_or(InputError::Commands)?;
        if !packet.starts_with(b"shallow ") {
            let oid = packet.get(41..81).ok_or(InputError::Commands)?;
            if !oid.iter().all(u8::is_ascii_hexdigit) {
                return Err(InputError::Commands);
            }
            has_new |= oid.iter().any(|c| *c != b'0');
        }
        bytes = bytes.get(length..).ok_or(InputError::Commands)?;
    }
    Ok(has_new)
}
