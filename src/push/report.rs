use super::*;

pub(crate) const REJECTED: &str =
    "Canopy publication rejected: refs, permissions or policy changed; fetch and retry";
const PACKET_BYTES: usize = 65520;

pub(crate) fn rejected_report(
    response: &GitHttpResponse,
    reason: &str,
) -> Result<GitHttpResponse, PushError> {
    let mut bytes = response.body.as_slice();
    let mut data = Vec::new();
    let mut progress = Vec::new();
    let first = if bytes.is_empty() {
        None
    } else {
        packet(&mut bytes)?
    };
    let sideband = first.is_some_and(|payload| matches!(payload.first(), Some(1..=3)));
    if sideband {
        let mut next = first;
        while let Some(payload) = next {
            match payload.split_first() {
                Some((1, body)) => data.extend_from_slice(body),
                Some((2, _)) => write_packet(&mut progress, payload)?,
                _ => return Err(PushError::InvalidResponse),
            }
            next = packet(&mut bytes)?;
        }
        if !bytes.is_empty() {
            return Err(PushError::InvalidResponse);
        }
    } else {
        data.clone_from(&response.body);
    }
    // A client that declined report-status cannot consume per-ref failures.
    // Use an explicit HTTP failure so an empty native reply cannot imply success.
    if data.is_empty() || data == b"0000" {
        return Ok(GitHttpResponse {
            status: 409,
            headers: vec![("Content-Type".into(), "text/plain; charset=utf-8".into())],
            body: format!("{reason}\n").into_bytes(),
        });
    }
    let mut bytes = data.as_slice();
    let unpack = packet(&mut bytes)?.ok_or(PushError::InvalidResponse)?;
    if unpack != b"unpack ok\n" {
        return Err(PushError::InvalidResponse);
    }
    let mut report = Vec::new();
    write_packet(&mut report, unpack)?;
    while let Some(payload) = packet(&mut bytes)? {
        if let Some(name) = payload
            .strip_prefix(b"ok ")
            .and_then(|s| s.strip_suffix(b"\n"))
        {
            let mut rejected = b"ng ".to_vec();
            rejected.extend_from_slice(name);
            rejected.extend_from_slice(format!(" {reason}\n").as_bytes());
            write_packet(&mut report, &rejected)?;
        } else if payload.starts_with(b"ng ") {
            write_packet(&mut report, payload)?;
        } else {
            // No proc-receive hook is installed, so report-status-v2 has the
            // ordinary report shape. Fail closed on any unexpected extension.
            return Err(PushError::InvalidResponse);
        }
    }
    if !bytes.is_empty() {
        return Err(PushError::InvalidResponse);
    }
    report.extend_from_slice(b"0000");
    let body = if sideband {
        for part in report.chunks(PACKET_BYTES - 5) {
            let mut payload = vec![1];
            payload.extend_from_slice(part);
            write_packet(&mut progress, &payload)?;
        }
        progress.extend_from_slice(b"0000");
        progress
    } else {
        report
    };
    Ok(GitHttpResponse {
        status: 200,
        headers: response
            .headers
            .iter()
            .filter(|(name, _)| !name.eq_ignore_ascii_case("Content-Length"))
            .cloned()
            .collect(),
        body,
    })
}

fn packet<'a>(bytes: &mut &'a [u8]) -> Result<Option<&'a [u8]>, PushError> {
    let length = bytes
        .get(..4)
        .filter(|header| header.iter().all(u8::is_ascii_hexdigit))
        .and_then(|header| std::str::from_utf8(header).ok())
        .and_then(|header| usize::from_str_radix(header, 16).ok())
        .ok_or(PushError::InvalidResponse)?;
    if length == 0 {
        *bytes = &bytes[4..];
        return Ok(None);
    }
    if !(5..=PACKET_BYTES).contains(&length) {
        return Err(PushError::InvalidResponse);
    }
    let payload = bytes.get(4..length).ok_or(PushError::InvalidResponse)?;
    *bytes = &bytes[length..];
    Ok(Some(payload))
}

fn write_packet(bytes: &mut Vec<u8>, payload: &[u8]) -> Result<(), PushError> {
    if payload.len() + 4 > PACKET_BYTES {
        return Err(PushError::InvalidResponse);
    }
    bytes.extend_from_slice(format!("{:04x}", payload.len() + 4).as_bytes());
    bytes.extend_from_slice(payload);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn response(body: Vec<u8>) -> GitHttpResponse {
        GitHttpResponse {
            status: 200,
            headers: vec![(
                "Content-Type".into(),
                "application/x-git-receive-pack-result".into(),
            )],
            body,
        }
    }

    #[test]
    fn fragmented_sideband_preserves_native_rejections_and_rejects_every_success()
    -> Result<(), PushError> {
        let mut report = Vec::new();
        write_packet(&mut report, b"unpack ok\n")?;
        for n in 0..4096 {
            write_packet(
                &mut report,
                format!("ok refs/tags/開発-{n:04}\n").as_bytes(),
            )?;
        }
        write_packet(&mut report, b"ng refs/heads/protected hook declined\n")?;
        report.extend_from_slice(b"0000");
        let plain = rejected_report(&response(report.clone()), REJECTED)?;
        for chunk_size in [37, PACKET_BYTES - 5] {
            let mut wire = Vec::new();
            write_packet(&mut wire, b"\x02native progress\n")?;
            for chunk in report.chunks(chunk_size) {
                let mut payload = vec![1];
                payload.extend_from_slice(chunk);
                write_packet(&mut wire, &payload)?;
            }
            wire.extend_from_slice(b"0000");
            let rejected = rejected_report(&response(wire), REJECTED)?;
            let mut bytes = rejected.body.as_slice();
            assert_eq!(
                packet(&mut bytes)?,
                Some(b"\x02native progress\n".as_slice())
            );
            let mut actual = Vec::new();
            while let Some(payload) = packet(&mut bytes)? {
                assert_eq!(payload[0], 1);
                actual.extend_from_slice(&payload[1..]);
            }
            assert_eq!(actual, plain.body);
        }
        let text = String::from_utf8(plain.body).unwrap();
        assert_eq!(text.matches(REJECTED).count(), 4096);
        assert!(text.contains("ng refs/heads/protected hook declined\n"));
        Ok(())
    }

    #[test]
    fn malformed_reports_cannot_be_rewritten_as_success() {
        for body in [
            b"xxxx".as_slice(),
            b"0003",
            b"0008ok",
            b"000eunpack ok\n",
            b"000funknown hi\n0000",
            b"0006\x03x0000",
        ] {
            assert!(
                rejected_report(&response(body.to_vec()), REJECTED).is_err(),
                "{body:?}"
            );
        }
        for body in [b"".as_slice(), b"0000", b"0007\x02hi0000"] {
            assert_eq!(
                rejected_report(&response(body.to_vec()), REJECTED)
                    .unwrap()
                    .status,
                409
            );
        }
    }
}
