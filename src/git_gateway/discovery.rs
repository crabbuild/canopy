use super::*;
use std::time::Instant;

pub(super) async fn is_ref_discovery(request: &GitHttpRequest) -> Result<bool, InputError> {
    if request.method == "GET" && request.path_info == "/repo.git/info/refs" {
        let mut query = url::form_urlencoded::parse(request.query.as_bytes());
        return Ok(matches!(query.next(), Some((key, value))
            if key == "service" && matches!(value.as_ref(), "git-upload-pack" | "git-receive-pack"))
            && query.next().is_none());
    }
    if !request.protocol_v2
        || request.method != "POST"
        || request.path_info != "/repo.git/git-upload-pack"
        || request.content_type.as_deref() != Some("application/x-git-upload-pack-request")
    {
        return Ok(false);
    }
    // Inspect only bounded command headers. Other requests use fetch preparation;
    // native Git still validates their complete protocol commands.
    Ok(ls_refs(&request.body.prefix(256 * 1024).await?))
}

fn ls_refs(mut bytes: &[u8]) -> bool {
    let mut command = None;
    loop {
        let Some(length) = bytes.get(..4).and_then(|header| {
            std::str::from_utf8(header)
                .ok()
                .and_then(|value| usize::from_str_radix(value, 16).ok())
        }) else {
            return false;
        };
        if length <= 1 {
            return command == Some(b"ls-refs".as_slice());
        }
        if !(5..=65520).contains(&length) {
            return false;
        }
        let Some(payload) = bytes.get(4..length) else {
            return false;
        };
        let payload = payload.strip_suffix(b"\n").unwrap_or(payload);
        if let Some(value) = payload.strip_prefix(b"command=") {
            if command.is_some() {
                return false;
            }
            command = Some(value);
        }
        bytes = &bytes[length..];
    }
}

impl GitGateway {
    pub(super) async fn discovery_cache(
        &self,
        snapshot: RefSnapshot,
    ) -> Result<GitHttpBackend, GatewayError> {
        let started = Instant::now();
        let backend = GitHttpBackend::initialize(
            self.scratch_root.clone(),
            self.disk_budget.clone(),
            &snapshot.head,
            self.repository.object_format(),
        )
        .await?;
        let mut pending: BTreeSet<_> = snapshot
            .refs
            .values()
            .filter_map(|state| state.oid)
            .collect();
        let mut visited = BTreeSet::new();
        let mut stats = Hydration::default();
        while !pending.is_empty() {
            let ids: Vec<_> = pending.iter().take(MAX_OBJECTS).copied().collect();
            let page = self
                .repository
                .selected_objects(&ids)
                .await
                .map_err(|error| GatewayError::Cell(Box::new(error)))?;
            if page.is_empty() {
                return Err(GatewayError::MalformedCache);
            }
            for object in page {
                pending.remove(&object.oid);
                visited.insert(object.oid);
                let tag = object.kind == ObjectKind::Tag;
                let target = self
                    .cache_object(&backend.cache, object, &mut stats)
                    .await?;
                if tag {
                    let target = target.ok_or(GatewayError::MalformedCache)?;
                    if !visited.contains(&target) {
                        pending.insert(target);
                    }
                }
            }
        }
        // Native discovery checks ref target existence and peels tag chains.
        // Commit parents and tree contents are needed only by later transfer RPCs.
        backend.cache.store_refs(&snapshot.refs).await?;
        tracing::debug!(
            repository = %hex::encode(self.repository.repository_id()),
            objects = stats.objects,
            bytes = stats.bytes,
            elapsed_seconds = started.elapsed().as_secs_f64(),
            "prepared Git ref discovery"
        );
        Ok(backend)
    }
}

#[cfg(test)]
mod tests {
    use super::ls_refs;

    #[test]
    fn command_headers_distinguish_discovery_from_fetch_and_arguments() {
        for bytes in [
            b"0014command=ls-refs\n00010009peel\n0000".as_slice(),
            b"0013command=ls-refs0000",
            b"000cagent=x\n0014command=ls-refs\n0001",
        ] {
            assert!(ls_refs(bytes), "{bytes:?}");
        }
        for bytes in [
            b"0012command=fetch\n0001".as_slice(),
            b"0014command=ls-refs\n0012command=fetch\n0001",
            b"00010014command=ls-refs\n0000",
            b"0014command=ls-refs\n",
            b"0014command=ls-refs\n0002",
            b"ffffcommand=ls-refs\n0001",
            b"xxxx",
        ] {
            assert!(!ls_refs(bytes), "{bytes:?}");
        }
    }
}
