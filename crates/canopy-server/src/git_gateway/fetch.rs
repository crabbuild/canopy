use super::*;

pub(super) struct FetchRequest {
    pub(super) wants: BTreeSet<crate::ObjectId>,
    pub(super) filter: Option<String>,
    pub(super) needs_blob_sizes: bool,
}

impl FetchRequest {
    pub(super) async fn read(request: &GitHttpRequest) -> Result<Self, InputError> {
        if request.method != "POST"
            || request.path_info != "/repo.git/git-upload-pack"
            || request.content_type.as_deref() != Some("application/x-git-upload-pack-request")
        {
            return Ok(Self {
                wants: BTreeSet::new(),
                filter: None,
                needs_blob_sizes: false,
            });
        }
        Self::parse(
            &request
                .body
                .prefix(MAX_FETCH_REQUEST_BYTES as usize)
                .await?,
        )
    }

    pub(super) fn parse(mut bytes: &[u8]) -> Result<Self, InputError> {
        let mut wants = BTreeSet::new();
        let mut filter = None;
        while !bytes.is_empty() {
            let header = bytes.get(..4).ok_or(InputError::Fetch)?;
            if !header.iter().all(u8::is_ascii_hexdigit) {
                return Err(InputError::Fetch);
            }
            let length = std::str::from_utf8(header)
                .ok()
                .and_then(|value| usize::from_str_radix(value, 16).ok())
                .ok_or(InputError::Fetch)?;
            if length <= 2 {
                bytes = &bytes[4..];
                continue;
            }
            if !(5..=65520).contains(&length) {
                return Err(InputError::Fetch);
            }
            let payload = bytes.get(4..length).ok_or(InputError::Fetch)?;
            let payload = payload.strip_suffix(b"\n").unwrap_or(payload);
            if let Some(want) = payload.strip_prefix(b"want ") {
                let oid = want
                    .split(|byte| *byte == b' ')
                    .next()
                    .ok_or(InputError::Fetch)?;
                let id = crate::ObjectId::from_hex(oid).map_err(|_| InputError::Fetch)?;
                wants.insert(id);
            }
            if let Some(value) = payload.strip_prefix(b"filter ")
                && filter.replace(value).is_some()
            {
                return Err(InputError::Fetch);
            }
            bytes = &bytes[length..];
        }
        let (filter, needs_blob_sizes) = match filter {
            Some(value) => {
                let value = std::str::from_utf8(value).map_err(|_| InputError::Fetch)?;
                (Some(value.to_owned()), check_filter_policy(value)?)
            }
            None => (None, false),
        };
        Ok(Self {
            wants,
            filter,
            needs_blob_sizes,
        })
    }
}

fn check_filter_policy(value: &str) -> Result<bool, InputError> {
    let mut needs_blob_sizes = false;
    // rev-list does not enforce uploadpackfilter.*. Match the transport policy
    // before traversal, including escaped subfilters, so sparse filters cannot
    // inspect pattern blobs outside the validated wants.
    let mut pending = vec![std::borrow::Cow::Borrowed(value)];
    while let Some(value) = pending.pop() {
        if value.contains('\0') {
            return Err(InputError::Fetch);
        }
        if let Some(combined) = value.strip_prefix("combine:") {
            for part in combined.split('+') {
                let decoded = percent_encoding::percent_decode_str(part)
                    .decode_utf8()
                    .map_err(|_| InputError::Fetch)?;
                pending.push(std::borrow::Cow::Owned(decoded.into_owned()));
            }
        } else if value.starts_with("blob:limit=") {
            needs_blob_sizes = true;
        } else if value != "blob:none"
            && !value.starts_with("blob:limit=")
            && !value.starts_with("tree:")
            && !value.starts_with("object:type=")
        {
            return Err(InputError::Fetch);
        }
    }
    Ok(needs_blob_sizes)
}

impl GitGateway {
    /// Validate every wanted object against the chosen live refs' complete
    /// certified closure. Native pack presence never authorizes a guessed OID.
    pub(crate) async fn validate_wants(
        workspace: &crate::packs::publication::NativeWorkspace,
        wants: &BTreeSet<crate::ObjectId>,
    ) -> Result<(), GatewayError> {
        if wants
            .iter()
            .any(|id| id.is_zero() || id.format() != workspace.object_format())
        {
            return Err(GatewayError::UnreachableWant);
        }
        // Even an empty discovery/negotiation group rechecks current access.
        if wants.is_empty() {
            workspace
                .contains(&[])
                .await
                .map_err(|e| GatewayError::Cell(Box::new(e)))?;
            return Ok(());
        }
        let mut ids = wants.iter().copied();
        loop {
            let page: Vec<_> = ids
                .by_ref()
                .take(crate::packs::metadata::PAGE_OBJECTS)
                .collect();
            if page.is_empty() {
                break;
            }
            if workspace
                .contains(&page)
                .await
                .map_err(|e| GatewayError::Cell(Box::new(e)))?
                .iter()
                .any(|present| !present)
            {
                return Err(GatewayError::UnreachableWant);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn packet(line: &str) -> Vec<u8> {
        format!("{:04x}{line}", line.len() + 4).into_bytes()
    }

    #[test]
    fn fetch_filters_cannot_read_unvalidated_sparse_patterns() {
        for filter in [
            "sparse:oid=HEAD:private-pattern",
            "combine:tree:0+sparse%3Aoid%3DHEAD%3Aprivate-pattern",
            "combine:combine%3Atree%253A0%2Bsparse%253Aoid%253DHEAD",
            "combine:blob:none+%00sparse:oid=HEAD",
            "combine:blob:none+%ff",
        ] {
            let request = packet(&format!("filter {filter}\n"));
            assert!(FetchRequest::parse(&request).is_err(), "{filter}");
        }
    }

    #[test]
    fn fetch_selection_covers_v0_and_v2_without_ignoring_late_wants() {
        for prefix in [b"".as_slice(), b"0012command=fetch\n0001"] {
            let mut request = prefix.to_vec();
            request.extend(packet(&format!("want {} filter\n", "12".repeat(20))));
            request.extend(packet("filter blob:none\n"));
            request.extend_from_slice(b"0000");
            request.extend(packet(&format!("want {}\n", "34".repeat(20))));
            request.extend(packet("done\n"));
            let parsed = FetchRequest::parse(&request).unwrap();
            assert_eq!(
                parsed.wants,
                BTreeSet::from([
                    crate::ObjectId::Sha1([0x12; 20]),
                    crate::ObjectId::Sha1([0x34; 20])
                ])
            );
            assert_eq!(parsed.filter.as_deref(), Some("blob:none"));
        }
        for input in [
            b"0003".as_slice(),
            b"+005x",
            b"0032want bad",
            b"0015filter blob:none\n0015filter blob:none\n",
        ] {
            assert!(FetchRequest::parse(input).is_err());
        }
    }
}
